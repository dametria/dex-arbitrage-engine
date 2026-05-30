"""
chain_searcher.py — Поиск арбитражных цепочек через on-chain QuoterV2.

Ключевые особенности:
  • quoteExactInput() — атомарное котирование всей цепочки
  • Проверка ликвидности пула ДО котирования (отсеивает 60-80% мусора)
  • Кэш ликвидности на цикл — один RPC вызов per пул, не per цепочка
  • Расчёт net_profit с вычетом GAS_COST_USD
  • dex_types[] для MultiHopSwapUniversal (0=Uni/Sushi, 1=Pancake)

Параметры ликвидности в .env:
  MAX_PRICE_IMPACT_PCT = 0.10   (макс. допустимый price impact %)
    impact = AMOUNT / (2 × liquidity_usd)
    Если impact > MAX_PRICE_IMPACT_PCT → пул отбрасывается

  Формула минимальной ликвидности:
    min_liq_usd = AMOUNT / (2 × MAX_PRICE_IMPACT_PCT / 100)
    При AMOUNT=100, MAX_PRICE_IMPACT_PCT=0.10:
    min_liq_usd = 100 / (2 × 0001) = $50,000
"""

import asyncio
import ast
import json
import os
from typing import Any, Callable, Awaitable

import aiohttp
from aiohttp import ClientResponseError
from dotenv import load_dotenv
from loguru import logger
from web3 import AsyncWeb3
from web3.exceptions import ContractLogicError, BadFunctionCallOutput
from web3.providers import AsyncHTTPProvider

load_dotenv()

MAX_CONCURRENT   = int(os.getenv("MAX_CONCURRENT_REQUESTS", "5"))
BATCH_SIZE       = int(os.getenv("BATCH_SIZE", "10"))
RETRY_ATTEMPTS   = int(os.getenv("RETRY_ATTEMPTS", "3"))
RETRY_DELAY      = float(os.getenv("RETRY_DELAY", "2.0"))
RATE_LIMIT_DELAY = 10.0

NETWORK_ERRORS = (ConnectionResetError, ConnectionError, OSError, TimeoutError)

# ── DEX → dexType для MultiHopSwapUniversal ──────────────────────────────────
# Контракт:
#   0 = Uniswap V3 / SushiSwap V3 / Ramses CL  (uniswapV3SwapCallback)
#   1 = PancakeSwap V3                          (pancakeV3SwapCallback)
#   2 = UniV2-совместимые V2 пулы              (transfer + swap, нет callback)
#   3 = SushiSwap V2                            (transfer + swap, нет callback)
#   4 = PancakeSwap V2                          (transfer + swap, нет callback)
#
# Все V2 DEX используют UniV2-совместимый интерфейс (getReserves + swap).
# Разница типов 2/3/4 только для идентификации DEX, механика одинакова.
DEX_TYPE_MAP: dict[str, int] = {
    # ── V3 (callback) ─────────────────────────────────────────────────────────
    "uniswap":       0,   # Uniswap V3
    "sushiswap":     0,   # SushiSwap V3 (uniswapV3SwapCallback)
    "ramses":        0,   # Ramses CL — UniV3-совместимый
    "pancakeswap":   1,   # PancakeSwap V3

    # ── V2 (нет callback, transfer ДО swap) ───────────────────────────────────
    "uniswap_v2":    2,   # Uniswap V2
    # ── Algebra V3 (algebraSwapCallback) ─────────────────────────────────────
    # Camelot V2 = Algebra V1.9 — концентрированная ликвидность, НЕ классический UniV2!
    # Эти DEX используют algebraSwapCallback вместо uniswapV3SwapCallback
    "camelot_v2":    5,   # Camelot V2 — Algebra V1.9 (algebraSwapCallback!)
    "zyberswap_v2":  5,   # ZyberSwap  — Algebra-based
    "swapr_v2":      5,   # Swapr      — Algebra-based
    "chronos_v2":    5,   # Chronos    — Algebra-based (требует проверки)

    # ── Классические UniV2 (transfer + swap, нет callback) ────────────────────
    "arbswap_v2":    2,   # ArbSwap V2
    "deltaswap_v2":  2,   # DeltaSwap V2
    "elkfinance_v2": 2,   # Elk Finance V2
    "magicswap_v2":  2,   # MagicSwap V2
    "mindgames_v2":  2,   # MindGames V2
    "oreoswap_v2":   2,   # OreoSwap V2
    "ramses_v2":     2,   # Ramses V2 (Solidly AMM — классический V2)
    "solidlizard_v2":2,   # SolidLizard V2
    "spartadex_v2":  2,   # SpartaDEX V2
    "sterling_v2":   2,   # Sterling V2
    "sushiswap_v2":  3,   # SushiSwap V2
    "pancakeswap_v2":4,   # PancakeSwap V2
}

# Типы, которые являются классическим V2 (transfer + swap, нет callback)
V2_DEX_TYPES = {2, 3, 4}

# Типы Algebra/Camelot (V3-архитектура с algebraSwapCallback)
ALGEBRA_DEX_TYPES = {5}

# dexId которые не поддерживаются (0x-адреса как dexId, нестандартные пулы)
# Такие пулы будут пропущены при котировании
_UNSUPPORTED_DEX_PREFIX = ("0x",)


def _is_supported_dex(dex_id: str) -> bool:
    """True если dexId известен и поддерживается контрактом."""
    if any(dex_id.startswith(p) for p in _UNSUPPORTED_DEX_PREFIX):
        return False
    return dex_id.lower() in DEX_TYPE_MAP


def dex_type_for(dex_id: str) -> int:
    """
    Возвращает uint8 тип пула для контракта MultiHopSwapUniversal.
    Неизвестные, но поддерживаемые DEX → 0 (UniV3-совместимый).
    Неподдерживаемые (0x-адрес как dexId) → 0 (будут отфильтрованы раньше).
    """
    t = DEX_TYPE_MAP.get(dex_id.lower())
    if t is None:
        if not any(dex_id.startswith(p) for p in _UNSUPPORTED_DEX_PREFIX):
            logger.warning(f"Неизвестный dexId='{dex_id}', тип → 0")
        return 0
    return t


def is_v2_dex(dex_id: str) -> bool:
    """True если пул классический V2 (transfer + swap, нет callback)."""
    return dex_type_for(dex_id) in V2_DEX_TYPES


def is_algebra_dex(dex_id: str) -> bool:
    """True если пул Algebra/Camelot (algebraSwapCallback, V3-архитектура)."""
    return dex_type_for(dex_id) in ALGEBRA_DEX_TYPES


# ══════════════════════════════════════════════════════════════
#  Построение packed path для quoteExactInput
# ══════════════════════════════════════════════════════════════

def build_quote_path(pools_in_order: list[dict], token_keys: list[tuple]) -> bytes:
    """
    abi.encodePacked(token0, fee0, token1, fee1, token2, ...)
    Первый токен добавляется один раз, затем fee+tokenOut для каждого пула.
    """
    path = b""
    for idx, (pool, (in_key, out_key)) in enumerate(zip(pools_in_order, token_keys)):
        if idx == 0:
            path += bytes.fromhex(pool[in_key]["address"].replace("0x", "").zfill(40))
        path += pool["fee"].to_bytes(3, "big")
        path += bytes.fromhex(pool[out_key]["address"].replace("0x", "").zfill(40))
    return path


# ══════════════════════════════════════════════════════════════
#  ABI
# ══════════════════════════════════════════════════════════════

# ── ABI V2 пула (getReserves + token0/token1) ────────────────────────────────
POOL_V2_ABI = [
    {
        "inputs": [],
        "name": "getReserves",
        "outputs": [
            {"internalType": "uint112", "name": "reserve0",          "type": "uint112"},
            {"internalType": "uint112", "name": "reserve1",          "type": "uint112"},
            {"internalType": "uint32",  "name": "blockTimestampLast","type": "uint32"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token0",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token1",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    # totalSupply нужен для оценки ликвидности V2 пула
    {
        "inputs": [],
        "name": "totalSupply",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

POOL_LIQ_ABI = [
    {
        "inputs": [],
        "name": "liquidity",
        "outputs": [{"internalType": "uint128", "name": "", "type": "uint128"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"name": "sqrtPriceX96",              "type": "uint160"},
            {"name": "tick",                      "type": "int24"},
            {"name": "observationIndex",          "type": "uint16"},
            {"name": "observationCardinality",    "type": "uint16"},
            {"name": "observationCardinalityNext","type": "uint16"},
            # uint32 совместим с Uniswap(uint8) и PancakeSwap(uint32)
            {"name": "feeProtocol",               "type": "uint32"},
            {"name": "unlocked",                  "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

QUOTE_EXACT_INPUT_ABI = [
    {
        "inputs": [
            {"internalType": "bytes",   "name": "path",     "type": "bytes"},
            {"internalType": "uint256", "name": "amountIn", "type": "uint256"},
        ],
        "name": "quoteExactInput",
        "outputs": [
            {"internalType": "uint256",   "name": "amountOut",                   "type": "uint256"},
            {"internalType": "uint160[]", "name": "sqrtPriceX96AfterList",       "type": "uint160[]"},
            {"internalType": "uint32[]",  "name": "initializedTicksCrossedList", "type": "uint32[]"},
            {"internalType": "uint256",   "name": "gasEstimate",                 "type": "uint256"},
        ],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {
                "components": [
                    {"internalType": "address", "name": "tokenIn",           "type": "address"},
                    {"internalType": "address", "name": "tokenOut",          "type": "address"},
                    {"internalType": "uint256", "name": "amountIn",          "type": "uint256"},
                    {"internalType": "uint24",  "name": "fee",               "type": "uint24"},
                    {"internalType": "uint160", "name": "sqrtPriceLimitX96", "type": "uint160"},
                ],
                "internalType": "struct IQuoterV2.QuoteExactInputSingleParams",
                "name": "params",
                "type": "tuple",
            }
        ],
        "name": "quoteExactInputSingle",
        "outputs": [
            {"internalType": "uint256", "name": "amountOut",               "type": "uint256"},
            {"internalType": "uint160", "name": "sqrtPriceX96After",       "type": "uint160"},
            {"internalType": "uint32",  "name": "initializedTicksCrossed", "type": "uint32"},
            {"internalType": "uint256", "name": "gasEstimate",             "type": "uint256"},
        ],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]


# ══════════════════════════════════════════════════════════════
#  Конвертация sqrtPriceX96 → USD-ликвидность
# ══════════════════════════════════════════════════════════════

def liq_units_to_usd(
    liquidity: int,
    sqrt_price_x96: int,
    dec0: int,
    dec1: int,
    token0_is_stable: bool,
) -> float:
    """
    Переводит liquidity (единицы V3) в приблизительный USD.

    Формула: L = liquidity / sqrt(price) × 2  (упрощение для in-range)
    Умножаем на цену стейблкоина чтобы получить USD.

    token0_is_stable: True если token0 — USDC/USDT/DAI (6 decimals стейбл).
    """
    if sqrt_price_x96 == 0 or liquidity == 0:
        return 0.0

    # price = (sqrtP / 2^96)^2 × 10^dec0 / 10^dec1
    # = кол-во token1 за 1 token0
    price = (sqrt_price_x96 / (2 ** 96)) ** 2 * (10 ** dec0) / (10 ** dec1)

    if price <= 0:
        return 0.0

    sqrt_p = sqrt_price_x96 / (2 ** 96)

    # Количество token0 и token1 в активном диапазоне (упрощение для текущей цены)
    # amount0 ≈ L / sqrt_p,  amount1 ≈ L × sqrt_p
    amount0 = liquidity / sqrt_p / (10 ** dec0)   # в единицах token0
    amount1 = liquidity * sqrt_p / (10 ** dec1)   # в единицах token1

    if token0_is_stable:
        # token0 — стейбл ($1), token1 — что-то другое
        # USD = amount0 × 1 + amount1 × (1/price)
        usd = amount0 + amount1 / price
    else:
        # token1 — стейбл ($1), token0 — что-то другое
        # USD = amount0 × price + amount1 × 1
        usd = amount0 * price + amount1

    return usd


STABLE_SYMBOLS = {"USDC", "USDT", "DAI", "USD₮0", "USDC.e", "USDT.e", "BUSD", "LUSD", "crvUSD"}


def is_stable(symbol: str) -> bool:
    return symbol.upper() in {s.upper() for s in STABLE_SYMBOLS}


# ══════════════════════════════════════════════════════════════
#  Retry-декоратор
# ══════════════════════════════════════════════════════════════

def with_retry(attempts: int = RETRY_ATTEMPTS, delay: float = RETRY_DELAY):
    def decorator(fn):
        async def wrapper(*args, **kwargs):
            last_err = None
            for attempt in range(1, attempts + 1):
                try:
                    return await fn(*args, **kwargs)
                except ClientResponseError as e:
                    last_err = e
                    wait = (RATE_LIMIT_DELAY if e.status == 429 else delay) * attempt
                    if attempt < attempts:
                        await asyncio.sleep(wait)
                except NETWORK_ERRORS as e:
                    last_err = e
                    if attempt < attempts:
                        await asyncio.sleep(delay * attempt)
                except (ContractLogicError, BadFunctionCallOutput):
                    return None
                except (aiohttp.ClientError, *NETWORK_ERRORS):
                    return None
            logger.debug(f"[{fn.__name__}] Все попытки исчерпаны: {last_err}")
            return None
        return wrapper
    return decorator


# ══════════════════════════════════════════════════════════════
#  MultiChainSearcher
# ══════════════════════════════════════════════════════════════

class MultiChainSearcher:
    """
    Ищет арбитражные цепочки 2/3/4/5 шагов через QuoterV2.

    Проверка ликвидности:
      Перед котированием каждый пул цепочки проверяется на
      минимальную ликвидность. Пул с недостаточной ликвидностью
      означает высокий price impact — цепочка отбрасывается БЕЗ
      RPC вызова к квотеру, что экономит запросы.

      Кэш ликвидности: обновляется один раз в начале каждого цикла
      для всех пулов из all_pools.json. Это ~258 вызовов, но они
      параллельны и выполняются один раз, а не при каждой цепочке.

    chain_info:
      pools_addresses     : list[str]  — адреса пулов
      dex_types           : list[int]  — 0=Uni/Sushi, 1=Pancake
      token_path          : list[str]  — токены маршрута
      amount_in_wei       : int
      amount_out_min      : int
      step_count          : int
      chain_label         : str
      dex_route           : str
      profit              : float      — net (после газа)
      profit_pct          : float
      cumulative_decimals : int
      best_pools          : list[dict]
      liq_info            : list[dict] — ликвидность каждого пула
    """

    def __init__(
        self,
        active_steps: list[int],
        execute_fn: Callable[[dict], Awaitable[None]],
        min_profit_pct: float,
        shared_web3: "AsyncWeb3 | None" = None,
    ):
        bad = [s for s in active_steps if s not in (2, 3, 4, 5)]
        if bad:
            raise ValueError(f"active_steps должны быть в (2,3,4,5): {bad}")

        self.active_steps   = sorted(set(active_steps))
        self.execute_fn     = execute_fn
        self.min_profit_pct = min_profit_pct
        self.script_dir     = os.path.dirname(os.path.abspath(__file__))

        self._own_web3 = shared_web3 is None
        self.web3      = shared_web3 or AsyncWeb3(
            AsyncHTTPProvider(os.getenv("ARBITRUM_RPC_URL"))
        )
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT)

        self.all_pools: dict               = {}
        self.cumulative_token_name: str    = ""
        self.cumulative_token_address: str = ""
        self.list_quoter: dict             = {}
        self.amount_human: float           = 0.0
        self.slippage_bps: int             = 8
        self.gas_cost_usd: float           = 0.028
        self.max_price_impact_pct: float   = 0.10

        # Кэш ликвидности: {pool_address_lower: liq_usd | None}
        # None = ошибка чтения, 0 = пустой пул
        self._liq_cache: dict[str, float | None] = {}

    # ── Загрузка данных ───────────────────────────────────────────────────────

    def uploading_data(self) -> bool:
        try:
            def load(p):
                with open(os.path.join(self.script_dir, p), encoding="utf-8") as f:
                    return json.load(f)

            self.all_pools = load("pool_collection/pools/all_pools.json")

            name, addr = ast.literal_eval(os.getenv("CUMULATIVE_TOKEN"))
            self.cumulative_token_name    = name
            self.cumulative_token_address = addr.lower()

            # Квотеры нужны для V3 и Algebra пулов.
            # V3 (Uni/Sushi/Pancake/Ramses): QuoterV2.quoteExactInput()
            # Algebra (Camelot и др.): свой QuoterV2 с тем же интерфейсом
            # Классические V2 (UniV2/SushiV2): котируются через getReserves()
            self.list_quoter = {
                "uniswap":     os.getenv("UNISWAP_QUOTER", ""),
                "sushiswap":   os.getenv("SUSHISWAP_QUOTER", ""),
                "pancakeswap": os.getenv("PANCAKESWAP_QUOTER", ""),
                "ramses":      os.getenv("RAMSES_QUOTER", ""),
                # Algebra-based DEX — у каждого свой квотер
                "camelot_v2":    os.getenv("CAMELOT_QUOTER", ""),
                "zyberswap_v2":  os.getenv("CAMELOT_QUOTER", ""),  # тот же Algebra
                "swapr_v2":      os.getenv("CAMELOT_QUOTER", ""),  # тот же Algebra
                "chronos_v2":    os.getenv("CAMELOT_QUOTER", ""),  # тот же Algebra
            }
            self.amount_human          = float(os.getenv("AMOUNT"))
            self.slippage_bps          = int(os.getenv("SLIPPAGE_BPS", "8"))
            self.gas_cost_usd          = float(os.getenv("GAS_COST_USD", "0.028"))
            self.max_price_impact_pct  = float(os.getenv("MAX_PRICE_IMPACT_PCT", "0.10"))
            return True
        except Exception as e:
            logger.error(f"Ошибка загрузки данных: {e}")
            return False

    # ══════════════════════════════════════════════════════════════
    #  КЭШИРОВАНИЕ ЛИКВИДНОСТИ
    # ══════════════════════════════════════════════════════════════

    async def _fetch_pool_liquidity(self, pool_info: dict) -> "float | None":
        """
        Читает ликвидность одного пула on-chain.
        Поддерживает V2 и V3 пулы.
        Возвращает None при ошибке, 0.0 если пул пустой.
        """
        pool_addr = pool_info["pool"]
        dex_id    = pool_info.get("dexId", "uniswap")
        is_v2     = is_v2_dex(dex_id)

        dec0  = pool_info["token0"]["decimals"]
        dec1  = pool_info["token1"]["decimals"]
        sym0  = pool_info["token0"]["symbol"]
        sym1  = pool_info["token1"]["symbol"]

        t0_stable = is_stable(sym0)
        t1_stable = is_stable(sym1)

        try:
            async with self._semaphore:
                addr_cs = self.web3.to_checksum_address(pool_addr)

                if is_v2:
                    # ── V2: ликвидность через резервы ─────────────────────────
                    # Поддерживаем два формата getReserves():
                    #   Стандарт UniV2: (uint112, uint112, uint32) — 3 поля
                    #   Camelot/Algebra: (uint128, uint128) — 2 поля (без timestamp)
                    contract = self.web3.eth.contract(address=addr_cs, abi=POOL_V2_ABI)
                    try:
                        reserves = await contract.functions.getReserves().call()
                    except (ContractLogicError, BadFunctionCallOutput):
                        # Fallback: Camelot/Algebra формат (2 поля)
                        _abi2: Any = [{"inputs":[],"name":"getReserves","outputs":[
                            {"name":"reserve0","type":"uint128"},
                            {"name":"reserve1","type":"uint128"},
                        ],"stateMutability":"view","type":"function"}]
                        contract2 = self.web3.eth.contract(address=addr_cs, abi=_abi2)
                        reserves = await contract2.functions.getReserves().call()
                    r0 = reserves[0]
                    r1 = reserves[1]

                    if r0 == 0 and r1 == 0:
                        return 0.0

                    if not t0_stable and not t1_stable:
                        return float("inf")

                    # Оцениваем USD через стейбл-резерв × 2
                    if t0_stable:
                        liq_usd = (r0 / 10 ** dec0) * 2
                    else:
                        liq_usd = (r1 / 10 ** dec1) * 2

                    return liq_usd

                else:
                    # ── V3: ликвидность через liquidity + sqrtPriceX96 ────────
                    contract = self.web3.eth.contract(address=addr_cs, abi=POOL_LIQ_ABI)
                    liq_raw, slot0 = await asyncio.gather(
                        contract.functions.liquidity().call(),
                        contract.functions.slot0().call(),
                        return_exceptions=True,
                    )

                    if isinstance(liq_raw, BaseException) or isinstance(slot0, BaseException):
                        return None

                    if liq_raw == 0:
                        return 0.0

                    if not t0_stable and not t1_stable:
                        return float("inf")

                    liq_usd = liq_units_to_usd(
                        liq_raw, slot0[0], dec0, dec1,
                        token0_is_stable=t0_stable,
                    )
                    return liq_usd

        except (ContractLogicError, BadFunctionCallOutput, *NETWORK_ERRORS):
            return None
        except Exception:
            return None

    async def refresh_liquidity_cache(self) -> None:
        """
        Обновляет кэш ликвидности для ВСЕХ пулов из all_pools.json.
        Вызывается один раз в начале каждого цикла поиска.
        Параллельные запросы ограничены семафором MAX_CONCURRENT.
        """
        # Собираем уникальные пулы
        all_pool_infos: dict[str, dict] = {}  # addr_lower → pool_info
        for pools in self.all_pools.values():
            for p in pools:
                key = p["pool"].lower()
                if key not in all_pool_infos:
                    all_pool_infos[key] = p

        total = len(all_pool_infos)
        logger.info(f"  🔍 Обновляем ликвидность {total} пулов...")

        results = await asyncio.gather(
            *[self._fetch_pool_liquidity(info)
              for info in all_pool_infos.values()],
            return_exceptions=True,
        )

        self._liq_cache = {}
        ok = skipped = zero = 0
        for pool_info, liq in zip(all_pool_infos.values(), results):
            key = pool_info["pool"].lower()
            if isinstance(liq, BaseException) or liq is None:
                self._liq_cache[key] = None
                skipped += 1
            elif liq == 0.0:
                self._liq_cache[key] = 0.0
                zero += 1
            else:
                self._liq_cache[key] = liq
                ok += 1

        # Минимальная ликвидность для нашей суммы
        min_liq = self._min_liq_usd()
        # Сколько пулов проходит фильтр
        passing = sum(
            1 for v in self._liq_cache.values()
            if v is not None and (v == float("inf") or v >= min_liq)
        )

        logger.info(
            f"  📊 Ликвидность: читаем={ok} | пустых={zero} | ошибок={skipped} "
            f"| проходят фильтр=${min_liq:,.0f}: {passing}/{total}"
        )

    def _min_liq_usd(self) -> float:
        """
        Минимальная ликвидность пула в USD при которой price impact
        не превышает MAX_PRICE_IMPACT_PCT.

        impact = AMOUNT / (2 × liq_usd)  <  max_impact/100
        → liq_usd > AMOUNT / (2 × max_impact/100)
        → liq_usd > AMOUNT × 100 / (2 × max_impact)
        """
        return self.amount_human * 100.0 / (2.0 * self.max_price_impact_pct)

    def _pool_liq_ok(self, pool_addr: str) -> "tuple[bool, float | None]":
        """
        Проверяет пул по кэшу.
        Возвращает (passed, liq_usd).
        passed=True если ликвидность достаточна или нет данных (даём шанс).
        """
        key = pool_addr.lower()
        liq = self._liq_cache.get(key)

        # Нет в кэше — не блокируем (не хотим пропускать из-за ошибки чтения)
        if liq is None:
            return True, None

        # Пустой пул — блокируем
        if liq == 0.0:
            return False, 0.0

        # Нет стейбла — не можем оценить, разрешаем
        if liq == float("inf"):
            return True, None

        min_liq = self._min_liq_usd()
        return liq >= min_liq, liq

    def _chain_liq_ok(self, pools: list[dict]) -> "tuple[bool, str]":
        """
        Проверяет все пулы цепочки.
        Возвращает (ok, причина_отказа).
        Блокирует цепочку по СЛАБЕЙШЕМУ пулу.
        """
        min_liq = self._min_liq_usd()
        for pool in pools:
            ok, liq_usd = self._pool_liq_ok(pool["pool"])
            if not ok:
                sym0 = pool["token0"]["symbol"]
                sym1 = pool["token1"]["symbol"]
                liq_str = f"${liq_usd:,.0f}" if liq_usd is not None else "?"
                return False, (
                    f"{sym0}/{sym1} лик={liq_str} < мин=${min_liq:,.0f} "
                    f"(impact>{self.max_price_impact_pct:.2f}%)"
                )
        return True, ""

    # ══════════════════════════════════════════════════════════════
    #  Определение ключей токенов
    # ══════════════════════════════════════════════════════════════

    def _init_tokens(self, pool: dict) -> "tuple[str | None, str | None]":
        if pool["token0"]["address"].lower() == self.cumulative_token_address:
            return "token1", "token0"
        elif pool["token1"]["address"].lower() == self.cumulative_token_address:
            return "token0", "token1"
        return None, None

    @staticmethod
    def _tokens_by_addr(pool: dict, addr: str) -> "tuple[str | None, str | None]":
        a = addr.lower()
        if pool["token0"]["address"].lower() == a:
            return "token0", "token1"
        elif pool["token1"]["address"].lower() == a:
            return "token1", "token0"
        return None, None

    # ══════════════════════════════════════════════════════════════
    #  Котирование
    # ══════════════════════════════════════════════════════════════

    @with_retry()
    async def _quote_chain_atomic(
        self,
        pools_in_order: list[dict],
        token_keys: list[tuple],
        amount_in_wei: int,
        dex_id: str,
    ) -> "int | None":
        """
        Атомарное котирование всей цепочки через quoteExactInput.
        Используется ТОЛЬКО если все пулы V3.
        """
        quoter_addr = self.list_quoter.get(dex_id)
        if not quoter_addr:
            return None
        path = build_quote_path(pools_in_order, token_keys)
        async with self._semaphore:
            quoter = self.web3.eth.contract(
                address=self.web3.to_checksum_address(quoter_addr),
                abi=QUOTE_EXACT_INPUT_ABI,
            )
            result = await quoter.functions.quoteExactInput(
                path, amount_in_wei
            ).call()
            out = result[0]
            return out if out > 0 else None

    async def _quote_chain_mixed(
        self,
        pools_in_order: list[dict],
        token_keys: list[tuple],
        amount_in_wei: int,
    ) -> "int | None":
        """
        Пошаговое котирование для смешанных V2+V3 цепочек.
        Каждый V2 пул котируется через _quote_v2(),
        каждый V3 — через _quote_single().
        Менее точен чем quoteExactInput для чистых V3 цепочек,
        но единственный способ для смешанных маршрутов.
        """
        current = amount_in_wei
        for pool, (in_key, out_key) in zip(pools_in_order, token_keys):
            dex_id = pool["dexId"]
            if is_v2_dex(dex_id):
                # Классический UniV2: формула резервов (точна для x*y=k)
                out = await self._quote_v2(pool, in_key, current)
            else:
                # V3 и Algebra: QuoterV2 (Algebra поддерживает тот же интерфейс)
                out = await self._quote_single(pool, in_key, out_key, current)
            if out is None or out == 0:
                return None
            current = out
        return current

    @with_retry()
    async def _quote_single(
        self,
        pool: dict,
        token_in_key: str,
        token_out_key: str,
        amount_in_wei: int,
    ) -> "int | None":
        """Одношаговое котирование — только для выбора лучшего пула."""
        quoter_addr = self.list_quoter.get(pool["dexId"])
        if not quoter_addr:
            return None
        async with self._semaphore:
            quoter = self.web3.eth.contract(
                address=self.web3.to_checksum_address(quoter_addr),
                abi=QUOTE_EXACT_INPUT_ABI,
            )
            result = await quoter.functions.quoteExactInputSingle({
                "tokenIn":           self.web3.to_checksum_address(
                    pool[token_in_key]["address"]),
                "tokenOut":          self.web3.to_checksum_address(
                    pool[token_out_key]["address"]),
                "amountIn":          amount_in_wei,
                "fee":               pool["fee"],
                "sqrtPriceLimitX96": 0,
            }).call()
            out = result[0]
            return out if out > 0 else None

    async def _quote_v2(
        self,
        pool: dict,
        token_in_key: str,
        amount_in_wei: int,
    ) -> "int | None":
        """
        Котировка V2 пула по формуле резервов.
        Не требует внешнего квотера — читаем getReserves() напрямую.
        Комиссия V2: 0.3% (множитель 997/1000).
        PancakeSwap V2: 0.25% (множитель 9975/10000) — аппроксимируем 997/1000.
        """
        try:
            async with self._semaphore:
                contract = self.web3.eth.contract(
                    address=self.web3.to_checksum_address(pool["pool"]),
                    abi=POOL_V2_ABI,
                )
                reserves = await contract.functions.getReserves().call()
                r0, r1 = reserves[0], reserves[1]

                # Определяем какой резерв — вход, какой — выход
                t0_addr = pool["token0"]["address"].lower()
                tin_addr = pool[token_in_key]["address"].lower()

                if tin_addr == t0_addr:
                    r_in, r_out = r0, r1
                else:
                    r_in, r_out = r1, r0

                if r_in == 0 or r_out == 0:
                    return None

                # amountOut = amountIn * 997 * rOut / (rIn * 1000 + amountIn * 997)
                fee_num = amount_in_wei * 997
                amount_out = (fee_num * r_out) // (r_in * 1000 + fee_num)
                return amount_out if amount_out > 0 else None

        except (ContractLogicError, BadFunctionCallOutput, *NETWORK_ERRORS):
            return None
        except Exception:
            return None

    async def _best_single_quote(
        self,
        pools: list,
        in_key: str,
        out_key: str,
        amount_in_wei: int,
    ) -> "tuple[dict | None, int]":
        """
        Лучший пул среди вариантов одной пары (по котировке).
        Автоматически выбирает V2 или V3 котировщик по dexId.
        """
        async def _one(p):
            dex_id = p["dexId"]
            if is_v2_dex(dex_id):
                # Классический UniV2: формула резервов (квотер не нужен)
                out = await self._quote_v2(p, in_key, amount_in_wei)
            else:
                # V3 и Algebra: QuoterV2 (точная on-chain котировка)
                out = await self._quote_single(p, in_key, out_key, amount_in_wei)
            return (p, out) if out else None

        results = await asyncio.gather(*[_one(p) for p in pools])
        valid   = [r for r in results if r is not None]
        if not valid:
            return None, 0
        return max(valid, key=lambda x: x[1])

    # ══════════════════════════════════════════════════════════════
    #  Построение кандидатов
    # ══════════════════════════════════════════════════════════════

    def _build_candidates(self, step_count: int) -> list:
        cum   = self.cumulative_token_name
        pairs = list(self.all_pools.keys())

        if step_count == 2:
            result = [(p,) for p in pairs if cum in p.split("-")]
            logger.debug(f"[2-шаг] Кандидатов до фильтра ликвидности: {len(result)}")
            return result

        adj: dict[str, list[str]] = {}
        for pair in pairs:
            a1, a2 = pair.split("-")
            adj.setdefault(a1, []).append(pair)
            adj.setdefault(a2, []).append(pair)

        result = []
        n = step_count

        def dfs(path: list, cur_token: str, depth: int, used: set):
            if depth == n:
                if cur_token == cum:
                    result.append(tuple(path))
                return
            for edge in adj.get(cur_token, []):
                if edge in used:
                    continue
                a1, a2 = edge.split("-")
                nxt = a2 if a1 == cur_token else a1
                if depth < n - 1 and nxt == cum:
                    continue
                dfs(path + [edge], nxt, depth + 1, used | {edge})

        for start_p in adj.get(cum, []):
            a1, a2 = start_p.split("-")
            mid1 = a2 if a1 == cum else a1
            dfs([start_p], mid1, 1, {start_p})

        logger.debug(f"[{n}-шаг] Кандидатов до фильтра ликвидности: {len(result)}")
        return result

    # ══════════════════════════════════════════════════════════════
    #  Обработка одной цепочки
    # ══════════════════════════════════════════════════════════════

    async def _process_chain(
        self, pair_names: tuple, step_count: int
    ) -> "dict | None":
        """
        1. Проверяет все пулы цепочки по кэшу ликвидности.
        2. Если ликвидность ок — выбирает лучший пул для каждой пары.
        3. Делает атомарное котирование всей цепочки.
        4. Проверяет net_profit >= MIN_PROFIT_PCT.
        """
        # ── Шаг 0: быстрая проверка ликвидности по кэшу ─────────────────────
        # Берём первый пул каждой пары для проверки (потом выберем лучший)
        candidate_pools = []
        for pair_name in pair_names:
            pair_pool_list = self.all_pools.get(pair_name)
            if not pair_pool_list:
                return None
            # Проверяем все варианты пула — если хоть один проходит, ок
            # Пулы с неподдерживаемым dexId (0x-адрес) пропускаем
            best_candidate = None
            for p in pair_pool_list:
                if not _is_supported_dex(p["dexId"]):
                    continue
                ok, liq_usd = self._pool_liq_ok(p["pool"])
                if ok:
                    best_candidate = p
                    break
            if best_candidate is None:
                # Все варианты пары провалили проверку ликвидности
                liq_vals = []
                for p in pair_pool_list:
                    _, lv = self._pool_liq_ok(p["pool"])
                    if lv is not None and lv != float("inf"):
                        liq_vals.append(lv)
                min_liq = self._min_liq_usd()
                best_liq = max(liq_vals) if liq_vals else 0
                logger.debug(
                    f"  ⛔ {pair_name}: ликвидность ${best_liq:,.0f} "
                    f"< мин ${min_liq:,.0f} "
                    f"(impact>{self.max_price_impact_pct:.2f}%) → пропускаем"
                )
                return None
            candidate_pools.append(best_candidate)

        # ── Шаг 1: определяем направление токенов ────────────────────────────
        first_pools = self.all_pools[pair_names[0]]
        aux_key, cum_key = self._init_tokens(first_pools[0])
        if aux_key is None:
            return None

        cum_decimals  = first_pools[0][cum_key]["decimals"]
        amount_in_wei = int(self.amount_human * 10 ** cum_decimals)

        best_pools: list[dict] = []
        token_keys: list[tuple] = []

        # ── Шаг 2: выбираем лучший пул для каждой пары по котировке ──────────
        if step_count == 2:
            best_1, amt1 = await self._best_single_quote(
                first_pools, cum_key, aux_key, amount_in_wei)
            if best_1 is None:
                return None
            best_2, _ = await self._best_single_quote(
                first_pools, aux_key, cum_key, amt1 or amount_in_wei)
            if best_2 is None:
                return None
            best_pools = [best_1, best_2]
            token_keys = [(cum_key, aux_key), (aux_key, cum_key)]

        else:
            best_1, cur = await self._best_single_quote(
                first_pools, cum_key, aux_key, amount_in_wei)
            if best_1 is None:
                return None
            best_pools.append(best_1)
            token_keys.append((cum_key, aux_key))
            prev_addr = first_pools[0][aux_key]["address"]

            for pair_name in pair_names[1:]:
                pools = self.all_pools[pair_name]
                tin, tout = self._tokens_by_addr(pools[0], prev_addr)
                if tin is None:
                    return None
                best_p, cur = await self._best_single_quote(
                    pools, tin, tout, cur)
                if best_p is None:
                    return None
                best_pools.append(best_p)
                token_keys.append((tin, tout))
                prev_addr = pools[0][tout]["address"]

        # ── Шаг 3: финальная проверка ликвидности выбранных пулов ────────────
        # (выбранный лучший пул мог не совпасть с кандидатом из шага 0)
        liq_ok, reject_reason = self._chain_liq_ok(best_pools)
        if not liq_ok:
            logger.debug(f"  ⛔ {' → '.join(pair_names)}: {reject_reason}")
            return None

        # Собираем данные ликвидности для лога
        liq_info = []
        for p in best_pools:
            lv = self._liq_cache.get(p["pool"].lower())
            liq_info.append({
                "pair":    f"{p['token0']['symbol']}/{p['token1']['symbol']}",
                "liq_usd": lv,
            })

        # ── Шаг 4: котирование всей цепочки ──────────────────────────────────
        # Если есть хотя бы один V2 пул — используем пошаговое котирование.
        # Для чистых V3 цепочек — атомарный quoteExactInput (точнее).
        # Пошаговое котирование нужно только если есть классические V2 пулы.
        # Algebra котируется через QuoterV2 (как V3) — можно в атомарном режиме,
        # НО quoteExactInput не поддерживает смешанные пути V3+Algebra в одном path.
        # Поэтому для смешанных цепочек всегда используем пошаговое котирование.
        #
        # ВАЖНО: quoteExactInput котирует ВЕСЬ packed-path через ОДИН quoter,
        # а каждый QuoterV2 ищет пулы только в СВОЕЙ фабрике по (token, fee).
        # Если цепочка пересекает границу DEX (напр. Sushi→Uni→Uni), единый
        # quoter найдёт для чужих хопов не те пулы (или пустые) → котировка
        # уходит в −99%, хотя min_liq в логе берётся из выбранных best_pools.
        # Поэтому атомарную ветку используем ТОЛЬКО когда все пулы цепочки
        # обслуживаются одним и тем же quoter'ом (одна фабрика). Иначе —
        # пошагово, где каждый хоп котируется quoter'ом своего DEX.
        has_v2_classic = any(is_v2_dex(p["dexId"]) for p in best_pools)
        has_algebra    = any(is_algebra_dex(p["dexId"]) for p in best_pools)

        # Уникальные quoter-адреса по всем пулам цепочки.
        # None означает, что для dexId нет quoter'а (классический V2) —
        # такой пул котируется через getReserves, а не атомарно.
        quoter_addrs = {self.list_quoter.get(p["dexId"]) for p in best_pools}
        single_quoter = len(quoter_addrs) == 1 and None not in quoter_addrs

        has_mixed = has_v2_classic or has_algebra or not single_quoter

        if has_mixed:
            # Смешанная / разнодексовая цепочка: шаг за шагом.
            # Каждый хоп котируется quoter'ом своего DEX (_quote_single) либо
            # по резервам (_quote_v2) — теми же пулами, что в best_pools.
            amount_out_wei = await self._quote_chain_mixed(
                best_pools, token_keys, amount_in_wei)
        else:
            # Все пулы на одной фабрике: атомарный quoteExactInput (точнее,
            # 1 RPC-вызов). dexId у всех одинаковый — берём первый.
            dex_id_first   = best_pools[0]["dexId"]
            amount_out_wei = await self._quote_chain_atomic(
                best_pools, token_keys, amount_in_wei, dex_id_first)

        if amount_out_wei is None:
            return None

        # ── Шаг 5: строим token_path ──────────────────────────────────────────
        token_path: list[str] = []
        for idx, (pool, (in_k, out_k)) in enumerate(zip(best_pools, token_keys)):
            if idx == 0:
                token_path.append(pool[in_k]["address"])
            token_path.append(pool[out_k]["address"])

        # ── Шаг 6: расчёт прибыли ─────────────────────────────────────────────
        amount_out_human = amount_out_wei / 10 ** cum_decimals
        gross_profit     = amount_out_human - self.amount_human
        net_profit       = gross_profit - self.gas_cost_usd
        profit_pct       = net_profit / self.amount_human * 100
        chain_label      = " → ".join(pair_names)
        sign             = "+" if net_profit >= 0 else ""
        def _pool_label(pool_info: dict) -> str:
            dex = pool_info["dexId"]
            base = dex.split("_v2")[0] if "_v2" in dex else dex
            name = base[:4].capitalize()
            if is_algebra_dex(dex):
                ver = "Alg"  # Algebra/Camelot
            elif is_v2_dex(dex):
                ver = "V2"
            else:
                ver = "V3"
            return f"{name}{ver}"
        dex_route = "→".join(_pool_label(bp) for bp in best_pools)

        # Лог ликвидности самого слабого пула
        valid_liqs = [
            li["liq_usd"] for li in liq_info
            if li["liq_usd"] is not None and li["liq_usd"] != float("inf")
        ]
        min_pool_liq = min(valid_liqs) if valid_liqs else None
        liq_str = f"min_liq=${min_pool_liq:,.0f}" if min_pool_liq else "liq=?"

        logger.info(
            f"[{step_count}шаг|{dex_route}] {chain_label}: "
            f"in={self.amount_human:.2f} "
            f"gross={gross_profit:+.6f} "
            f"net={sign}{net_profit:.6f} ({sign}{profit_pct:.4f}%) "
            f"{liq_str}"
        )

        if profit_pct < self.min_profit_pct:
            return None

        amount_out_min = int(amount_out_wei * (10_000 - self.slippage_bps) / 10_000)
        pools_addresses = [p["pool"] for p in best_pools]
        dex_types_list  = [dex_type_for(p["dexId"]) for p in best_pools]

        logger.success(
            f"  💰 [{step_count}шаг|{dex_route}] {chain_label}: "
            f"net={sign}{net_profit:.4f}$ ({sign}{profit_pct:.4f}%) "
            f"{liq_str} → исполняю..."
        )

        return {
            "step_count":          step_count,
            "chain_label":         chain_label,
            "dex_route":           dex_route,
            "pools_addresses":     pools_addresses,
            "dex_types":           dex_types_list,
            "token_path":          token_path,
            "amount_in_wei":       amount_in_wei,
            "amount_out_min":      amount_out_min,
            "profit":              net_profit,
            "profit_pct":          profit_pct,
            "cumulative_decimals": cum_decimals,
            "best_pools":          best_pools,
            "liq_info":            liq_info,
        }

    # ══════════════════════════════════════════════════════════════
    #  Запуск
    # ══════════════════════════════════════════════════════════════

    async def run(self) -> None:
        if not await self.web3.is_connected():
            raise ConnectionError("Нет соединения с Web3. Проверьте ARBITRUM_RPC_URL.")

        # Обновляем кэш ликвидности один раз в начале цикла
        await self.refresh_liquidity_cache()

        for step_count in self.active_steps:
            candidates = self._build_candidates(step_count)
            if not candidates:
                logger.info(f"[{step_count}-шаг] Кандидатов нет")
                continue

            total = len(candidates)
            min_liq = self._min_liq_usd()
            logger.info(
                f"[{step_count}-шаг] {total} цепочек | "
                f"фильтр: impact≤{self.max_price_impact_pct:.2f}% "
                f"→ мин.ликвидность ${min_liq:,.0f}"
            )

            for batch_start in range(0, total, BATCH_SIZE):
                batch = candidates[batch_start:batch_start + BATCH_SIZE]

                results = await asyncio.gather(
                    *[self._process_chain(c, step_count) for c in batch],
                    return_exceptions=True,
                )

                for r in results:
                    if isinstance(r, dict):
                        await self.execute_fn(r)

    async def close(self) -> None:
        if self._own_web3:
            await _close_web3_session(self.web3)


# ══════════════════════════════════════════════════════════════
#  Утилиты закрытия сессий
# ══════════════════════════════════════════════════════════════

async def _close_web3_session(w3: AsyncWeb3) -> None:
    try:
        provider = w3.provider
        for attr in ("_session", "session", "_request_session"):
            sess = getattr(provider, attr, None)
            if sess is not None and hasattr(sess, "closed") and not sess.closed:
                await sess.close()
                await asyncio.sleep(0.1)
                return
        kwargs = getattr(provider, "_request_kwargs", None)
        if isinstance(kwargs, dict):
            sess = kwargs.get("session")
            if sess and hasattr(sess, "closed") and not sess.closed:
                await sess.close()
                await asyncio.sleep(0.1)
    except (AttributeError, RuntimeError):
        pass


async def close_all_sessions() -> None:
    import gc
    closed = 0
    for obj in gc.get_objects():
        try:
            if isinstance(obj, aiohttp.ClientSession) and not obj.closed:
                await obj.close()
                closed += 1
        except (AttributeError, RuntimeError):
            pass
    if closed:
        logger.debug(f"🔌 Закрыто aiohttp сессий: {closed}")
    await asyncio.sleep(0.25)
