import asyncio
import json
import os
import time
from typing import Any

import aiohttp
from dotenv import load_dotenv
from loguru import logger
from web3 import AsyncWeb3
from web3.exceptions import ContractLogicError, BadFunctionCallOutput
from web3.providers import AsyncHTTPProvider

load_dotenv()

# Минимальные резервы (в человеческих единицах, НЕ в wei)
MIN_RESERVES = {
    "stablecoins": 1_000,
    "WETH": 0.5,
    "WBTC": 0.03,
    "default": 10_000,
}

STABLECOINS = frozenset([
    "USDC", "USD₮0", "USDC.e", "xUSD", "USDe", "axlUSDC", "MIM", "USD24",
    "USDs", "FRAX", "alUSD", "FUSD", "fUSDC", "GHO", "satUSD", "DAI",
    "USDai", "USDS", "MAI", "DOLA", "USDT", "USD.a", "EUROS", "USDY",
    "UKSDT", "USDRIF", "USDV", "gmUSD", "BOB", "USDT+", "DAI+", "USD0++",
    "FJPY", "FEUR", "EURS", "FSGD", "BUCK", "rgUSD", "USDW",
])

# Сетевые исключения, при которых делаем повторный запрос
NETWORK_ERRORS = (
    ConnectionError,
    ConnectionResetError,
    ConnectionRefusedError,
    TimeoutError,
    OSError,
)

MAX_RETRIES    = 3
RETRY_DELAY    = 2.0
MAX_CONCURRENCY = 5


async def retry_call(coro_func, *args, retries=MAX_RETRIES, delay=RETRY_DELAY, label="", **kwargs):
    """
    Универсальная обёртка с повторными попытками для сетевых ошибок.
    При ошибке 429 делает увеличенную паузу.

    Возвращает None при:
      • ContractLogicError — контракт ответил revert (функция не существует,
                              нет доступа, неверные аргументы)
      • BadFunctionCallOutput — не удалось декодировать ответ контракта
                              (нестандартный ABI, другой формат возврата)
      • Исчерпаны все попытки сетевых ошибок

    Пробрасывает исключение при:
      • Прочих HTTP ошибках (не 429)
    """
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return await coro_func(*args, **kwargs)
        except (ContractLogicError, BadFunctionCallOutput) as e:
            # Контракт ответил revert или вернул нераспознанный формат —
            # это не сетевая ошибка, повторять бессмысленно
            logger.debug(f"[{label}] Контракт вернул ошибку: {type(e).__name__}: {e}")
            return None
        except aiohttp.ClientResponseError as e:
            last_error = e
            if e.status == 429:
                wait = delay * attempt * 3
                logger.warning(
                    f"[{label}] 429 Too Many Requests (попытка {attempt}/{retries}), "
                    f"ждём {wait:.1f}с..."
                )
                if attempt < retries:
                    await asyncio.sleep(wait)
            else:
                raise
        except NETWORK_ERRORS as e:
            last_error = e
            logger.warning(
                f"[{label}] Сетевая ошибка (попытка {attempt}/{retries}): "
                f"{type(e).__name__}: {e}"
            )
            if attempt < retries:
                await asyncio.sleep(delay * attempt)
        except Exception:
            raise
    logger.error(f"[{label}] Все {retries} попытки исчерпаны. Последняя ошибка: {last_error}")
    return None


class PoolsCheck:

    def __init__(self):
        rpc_url = os.getenv("ARBITRUM_RPC_URL")
        self.web3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))

        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.erc20_abi  = []
        self.pool_v3_abi   = []     # V3 ABI (fee, slot0, token0, token1, ...)
        self.pool_v2_abi = []    # V2 ABI (getReserves, token0, token1)
        self.tokens:    dict = {}
        self.pools:     dict = {}
        self.all_pools: dict = {}
        self._lock = asyncio.Lock()

    # ──────────────────────────────────────────────
    # Загрузка файлов
    # ──────────────────────────────────────────────

    def _load_json(self, *path_parts) -> dict | list:
        path = os.path.join(self.script_dir, *path_parts)
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def uploading_data(self) -> bool:
        try:
            self.tokens      = self._load_json( "pools", "tokens.json")
            logger.debug("Файл tokens.json загружен")
            self.erc20_abi   = self._load_json("abi", "erc20.json")
            logger.debug("Файл erc20.json загружен")
            self.pool_v3_abi    = self._load_json("abi", "pool_abi_v3.json")
            logger.debug("Файл pool_abi_v3.json загружен (V3)")
            self.pools       = self._load_json( "pools", "pools_dexscreener.json")
            logger.debug("Файл pools.json загружен")

            # V2 ABI загружаем если файл есть, иначе используем встроенный
            v2_abi_path = os.path.join(self.script_dir, "abi", "pool_abi_v2.json")
            if os.path.exists(v2_abi_path):
                self.pool_v2_abi = self._load_json("abi", "pool_abi_v2.json")
                logger.debug("Файл pool_abi_v2.json загружен (V2)")
            else:
                self.pool_v2_abi = self._builtin_v2_abi()
                logger.debug("Используется встроенный V2 ABI (pool_abi_v2.json не найден)")

            return True
        except FileNotFoundError as e:
            logger.error(f"Отсутствует один из файлов для загрузки: {e}")
            return False

    @staticmethod
    def _builtin_v2_abi() -> list:
        """
        Встроенный минимальный ABI для Uniswap V2-совместимых пулов.
        Используется если файл abi/pool_v2.json отсутствует.
        """
        return [
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
            {
                "inputs": [],
                "name": "getReserves",
                "outputs": [
                    {"internalType": "uint112", "name": "reserve0", "type": "uint112"},
                    {"internalType": "uint112", "name": "reserve1", "type": "uint112"},
                    {"internalType": "uint32",  "name": "blockTimestampLast", "type": "uint32"},
                ],
                "stateMutability": "view",
                "type": "function",
            },
        ]

    # ──────────────────────────────────────────────
    # Добавление пула (thread-safe)
    # ──────────────────────────────────────────────

    async def add_pool(self, data: dict):
        pool = {
            "dexId": data["dex_id"],
            "pool":  data["pool"],
            "fee":   data["fee"],
            "token0": {
                "address":  data["token0_address"],
                "symbol":   data["token0_symbol"],
                "decimals": data["token0_decimals"],
            },
            "token1": {
                "address":  data["token1_address"],
                "symbol":   data["token1_symbol"],
                "decimals": data["token1_decimals"],
            },
        }
        async with self._lock:
            key   = data["key_pairs"]
            value = self.all_pools.get(key, [])
            if pool not in value:
                value.append(pool)
                self.all_pools[key] = value

    # ──────────────────────────────────────────────
    # Основной цикл
    # ──────────────────────────────────────────────

    async def start(self):
        start_time = time.time()
        if not self.uploading_data():
            return

        connected = await retry_call(self.web3.is_connected, label="Web3.is_connected")
        if not connected:
            logger.error("❌ Нет соединения с Web3. Проверьте ARBITRUM_RPC_URL.")
            return

        semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

        tasks = []
        for dex_id, pool_list in self.pools.items():
            logger.info("Добавляю задачи для биржи {dex_id}", dex_id=dex_id)
            for pool in pool_list:
                tasks.append(self._process_pool(semaphore, dex_id, pool))

        await asyncio.gather(*tasks)

        if self.all_pools:
            self.save_results()
        else:
            logger.warning("Файл all_pools.json не создан — нет валидных пулов")

        elapsed = time.time() - start_time
        logger.info(f"Проверка завершена за {elapsed:.2f} секунд")

    async def _process_pool(self, semaphore: asyncio.Semaphore, dex_id: str, pool: str):
        """
        Проверяет один пул. Алгоритм:
          1. Проверяем существование контракта (bytecode != 0x)
          2. Пробуем fee()   → успешно = V3, передаём в pool_type_definition
          3. Пробуем getReserves() → успешно = V2, передаём в pool_type_definition_v2
          4. Ни то ни другое → пул нестандартный (Balancer, Curve, DODO...) → пропускаем

        Такой двухэтапный подход исключает ошибки на нестандартных пулах,
        которые DexScreener тоже агрегирует, но они не совместимы с V2/V3 API.
        """
        async with semaphore:
            logger.info("Проверяю пул: {pool}", pool=pool)

            pool_checksum = await self.checking_existence_contract(pool)
            if not pool_checksum:
                return

            # Шаг 1: пробуем V3 (fee)
            if await self._is_v3_pool(pool_checksum):
                await self.pool_type_definition(dex_id, pool_checksum)
                return

            # Шаг 2: пробуем V2 (getReserves)
            if await self._is_v2_pool(pool_checksum):
                v2_dex_id = f"{dex_id}_v2"
                await self.pool_type_definition_v2(v2_dex_id, pool_checksum)
                return

            # Шаг 3: нестандартный пул (Balancer, Curve, DODO и т.д.)
            logger.debug(
                f"Пул {pool_checksum} не является V3 или V2-совместимым — пропускаем"
            )

    # ──────────────────────────────────────────────
    # Определение версии пула
    # ──────────────────────────────────────────────

    async def _is_v3_pool(self, pool: str) -> bool:
        """
        True если пул V3 — успешно возвращает fee().
        Нет fee() → False (V2 или нестандартный).
        """
        try:
            pool_cs       = self.web3.to_checksum_address(pool)
            pool_contract = self.web3.eth.contract(address=pool_cs, abi=self.pool_v3_abi)

            async def _fee():
                return await pool_contract.functions.fee().call()

            result = await asyncio.wait_for(_fee(), timeout=5.0)
            return result is not None

        except (ContractLogicError, BadFunctionCallOutput,
                asyncio.TimeoutError, *NETWORK_ERRORS):
            return False

    async def _is_v2_pool(self, pool: str) -> bool:
        """
        True если пул V2-совместимый — успешно возвращает getReserves().
        Проверяем оба формата: стандартный UniV2 (3 поля) и Camelot/Algebra (2 поля).
        Если ни один не работает → False (нестандартный пул: Balancer, Curve и т.д.).
        """
        result = await self._get_reserves_v2(self.web3.to_checksum_address(pool))
        return result is not None

    # ──────────────────────────────────────────────
    # Проверка существования контракта
    # ──────────────────────────────────────────────

    async def checking_existence_contract(self, pool: str):
        try:
            pool_checksum = self.web3.to_checksum_address(pool)

            async def _get_code():
                return await self.web3.eth.get_code(pool_checksum)

            pool_code = await retry_call(_get_code, label=f"get_code:{pool}")

            if pool_code is None:
                return None
            if pool_code in (b"\x00", b"", b"0x", b"0x0"):
                logger.info("Пул {pool} не существует", pool=pool)
                return None
            return pool_checksum

        except ValueError:
            logger.error("Неверный формат адреса пула: {pool}", pool=pool)
            return None
        except Exception as error:
            logger.exception("Неопознанная ошибка при проверке контракта {pool}", pool=pool)
            print(error)
            return None

    # ──────────────────────────────────────────────
    # Проверка V3 пула (оригинальная логика)
    # ──────────────────────────────────────────────

    async def pool_type_definition(self, dex_id: str, pool: str):
        """
        Полная проверка V3 пула:
          1. Читаем token0, token1
          2. Проверяем ликвидность через balanceOf(pool)
          3. Читаем fee
          4. Проверяем котировку через QuoterV2
          5. Добавляем в all_pools с dexId как есть (uniswap/sushiswap/pancakeswap)
        """
        try:
            pool_cs       = self.web3.to_checksum_address(pool)
            pool_contract = self.web3.eth.contract(address=pool_cs, abi=self.pool_v3_abi)

            async def _get_token0():
                return await pool_contract.functions.token0().call()

            async def _get_token1():
                return await pool_contract.functions.token1().call()

            token0_raw = await retry_call(_get_token0, label=f"token0:{pool}")
            token1_raw = await retry_call(_get_token1, label=f"token1:{pool}")

            if token0_raw is None or token1_raw is None:
                return

            token0    = token0_raw.lower()
            token1    = token1_raw.lower()
            token0_cs = self.web3.to_checksum_address(token0)
            token1_cs = self.web3.to_checksum_address(token1)

            token0_symbol, token0_decimals, token0_balance = await self.get_token_info(token0_cs, pool)
            if None in (token0_symbol, token0_decimals, token0_balance):
                return
            if not self.check_reserve(token0_symbol, token0_decimals, token0_balance):
                return

            token1_symbol, token1_decimals, token1_balance = await self.get_token_info(token1_cs, pool)
            if None in (token1_symbol, token1_decimals, token1_balance):
                return
            if not self.check_reserve(token1_symbol, token1_decimals, token1_balance):
                return

            async def _get_fee():
                return await pool_contract.functions.fee().call()

            fee = await retry_call(_get_fee, label=f"fee:{pool}")
            if fee is None:
                return

            key_pairs = self.key_creation(token0, token1)
            if key_pairs is None:
                return

            await self.add_pool({
                "key_pairs":        key_pairs,
                "dex_id":           dex_id,
                "pool":             pool,
                "fee":              fee,
                "token0_address":   token0,
                "token0_symbol":    token0_symbol,
                "token0_decimals":  token0_decimals,
                "token1_address":   token1,
                "token1_symbol":    token1_symbol,
                "token1_decimals":  token1_decimals,
            })

            logger.success(f"[V3] Пул добавлен: {key_pairs}  dex={dex_id}  fee={fee}")

        except ContractLogicError:
            logger.error("Ошибка (ContractLogicError) проверки V3 пула: {pool}", pool=pool)
        except Exception as error:
            logger.exception("Неопознанная ошибка V3 {pool}", pool=pool)
            print(error)

    # ──────────────────────────────────────────────
    # Чтение резервов V2 (с поддержкой нестандартных форматов)
    # ──────────────────────────────────────────────

    async def _get_reserves_v2(self, pool_cs: str) -> "tuple[int, int] | None":
        """
        Читает резервы V2-совместимого пула.

        Поддерживает два формата getReserves():
          • Стандартный UniV2 / SushiV2 / PancakeV2:
              → (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast)
          • Camelot V2 / Algebra Protocol:
              → (uint128 reserve0, uint128 reserve1)  — без timestamp

        Перехватывает только ошибки контракта (revert, неверный ABI).
        Сетевые ошибки возвращают None.

        Возвращает (reserve0, reserve1) или None если оба формата провалились.
        """
        cs = self.web3.to_checksum_address(pool_cs)

        # Попытка 1: стандартный UniV2 (3 поля: reserve0, reserve1, blockTimestampLast)
        abi_3: Any = [{"inputs": [], "name": "getReserves", "outputs": [
            {"name": "reserve0",          "type": "uint112"},
            {"name": "reserve1",          "type": "uint112"},
            {"name": "blockTimestampLast","type": "uint32"},
        ], "stateMutability": "view", "type": "function"}]

        try:
            r = await self.web3.eth.contract(address=cs, abi=abi_3).functions.getReserves().call()
            return int(r[0]), int(r[1])
        except (ContractLogicError, BadFunctionCallOutput):
            pass  # Пробуем второй формат
        except (*NETWORK_ERRORS, Exception):
            return None  # Сетевая или неизвестная ошибка

        # Попытка 2: Camelot / Algebra (2 поля: reserve0, reserve1 без timestamp)
        abi_2: Any = [{"inputs": [], "name": "getReserves", "outputs": [
            {"name": "reserve0", "type": "uint128"},
            {"name": "reserve1", "type": "uint128"},
        ], "stateMutability": "view", "type": "function"}]

        try:
            r = await self.web3.eth.contract(address=cs, abi=abi_2).functions.getReserves().call()
            return int(r[0]), int(r[1])
        except (ContractLogicError, BadFunctionCallOutput):
            pass  # Оба формата не подошли — не V2 пул
        except (*NETWORK_ERRORS, Exception):
            return None

        return None

    # ──────────────────────────────────────────────
    # Проверка V2 пула (новая логика)
    # ──────────────────────────────────────────────

    async def pool_type_definition_v2(self, dex_id: str, pool: str):
        """
        Полная проверка V2 пула (Uniswap V2-совместимый):

          1. Читаем token0, token1 (API совпадает с V3)
          2. Читаем getReserves() — source of truth для V2 ликвидности
          3. Проверяем резервы через check_reserve_v2() по данным getReserves
          4. fee = 0 (V2 комиссия 0.3% зашита в контракт, не хранится on-chain)
          5. dexId уже содержит суффикс "_v2" (uniswap_v2 / sushiswap_v2 / ...)

        Отличия от V3:
          - Нет fee() → fee = 0
          - Нет QuoterV2 → проверяем ликвидность напрямую через getReserves()
          - Ликвидность = резервы пула (reserve0, reserve1)
        """
        try:
            pool_cs       = self.web3.to_checksum_address(pool)
            pool_contract = self.web3.eth.contract(address=pool_cs, abi=self.pool_v2_abi)

            # ── Токены ────────────────────────────────────────────────────
            async def _get_token0():
                return await pool_contract.functions.token0().call()

            async def _get_token1():
                return await pool_contract.functions.token1().call()

            token0_raw = await retry_call(_get_token0, label=f"v2:token0:{pool}")
            token1_raw = await retry_call(_get_token1, label=f"v2:token1:{pool}")

            if token0_raw is None or token1_raw is None:
                logger.debug(f"[V2] Не удалось прочитать токены пула {pool}")
                return

            token0    = token0_raw.lower()
            token1    = token1_raw.lower()
            token0_cs = self.web3.to_checksum_address(token0)
            token1_cs = self.web3.to_checksum_address(token1)

            # ── Резервы (с поддержкой UniV2 и Camelot/Algebra) ───────────
            reserves = await self._get_reserves_v2(pool_cs)
            if reserves is None:
                logger.debug(f"[V2] getReserves() не поддерживается пулом {pool}")
                return

            reserve0, reserve1 = reserves

            if reserve0 == 0 or reserve1 == 0:
                logger.debug(f"[V2] Пул {pool} пустой (reserve0={reserve0}, reserve1={reserve1})")
                return

            # ── Информация о токенах (symbol, decimals) ───────────────────
            # balanceOf не нужен для проверки резервов в V2 — используем getReserves
            token0_symbol, token0_decimals = await self.get_token_info_v2(token0_cs)
            if None in (token0_symbol, token0_decimals):
                return

            token1_symbol, token1_decimals = await self.get_token_info_v2(token1_cs)
            if None in (token1_symbol, token1_decimals):
                return

            # ── Проверка ликвидности по резервам ──────────────────────────
            if not self.check_reserve(token0_symbol, token0_decimals, reserve0):
                logger.debug(
                    f"[V2] {token0_symbol} резерв мал: "
                    f"{reserve0 / 10**token0_decimals:.4f} < MIN"
                )
                return

            if not self.check_reserve(token1_symbol, token1_decimals, reserve1):
                logger.debug(
                    f"[V2] {token1_symbol} резерв мал: "
                    f"{reserve1 / 10**token1_decimals:.4f} < MIN"
                )
                return

            # ── Ключ пары ─────────────────────────────────────────────────
            key_pairs = self.key_creation(token0, token1)
            if key_pairs is None:
                logger.debug(
                    f"[V2] Токены не найдены в tokens.json: "
                    f"{token0_symbol}/{token1_symbol}"
                )
                return

            # ── Добавляем пул с fee=0 и dexId с суффиксом _v2 ───────────
            await self.add_pool({
                "key_pairs":        key_pairs,
                "dex_id":           dex_id,   # уже "uniswap_v2" / "sushiswap_v2" / ...
                "pool":             pool,
                "fee":              0,         # V2 комиссия фиксирована 0.3%, не on-chain
                "token0_address":   token0,
                "token0_symbol":    token0_symbol,
                "token0_decimals":  token0_decimals,
                "token1_address":   token1,
                "token1_symbol":    token1_symbol,
                "token1_decimals":  token1_decimals,
            })

            logger.success(
                f"[V2] Пул добавлен: {key_pairs}  dex={dex_id}  "
                f"r0={reserve0/10**token0_decimals:.4f}{token0_symbol}  "
                f"r1={reserve1/10**token1_decimals:.4f}{token1_symbol}"
            )

        except Exception as error:
            # ContractLogicError и BadFunctionCallOutput перехватываются в retry_call
            # и возвращают None — сюда они не доходят.
            # Этот блок ловит только непредвиденные ошибки.
            logger.warning(
                f"[V2] Пул {pool} пропущен: {type(error).__name__}: {error}"
            )

    # ──────────────────────────────────────────────
    # Информация о токене (V3 — с balanceOf пула)
    # ──────────────────────────────────────────────

    async def get_token_info(self, token_address: str, pool_address: str):
        """
        Возвращает (symbol, decimals, balanceOf(pool)).
        Используется для V3 — балансы пула = ликвидность.
        """
        try:
            token_cs = self.web3.to_checksum_address(token_address)
            pool_cs  = self.web3.to_checksum_address(pool_address)
            contract = self.web3.eth.contract(address=token_cs, abi=self.erc20_abi)

            async def _symbol():
                return await contract.functions.symbol().call()

            async def _decimals():
                return await contract.functions.decimals().call()

            async def _balance():
                return await contract.functions.balanceOf(pool_cs).call()

            symbol   = await retry_call(_symbol,   label=f"symbol:{token_address}")
            decimals = await retry_call(_decimals, label=f"decimals:{token_address}")
            balance  = await retry_call(_balance,  label=f"balanceOf:{token_address}")

            if None in (symbol, decimals, balance):
                return None, None, None

            return symbol, decimals, balance

        except Exception as error:
            logger.exception("Неопознанная ошибка get_token_info {token}", token=token_address)
            print(error)
            return None, None, None

    # ──────────────────────────────────────────────
    # Информация о токене (V2 — только symbol и decimals)
    # ──────────────────────────────────────────────

    async def get_token_info_v2(self, token_address: str):
        """
        Для V2 пулов ликвидность берётся из getReserves(), а не balanceOf.
        Поэтому читаем только symbol и decimals.
        Возвращает (symbol, decimals).
        """
        try:
            token_cs = self.web3.to_checksum_address(token_address)
            contract = self.web3.eth.contract(address=token_cs, abi=self.erc20_abi)

            async def _symbol():
                return await contract.functions.symbol().call()

            async def _decimals():
                return await contract.functions.decimals().call()

            symbol   = await retry_call(_symbol,   label=f"v2:symbol:{token_address}")
            decimals = await retry_call(_decimals, label=f"v2:decimals:{token_address}")

            if None in (symbol, decimals):
                return None, None

            return symbol, decimals

        except Exception as error:
            logger.exception("Неопознанная ошибка get_token_info_v2 {token}", token=token_address)
            print(error)
            return None, None

    # ──────────────────────────────────────────────
    # Проверка ликвидности
    # ──────────────────────────────────────────────

    @staticmethod
    def check_reserve(symbol: str, decimals: int, raw_balance: int) -> bool:
        """
        Проверяет что raw_balance (в wei/units) удовлетворяет MIN_RESERVES.
        Работает одинаково для V3 (balanceOf) и V2 (reserve из getReserves).
        """
        human_balance = raw_balance / (10 ** decimals)
        if symbol in STABLECOINS:
            return human_balance >= MIN_RESERVES["stablecoins"]
        elif symbol == "WETH":
            return human_balance >= MIN_RESERVES["WETH"]
        elif symbol == "WBTC":
            return human_balance >= MIN_RESERVES["WBTC"]
        else:
            return human_balance >= MIN_RESERVES["default"]

    # ──────────────────────────────────────────────
    # Создание ключа пары
    # ──────────────────────────────────────────────

    def key_creation(self, token0: str, token1: str):
        token0_symbol = ""
        token1_symbol = ""
        for key, token in self.tokens.items():
            if token == token0:
                token0_symbol = key
            elif token == token1:
                token1_symbol = key
        if not token0_symbol or not token1_symbol:
            return None
        return f"{token0_symbol}-{token1_symbol}"

    # ──────────────────────────────────────────────
    # Сохранение результатов
    # ──────────────────────────────────────────────

    def save_results(self):
        output_dir = os.path.join(self.script_dir, "pools")
        os.makedirs(output_dir, exist_ok=True)
        pools_path = os.path.join(output_dir, "all_pools.json")
        with open(pools_path, "w", encoding="utf-8") as f:
            json.dump(self.all_pools, f, ensure_ascii=False, indent=4)
        logger.info("Файл all_pools.json успешно создан")


if __name__ == "__main__":
    check = PoolsCheck()
    asyncio.run(check.start())
