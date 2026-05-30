"""
╔══════════════════════════════════════════════════════════╗
║              Arbitrage Bot  —  Arbitrum DEX              ║
║   V3: Uniswap/SushiSwap/PancakeSwap/Ramses CL            ║
║   V2: Uniswap/SushiSwap/PancakeSwap/Camelot и др.        ║
║   Контракт: MultiHopSwapUniversal                        ║
╚══════════════════════════════════════════════════════════╝

Архитектура:
  • Поиск    = on-chain QuoterV2 (реальные цены)
  • Поиск 2/3/4/5 шагов — за один проход
  • Исполнение = MultiHopSwapUniversal.exactInput
      pools[]    — адреса пулов (из поля "pool" в all_pools.json)
      dexTypes[] — uint8[]: 0=Uniswap/Sushi, 1=PancakeSwap
      tokens[]   — маршрут токенов (len = pools+1)
  • approve выдаётся один раз на uint256.max
  • Батчи по BATCH_SIZE, асинхронность
"""

import asyncio
import importlib
import os
import sys
import time

from dotenv import load_dotenv
from loguru import logger
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

load_dotenv()

# ─── Логгер ───────────────────────────────────────────────────────────────────
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
    colorize=True,
    level="DEBUG",
)
os.makedirs("logs", exist_ok=True)
logger.add("logs/bot.log", rotation="10 MB", retention="7 days",
           encoding="utf-8", level="DEBUG")

CYCLE_SLEEP_SECONDS = int(os.getenv("CYCLE_SLEEP", "30"))
DEFAULT_GAS_LIMIT   = int(os.getenv("GAS_LIMIT", "800000"))

# ══════════════════════════════════════════════════════════════════════════════
#  ABI MultiHopSwapUniversal
#  exactInput принимает tuple с полем dexTypes (uint8[])
# ══════════════════════════════════════════════════════════════════════════════

MULTIHOP_ABI = [
    # exactInput(ExactInputParams)
    {
        "name": "exactInput",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {
                "name": "p",
                "type": "tuple",
                "components": [
                    {"name": "pools",            "type": "address[]"},
                    {"name": "dexTypes",         "type": "uint8[]"},
                    # dexTypes[i]:
                    #   0 = Uniswap V3 / SushiSwap V3 / Ramses CL  (uniswapV3SwapCallback)
                    #   1 = PancakeSwap V3                          (pancakeV3SwapCallback)
                    #   2 = Uniswap V2 (классический)               (нет callback)
                    #   3 = SushiSwap V2                            (нет callback)
                    #   4 = PancakeSwap V2                          (нет callback)
                    #   5 = Camelot / Algebra                       (algebraSwapCallback!)
                    {"name": "tokens",           "type": "address[]"},
                    {"name": "amountIn",         "type": "uint256"},
                    {"name": "amountOutMinimum", "type": "uint256"},
                    {"name": "recipient",        "type": "address"},
                    {"name": "deadline",         "type": "uint256"},
                ],
            }
        ],
        "outputs": [{"name": "amountOut", "type": "uint256"}],
    },
    # getPoolTokens(address) view
    {
        "name": "getPoolTokens",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "pool", "type": "address"}],
        "outputs": [
            {"name": "token0", "type": "address"},
            {"name": "token1", "type": "address"},
        ],
    },
    # tokenBalance(address) view
    {
        "name": "tokenBalance",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "token", "type": "address"}],
        "outputs": [{"name": "", "type": "uint256"}],
    },
]

ERC20_ABI = [
    {
        "name": "balanceOf",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "account", "type": "address"}],
        "outputs": [{"name": "", "type": "uint256"}],
    },
    {
        "name": "allowance",
        "type": "function",
        "stateMutability": "view",
        "inputs": [
            {"name": "owner",   "type": "address"},
            {"name": "spender", "type": "address"},
        ],
        "outputs": [{"name": "", "type": "uint256"}],
    },
    {
        "name": "approve",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "amount",  "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "bool"}],
    },
    {
        "name": "symbol",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [{"name": "", "type": "string"}],
    },
    {
        "name": "decimals",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [{"name": "", "type": "uint8"}],
    },
]

# ══════════════════════════════════════════════════════════════════════════════
#  Раздел 1: Поиск и проверка пулов
# ══════════════════════════════════════════════════════════════════════════════

def _get_pool_dir() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "pool_collection")


def run_search_pools() -> None:
    pool_dir = _get_pool_dir()
    sys.path.insert(0, pool_dir)
    try:
        parser = importlib.import_module("parser_dexscreen_api")
        importlib.reload(parser)
        logger.info("═" * 55)
        logger.info("  Запуск поиска пулов...")
        logger.info("═" * 55)
        parser.DexscreenerParser().start()
        logger.success("Поиск завершён → pool_collection/pools/pools_dexscreener.json")
    except ImportError as e:
        logger.error(f"Не найден модуль parser_dexscreen_api: {e}")
    except Exception:
        logger.exception("Ошибка при поиске пулов")
    finally:
        sys.path.pop(0)


def run_pools_check() -> None:
    pool_dir = _get_pool_dir()
    sys.path.insert(0, pool_dir)
    try:
        pools_mod = importlib.import_module("pools_check")
        importlib.reload(pools_mod)
        logger.info("═" * 55)
        logger.info("  Запуск проверки пулов...")
        logger.info("═" * 55)
        asyncio.run(pools_mod.PoolsCheck().start())
        logger.success("Проверка завершена → pool_collection/pools/all_pools.json")
    except ImportError as e:
        logger.error(f"Не найден модуль pools_check: {e}")
    except Exception:
        logger.exception("Ошибка при проверке пулов")
    finally:
        sys.path.pop(0)


# ══════════════════════════════════════════════════════════════════════════════
#  Раздел 2: Газ (EIP-1559)
# ══════════════════════════════════════════════════════════════════════════════

async def _get_gas_params(web3: AsyncWeb3) -> dict:
    """maxFeePerGas = baseFee × 2 + priority."""
    try:
        latest   = await web3.eth.get_block("latest")
        base_fee = latest.get("baseFeePerGas", 0)
        priority = web3.to_wei(float(os.getenv("MAX_PRIORITY_FEE_GWEI", "0.01")), "gwei")
        max_fee  = int(base_fee * 2) + priority
    except (ValueError, KeyError, TypeError):
        priority = web3.to_wei(float(os.getenv("MAX_PRIORITY_FEE_GWEI", "0.01")), "gwei")
        max_fee  = web3.to_wei(float(os.getenv("MAX_FEE_PER_GAS_GWEI", "0.1")), "gwei")
    return {
        "maxFeePerGas":         max_fee,
        "maxPriorityFeePerGas": priority,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  Раздел 3: Approve (один раз на uint256.max)
# ══════════════════════════════════════════════════════════════════════════════

_approve_cache: dict[str, bool] = {}
MAX_UINT256 = 2 ** 256 - 1


async def _ensure_approve(
    web3: AsyncWeb3,
    token_addr: str,
    owner: str,
    spender: str,
    amount_needed: int,
    private_key: str,
) -> bool:
    key = token_addr.lower()
    if _approve_cache.get(key):
        return True

    owner_cs  = web3.to_checksum_address(owner)
    spender_cs = web3.to_checksum_address(spender)
    token     = web3.eth.contract(
        address=web3.to_checksum_address(token_addr), abi=ERC20_ABI)
    allowance = await token.functions.allowance(owner_cs, spender_cs).call()
    if allowance >= amount_needed:
        _approve_cache[key] = True
        return True

    symbol = await token.functions.symbol().call()
    logger.info(f"  🔑 Approve {symbol} → {spender[:10]}… (uint256.max)")

    gas_params = await _get_gas_params(web3)
    nonce      = await web3.eth.get_transaction_count(owner)
    tx = await token.functions.approve(
        spender_cs, MAX_UINT256
    ).build_transaction({
        "from":  owner_cs,
        "nonce": nonce,
        "gas":   120_000,
        **gas_params,
    })
    signed  = web3.eth.account.sign_transaction(tx, private_key)
    tx_hash = await web3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = await web3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

    if receipt.get("status") == 1:
        _approve_cache[key] = True
        logger.success(f"  ✅ Approve {symbol}: {tx_hash.hex()}")
        return True
    else:
        logger.error(f"  ❌ Approve {symbol} reverted!")
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  Раздел 4: Исполнение через MultiHopSwapUniversal.exactInput
# ══════════════════════════════════════════════════════════════════════════════

async def execute_chain(
    web3: AsyncWeb3,
    contract,
    chain_info: dict,
) -> bool:
    """
    Отправляет TX в MultiHopSwapUniversal.exactInput.

    chain_info обязан содержать:
      pools_addresses : list[str]   — адреса пулов (поле "pool" из all_pools.json)
      dex_types       : list[int]   — 0=Uniswap/Sushi, 1=Pancake (len == pools)
      token_path      : list[str]   — маршрут токенов (len = pools+1)
      amount_in_wei   : int
      amount_out_min  : int
      cumulative_decimals : int
      chain_label     : str
      step_count      : int
      dex_route       : str         (для логов)
    """
    label    = chain_info["chain_label"]
    decimals = chain_info["cumulative_decimals"]
    n_steps  = chain_info["step_count"]
    dex_route = chain_info.get("dex_route", "?")

    pools_addresses = chain_info["pools_addresses"]
    dex_types       = chain_info["dex_types"]        # ← uint8[] для контракта
    token_path      = chain_info["token_path"]
    amount_in_wei   = chain_info["amount_in_wei"]
    amount_out_min  = chain_info["amount_out_min"]
    profit_pct      = chain_info.get("profit_pct", 0)

    # ── Проверки структуры ────────────────────────────────────────────────────
    if len(pools_addresses) != n_steps:
        logger.warning(f"  ⚠️  [{label}] pools ({len(pools_addresses)}) ≠ steps ({n_steps})")
        return False
    if len(dex_types) != n_steps:
        logger.warning(f"  ⚠️  [{label}] dex_types ({len(dex_types)}) ≠ steps ({n_steps})")
        return False
    if len(token_path) != n_steps + 1:
        logger.warning(f"  ⚠️  [{label}] token_path ({len(token_path)}) ≠ steps+1 ({n_steps+1})")
        return False

    try:
        private_key = os.environ["OWNER_PRIVATE_KEY"]
        owner_addr  = web3.eth.account.from_key(private_key).address

        # ── Проверка баланса ──────────────────────────────────────────────────
        token_in_cs = web3.to_checksum_address(token_path[0])
        erc20       = web3.eth.contract(address=token_in_cs, abi=ERC20_ABI)
        balance     = await erc20.functions.balanceOf(owner_addr).call()
        if balance < amount_in_wei:
            logger.warning(
                f"  ⚠️  [{label}] Мало баланса: "
                f"{balance / 10**decimals:.4f} < {amount_in_wei / 10**decimals:.4f}"
            )
            return False

        # ── Approve tokenIn (один раз) ────────────────────────────────────────
        approved = await _ensure_approve(
            web3, token_path[0], owner_addr,
            contract.address, amount_in_wei, private_key
        )
        if not approved:
            logger.warning(f"  ⚠️  [{label}] Approve не прошёл")
            return False

        # ── Параметры exactInput ──────────────────────────────────────────────
        deadline  = int(time.time()) + int(os.getenv("DEADLINE_SECS", "120"))
        pools_cs  = [web3.to_checksum_address(a) for a in pools_addresses]
        tokens_cs = [web3.to_checksum_address(a) for a in token_path]

        logger.info(
            f"  📋 [{n_steps}шаг|{dex_route}] {label} | "
            f"in={amount_in_wei / 10**decimals:.4f} "
            f"out_min={amount_out_min / 10**decimals:.4f} "
            f"profit≥{profit_pct:.4f}%"
        )
        logger.debug(
            f"       pools={pools_cs}\n"
            f"       dexTypes={dex_types}\n"
            f"       tokens={tokens_cs}"
        )

        # ── TX ────────────────────────────────────────────────────────────────
        gas_params = await _get_gas_params(web3)
        nonce      = await web3.eth.get_transaction_count(owner_addr)

        # Tuple передаётся в порядке полей ExactInputParams в контракте:
        # pools, dexTypes, tokens, amountIn, amountOutMinimum, recipient, deadline
        tx = await contract.functions.exactInput((
            pools_cs,
            dex_types,      # ← uint8[]
            tokens_cs,
            amount_in_wei,
            amount_out_min,
            owner_addr,
            deadline,
        )).build_transaction({
            "from":  owner_addr,
            "nonce": nonce,
            "gas":   DEFAULT_GAS_LIMIT,
            **gas_params,
        })

        signed  = web3.eth.account.sign_transaction(tx, private_key)
        tx_hash = await web3.eth.send_raw_transaction(signed.raw_transaction)
        logger.info(f"  📤 [{label}] TX: {tx_hash.hex()}")

        receipt      = await web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        gas_used     = receipt.get("gasUsed", 0)
        gas_price    = receipt.get("effectiveGasPrice", 0)
        gas_cost_eth = gas_used * gas_price / 1e18

        if receipt.get("status") == 1:
            profit_human = chain_info.get("profit", 0)
            logger.success(
                f"  ✅ [{label}] УСПЕХ | "
                f"profit≈{profit_human:.6f} | gas={gas_cost_eth:.6f} ETH"
            )
            logger.success(f"  🔗 https://arbiscan.io/tx/{tx_hash.hex()}")
            return True
        else:
            logger.warning(
                f"  ❌ [{label}] TX откатилась | газ: {gas_cost_eth:.6f} ETH"
            )
            logger.warning(f"  🔗 https://arbiscan.io/tx/{tx_hash.hex()}")
            return False

    except Exception as e:
        err = str(e)
        if "nonce" in err.lower():
            logger.warning(f"  ⚠️  [{label}] Nonce конфликт")
        elif "insufficient funds" in err.lower():
            logger.warning(f"  ⚠️  [{label}] Не хватает ETH на газ")
        elif "already known" in err.lower():
            logger.warning(f"  ⚠️  [{label}] TX уже в мемпуле")
        elif "InsufficientOutput" in err:
            logger.warning(f"  ⚠️  [{label}] InsufficientOutput — slippage, увеличьте SLIPPAGE_BPS")
        elif "TokenMismatch" in err:
            logger.warning(f"  ⚠️  [{label}] TokenMismatch — token_path не совпадает с пулами")
        elif "OnlyPool" in err:
            logger.warning(
                f"  ⚠️  [{label}] OnlyPool — неверный dexType: "
                f"dex_types={dex_types}. "
                f"Проверьте что dexId пула совпадает с типом callback."
            )
        elif "InvalidDexType" in err:
            logger.warning(f"  ⚠️  [{label}] InvalidDexType — dex_types[i] должен быть 0-4")
        elif "V2ZeroOutput" in err:
            logger.warning(f"  ⚠️  [{label}] V2ZeroOutput — V2 пул пустой или резервы = 0")
        elif "AlgebraOnlyPool" in err:
            logger.warning(f"  ⚠️  [{label}] AlgebraOnlyPool — Algebra callback от неверного пула, проверьте dexType=5 для Camelot")
        elif "ArrayLengthMismatch" in err:
            logger.warning(f"  ⚠️  [{label}] ArrayLengthMismatch — pools и dexTypes разной длины")
        else:
            logger.warning(f"  ⚠️  [{label}] Ошибка: {err[:200]}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  Раздел 5: Основной цикл бота
# ══════════════════════════════════════════════════════════════════════════════

async def run_bot(active_steps: list[int]) -> None:
    from chain_searcher import MultiChainSearcher, close_all_sessions

    web3 = AsyncWeb3(AsyncHTTPProvider(os.environ["ARBITRUM_RPC_URL"]))

    if not await web3.is_connected():
        logger.error("Нет соединения с Web3. Проверьте ARBITRUM_RPC_URL.")
        await close_all_sessions()
        return

    contract = web3.eth.contract(
        address=web3.to_checksum_address(os.environ["MULTIHOP_CONTRACT"]),
        abi=MULTIHOP_ABI,
    )

    min_profit_pct = float(os.getenv("MIN_PROFIT_PCT", "0.15"))
    slippage_bps   = int(os.getenv("SLIPPAGE_BPS", "50"))

    labels = " + ".join(f"{s}-шаг" for s in active_steps)
    logger.info(f"📋 Порог прибыли: ≥{min_profit_pct:.3f}%  slippage={slippage_bps}bps")
    logger.info(f"🔧 Режимы: {labels} | gas_limit={DEFAULT_GAS_LIMIT}")
    logger.info(f"📦 Контракт: {os.environ['MULTIHOP_CONTRACT']}")
    logger.info("🤖 Бот запущен. Ctrl+C для остановки.")

    stats = {"cycles": 0, "found": 0, "executed": 0, "tx_sent": 0}

    async def on_profitable(chain_info: dict) -> None:
        stats["found"]   += 1
        stats["tx_sent"] += 1
        if await execute_chain(web3, contract, chain_info):
            stats["executed"] += 1

    try:
        while True:
            stats["cycles"] += 1
            logger.info("═" * 55)
            logger.info(
                f"  Цикл #{stats['cycles']}  |  "
                f"найдено: {stats['found']}  |  "
                f"TX: {stats['tx_sent']}  |  "
                f"успех: {stats['executed']}"
            )
            logger.info("═" * 55)

            try:
                searcher = MultiChainSearcher(
                    active_steps   = active_steps,
                    execute_fn     = on_profitable,
                    min_profit_pct = min_profit_pct,
                    shared_web3    = web3,
                )
                if searcher.uploading_data():
                    await searcher.run()
                else:
                    logger.error("Ошибка загрузки данных, пропуск цикла")
            except KeyboardInterrupt:
                raise
            except (ConnectionError, RuntimeError, ValueError) as _exc:
                logger.exception(f"Ошибка в цикле поиска: {_exc}")
            except Exception:
                logger.exception("Неожиданная ошибка в цикле поиска")

            found_this = stats["found"] - stats.get("_last_found", 0)
            if found_this == 0:
                logger.info(f"⏳ Ничего не найдено. Ожидание {CYCLE_SLEEP_SECONDS}s...")
                await asyncio.sleep(CYCLE_SLEEP_SECONDS)
            stats["_last_found"] = stats["found"]

    finally:
        await close_all_sessions()
        logger.info(
            f"📊 Итого: циклов={stats['cycles']}, "
            f"найдено={stats['found']}, TX={stats['tx_sent']}, "
            f"успех={stats['executed']}"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  Раздел 6: Меню
# ══════════════════════════════════════════════════════════════════════════════

MENU_BANNER = """
╔══════════════════════════════════════════════════════════╗
║   Arbitrage Bot — Arbitrum DEX (MultiHopSwapUniversal)   ║
╠══════════════════════════════════════════════════════════╣
║  1. Поиск пулов        (search_pools → pools.json)       ║
║  2. Проверка пулов     (pools_check → all_pools.json)    ║
║  3. Запуск бота        (on-chain поиск + исполнение)     ║
║  0. Выход                                                ║
╚══════════════════════════════════════════════════════════╝"""

STEPS_BANNER = """
╔══════════════════════════════════════════════════════════╗
║           Выберите режимы поиска цепочек                 ║
╠══════════════════════════════════════════════════════════╣
║  Введите номера через пробел или запятую.                ║
║  Примеры: 2 / 3 / 2 3 / 2 3 4 5                          ║
║  Enter без ввода → все режимы (2 3 4 5)                  ║
╚══════════════════════════════════════════════════════════╝"""


def _prompt_active_steps() -> list[int]:  # noqa: возвращает [] если отмена
    print(STEPS_BANNER)
    while True:
        try:
            raw = input("Шаги поиска [по умолчанию: 2 3 4 5]: ").strip()
        except (KeyboardInterrupt, EOFError):
            return []
        if not raw:
            return [2, 3, 4, 5]
        parts = raw.replace(",", " ").split()
        try:
            steps = [int(p) for p in parts]
        except ValueError:
            print("❌ Введите числа: 2, 3, 4 или 5.")
            continue
        invalid = [s for s in steps if s not in {2, 3, 4, 5}]
        if invalid:
            print(f"❌ Недопустимые значения: {invalid}.")
            continue
        unique = sorted(set(steps))
        if not unique:
            print("❌ Необходимо выбрать хотя бы один шаг.")
            continue
        print(f"✅ Выбраны режимы: {unique}")
        return unique


def _check_env() -> tuple[list, list]:
    required = [
        "ARBITRUM_RPC_URL", "CUMULATIVE_TOKEN", "AMOUNT", "UNISWAP_QUOTER",
        "RAMSES_QUOTER",
    ]
    bot_required = [
        "MULTIHOP_CONTRACT",
        "OWNER_PRIVATE_KEY",
    ]
    missing_base = [k for k in required     if not os.getenv(k)]
    missing_bot  = [k for k in bot_required if not os.getenv(k)]
    if missing_base:
        logger.warning(f"⚠️  Отсутствуют переменные: {', '.join(missing_base)}")
    if not os.getenv("MIN_PROFIT_PCT"):
        logger.debug("MIN_PROFIT_PCT не задана → 0.15%")
    if not os.getenv("SLIPPAGE_BPS"):
        logger.debug("SLIPPAGE_BPS не задана → 50 (0.5%)")
    return missing_base, missing_bot


def main():
    missing_base, missing_bot = _check_env()
    print(MENU_BANNER)
    if missing_base:
        print(f"\n⚠️  Отсутствуют переменные .env: {', '.join(missing_base)}")

    while True:
        print()
        try:
            choice = input("Выберите действие [0-3]: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nВыход.")
            break

        if choice == "0":
            print("Выход.")
            break
        elif choice == "1":
            run_search_pools()
        elif choice == "2":
            run_pools_check()
        elif choice == "3":
            if missing_bot:
                print(f"\n❌ Для бота нужны переменные: {', '.join(missing_bot)}")
                print("   Заполните .env и перезапустите.")
                continue
            active_steps = _prompt_active_steps()
            if not active_steps:
                print("Отменено.")
                continue
            try:
                asyncio.run(run_bot(active_steps))
            except KeyboardInterrupt:
                logger.info("Бот остановлен пользователем.")
        else:
            print("Неверный выбор. Введите 0, 1, 2 или 3.")


if __name__ == "__main__":
    main()
