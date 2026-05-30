# DEX Arbitrage Engine

Production-grade Python engine that scans Arbitrum DEX pools for multi-hop arbitrage cycles and executes them through a deployed Solidity contract — optionally with Balancer V2 flash-loans for zero-capital execution.

[![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python&logoColor=white)](https://www.python.org/)
[![web3.py](https://img.shields.io/badge/web3.py-7.x-orange)](https://web3py.readthedocs.io/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

This is the **off-chain half** of a complete DEX arbitrage system. The **on-chain executor** lives in a separate repo:  
**👉 [multihop-swap-universal](https://github.com/Garison87/multihop-swap-universal)** — Solidity contract for multi-hop swaps with flash-loan integration.

---

## How it fits together

```
        ┌───────────────────────────────────────────────────────┐
        │   This repo: dex-arbitrage-engine (Python / off-chain) │
        │                                                         │
        │  1. parser_dexscreen_api.py  →  pulls fresh pool list  │
        │  2. pools_check.py           →  validates pools on-chain│
        │  3. chain_searcher.py        →  finds profitable cycles│
        │  4. main.py                  →  signs and submits txs  │
        └───────────────────────────┬───────────────────────────┘
                                    │ encoded calldata
                                    ▼
        ┌───────────────────────────────────────────────────────┐
        │   multihop-swap-universal (Solidity / on-chain)        │
        │                                                         │
        │  • exactInput()  →  multi-hop swap (capital required)  │
        │  • flashSwap()   →  Balancer flash-loan multi-hop       │
        └───────────────────────────────────────────────────────┘
                                    │
                                    ▼
              Uniswap V3 / Pancake V3 pools on Arbitrum
```

---

## Components

### 1. `parser_dexscreen_api.py` — Pool Discovery

Fetches all DEX pools associated with a list of tokens from the public Dexscreener API.

- Reads `tokens.json` (your token watchlist)
- Calls Dexscreener for each token (with backoff for 429 / network errors)
- Batches requests with concurrency limit + pauses
- Outputs `pools_dexscreener.json`: `{dex_id: [pool_address, ...]}`

### 2. `pools_check.py` — Pool Validation

Validates raw pools via on-chain RPC calls. Filters out:

- Pools with mismatched / non-canonical tokens
- Pools with insufficient liquidity
- Dead or migrated pools
- Pools with non-standard fee tiers

Outputs `all_pools.json`: enriched, validated pool metadata grouped by token pair.

### 3. `chain_searcher.py` — Arbitrage Cycle Detector

The core engine. For a chosen base token (e.g., WETH), it:

- Builds a graph of token pairs from validated pools
- Enumerates loops of length 2-5 hops
- For each candidate cycle, calls QuoterV2 contracts (Uniswap, Pancake, Sushi, Algebra-family) **step-by-step** — this is critical, because Quoter contracts cannot quote cross-factory paths atomically
- Caches liquidity reads to avoid redundant RPC calls
- Filters cycles by min-profit threshold and max-price-impact
- Returns ranked list of executable arbitrage opportunities

**Why step-by-step quoting?** Each DEX has its own `QuoterV2` that only knows about its own factory's pools. A path `Uniswap → PancakeSwap → SushiSwap` cannot be quoted with a single `quoteExactInput()` call — you must quote each hop individually and chain the outputs.

### 4. `main.py` — Execution Loop

Wires everything together:

- Loads contract ABI and connects to RPC
- Loops every `CYCLE_SLEEP` seconds:
  - Runs `chain_searcher` for fresh opportunities
  - For the best opportunity above `MIN_PROFIT_PCT`:
    - Builds calldata for `exactInput()` (classic) or `flashSwap()` (flash-loan)
    - Estimates gas, sets priority fee
    - Signs and submits the transaction
    - Logs Arbiscan link
- Two execution modes via `USE_FLASHLOAN`:
  - **`false`**: classic — uses contract's own balance of `CUMULATIVE_TOKEN`
  - **`true`**: flash-loan — borrows from Balancer V2 Vault (0% fee), executes cycle, repays loan, keeps profit

---

## Quickstart

### Prerequisites

- Python 3.11+
- An Arbitrum RPC endpoint (Alchemy free tier, Infura, or public RPC)
- A wallet with some ETH for gas
- (Optional) Deployed [multihop-swap-universal](https://github.com/Garison87/multihop-swap-universal) contract if you want to execute trades

### Install

```bash
git clone https://github.com/Garison87/dex-arbitrage-engine.git
cd dex-arbitrage-engine

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Edit .env: fill in ARBITRUM_RPC_URL at minimum
```

### Run pool discovery + validation

```bash
# Place your tokens.json in src/pools/ (see examples/tokens.example.json)
python src/parser_dexscreen_api.py
python src/pools_check.py
```

This populates `src/pools/pools_dexscreener.json` and `src/pools/all_pools.json`.

### Run the search loop (read-only — finds opportunities, no trades)

```bash
python src/chain_searcher.py
```

You'll see cycles like:
```
[CHAIN-FOUND] WETH → USDC → ARB → WETH | Δ=+0.18% | gross=$2.45 | gas=$0.03 | net=+$2.42
```

### Execute trades (requires MULTIHOP_CONTRACT + OWNER_PRIVATE_KEY)

```bash
python src/main.py
```

⚠️ **Test on a forked node first** (Foundry's `anvil --fork-url` or Hardhat). Set `USE_FLASHLOAN=false` and a tiny `AMOUNT` for your first live run.

---

## Configuration

All parameters are in `.env` — see `.env.example` for the full annotated list. Key knobs:

| Variable | What it does | Default |
|----------|-------------|---------|
| `AMOUNT` | Input size of CUMULATIVE_TOKEN | required |
| `MIN_PROFIT_PCT` | Skip cycles with profit below this | 0.15% |
| `MAX_PRICE_IMPACT_PCT` | Skip cycles where any hop has higher impact | 0.10% |
| `SLIPPAGE_BPS` | Allowed slippage on each swap | 50 bps |
| `GAS_COST_USD` | Used to compute net profit | $0.028 |
| `USE_FLASHLOAN` | Use Balancer flash-loan vs. capital | `false` |
| `CYCLE_SLEEP` | Seconds between scan cycles | 30 |

---

## Example data files

This repo ships with **small example JSONs** under `examples/`:

- `tokens.example.json` — 7 sample tokens (WETH, USDC, ARB, etc.)
- `pools_dexscreener.example.json` — 5 sample pool addresses across 3 DEXes
- `all_pools.example.json` — 2 sample validated pool pairs with full metadata

To run on real data, you generate your own pool dataset by running `parser_dexscreen_api.py` and `pools_check.py`. The output goes into `src/pools/` (gitignored).

---

## Performance notes

`chain_searcher` is the hot path. Performance optimizations applied:

- **Async batching of Quoter calls**: configurable via `MAX_CONCURRENT_REQUESTS` and `BATCH_SIZE`. Default of 5/10 works well with most public RPCs without rate-limiting.
- **Per-cycle liquidity cache**: each pool's liquidity is read once per scan cycle and reused across all candidate chains it appears in.
- **Early termination**: if any hop fails or produces negative output, the cycle is dropped without quoting remaining hops.
- **Retry policy with exponential backoff**: 3 attempts × `RETRY_DELAY^attempt` for transient RPC errors. `429 Too Many Requests` triggers exponential backoff up to 5 retries.

Typical scan on Arbitrum with ~1500 pools: 15-30 seconds per cycle, 50-200 candidate chains evaluated.

---

## Disclaimer

This is **a research / educational project**. It demonstrates a complete off-chain → on-chain arbitrage architecture, but:

- Real-world arbitrage is heavily competitive (MEV bots, private mempools)
- Public RPCs leak your strategy in the public mempool
- You will likely lose money against more sophisticated participants
- Use small amounts and test exhaustively before any live execution

Use at your own risk. No warranty.

---

## License

MIT


# DEX Arbitrage Engine (Русский)

Off-chain движок на Python для поиска и исполнения арбитражных циклов через DEX-пулы Arbitrum. Связан с **on-chain** контрактом [multihop-swap-universal](https://github.com/Garison87/multihop-swap-universal), который выполняет сами свопы.

## Архитектура

Четыре компонента, работающие в pipeline:

1. **`parser_dexscreen_api.py`** — скачивает пулы через Dexscreener API
2. **`pools_check.py`** — валидирует пулы через on-chain RPC
3. **`chain_searcher.py`** — ищет прибыльные циклы 2-5 hop через QuoterV2
4. **`main.py`** — подписывает и отправляет транзакции на контракт

Поддерживает два режима исполнения:

- **Обычный**: использует баланс контракта (`USE_FLASHLOAN=false`)
- **Flash-loan**: занимает у Balancer V2 Vault (0% fee), исполняет цикл, возвращает займ (`USE_FLASHLOAN=true`)

## Установка

```bash
git clone https://github.com/Garison87/dex-arbitrage-engine.git
cd dex-arbitrage-engine

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Заполни ARBITRUM_RPC_URL минимум
```

## Запуск

```bash
# Сбор пулов
python src/parser_dexscreen_api.py
python src/pools_check.py

# Только поиск (без транзакций)
python src/chain_searcher.py

# Полный режим с исполнением
python src/main.py
```

⚠️ Сначала тестируй на форке (`anvil --fork-url`). Не запускай на боевом без понимания рисков.

## Архитектурные решения

**Почему step-by-step квотирование, а не атомарное?**  
Каждый Quoter (Uniswap/Pancake/Sushi) знает только пулы своей фабрики. Путь `Uniswap → Pancake → Sushi` нельзя квотировать одним вызовом — нужно квотировать каждый хоп отдельно и связывать выходы.

**Почему cache ликвидности per cycle?**  
Один пул может участвовать в 10+ цепочках за цикл сканирования. Кешируем `liquidity()` чтение один раз на цикл — экономит ~70% RPC-вызовов.

**Почему async + батчи?**  
Публичные RPC лимитируют до ~10 req/sec. Параллелим до 5 одновременных запросов с батчами по 10, плюс exponential backoff на 429.

## Дисклеймер

Образовательный/портфолио проект. На реальном рынке арбитраж жёстко конкурентен (MEV-боты, приватные мемпулы). Запуск на mainnet — на свой риск.

## Лицензия

MIT
