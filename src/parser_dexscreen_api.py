import asyncio
import os
import json
import aiohttp
from dotenv import load_dotenv
from loguru import logger


load_dotenv()


class DexscreenerParser:
    BASE_URL = "https://api.dexscreener.com/token-pairs/v1/arbitrum/"
    MAX_CONCURRENT = 7   # не более 7 запросов одновременно
    MAX_RETRIES = 5      # максимум 5 попыток на запрос
    BATCH_SIZE = 100     # пауза каждые N токенов
    BATCH_PAUSE = 60     # пауза в секундах

    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.tokens: dict = {}
        self.pools: dict = {}

    def _load_tokens(self):
        """Загрузка файла с токенами"""
        try:
            with open(os.path.join(self.script_dir, "pools", "tokens.json"), "r", encoding="utf-8") as f:
                self.tokens = json.load(f)
            logger.debug("Файл tokens.json загружен")
        except FileNotFoundError:
            logger.error("Отсутствует файл tokens.json")

    async def _fetch(
        self,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        token_name: str,
        token_address: str,
    ) -> tuple[str, list] | None:
        """Один запрос с повторами при сетевых ошибках и 429."""
        url = self.BASE_URL + token_address

        async with semaphore:
            for attempt in range(1, self.MAX_RETRIES + 1):
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:

                        # 429 Too Many Requests — ждём и повторяем
                        if resp.status == 429:
                            wait = 2 ** attempt  # 2, 4, 8, 16, 32 сек
                            if attempt < self.MAX_RETRIES:
                                logger.warning(
                                    f"[{token_name}] 429 Too Many Requests "
                                    f"(попытка {attempt}/{self.MAX_RETRIES})"
                                    f" — повтор через {wait} сек"
                                )
                                await asyncio.sleep(wait)
                                continue
                            else:
                                logger.error(f"[{token_name}] все {self.MAX_RETRIES} попытки исчерпаны — пропускаем")
                                return None

                        # Остальные HTTP-ошибки — повторять бессмысленно
                        if resp.status >= 400:
                            logger.warning(f"[{token_name}] HTTP {resp.status} — пропускаем")
                            return None

                        data = await resp.json(content_type=None)
                        logger.debug(f"[{token_name}] получено {len(data)} пулов")
                        return token_name, data

                except asyncio.TimeoutError:
                    wait = 2 ** (attempt - 1)
                    if attempt < self.MAX_RETRIES:
                        logger.warning(
                            f"[{token_name}] таймаут "
                            f"(попытка {attempt}/{self.MAX_RETRIES})"
                            f" — повтор через {wait} сек"
                        )
                        await asyncio.sleep(wait)
                    else:
                        logger.error(f"[{token_name}] все {self.MAX_RETRIES} попытки исчерпаны — пропускаем")

                except aiohttp.ClientConnectionError as e:
                    wait = 2 ** (attempt - 1)
                    if attempt < self.MAX_RETRIES:
                        logger.warning(
                            f"[{token_name}] {type(e).__name__} "
                            f"(попытка {attempt}/{self.MAX_RETRIES})"
                            f" — повтор через {wait} сек"
                        )
                        await asyncio.sleep(wait)
                    else:
                        logger.error(f"[{token_name}] все {self.MAX_RETRIES} попытки исчерпаны — пропускаем")

                except aiohttp.ClientError as e:
                    logger.warning(f"[{token_name}] {type(e).__name__} — пропускаем")
                    return None

        return None

    async def _run(self):
        self._load_tokens()
        if not self.tokens:
            return

        all_tokens = list(self.tokens.items())
        total = len(all_tokens)
        # Разбиваем на батчи по BATCH_SIZE
        batches = [
            all_tokens[i : i + self.BATCH_SIZE]
            for i in range(0, total, self.BATCH_SIZE)
        ]

        logger.info(f"Всего токенов: {total}, батчей: {len(batches)} (по {self.BATCH_SIZE} шт.)")

        semaphore = asyncio.Semaphore(self.MAX_CONCURRENT)
        connector = aiohttp.TCPConnector(ssl=False)
        all_results = []

        async with aiohttp.ClientSession(connector=connector) as session:
            for batch_num, batch in enumerate(batches, start=1):
                logger.info(f"Батч {batch_num}/{len(batches)}: обрабатываем токены {(batch_num - 1) * self.BATCH_SIZE + 1}–{min(batch_num * self.BATCH_SIZE, total)}")

                tasks = [
                    self._fetch(session, semaphore, name, address)
                    for name, address in batch
                ]
                results = await asyncio.gather(*tasks)
                all_results.extend(results)

                # Пауза после каждого батча, кроме последнего
                if batch_num < len(batches):
                    logger.info(f"Пауза {self.BATCH_PAUSE} сек перед следующим батчем...")
                    await asyncio.sleep(self.BATCH_PAUSE)

        for result in all_results:
            if result is None:
                continue
            _, pools_data = result
            for pool in pools_data:
                dex_id = pool.get("dexId", "")
                pair_address = pool.get("pairAddress", "").lower()
                if dex_id and pair_address:
                    bucket = self.pools.setdefault(dex_id, [])
                    if pair_address not in bucket:
                        bucket.append(pair_address)

        self._save_file()

    def start(self):
        """Публичный синхронный вход."""
        asyncio.run(self._run())

    def _save_file(self):
        path = os.path.join(self.script_dir, "pools", "pools_dexscreener.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.pools, f, ensure_ascii=False, indent=4)
        logger.info(f"Сохранено {sum(len(v) for v in self.pools.values())} пулов → {path}")


if __name__ == "__main__":
    parser = DexscreenerParser()
    parser.start()
