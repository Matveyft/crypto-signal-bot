"""Запуск всех коллекторов: Bybit WS + Funding + Aggregator.

Запуск: python -m scripts.run_collector
Остановка: Ctrl+C или SIGTERM (graceful shutdown).
"""

import asyncio
import signal

from data_layer.collectors.aggregator import Aggregator
from data_layer.collectors.bybit_collector import BybitCollector
from data_layer.collectors.funding_collector import FundingCollector
from data_layer.storage.redis_cache import RedisCache
from data_layer.storage.timescale_manager import TimescaleManager
from data_layer.utils import load_config, setup_logging

logger = setup_logging("run_collector")


async def main() -> None:
    """Поднимает БД/Redis, запускает коллекторы и ждёт сигнала остановки."""
    cfg = load_config("symbols")
    symbols: list[str] = cfg["symbols"]

    db = TimescaleManager()
    cache = RedisCache()
    await db.init_schema()

    collectors = [
        BybitCollector(symbols, db, cache),
        FundingCollector(symbols, db, cache),
        Aggregator(
            symbols, db,
            target_timeframes=cfg["timeframes"]["aggregated"],
            poll_interval_sec=60,
        ),
    ]

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows
            signal.signal(sig, lambda *_: stop.set())

    # start() создаёт фоновую задачу и сразу возвращается
    await asyncio.gather(*(c.start() for c in collectors))
    logger.info("All collectors started for %d symbols", len(symbols))
    try:
        await stop.wait()
    finally:
        logger.info("Shutting down...")
        await asyncio.gather(*(c.stop() for c in collectors), return_exceptions=True)
        await cache.close()
        await db.close()
    logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
