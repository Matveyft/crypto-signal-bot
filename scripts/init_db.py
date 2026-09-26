"""Инициализация БД: extension, таблицы, hypertables, индексы.

Запуск: python -m scripts.init_db
"""

import asyncio

from data_layer.storage.timescale_manager import TimescaleManager
from data_layer.utils import setup_logging

logger = setup_logging("init_db")


async def main() -> None:
    """Создаёт схему и hypertables в TimescaleDB."""
    db = TimescaleManager()
    try:
        await db.init_schema()
        logger.info("Database initialized successfully")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
