"""Менеджер TimescaleDB: схема, hypertables, bulk-запись и чтение в pandas."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Sequence

import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from data_layer.storage.models import Base, FundingData, OHLCV
from data_layer.utils import load_config, setup_logging

logger = setup_logging()


class TimescaleManager:
    """Асинхронный доступ к TimescaleDB через SQLAlchemy (asyncpg)."""

    def __init__(self, db_config: dict[str, Any] | None = None) -> None:
        """Инициализирует менеджер.

        Args:
            db_config: Секция 'timescaledb' из database.yaml. Если None —
                конфиг загружается автоматически.
        """
        if db_config is None:
            db_config = load_config("database")["timescaledb"]
        self._url = (
            f"postgresql+asyncpg://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['db']}"
        )
        self._pool_size = int(db_config.get("pool_size", 10))
        self._engine: AsyncEngine | None = None

    @property
    def engine(self) -> AsyncEngine:
        """Лениво создаёт пул соединений.

        Returns:
            Асинхронный движок SQLAlchemy.
        """
        if self._engine is None:
            self._engine = create_async_engine(
                self._url,
                pool_size=self._pool_size,
                max_overflow=5,
                pool_pre_ping=True,
                echo=False,
            )
        return self._engine

    async def init_schema(self, run_migration: bool = True) -> None:
        """Создаёт таблицы, hypertables и индексы.

        Args:
            run_migration: Если True, сначала выполняется
                `CREATE EXTENSION IF NOT EXISTS timescaledb`.
        """
        if run_migration:
            async with self.engine.begin() as conn:
                await conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))

        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            # Hypertables: партиционирование по timestamp.
            # Уникальные индексы обязаны включать колонку партиционирования.
            await conn.execute(text(
                "SELECT create_hypertable('ohlcv', 'timestamp',"
                " chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE)"
            ))
            await conn.execute(text(
                "SELECT create_hypertable('funding_data', 'timestamp',"
                " chunk_time_interval => INTERVAL '30 days', if_not_exists => TRUE)"
            ))
        logger.info("Schema initialized (tables + hypertables)")

    async def upsert_ohlcv(self, rows: Sequence[dict[str, Any]]) -> int:
        """Массовая запись свечей (idempotent, ON CONFLICT DO NOTHING).

        Args:
            rows: Список dict с ключами symbol, timeframe, timestamp,
                open, high, low, close, volume.

        Returns:
            Количество строк во входном батче.
        """
        if not rows:
            return 0
        stmt = pg_insert(OHLCV).values(list(rows)).on_conflict_do_nothing(
            index_elements=["symbol", "timeframe", "timestamp"]
        )
        async with self.engine.begin() as conn:
            await conn.execute(stmt)
        return len(rows)

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 500,
        start: datetime | None = None,
    ) -> pd.DataFrame:
        """Читает свечи в DataFrame (по возрастанию timestamp).

        Args:
            symbol: Символ в формате ccxt ('BTC/USDT:USDT').
            timeframe: Таймфрейм ('1m', '5m', '1h', ...).
            limit: Максимум свечей.
            start: Если задано — только свечи после этого времени.

        Returns:
            DataFrame [timestamp, open, high, low, close, volume].
        """
        query = (
            "SELECT timestamp, open, high, low, close, volume FROM ohlcv "
            "WHERE symbol = :symbol AND timeframe = :tf"
        )
        params: dict[str, Any] = {"symbol": symbol, "tf": timeframe}
        if start is not None:
            query += " AND timestamp >= :start"
            params["start"] = start
        query += " ORDER BY timestamp DESC LIMIT :limit"
        params["limit"] = limit

        async with self.engine.connect() as conn:
            result = await conn.execute(text(query), params)
            records = result.mappings().all()
        df = pd.DataFrame(
            [dict(r) for r in records],
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )
        if not df.empty:
            df = df.iloc[::-1].reset_index(drop=True)  # возрастание времени
        return df

    async def upsert_funding(self, rows: Sequence[dict[str, Any]]) -> int:
        """Массовая запись funding-данных (idempotent).

        Args:
            rows: dict с ключами symbol, timestamp, funding_rate,
                open_interest, long_short_ratio.

        Returns:
            Количество строк во входном батче.
        """
        if not rows:
            return 0
        stmt = pg_insert(FundingData).values(list(rows)).on_conflict_do_nothing(
            index_elements=["symbol", "timestamp"]
        )
        async with self.engine.begin() as conn:
            await conn.execute(stmt)
        return len(rows)

    async def fetch_latest_funding(self, symbol: str) -> dict[str, Any] | None:
        """Возвращает последнюю запись funding_data для символа.

        Args:
            symbol: Символ в формате ccxt.

        Returns:
            dict с funding_rate, open_interest, long_short_ratio или None.
        """
        query = (
            "SELECT timestamp, funding_rate, open_interest, long_short_ratio "
            "FROM funding_data WHERE symbol = :symbol "
            "ORDER BY timestamp DESC LIMIT 1"
        )
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query), {"symbol": symbol})
            row = result.mappings().first()
        return dict(row) if row else None

    async def fetch_ohlcv_since(
        self, symbol: str, timeframe: str, since: datetime
    ) -> pd.DataFrame:
        """Читает все свечи после `since` (для агрегатора, без LIMIT).

        Args:
            symbol: Символ в формате ccxt.
            timeframe: Таймфрейм.
            since: Нижняя граница (включительно).

        Returns:
            DataFrame с колонками OHLCV.
        """
        query = (
            "SELECT timestamp, open, high, low, close, volume FROM ohlcv "
            "WHERE symbol = :symbol AND timeframe = :tf AND timestamp >= :since "
            "ORDER BY timestamp ASC"
        )
        async with self.engine.connect() as conn:
            result = await conn.execute(
                text(query), {"symbol": symbol, "tf": timeframe, "since": since}
            )
            records = result.mappings().all()
        return pd.DataFrame(
            [dict(r) for r in records],
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )

    async def data_lag_seconds(self, symbol: str, timeframe: str = "1m") -> float | None:
        """Health check: сколько секунд назад была последняя свеча.

        Args:
            symbol: Символ в формате ccxt.
            timeframe: Таймфрейм для проверки.

        Returns:
            Возраст последней свечи в секундах или None, если данных нет.
        """
        query = (
            "SELECT EXTRACT(EPOCH FROM (now() - MAX(timestamp))) AS lag "
            "FROM ohlcv WHERE symbol = :symbol AND timeframe = :tf"
        )
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query), {"symbol": symbol, "tf": timeframe})
            row = result.mappings().first()
        return float(row["lag"]) if row and row["lag"] is not None else None

    async def close(self) -> None:
        """Закрывает пул соединений (graceful shutdown)."""
        if self._engine is not None:
            await self._engine.dispose()
            logger.info("DB engine disposed")
