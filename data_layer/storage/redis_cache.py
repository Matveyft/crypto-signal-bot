"""Real-time кэш в Redis: свечи, CVD, orderbook, funding.

Схема ключей:
    candle:{symbol}:{tf}   — текущая (незакрытая) свеча, TTL 5 мин
    cvd:{symbol}           — суммарный CVD + окна по минутам, TTL 15 мин
    orderbook:{symbol}     — снапшот стакана + метрики, TTL 10 сек
    funding:{symbol}       — последний funding-снапшот, TTL 1 час
"""

from __future__ import annotations

import json
from typing import Any

import redis.asyncio as aioredis

from data_layer.utils import load_config

TTL_CANDLE_SEC = 300
TTL_ORDERBOOK_SEC = 10
TTL_FUNDING_SEC = 3600
TTL_CVD_SEC = 900


def _key(*parts: str) -> str:
    """Собирает redis-ключ из частей.

    Returns:
        Ключ вида 'candle:BTC/USDT:USDT:1m'.
    """
    return ":".join(parts)


class RedisCache:
    """Тонкая обёртка над redis.asyncio с JSON-сериализацией и pipeline."""

    def __init__(self, redis_config: dict[str, Any] | None = None) -> None:
        """Создаёт клиент Redis.

        Args:
            redis_config: Секция 'redis' из database.yaml (или None — автозагрузка).
        """
        if redis_config is None:
            redis_config = load_config("database")["redis"]
        self._client = aioredis.Redis(
            host=redis_config["host"],
            port=int(redis_config["port"]),
            db=int(redis_config.get("db", 0)),
            decode_responses=True,
            socket_keepalive=True,
            health_check_interval=30,
        )

    @property
    def client(self) -> aioredis.Redis:
        """Прямой доступ к клиенту (для pipeline).

        Returns:
            redis.asyncio.Redis клиент.
        """
        return self._client

    # ------------------------------------------------------------------ candle
    async def set_candle(self, symbol: str, timeframe: str, candle: dict[str, Any]) -> None:
        """Сохраняет текущую (незакрытую) свечу.

        Args:
            symbol: Символ ccxt.
            timeframe: Таймфрейм.
            candle: dict с timestamp, open, high, low, close, volume.
        """
        await self._client.set(
            _key("candle", symbol, timeframe), json.dumps(candle), ex=TTL_CANDLE_SEC
        )

    async def get_candle(self, symbol: str, timeframe: str) -> dict[str, Any] | None:
        """Читает текущую свечу.

        Returns:
            dict свечи или None, если ключ истёк.
        """
        raw = await self._client.get(_key("candle", symbol, timeframe))
        return json.loads(raw) if raw else None

    # --------------------------------------------------------------------- cvd
    async def set_cvd(self, symbol: str, cvd_data: dict[str, Any]) -> None:
        """Сохраняет CVD-снапшот.

        Args:
            symbol: Символ ccxt.
            cvd_data: dict с ключами total, m5, m15, m30, updated_at.
        """
        await self._client.set(_key("cvd", symbol), json.dumps(cvd_data), ex=TTL_CVD_SEC)

    async def get_cvd(self, symbol: str) -> dict[str, Any] | None:
        """Читает CVD-снапшот.

        Returns:
            dict с total/m5/m15/m30 или None.
        """
        raw = await self._client.get(_key("cvd", symbol))
        return json.loads(raw) if raw else None

    # --------------------------------------------------------------- orderbook
    async def set_orderbook(self, symbol: str, snapshot: dict[str, Any]) -> None:
        """Сохраняет снапшот стакана с метриками.

        Args:
            symbol: Символ ccxt.
            snapshot: dict с bids, asks, imbalance, walls, updated_at.
        """
        await self._client.set(
            _key("orderbook", symbol), json.dumps(snapshot), ex=TTL_ORDERBOOK_SEC
        )

    async def get_orderbook(self, symbol: str) -> dict[str, Any] | None:
        """Читает снапшот стакана.

        Returns:
            dict со снапшотом или None (TTL 10 сек — устаревший не вернётся).
        """
        raw = await self._client.get(_key("orderbook", symbol))
        return json.loads(raw) if raw else None

    # ----------------------------------------------------------------- funding
    async def set_funding(self, symbol: str, funding: dict[str, Any]) -> None:
        """Сохраняет funding-снапшот.

        Args:
            symbol: Символ ccxt.
            funding: dict с funding_rate, open_interest, long_short_ratio.
        """
        await self._client.set(
            _key("funding", symbol), json.dumps(funding), ex=TTL_FUNDING_SEC
        )

    async def get_funding(self, symbol: str) -> dict[str, Any] | None:
        """Читает funding-снапшот.

        Returns:
            dict или None.
        """
        raw = await self._client.get(_key("funding", symbol))
        return json.loads(raw) if raw else None

    # ---------------------------------------------------------------- pipeline
    async def get_many(self, keys: list[str]) -> list[Any]:
        """Batch-чтение через pipeline (без отдельных ROUND-TRIP'ов).

        Args:
            keys: Список готовых ключей.

        Returns:
            Список значений (JSON-декодированных) или None по каждому ключу.
        """
        async with self._client.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.get(key)
            raw_values = await pipe.execute()
        return [json.loads(v) if v else None for v in raw_values]

    async def ping(self) -> bool:
        """Проверка соединения.

        Returns:
            True, если Redis отвечает.
        """
        try:
            return bool(await self._client.ping())
        except Exception:
            return False

    async def close(self) -> None:
        """Закрывает соединение (graceful shutdown)."""
        await self._client.aclose()
