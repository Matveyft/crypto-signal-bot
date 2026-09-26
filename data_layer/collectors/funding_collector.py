"""Funding collector: REST polling каждые 5 минут.

Источники:
    funding rate      -> ccxt fetch_funding_rate
    open interest     -> ccxt fetch_open_interest
    long/short ratio  -> Bybit V5 /v5/market/account-ratio (кастомный запрос)
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp
import ccxt.async_support as ccxt

from data_layer.collectors.base_collector import BaseCollector
from data_layer.storage.redis_cache import RedisCache
from data_layer.storage.timescale_manager import TimescaleManager
from data_layer.utils import symbol_to_bybit

ACCOUNT_RATIO_URL = "https://api.bybit.com/v5/market/account-ratio"
POLL_INTERVAL_SEC = 300  # 5 минут


class FundingCollector(BaseCollector):
    """Периодический опрос funding rate, OI и long/short ratio."""

    name = "funding"

    def __init__(
        self,
        symbols: list[str],
        db: TimescaleManager,
        cache: RedisCache,
        poll_interval_sec: int = POLL_INTERVAL_SEC,
    ) -> None:
        """Инициализирует коллектор.

        Args:
            symbols: Символы в формате ccxt.
            db: Менеджер TimescaleDB.
            cache: Redis-кэш.
            poll_interval_sec: Интервал опроса.
        """
        super().__init__()
        self._symbols = symbols
        self._db = db
        self._cache = cache
        self._poll_interval = poll_interval_sec
        self._exchange = ccxt.bybit({"enableRateLimit": True})

    async def _run(self) -> None:
        """Цикл опроса всех символов с записью в БД и Redis."""
        async with aiohttp.ClientSession() as http:
            while not self._stop_event.is_set():
                started = time.time()
                for symbol in self._symbols:
                    if self._stop_event.is_set():
                        break
                    await self._poll_symbol(symbol, http)
                elapsed = time.time() - started
                await self._sleep(max(1.0, self._poll_interval - elapsed))

    async def _poll_symbol(self, symbol: str, http: aiohttp.ClientSession) -> None:
        """Собирает метрики одного символа и сохраняет их.

        Args:
            symbol: Символ в формате ccxt.
            http: aiohttp-сессия для кастомных запросов.
        """
        try:
            funding = await self._exchange.fetch_funding_rate(symbol)
            funding_rate = float(funding["fundingRate"]) if funding.get("fundingRate") is not None else None
        except Exception as exc:
            self.logger.warning("fetch_funding_rate failed for %s: %s", symbol, exc)
            funding_rate = None

        try:
            oi = await self._exchange.fetch_open_interest(symbol)
            open_interest = float(oi["openInterestAmount"]) if oi.get("openInterestAmount") is not None else None
        except Exception as exc:
            self.logger.warning("fetch_open_interest failed for %s: %s", symbol, exc)
            open_interest = None

        long_short_ratio = await self._fetch_account_ratio(http, symbol)

        # Процент изменения OI к предыдущему снапшоту (нужен breakout-сетапу)
        prev = await self._cache.get_funding(symbol)
        oi_change_pct = None
        if open_interest is not None and prev and prev.get("open_interest"):
            oi_change_pct = (open_interest - prev["open_interest"]) / prev["open_interest"] * 100

        row = {
            "symbol": symbol,
            "timestamp": datetime.now(tz=timezone.utc),
            "funding_rate": funding_rate,
            "open_interest": open_interest,
            "long_short_ratio": long_short_ratio,
        }
        if any(v is not None for k, v in row.items() if k not in ("symbol", "timestamp")):
            await self._db.upsert_funding([row])
            await self._cache.set_funding(symbol, {
                "funding_rate": funding_rate,
                "open_interest": open_interest,
                "long_short_ratio": long_short_ratio,
                "oi_change_pct": oi_change_pct,
                "updated_at": time.time(),
            })
            self.stats["messages_processed"] += 1
            self.stats["last_message_at"] = time.time()

    async def _fetch_account_ratio(
        self, http: aiohttp.ClientSession, symbol: str
    ) -> float | None:
        """Запрашивает global long/short ratio через Bybit V5 API.

        Args:
            http: aiohttp-сессия.
            symbol: Символ в формате ccxt.

        Returns:
            Отношение long/short (например 1.85) или None при ошибке.
        """
        try:
            params = {
                "category": "linear",
                "symbol": symbol_to_bybit(symbol),
                "period": "5min",
                "limit": 1,
            }
            async with http.get(ACCOUNT_RATIO_URL, params=params) as resp:
                payload = await resp.json()
            rows = payload.get("result", {}).get("list", [])
            if rows:
                row = rows[0]
                if "longShortRatio" in row:
                    return float(row["longShortRatio"])
                # Актуальный формат V5: buyRatio / sellRatio
                buy = float(row.get("buyRatio", 0))
                sell = float(row.get("sellRatio", 0))
                if sell > 0:
                    return buy / sell
        except Exception as exc:
            self.logger.warning("account-ratio failed for %s: %s", symbol, exc)
        return None

    async def cleanup(self) -> None:
        """Закрывает ccxt-сессию."""
        await self._exchange.close()
