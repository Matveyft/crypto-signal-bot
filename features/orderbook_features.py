"""Фичи стакана и потока сделок: imbalance, стены, CVD.

Данные берутся из Redis-кэша, который наполняет BybitCollector.
"""

from __future__ import annotations

from typing import Any

from data_layer.storage.redis_cache import RedisCache


class OrderbookAnalyzer:
    """Анализ стакана и CVD по данным из Redis."""

    def __init__(self, cache: RedisCache, wall_notional_threshold: float = 100_000.0) -> None:
        """Инициализирует анализатор.

        Args:
            cache: Redis-кэш со снапшотами.
            wall_notional_threshold: Минимальный объём (в USDT) уровня-стены.
        """
        self._cache = cache
        self._wall_threshold = wall_notional_threshold

    @staticmethod
    def calculate_imbalance(
        bids: list[list[float]], asks: list[list[float]], depth: int = 10
    ) -> float | None:
        """Считает дисбаланс стакана: bid volume / ask volume в топ-N уровнях.

        Args:
            bids: [[price, size], ...] по убыванию цены.
            asks: [[price, size], ...] по возрастанию цены.
            depth: Глубина учёта (уровней с каждой стороны).

        Returns:
            Имбаланс (>1 — давление покупателей) или None при пустом стакане.
        """
        bid_vol = sum(size for _, size in bids[:depth])
        ask_vol = sum(size for _, size in asks[:depth])
        if ask_vol <= 0 or bid_vol <= 0:
            return None
        return bid_vol / ask_vol

    def detect_walls(
        self,
        bids: list[list[float]],
        asks: list[list[float]],
        threshold_notional: float | None = None,
    ) -> dict[str, list[dict[str, float]]]:
        """Ищет крупные уровни (стены) в стакане.

        Args:
            bids: [[price, size], ...].
            asks: [[price, size], ...].
            threshold_notional: Порог объёма в USDT (size уже в базовой монете,
                неional = price * size).

        Returns:
            {'bid_walls': [{'price', 'notional'}], 'ask_walls': [...]}.
        """
        threshold = threshold_notional or self._wall_threshold

        def walls(levels: list[list[float]]) -> list[dict[str, float]]:
            found = []
            for price, size in levels:
                notional = price * size
                if notional >= threshold:
                    found.append({"price": price, "notional": notional})
            return found

        return {"bid_walls": walls(bids), "ask_walls": walls(asks)}

    async def calculate_cvd(self, symbol: str, window: str = "15min") -> float | None:
        """Возвращает CVD за окно из Redis-снапшота.

        Args:
            symbol: Символ в формате ccxt.
            window: '5min' | '15min' | '30min' | 'total'.

        Returns:
            CVD (buy volume - sell volume) или None.
        """
        data = await self._cache.get_cvd(symbol)
        if data is None:
            return None
        window_keys = {"5min": "m5", "15min": "m15", "30min": "m30", "total": "total"}
        key = window_keys.get(window)
        if key is None:
            raise ValueError(f"Unknown CVD window: {window!r}")
        return data.get(key)

    # ------------------------------------------------------- high-level API
    async def get_snapshot(self, symbol: str) -> dict[str, Any] | None:
        """Полный снапшот стакана с метриками из Redis.

        Args:
            symbol: Символ в формате ccxt.

        Returns:
            dict с bids, asks, imbalance, walls или None, если данных нет.
        """
        data = await self._cache.get_orderbook(symbol)
        if data is None:
            return None
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        imbalance = self.calculate_imbalance(bids, asks, depth=10)
        walls = self.detect_walls(bids, asks)
        return {
            "bids": bids,
            "asks": asks,
            "imbalance": imbalance if imbalance is not None else data.get("imbalance"),
            "bid_walls": walls["bid_walls"],
            "ask_walls": walls["ask_walls"],
            "updated_at": data.get("updated_at"),
        }

    async def get_flow_metrics(self, symbol: str) -> dict[str, Any]:
        """Имбаланс + стены + CVD одним вызовом (для signal_generator).

        Args:
            symbol: Символ в формате ccxt.

        Returns:
            dict: imbalance, cvd_30m, bid_walls_present, ask_walls_present,
            snapshot_age_sec. Значения None, если данных нет.
        """
        snapshot = await self.get_snapshot(symbol)
        cvd = await self.calculate_cvd(symbol, "30min")
        if snapshot is None:
            return {
                "imbalance": None, "cvd_30m": cvd,
                "bid_walls_present": False, "ask_walls_present": False,
                "snapshot_age_sec": None,
            }
        import time as _time

        age = _time.time() - snapshot["updated_at"] if snapshot.get("updated_at") else None
        return {
            "imbalance": snapshot["imbalance"],
            "cvd_30m": cvd,
            "bid_walls_present": len(snapshot["bid_walls"]) > 0,
            "ask_walls_present": len(snapshot["ask_walls"]) > 0,
            "snapshot_age_sec": age,
        }
