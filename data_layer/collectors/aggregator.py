"""Агрегация 1m свечей в 5m, 15m, 1h, 4h, 1d (pandas resample)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from data_layer.collectors.base_collector import BaseCollector
from data_layer.storage.timescale_manager import TimescaleManager
from data_layer.utils import TIMEFRAME_MINUTES

RESAMPLE_RULES: dict[str, str] = {
    "5m": "5min", "15m": "15min", "1h": "1h", "4h": "4h", "1d": "1D",
}


def aggregate(
    df_1m: pd.DataFrame,
    source_tf: str = "1m",
    target_tf: str = "5m",
) -> pd.DataFrame:
    """Агрегирует свечи младшего ТФ в старший через pandas resample.

    Отбрасывает незавершённую последнюю свечу целевого ТФ.

    Args:
        df_1m: DataFrame [timestamp, open, high, low, close, volume].
        source_tf: Исходный таймфрейм (для валидации).
        target_tf: Целевой таймфрейм ('5m', '15m', '1h', '4h', '1d').

    Returns:
        Агрегированный DataFrame с теми же колонками.

    Raises:
        ValueError: Неверный таймфрейм.
    """
    if source_tf != "1m":
        raise ValueError(f"Aggregation supported only from 1m, got {source_tf}")
    if target_tf not in RESAMPLE_RULES:
        raise ValueError(f"Unknown target timeframe: {target_tf}")
    if df_1m.empty:
        return df_1m.iloc[0:0].copy()

    df = df_1m.set_index(pd.DatetimeIndex(df_1m["timestamp"])).sort_index()
    agg = df.resample(RESAMPLE_RULES[target_tf]).agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    )
    # Последняя незавершённая свеча целевого ТФ
    tf_sec = TIMEFRAME_MINUTES[target_tf] * 60
    last_bucket_start = agg.index[-1].timestamp()
    now = datetime.now(tz=timezone.utc).timestamp()
    if now - last_bucket_start < tf_sec:
        agg = agg.iloc[:-1]

    agg = agg.dropna(subset=["open", "close"])
    result = agg.reset_index().rename(columns={"index": "timestamp"})
    return result[["timestamp", "open", "high", "low", "close", "volume"]]


class Aggregator(BaseCollector):
    """Периодическая агрегация 1m -> всех старших ТФ для списка символов."""

    name = "aggregator"

    def __init__(
        self,
        symbols: list[str],
        db: TimescaleManager,
        target_timeframes: list[str] | None = None,
        poll_interval_sec: int = 60,
        lookback_hours: int = 24,
    ) -> None:
        """Инициализирует агрегатор.

        Args:
            symbols: Символы в формате ccxt.
            db: Менеджер TimescaleDB.
            target_timeframes: Список целевых ТФ (по умолчанию все из RESAMPLE_RULES).
            poll_interval_sec: Период запуска агрегации.
            lookback_hours: Окно чтения 1m-свечей за один проход.
        """
        super().__init__()
        self._symbols = symbols
        self._db = db
        self._targets = target_timeframes or list(RESAMPLE_RULES)
        self._poll_interval = poll_interval_sec
        self._lookback = timedelta(hours=lookback_hours)

    async def _run(self) -> None:
        """Цикл агрегации всех символов."""
        while not self._stop_event.is_set():
            started = asyncio.get_event_loop().time()
            for symbol in self._symbols:
                if self._stop_event.is_set():
                    break
                try:
                    await self.aggregate_symbol(symbol)
                except Exception:
                    self.stats["errors"] += 1
                    self.logger.exception("Aggregation failed for %s", symbol)
            elapsed = asyncio.get_event_loop().time() - started
            await self._sleep(max(1.0, self._poll_interval - elapsed))

    async def aggregate_symbol(self, symbol: str) -> int:
        """Агрегирует один символ по всем целевым ТФ.

        Args:
            symbol: Символ в формате ccxt.

        Returns:
            Суммарное число записанных свечей.
        """
        since = datetime.now(tz=timezone.utc) - self._lookback
        df_1m = await self._db.fetch_ohlcv_since(symbol, "1m", since)
        if df_1m.empty:
            return 0
        written = 0
        for tf in self._targets:
            agg = aggregate(df_1m, "1m", tf)
            if agg.empty:
                continue
            rows: list[dict[str, Any]] = []
            for _, r in agg.iterrows():
                ts = r["timestamp"]
                rows.append({
                    "symbol": symbol,
                    "timeframe": tf,
                    "timestamp": ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts,
                    "open": float(r["open"]),
                    "high": float(r["high"]),
                    "low": float(r["low"]),
                    "close": float(r["close"]),
                    "volume": float(r["volume"]),
                })
            written += await self._db.upsert_ohlcv(rows)
            self.stats["messages_processed"] += len(rows)
        self.stats["last_message_at"] = asyncio.get_event_loop().time()
        self.logger.debug("Aggregated %s: +%d candles", symbol, written)
        return written
