"""Загрузка исторических OHLCV через REST (ccxt) для всех символов и ТФ.

Запуск:
    python -m scripts.load_historical [--days 30] [--timeframes 1m,5m,...]

Примечание: 30 дней минуток — это ~43k свечей на символ (43 запроса).
Старшие ТФ дешевле грузить напрямую, а не агрегировать из 1m.
"""

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import ccxt.async_support as ccxt

from data_layer.storage.timescale_manager import TimescaleManager
from data_layer.utils import load_config, setup_logging

logger = setup_logging("load_historical")


def parse_args() -> argparse.Namespace:
    """Парсит аргументы командной строки.

    Returns:
        Namespace с days и timeframes.
    """
    parser = argparse.ArgumentParser(description="Load historical OHLCV")
    parser.add_argument("--days", type=int, default=None, help="Days of history")
    parser.add_argument(
        "--timeframes", type=str, default=None,
        help="Comma-separated timeframes (default: all from symbols.yaml)",
    )
    parser.add_argument("--symbols", type=str, default=None,
                        help="Comma-separated ccxt symbols (default: all)")
    return parser.parse_args()


async def load_symbol_timeframe(
    exchange: ccxt.bybit,
    db: TimescaleManager,
    symbol: str,
    timeframe: str,
    since: datetime,
) -> int:
    """Загружает историю одного символа/ТФ с пагинацией.

    Args:
        exchange: ccxt bybit (async).
        db: Менеджер TimescaleDB.
        symbol: Символ в формате ccxt.
        timeframe: Таймфрейм.
        since: Начальная дата.

    Returns:
        Число записанных свечей.
    """
    all_candles: list[dict[str, Any]] = []
    cursor = int(since.timestamp() * 1000)
    end_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    while cursor < end_ms:
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, since=cursor, limit=1000)
        if not ohlcv:
            break
        for ts, o, h, l, c, v in ohlcv:
            if ts >= end_ms:
                continue
            all_candles.append({
                "symbol": symbol,
                "timeframe": timeframe,
                "timestamp": datetime.fromtimestamp(ts / 1000, tz=timezone.utc),
                "open": float(o), "high": float(h), "low": float(l),
                "close": float(c), "volume": float(v),
            })
        cursor = ohlcv[-1][0] + 1  # следующая страница
        if len(ohlcv) < 1000:
            break

    # Дедупликация на всякий случай (REST может пересекаться на границах)
    unique = {c["timestamp"]: c for c in all_candles}
    written = 0
    batch = list(unique.values())
    for i in range(0, len(batch), 1000):
        written += await db.upsert_ohlcv(batch[i : i + 1000])
    return written


async def main() -> None:
    """Загружает историю для всех символов/ТФ из конфига."""
    args = parse_args()
    cfg = load_config("symbols")
    symbols = (args.symbols.split(",") if args.symbols else cfg["symbols"])
    timeframes = (
        args.timeframes.split(",") if args.timeframes
        else [cfg["timeframes"]["base"]] + cfg["timeframes"]["aggregated"]
    )
    days = args.days or cfg["historical"]["days_back"]
    since = datetime.now(tz=timezone.utc) - timedelta(days=days)

    exchange = ccxt.bybit({"enableRateLimit": True})
    db = TimescaleManager()
    try:
        total = 0
        for symbol in symbols:
            for tf in timeframes:
                try:
                    n = await load_symbol_timeframe(exchange, db, symbol, tf, since)
                    total += n
                    logger.info("Loaded %s %s: %d candles", symbol, tf, n)
                except Exception:
                    logger.exception("Failed to load %s %s", symbol, tf)
        logger.info("Historical load complete: %d candles total", total)
    finally:
        await exchange.close()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
