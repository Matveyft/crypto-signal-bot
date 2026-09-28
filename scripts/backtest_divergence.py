"""Бэктест дивергенций (regular/hidden) на истории Bybit — без БД.

Качает H1/D1 через публичный REST (кэш в data/backtest/), ищет дивергенции
features.divergence, симулирует сделки:

    вход   — open следующей H1 после триггер-свечи (закрытие выше/ниже
             хая/лоя предыдущей) в окне entry_window баров от подтверждения;
    стоп   — за пивотом ± atr_mult * ATR;
    тейк   — RR * риск; таймаут — max_hold баров, выход по закрытию.

Ветки:
    D1  regular  — разворот: дивергенция + не глубже 10% от EMA200 D1
                   (по тренду стороны входа) + ADX(D1) < 40;
    D2  hidden   — продолжение тренда D1 (EMA50/EMA200 + цена по нужной
                   стороне EMA50).

Не учитывается (нет истории): CVD/стакан, funding, фильтр времени.
Запуск:
    python -m scripts.backtest_divergence [--days 365] [--rr 2.0]
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import ccxt
import pandas as pd

from features.divergence import Divergence, detect_divergences
from features.technical_indicators import TechnicalFeatures
from data_layer.utils import load_config, setup_logging

logger = setup_logging("backtest_divergence")
CACHE_DIR = os.path.join("data", "backtest")


# --------------------------------------------------------------------- data
def parse_args() -> argparse.Namespace:
    """Парсит аргументы командной строки."""
    parser = argparse.ArgumentParser(description="Divergence backtest")
    parser.add_argument("--days", type=int, default=365, help="Дней истории H1")
    parser.add_argument("--symbols", type=str, default=None,
                        help="Символы через запятую (default: все из symbols.yaml)")
    parser.add_argument("--order", type=int, default=5, help="Полуокно пивота")
    parser.add_argument("--lookback", type=int, default=40,
                        help="Макс баров между пивотами")
    parser.add_argument("--min-gap", type=float, default=3.0,
                        help="Мин зазор RSI между пивотами")
    parser.add_argument("--entry-window", type=int, default=6,
                        help="Баров на ожидание триггера после подтверждения")
    parser.add_argument("--max-hold", type=int, default=48,
                        help="Макс баров удержания (H1)")
    parser.add_argument("--rr", type=float, default=2.0, help="Risk/reward тейка")
    parser.add_argument("--atr-mult", type=float, default=0.5,
                        help="Буфер стопа за пивотом, в ATR")
    parser.add_argument("--refresh", action="store_true",
                        help="Перекачать кэш данных")
    parser.add_argument("--skip-last-days", type=int, default=0,
                        help="Отрезать последние N дней (для out-of-sample)")
    return parser.parse_args()


def fetch_ohlcv(
    exchange: ccxt.bybit, symbol: str, timeframe: str, days: int
) -> pd.DataFrame:
    """Качает OHLCV с пагинацией (по образцу scripts.load_historical).

    Args:
        exchange: ccxt bybit с enableRateLimit.
        symbol: Символ ccxt ('BTC/USDT:USDT').
        timeframe: Таймфрейм ccxt.
        days: Сколько дней назад начать.

    Returns:
        DataFrame [timestamp, open, high, low, close, volume].
    """
    since_ms = int((datetime.now(tz=timezone.utc) - timedelta(days=days))
                   .timestamp() * 1000)
    end_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    rows: list[dict[str, Any]] = []
    cursor = since_ms
    while cursor < end_ms:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=cursor, limit=1000)
        if not ohlcv:
            break
        for ts, o, h, low, c, v in ohlcv:
            if ts < end_ms:
                rows.append({
                    "timestamp": pd.to_datetime(ts, unit="ms", utc=True),
                    "open": float(o), "high": float(h), "low": float(low),
                    "close": float(c), "volume": float(v),
                })
        cursor = ohlcv[-1][0] + 1
        if len(ohlcv) < 1000:
            break
    unique = {r["timestamp"]: r for r in rows}
    return pd.DataFrame(
        sorted(unique.values(), key=lambda r: r["timestamp"])
    ).reset_index(drop=True)


def load_series(
    exchange: ccxt.bybit, symbol: str, days: int, refresh: bool
) -> pd.DataFrame:
    """H1 с индикаторами + вчерашние D1-значения (без внутридневной утечки).

    D1-колонки (ema_50, ema_200, adx) сдвигаются на день назад перед
    merge_asof: в течение дня видны только значения ПОЛНОСТЬЮ закрытого
    дневного бара. D1 качается с запасом 400 дней на прогрев EMA200.

    Args:
        exchange: ccxt bybit.
        symbol: Символ ccxt.
        days: Дней H1-истории.
        refresh: Перекачать кэш.

    Returns:
        H1 DataFrame с индикаторами и колонками d1_ema50, d1_ema200, d1_adx,
        d1_close.
    """
    os.makedirs(CACHE_DIR, exist_ok=True)
    frames: dict[str, pd.DataFrame] = {}
    for tf, lookback_days in (("1h", days + 3), ("1d", days + 400)):
        # Диапазон в имени кэша: разные --days не должны делить файл
        path = os.path.join(
            CACHE_DIR,
            f"{symbol.replace('/', '_')}_{tf}_{lookback_days}d.csv",
        )
        if refresh or not os.path.exists(path):
            df = fetch_ohlcv(exchange, symbol, tf, lookback_days)
            df.to_csv(path, index=False)
            logger.info("Fetched %s %s: %d bars", symbol, tf, len(df))
        else:
            df = pd.read_csv(path, parse_dates=["timestamp"])
        frames[tf] = df

    h1 = TechnicalFeatures.apply_full(frames["1h"].copy())
    d1 = TechnicalFeatures.apply_full(frames["1d"].copy())
    # Только закрытый дневной бар: сдвиг на строку до merge_asof
    d1_prev = d1[["timestamp", "ema_50", "ema_200", "adx", "close"]].copy()
    d1_prev[["ema_50", "ema_200", "adx", "close"]] = \
        d1_prev[["ema_50", "ema_200", "adx", "close"]].shift(1)
    d1_prev = d1_prev.rename(columns={
        "ema_50": "d1_ema50", "ema_200": "d1_ema200",
        "adx": "d1_adx", "close": "d1_close"})
    h1 = pd.merge_asof(
        h1.sort_values("timestamp"), d1_prev.sort_values("timestamp"),
        on="timestamp", direction="backward",
    )
    return h1.reset_index(drop=True)


def trim_last_days(df: pd.DataFrame, days: int) -> pd.DataFrame:
    """Отрезает последние `days` дней (out-of-sample-разбиение).

    Args:
        df: H1 DataFrame с timestamp.
        days: Сколько дней с конца убрать.

    Returns:
        Обрезанный DataFrame.
    """
    if days <= 0 or df.empty:
        return df
    cutoff = df["timestamp"].iloc[-1] - pd.Timedelta(days=days)
    return df[df["timestamp"] <= cutoff].reset_index(drop=True)


# ------------------------------------------------------------------ trading
def _filters_ok(row: pd.Series, div: Divergence) -> str | None:
    """Контекст тренда D1: возвращает ветку ('D1'/'D2') или None.

    Args:
        row: Строка H1 на баре подтверждения (с d1_* колонками).
        div: Дивергенция.

    Returns:
        'D1' — regular с фильтром разворота, 'D2' — hidden в тренде D1.
    """
    d = row.to_dict()
    if any(pd.isna(d.get(c)) for c in ("d1_ema50", "d1_ema200", "d1_adx",
                                       "d1_close")):
        return None
    adx_ok = d["d1_adx"] < 40
    if div.kind == "regular":
        if div.direction == "bullish":
            return "D1" if adx_ok and d["d1_close"] >= d["d1_ema200"] * 0.9 else None
        return "D1" if adx_ok and d["d1_close"] <= d["d1_ema200"] * 1.1 else None
    # hidden: вход только по направлению дневного тренда
    up = d["d1_ema50"] > d["d1_ema200"] and d["d1_close"] > d["d1_ema50"]
    down = d["d1_ema50"] < d["d1_ema200"] and d["d1_close"] < d["d1_ema50"]
    if div.direction == "bullish":
        return "D2" if up else None
    return "D2" if down else None


def run_symbol(h1: pd.DataFrame, symbol: str, args: argparse.Namespace
               ) -> list[dict[str, Any]]:
    """Прогоняет одну ветку истории: дивергенции → триггер → сделка.

    Одна позиция на символ одновременно (busy_until); каждая дивергенция
    используется один раз.

    Args:
        h1: DataFrame из load_series.
        symbol: Символ ccxt.
        args: Параметры CLI.

    Returns:
        Список сделок (dict).
    """
    divs = detect_divergences(
        h1, indicator="rsi", order=args.order, lookback=args.lookback,
        min_gap=args.min_gap,
    )
    closes, opens = h1["close"].to_numpy(), h1["open"].to_numpy()
    highs, lows = h1["high"].to_numpy(), h1["low"].to_numpy()
    atrs = h1["atr"].to_numpy()
    n = len(h1)
    trades: list[dict[str, Any]] = []
    busy_until = -1
    for div in divs:
        c = div.confirmed_at
        if c <= busy_until or c >= n - 2:
            continue
        branch = _filters_ok(h1.iloc[c], div)
        if branch is None:
            continue
        # Триггер: свеча импульса в сторону входа в окне после подтверждения
        e: int | None = None
        for j in range(c + 1, min(c + 1 + args.entry_window, n - 1)):
            if div.direction == "bullish":
                if closes[j] > highs[j - 1] and closes[j] > opens[j]:
                    e = j + 1
                    break
            elif closes[j] < lows[j - 1] and closes[j] < opens[j]:
                e = j + 1
                break
        if e is None:
            continue
        entry = float(opens[e])
        pivot_price = float(lows[div.p2]) if div.direction == "bullish" \
            else float(highs[div.p2])
        buf = args.atr_mult * float(atrs[c])
        if div.direction == "bullish":
            stop = pivot_price - buf
            risk = entry - stop
        else:
            stop = pivot_price + buf
            risk = stop - entry
        if risk <= 0:
            continue
        tp = entry + args.rr * risk if div.direction == "bullish" \
            else entry - args.rr * risk
        # Симуляция выхода: стоп приоритетен при касании обоих в одной свече
        last = min(e + args.max_hold, n - 1)
        r_mult, exit_idx, result = None, last, "timeout"
        for k in range(e, last + 1):
            if div.direction == "bullish":
                if lows[k] <= stop:
                    r_mult, result = -1.0, "sl"
                    break
                if highs[k] >= tp:
                    r_mult, result = args.rr, "tp"
                    break
            else:
                if highs[k] >= stop:
                    r_mult, result = -1.0, "sl"
                    break
                if lows[k] <= tp:
                    r_mult, result = args.rr, "tp"
                    break
        if r_mult is None:
            exit_px = float(closes[last])
            r_mult = ((exit_px - entry) / risk if div.direction == "bullish"
                      else (entry - exit_px) / risk)
        busy_until = exit_idx
        trades.append({
            "symbol": symbol, "branch": branch, "kind": div.kind,
            "direction": div.direction, "strength": round(div.strength, 1),
            "confirmed_at": str(h1["timestamp"].iloc[c]),
            "entry_at": str(h1["timestamp"].iloc[e]),
            "entry": round(entry, 6), "stop": round(stop, 6),
            "tp": round(tp, 6), "bars_held": exit_idx - e + 1,
            "result": result, "r_multiple": round(float(r_mult), 3),
        })
    return trades


# -------------------------------------------------------------------- stats
def print_stats(trades: list[dict[str, Any]], args: argparse.Namespace
                ) -> pd.DataFrame:
    """Печатает сводку по веткам/направлениям и по символам.

    Args:
        trades: Все сделки.
        args: Параметры CLI.

    Returns:
        DataFrame сделок.
    """
    df = pd.DataFrame(trades)
    if df.empty:
        print("Сделок нет — фильтры слишком строгие или данных мало.")
        return df
    print(f"\n{'=' * 72}\nБЭКТЕСТ ДИВЕРГЕНЦИЙ  ({args.days} дней, "
          f"RR={args.rr}, stop=pivot+/-{args.atr_mult}ATR, "
          f"hold<={args.max_hold}h)\n{'=' * 72}")
    for (branch, direction), grp in df.groupby(["branch", "direction"]):
        wins = (grp["r_multiple"] > 0).sum()
        total_r = grp["r_multiple"].sum()
        neg = grp.loc[grp["r_multiple"] < 0, "r_multiple"].sum()
        pf = total_r / abs(neg) if neg < 0 else float("inf")
        print(f"{branch} {direction:8} | сделок {len(grp):3} | "
              f"winrate {wins / len(grp) * 100:5.1f}% | avg R "
              f"{grp['r_multiple'].mean():+.3f} | sum {total_r:+.1f}R | "
              f"PF {pf:.2f}")
    print("-" * 72)
    for symbol, grp in df.groupby("symbol"):
        print(f"{symbol:16} | сделок {len(grp):3} | "
              f"sum {grp['r_multiple'].sum():+.1f}R")
    total_r = df["r_multiple"].sum()
    print("-" * 72)
    print(f"ИТОГО: {len(df)} сделок, {total_r:+.1f}R "
          f"(1R = риск на сделку)")
    print("Не бэктестится (нет истории): CVD/стакан, funding, фильтр времени.")
    return df


def main() -> None:
    """Качает данные, гоняет бэктест, сохраняет сделки в CSV."""
    # Windows-консоль cp1251 не знает юникодных символов отчёта
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    args = parse_args()
    cfg = load_config("symbols")
    symbols = (args.symbols.split(",") if args.symbols else cfg["symbols"])
    exchange = ccxt.bybit({"enableRateLimit": True})
    all_trades: list[dict[str, Any]] = []
    try:
        for symbol in symbols:
            h1 = load_series(exchange, symbol, args.days, args.refresh)
            h1 = trim_last_days(h1, args.skip_last_days)
            logger.info("%s: %d H1 bars, indicators ready", symbol, len(h1))
            trades = run_symbol(h1, symbol, args)
            all_trades.extend(trades)
            logger.info("%s: %d trades", symbol, len(trades))
    finally:
        exchange.close()
    df = print_stats(all_trades, args)
    if not df.empty:
        out = os.path.join(CACHE_DIR, "trades_divergence.csv")
        df.to_csv(out, index=False)
        print(f"\nСделки: {out}")


if __name__ == "__main__":
    main()
