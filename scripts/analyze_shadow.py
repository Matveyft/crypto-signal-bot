"""Отчёт по теневым сигналам (таблица shadow_signals).

Запуск на VPS:
    .venv/bin/python -m scripts.analyze_shadow             # последние 60 дней
    .venv/bin/python -m scripts.analyze_shadow --days 14 --onset

Два взгляда на одни и те же события:

1. «Механика бота» — тот же replay, что в analyze_quality: лимитка на
   limit_offset_pct лучше цены в момент события, стоп и цели из RiskManager
   (записаны в строке), TTL лимитки как у бота. Исход в R.
2. «Событие» — движение цены после события за 1ч / 4ч / 12ч против
   безусловной базы: те же горизонты на каждой 1m-свечке тех же монет за тот
   же период. База отвечает на вопрос «что будет при входе в случайную
   минуту»: событие имеет смысл только там, где оно лучше базы.

Ничего не отправляет наружу.
"""

from __future__ import annotations

import argparse
import asyncio
import math
import statistics
from bisect import bisect_left
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import numpy as np
from sqlalchemy import text

from data_layer.storage.timescale_manager import TimescaleManager
from data_layer.utils import load_config
from scripts.analyze_quality import replay

HORIZONS_MIN = (60, 240, 720)
MAX_HOLD_MIN = 7 * 24 * 60     # стоп или цель ждём не дольше недели

ROWS_QUERY = text(
    "SELECT id, strategy, symbol, timeframe, side, detected_at, onset,"
    " entry, limit_price, stop, target1, target2"
    " FROM shadow_signals WHERE detected_at >= :since ORDER BY detected_at"
)
CANDLES_QUERY = text(
    "SELECT timestamp, high, low, close FROM ohlcv"
    " WHERE symbol = :sym AND timeframe = '1m' AND timestamp >= :since"
    " ORDER BY timestamp"
)


def _dist(values: list) -> tuple[int, float, float, float] | None:
    """(n, среднее, медиана, доля > 0 в %) или None для пустого набора."""
    vals = [float(v) for v in values if v is not None]
    if not vals:
        return None
    return (len(vals), statistics.fmean(vals), statistics.median(vals),
            sum(1 for v in vals if v > 0) / len(vals) * 100)


def _f(v: float | None, width: int, signed: bool = True, prec: int = 2) -> str:
    """Число по правому краю; None — прочерк той же ширины."""
    if v is None:
        return "-".rjust(width)
    return format(v, f">{'+' if signed else ''}{width}.{prec}f")


def evaluate(
    row: dict, candles: list, times: list | None = None, ttl_min: float = 120,
) -> dict:
    """Одна строка: исход «механикой бота» и доходности на горизонтах.

    Args:
        row: Строка shadow_signals.
        candles: 1m-свечи монеты [(timestamp, high, low, close)] по времени.
        times: Только timestamps тех же свечей (None — собрать из candles).
        ttl_min: Время жизни лимитки бота.

    Returns:
        dict: status ('tp' | 'sl' | 'running' | 'open' | 'expired' | 'no_data'
        | None), r, mfe, mae и 'h60'/'h240'/'h720' — доходность в % в сторону
        сделки (None, если горизонт ещё не прошёл).
    """
    out: dict = {"status": None, "r": None, "mfe": None, "mae": None}
    sign = 1.0 if row["side"] == "LONG" else -1.0
    if times is None:
        times = [c[0] for c in candles]
    pos = bisect_left(times, row["detected_at"])
    entry = row["entry"]
    for h in HORIZONS_MIN:
        j = pos + h - 1
        out[f"h{h}"] = (sign * (candles[j][3] / entry - 1.0) * 100
                        if entry and j < len(candles) else None)
    if not (entry and row["stop"] and row["target2"]):
        return out
    if not candles:
        out["status"] = "no_data"
        return out
    rep = replay({
        "side": row["side"], "limit_price": row["limit_price"], "entry": entry,
        "cur_stop": row["stop"], "target": row["target2"],
        "opened_at": row["detected_at"], "sig_stop": row["stop"],
    }, candles[pos:pos + MAX_HOLD_MIN])
    age_min = (times[-1] - row["detected_at"]).total_seconds() / 60
    if not rep["filled"]:
        out["status"] = "open" if age_min < ttl_min else "expired"
    elif (rep["fill_min"] or 0) > ttl_min:
        out["status"] = "expired"
    else:
        out.update(status=rep["status"], r=rep["r"], mfe=rep["mfe"], mae=rep["mae"])
    return out


def baseline(closes: np.ndarray) -> dict[int, np.ndarray]:
    """Доходность за h минут на каждой 1m-свечке (в %, в сторону лонга).

    Вход — закрытие свечи k, выход — закрытие свечи k + h.

    Args:
        closes: Закрытия 1m-свечей по времени.

    Returns:
        {h: массив доходностей} для каждого горизонта.
    """
    out: dict[int, np.ndarray] = {}
    n = len(closes)
    for h in HORIZONS_MIN:
        out[h] = (closes[h:] / closes[:n - h] - 1.0) * 100 if n > h else np.array([])
    return out


def build_report(
    rows: list[dict], candles: dict[str, list], ttl_min: float,
) -> list[str]:
    """Все секции отчёта списком строк.

    Args:
        rows: Строки shadow_signals.
        candles: 1m-свечи по символу [(timestamp, high, low, close)].
        ttl_min: Время жизни лимитки бота.
    """
    out: list[str] = []
    if not rows:
        return ["Теневых сигналов за период нет."]
    since = min(r["detected_at"] for r in rows)
    until = max(r["detected_at"] for r in rows)
    times = {sym: [c[0] for c in kl] for sym, kl in candles.items()}
    for r in rows:
        kl = candles.get(r["symbol"], [])
        r["ev"] = evaluate(r, kl, times.get(r["symbol"]), ttl_min)

    out.append("================= ТЕНЕВЫЕ СИГНАЛЫ =================")
    out.append(f"событий: {len(rows)} | период {since:%Y-%m-%d %H:%M} .. "
               f"{until:%Y-%m-%d %H:%M} UTC")
    out.append(f"без уровней (не было цены или ATR в момент события): "
               f"{sum(1 for r in rows if not r['stop'])} — в расчётах не участвуют")

    # Кандидаты — строки с уровнями; без уровней остаются только в счётчике выше
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rows:
        if r["stop"]:
            groups[(r["strategy"], r["side"])].append(r)
    keys = sorted(groups)

    # --- 1. Механика бота
    out.append("")
    out.append("========== 1. МЕХАНИКА БОТА (replay по 1m, без трейлинга) ==========")
    out.append("лимитка как у бота, стоп/цели записаны на момент события;"
               f" TTL лимитки {ttl_min:.0f} мин")
    out.append(f"{'сетап':<24}{'сторона':<7}{'n':>4}{'ист':>5}{'ожид':>5}{'TP':>5}{'SL':>5}"
               f"{'игра':>6}{'win%':>6}{'avgR':>7}{'sumR':>8}{'медMFE':>8}{'медMAE':>8}")
    if not keys:
        out.append("кандидатов с уровнями за период нет")
    for key in keys:
        grp = groups[key]
        cnt: dict = defaultdict(int)
        for r in grp:
            cnt[r["ev"]["status"]] += 1
        closed = [r["ev"]["r"] for r in grp if r["ev"]["status"] in ("tp", "sl")]
        wins = sum(1 for x in closed if x > 0)
        mfes = [r["ev"]["mfe"] for r in grp if r["ev"]["mfe"] is not None]
        maes = [r["ev"]["mae"] for r in grp if r["ev"]["mae"] is not None]
        avg_r = statistics.fmean(closed) if closed else None
        out.append(
            f"{key[0][:23]:<24}{key[1]:<7}{len(grp):>4}{cnt['expired']:>5}{cnt['open']:>5}"
            f"{cnt['tp']:>5}{cnt['sl']:>5}{cnt['running']:>6}"
            f"{(wins / len(closed) * 100) if closed else 0:>6.0f}"
            f"{_f(avg_r, 7)}{_f(sum(closed) if closed else None, 8, prec=1)}"
            f"{_f(statistics.median(mfes) if mfes else None, 8, signed=False)}"
            f"{_f(statistics.median(maes) if maes else None, 8, signed=False)}"
        )
    out.append("ист = лимитка не исполнена за TTL, ожид = ждёт исполнения,"
               " игра = исполнена, но ни стопа, ни цели (до 7 суток);"
               " win% и R — по закрытым (TP + SL)")

    # --- 2. Событие против базы
    parts_by_h: dict[int, list[np.ndarray]] = {h: [] for h in HORIZONS_MIN}
    for kl in candles.values():
        if not kl:
            continue
        base = baseline(np.array([x[3] for x in kl], dtype=float))
        for h in HORIZONS_MIN:
            if base[h].size:
                parts_by_h[h].append(base[h])
    pooled = {h: np.concatenate(v) if v else np.array([]) for h, v in parts_by_h.items()}
    out.append("")
    out.append("========== 2. СОБЫТИЕ ПРОТИВ БАЗЫ (доходность в сторону сделки, %) ==========")
    out.append("база — вход в случайную минуту в ту же сторону, те же монеты и период")
    for side, sign in (("LONG", 1.0), ("SHORT", -1.0)):
        parts = []
        for h in HORIZONS_MIN:
            d = _dist(sign * pooled[h])
            parts.append(f"{h // 60}ч: ср {d[1]:+.3f} мед {d[2]:+.3f} win {d[3]:.0f}%"
                         if d else f"{h // 60}ч: -")
        out.append(f"  {side:<6}" + " | ".join(parts))
    out.append("")
    out.append(f"{'сетап':<24}{'сторона':<7}{'n':>4}  " + "  ".join(
        f"{h // 60}ч: ср / мед / win   − база (t)" for h in HORIZONS_MIN))
    for key in keys:
        grp = groups[key]
        sign = 1.0 if key[1] == "LONG" else -1.0
        parts = []
        for h in HORIZONS_MIN:
            vals = [r["ev"][f"h{h}"] for r in grp if r["ev"][f"h{h}"] is not None]
            d, base = _dist(vals), _dist(sign * pooled[h])
            if not d or not base:
                parts.append(f"{h // 60}ч: недостаточно данных".ljust(34))
                continue
            sd = statistics.stdev(vals) if len(vals) > 1 else 0.0
            diff = d[1] - base[1]
            t = diff / (sd / math.sqrt(d[0])) if sd > 0 else 0.0
            parts.append(f"{d[1]:+.3f} / {d[2]:+.3f} / {d[3]:.0f}%   "
                         f"{diff:+.3f} ({t:+.1f})".rjust(34))
        out.append(f"{key[0][:23]:<24}{key[1]:<7}{len(grp):>4}  " + "  ".join(parts))
    out.append("«− база» — на сколько процентных пунктов событие в среднем лучше"
               " случайной минуты; t ниже 2 — в пределах шума")
    return out


async def main() -> None:
    """Точка входа: строки shadow_signals + 1m-свечи -> отчёт."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=60, help="сколько дней назад брать события")
    parser.add_argument("--onset", action="store_true",
                        help="только первые события серии (столбец onset)")
    args = parser.parse_args()

    ttl_min = float(load_config("strategy_params").get("signal", {}).get(
        "limit_ttl_minutes", 120))
    db = TimescaleManager()
    try:
        async with db.engine.begin() as conn:
            since = datetime.now(tz=timezone.utc) - timedelta(days=args.days)
            rows = [dict(r) for r in (await conn.execute(
                ROWS_QUERY, {"since": since})).mappings().all()]
            candles: dict[str, list] = {}
            for sym in {r["symbol"] for r in rows}:
                candles[sym] = [tuple(x) for x in (await conn.execute(
                    CANDLES_QUERY, {"sym": sym, "since": since})).all()]
    finally:
        await db.close()
    if args.onset:
        rows = [r for r in rows if r["onset"]]
    print("\n".join(build_report(rows, candles, ttl_min)))


if __name__ == "__main__":
    asyncio.run(main())
