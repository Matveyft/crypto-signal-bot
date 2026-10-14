"""Реплей позиций по 1m-свечам: честные исходы, MFE/MAE, лимитки, время.

Запуск на VPS:
    .venv/bin/python -m scripts.analyze_quality

Отличия от живого трекера (run_strategy): исходы определяются по high/low
1m-свечей, а не по последней цене раз в 30 сек; при касании стопа и тейка
внутри одной свечи первым считается СТОП (консервативно). Трейлинг-стоп
НЕ моделируется — чистая логика "исходный стоп vs цель", чтобы увидеть,
как сетапы живут сами по себе. Лимитка исполняется по касанию (как у бота).
"""

from __future__ import annotations

import asyncio
import statistics
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from data_layer.storage.timescale_manager import TimescaleManager

RISK_USDT = 25.0  # 0.5% от 5000

POSI_QUERY = text(
    "SELECT p.id, p.symbol, p.side, p.limit_price, p.entry, p.stop AS cur_stop,"
    " p.target, p.status, p.opened_at, p.closed_at, p.exit_price,"
    " s.strategy, s.confidence, s.stop AS sig_stop"
    " FROM positions p LEFT JOIN signals s ON s.id = p.signal_id"
    " ORDER BY p.opened_at"
)
CANDLES_QUERY = text(
    "SELECT timestamp, high, low FROM ohlcv"
    " WHERE symbol = :sym AND timeframe = '1m' AND timestamp >= :since"
    " ORDER BY timestamp"
)


def _med(vals: list[float]) -> float | None:
    return statistics.median(vals) if vals else None


def _f(v: float | None, spec: str = ".2f") -> str:
    return "-" if v is None else format(v, spec)


def replay(pos: dict, candles: list) -> dict:
    """Прогон одной позиции по свечам: fill -> стоп/тейк, MFE/MAE в R."""
    out: dict = {"filled": False, "status": "no_data", "exit": None, "r": None,
                 "mfe": None, "mae": None, "fill_min": None, "exit_min": None}
    side = pos["side"]
    limit = pos["limit_price"] or pos["entry"]
    stop0 = pos["sig_stop"] if pos["sig_stop"] is not None else pos["cur_stop"]
    target = pos["target"]
    if stop0 is None or target is None or not candles:
        return out
    risk = abs(limit - stop0)
    if risk <= 0:
        return out
    is_long = side == "LONG"
    t0 = pos["opened_at"]

    fill_i = None
    for i, c in enumerate(candles):
        if c[0] < t0:
            continue
        if (is_long and c[2] <= limit) or (not is_long and c[1] >= limit):
            fill_i = i
            break
    if fill_i is None:
        out["status"] = "no_fill"
        return out
    out["filled"] = True
    out["fill_min"] = (candles[fill_i][0] - t0).total_seconds() / 60

    fav = adv = 0.0
    for i in range(fill_i, len(candles)):
        hi, lo = candles[i][1], candles[i][2]
        if is_long:
            fav, adv = max(fav, hi - limit), max(adv, limit - lo)
            if lo <= stop0:              # стоп приоритетен внутри свечи
                out.update(status="sl", exit=stop0, r=-1.0)
                out["exit_min"] = (candles[i][0] - candles[fill_i][0]).total_seconds() / 60
                break
            if hi >= target:
                out.update(status="tp", exit=target,
                           r=(target - limit) / risk)
                out["exit_min"] = (candles[i][0] - candles[fill_i][0]).total_seconds() / 60
                break
        else:
            fav, adv = max(fav, limit - lo), max(adv, hi - limit)
            if hi >= stop0:
                out.update(status="sl", exit=stop0, r=-1.0)
                out["exit_min"] = (candles[i][0] - candles[fill_i][0]).total_seconds() / 60
                break
            if lo <= target:
                out.update(status="tp", exit=target,
                           r=(limit - target) / risk)
                out["exit_min"] = (candles[i][0] - candles[fill_i][0]).total_seconds() / 60
                break
    else:
        out["status"] = "running"
    out["mfe"], out["mae"] = fav / risk, adv / risk
    return out


def missed_winners(pos: dict, candles: list, horizon_min: int = 120) -> bool:
    """TTL-отмена: дошла бы цена до цели за окно жизни + horizon.

    Касание лимитки после отмены не считаем препятствием: бот в момент
    TTL уже выбыл из очереди, сетап для него потерян.
    """
    side, target = pos["side"], pos["target"]
    t0, t1 = pos["opened_at"], (pos["closed_at"] or pos["opened_at"]) + \
        timedelta(minutes=horizon_min)
    win = [c for c in candles if t0 <= c[0] <= t1]
    if not win or target is None:
        return False
    if side == "LONG":
        return max(c[1] for c in win) >= target
    return min(c[2] for c in win) <= target


async def main() -> None:
    """Точка входа."""
    db = TimescaleManager()
    try:
        async with db.engine.begin() as conn:
            posis = [dict(r) for r in (await conn.execute(POSI_QUERY)).mappings().all()]
            since = min(p["opened_at"] for p in posis) - timedelta(hours=2) if posis \
                else datetime.now(tz=timezone.utc)
            candles: dict[str, list] = {}
            for sym in {p["symbol"] for p in posis}:
                rows = (await conn.execute(
                    CANDLES_QUERY, {"sym": sym, "since": since})).all()
                candles[sym] = [(r[0], r[1], r[2]) for r in rows]
    finally:
        await db.close()
    if not posis:
        print("Позиций нет.")
        return

    for p in posis:
        p["rep"] = replay(p, candles.get(p["symbol"], []))

    bot_closed = [p for p in posis if p["status"] in ("target", "stopped")]
    rep_done = [p for p in posis if p["rep"]["status"] in ("tp", "sl")]
    rep_tp = [p for p in rep_done if p["rep"]["status"] == "tp"]
    dis = [p for p in bot_closed if p["rep"]["status"] in ("tp", "sl") and (
        (p["status"] == "target") != (p["rep"]["status"] == "tp"))]
    rs = [p["rep"]["r"] for p in rep_done]

    print("================= REPLAY vs БОТ (по 1m свечам) =================")
    print(f"бот закрыл: {len(bot_closed)} (TP {sum(1 for p in bot_closed if p['status'] == 'target')}"
          f" / SL {sum(1 for p in bot_closed if p['status'] == 'stopped')})")
    print(f"реплей: закрыто {len(rep_done)} (TP {len(rep_tp)} / SL {len(rep_done) - len(rep_tp)}),"
          f" не закрылось {sum(1 for p in posis if p['rep']['status'] == 'running')},"
          f" не исполнилось {sum(1 for p in posis if p['rep']['status'] == 'no_fill')}")
    if dis:
        print(f"РАСХОЖДЕНИЯ исходов (id): {[p['id'] for p in dis]}")
    if rs:
        print(f"R по свечам: avg {statistics.mean(rs):+.2f} | медиана {statistics.median(rs):+.2f}"
              f" | сумма {sum(rs):+.1f}R | виртуально {sum(rs) * RISK_USDT:+.0f} USD")

    filled = [p for p in posis if p["rep"]["filled"]]
    groups: dict[str, list[dict]] = defaultdict(list)
    for p in filled:
        groups[p["strategy"] or "?"].append(p)

    print("")
    print("========== MFE/MAE в R (исполненные, реплей без трейлинга) ==========")
    print(f"{'сетап':<22}{'n':>3}{'медMFE':>8}{'медMAE':>8}{'>=0.5R':>8}"
          f"{'>=1R':>7}{'>=1.5R':>8}{'>=2R':>7}{'медмин':>8}")
    for name, rows in sorted(groups.items()):
        mfes = [p["rep"]["mfe"] for p in rows]
        maes = [p["rep"]["mae"] for p in rows]
        mins = [p["rep"]["exit_min"] for p in rows if p["rep"]["exit_min"] is not None]
        print(f"{name[:22]:<22}{len(rows):>3}{_f(_med(mfes)):>8}{_f(_med(maes)):>8}"
              f"{sum(1 for m in mfes if m >= 0.5) / len(rows) * 100:>7.0f}%"
              f"{sum(1 for m in mfes if m >= 1) / len(rows) * 100:>6.0f}%"
              f"{sum(1 for m in mfes if m >= 1.5) / len(rows) * 100:>7.0f}%"
              f"{sum(1 for m in mfes if m >= 2) / len(rows) * 100:>6.0f}%"
              f"{_f(_med(mins), '.0f'):>8}")

    losers = [p for p in filled if p["rep"]["status"] == "sl"]
    if losers:
        fast = sum(1 for p in losers if (p["rep"]["exit_min"] or 1e9) < 30)
        never = sum(1 for p in losers if (p["rep"]["mfe"] or 0) < 0.3)
        print(f"\nполные стопы: {len(losers)} | из них быстрее 30 мин: {fast}"
              f" | не увидели даже +0.3R: {never}")

    canc = [p for p in posis if p["status"] == "cancelled"]
    if canc:
        win_missed = [p for p in canc if missed_winners(p, candles.get(p["symbol"], []))]
        print(f"\nTTL-отмены: {len(canc)} | упущенные победители (цена дошла до"
              f" цели без нас, +2ч): {len(win_missed)}")

    print("\n================= ЧАСЫ ВХОДА (UTC, реплей) =================")
    print(f"{'час':<8}{'n':>4}{'TP':>4}{'SL':>4}{'win%':>6}{'суммаR':>8}")
    for lo in range(0, 24, 3):
        rows = [p for p in rep_done if lo <= p["opened_at"].hour < lo + 3]
        if not rows:
            continue
        tp_n = sum(1 for p in rows if p["rep"]["status"] == "tp")
        print(f"{lo:02d}-{lo + 3:02d}{'':<4}{len(rows):>4}{tp_n:>4}{len(rows) - tp_n:>4}"
              f"{tp_n / len(rows) * 100:>6.0f}{sum(p['rep']['r'] for p in rows):>8.1f}")


if __name__ == "__main__":
    asyncio.run(main())
