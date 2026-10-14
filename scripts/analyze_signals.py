"""Отчёт по качеству сигналов: воронка исходов, винрейты и R-профиль.

Запуск на VPS:
    .venv/bin/python -m scripts.analyze_signals          # агрегаты
    .venv/bin/python -m scripts.analyze_signals --dump   # + все сделки построчно

Считает по таблицам signals + positions: исходы (TP/SL/TTL/open), винрейт
и сумму R по сетапам/символам/сторонам, зависимость исхода от confidence,
долю выходов в безубыток после трейлинга. Виртуальные деньги — из расчёта
риска $25/сделку (0.5% от 5000), без комиссий и фандинга.

Важно: исходный стоп берём из signals.stop — в positions.stop он
перезаписывается на безубыток при активации трейлинга.
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
from collections import defaultdict
from datetime import datetime, timezone

from sqlalchemy import text

from data_layer.storage.timescale_manager import TimescaleManager

RISK_PER_TRADE_USDT = 25.0  # 0.5% от депозита 5000 USDT

QUERY = text(
    "SELECT p.id, p.symbol, p.side, p.limit_price, p.entry, p.stop AS cur_stop,"
    " p.target, p.size, p.trailing_active, p.status, p.opened_at, p.closed_at,"
    " p.exit_price, p.pnl_pct, s.id AS sig_id, s.strategy, s.confidence,"
    " s.stop AS sig_stop, s.reason"
    " FROM positions p LEFT JOIN signals s ON s.id = p.signal_id"
    " ORDER BY p.opened_at"
)


def _fmt(v: float | None, spec: str = ".2f") -> str:
    return "-" if v is None else format(v, spec)


def _hours(td) -> float | None:
    return None if td is None else td.total_seconds() / 3600


def _r_multiple(tr: dict) -> float | None:
    """R исхода относительно исходного стопа из signals."""
    if tr["exit_price"] is None:
        return None
    stop0 = tr["sig_stop"] if tr["sig_stop"] is not None else tr["cur_stop"]
    risk = abs(tr["entry"] - stop0)
    if risk <= 0:
        return None
    if tr["side"] == "LONG":
        return (tr["exit_price"] - tr["entry"]) / risk
    return (tr["entry"] - tr["exit_price"]) / risk


def _planned_rr(tr: dict) -> float | None:
    stop0 = tr["sig_stop"] if tr["sig_stop"] is not None else tr["cur_stop"]
    risk = abs(tr["entry"] - stop0)
    if risk <= 0:
        return None
    if tr["side"] == "LONG":
        return (tr["target"] - tr["entry"]) / risk
    return (tr["entry"] - tr["target"]) / risk


def _agg_block(title: str, groups: dict[str, list[dict]]) -> list[str]:
    """Таблица winrate/R по разрезу (сетап/символ/сторона)."""
    lines = [f"--- {title} ---",
             f"{'группа':<22}{'n':>4}{'fill':>5}{'tp':>4}{'sl':>4}{'win%':>6}"
             f"{'avgR':>7}{'sumR':>7}{'RRпл':>6}{'часы':>6}"]
    for name in sorted(groups):
        rows = groups[name]
        closed = [r for r in rows if r["status"] in ("target", "stopped")]
        tp = sum(1 for r in closed if r["status"] == "target")
        sl = len(closed) - tp
        rs = [r["r"] for r in closed if r["r"] is not None]
        rrs = [r["rr"] for r in rows if r["rr"] is not None]
        holds = [_hours(r["closed_at"] - r["opened_at"]) for r in closed]
        holds = [h for h in holds if h is not None]
        win = f"{tp / len(closed) * 100:.0f}" if closed else "-"
        lines.append(
            f"{name[:22]:<22}{len(rows):>4}{sum(1 for r in rows if r['filled']):>5}"
            f"{tp:>4}{sl:>4}{win:>6}{_fmt(statistics.mean(rs) if rs else None):>7}"
            f"{_fmt(sum(rs) if rs else None):>7}"
            f"{_fmt(statistics.mean(rrs) if rrs else None):>6}"
            f"{_fmt(statistics.median(holds) if holds else None):>6}"
        )
    return lines


def build_report(trades: list[dict]) -> list[str]:
    """Все секции отчёта списком строк."""
    out: list[str] = []
    total = len(trades)
    if not total:
        return ["Позиций в базе нет — анализировать нечего."]

    by: dict[str, dict[str, list[dict]]] = {k: defaultdict(list)
                                            for k in ("setup", "symbol", "side")}
    for tr in trades:
        tr["r"] = _r_multiple(tr)
        tr["rr"] = _planned_rr(tr)
        tr["filled"] = tr["status"] in ("open", "target", "stopped")
        by["setup"][tr["strategy"] or "?"].append(tr)
        by["symbol"][tr["symbol"]].append(tr)
        by["side"][tr["side"]].append(tr)

    dates = [tr["opened_at"] for tr in trades]
    closed = [tr for tr in trades if tr["status"] in ("target", "stopped")]
    tp = [tr for tr in closed if tr["status"] == "target"]
    sl_tr = [tr for tr in closed if tr["status"] == "stopped"]
    be = [tr for tr in sl_tr if tr["r"] is not None and abs(tr["r"]) < 0.05]
    cancelled = [tr for tr in trades if tr["status"] == "cancelled"]
    open_pos = [tr for tr in trades if tr["status"] == "open"]
    pending = [tr for tr in trades if tr["status"] == "pending"]
    rs = [tr["r"] for tr in closed if tr["r"] is not None]
    no_signal = sum(1 for tr in trades if tr["sig_id"] is None)

    out.append("================= ОБЩЕЕ =================")
    out.append(f"позиций: {total} | период {min(dates):%Y-%m-%d} .. {max(dates):%Y-%m-%d}"
               f" | без signals-строки: {no_signal}")
    out.append(f"pending: {len(pending)} | open: {len(open_pos)}"
               f" | cancelled(TTL): {len(cancelled)} | closed: {len(closed)}")
    fill = sum(1 for tr in trades if tr["filled"])
    out.append(f"исполнение лимиток: {fill}/{total} = {fill / total * 100:.0f}%")
    if closed:
        out.append(f"исходы: TP {len(tp)} / SL {len(sl_tr)}"
                   f" -> винрейт {len(tp) / len(closed) * 100:.0f}%")
    else:
        out.append("закрытых исходов нет")
    if sl_tr:
        out.append(f"SL: полный стоп {len(sl_tr) - len(be)} | безубыток после трейлинга {len(be)}")
    if rs:
        out.append(f"R: avg {statistics.mean(rs):+.2f} | медиана {statistics.median(rs):+.2f}"
                   f" | сумма {sum(rs):+.1f}R")
        out.append(f"виртуальный PnL: {sum(rs) * RISK_PER_TRADE_USDT:+.0f} USD"
                   f" (риск ${RISK_PER_TRADE_USDT:.0f}/сделка, без комиссий/фандинга)")
    if no_signal:
        out.append(f"ВНИМАНИЕ: {no_signal} позиций без связи с signals")

    out.append("")
    out += _agg_block("ПО СЕТАПАМ", by["setup"])
    out.append("")
    out += _agg_block("ПО СИМВОЛАМ", by["symbol"])
    out.append("")
    out += _agg_block("LONG vs SHORT", by["side"])

    out.append("")
    out.append("--- CONFIDENCE vs ИСХОД (закрытые сделки) ---")
    out.append(f"{'диапазон':<12}{'n':>4}{'tp':>4}{'sl':>4}{'win%':>6}{'avgR':>7}")
    for lo, hi in [(0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 1.01)]:
        bucket = [tr for tr in closed
                  if tr["confidence"] is not None and lo <= tr["confidence"] < hi]
        if not bucket:
            continue
        btp = sum(1 for tr in bucket if tr["status"] == "target")
        brs = [tr["r"] for tr in bucket if tr["r"] is not None]
        out.append(f"{lo:.2f}-{hi:.2f}{'':<2}{len(bucket):>4}{btp:>4}{len(bucket) - btp:>4}"
                   f"{btp / len(bucket) * 100:>6.0f}"
                   f"{_fmt(statistics.mean(brs) if brs else None):>7}")

    if open_pos:
        out.append("")
        out.append("--- ОТКРЫТЫЕ СЕЙЧАС ---")
        out.append(f"{'id':>5} {'символ':<20}{'side':<7}{'entry':>11}{'stop':>11}"
                   f"{'target':>11}{'trail':>6}{'часов':>7}")
        for tr in open_pos:
            age = _hours(datetime.now(tz=timezone.utc) - tr["opened_at"])
            out.append(f"{tr['id']:>5} {tr['symbol']:<20}{tr['side']:<7}"
                       f"{tr['entry']:>11.6g}{tr['cur_stop']:>11.6g}{tr['target']:>11.6g}"
                       f"{str(tr['trailing_active']):>6}{_fmt(age):>7}")
    return out


def dump_trades(trades: list[dict]) -> list[str]:
    """Построчный дамп всех сделок (TSV)."""
    lines = ["id\tdate\tsymbol\tsetup\tside\tconf\tlimit\tstop0\ttarget"
             "\tstatus\texit\tR\tчасы"]
    for tr in trades:
        stop0 = tr["sig_stop"] if tr["sig_stop"] is not None else tr["cur_stop"]
        hold = _hours(tr["closed_at"] - tr["opened_at"]) if tr["closed_at"] else \
            _hours(datetime.now(tz=timezone.utc) - tr["opened_at"])
        lines.append("\t".join(str(x) for x in [
            tr["id"], tr["opened_at"].strftime("%m-%d %H:%M"), tr["symbol"],
            tr["strategy"] or "?", tr["side"], _fmt(tr["confidence"]),
            _fmt(tr["limit_price"], ".6g"), _fmt(stop0, ".6g"),
            _fmt(tr["target"], ".6g"), tr["status"],
            _fmt(tr["exit_price"], ".6g"), _fmt(tr["r"], ".2f"), _fmt(hold, ".1f"),
        ]))
    return lines


async def main() -> None:
    """Точка входа: выгрузка позиций и печать отчёта."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dump", action="store_true", help="добавить построчный дамп")
    args = parser.parse_args()

    db = TimescaleManager()
    try:
        async with db.engine.begin() as conn:
            trades = [dict(r) for r in (await conn.execute(QUERY)).mappings().all()]
    finally:
        await db.close()

    print("\n".join(build_report(trades)))
    if args.dump:
        print("\n================= СДЕЛКИ =================")
        print("\n".join(dump_trades(trades)))


if __name__ == "__main__":
    asyncio.run(main())
