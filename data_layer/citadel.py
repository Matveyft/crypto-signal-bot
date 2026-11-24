"""Клиент архива Citadel (cryptoangel.trade): HTTP и разбор ответов.

Источник — сторонний терминал «Citadel — Inst FA Live». Его архив открыт без
ключей; форматы ответов повторяют запросы самой страницы терминала (один
HTML-файл: pollAlerts / pollTrdr / marksPoll / hubObNow). Формат может
измениться в любой момент, поэтому разбор проверяет типы и пропускает то,
чего не понял, вместо исключения.
"""

from __future__ import annotations

import asyncio
import json
import math
from datetime import datetime, timezone
from typing import Any, Sequence

import aiohttp

# Глубины стакана в ответе архива: % от цены; объём — от цены до глубины.
DEPTHS: tuple[str, ...] = (
    "0.5", "1", "2", "2.5", "3", "5", "10", "15", "25", "60", "100",
)
# Семейство алерта -> сторона сделки. «Истощение» (ex-*) — продавцы
# выдохлись, терминал рисует его со стороны лонга.
ALERT_SIDES: dict[str, str] = {"bid": "LONG", "ask": "SHORT", "ex": "LONG"}
# Сила алерта по виду (как в терминале: 0 — средняя, 3 — максимальная).
ALERT_POWER: dict[str, int] = {
    "bid-8-30": 0, "bid-3-15": 1, "bid-3-30": 2,
    "ask-8-30": 0, "ask-3-15": 1, "ask-3-30": 2,
    "ex-50": 0, "ex-40": 1, "ex-30": 2, "ex-20": 3,
}
TRDR_SIDES: dict[int, str] = {1: "LONG", -1: "SHORT"}


class CitadelError(Exception):
    """Архив не ответил или ответ не удалось разобрать."""


def _int(value: Any) -> int | None:
    """Целое из JSON (bool не считается числом)."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def _num(value: Any) -> float | None:
    """Конечное число из JSON (bool не считается числом)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def _ms_to_dt(ms: int) -> datetime:
    """Миллисекунды эпохи -> aware datetime в UTC."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


class CitadelClient:
    """Тонкая обёртка: GET по пути архива и JSON в ответ."""

    def __init__(self, base_url: str, session: aiohttp.ClientSession) -> None:
        """Создаёт клиента поверх готовой сессии.

        Args:
            base_url: Корень архива, например 'https://cryptoangel.trade'.
            session: aiohttp-сессия (таймауты и заголовки — на ней).
        """
        self._base = base_url.rstrip("/")
        self._session = session

    def url(self, path: str) -> str:
        """Полный адрес запроса (для логов и тестов)."""
        return f"{self._base}{path}"

    async def get_json(self, path: str) -> Any:
        """Запрашивает путь и возвращает разобранный JSON.

        Args:
            path: Путь с параметрами, начиная с '/'.

        Returns:
            Ответ архива (dict / list).

        Raises:
            CitadelError: Сеть, таймаут, статус не 200 или не JSON.
        """
        try:
            async with self._session.get(self.url(path)) as resp:
                if resp.status != 200:
                    raise CitadelError(f"HTTP {resp.status}")
                raw = await resp.read()
        except CitadelError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
            raise CitadelError(f"{type(exc).__name__}: {exc}") from exc
        try:
            return json.loads(raw)
        except ValueError as exc:
            raise CitadelError("ответ не JSON") from exc


def parse_alert(row: Any) -> dict[str, Any] | None:
    """Строка /api/alerts -> событие алерта.

    Args:
        row: [t_ms, kind, id] (id может отсутствовать).

    Returns:
        dict с time, time_ms, kind, id, side (None у неизвестного вида),
        power — или None, если строка битая.
    """
    if not isinstance(row, (list, tuple)) or len(row) < 2:
        return None
    t, kind = _int(row[0]), row[1]
    if t is None or not isinstance(kind, str) or not kind:
        return None
    alert_id = row[2] if len(row) > 2 else None
    return {
        "time": _ms_to_dt(t),
        "time_ms": t,
        "kind": kind,
        "id": alert_id,
        "side": ALERT_SIDES.get(kind.split("-", 1)[0]),
        "power": ALERT_POWER.get(kind),
    }


def parse_trdr(row: Any) -> dict[str, Any] | None:
    """Строка /api/trdr -> метка TRDR.

    Args:
        row: [t_ms, kind, side (+1/-1), id] (id может отсутствовать).

    Returns:
        dict с time, time_ms, kind, id, side (None, если сторона не ±1)
        — или None, если строка битая.
    """
    if not isinstance(row, (list, tuple)) or len(row) < 3:
        return None
    t, kind, raw_side = _int(row[0]), row[1], _int(row[2])
    if t is None or not isinstance(kind, str) or not kind:
        return None
    return {
        "time": _ms_to_dt(t),
        "time_ms": t,
        "kind": kind,
        "id": row[3] if len(row) > 3 else None,
        "side": TRDR_SIDES.get(raw_side),
    }


def parse_marks(data: Any) -> dict[str, dict[str, Any]]:
    """Ответ /api/marks -> последняя метка по каждой монете.

    Args:
        data: Ответ вида {coins: {SYMBOL: {c, c24, ok, ev: {...} | None}}}.

    Returns:
        {SYMBOL: {'price', 'change_24h', 'ok', 'event'}}, где event — None или
        dict с type ('cas' | 'trap'), dir (+1/-1), time, time_ms, price, oi,
        thr, conf, spot_ok, fut_ok.
    """
    coins = data.get("coins") if isinstance(data, dict) else None
    if not isinstance(coins, dict):
        raise CitadelError("marks: нет coins")
    out: dict[str, dict[str, Any]] = {}
    for symbol, mark in coins.items():
        if not isinstance(mark, dict):
            continue
        ev = mark.get("ev")
        event = None
        if isinstance(ev, dict):
            t, direction = _int(ev.get("t")), _int(ev.get("dir"))
            if t is not None and direction in (1, -1):
                event = {
                    "type": ev.get("type") if ev.get("type") in ("cas", "trap") else None,
                    "dir": direction,
                    "time": _ms_to_dt(t),
                    "time_ms": t,
                    "price": _num(ev.get("price")),
                    "oi": _num(ev.get("oi")),
                    "thr": _num(ev.get("thr")),
                    "conf": ev.get("conf") if isinstance(ev.get("conf"), str) else "",
                    "spot_ok": ev.get("spotOk") is True,
                    "fut_ok": ev.get("futOk") is True,
                }
        out[symbol] = {
            "price": _num(mark.get("c")),
            "change_24h": _num(mark.get("c24")),
            "ok": mark.get("ok") is True,
            "event": event,
        }
    return out


def parse_book(
    coin: Any, groups: Sequence[str], top: int, now_ms: int,
) -> dict[str, Any] | None:
    """Монета из /api/ob_now -> снимок агрегированного стакана.

    Args:
        coin: {'dh': [t, mid, bids+asks по 11 глубин на каждую группу...],
               'walls': [[k, t0, src, side, _, price, usd, ..., dist, _, state, _]],
               'ids': [источники]}.
        groups: Порядок групп глубины в dh после mid ('ALL', 'SPOT', 'PERP').
        top: Сколько стен каждой стороны оставить.
        now_ms: Текущее время в мс (возраст снимка).

    Returns:
        dict с t, age_sec, mid, sources, n_walls, depth, ratio, walls —
        или None, если глубины нет.
    """
    if not isinstance(coin, dict):
        return None
    dh = coin.get("dh")
    if not isinstance(dh, list) or len(dh) < 2:
        return None
    t, mid = _int(dh[0]), _num(dh[1])
    if t is None or mid is None:
        return None
    n_dep = len(DEPTHS)
    depth: dict[str, dict[str, dict[str, float | None]]] = {}
    ratio: dict[str, dict[str, float | None]] = {}
    for gi, name in enumerate(groups):
        values = dh[2 + gi] if len(dh) > 2 + gi else None
        if not isinstance(values, list) or len(values) < 2 * n_dep:
            continue
        bids = {d: _num(values[i]) for i, d in enumerate(DEPTHS)}
        asks = {d: _num(values[n_dep + i]) for i, d in enumerate(DEPTHS)}
        depth[name] = {"bids": bids, "asks": asks}
        ratio[name] = {
            d: (bids[d] / asks[d]) if bids[d] and asks[d] else None for d in DEPTHS
        }
    if not depth:
        return None
    # Стены двух родов: сводная по всем биржам (src = ALL, в последнем поле —
    # разбивка по биржам) и стены отдельных бирж. Сводные по SPOT/PERP не
    # берём: это та же цена, что и у ALL.
    group_set = set(groups)
    main_group = "ALL" if "ALL" in group_set else None
    walls: dict[str, list[dict[str, Any]]] = {"bids": [], "asks": []}
    by_source: dict[str, list[dict[str, Any]]] = {"bids": [], "asks": []}
    for w in coin.get("walls") or []:
        if not isinstance(w, (list, tuple)) or len(w) < 12:
            continue
        side, usd, price = _int(w[3]), _num(w[6]), _num(w[5])
        if side not in (1, -1) or not usd or not price:
            continue
        if len(w) > 13 and w[13]:       # снята или съедена
            continue
        first_seen = _int(w[1])
        entry: dict[str, Any] = {
            "price": price,
            "usd": usd,
            "src": w[2] if isinstance(w[2], str) else "",
            "dist_pct": _num(w[11]),
            "life_sec": round((t - first_seen) / 1000) if first_seen is not None else None,
            "parts": {},                  # разбивка по биржам — только у сводной
        }
        if entry["src"] in group_set:
            if main_group is not None and entry["src"] != main_group:
                continue
            entry["parts"] = _wall_parts(w[14] if len(w) > 14 else None)
            target = walls
        else:
            target = by_source
        target["bids" if side == 1 else "asks"].append(entry)
    for book in (walls, by_source):
        for side in book:
            book[side].sort(key=lambda x: -x["usd"])
            book[side] = book[side][:max(0, int(top))]
    return {
        "t": t,
        "age_sec": max(0, round((now_ms - t) / 1000)),
        "mid": mid,
        "sources": len(coin.get("ids") or []),
        "n_walls": len(coin.get("walls") or []),
        "depth": depth,
        "ratio": ratio,
        "walls": walls,
        "by_source": by_source,
    }


def _wall_parts(text: Any, top: int = 6) -> dict[str, float]:
    """Разбивка агрегированной стены по биржам: 'binF:72391213,okF:832972' -> {биржа: $}.

    Args:
        text: Строка разбивки из ответа архива.
        top: Сколько крупнейших бирж оставить.

    Returns:
        dict, упорядоченный по убыванию объёма (пустой, если разбивки нет).
    """
    if not isinstance(text, str) or not text:
        return {}
    parts: list[tuple[str, float]] = []
    for chunk in text.split(","):
        src, _, raw = chunk.partition(":")       # объём приходит строкой
        try:
            usd = float(raw)
        except (TypeError, ValueError):
            continue
        if src and math.isfinite(usd):
            parts.append((src, usd))
    parts.sort(key=lambda x: -x[1])
    return dict(parts[:top])
