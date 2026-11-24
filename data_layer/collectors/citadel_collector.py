"""Citadel collector: опрос архива cryptoangel.trade -> Redis + теневые сигналы.

Забирает четыре ленты стороннего терминала Citadel (config/citadel.yaml):
    /api/alerts  — алерты «Тотал Анализ»: имбаланс стакана BTC. Это состояние,
                   а не событие: пока перевес держится, повторяется ~раз в час
    /api/trdr    — метки TRDR по BTC (DIAMOND, Double Diamond, ...)
    /api/marks   — последний каскад или ловушка по монете (считает сервер)
    /api/ob_now  — агрегированный стакан ~15 бирж: глубина и стены

Redis (ключи citadel:*):
    alerts               последние алерты (до 50)          TTL 1 ч
    trdr                 последние метки TRDR (до 50)      TTL 1 ч
    mark:{symbol}:{tf}   последний каскад/ловушка          TTL 6 ч
    book:{symbol}        агрегированный стакан             TTL 2 мин
    status               состояние лент и покрытие монет  TTL 15 мин
    cursor               курсор инкрементальных лент       без TTL

Citadel знает не все монеты бота: чего он не отдаёт, того просто нет в Redis
(см. status.uncovered) — остальное работает как раньше.

Теневые сигналы: каждое событие записывается в shadow_signals с входом,
лимиткой, стопом и целями, посчитанными правилами бота на момент обнаружения.
Без строки в positions, без cooldown и без Telegram — живые сетапы не
затрагиваются. Отчёт: python -m scripts.analyze_shadow

Кандидат — только свежее событие (candidate_max_delay_min). Алерты за lookback
при первом запуске и всё, что пришло с опозданием, пишется без уровней: цена
на момент такого события уже неизвестна.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import OrderedDict, deque
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import aiohttp

from data_layer.citadel import (
    CitadelClient,
    CitadelError,
    parse_alert,
    parse_book,
    parse_marks,
    parse_trdr,
)
from data_layer.collectors.base_collector import BaseCollector
from data_layer.storage.redis_cache import RedisCache
from data_layer.storage.timescale_manager import TimescaleManager
from data_layer.utils import TIMEFRAME_MINUTES, load_config, symbol_to_bybit
from features.technical_indicators import TechnicalFeatures
from strategy.risk_manager import RiskManager

SOURCE = "citadel"
STRATEGY_PREFIX = "cdl_"
FEEDS = ("alerts", "trdr", "marks", "book")
ATR_BARS = 250            # столько же H1-свечей видит стратегия (BARS_NEEDED)
MIN_ATR_BARS = 20         # меньше — ATR не считаем
SEEN_LIMIT = 5000         # сколько ключей событий помнить в памяти
PENDING_LIMIT = 2000      # сколько строк ждать записи в БД при её сбое
RECENT_LIMIT = 50
TTL_LIST_SEC = 3600
TTL_MARK_SEC = 6 * 3600
TTL_BOOK_SEC = 120
TTL_STATUS_SEC = 900
CLOCK_SKEW_MS = 5 * 60_000


class _Batch:
    """События одной ленты за один опрос.

    Фиксируется в памяти (ключи, курсор, время последнего события вида) только
    после успешной записи в БД — сбой записи не должен терять события.
    """

    __slots__ = ("feed", "keys", "rows", "last", "cursors")

    def __init__(self, feed: str) -> None:
        self.feed = feed
        self.keys: list[Any] = []
        self.rows: list[dict[str, Any]] = []
        self.last: dict[tuple[str, str], int] = {}
        self.cursors: list[tuple[str, int]] = []


def _event_key(mark_id: Any, time_ms: int, kind: str) -> str:
    """Ключ дедупликации алерта/метки: id, а если его нет — время и вид."""
    return f"{kind}:{mark_id}" if mark_id is not None else f"{kind}:{time_ms}"


class CitadelCollector(BaseCollector):
    """Опрос архива Citadel: Redis-снапшоты и теневые сигналы."""

    name = "citadel"

    def __init__(
        self,
        symbols: list[str],
        db: TimescaleManager,
        cache: RedisCache,
        config: dict[str, Any] | None = None,
        strategy_params: dict[str, Any] | None = None,
    ) -> None:
        """Инициализирует коллектор.

        Args:
            symbols: Символы бота в формате ccxt.
            db: Менеджер TimescaleDB.
            cache: Redis-кэш.
            config: Секция из config/citadel.yaml (None — автозагрузка).
            strategy_params: Конфиг strategy_params.yaml — стопы, цели и
                лимитка считаются теми же правилами, что и живые сетапы.
        """
        cfg = config or load_config("citadel")
        super().__init__(max_backoff_sec=max(60.0, float(cfg.get("max_retry_sec", 900))))
        params = strategy_params if strategy_params is not None \
            else load_config("strategy_params")
        self._symbols = list(symbols)
        self._symbol_set = set(self._symbols)
        self._db = db
        self._cache = cache
        self._base_url = str(cfg.get("base_url", "https://cryptoangel.trade"))
        self._user_agent = str(cfg.get("user_agent", "market-data-client/1.0"))
        self._timeout_sec = float(cfg.get("timeout_sec", 20))
        poll = cfg.get("poll") or {}
        self._intervals: dict[str, float] = {
            "alerts": max(5.0, float(poll.get("alerts_sec", 20))),
            "trdr": max(5.0, float(poll.get("trdr_sec", 30))),
            "marks": max(10.0, float(poll.get("marks_sec", 30))),
            "book": max(10.0, float(poll.get("book_sec", 30))),
        }
        self._book_groups = list(cfg.get("book_groups") or ["ALL", "SPOT", "PERP"])
        self._book_walls = max(1, int(cfg.get("book_walls", 5)))
        self._marks_tfs = [tf for tf in cfg.get("marks_timeframes") or ["15m"]
                           if tf in TIMEFRAME_MINUTES]
        self._marks_days = int(cfg.get("marks_days", 30))
        self._marks_max_age_ms = float(cfg.get("marks_max_age_min", 30)) * 60_000
        self._marks_cfg = quote(
            json.dumps(cfg.get("marks_cfg") or {}, separators=(",", ":")), safe="",
        )
        self._alert_kinds = set(cfg.get("alert_kinds") or [])
        self._trdr_kinds = set(cfg.get("trdr_kinds") or [])
        self._lookback_ms = float(cfg.get("lookback_hours", 24)) * 3600_000
        self._onset_gap_ms = float(cfg.get("onset_gap_hours", 2)) * 3600_000
        # Позже этого срока после события цена на момент события уже не известна:
        # строка пишется в лог без уровней (не кандидат)
        self._max_delay_ms = float(cfg.get("candidate_max_delay_min", 10)) * 60_000

        # Символ Citadel ('BTCUSDT') <-> символ бота ('BTC/USDT:USDT')
        self._ccxt_of = {symbol_to_bybit(s): s for s in self._symbols}
        alert_symbol = cfg.get("alerts_symbol", "BTC/USDT:USDT")
        self._alert_symbol = alert_symbol if alert_symbol in self._symbol_set else None
        self._trdr_symbols = [s for s in cfg.get("trdr_symbols") or ["BTCUSDT"]
                              if s in self._ccxt_of]
        self._feed_symbols = list(self._ccxt_of)

        self._risk = RiskManager(params.get("risk_management"))
        self._limit_offset = float(
            params.get("signal", {}).get("limit_offset_pct", 0.15)
        ) / 100

        self._client: CitadelClient | None = None
        self._cursor: dict[str, int] = {}
        self._seen: dict[str, OrderedDict] = {feed: OrderedDict() for feed in FEEDS}
        self._last: dict[tuple[str, str], int] = {}
        self._pending: OrderedDict[tuple, dict[str, Any]] = OrderedDict()
        self._recent: dict[str, deque] = {
            "alerts": deque(maxlen=RECENT_LIMIT),
            "trdr": deque(maxlen=RECENT_LIMIT),
        }
        self._feeds: dict[str, dict[str, Any]] = {
            feed: {"ok": None, "at": 0.0, "fails": 0, "error": ""}
            for feed in FEEDS
        }
        self._covered: dict[str, set[str]] = {"marks": set(), "book": set()}
        self._covered_known = False
        self.stats["shadow_rows"] = 0

    # ------------------------------------------------------------------ run
    async def _run(self) -> None:
        """Открывает HTTP-сессию и запускает по циклу на каждую ленту."""
        timeout = aiohttp.ClientTimeout(total=self._timeout_sec, connect=10)
        headers = {"User-Agent": self._user_agent, "Accept": "application/json"}
        feeds = self._active_feeds()
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            self._client = CitadelClient(self._base_url, session)
            await self._restore()
            if not feeds:
                self.logger.warning("Citadel: нет монет для опроса, коллектор простаивает")
                await self._stop_event.wait()
                return
            self.logger.info("Citadel: опрос %s по %d монетам бота",
                             ", ".join(feeds), len(self._symbols))
            await asyncio.gather(*(self._poll_loop(name) for name in feeds))

    def _active_feeds(self) -> list[str]:
        """Ленты, для которых у бота есть подходящие монеты."""
        feeds: list[str] = []
        if self._alert_symbol is not None:
            feeds.append("alerts")
        if self._trdr_symbols:
            feeds.append("trdr")
        if self._feed_symbols:
            feeds += ["marks", "book"]
        return feeds

    async def _poll_loop(self, name: str) -> None:
        """Опрашивает одну ленту; сбой — пауза с ростом до max_retry_sec."""
        poll = getattr(self, f"_poll_{name}")
        interval = self._intervals[name]
        delay, fails = interval, 0
        while not self._stop_event.is_set():
            try:
                await poll()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                fails += 1
                delay = min(interval * (2 ** min(fails, 16)), self._max_backoff_sec)
                self._feeds[name].update(
                    ok=False, fails=fails, error=f"{type(exc).__name__}: {exc}"[:200])
                if fails <= 3 or fails % 20 == 0:
                    self.logger.warning("Citadel %s: сбой №%d, повтор через %.0f с: %s",
                                        name, fails, delay, exc)
            else:
                if fails:
                    self.logger.info("Citadel %s: ответ восстановился после %d сбоев",
                                     name, fails)
                fails, delay = 0, interval
                self._feeds[name].update(ok=True, fails=0, error="", at=time.time())
                self.stats["messages_processed"] += 1
                self.stats["last_message_at"] = time.time()
            await self._publish_status()
            await self._sleep(delay)

    # ---------------------------------------------------------------- feeds
    async def _poll_alerts(self) -> None:
        """Новые алерты «Тотал Анализ» (только BTC)."""
        client = self._need_client()
        since = self._cursor["alerts"]
        data = await client.get_json(f"/api/alerts?from={since}")
        rows = data.get("rows") if isinstance(data, dict) else None
        if not isinstance(rows, list):
            raise CitadelError("alerts: нет списка rows")
        events: list[dict[str, Any]] = []
        cursor = since
        for raw in rows:
            parsed = parse_alert(raw)
            if parsed is None:
                continue
            cursor = max(cursor, parsed["time_ms"])
            kind = parsed["kind"]
            if parsed["side"] is None or not self._allowed(self._alert_kinds, kind):
                continue
            events.append({
                "key": _event_key(parsed["id"], parsed["time_ms"], kind),
                "strategy": f"{STRATEGY_PREFIX}alert_{kind}",
                "symbol": self._alert_symbol,
                "side": parsed["side"],
                "timeframe": "",
                "time": parsed["time"],
                "time_ms": parsed["time_ms"],
                "since_ms": parsed["time_ms"],
                "strength": parsed["power"],
                "price": None,
                "payload": {"kind": kind, "id": parsed["id"]},
            })
        events.sort(key=lambda e: e["time_ms"])
        batch = await self._prepare("alerts", events)
        batch.cursors.append(("alerts", cursor))
        await self._commit(batch)
        for row in batch.rows:
            self._recent["alerts"].append({
                "t": int(row["event_time"].timestamp() * 1000),
                "kind": row["payload"]["kind"],
                "side": row["side"],
            })
        await self._cache.set_citadel("cursor", self._cursor, None)
        await self._cache.set_citadel(
            "alerts",
            {"t": int(time.time() * 1000), "rows": list(self._recent["alerts"])},
            TTL_LIST_SEC,
        )

    async def _poll_trdr(self) -> None:
        """Новые метки TRDR по биткоину."""
        client = self._need_client()
        events: list[dict[str, Any]] = []
        cursors: list[tuple[str, int]] = []
        for citadel_symbol in self._trdr_symbols:
            cursor_key = f"trdr:{citadel_symbol}"
            since = self._cursor[cursor_key]
            data = await client.get_json(
                f"/api/trdr?symbol={citadel_symbol}&from={since}")
            rows = data.get("rows") if isinstance(data, dict) else None
            if not isinstance(rows, list):
                raise CitadelError("trdr: нет списка rows")
            cursor = since
            for raw in rows:
                parsed = parse_trdr(raw)
                if parsed is None:
                    continue
                cursor = max(cursor, parsed["time_ms"])
                kind = parsed["kind"]
                if parsed["side"] is None or not self._allowed(self._trdr_kinds, kind):
                    continue
                events.append({
                    "key": _event_key(parsed["id"], parsed["time_ms"], kind),
                    "strategy": f"{STRATEGY_PREFIX}trdr_{kind}",
                    "symbol": self._ccxt_of[citadel_symbol],
                    "side": parsed["side"],
                    "timeframe": "",
                    "time": parsed["time"],
                    "time_ms": parsed["time_ms"],
                    "since_ms": parsed["time_ms"],
                    "strength": None,
                    "price": None,
                    "payload": {"kind": kind, "id": parsed["id"]},
                })
            cursors.append((cursor_key, cursor))
        events.sort(key=lambda e: e["time_ms"])
        batch = await self._prepare("trdr", events)
        batch.cursors = cursors
        await self._commit(batch)
        for row in batch.rows:
            self._recent["trdr"].append({
                "t": int(row["event_time"].timestamp() * 1000),
                "kind": row["payload"]["kind"],
                "side": row["side"],
            })
        await self._cache.set_citadel("cursor", self._cursor, None)
        await self._cache.set_citadel(
            "trdr",
            {"t": int(time.time() * 1000), "rows": list(self._recent["trdr"])},
            TTL_LIST_SEC,
        )

    async def _poll_marks(self) -> None:
        """Последний каскад или ловушка по монетам; свежие — кандидаты."""
        client = self._need_client()
        now_ms = int(time.time() * 1000)
        covered: set[str] = set()
        events: list[dict[str, Any]] = []
        symbols_param = ",".join(self._feed_symbols)
        for tf in self._marks_tfs:
            tf_ms = TIMEFRAME_MINUTES[tf] * 60_000
            data = await client.get_json(
                f"/api/marks?symbols={symbols_param}&tf={tf}"
                f"&days={self._marks_days}&cfg={self._marks_cfg}"
            )
            for citadel_symbol, mark in parse_marks(data).items():
                symbol = self._ccxt_of.get(citadel_symbol)
                if symbol is None:
                    continue
                # ok=False — Citadel следит за ценой монеты, но событий по ней
                # не считает (монеты нет в его списке): не считаем покрытой
                if mark["ok"]:
                    covered.add(symbol)
                ev = mark["event"]
                event_json = None
                if ev is not None:
                    event_json = {
                        "type": ev["type"],
                        "dir": ev["dir"],
                        "t": ev["time_ms"],
                        "price": ev["price"],
                        "oi": ev["oi"],
                        "thr": ev["thr"],
                        "conf": ev["conf"],
                        "spot_ok": ev["spot_ok"],
                        "fut_ok": ev["fut_ok"],
                    }
                await self._cache.set_citadel(f"mark:{symbol}:{tf}", {
                    "tf": tf,
                    "price": mark["price"],
                    "change_24h": mark["change_24h"],
                    "ok": mark["ok"],
                    "event": event_json,
                }, TTL_MARK_SEC)
                # Кандидат — только свежее событие: старое цена уже отработала
                if ev is None or ev["type"] is None or not mark["ok"]:
                    continue
                age_ms = now_ms - (ev["time_ms"] + tf_ms)   # от закрытия свечи события
                if not -CLOCK_SKEW_MS <= age_ms <= self._marks_max_age_ms:
                    continue
                strength = abs(ev["oi"] / ev["thr"]) if ev["oi"] and ev["thr"] else None
                events.append({
                    "key": f"{symbol}:{tf}:{ev['type']}:{ev['dir']}:{ev['time_ms']}",
                    "strategy": f"{STRATEGY_PREFIX}{'cascade' if ev['type'] == 'cas' else 'trap'}",
                    "symbol": symbol,
                    "side": "LONG" if ev["dir"] > 0 else "SHORT",
                    "timeframe": tf,
                    "time": ev["time"],
                    "time_ms": ev["time_ms"],
                    "since_ms": ev["time_ms"] + tf_ms,     # событие известно с закрытия свечи
                    "strength": strength,
                    "price": ev["price"],
                    "payload": event_json,
                })
        events.sort(key=lambda e: e["time_ms"])
        self._covered["marks"] = covered
        self._covered_known = True
        await self._commit(await self._prepare("marks", events))

    async def _poll_book(self) -> None:
        """Агрегированный стакан: глубина по группам рынков и стены."""
        client = self._need_client()
        data = await client.get_json(
            f"/api/ob_now?symbols={','.join(self._feed_symbols)}"
            f"&g={','.join(self._book_groups)}&rmax={self._book_walls + 3}"
        )
        coins = data.get("coins") if isinstance(data, dict) else None
        if not isinstance(coins, dict):
            raise CitadelError("ob_now: нет coins")
        raw_groups = data.get("dhg")
        groups = [g for g in raw_groups if isinstance(g, str)] \
            if isinstance(raw_groups, list) else self._book_groups
        now_ms = int(time.time() * 1000)
        covered: set[str] = set()
        for citadel_symbol, coin in coins.items():
            symbol = self._ccxt_of.get(citadel_symbol)
            if symbol is None:
                continue
            snap = parse_book(coin, groups, self._book_walls, now_ms)
            if snap is None:
                continue
            covered.add(symbol)
            await self._cache.set_citadel(f"book:{symbol}", snap, TTL_BOOK_SEC)
        self._covered["book"] = covered
        self._covered_known = True

    # ------------------------------------------------------------- staging
    async def _prepare(
        self, feed: str, events: list[dict[str, Any]],
    ) -> _Batch:
        """Отбирает новые события и превращает их в строки для БД.

        Args:
            feed: Имя ленты (своя история дедупликации).
            events: События по возрастанию времени.

        Returns:
            Пакет: ключи, строки и что зафиксировать после записи.
        """
        batch = _Batch(feed)
        seen = self._seen[feed]
        for ev in events:
            if ev["key"] in seen or ev["key"] in batch.keys:
                continue
            slot = (ev["strategy"], ev["symbol"])
            last = batch.last.get(slot, self._last.get(slot))
            onset = last is None or ev["time_ms"] - last >= self._onset_gap_ms
            batch.last[slot] = ev["time_ms"]
            batch.keys.append(ev["key"])
            batch.rows.append(await self._row(ev, onset))
        return batch

    async def _commit(self, batch: _Batch) -> None:
        """Пишет пакет в БД и только затем фиксирует его в памяти.

        Raises:
            Исключение БД: пакет останется непрошедшим и вернётся на следующем
            опросе (строки — в очереди на повторную запись).
        """
        rows = list(self._pending.values()) + batch.rows
        if rows:
            try:
                inserted = await self._db.insert_shadow_signals(rows)
            except Exception:
                for row in batch.rows:
                    self._remember_pending(row)
                raise
            self._pending.clear()
            self.stats["shadow_rows"] += inserted
        seen = self._seen[batch.feed]
        for key in batch.keys:
            seen[key] = None
        while len(seen) > SEEN_LIMIT:
            seen.popitem(last=False)
        self._last.update(batch.last)
        for cursor_key, value in batch.cursors:
            self._cursor[cursor_key] = value

    def _remember_pending(self, row: dict[str, Any]) -> None:
        """Строка, не записавшаяся в БД: повторить на следующем опросе."""
        key = (row["strategy"], row["symbol"], row["timeframe"],
               round(row["event_time"].timestamp() * 1000))
        self._pending[key] = row
        while len(self._pending) > PENDING_LIMIT:
            self._pending.popitem(last=False)

    async def _row(self, ev: dict[str, Any], onset: bool) -> dict[str, Any]:
        """Событие -> строка shadow_signals (уровни — по правилам бота).

        Уровни считаются только для свежих событий: у замеченного с опозданием
        цена на момент события уже неизвестна, и такой «кандидат» был бы
        выдумкой — строка остаётся как запись о событии, без уровней.
        """
        detected_at = datetime.now(tz=timezone.utc)
        delay_ms = detected_at.timestamp() * 1000 - ev["since_ms"]
        fresh = -CLOCK_SKEW_MS <= delay_ms <= self._max_delay_ms
        levels = None
        if fresh:
            try:
                levels = await self._levels(ev["symbol"], ev["side"], ev.get("price"))
            except Exception:
                self.logger.exception(
                    "Citadel: цена/ATR не получены для %s %s", ev["symbol"], ev["strategy"])
        return {
            "source": SOURCE,
            "strategy": ev["strategy"],
            "symbol": ev["symbol"],
            "timeframe": ev["timeframe"],
            "side": ev["side"],
            "event_time": ev["time"],
            "detected_at": detected_at,
            "onset": onset,
            "entry": levels["entry"] if levels else None,
            "limit_price": levels["limit_price"] if levels else None,
            "stop": levels["stop"] if levels else None,
            "target1": levels["target1"] if levels else None,
            "target2": levels["target2"] if levels else None,
            "atr": levels["atr"] if levels else None,
            "strength": ev["strength"],
            "payload": ev["payload"],
        }

    async def _levels(
        self, symbol: str, side: str, fallback_price: float | None,
    ) -> dict[str, float] | None:
        """Вход, лимитка, стоп и цели на момент обнаружения.

        Цена — живая с Bybit (та же, что видит стратегия); ATR — по H1-свечам
        бота за столько же баров, сколько берёт стратегия.

        Args:
            symbol: Символ в формате ccxt.
            side: 'LONG' | 'SHORT'.
            fallback_price: Цена источника, если своих данных нет.

        Returns:
            dict с entry, limit_price, stop, target1, target2, atr — или None,
            если цены или ATR нет.
        """
        price = fallback_price
        live = await self._cache.get_candle(symbol, "1m")
        if isinstance(live, dict) and live.get("close"):
            try:
                price = float(live["close"])
            except (TypeError, ValueError):
                price = fallback_price
        if not price:
            df = await self._db.fetch_ohlcv(symbol, "1m", limit=1)
            if df is not None and not df.empty:
                price = float(df["close"].iloc[-1])
        if not price or price <= 0:
            return None
        df = await self._db.fetch_ohlcv(symbol, "1h", limit=ATR_BARS)
        if df is None or len(df) < MIN_ATR_BARS:
            return None
        atr = float(TechnicalFeatures.calculate_atr(df)["atr"].iloc[-1])
        if not atr > 0:
            return None
        stop = self._risk.calculate_stop_loss(price, atr, side)
        target1, target2 = self._risk.calculate_take_profit(price, stop, side)
        offset = self._limit_offset
        limit = price * (1 - offset) if side == "LONG" else price * (1 + offset)
        return {
            "entry": price,
            "limit_price": round(limit, 8),
            "stop": stop,
            "target1": target1,
            "target2": target2,
            "atr": atr,
        }

    def _need_client(self) -> CitadelClient:
        """HTTP-клиент (сессия открывается в _run)."""
        if self._client is None:
            raise CitadelError("HTTP-сессия ещё не открыта")
        return self._client

    # --------------------------------------------------------------- redis
    async def _restore(self) -> None:
        """Читает курсоры из Redis (продолжение после перезапуска)."""
        now_ms = int(time.time() * 1000)
        default = now_ms - int(self._lookback_ms)
        self._cursor = {"alerts": default}
        for citadel_symbol in self._trdr_symbols:
            self._cursor[f"trdr:{citadel_symbol}"] = default
        try:
            saved = await self._cache.get_citadel("cursor")
        except Exception:
            self.logger.warning("Citadel: курсор из Redis не прочитан", exc_info=True)
            return
        if not isinstance(saved, dict):
            return
        newest = now_ms + 3_600_000
        for key, value in saved.items():
            if (key in self._cursor and isinstance(value, int)
                    and not isinstance(value, bool) and default <= value <= newest):
                self._cursor[key] = value

    async def _publish_status(self) -> None:
        """Ключ citadel:status — состояние лент и покрытие монет."""
        covered = self._covered["marks"] | self._covered["book"]
        known = self._covered_known
        status = {
            "t": int(time.time() * 1000),
            "feeds": {name: dict(state) for name, state in self._feeds.items()},
            "covered": sorted(covered) if known else None,
            "uncovered": sorted(self._symbol_set - covered) if known else None,
        }
        try:
            await self._cache.set_citadel("status", status, TTL_STATUS_SEC)
        except Exception:
            self.logger.debug("Citadel: status not saved", exc_info=True)

    @staticmethod
    def _allowed(kinds: set[str], kind: str) -> bool:
        """Пустой список видов в конфиге = разрешены все."""
        return not kinds or kind in kinds
