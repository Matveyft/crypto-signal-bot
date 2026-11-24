"""Unit-тесты Citadel: разбор ответов архива, коллектор (Redis + shadow_signals), отчёт."""

import asyncio
import json
import time
from collections import deque
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from data_layer.citadel import (
    DEPTHS,
    CitadelClient,
    CitadelError,
    parse_alert,
    parse_book,
    parse_marks,
    parse_trdr,
)
from data_layer.collectors.citadel_collector import CitadelCollector

UTC = timezone.utc
SYMBOLS = ["BTC/USDT:USDT", "ETH/USDT:USDT"]
PARAMS = {
    "risk_management": {"stop_loss_atr_multiplier": 2.0, "risk_reward_ratio": 1.5},
    "signal": {"limit_offset_pct": 0.15},
}
HOUR_MS = 3_600_000
MIN_MS = 60_000
GROUPS = ["ALL", "SPOT", "PERP"]


def now_ms() -> int:
    return int(time.time() * 1000)


def make_config(**overrides) -> dict:
    """Конфиг коллектора для тестов (минимальные интервалы)."""
    cfg = {
        "base_url": "https://citadel.test",
        "poll": {"alerts_sec": 5, "trdr_sec": 5, "marks_sec": 10, "book_sec": 10},
        "alerts_symbol": "BTC/USDT:USDT",
        "trdr_symbols": ["BTCUSDT"],
        "marks_timeframes": ["15m"],
        "marks_max_age_min": 30,
        "onset_gap_hours": 2,
        "lookback_hours": 24,
    }
    cfg.update(overrides)
    return cfg


def group(bids: list[float], asks: list[float]) -> list[float]:
    """Массив глубины одной группы: 11 бидов, затем 11 асков (недостающее — 0)."""
    pad = len(DEPTHS)
    return list(bids) + [0.0] * (pad - len(bids)) + list(asks) + [0.0] * (pad - len(asks))


def wall(src: str, side: int, price: float, usd: float, state=None, at: int = 0,
         parts: str = "") -> list:
    """Строка стены в формате /api/ob_now (15 полей); стена стоит час.

    parts — разбивка по биржам у агрегированной стены ('binF:500,okF:900').
    """
    return [f"{src}|{side}|1", at - HOUR_MS, src, side, 1, price, usd, usd,
            1, 1, at, 0.5, 1.0, state, parts]


def btc_coin(at: int | None = None) -> dict:
    """Монета из /api/ob_now: 3 группы глубины, агрегированные стены и стены бирж."""
    at = now_ms() if at is None else at
    return {
        "dh": [at, 100.0,
               group([60, 120], [40, 160]),       # ALL: 0.5% и 1%
               group([10, 20], [10, 30]),         # SPOT
               group([50, 100], [30, 130])],      # PERP
        "walls": [
            wall("ALL", 1, 99.0, 1400, at=at, parts="binF:500,okF:900"),    # бид по всем биржам
            wall("ALL", -1, 102.0, 1500, at=at, parts="binF:800,okS:700"),  # аск по всем биржам
            wall("PERP", 1, 99.0, 1390, at=at, parts="binF:490,okF:900"),   # та же цена, только перпы — пропускаем
            wall("binF", 1, 99.0, 500, at=at),            # бид $500
            wall("okF", 1, 98.0, 900, at=at),             # бид $900 — крупнее
            wall("binF", -1, 101.0, 700, at=at),          # аск
            wall("binF", -1, 102.0, 800, at=at),          # аск крупнее
            wall("binF", -1, 103.0, 9999, "eaten", at),   # съедена — не стена
        ],
        "ids": ["binF", "okF"],
        "n": 2,
        "t": at,
    }


def alert_response(*specs) -> dict:
    """Ответ /api/alerts: specs — (минут назад, вид, id)."""
    base = now_ms()
    return {"kinds": [], "rows": [[base - int(m * MIN_MS), kind, mark_id]
                                  for m, kind, mark_id in specs]}


# ------------------------------------------------------------------- fakes
class FakeCache:
    """Мок RedisCache: свечи на выдачу, запись citadel:* в словарь."""

    def __init__(self, candles: dict | None = None) -> None:
        self.candles = candles or {}
        self.stored: dict = {}

    async def get_candle(self, symbol: str, timeframe: str) -> dict | None:
        return self.candles.get((symbol, timeframe))

    async def get_citadel(self, name: str) -> object:
        return self.stored.get(name)

    async def set_citadel(self, name: str, data, ttl_sec=None) -> None:
        self.stored[name] = data


def _same_event(a: dict, b: dict) -> bool:
    return (a["strategy"], a["symbol"], a["timeframe"], a["event_time"]) == \
        (b["strategy"], b["symbol"], b["timeframe"], b["event_time"])


class FakeDb:
    """Мок TimescaleManager: H1-свечи с ATR = 2.0, запись shadow-строк.

    Постоянный ряд (high 101, low 99) даёт TR = 2 на каждой свече, значит
    ATR(Wilder) = 2.0 — при множителе 2.0 стоп лонга равен entry − 4.
    Повтор того же события не записывается (как ON CONFLICT DO NOTHING).
    """

    def __init__(self, bars: int = 250) -> None:
        self.rows: list[dict] = []
        self.fail = False
        self.h1 = pd.DataFrame({
            "timestamp": pd.date_range("2026-11-01", periods=bars, freq="1h", tz=UTC),
            "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1.0,
        })

    async def fetch_ohlcv(self, symbol, timeframe, limit=500, start=None):
        if timeframe == "1h":
            return self.h1.tail(limit).reset_index(drop=True)
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    async def insert_shadow_signals(self, rows: list[dict]) -> int:
        if self.fail:
            raise RuntimeError("db down")
        # как ON CONFLICT DO NOTHING: повторы не пишутся и внутри одного батча
        new: list[dict] = []
        for r in rows:
            if not any(_same_event(r, x) for x in self.rows + new):
                new.append(r)
        self.rows.extend(new)
        return len(new)


class FakeResponse:
    """Мок ответа aiohttp (async context manager)."""

    def __init__(self, payload=None, status: int = 200, raw: bytes | None = None) -> None:
        self.status = status
        self._raw = raw if raw is not None else json.dumps(payload).encode()

    async def read(self) -> bytes:
        return self._raw

    async def __aenter__(self) -> "FakeResponse":
        return self

    async def __aexit__(self, *exc) -> bool:
        return False


class FakeSession:
    """Мок aiohttp-сессии: ответы по порядку для каждого пути.

    Последний ответ очереди повторяется, пока его не заменят. Элемент очереди:
    dict (JSON-ответ), int (HTTP-статус), FakeResponse или исключение.
    """

    def __init__(self) -> None:
        self.routes: dict[str, deque] = {}
        self.calls: list[str] = []

    def on(self, prefix: str, *responses) -> None:
        self.routes[prefix] = deque(responses)

    def get(self, url: str) -> FakeResponse:
        path = url.split(".test", 1)[1]
        self.calls.append(path)
        queue = next((q for p, q in self.routes.items() if path.startswith(p)), None)
        if queue is None:
            return FakeResponse({}, status=404)
        item = queue.popleft() if len(queue) > 1 else queue[0]
        if isinstance(item, Exception):
            raise item
        if isinstance(item, FakeResponse):
            return item
        if isinstance(item, int):
            return FakeResponse({}, status=item)
        return FakeResponse(item)


def make_collector(session: FakeSession, cache=None, db=None, symbols=SYMBOLS,
                   **cfg_overrides) -> CitadelCollector:
    """Коллектор с поддельными сессией, Redis и БД (без _run)."""
    col = CitadelCollector(symbols, db or FakeDb(), cache or FakeCache(),
                           make_config(**cfg_overrides), PARAMS)
    col._client = CitadelClient("https://citadel.test", session)
    return col


# ------------------------------------------------------------ разбор: алерты
class TestParseAlert:
    def test_side_and_power(self) -> None:
        bid = parse_alert([1_700_000_000_000, "bid-8-30", 331])
        ask = parse_alert([1_700_000_000_000, "ask-3-30"])
        exh = parse_alert([1_700_000_000_000, "ex-20"])
        assert bid["side"] == "LONG" and bid["power"] == 0 and bid["id"] == 331
        assert ask["side"] == "SHORT" and ask["power"] == 2 and ask["id"] is None
        assert exh["side"] == "LONG" and exh["power"] == 3
        assert bid["time"].tzinfo is not None
        assert bid["time_ms"] == 1_700_000_000_000

    def test_garbage_rows(self) -> None:
        assert parse_alert(None) is None
        assert parse_alert("bid-8-30") is None
        assert parse_alert(["1700000000000", "bid-8-30"]) is None   # время не число
        assert parse_alert([1_700_000_000_000]) is None             # нет вида
        assert parse_alert([1_700_000_000_000, 42]) is None
        assert parse_alert([True, "bid-8-30"]) is None

    def test_unknown_kind_has_no_side(self) -> None:
        ev = parse_alert([1_700_000_000_000, "new-thing"])
        assert ev is not None
        assert ev["side"] is None and ev["power"] is None


# --------------------------------------------------------------- разбор: TRDR
class TestParseTrdr:
    def test_side_from_row(self) -> None:
        short = parse_trdr([1_700_000_000_000, "diamond", -1, 5165])
        long_ = parse_trdr([1_700_000_000_000, "btcdd", 1])
        assert short["side"] == "SHORT" and short["id"] == 5165
        assert long_["side"] == "LONG"

    def test_garbage(self) -> None:
        assert parse_trdr([1_700_000_000_000, "diamond", 0])["side"] is None
        assert parse_trdr([1_700_000_000_000, "diamond"]) is None
        assert parse_trdr("nope") is None


# -------------------------------------------------------------- разбор: метки
class TestParseMarks:
    def test_events(self) -> None:
        t = now_ms() - 20 * MIN_MS
        marks = parse_marks({"coins": {
            "BTCUSDT": {"c": 59700.5, "c24": 61000.0, "ok": True, "ev": {
                "type": "cas", "dir": -1, "t": t, "price": 63323.7, "oi": -1.25,
                "thr": -1.18, "conf": "S", "spotOk": True, "futOk": False}},
            "ETHUSDT": {"c": 1675.0, "ok": True, "ev": None},
            "XRPUSDT": {"c": 0.92, "ok": False},
        }})
        btc = marks["BTCUSDT"]
        assert btc["ok"] is True and btc["price"] == 59700.5
        ev = btc["event"]
        assert ev["type"] == "cas" and ev["dir"] == -1 and ev["time_ms"] == t
        assert ev["spot_ok"] is True and ev["fut_ok"] is False
        assert marks["ETHUSDT"]["event"] is None
        assert marks["XRPUSDT"]["ok"] is False

    def test_without_coins_raises(self) -> None:
        with pytest.raises(CitadelError):
            parse_marks({"ok": False})
        with pytest.raises(CitadelError):
            parse_marks([])


# ------------------------------------------------------------- разбор: стакан
class TestParseBook:
    def test_depth_and_ratio(self) -> None:
        at = 1_700_000_000_000
        snap = parse_book(btc_coin(at), GROUPS, 5, at)
        assert snap is not None
        assert snap["depth"]["ALL"]["bids"]["0.5"] == 60
        assert snap["depth"]["ALL"]["bids"]["1"] == 120
        assert snap["depth"]["ALL"]["asks"]["1"] == 160
        assert snap["ratio"]["ALL"]["0.5"] == pytest.approx(60 / 40)
        assert snap["ratio"]["SPOT"]["1"] == pytest.approx(20 / 30)
        assert snap["ratio"]["PERP"]["1"] == pytest.approx(100 / 130)
        assert snap["ratio"]["ALL"]["5"] is None             # нулевая сторона — не считаем
        assert snap["sources"] == 2 and snap["age_sec"] == 0 and snap["mid"] == 100.0

    def test_short_depth_group_is_skipped(self) -> None:
        at = 1_700_000_000_000
        coin = btc_coin(at)
        coin["dh"][3] = [1, 2, 3]
        snap = parse_book(coin, GROUPS, 5, at)
        assert snap is not None
        assert set(snap["depth"]) == {"ALL", "PERP"}

    def test_walls_split_by_kind_top_per_side_without_closed(self) -> None:
        at = 1_700_000_000_000
        snap = parse_book(btc_coin(at), GROUPS, 5, at)
        agg, own = snap["walls"], snap["by_source"]
        # агрегированные стены — с разбивкой по биржам, largest first
        assert [(w["src"], w["usd"], w["price"]) for w in agg["bids"]] == [("ALL", 1400, 99.0)]
        assert [(w["src"], w["usd"]) for w in agg["asks"]] == [("ALL", 1500)]
        assert agg["bids"][0]["parts"] == {"okF": 900, "binF": 500}
        assert agg["asks"][0]["parts"] == {"binF": 800, "okS": 700}
        # стены отдельных бирж: съеденная не попала
        assert [(w["src"], w["usd"]) for w in own["bids"]] == [("okF", 900), ("binF", 500)]
        assert [(w["src"], w["usd"]) for w in own["asks"]] == [("binF", 800), ("binF", 700)]
        assert agg["bids"][0]["life_sec"] == 3600 and agg["bids"][0]["dist_pct"] == 0.5
        assert own["bids"][0]["parts"] == {}            # у одной биржи разбивки нет
        top_one = parse_book(btc_coin(at), GROUPS, 1, at)
        assert [len(top_one[k][s]) for k in ("walls", "by_source")
                for s in ("bids", "asks")] == [1, 1, 1, 1]

    def test_no_depth_is_none(self) -> None:
        assert parse_book({"walls": []}, ["ALL"], 5, 0) is None
        assert parse_book({"dh": ["x", 1.0]}, ["ALL"], 5, 0) is None
        assert parse_book({"dh": [1, "mid", [0] * 22]}, ["ALL"], 5, 0) is None
        assert parse_book({"dh": [1, 1.0, [0] * 5]}, ["ALL"], 5, 0) is None
        assert parse_book(None, ["ALL"], 5, 0) is None


# ------------------------------------------------------------------ клиент
class TestCitadelClient:
    @pytest.mark.asyncio
    async def test_returns_json_and_keeps_query(self) -> None:
        session = FakeSession()
        session.on("/api/alerts", {"rows": [[1, "bid-8-30", 5]]})
        client = CitadelClient("https://citadel.test/", session)
        data = await client.get_json("/api/alerts?from=1")
        assert data == {"rows": [[1, "bid-8-30", 5]]}
        assert session.calls == ["/api/alerts?from=1"]

    @pytest.mark.asyncio
    async def test_http_error(self) -> None:
        session = FakeSession()
        session.on("/api/alerts", 404)
        with pytest.raises(CitadelError, match="HTTP 404"):
            await CitadelClient("https://citadel.test", session).get_json("/api/alerts")

    @pytest.mark.asyncio
    async def test_not_json(self) -> None:
        session = FakeSession()
        session.on("/api/trdr", FakeResponse(raw=b"<html>maintenance</html>"))
        with pytest.raises(CitadelError, match="JSON"):
            await CitadelClient("https://citadel.test", session).get_json("/api/trdr")

    @pytest.mark.asyncio
    async def test_network_error(self) -> None:
        session = FakeSession()
        session.on("/api/marks", asyncio.TimeoutError())
        with pytest.raises(CitadelError, match="TimeoutError"):
            await CitadelClient("https://citadel.test", session).get_json("/api/marks")


# --------------------------------------------------------------- коллектор
class TestCollectorAlerts:
    @pytest.mark.asyncio
    async def test_first_poll_writes_candidate_with_bot_levels(self) -> None:
        session, db = FakeSession(), FakeDb()
        cache = FakeCache({("BTC/USDT:USDT", "1m"): {"close": 50000.0}})
        session.on("/api/alerts", alert_response((1, "bid-8-30", 10)))
        col = make_collector(session, cache, db)
        await col._restore()
        await col._poll_alerts()

        assert session.calls[0].startswith("/api/alerts?from=")
        row = db.rows[0]
        assert (row["strategy"], row["symbol"], row["side"]) == \
            ("cdl_alert_bid-8-30", "BTC/USDT:USDT", "LONG")
        assert row["onset"] is True and row["strength"] == 0
        assert row["timeframe"] == "" and row["source"] == "citadel"
        assert row["entry"] == 50000.0
        assert row["atr"] == pytest.approx(2.0)
        assert row["stop"] == pytest.approx(49996.0)          # 2.0 ATR
        assert row["target1"] == pytest.approx(50004.5)       # 0.75 * 1.5 R
        assert row["target2"] == pytest.approx(50006.0)       # 1.5 R
        assert row["limit_price"] == pytest.approx(49925.0)   # 0.15% лучше входа
        assert row["event_time"] < row["detected_at"]
        event_ms = int(row["event_time"].timestamp() * 1000)
        # Redis: список алертов и подвинутый курсор
        assert cache.stored["alerts"]["rows"] == [
            {"t": event_ms, "kind": "bid-8-30", "side": "LONG"}]
        assert cache.stored["cursor"]["alerts"] == event_ms
        assert col.stats["shadow_rows"] == 1

    @pytest.mark.asyncio
    async def test_repeat_poll_does_not_duplicate(self) -> None:
        session, db = FakeSession(), FakeDb()
        response = alert_response((2, "bid-8-30", 10))
        session.on("/api/alerts", response)
        col = make_collector(session, FakeCache(), db)
        await col._restore()
        await col._poll_alerts()
        await col._poll_alerts()                 # сервер повторяет ту же строку
        assert len(db.rows) == 1
        response["rows"].append([now_ms(), "ask-3-15", 11])
        await col._poll_alerts()
        assert [r["strategy"] for r in db.rows] == \
            ["cdl_alert_bid-8-30", "cdl_alert_ask-3-15"]
        assert db.rows[1]["side"] == "SHORT"
        assert col.stats["shadow_rows"] == 2

    @pytest.mark.asyncio
    async def test_onset_marks_first_of_a_series(self) -> None:
        session, db = FakeSession(), FakeDb()
        session.on("/api/alerts", alert_response(
            (300, "bid-8-30", 1), (270, "bid-8-30", 2), (240, "bid-8-30", 3), (60, "bid-8-30", 4),
        ))
        col = make_collector(session, FakeCache(), db)
        await col._restore()
        await col._poll_alerts()
        assert [r["onset"] for r in db.rows] == [True, False, False, True]

    @pytest.mark.asyncio
    async def test_db_failure_keeps_events_for_retry(self) -> None:
        session, db = FakeSession(), FakeDb()
        session.on("/api/alerts", alert_response((3, "bid-3-15", 20), (2, "ask-8-30", 21)))
        col = make_collector(session, FakeCache(), db)
        await col._restore()
        cursor_before = col._cursor["alerts"]
        db.fail = True
        with pytest.raises(RuntimeError):
            await col._poll_alerts()
        # ничего не зафиксировано: события вернутся на следующем опросе
        assert col._cursor["alerts"] == cursor_before
        assert len(col._seen["alerts"]) == 0
        assert len(col._pending) == 2

        db.fail = False
        await col._poll_alerts()
        assert len(db.rows) == 2
        assert len(col._pending) == 0 and len(col._seen["alerts"]) == 2
        assert col._cursor["alerts"] > cursor_before
        assert col.stats["shadow_rows"] == 2

    @pytest.mark.asyncio
    async def test_not_enough_candles_row_has_no_levels(self) -> None:
        session, db = FakeSession(), FakeDb(bars=10)   # меньше 20 свечей — ATR нет
        session.on("/api/alerts", alert_response((1, "bid-8-30", 30)))
        col = make_collector(session, FakeCache(), db)
        await col._restore()
        await col._poll_alerts()
        assert len(db.rows) == 1
        assert db.rows[0]["stop"] is None and db.rows[0]["entry"] is None

    @pytest.mark.asyncio
    async def test_kind_filter_and_unknown_kind(self) -> None:
        session, db = FakeSession(), FakeDb()
        session.on("/api/alerts", alert_response(
            (1, "bid-8-30", 40), (1, "ex-50", 41), (0.5, "new-thing", 42),
        ))
        col = make_collector(session, FakeCache(), db, alert_kinds=["ex-50"])
        await col._restore()
        await col._poll_alerts()
        # записан только разрешённый вид, а курсор ушёл и по пропущенным строкам
        assert [r["strategy"] for r in db.rows] == ["cdl_alert_ex-50"]
        assert col._cursor["alerts"] == pytest.approx(now_ms() - MIN_MS / 2, abs=5000)

    @pytest.mark.asyncio
    async def test_late_event_is_logged_without_levels(self) -> None:
        # алерт за lookback_hours при первом запуске: цена на момент события
        # уже неизвестна — строка остаётся, но кандидатом не становится
        session, db = FakeSession(), FakeDb()
        session.on("/api/alerts", alert_response((30, "bid-8-30", 50), (2, "bid-8-30", 51)))
        cache = FakeCache({("BTC/USDT:USDT", "1m"): {"close": 50000.0}})
        col = make_collector(session, cache, db)
        await col._restore()
        await col._poll_alerts()
        assert [(r["onset"], r["stop"] is None, r["entry"] is None) for r in db.rows] == \
            [(True, True, True), (False, False, False)]
        assert len(cache.stored["alerts"]["rows"]) == 2     # в Redis история есть целиком

    def test_feeds_depend_on_configured_symbols(self) -> None:
        col = CitadelCollector(["ETH/USDT:USDT"], FakeDb(), FakeCache(),
                               make_config(), PARAMS)
        assert col._active_feeds() == ["marks", "book"]     # BTC нет: алерты и TRDR не о чём
        assert make_collector(FakeSession())._active_feeds() == \
            ["alerts", "trdr", "marks", "book"]
        empty = CitadelCollector([], FakeDb(), FakeCache(), make_config(), PARAMS)
        assert empty._active_feeds() == []


class TestCollectorTrdr:
    @pytest.mark.asyncio
    async def test_rows_cursor_and_redis_list(self) -> None:
        session, db = FakeSession(), FakeDb()
        t1, t2, t3 = now_ms() - 3 * MIN_MS, now_ms() - 2 * MIN_MS, now_ms() - MIN_MS
        session.on("/api/trdr", {"sym": "BTCUSDT", "kinds": [], "rows": [
            [t1, "diamond", -1, 501], [t2, "btcdd", 1, 502], [t3, "liq100", 0, 503],
        ]})
        col = make_collector(session, FakeCache(), db)
        await col._restore()
        await col._poll_trdr()
        assert session.calls[0].startswith("/api/trdr?symbol=BTCUSDT&from=")
        assert [r["strategy"] for r in db.rows] == ["cdl_trdr_diamond", "cdl_trdr_btcdd"]
        assert [r["side"] for r in db.rows] == ["SHORT", "LONG"]
        assert all(r["symbol"] == "BTC/USDT:USDT" for r in db.rows)
        # строка без стороны (side=0) пропущена, но курсор двигается и по ней
        assert col._cursor["trdr:BTCUSDT"] == t3
        assert [x["kind"] for x in col._cache.stored["trdr"]["rows"]] == ["diamond", "btcdd"]


class TestCollectorMarks:
    @staticmethod
    def _response(fresh_type: str = "trap") -> dict:
        t_fresh = now_ms() - 20 * MIN_MS          # свеча 15m закрылась 5 минут назад
        t_stale = now_ms() - 3 * 24 * HOUR_MS
        return {"t": now_ms(), "tf": "15m", "coins": {
            "BTCUSDT": {"c": 59700.0, "c24": 61000.0, "ok": True, "ev": {
                "type": fresh_type, "dir": 1, "t": t_fresh, "price": 59600.0,
                "oi": 1.95, "thr": 1.30, "conf": "S", "spotOk": True, "futOk": False}},
            "ETHUSDT": {"c": 1675.0, "c24": 1700.0, "ok": True, "ev": {
                "type": "cas", "dir": -1, "t": t_stale, "price": 2200.0,
                "oi": -2.0, "thr": -1.2, "conf": "F", "spotOk": False, "futOk": True}},
            "1000PEPEUSDT": {"c": 0.0034, "c24": 0.0034, "ok": True, "ev": {
                "type": "cas", "dir": 1, "t": t_fresh, "price": 1.0,
                "oi": -2.0, "thr": -1.2, "conf": "", "spotOk": False, "futOk": False}},
        }}

    @pytest.mark.asyncio
    async def test_only_fresh_known_symbol_becomes_candidate(self) -> None:
        session, db = FakeSession(), FakeDb()
        session.on("/api/marks", self._response())
        cache = FakeCache({("BTC/USDT:USDT", "1m"): {"close": 59700.0}})
        col = make_collector(session, cache, db)
        await col._restore()
        await col._poll_marks()
        await col._publish_status()

        assert session.calls[0].startswith("/api/marks?symbols=BTCUSDT,ETHUSDT&tf=15m")
        assert len(db.rows) == 1            # свежий BTC; ETH — старый, PEPE — не наша монета
        row = db.rows[0]
        assert (row["strategy"], row["side"], row["timeframe"]) == \
            ("cdl_trap", "LONG", "15m")
        assert row["strength"] == pytest.approx(1.95 / 1.30)
        assert row["entry"] == 59700.0 and row["stop"] == pytest.approx(59696.0)
        assert row["payload"]["type"] == "trap"
        # в Redis лежит последняя метка обеих монет, включая старую
        assert cache.stored["mark:ETH/USDT:USDT:15m"]["event"]["type"] == "cas"
        assert cache.stored["mark:BTC/USDT:USDT:15m"]["event"]["dir"] == 1
        assert "mark:1000PEPE/USDT:USDT:15m" not in cache.stored
        status = cache.stored["status"]
        assert status["covered"] == ["BTC/USDT:USDT", "ETH/USDT:USDT"]
        assert status["uncovered"] == []

    @pytest.mark.asyncio
    async def test_same_event_seen_again_is_not_duplicated(self) -> None:
        session, db = FakeSession(), FakeDb()
        session.on("/api/marks", self._response())
        col = make_collector(session, FakeCache(), db)
        await col._restore()
        await col._poll_marks()
        await col._poll_marks()
        assert len(db.rows) == 1 and col.stats["shadow_rows"] == 1

    @pytest.mark.asyncio
    async def test_price_falls_back_to_source(self) -> None:
        session, db = FakeSession(), FakeDb()
        session.on("/api/marks", self._response("cas"))
        col = make_collector(session, FakeCache(), db)    # своих свечей нет
        await col._restore()
        await col._poll_marks()
        assert db.rows[0]["strategy"] == "cdl_cascade"
        assert db.rows[0]["entry"] == 59600.0             # цена из метки

    @pytest.mark.asyncio
    async def test_incomplete_archive_is_not_a_candidate(self) -> None:
        response = self._response()
        response["coins"]["BTCUSDT"]["ok"] = False
        session, db = FakeSession(), FakeDb()
        session.on("/api/marks", response)
        col = make_collector(session, FakeCache(), db)
        await col._restore()
        await col._poll_marks()
        assert db.rows == []


class TestCollectorBook:
    @pytest.mark.asyncio
    async def test_known_symbols_written_and_uncovered_listed(self) -> None:
        session, db = FakeSession(), FakeDb()
        cache = FakeCache()
        symbols = SYMBOLS + ["ADA/USDT:USDT"]
        session.on("/api/ob_now", {"coins": {"BTCUSDT": btc_coin()},
                                   "dhg": GROUPS, "t": 0, "pass_ms": 1})
        col = CitadelCollector(symbols, db, cache, make_config(), PARAMS)
        col._client = CitadelClient("https://citadel.test", session)
        await col._restore()
        await col._poll_book()
        await col._publish_status()

        assert "/api/ob_now?symbols=BTCUSDT,ETHUSDT,ADAUSDT&g=ALL,SPOT,PERP&rmax=8" \
            in session.calls[0]
        book = cache.stored["book:BTC/USDT:USDT"]
        assert book["ratio"]["ALL"]["1"] == pytest.approx(120 / 160)
        assert [w["usd"] for w in book["walls"]["bids"]] == [1400]       # по всем биржам
        assert [w["src"] for w in book["by_source"]["bids"]] == ["okF", "binF"]
        assert book["by_source"]["asks"][0]["usd"] == 800
        assert [k for k in cache.stored if k.startswith("book:")] == ["book:BTC/USDT:USDT"]
        status = cache.stored["status"]
        assert status["covered"] == ["BTC/USDT:USDT"]
        assert status["uncovered"] == ["ADA/USDT:USDT", "ETH/USDT:USDT"]
        assert db.rows == []                               # стакан — не кандидат

    @pytest.mark.asyncio
    async def test_bad_response_raises(self) -> None:
        session = FakeSession()
        session.on("/api/ob_now", {"ok": False})
        col = make_collector(session)
        await col._restore()
        with pytest.raises(CitadelError):
            await col._poll_book()


class TestCollectorLoop:
    @pytest.mark.asyncio
    async def test_failure_backoff_and_recovery(self) -> None:
        col = make_collector(FakeSession())
        await col._restore()
        calls, sleeps = [], []

        async def flaky() -> None:
            calls.append(dict(col._feeds["alerts"]))      # состояние ДО этого вызова
            if len(calls) == 1:
                raise CitadelError("HTTP 502")
            if len(calls) == 3:
                col._stop_event.set()

        async def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)

        col._poll_alerts = flaky
        col._sleep = fake_sleep
        await col._poll_loop("alerts")

        assert [c["ok"] for c in calls] == [None, False, True]
        assert calls[1]["error"].startswith("CitadelError") and calls[1]["fails"] == 1
        assert sleeps == [10, 5, 5]                        # пауза выросла, затем норма
        assert col._feeds["alerts"]["fails"] == 0
        assert col.stats["messages_processed"] == 2

    @pytest.mark.asyncio
    async def test_one_failing_feed_does_not_stop_others(self) -> None:
        session, db = FakeSession(), FakeDb()
        session.on("/api/alerts", 500)                      # эта лента всегда падает
        session.on("/api/ob_now", {"coins": {"BTCUSDT": btc_coin()}, "dhg": GROUPS})
        cache = FakeCache()
        col = make_collector(session, cache, db)
        await col._restore()
        sleeps: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)
            if len(sleeps) >= 4:                 # по два цикла на каждую ленту
                col._stop_event.set()
            await asyncio.sleep(0)

        col._sleep = fake_sleep
        await asyncio.gather(col._poll_loop("alerts"), col._poll_loop("book"))
        assert "book:BTC/USDT:USDT" in cache.stored
        assert col._feeds["book"]["ok"] is True
        assert col._feeds["alerts"]["ok"] is False
        assert col._feeds["alerts"]["fails"] == 2
        assert sleeps == [10, 10, 20, 10]        # падающая лента ждёт всё дольше

    @pytest.mark.asyncio
    async def test_cursor_restored_from_redis(self) -> None:
        saved = now_ms() - 5 * HOUR_MS
        cache = FakeCache()
        cache.stored["cursor"] = {"alerts": saved, "trdr:BTCUSDT": "bad",
                                  "trdr:DOGEUSDT": 1}
        col = make_collector(FakeSession(), cache)
        await col._restore()
        assert col._cursor["alerts"] == saved              # продолжаем, а не с нуля
        assert col._cursor["trdr:BTCUSDT"] == pytest.approx(now_ms() - 24 * HOUR_MS, abs=5000)

    @pytest.mark.asyncio
    async def test_old_cursor_is_capped_by_lookback(self) -> None:
        cache = FakeCache()
        cache.stored["cursor"] = {"alerts": now_ms() - 10 * 24 * HOUR_MS}
        col = make_collector(FakeSession(), cache)
        await col._restore()
        assert col._cursor["alerts"] == pytest.approx(now_ms() - 24 * HOUR_MS, abs=5000)


# ------------------------------------------------------------------- отчёт
T0 = datetime(2026, 11, 20, 12, 0, 30, tzinfo=UTC)   # обнаружение в середине минуты


def candles_from(start: datetime, bars: list[tuple[float, float, float]]) -> list:
    """1m-свечи [(high, low, close)] с шагом в минуту от start."""
    return [(start + timedelta(minutes=i), hi, lo, c) for i, (hi, lo, c) in enumerate(bars)]


def shadow_row(side="LONG", entry=100.0, stop=96.0, target=106.0, offset=0.0015) -> dict:
    limit = entry * (1 - offset) if side == "LONG" else entry * (1 + offset)
    return {"id": 1, "strategy": "cdl_alert_bid-8-30", "symbol": "BTC/USDT:USDT",
            "timeframe": "", "side": side, "detected_at": T0, "onset": True,
            "entry": entry, "limit_price": round(limit, 8),
            "stop": stop, "target1": 103.0, "target2": target}


class TestShadowReport:
    def test_evaluate_replays_bot_mechanics(self) -> None:
        from scripts.analyze_shadow import evaluate

        row = shadow_row()
        kl = candles_from(T0 - timedelta(minutes=5),
                          [(100.5, 99.0, 100.0)] * 5       # до обнаружения
                          + [(100.0, 99.8, 99.9)]           # T0 — лимитка 99.85 исполнена
                          + [(101.0, 99.9, 100.8)] * 9      # рост
                          + [(106.8, 105.0, 106.5)]         # +10 мин — цель 106
                          + [(107.0, 106.0, 106.5)] * 100)
        ev = evaluate(row, kl, [c[0] for c in kl], ttl_min=120)
        assert ev["status"] == "tp"
        # R считается от лимитки (как в analyze_quality): риск = 99.85 - 96 = 3.85
        assert ev["r"] == pytest.approx((106.0 - 99.85) / 3.85)
        assert ev["mfe"] > ev["r"]
        assert ev["mae"] == pytest.approx((99.85 - 99.8) / 3.85)  # ход против, в R (>= 0)
        assert ev["h60"] == pytest.approx(6.5)                   # +6.5% за час в сторону сделки

    def test_evaluate_limit_never_filled(self) -> None:
        from scripts.analyze_shadow import evaluate

        row = shadow_row()
        short = candles_from(T0 - timedelta(minutes=5), [(100.5, 99.9, 100.0)] * 70)
        status = evaluate(row, short, [c[0] for c in short], ttl_min=120)["status"]
        assert status == "open"                     # TTL ещё не вышел — ждём
        long_kl = candles_from(T0 - timedelta(minutes=5), [(100.5, 99.9, 100.0)] * 200)
        status = evaluate(row, long_kl, [c[0] for c in long_kl], ttl_min=120)["status"]
        assert status == "expired"

    def test_evaluate_short_and_missing_levels(self) -> None:
        from scripts.analyze_shadow import evaluate

        row = shadow_row(side="SHORT", stop=104.0, target=94.0)
        kl = candles_from(T0 - timedelta(minutes=5), [(100.1, 99.5, 100.0)] * 200)
        times = [c[0] for c in kl]
        ev = evaluate(row, kl, times, ttl_min=120)
        assert ev["status"] == "expired"          # лимитка 100.15 не тронута
        assert ev["h60"] == pytest.approx(0.0)    # шорт: знак перевёрнут
        bare = dict(row, entry=None, stop=None, target2=None)
        assert evaluate(bare, kl, times, 120)["status"] is None
        assert evaluate(shadow_row(), [], [], 120)["status"] == "no_data"

    def test_baseline_matches_direct_calculation(self) -> None:
        import numpy as np

        from scripts.analyze_shadow import baseline

        closes = 100.0 * 1.01 ** np.arange(66)      # +1% за минуту, 66 свечей
        base = baseline(closes)
        # за 60 минут: вход по закрытию k, выход по закрытию k + 60
        expected = (1.01 ** 60 - 1.0) * 100
        assert base[60].shape == (6,)
        assert base[60].tolist() == pytest.approx([expected] * 6)
        assert base[60][0] == pytest.approx((closes[60] / closes[0] - 1.0) * 100)
        assert base[240].size == 0 and base[720].size == 0   # истории не хватает
        assert baseline(np.array([]))[60].size == 0

    def test_build_report_renders_both_sections(self) -> None:
        from scripts.analyze_shadow import build_report

        kl = candles_from(T0 - timedelta(minutes=10),
                          [(100.5, 99.8, 100.0)] * 5 + [(100.5, 99.8, 100.2)] * 760)
        long_row = shadow_row()
        short_row = shadow_row(side="SHORT", stop=104.0, target=94.0)
        bare = dict(short_row, stop=None)                       # без уровней
        lines = build_report([long_row, short_row, bare], {"BTC/USDT:USDT": kl}, 120)
        text = "\n".join(lines)
        assert "МЕХАНИКА БОТА" in text and "СОБЫТИЕ ПРОТИВ БАЗЫ" in text
        assert "без уровней (не было цены или ATR в момент события): 1" in text
        assert "cdl_alert_bid-8-30" in text
        assert "LONG " in text and "SHORT " in text
        assert build_report([], {}, 120) == ["Теневых сигналов за период нет."]

    def test_report_numbers_with_closed_trades(self) -> None:
        # путь с числами: лонг доходит до цели, шорт ловит стоп
        from scripts.analyze_shadow import _f, build_report

        win_kl = candles_from(T0 - timedelta(minutes=5),
                              [(100.5, 99.0, 100.0)] * 5 + [(100.0, 99.8, 99.9)]
                              + [(101.0, 99.9, 100.8)] * 9 + [(106.8, 105.0, 106.5)])
        loss_kl = candles_from(T0 - timedelta(minutes=5),
                               [(100.5, 99.5, 100.0)] * 5 + [(100.3, 99.8, 100.2)]
                               + [(100.9, 100.0, 100.8)] * 8 + [(104.5, 103.0, 104.0)])
        win = dict(shadow_row(), symbol="X/USDT:USDT")                                   # TP
        loss = dict(shadow_row(side="SHORT", stop=104.0, target=94.0), symbol="Y/USDT:USDT")  # SL
        kl = {"X/USDT:USDT": win_kl, "Y/USDT:USDT": loss_kl}
        lines = build_report([win, loss], kl, 120)
        # первая таблица («механика бота») идёт раньше разбора события против базы
        rows: dict[str, list[str]] = {}
        for x in lines:
            if x.startswith("cdl_alert_bid-8-30"):
                rows.setdefault(x[24:29], x[24:].split())
        # сторона, n, ист, ожид, TP, SL, игра, win%, avgR, sumR — R от лимитки (99.85)
        assert rows["LONG "][:10] == ["LONG", "1", "0", "0", "1", "0", "0", "100",
                                      "+1.60", "+1.6"]
        assert rows["SHORT"][:10] == ["SHORT", "1", "0", "0", "0", "1", "0", "0",
                                      "-1.00", "-1.0"]
        assert _f(None, 7) == "      -"
        assert _f(-1.0, 7) == "  -1.00"
        assert _f(12.345, 8, prec=1) == "   +12.3"
        assert _f(0.5, 8, signed=False) == "    0.50"
