"""Unit-тесты: LevelDetector, SignalFilters, RiskManager, OrderbookAnalyzer."""

import numpy as np
import pandas as pd
import pytest

from features.support_resistance import LevelDetector
from strategy.filters import SignalFilters
from strategy.risk_manager import RiskManager


# ------------------------------------------------------ OrderbookAnalyzer tests
class FakeRedisCache:
    """Мок RedisCache: отдаёт CVD и стакан, пишет set_orderbook в список."""

    def __init__(
        self, cvd_data: dict | None, orderbook: dict | None = None
    ) -> None:
        self._cvd = cvd_data
        self._orderbook = orderbook
        self.orderbook_pushes: list[tuple[str, dict]] = []

    async def get_cvd(self, symbol: str) -> dict | None:
        return self._cvd

    async def get_orderbook(self, symbol: str) -> dict | None:
        return self._orderbook

    async def set_orderbook(self, symbol: str, snapshot: dict) -> None:
        self.orderbook_pushes.append((symbol, snapshot))


class TestOrderbookAnalyzer:
    """Тесты чтения CVD-окон (ключи снапшота: m5/m15/m30/total)."""

    @pytest.mark.asyncio
    async def test_cvd_window_keys(self) -> None:
        from features.orderbook_features import OrderbookAnalyzer

        cache = FakeRedisCache({"total": 100.0, "m5": 10.0, "m15": 40.0, "m30": 100.0})
        analyzer = OrderbookAnalyzer(cache)
        assert await analyzer.calculate_cvd("BTC/USDT:USDT", "5min") == 10.0
        assert await analyzer.calculate_cvd("BTC/USDT:USDT", "15min") == 40.0
        assert await analyzer.calculate_cvd("BTC/USDT:USDT", "30min") == 100.0
        assert await analyzer.calculate_cvd("BTC/USDT:USDT", "total") == 100.0

    @pytest.mark.asyncio
    async def test_cvd_missing_snapshot(self) -> None:
        from features.orderbook_features import OrderbookAnalyzer

        analyzer = OrderbookAnalyzer(FakeRedisCache(None))
        assert await analyzer.calculate_cvd("BTC/USDT:USDT", "30min") is None

    @pytest.mark.asyncio
    async def test_cvd_unknown_window_raises(self) -> None:
        from features.orderbook_features import OrderbookAnalyzer

        analyzer = OrderbookAnalyzer(FakeRedisCache({"total": 1.0}))
        with pytest.raises(ValueError):
            await analyzer.calculate_cvd("BTC/USDT:USDT", "7min")


class TestImbalanceGuard:
    """Guard от деградировавших снапшотов стакана в calculate_imbalance."""

    @staticmethod
    def _book(n_bids: int = 10, n_asks: int = 10,
              bid_sz: float = 5.0, ask_sz: float = 5.0
              ) -> tuple[list[list[float]], list[list[float]]]:
        bids = [[100.0 - i, bid_sz] for i in range(n_bids)]
        asks = [[100.5 + i, ask_sz] for i in range(n_asks)]
        return bids, asks

    def test_balanced_book(self) -> None:
        from features.orderbook_features import OrderbookAnalyzer

        bids, asks = self._book()
        assert OrderbookAnalyzer.calculate_imbalance(bids, asks) == 1.0

    def test_partial_book_rejected(self) -> None:
        # асков меньше 10 уровней (рассинхрон WS) -> imb не считается
        from features.orderbook_features import OrderbookAnalyzer

        bids, asks = self._book(n_asks=2)
        assert OrderbookAnalyzer.calculate_imbalance(bids, asks) is None

    def test_extreme_ratio_rejected(self) -> None:
        # 5.0/0.2 = 25x — вне sanity-границ, живой стакан так не перекошен
        from features.orderbook_features import OrderbookAnalyzer

        bids, asks = self._book(ask_sz=0.2)
        assert OrderbookAnalyzer.calculate_imbalance(bids, asks) is None

    def test_zero_volume_rejected(self) -> None:
        from features.orderbook_features import OrderbookAnalyzer

        bids, asks = self._book(bid_sz=0.0)
        assert OrderbookAnalyzer.calculate_imbalance(bids, asks) is None

    @pytest.mark.asyncio
    async def test_get_snapshot_fallback_filters_garbage(self) -> None:
        # битый снапшот: короткие списки + мусорный imb 167.6 из Redis
        from features.orderbook_features import OrderbookAnalyzer

        cache = FakeRedisCache(None, {
            "bids": [[100.0, 5.0]], "asks": [[100.5, 5.0]],
            "imbalance": 167.6, "updated_at": 0,
        })
        snap = await OrderbookAnalyzer(cache).get_snapshot("BTC/USDT:USDT")
        assert snap is not None and snap["imbalance"] is None

    @pytest.mark.asyncio
    async def test_get_snapshot_fallback_accepts_sane(self) -> None:
        # старый формат (без списков уровней) с валидным imb — работает
        from features.orderbook_features import OrderbookAnalyzer

        cache = FakeRedisCache(None, {
            "bids": [[100.0, 5.0]], "asks": [[100.5, 5.0]],
            "imbalance": 2.5, "updated_at": 0,
        })
        snap = await OrderbookAnalyzer(cache).get_snapshot("BTC/USDT:USDT")
        assert snap is not None and snap["imbalance"] == 2.5


class TestBybitCollectorOrderbook:
    """Guard коллектора: деградировавший стакан не пушится в Redis."""

    @staticmethod
    def _collector() -> tuple["FakeRedisCache", "BybitCollector"]:
        from data_layer.collectors.bybit_collector import BybitCollector

        cache = FakeRedisCache(None)
        return cache, BybitCollector(["BTC/USDT:USDT"], db=None, cache=cache)

    @pytest.mark.asyncio
    async def test_degraded_book_not_pushed(self) -> None:
        # снапшот с 3 бидами и пустыми асками — пуша быть не должно
        cache, col = self._collector()
        await col._handle_orderbook({
            "topic": "orderbook.50.BTCUSDT", "type": "snapshot",
            "data": {"b": [["100", "1"]] * 3, "a": []},
        })
        assert cache.orderbook_pushes == []
        assert col._degraded_books["BTC/USDT:USDT"] == 1

    @pytest.mark.asyncio
    async def test_healthy_book_pushed(self) -> None:
        cache, col = self._collector()
        b = [[str(100 - i), "2"] for i in range(12)]
        a = [[str(100.5 + i), "1"] for i in range(12)]
        await col._handle_orderbook({
            "topic": "orderbook.50.BTCUSDT", "type": "snapshot",
            "data": {"b": b, "a": a},
        })
        assert len(cache.orderbook_pushes) == 1
        sym, snap = cache.orderbook_pushes[0]
        assert sym == "BTC/USDT:USDT"
        assert abs(snap["imbalance"] - 2.0) < 1e-9

    @pytest.mark.asyncio
    async def test_book_recovers_after_degradation(self) -> None:
        # после деградации свежий snapshot снова пушится, счётчик сброшен
        cache, col = self._collector()
        await col._handle_orderbook({
            "topic": "orderbook.50.BTCUSDT", "type": "snapshot",
            "data": {"b": [["100", "1"]] * 3, "a": []},
        })
        b = [[str(100 - i), "2"] for i in range(12)]
        a = [[str(100.5 + i), "1"] for i in range(12)]
        await col._handle_orderbook({
            "topic": "orderbook.50.BTCUSDT", "type": "snapshot",
            "data": {"b": b, "a": a},
        })
        assert len(cache.orderbook_pushes) == 1
        assert col._degraded_books["BTC/USDT:USDT"] == 0


class FakeWs:
    """Мок WebSocket: запоминает отправленные сообщения."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send(self, raw: str) -> None:
        self.sent.append(raw)


class TestOrderbookResubscribe:
    """Форс-ресабскрайб топика при длительной деградации стакана."""

    DEGRADED = {"topic": "orderbook.50.BTCUSDT", "type": "snapshot",
                "data": {"b": [["100", "1"]] * 3, "a": []}}

    @staticmethod
    def _collector_with_ws() -> tuple["FakeRedisCache", "BybitCollector", FakeWs]:
        from data_layer.collectors.bybit_collector import (
            BybitCollector,
        )

        cache = FakeRedisCache(None)
        col = BybitCollector(["BTC/USDT:USDT"], db=None, cache=cache)
        ws = FakeWs()
        col._ws = ws
        return cache, col, ws

    @pytest.mark.asyncio
    async def test_resubscribe_fires_after_threshold(self) -> None:
        import json as _json

        from data_layer.collectors.bybit_collector import (
            ORDERBOOK_RESUBSCRIBE_AFTER,
        )

        _, col, ws = self._collector_with_ws()
        for _ in range(ORDERBOOK_RESUBSCRIBE_AFTER):
            await col._handle_orderbook(self.DEGRADED)
        ops = [_json.loads(s)["op"] for s in ws.sent]
        assert ops == ["unsubscribe", "subscribe"]
        topics = [_json.loads(s)["args"][0] for s in ws.sent]
        assert topics == ["orderbook.50.BTCUSDT"] * 2
        # счётчик сброшен — следующий цикл начнётся с нуля
        assert col._degraded_books["BTC/USDT:USDT"] == 0

    @pytest.mark.asyncio
    async def test_below_threshold_no_resubscribe(self) -> None:
        from data_layer.collectors.bybit_collector import (
            ORDERBOOK_RESUBSCRIBE_AFTER,
        )

        _, col, ws = self._collector_with_ws()
        for _ in range(ORDERBOOK_RESUBSCRIBE_AFTER - 1):
            await col._handle_orderbook(self.DEGRADED)
        assert ws.sent == []

    @pytest.mark.asyncio
    async def test_resubscribe_cooldown(self) -> None:
        import time as _time

        from data_layer.collectors.bybit_collector import (
            ORDERBOOK_RESUBSCRIBE_AFTER,
        )

        _, col, ws = self._collector_with_ws()
        # только что ресабскрайбили — повтор заблокирован cooldown'ом
        col._resubscribe_at["BTC/USDT:USDT"] = _time.time()
        for _ in range(ORDERBOOK_RESUBSCRIBE_AFTER * 3):
            await col._handle_orderbook(self.DEGRADED)
        assert ws.sent == []


# ------------------------------------------------------------------ fixtures
def make_df(n: int = 300, trend: float = 0.0, seed: int = 42) -> pd.DataFrame:
    """Синтетический OHLCV DataFrame.

    Args:
        n: Число свечей.
        trend: Дрейф цены за свечу.
        seed: Seed генератора.

    Returns:
        DataFrame [timestamp, open, high, low, close, volume].
    """
    rng = np.random.default_rng(seed)
    close = 100.0 * np.cumprod(1 + rng.normal(trend, 0.01, n))
    high = close * (1 + np.abs(rng.normal(0, 0.004, n)))
    low = close * (1 - np.abs(rng.normal(0, 0.004, n)))
    open_ = np.roll(close, 1)
    open_[0] = close[0]
    return pd.DataFrame({
        "timestamp": pd.date_range("2026-01-01", periods=n, freq="1h", tz="UTC"),
        "open": open_, "high": high, "low": low, "close": close,
        "volume": rng.uniform(100, 1000, n),
    })


# ------------------------------------------------------- LevelDetector tests
class TestLevelDetector:
    """Тесты кластеризации и вспомогательных методов."""

    def test_cluster_levels_merges_close_levels(self) -> None:
        levels = [
            {"price": 100.0, "type": "high"},
            {"price": 100.15, "type": "high"},
            {"price": 100.25, "type": "low"},
        ]
        clusters = LevelDetector().cluster_levels(levels, tolerance_pct=0.3)
        assert len(clusters) == 1
        assert abs(clusters[0]["price"] - 100.1333) < 0.01

    def test_cluster_levels_separates_far_levels(self) -> None:
        levels = [{"price": 100.0, "type": "high"}, {"price": 110.0, "type": "low"}]
        clusters = LevelDetector().cluster_levels(levels, tolerance_pct=0.3)
        assert len(clusters) == 2

    def test_cluster_levels_empty(self) -> None:
        assert LevelDetector().cluster_levels([]) == []

    def test_high_low_cluster_gets_double_weight(self) -> None:
        levels = [
            {"price": 100.0, "type": "high"},
            {"price": 100.1, "type": "low"},
        ]
        clusters = LevelDetector().cluster_levels(levels, tolerance_pct=0.5)
        assert clusters[0]["touches"] == 4  # 2 уровня * weight 2

    def test_find_swing_levels_finds_extremes(self) -> None:
        df = make_df(300)
        detector = LevelDetector(swing_order=10)
        levels = detector.find_swing_levels(df)
        assert len(levels) > 5
        prices = df["high"].values
        for level in levels:
            if level["type"] == "high":
                assert level["price"] in prices

    def test_volume_profile_poc_between_val_vah(self) -> None:
        df = make_df(300)
        vp = LevelDetector().volume_profile_levels(df)
        assert vp["poc"] is not None
        assert vp["val"] <= vp["poc"] <= vp["vah"]

    def test_volume_profile_flat_series(self) -> None:
        df = pd.DataFrame({
            "high": [100.0] * 10, "low": [100.0] * 10,
            "close": [100.0] * 10, "volume": [1.0] * 10,
        })
        vp = LevelDetector().volume_profile_levels(df)
        assert vp["poc"] == 100.0

    def test_count_touches(self) -> None:
        df = pd.DataFrame({
            "high": [101.0, 90.0, 101.0, 90.0, 101.0],
            "low": [99.0, 89.0, 99.0, 89.0, 99.0],
        })
        detector = LevelDetector(touch_tolerance_pct=0.5)
        assert detector.count_touches(df, 100.0) == 3  # касания через high
        assert detector.count_touches(df, 90.0) == 2

    def test_nearest_level(self) -> None:
        assert LevelDetector.nearest_level([95, 98, 102], 100, below=True) == 98
        assert LevelDetector.nearest_level([95, 98, 102], 100, below=False) == 102
        assert LevelDetector.nearest_level([105], 100, below=True) is None


# --------------------------------------------------------- SignalFilters tests
class TestSignalFilters:
    """Тесты фильтров на mock-данных."""

    def make_filters(self) -> SignalFilters:
        return SignalFilters({
            "min_volatility_pct": 0.5,
            "min_volume_ratio": 1.0,
            "max_funding_rate": 0.001,
            "excluded_hours_utc": [3, 4, 5, 6, 7],
        })

    def test_time_filter_excludes_low_liquidity(self) -> None:
        f = self.make_filters()
        for hour in (3, 5, 7):
            assert f.time_filter(hour) == (False, f.time_filter(hour)[1])
            assert f.time_filter(hour)[0] is False
        for hour in (2, 8, 12, 23):
            assert f.time_filter(hour)[0] is True

    def test_funding_filter(self) -> None:
        f = self.make_filters()
        assert f.funding_filter(None)[0] is True
        assert f.funding_filter(0.0001)[0] is True
        assert f.funding_filter(-0.0001)[0] is True
        assert f.funding_filter(0.002)[0] is False
        assert f.funding_filter(-0.002)[0] is False

    def test_volume_filter(self) -> None:
        f = self.make_filters()
        df = make_df(50, seed=1)
        df.loc[df.index[-1], "volume"] = df["volume"].tail(21).head(20).mean() * 2
        assert f.volume_filter(df)[0] is True
        df.loc[df.index[-1], "volume"] = 0.1
        assert f.volume_filter(df)[0] is False

    def test_volatility_filter(self) -> None:
        f = self.make_filters()
        df = make_df(100, seed=2)
        df["atr"] = df["close"] * 0.01  # 1% ATR
        assert f.volatility_filter(df)[0] is True
        df["atr"] = df["close"] * 0.001  # 0.1% ATR
        assert f.volatility_filter(df)[0] is False

    def test_trend_filter(self) -> None:
        f = self.make_filters()
        df = pd.DataFrame({
            "close": [100.0, 101.0], "ema_50": [99.0, 99.5], "ema_200": [95.0, 95.0],
        })
        assert f.trend_filter(df, "long")[0] is True
        assert f.trend_filter(df, "short")[0] is False
        df_inv = pd.DataFrame({
            "close": [90.0, 89.0], "ema_50": [95.0, 94.0], "ema_200": [99.0, 98.0],
        })
        assert f.trend_filter(df_inv, "short")[0] is True


# --------------------------------------------------------- RiskManager tests
class TestRiskManager:
    """Тесты расчёта позиции и стопов."""

    def make_rm(self, **overrides) -> RiskManager:
        params = {
            "risk_per_trade_pct": 1.5, "stop_loss_atr_multiplier": 1.5,
            "risk_reward_ratio": 2.0, "trailing_stop_activation_rr": 1.0,
            "leverage": 5,
        }
        params.update(overrides)
        return RiskManager(params)

    def test_position_size_exact_risk(self) -> None:
        rm = self.make_rm()
        # риск 1.5% от 10000 = 150; дистанция стопа 2.0 => qty = 75
        size = rm.calculate_position_size(10_000, entry_price=100.0, stop_loss=98.0)
        assert size["qty"] == pytest.approx(75.0)
        assert size["risk_amount"] == pytest.approx(150.0)
        assert size["risk_pct"] == pytest.approx(1.5)

    def test_position_size_margin_capped(self) -> None:
        rm = self.make_rm()
        # крошечный стоп => огромный notional, маржа режется балансом*плечо
        size = rm.calculate_position_size(1_000, 100.0, 99.999)
        assert size["notional_usdt"] <= 1_000 * 5 + 0.01
        assert size["margin_usdt"] <= 1_000

    def test_position_size_invalid_inputs(self) -> None:
        rm = self.make_rm()
        with pytest.raises(ValueError):
            rm.calculate_position_size(0, 100.0, 98.0)
        with pytest.raises(ValueError):
            rm.calculate_position_size(1_000, -5.0, -6.0)
        with pytest.raises(ValueError):
            rm.calculate_position_size(1_000, 100.0, 100.0)  # нулевой риск

    def test_stop_loss_long_with_swing(self) -> None:
        rm = self.make_rm()
        # swing 97.0, буфер 0.3*ATR=0.6 => структурный стоп 96.4
        assert rm.calculate_stop_loss(100.0, 2.0, "LONG", swing_level=97.0) == pytest.approx(96.4)

    def test_stop_loss_short_atr_only(self) -> None:
        rm = self.make_rm()
        assert rm.calculate_stop_loss(100.0, 2.0, "SHORT") == pytest.approx(103.0)

    def test_take_profit_rr(self) -> None:
        rm = self.make_rm()
        t1, t2 = rm.calculate_take_profit(100.0, 98.0, "LONG")
        assert t1 == pytest.approx(103.0)  # 0.75 * 2.0 * 2
        assert t2 == pytest.approx(104.0)  # 2.0 R/R

    def test_take_profit_respects_resistance(self) -> None:
        rm = self.make_rm()
        t1, t2 = rm.calculate_take_profit(100.0, 98.0, "LONG", resistance_levels=[102.0])
        assert t1 < 102.0  # TP1 перед сопротивлением

    def test_should_trail_stop(self) -> None:
        rm = self.make_rm()
        # риск 3.6 (стоп 96.4): 1:1 => 103.6
        assert rm.should_trail_stop(103.7, 100.0, 96.4) is True
        assert rm.should_trail_stop(103.5, 100.0, 96.4) is False
        # SHORT: риск 3.6 => 96.4
        assert rm.should_trail_stop(96.3, 100.0, 103.6) is True
        assert rm.should_trail_stop(96.5, 100.0, 103.6) is False

    def test_trailing_stop_price_direction(self) -> None:
        assert RiskManager.trailing_stop_price(110.0, 2.0, "LONG") == pytest.approx(108.0)
        assert RiskManager.trailing_stop_price(90.0, 2.0, "SHORT") == pytest.approx(92.0)
