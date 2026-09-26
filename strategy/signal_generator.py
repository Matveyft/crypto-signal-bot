"""Генератор сигналов: 3 сетапа, фильтры, confidence score.

Сетапы:
    A) trend_pullback   — вход по тренду на откате к поддержке
    B) mean_reversion   — разворот из перекупленности/перепроданности
    C) breakout_retest  — пробой консолидации и ретест уровня
"""

from __future__ import annotations

import time
from typing import Any

import pandas as pd

from data_layer.storage.redis_cache import RedisCache
from data_layer.storage.timescale_manager import TimescaleManager
from data_layer.utils import load_config, setup_logging
from features.market_regime import RegimeDetector
from features.orderbook_features import OrderbookAnalyzer
from features.support_resistance import LevelDetector
from features.technical_indicators import TechnicalFeatures
from strategy.filters import SignalFilters
from strategy.risk_manager import RiskManager

BARS_NEEDED = {"1d": 250, "4h": 250, "1h": 250, "15m": 200}


class SignalGenerator:
    """Главный класс генерации торговых сигналов."""

    def __init__(
        self,
        db: TimescaleManager,
        cache: RedisCache,
        strategy_params: dict[str, Any] | None = None,
    ) -> None:
        """Инициализирует генератор.

        Args:
            db: Менеджер TimescaleDB.
            cache: Redis-кэш.
            strategy_params: Полный конфиг strategy_params.yaml (None — загрузить).
        """
        self.logger = setup_logging("signal_generator")
        self._db = db
        self._cache = cache
        self.params = strategy_params or load_config("strategy_params")
        self.filters = SignalFilters(self.params.get("filters"))
        self.risk = RiskManager(self.params.get("risk_management"))
        self.levels = LevelDetector(**self.params.get("levels", {}))
        self.orderbook = OrderbookAnalyzer(cache)
        self.regime = RegimeDetector()
        self._last_signal_at: dict[str, float] = {}  # cooldown per symbol
        self._last_scan_log: dict[str, float] = {}   # троттлинг телеметрии
        self._scan_log_interval = 900  # телеметрия раз в 15 мин на символ

    # ----------------------------------------------------------------- public
    async def generate_signals(self, symbol: str) -> dict[str, Any] | None:
        """Полный пайплайн: данные -> индикаторы -> уровни -> сетапы.

        Args:
            symbol: Символ в формате ccxt.

        Returns:
            Сигнал dict или None. Формат:
            {'type': 'LONG'|'SHORT', 'strategy': str, 'symbol': str,
             'entry': float, 'stop': float, 'targets': [t1, t2],
             'confidence': 0-1, 'reason': str, 'position_size': {...}}
        """
        started = time.time()
        try:
            data = await self._load_data(symbol)
            if data is None:
                return None
            flow = await self.orderbook.get_flow_metrics(symbol)
            funding = await self._cache.get_funding(symbol)
            # Live-цена: текущая минутка из Redis (закрытая H1 в БД отстаёт до часа)
            live = await self._cache.get_candle(symbol, "1m")
            data["live_price"] = float(live["close"]) if live else None
            self._log_scan_telemetry(symbol, data, flow, funding)

            # Жёсткие pre-trade фильтры: не прошли — сетапы даже не смотрим
            ok, reason = self.filters.volatility_filter(data["df_h1"])
            if not ok:
                self.logger.debug("%s rejected by volatility filter: %s", symbol, reason)
                return None
            ok, reason = self.filters.time_filter()
            if not ok:
                self.logger.debug("%s rejected by time filter: %s", symbol, reason)
                return None
            ok, reason = self.filters.funding_filter(
                (funding or {}).get("funding_rate")
            )
            if not ok:
                self.logger.debug("%s rejected by funding filter: %s", symbol, reason)
                return None

            candidates = []
            for setup_fn, name in (
                (self.trend_pullback_setup, "trend_pullback"),
                (self.mean_reversion_setup, "mean_reversion"),
                (self.breakout_retest_setup, "breakout_retest"),
            ):
                try:
                    signal = setup_fn(**data, flow=flow, funding=funding)
                except Exception:
                    self.logger.exception("Setup %s failed for %s", name, symbol)
                    continue
                if signal is not None:
                    candidates.append(signal)

            if not candidates:
                self.logger.debug("No setups for %s (%.0f ms)",
                                  symbol, (time.time() - started) * 1000)
                return None
            best = max(candidates, key=lambda s: s["confidence"])
            best["symbol"] = symbol
            best["position_size"] = self.risk.calculate_position_size(
                account_balance=10_000.0,  # дефолт; переопределяется caller'ом
                entry_price=best["entry"],
                stop_loss=best["stop"],
            )
            self.logger.info("SIGNAL %s %s %s conf=%.2f (%.0f ms): %s",
                             symbol, best["type"], best["strategy"],
                             best["confidence"], (time.time() - started) * 1000,
                             best["reason"])
            return best
        except Exception:
            self.logger.exception("generate_signals failed for %s", symbol)
            return None

    def _log_scan_telemetry(
        self,
        symbol: str,
        data: dict[str, Any],
        flow: dict[str, Any],
        funding: dict[str, Any] | None,
    ) -> None:
        """Раз в 15 минут пишет в INFO краткое состояние рынка по символу.

        Делает бот прозрачным: в логе видно, какие условия выполнялись
        в момент скана и что именно блокировало сетапы.

        Args:
            symbol: Символ ccxt.
            data: Результат _load_data (df_d1, df_h4, df_h1, df_m15).
            flow: Метрики стакана/CVD.
            funding: Funding-снапшот из Redis.
        """
        now = time.time()
        if now - self._last_scan_log.get(symbol, 0) < self._scan_log_interval:
            return
        self._last_scan_log[symbol] = now

        d1 = data["df_d1"].iloc[-1]
        h1 = data["df_h1"].iloc[-1]
        trend = self.regime.detect_trend(data["df_d1"])
        vol_ok, vol_reason = self.filters.volatility_filter(data["df_h1"])
        time_ok, _ = self.filters.time_filter()
        funding_rate = (funding or {}).get("funding_rate")
        price = data.get("live_price") or float(h1["close"])
        price_src = "live" if data.get("live_price") else "h1"
        self.logger.info(
            "SCAN %s: price=%.6g(%s) trend=%s adx=%.0f rsi_h1=%.0f atr%%=%.2f "
            "| vol_filter=%s(%s) time_filter=%s | imb=%s cvd30=%s fr=%s",
            symbol, price, price_src, trend, float(d1["adx"]),
            float(h1["rsi"]), float(h1["atr"]) / float(h1["close"]) * 100,
            vol_ok, vol_reason[:50], time_ok,
            flow.get("imbalance"), flow.get("cvd_30m"), funding_rate,
        )

    def in_cooldown(self, symbol: str) -> bool:
        """Проверяет cooldown по символу (не чаще раза в N минут).

        Args:
            symbol: Символ ccxt.

        Returns:
            True, если сигнал по символу недавно уже был.
        """
        cooldown = self.params.get("signal", {}).get("cooldown_minutes", 60)
        last = self._last_signal_at.get(symbol)
        return last is not None and (time.time() - last) < cooldown * 60

    def mark_signaled(self, symbol: str) -> None:
        """Фиксирует время последнего сигнала (для cooldown).

        Args:
            symbol: Символ ccxt.
        """
        self._last_signal_at[symbol] = time.time()

    # ------------------------------------------------------------------- data
    async def _load_data(self, symbol: str) -> dict[str, Any] | None:
        """Грузит свечи D1/H4/H1/M15, считает индикаторы и уровни.

        Args:
            symbol: Символ ccxt.

        Returns:
            dict: df_d1, df_h4, df_h1, df_m15, levels — или None, если
            данных мало.
        """
        frames: dict[str, pd.DataFrame] = {}
        for tf in ("1d", "4h", "1h", "15m"):
            df = await self._db.fetch_ohlcv(symbol, tf, limit=BARS_NEEDED[tf])
            if len(df) < 60:  # слишком мало истории
                self.logger.debug("%s %s: only %d bars", symbol, tf, len(df))
                return None
            frames[tf] = TechnicalFeatures.apply_full(df)

        key_levels = self.levels.get_key_levels(
            frames["1d"], frames["4h"], frames["1h"]
        )
        return {
            "df_d1": frames["1d"], "df_h4": frames["4h"],
            "df_h1": frames["1h"], "df_m15": frames["15m"],
            "levels": key_levels,
        }

    # ------------------------------------------------------- setup A: pullback
    def trend_pullback_setup(
        self,
        df_d1: pd.DataFrame,
        df_h4: pd.DataFrame,
        df_h1: pd.DataFrame,
        df_m15: pd.DataFrame,
        levels: dict[str, Any],
        flow: dict[str, Any],
        funding: dict[str, Any] | None = None,
        live_price: float | None = None,
    ) -> dict[str, Any] | None:
        """Сетап A: откат к поддержке в подтверждённом тренде старшего ТФ.

        LONG: [D1] uptrend + ADX>25; [H4] откат к зоне поддержки;
        [H1] RSI восстановился > 40, MACD histogram растёт;
        [M15] bullish-паттерн и пробой локального хая;
        imbalance > 1.8, CVD(30m) > 0. SHORT — зеркально.

        Returns:
            Сигнал dict или None.
        """
        cfg = self.params.get("trend_pullback", {})
        d1, h4, h1, m15 = df_d1.iloc[-1], df_h4.iloc[-1], df_h1.iloc[-1], df_m15.iloc[-1]

        # --- D1: направление и сила тренда
        d1_trend = self.regime.detect_trend(df_d1, adx_threshold=cfg.get("d1_adx_min", 25))
        if d1_trend not in ("uptrend", "downtrend"):
            return None
        if float(d1["adx"]) < cfg.get("d1_adx_min", 25):
            return None

        direction = "LONG" if d1_trend == "uptrend" else "SHORT"
        price = live_price or float(h1["close"])

        # --- H4: откат к зоне поддержки/сопротивления
        if direction == "LONG":
            zone_ok = self._in_pullback_zone_long(df_h4, levels, price)
        else:
            zone_ok = self._in_pullback_zone_short(df_h4, levels, price)
        if not zone_ok:
            return None

        # --- H1: моментум разворачивается
        h1_prev = df_h1.iloc[-2]
        if direction == "LONG":
            momentum = (
                float(h1["rsi"]) > cfg.get("h1_rsi_min", 40)
                and float(h1["close"]) > float(h1["ema_20"])
                and float(h1["macd_histogram"]) > float(h1_prev["macd_histogram"])
            )
        else:
            momentum = (
                float(h1["rsi"]) < 100 - cfg.get("h1_rsi_min", 40)
                and float(h1["close"]) < float(h1["ema_20"])
                and float(h1["macd_histogram"]) < float(h1_prev["macd_histogram"])
            )
        if not momentum:
            return None

        # --- M15: подтверждение паттерном и пробоем локального экстремума
        lookback = cfg.get("m15_lookback_bars", 4)
        if direction == "LONG":
            pattern = self._bullish_pattern(df_m15)
            local_high = float(df_m15["high"].iloc[-lookback - 1 : -1].max())
            confirmed = pattern and float(m15["close"]) > local_high
        else:
            pattern = self._bearish_pattern(df_m15)
            local_low = float(df_m15["low"].iloc[-lookback - 1 : -1].min())
            confirmed = pattern and float(m15["close"]) < local_low
        if not confirmed:
            return None

        # --- Flow: стакан и CVD
        imbalance = flow.get("imbalance")
        cvd = flow.get("cvd_30m")
        imb_min = cfg.get("orderbook_imbalance_min", 1.8)
        if imbalance is None or cvd is None:
            return None
        if direction == "LONG" and not (imbalance >= imb_min and cvd > 0):
            return None
        if direction == "SHORT" and not (imbalance <= 1 / imb_min and cvd < 0):
            return None

        atr = float(h1["atr"])
        entry = price
        swing = (levels["support"][0] if levels["support"] else None) \
            if direction == "LONG" else \
            (levels["resistance"][0] if levels["resistance"] else None)
        stop = self.risk.calculate_stop_loss(entry, atr, direction, swing)
        t1, t2 = self.risk.calculate_take_profit(
            entry, stop, direction,
            levels["support"] if direction == "SHORT" else levels["resistance"],
        )
        confidence = self._confidence(
            base=0.6,
            adx=float(d1["adx"]), direction=direction,
            imbalance=imbalance, cvd_positive=(cvd > 0) == (direction == "LONG"),
            rsi_h1=float(h1["rsi"]), direction_long=direction == "LONG",
        )
        return {
            "type": direction, "strategy": "trend_pullback",
            "entry": entry, "stop": stop, "targets": [t1, t2],
            "confidence": confidence,
            "reason": f"D1 {d1_trend} ADX={d1['adx']:.0f}, H4 pullback to zone, "
                      f"H1 momentum ok, M15 pattern, imb={imbalance:.2f} cvd30={cvd:.4f}",
        }

    # --------------------------------------------------- setup B: mean reversion
    def mean_reversion_setup(
        self,
        df_d1: pd.DataFrame,
        df_h4: pd.DataFrame,
        df_h1: pd.DataFrame,
        df_m15: pd.DataFrame,
        levels: dict[str, Any],
        flow: dict[str, Any],
        funding: dict[str, Any] | None = None,
        live_price: float | None = None,
    ) -> dict[str, Any] | None:
        """Сетап B: разворот из экстремума при перегруженном funding.

        LONG: не в глубоком даунтренде; RSI<30, price<BB lower, StochRSI<20;
        M15 первая зелёная после 3+ красных на объёме 1.5x, reclaim EMA9;
        bid-стены; funding < 0. SHORT — зеркально.

        Returns:
            Сигнал dict или None.
        """
        cfg = self.params.get("mean_reversion", {})
        h1, m15 = df_h1.iloc[-1], df_m15.iloc[-1]
        funding_rate = (funding or {}).get("funding_rate")

        # --- H1: экстремум
        rsi = float(h1["rsi"])
        long_extreme = (
            rsi < cfg.get("h1_rsi_max", 30)
            and float(h1["close"]) < float(h1["bb_lower"])
            and float(h1["stoch_rsi"]) < cfg.get("h1_stoch_rsi_max", 20)
        )
        short_extreme = (
            rsi > cfg.get("h1_rsi_min", 70)
            and float(h1["close"]) > float(h1["bb_upper"])
            and float(h1["stoch_rsi"]) > cfg.get("h1_stoch_rsi_min", 80)
        )
        if not (long_extreme or short_extreme):
            return None
        direction = "LONG" if long_extreme else "SHORT"

        # --- D1/H4: не против сильного тренда
        max_dist = cfg.get("max_dist_from_ema200_pct", 10)
        d1 = df_d1.iloc[-1]
        if direction == "LONG" and float(d1["close"]) < float(d1["ema_200"]) * (1 - max_dist / 100):
            return None
        if direction == "SHORT" and float(d1["close"]) > float(d1["ema_200"]) * (1 + max_dist / 100):
            return None

        # --- Funding: толпа на противоположной стороне
        if funding_rate is not None:
            if direction == "LONG" and funding_rate >= cfg.get("funding_rate_max", 0):
                return None
            if direction == "SHORT" and funding_rate <= -cfg.get("funding_rate_max", 0):
                return None

        # --- M15: разворотная свеча на объёме
        n_bars = 3
        spike = cfg.get("m15_volume_spike_ratio", 1.5)
        avg_vol = float(df_m15["volume"].tail(20).mean())
        ema9 = TechnicalFeatures.calculate_ema(df_m15.copy(), [9])["ema_9"].iloc[-1]
        if direction == "LONG":
            reds = (df_m15["close"].iloc[-n_bars - 1 : -1]
                    < df_m15["open"].iloc[-n_bars - 1 : -1]).sum()
            reversal = (
                float(m15["close"]) > float(m15["open"]) and reds >= n_bars
                and float(m15["volume"]) > spike * avg_vol
                and float(m15["close"]) > float(ema9)
            )
        else:
            greens = (df_m15["close"].iloc[-n_bars - 1 : -1]
                      > df_m15["open"].iloc[-n_bars - 1 : -1]).sum()
            reversal = (
                float(m15["close"]) < float(m15["open"]) and greens >= n_bars
                and float(m15["volume"]) > spike * avg_vol
                and float(m15["close"]) < float(ema9)
            )
        if not reversal:
            return None

        # --- Orderbook: стены в сторону входа
        if direction == "LONG" and not flow.get("bid_walls_present"):
            return None
        if direction == "SHORT" and not flow.get("ask_walls_present"):
            return None

        atr = float(h1["atr"])
        entry = live_price or float(m15["close"])
        stop = self.risk.calculate_stop_loss(entry, atr, direction)
        t1, t2 = self.risk.calculate_take_profit(
            entry, stop, direction,
            levels["support"] if direction == "LONG" else levels["resistance"],
        )
        confidence = self._confidence(
            base=0.55, adx=25.0, direction=direction,
            imbalance=flow.get("imbalance"), cvd_positive=None,
            rsi_h1=rsi, direction_long=direction == "LONG", mean_reversion=True,
        )
        return {
            "type": direction, "strategy": "mean_reversion",
            "entry": entry, "stop": stop, "targets": [t1, t2],
            "confidence": confidence,
            "reason": f"H1 RSI={rsi:.1f} BB-extreme, funding={funding_rate}, "
                      f"M15 reversal on volume, walls present",
        }

    # ------------------------------------------------------ setup C: breakout
    def breakout_retest_setup(
        self,
        df_d1: pd.DataFrame,
        df_h4: pd.DataFrame,
        df_h1: pd.DataFrame,
        df_m15: pd.DataFrame,
        levels: dict[str, Any],
        flow: dict[str, Any],
        funding: dict[str, Any] | None = None,
        live_price: float | None = None,
    ) -> dict[str, Any] | None:
        """Сетап C: пробой консолидации и успешный ретест.

        LONG: [H4] BB-сжатие + 5+ касаний сопротивления; [H1] пробой на
        объёме 2x + OI +5%; [M15] ретест с bullish rejection;
        imbalance > 2.0, CVD > 0. SHORT — зеркально.

        Returns:
            Сигнал dict или None.
        """
        cfg = self.params.get("breakout", {})
        h4, h1 = df_h4.iloc[-1], df_h1.iloc[-1]

        # --- H4: консолидация
        if not self.regime.is_consolidation(
            df_h4, ratio=cfg.get("h4_bb_width_ratio", 0.5)
        ):
            return None

        # Определяем границы консолидации по последним 60 H4-свечам
        window = df_h4.tail(60)
        range_high = float(window["high"].max())
        range_low = float(window["low"].min())
        current = float(h1["close"])

        # Направление пробоя: последняя H1-свеча закрылась за границей
        vol_ratio = float(h1["volume"]) / max(float(df_h1["volume"].tail(20).mean()), 1e-9)
        if vol_ratio < cfg.get("h1_volume_spike_ratio", 2.0):
            return None

        upper_break = float(h1["close"]) > range_high and float(h1["open"]) <= range_high
        lower_break = float(h1["close"]) < range_low and float(h1["open"]) >= range_low
        if not (upper_break or lower_break):
            return None
        direction = "LONG" if upper_break else "SHORT"

        # Касаний пробитой границы до пробоя
        tol = cfg.get("touch_tolerance_pct", 0.5)
        boundary = range_high if upper_break else range_low
        boundary_touches = self.levels.count_touches(window.iloc[:-3], boundary, tol)
        if boundary_touches < cfg.get("h4_min_touches", 5):
            return None

        # --- OI рост (из funding snapshot)
        oi_change_pct = (funding or {}).get("oi_change_pct")
        if oi_change_pct is None or oi_change_pct < cfg.get("h1_oi_change_min_pct", 5):
            return None

        # --- M15: ретест пробитого уровня держится
        m15 = df_m15.iloc[-1]
        m15_prev = df_m15.iloc[-2]
        if direction == "LONG":
            retest = (
                float(m15["low"]) <= boundary * (1 + 0.002)
                and float(m15["close"]) > boundary
                and self._bullish_rejection(df_m15)
            )
        else:
            retest = (
                float(m15["high"]) >= boundary * (1 - 0.002)
                and float(m15["close"]) < boundary
                and self._bearish_rejection(df_m15)
            )
        if not retest:
            return None

        # --- Flow
        imbalance = flow.get("imbalance")
        cvd = flow.get("cvd_30m")
        imb_min = cfg.get("m15_orderbook_imbalance_min", 2.0)
        if imbalance is None or cvd is None:
            return None
        if direction == "LONG" and not (imbalance >= imb_min and cvd > 0):
            return None
        if direction == "SHORT" and not (imbalance <= 1 / imb_min and cvd < 0):
            return None

        atr = float(h1["atr"])
        entry = live_price or current
        stop = self.risk.calculate_stop_loss(
            entry, atr, direction,
            boundary if direction == "LONG" else boundary,
        )
        t1, t2 = self.risk.calculate_take_profit(
            entry, stop, direction,
            levels["resistance"] if direction == "LONG" else levels["support"],
        )
        confidence = self._confidence(
            base=0.65, adx=float(h4["adx"]), direction=direction,
            imbalance=imbalance, cvd_positive=(cvd > 0) == (direction == "LONG"),
            rsi_h1=float(h1["rsi"]), direction_long=direction == "LONG",
        )
        return {
            "type": direction, "strategy": "breakout_retest",
            "entry": entry, "stop": stop, "targets": [t1, t2],
            "confidence": confidence,
            "reason": f"H4 squeeze ({boundary_touches} touches), H1 break "
                      f"{vol_ratio:.1f}x vol OI+{oi_change_pct:.0f}%, M15 retest hold, "
                      f"imb={imbalance:.2f}",
        }

    # ----------------------------------------------------------- pattern utils
    @staticmethod
    def _bullish_pattern(df: pd.DataFrame) -> bool:
        """Bullish engulfing или pin bar на последней свече.

        Args:
            df: M15 DataFrame.

        Returns:
            True при паттерне.
        """
        last, prev = df.iloc[-1], df.iloc[-2]
        body = abs(float(last["close"]) - float(last["open"]))
        prev_body = abs(float(prev["close"]) - float(prev["open"]))
        range_ = float(last["high"]) - float(last["low"])
        engulfing = (
            float(last["close"]) > float(last["open"])
            and float(prev["close"]) < float(prev["open"])
            and body > prev_body
        )
        lower_wick = min(float(last["open"]), float(last["close"])) - float(last["low"])
        pin_bar = range_ > 0 and lower_wick > 2 * body and lower_wick > 0.6 * range_
        return bool(engulfing or pin_bar)

    @staticmethod
    def _bearish_pattern(df: pd.DataFrame) -> bool:
        """Bearish engulfing или pin bar.

        Args:
            df: M15 DataFrame.

        Returns:
            True при паттерне.
        """
        last, prev = df.iloc[-1], df.iloc[-2]
        body = abs(float(last["close"]) - float(last["open"]))
        prev_body = abs(float(prev["close"]) - float(prev["open"]))
        range_ = float(last["high"]) - float(last["low"])
        engulfing = (
            float(last["close"]) < float(last["open"])
            and float(prev["close"]) > float(prev["open"])
            and body > prev_body
        )
        upper_wick = float(last["high"]) - max(float(last["open"]), float(last["close"]))
        pin_bar = range_ > 0 and upper_wick > 2 * body and upper_wick > 0.6 * range_
        return bool(engulfing or pin_bar)

    @staticmethod
    def _bullish_rejection(df: pd.DataFrame) -> bool:
        """Свеча отклонения вниз с закрытием в верхней половине диапазона.

        Returns:
            True при rejection.
        """
        last = df.iloc[-1]
        range_ = float(last["high"]) - float(last["low"])
        if range_ <= 0:
            return False
        close_pos = (float(last["close"]) - float(last["low"])) / range_
        return close_pos >= 0.6

    @staticmethod
    def _bearish_rejection(df: pd.DataFrame) -> bool:
        """Свеча отклонения вверх с закрытием в нижней половине диапазона.

        Returns:
            True при rejection.
        """
        last = df.iloc[-1]
        range_ = float(last["high"]) - float(last["low"])
        if range_ <= 0:
            return False
        close_pos = (float(last["close"]) - float(last["low"])) / range_
        return close_pos <= 0.4

    # ------------------------------------------------------------ zone checks
    def _in_pullback_zone_long(
        self, df_h4: pd.DataFrame, levels: dict[str, Any], price: float
    ) -> bool:
        """Проверяет, что цена в зоне поддержки (уровень/Fib/EMA50/POC).

        Args:
            df_h4: H4 с индикаторами.
            levels: Результат get_key_levels.
            price: Текущая цена.

        Returns:
            True, если в зоне отката.
        """
        cfg = self.params.get("trend_pullback", {})
        tol_pct = 0.5  # ширина зоны, %
        candidates: list[float] = []
        # Свинг-поддержки
        candidates.extend(levels.get("support", [])[:3])
        # EMA50 H4
        ema50 = df_h4.iloc[-1].get("ema_50")
        if ema50 is not None and not pd.isna(ema50):
            candidates.append(float(ema50))
        # Fib 0.382-0.5 последнего H4-свинга
        window = df_h4.tail(50)
        swing_high = float(window["high"].max())
        swing_low = float(window["low"].min())
        fib = LevelDetector.fib_levels(swing_low, swing_high)
        zone_lo = fib["fib_0.5"]
        zone_hi = fib["fib_0.382"]
        fib_zone = zone_lo <= price <= zone_hi
        # POC
        poc = levels.get("poc")
        if poc is not None:
            candidates.append(float(poc))

        near_level = any(
            abs(price - lv) / price * 100 <= tol_pct for lv in candidates if lv > 0
        )
        return bool(near_level or fib_zone)

    def _in_pullback_zone_short(
        self, df_h4: pd.DataFrame, levels: dict[str, Any], price: float
    ) -> bool:
        """Зеркально _in_pullback_zone_long: зона сопротивления.

        Returns:
            True, если в зоне откката для SHORT.
        """
        tol_pct = 0.5
        candidates: list[float] = []
        candidates.extend(levels.get("resistance", [])[:3])
        ema50 = df_h4.iloc[-1].get("ema_50")
        if ema50 is not None and not pd.isna(ema50):
            candidates.append(float(ema50))
        window = df_h4.tail(50)
        swing_low = float(window["low"].min())
        swing_high = float(window["high"].max())
        fib = LevelDetector.fib_levels(swing_low, swing_high)
        # для SHORT: откат вверх = расширение от свинг-лоу
        zone_lo = swing_low + 0.382 * (swing_high - swing_low)
        zone_hi = swing_low + 0.5 * (swing_high - swing_low)
        fib_zone = zone_lo <= price <= zone_hi
        poc = levels.get("poc")
        if poc is not None:
            candidates.append(float(poc))
        near_level = any(
            abs(price - lv) / price * 100 <= tol_pct for lv in candidates if lv > 0
        )
        return bool(near_level or fib_zone)

    # ------------------------------------------------------------- confidence
    @staticmethod
    def _confidence(
        base: float,
        adx: float,
        direction: str,
        imbalance: float | None,
        cvd_positive: bool | None,
        rsi_h1: float,
        direction_long: bool,
        mean_reversion: bool = False,
    ) -> float:
        """Собирает confidence score из компонентов.

        Args:
            base: База сетапа.
            adx: ADX D1.
            direction: 'LONG'|'SHORT' (не используется, совместимость).
            imbalance: Имбаланс стакана.
            cvd_positive: Совпадает ли CVD с направлением.
            rsi_h1: RSI H1.
            direction_long: True для LONG.
            mean_reversion: Для сетапа B — другой RSI-скоринг.

        Returns:
            confidence в [0.3, 0.95].
        """
        score = base
        if adx > 35:
            score += 0.1
        elif adx > 28:
            score += 0.05
        if imbalance is not None:
            if direction_long and imbalance > 2.5:
                score += 0.08
            elif not direction_long and imbalance < 0.4:
                score += 0.08
        if cvd_positive:
            score += 0.07
        if mean_reversion:
            depth = abs(50 - rsi_h1) / 50  # глубже экстремум — выше скор
            score += min(depth, 1.0) * 0.1
        else:
            if direction_long and 40 <= rsi_h1 <= 60:
                score += 0.05
            elif not direction_long and 40 <= rsi_h1 <= 60:
                score += 0.05
        return round(max(0.3, min(score, 0.95)), 3)
