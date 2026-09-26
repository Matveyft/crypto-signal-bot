"""Предторговые фильтры: волатильность, объём, funding, время, тренд, корреляция."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd


class SignalFilters:
    """Все методы возвращают (bool, reason) — прошли/нет и почему."""

    def __init__(self, params: dict[str, Any] | None = None) -> None:
        """Инициализирует фильтры параметрами из strategy_params.yaml.

        Args:
            params: Секция 'filters' конфига (None — дефолты).
        """
        params = params or {}
        self.min_volatility_pct = float(params.get("min_volatility_pct", 0.5))
        self.min_volume_ratio = float(params.get("min_volume_ratio", 1.0))
        self.max_funding_rate = float(params.get("max_funding_rate", 0.001))
        self.excluded_hours_utc = set(params.get("excluded_hours_utc", [3, 4, 5, 6, 7]))

    # ------------------------------------------------------------ volatility
    def volatility_filter(self, df_h1: pd.DataFrame) -> tuple[bool, str]:
        """ATR за последний час > min_volatility_pct % цены.

        Args:
            df_h1: H1 с колонками atr, close (или сырой — посчитаем).

        Returns:
            (passed, reason).
        """
        if df_h1.empty:
            return False, "no h1 data"
        atr, close = df_h1.get("atr"), df_h1["close"].iloc[-1]
        if atr is None or pd.isna(atr.iloc[-1]):
            from features.technical_indicators import TechnicalFeatures

            df_h1 = TechnicalFeatures.calculate_atr(df_h1)
            atr = df_h1["atr"]
        atr_pct = float(atr.iloc[-1]) / float(close) * 100
        if atr_pct < self.min_volatility_pct:
            return False, f"volatility too low: ATR {atr_pct:.2f}% < {self.min_volatility_pct}%"
        return True, f"volatility ok: ATR {atr_pct:.2f}%"

    # ---------------------------------------------------------------- volume
    def volume_filter(
        self, df: pd.DataFrame, ma_period: int = 20
    ) -> tuple[bool, str]:
        """Объём последней свечи > min_volume_ratio * MA(ma_period) объёма.

        Args:
            df: DataFrame с колонкой volume (любой ТФ).
            ma_period: Период среднего объёма.

        Returns:
            (passed, reason).
        """
        if len(df) < ma_period + 1:
            return False, f"not enough bars for volume MA: {len(df)}"
        avg_volume = df["volume"].tail(ma_period).mean()
        last_volume = df["volume"].iloc[-1]
        if avg_volume <= 0:
            return False, "zero average volume"
        ratio = last_volume / avg_volume
        if ratio < self.min_volume_ratio:
            return False, f"volume low: {ratio:.2f}x < {self.min_volume_ratio}x MA{ma_period}"
        return True, f"volume ok: {ratio:.2f}x MA{ma_period}"

    # --------------------------------------------------------------- funding
    def funding_filter(self, funding_rate: float | None) -> tuple[bool, str]:
        """|funding rate| < max_funding_rate (перегретый рынок не берём).

        Args:
            funding_rate: Текущий funding rate (доля, например 0.0001).

        Returns:
            (passed, reason).
        """
        if funding_rate is None:
            return True, "no funding data (skip check)"
        if abs(funding_rate) >= self.max_funding_rate:
            return False, f"funding extreme: {funding_rate:.5f}"
        return True, f"funding ok: {funding_rate:.5f}"

    # ------------------------------------------------------------------ time
    def time_filter(
        self, current_hour_utc: int | None = None
    ) -> tuple[bool, str]:
        """Исключает часы низкой ликвидности (по умолчанию 3-7 UTC).

        Args:
            current_hour_utc: Час UTC (None — взять текущий).

        Returns:
            (passed, reason).
        """
        hour = current_hour_utc if current_hour_utc is not None else datetime.now(
            tz=timezone.utc
        ).hour
        if hour in self.excluded_hours_utc:
            return False, f"low liquidity hour: {hour} UTC"
        return True, f"hour ok: {hour} UTC"

    # ----------------------------------------------------------------- trend
    def trend_filter(
        self, df_d1: pd.DataFrame, direction: str = "long"
    ) -> tuple[bool, str]:
        """Для long: EMA50 > EMA200 и price > EMA50; для short — зеркально.

        Args:
            df_d1: D1 с ema_50, ema_200, close.
            direction: 'long' | 'short'.

        Returns:
            (passed, reason).
        """
        if df_d1.empty:
            return False, "no d1 data"
        last = df_d1.iloc[-1]
        for col in ("ema_50", "ema_200"):
            if col not in df_d1.columns or pd.isna(last.get(col)):
                return False, f"missing {col}"
        price = float(last["close"])
        ema50, ema200 = float(last["ema_50"]), float(last["ema_200"])
        if direction == "long":
            ok = ema50 > ema200 and price > ema50
        else:
            ok = ema50 < ema200 and price < ema50
        if not ok:
            return False, f"d1 trend against {direction}: price={price:.2f} ema50={ema50:.2f} ema200={ema200:.2f}"
        return True, f"d1 trend supports {direction}"

    # ----------------------------------------------------------- correlation
    @staticmethod
    def correlation_filter(
        symbol: str, symbol_df: pd.DataFrame, btc_df: pd.DataFrame,
        window: int = 48, min_corr: float = 0.5,
    ) -> tuple[bool, str]:
        """Проверяет связку альткоина с BTC.

        Отвязка (corr < min_corr) — не отвергаем сигнал, а помечаем как
        opportunity: альт движется по своим причинам.

        Args:
            symbol: Символ (для reason-строки).
            symbol_df: H1 свечи альткоина.
            btc_df: H1 свечи BTC.
            window: Окно корреляции (свечей).
            min_corr: Порог связности.

        Returns:
            (passed, reason) — passed всегда True, отвязка не блокирует.
        """
        if symbol.startswith("BTC/"):
            return True, "BTC itself"
        n = min(len(symbol_df), len(btc_df), window)
        if n < 20:
            return True, "not enough data for correlation"
        alt_ret = symbol_df["close"].tail(n).pct_change().dropna()
        btc_ret = btc_df["close"].tail(n).pct_change().dropna()
        n = min(len(alt_ret), len(btc_ret))
        corr = float(alt_ret.tail(n).corr(btc_ret.tail(n)))
        if pd.isna(corr):
            return True, "correlation undefined"
        if corr < min_corr:
            return True, f"decoupled from BTC (corr={corr:.2f}) — opportunity"
        return True, f"correlated with BTC (corr={corr:.2f})"
