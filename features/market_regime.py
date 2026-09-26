"""Определение режима рынка: тренд, волатильность, консолидация."""

from __future__ import annotations

import pandas as pd

from features.technical_indicators import TechnicalFeatures


class RegimeDetector:
    """Классификация состояния рынка по готовым индикаторам."""

    ATR_PERCENTILE_WINDOW = 252  # окно для percentile волатильности (~10 дней на H1)

    def detect_trend(
        self,
        df: pd.DataFrame,
        adx_threshold: float = 20.0,
        slope_periods: int = 10,
    ) -> str:
        """Определяет тренд по наклону EMA и силе ADX.

        Требует колонки ema_50, ema_200, adx, close (см. apply_full).

        Args:
            df: DataFrame с индикаторами.
            adx_threshold: ADX ниже — считаем рынок боковым.
            slope_periods: За сколько свечей мерить наклон EMA50.

        Returns:
            'uptrend' | 'downtrend' | 'sideways'.
        """
        if len(df) < max(slope_periods, 5):
            return "sideways"
        last = df.iloc[-1]
        if any(pd.isna(last.get(c)) for c in ("ema_50", "ema_200", "adx", "close")):
            return "sideways"

        ema_now = last["ema_50"]
        ema_before = df["ema_50"].iloc[-1 - slope_periods]
        slope = (ema_now - ema_before) / ema_before if ema_before else 0.0
        strong = last["adx"] >= adx_threshold

        if last["close"] > last["ema_50"] > last["ema_200"] and slope > 0 and strong:
            return "uptrend"
        if last["close"] < last["ema_50"] < last["ema_200"] and slope < 0 and strong:
            return "downtrend"
        return "sideways"

    def detect_volatility_regime(self, df: pd.DataFrame) -> str:
        """Классифицирует волатильность по перцентилю ATR.

        Требует колонки atr, close.

        Args:
            df: DataFrame с индикаторами.

        Returns:
            'low' (нижняя треть), 'medium', 'high' (верхняя треть).
        """
        if "atr" not in df.columns or df["atr"].dropna().empty:
            return "medium"
        atr_pct = df["atr"] / df["close"] * 100
        window = atr_pct.tail(self.ATR_PERCENTILE_WINDOW)
        current = window.iloc[-1]
        pct = float((window < current).mean())  # перцентиль текущего значения
        if pct < 1 / 3:
            return "low"
        if pct > 2 / 3:
            return "high"
        return "medium"

    def is_consolidation(
        self, df: pd.DataFrame, ratio: float = 0.5, lookback: int = 20
    ) -> bool:
        """Проверяет сжатие волатильности: BB width < ratio * своей средней.

        Требует bb_width (см. calculate_bollinger_bands).

        Args:
            df: DataFrame с индикаторами.
            ratio: Порог относительно средней ширины.
            lookback: Окно усреднения ширины.

        Returns:
            True, если рынок в консолидации.
        """
        if "bb_width" not in df.columns or len(df) < lookback + 1:
            return False
        avg_width = df["bb_width"].tail(lookback).mean()
        if not avg_width or pd.isna(avg_width):
            return False
        return bool(df["bb_width"].iloc[-1] < ratio * avg_width)

    def full_regime(self, df: pd.DataFrame) -> dict[str, str | bool]:
        """Полная сводка режима рынка.

        Args:
            df: DataFrame с индикаторами (apply_full).

        Returns:
            dict: trend, volatility, consolidation.
        """
        return {
            "trend": self.detect_trend(df),
            "volatility": self.detect_volatility_regime(df),
            "consolidation": self.is_consolidation(df),
        }


def prepare_regime_input(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет индикаторы, нужные RegimeDetector, если их нет.

    Args:
        df: «Сырой» DataFrame OHLCV.

    Returns:
        DataFrame с ema_50, ema_200, adx, atr, bb_width.
    """
    needed = {"ema_50", "ema_200", "adx", "atr", "bb_width"}
    if not needed.issubset(df.columns):
        df = TechnicalFeatures.apply_full(df)
    return df
