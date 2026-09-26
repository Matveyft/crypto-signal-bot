"""Определение уровней поддержки/сопротивления.

Методы: swing-экстремумы (scipy), кластеризация близких уровней,
подсчёт касаний, volume profile (POC/VAH/VAL), сбор ключевых уровней
с нескольких таймфреймов.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from scipy.signal import argrelextrema


class LevelDetector:
    """Поиск и оценка уровней поддержки/сопротивления."""

    def __init__(
        self,
        swing_order: int = 5,
        cluster_tolerance_pct: float = 0.3,
        touch_tolerance_pct: float = 0.5,
        min_touches: int = 2,
        volume_profile_bins: int = 50,
    ) -> None:
        """Инициализирует детектор.

        Args:
            swing_order: Окно для локальных экстремумов (scipy order).
            cluster_tolerance_pct: Расстояние слияния уровней, % от цены.
            touch_tolerance_pct: Зона касания уровня, % от цены.
            min_touches: Минимум касаний для «сильного» уровня.
            volume_profile_bins: Число бинов volume profile.
        """
        self.swing_order = swing_order
        self.cluster_tolerance_pct = cluster_tolerance_pct
        self.touch_tolerance_pct = touch_tolerance_pct
        self.min_touches = min_touches
        self.volume_profile_bins = volume_profile_bins

    # --------------------------------------------------------------- swings
    def find_swing_levels(self, df: pd.DataFrame, order: int | None = None) -> list[dict[str, Any]]:
        """Находит локальные максимумы/минимумы через argrelextrema.

        Args:
            df: DataFrame с колонками high, low, close.
            order: Окно экстремума (по умолчанию из конфига).

        Returns:
            Список dict: {'price': float, 'type': 'high'|'low',
            'touches': 1, 'timestamp': datetime|None}.
        """
        order = order or self.swing_order
        levels: list[dict[str, Any]] = []
        if len(df) < order * 2 + 1:
            return levels

        highs = df["high"].values
        lows = df["low"].values
        high_idx = argrelextrema(highs, np.greater_equal, order=order)[0]
        low_idx = argrelextrema(lows, np.less_equal, order=order)[0]
        ts_col = df.get("timestamp")

        for i in high_idx:
            levels.append({
                "price": float(highs[i]),
                "type": "high",
                "touches": 1,
                "timestamp": ts_col.iloc[i] if ts_col is not None else None,
            })
        for i in low_idx:
            levels.append({
                "price": float(lows[i]),
                "type": "low",
                "touches": 1,
                "timestamp": ts_col.iloc[i] if ts_col is not None else None,
            })
        return sorted(levels, key=lambda x: x["price"])

    # ------------------------------------------------------------ clustering
    def cluster_levels(
        self, levels: list[dict[str, Any]], tolerance_pct: float | None = None
    ) -> list[dict[str, Any]]:
        """Группирует близкие уровни в кластеры (1D-кластеризация по цене).

        Args:
            levels: Список уровней от find_swing_levels или произвольный.
            tolerance_pct: Расстояние слияния, % (по умолчанию из конфига).

        Returns:
            Кластеры: {'price': средняя цена, 'touches': сумма касаний,
            'type': 'high'|'low' по большинству}.
        """
        tolerance_pct = tolerance_pct or self.cluster_tolerance_pct
        if not levels:
            return []

        sorted_levels = sorted(levels, key=lambda x: x["price"])
        clusters: list[list[dict[str, Any]]] = [[sorted_levels[0]]]

        for level in sorted_levels[1:]:
            cluster_prices = [lv["price"] for lv in clusters[-1]]
            cluster_mean = sum(cluster_prices) / len(cluster_prices)
            if abs(level["price"] - cluster_mean) / cluster_mean * 100 <= tolerance_pct:
                clusters[-1].append(level)
            else:
                clusters.append([level])

        result: list[dict[str, Any]] = []
        for group in clusters:
            prices = [lv["price"] for lv in group]
            n_high = sum(1 for lv in group if lv.get("type") == "high")
            n_low = sum(1 for lv in group if lv.get("type") == "low")
            # touch-вес: swing-точки, подтверждённые с обеих сторон, сильнее
            weight = 2 if n_high > 0 and n_low > 0 else 1
            result.append({
                "price": sum(prices) / len(prices),
                "touches": sum(lv.get("touches", 1) for lv in group) * weight,
                "type": "high" if n_high >= n_low else "low",
            })
        return result

    # --------------------------------------------------------------- touches
    def count_touches(
        self, df: pd.DataFrame, level: float, tolerance_pct: float | None = None
    ) -> int:
        """Считает касания уровня свечами (high/low в зоне допуска).

        Args:
            df: DataFrame с колонками high, low.
            level: Цена уровня.
            tolerance_pct: Зона касания, % (по умолчанию из конфига).

        Returns:
            Число касаний.
        """
        tolerance_pct = tolerance_pct or self.touch_tolerance_pct
        tol = level * tolerance_pct / 100
        # Касание = диапазон свечи пересекает зону [level - tol, level + tol]
        touches = (df["high"] >= level - tol) & (df["low"] <= level + tol)
        return int(touches.sum())

    # --------------------------------------------------------- volume profil
    def volume_profile_levels(
        self, df: pd.DataFrame, num_bins: int | None = None
    ) -> dict[str, float | None]:
        """Считает volume profile: POC, VAH, VAL (70% стоимости).

        Args:
            df: DataFrame с колонками close (или typ. price) и volume.
            num_bins: Число ценовых бинов.

        Returns:
            dict: {'poc': float|None, 'vah': float|None, 'val': float|None}.
        """
        num_bins = num_bins or self.volume_profile_bins
        if df.empty or num_bins < 2:
            return {"poc": None, "vah": None, "val": None}

        price = ((df["high"] + df["low"] + df["close"]) / 3).values
        volume = df["volume"].values
        if price.max() == price.min():
            return {"poc": float(price[0]), "vah": float(price[0]), "val": float(price[0])}

        hist, bin_edges = np.histogram(price, bins=num_bins, weights=volume)
        bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2

        poc_idx = int(np.argmax(hist))
        total_volume = hist.sum()
        target = total_volume * 0.70

        # Расширяем значение от POC, пока не наберём 70% объёма
        selected = {poc_idx}
        current = hist[poc_idx]
        lo, hi = poc_idx, poc_idx
        while current < target and (lo > 0 or hi < len(hist) - 1):
            below = hist[lo - 1] if lo > 0 else -1.0
            above = hist[hi + 1] if hi < len(hist) - 1 else -1.0
            if above >= below:
                hi += 1
                current += hist[hi]
                selected.add(hi)
            else:
                lo -= 1
                current += hist[lo]
                selected.add(lo)

        return {
            "poc": float(bin_centers[poc_idx]),
            "vah": float(bin_centers[max(selected)]),
            "val": float(bin_centers[min(selected)]),
        }

    # ------------------------------------------------------------ composite
    def get_key_levels(
        self, df_d1: pd.DataFrame, df_h4: pd.DataFrame, df_h1: pd.DataFrame
    ) -> dict[str, Any]:
        """Собирает ключевые уровни со всех ТФ, кластеризует, фильтрует.

        Args:
            df_d1: Дневные свечи (с индикаторами или без).
            df_h4: Четырёхчасовые свечи.
            df_h1: Часовые свечи.

        Returns:
            dict: {'support': [цены], 'resistance': [цены],
            'levels': [все кластеры], 'poc': float|None}.
        """
        all_levels: list[dict[str, Any]] = []
        for tf_name, df in (("d1", df_d1), ("h4", df_h4), ("h1", df_h1)):
            for level in self.find_swing_levels(df):
                level["timeframe"] = tf_name
                all_levels.append(level)

        clusters = self.cluster_levels(all_levels)
        current_price = float(df_h1["close"].iloc[-1]) if not df_h1.empty else 0.0

        # Фильтр по силе: подтверждённые уровни (>= min_touches с учётом веса)
        strong = [c for c in clusters if c["touches"] >= self.min_touches]
        if not strong:  # ничего «сильного» — берём все кластеры
            strong = clusters

        support = sorted(
            [c["price"] for c in strong if c["price"] < current_price], reverse=True
        )
        resistance = sorted(c["price"] for c in strong if c["price"] >= current_price)

        vp = self.volume_profile_levels(df_h1)
        return {
            "support": support,
            "resistance": resistance,
            "levels": strong,
            "poc": vp["poc"],
            "vah": vp["vah"],
            "val": vp["val"],
        }

    # -------------------------------------------------------------- helpers
    @staticmethod
    def nearest_level(
        levels: list[float], price: float, below: bool = True
    ) -> float | None:
        """Ближайший уровень к цене сверху или снизу.

        Args:
            levels: Список цен уровней.
            price: Текущая цена.
            below: True — ближайший снизу, False — сверху.

        Returns:
            Цена уровня или None.
        """
        if below:
            candidates = [lv for lv in levels if lv <= price]
            return max(candidates) if candidates else None
        candidates = [lv for lv in levels if lv > price]
        return min(candidates) if candidates else None

    @staticmethod
    def fib_levels(
        swing_low: float, swing_high: float
    ) -> dict[str, float]:
        """Уровни Фибоначчи для диапазона свинга.

        Args:
            swing_low: Минимум свинга.
            swing_high: Максимум свинга.

        Returns:
            dict: fib_0.382, fib_0.5, fib_0.618 и др.
        """
        diff = swing_high - swing_low
        return {
            "fib_0.236": swing_high - 0.236 * diff,
            "fib_0.382": swing_high - 0.382 * diff,
            "fib_0.5": swing_high - 0.5 * diff,
            "fib_0.618": swing_high - 0.618 * diff,
        }
