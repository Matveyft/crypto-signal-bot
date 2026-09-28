"""Детектор дивергенций: пивоты цены + расхождение с индикатором.

Divergence — расхождение направления цены и индикатора (RSI, MACD):
    regular  — разворотная: цена LL/HH, индикатор HL/LH;
    hidden   — трендовая (продолжение): цена HL/LH, индикатор LL/HH.

Пивот подтверждается `order` барами позже — поле confirmed_at исключает
заглядывание в будущее при бэктесте и live-использовании: до этого бара
пивот физически не мог быть известен.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class Pivot:
    """Локальный экстремум (сванг)."""

    idx: int           # индекс бара-пивота
    price: float       # low для 'low', high для 'high'
    kind: str          # 'low' | 'high'
    confirmed_at: int  # бар, с которого пивот известен (idx + order)


@dataclass(frozen=True)
class Divergence:
    """Расхождение цены и индикатора между соседними пивотами."""

    kind: str          # 'regular' | 'hidden'
    direction: str     # 'bullish' | 'bearish'
    indicator: str     # имя колонки ('rsi', 'macd_histogram', ...)
    p1: int            # индекс раннего пивота
    p2: int            # индекс позднего пивота
    confirmed_at: int  # бар, с которого дивергенция известна
    strength: float    # зазор индикатора (в его единицах, всегда > 0)


def find_pivots(df: pd.DataFrame, order: int = 5) -> list[Pivot]:
    """Ищет сванг-экстремумы: low/high ниже/выше всех в окне ±order.

    Связи (равные экстремумы в окне) не считаются пивотами.

    Args:
        df: DataFrame с колонками low, high.
        order: Полуокно подтверждения (баров с каждой стороны).

    Returns:
        Список Pivot по возрастанию idx.
    """
    lows = df["low"].to_numpy()
    highs = df["high"].to_numpy()
    n = len(df)
    pivots: list[Pivot] = []
    for i in range(order, n - order):
        win_lo = lows[i - order : i + order + 1]
        win_hi = highs[i - order : i + order + 1]
        if lows[i] == win_lo.min() and (win_lo == lows[i]).sum() == 1:
            pivots.append(Pivot(i, float(lows[i]), "low", i + order))
        if highs[i] == win_hi.max() and (win_hi == highs[i]).sum() == 1:
            pivots.append(Pivot(i, float(highs[i]), "high", i + order))
    return pivots


def detect_divergences(
    df: pd.DataFrame,
    indicator: str = "rsi",
    order: int = 5,
    lookback: int = 40,
    min_gap: float = 3.0,
    min_price_pct: float = 0.05,
    min_pair_bars: int = 5,
) -> list[Divergence]:
    """Ищет дивергенции между соседними пивотами одного типа.

    Правила (для пивотов-лоев, bullish-сторона):
        regular: цена ниже (LL), индикатор выше (HL) → разворот вверх;
        hidden:  цена выше (HL), индикатор ниже (LL) → продолжение вверх.
    Bearish — зеркально по пивотам-хаям.

    Args:
        df: DataFrame с low, high и колонкой indicator.
        indicator: Колонка индикатора (по умолчанию RSI).
        order: Полуокно пивота (баров).
        lookback: Максимум баров между пивотами пары.
        min_gap: Минимальный зазор индикатора (в его единицах).
        min_price_pct: Минимальное изменение цены между пивотами, %.
        min_pair_bars: Минимум баров между пивотами пары.

    Returns:
        Список Divergence, отсортированный по confirmed_at. Пары, чей
        второй пивот ещё не подтверждён к концу df, не возвращаются.
    """
    pivots = find_pivots(df, order=order)
    ind = df[indicator].to_numpy()
    n = len(df)
    out: list[Divergence] = []
    for a, b in zip(pivots, pivots[1:]):
        if a.kind != b.kind:
            continue
        if not (min_pair_bars <= b.idx - a.idx <= lookback):
            continue
        if b.confirmed_at > n - 1:
            continue  # пивот ещё не подтверждён — в будущее не смотрим
        gap = float(ind[b.idx] - ind[a.idx])
        price_chg = (b.price - a.price) / a.price * 100
        if a.kind == "low":
            if price_chg <= -min_price_pct and gap >= min_gap:
                out.append(Divergence("regular", "bullish", indicator,
                                      a.idx, b.idx, b.confirmed_at, gap))
            elif price_chg >= min_price_pct and -gap >= min_gap:
                out.append(Divergence("hidden", "bullish", indicator,
                                      a.idx, b.idx, b.confirmed_at, -gap))
        else:  # пивоты-хаи
            if price_chg >= min_price_pct and -gap >= min_gap:
                out.append(Divergence("regular", "bearish", indicator,
                                      a.idx, b.idx, b.confirmed_at, -gap))
            elif price_chg <= -min_price_pct and gap >= min_gap:
                out.append(Divergence("hidden", "bearish", indicator,
                                      a.idx, b.idx, b.confirmed_at, -gap))
    return sorted(out, key=lambda d: d.confirmed_at)
