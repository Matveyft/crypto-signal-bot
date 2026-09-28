"""Тесты детектора дивергенций: синтетические серии с известными паттернами."""

import pandas as pd

from features.divergence import detect_divergences, find_pivots


def make_df(
    n: int = 40,
    low_dips: dict[int, float] | None = None,
    high_bumps: dict[int, float] | None = None,
    rsi: dict[int, float] | None = None,
    base: float = 100.0,
) -> pd.DataFrame:
    """Собирает DataFrame с плоской серией и точечными экстремумами.

    Args:
        n: Число баров.
        low_dips: {индекс: low} — локальные впадины.
        high_bumps: {индекс: high} — локальные вершины.
        rsi: {индекс: значение} — RSI в точках, иначе 50.
        base: Базовая цена.

    Returns:
        DataFrame с low, high, close, rsi.
    """
    low_dips, high_bumps, rsi = low_dips or {}, high_bumps or {}, rsi or {}
    rows = []
    for i in range(n):
        low = low_dips.get(i, base - 0.5)
        high = high_bumps.get(i, base + 0.5)
        rows.append({
            "low": low, "high": high, "close": (low + high) / 2,
            "rsi": rsi.get(i, 50.0),
        })
    return pd.DataFrame(rows)


class TestFindPivots:
    """Пивоты: нахождение и подтверждение с задержкой order."""

    def test_pivot_found_with_confirmation_lag(self) -> None:
        df = make_df(low_dips={10: 95.0})
        pivots = find_pivots(df, order=5)
        lows = [p for p in pivots if p.kind == "low"]
        assert len(lows) == 1
        assert lows[0].idx == 10 and lows[0].price == 95.0
        assert lows[0].confirmed_at == 15  # известен только через 5 баров

    def test_tied_extremes_not_pivot(self) -> None:
        # два равных лоя в окне — связь, пивота нет
        df = make_df(low_dips={10: 95.0, 12: 95.0})
        lows = [p for p in find_pivots(df, order=5) if p.kind == "low"]
        assert lows == []


class TestDetectDivergences:
    """Классификация regular/hidden по парам пивотов."""

    def test_regular_bullish(self) -> None:
        # цена LL (95 -> 94), RSI HL (30 -> 36) => regular bullish
        df = make_df(low_dips={8: 95.0, 28: 94.0}, rsi={8: 30.0, 28: 36.0})
        divs = detect_divergences(df, indicator="rsi")
        assert len(divs) == 1
        d = divs[0]
        assert (d.kind, d.direction) == ("regular", "bullish")
        assert (d.p1, d.p2) == (8, 28)
        assert d.confirmed_at == 33
        assert d.strength == 6.0

    def test_hidden_bullish(self) -> None:
        # цена HL (94 -> 95), RSI LL (30 -> 24) => hidden bullish
        df = make_df(low_dips={8: 94.0, 28: 95.0}, rsi={8: 30.0, 28: 24.0})
        divs = detect_divergences(df, indicator="rsi")
        assert len(divs) == 1
        d = divs[0]
        assert (d.kind, d.direction) == ("hidden", "bullish")
        assert d.strength == 6.0

    def test_regular_bearish(self) -> None:
        # цена HH (105 -> 106), RSI LH (70 -> 64) => regular bearish
        df = make_df(high_bumps={8: 105.0, 28: 106.0}, rsi={8: 70.0, 28: 64.0})
        divs = detect_divergences(df, indicator="rsi")
        assert len(divs) == 1
        d = divs[0]
        assert (d.kind, d.direction) == ("regular", "bearish")

    def test_no_divergence_when_indicator_follows_price(self) -> None:
        # цена LL и RSI LL — расхождения нет
        df = make_df(low_dips={8: 95.0, 28: 94.0}, rsi={8: 36.0, 28: 30.0})
        assert detect_divergences(df, indicator="rsi") == []

    def test_min_gap_filters_weak_divergence(self) -> None:
        # зазор RSI 2 < min_gap 3 — не дивергенция
        df = make_df(low_dips={8: 95.0, 28: 94.0}, rsi={8: 30.0, 28: 32.0})
        assert detect_divergences(df, indicator="rsi", min_gap=3.0) == []

    def test_no_lookahead_unconfirmed_pivot(self) -> None:
        # второй пивот на баре 37: confirmed_at=42 > последнего бара 39
        df = make_df(n=40, low_dips={8: 95.0, 37: 94.0},
                     rsi={8: 30.0, 37: 36.0})
        assert detect_divergences(df, indicator="rsi") == []

    def test_lookback_limits_pair_distance(self) -> None:
        # пивоты на расстоянии 20 баров при lookback=15 — пара не берётся
        df = make_df(low_dips={8: 95.0, 28: 94.0}, rsi={8: 30.0, 28: 36.0})
        assert detect_divergences(df, indicator="rsi", lookback=15) == []
