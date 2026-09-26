"""Технические индикаторы на чистом pandas/numpy (без ta-lib/pandas-ta).

Все методы принимают DataFrame [timestamp, open, high, low, close, volume]
и возвращают его же с добавленными колонками.
"""

from __future__ import annotations

import pandas as pd


class TechnicalFeatures:
    """Набор технических индикаторов.

    Каждый метод мутирует переданный DataFrame (добавляет колонки) и
    возвращает его же — удобно чейнить в pipeline.
    """

    # ------------------------------------------------------------------- EMA
    @staticmethod
    def calculate_ema(df: pd.DataFrame, periods: list[int] | None = None) -> pd.DataFrame:
        """Добавляет колонки ema_20, ema_50, ema_200.

        Args:
            df: DataFrame с колонкой close.
            periods: Периоды EMA (по умолчанию [20, 50, 200]).

        Returns:
            DataFrame с колонками ema_{period}.
        """
        for period in periods or [20, 50, 200]:
            df[f"ema_{period}"] = df["close"].ewm(
                span=period, adjust=False, min_periods=period
            ).mean()
        return df

    # ------------------------------------------------------------------- RSI
    @staticmethod
    def calculate_rsi(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Добавляет колонку rsi (Wilder's smoothing).

        Args:
            df: DataFrame с колонкой close.
            period: Период RSI.

        Returns:
            DataFrame с колонкой rsi.
        """
        delta = df["close"].diff()
        gain = delta.clip(lower=0.0)
        loss = -delta.clip(upper=0.0)
        avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
        avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
        rs = avg_gain / avg_loss.replace(0.0, pd.NA)
        df["rsi"] = (100 - 100 / (1 + rs)).fillna(50.0)
        return df

    # ------------------------------------------------------------- Stoch RSI
    @staticmethod
    def calculate_stoch_rsi(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Добавляет колонку stoch_rsi (Stochastic от RSI, 0-100).

        Args:
            df: DataFrame с колонкой close.
            period: Период RSI и окна стохастика.

        Returns:
            DataFrame с колонкой stoch_rsi.
        """
        delta = df["close"].diff()
        gain = delta.clip(lower=0.0)
        loss = -delta.clip(upper=0.0)
        avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
        avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
        rsi = (100 - 100 / (1 + avg_gain / avg_loss.replace(0.0, pd.NA))).fillna(50.0)

        rolling_min = rsi.rolling(period, min_periods=period).min()
        rolling_max = rsi.rolling(period, min_periods=period).max()
        spread = (rolling_max - rolling_min).replace(0.0, pd.NA)
        df["stoch_rsi"] = ((rsi - rolling_min) / spread * 100).fillna(50.0)
        return df

    # ------------------------------------------------------------------ MACD
    @staticmethod
    def calculate_macd(
        df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9
    ) -> pd.DataFrame:
        """Добавляет macd, macd_signal, macd_histogram.

        Args:
            df: DataFrame с колонкой close.
            fast: Быстрая EMA.
            slow: Медленная EMA.
            signal: Сигнальная линия.

        Returns:
            DataFrame с колонками macd, macd_signal, macd_histogram.
        """
        ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
        ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
        df["macd"] = ema_fast - ema_slow
        df["macd_signal"] = df["macd"].ewm(span=signal, adjust=False).mean()
        df["macd_histogram"] = df["macd"] - df["macd_signal"]
        return df

    # --------------------------------------------------------- Bollinger Band
    @staticmethod
    def calculate_bollinger_bands(
        df: pd.DataFrame, period: int = 20, std: float = 2.0
    ) -> pd.DataFrame:
        """Добавляет bb_upper, bb_middle, bb_lower, bb_width.

        Args:
            df: DataFrame с колонкой close.
            period: Период скользящей средней.
            std: Число стандартных отклонений.

        Returns:
            DataFrame с BB-колонками (bb_width — в % от средней).
        """
        bb_middle = df["close"].rolling(period, min_periods=period).mean()
        rolling_std = df["close"].rolling(period, min_periods=period).std()
        df["bb_middle"] = bb_middle
        df["bb_upper"] = bb_middle + std * rolling_std
        df["bb_lower"] = bb_middle - std * rolling_std
        df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / bb_middle * 100
        return df

    # ------------------------------------------------------------------- ATR
    @staticmethod
    def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Добавляет atr (Average True Range, Wilder's smoothing).

        Args:
            df: DataFrame с колонками high, low, close.
            period: Период ATR.

        Returns:
            DataFrame с колонкой atr.
        """
        prev_close = df["close"].shift(1)
        tr = pd.concat(
            [df["high"] - df["low"],
             (df["high"] - prev_close).abs(),
             (df["low"] - prev_close).abs()],
            axis=1,
        ).max(axis=1)
        df["atr"] = tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
        return df

    # ------------------------------------------------------------------- ADX
    @staticmethod
    def calculate_adx(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Добавляет adx (сила тренда, без направления).

        Args:
            df: DataFrame с колонками high, low, close.
            period: Период сглаживания.

        Returns:
            DataFrame с колонкой adx.
        """
        up = df["high"].diff()
        down = -df["low"].diff()
        plus_dm = up.where((up > down) & (up > 0), 0.0)
        minus_dm = down.where((down > up) & (down > 0), 0.0)

        prev_close = df["close"].shift(1)
        tr = pd.concat(
            [df["high"] - df["low"],
             (df["high"] - prev_close).abs(),
             (df["low"] - prev_close).abs()],
            axis=1,
        ).max(axis=1)

        atr = tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
        plus_di = 100 * plus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr
        minus_di = 100 * minus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr
        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, pd.NA)
        df["adx"] = dx.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
        return df

    # ------------------------------------------------------------------- OBV
    @staticmethod
    def calculate_obv(df: pd.DataFrame) -> pd.DataFrame:
        """Добавляет obv (On-Balance Volume).

        Args:
            df: DataFrame с колонками close, volume.

        Returns:
            DataFrame с колонкой obv.
        """
        direction = df["close"].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        df["obv"] = (direction * df["volume"]).cumsum()
        return df

    # ------------------------------------------------------------------ VWAP
    @staticmethod
    def calculate_vwap(df: pd.DataFrame) -> pd.DataFrame:
        """Добавляет vwap — внутридневный, сбрасывается каждый UTC-день.

        Args:
            df: DataFrame с колонками timestamp, high, low, close, volume.
                timestamp должен быть datetime-подобным.

        Returns:
            DataFrame с колонкой vwap.
        """
        if "timestamp" not in df.columns:
            raise ValueError("calculate_vwap requires 'timestamp' column")
        ts = pd.to_datetime(df["timestamp"])
        typical_price = (df["high"] + df["low"] + df["close"]) / 3
        tp_vol = typical_price * df["volume"]
        day = ts.dt.date
        df["vwap"] = tp_vol.groupby(day).cumsum() / df["volume"].groupby(day).cumsum()
        return df

    # --------------------------------------------------------------- pipeline
    @classmethod
    def apply_full(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Добавляет все индикаторы сразу (для стратегий).

        Args:
            df: DataFrame [timestamp, open, high, low, close, volume].

        Returns:
            DataFrame со всеми колонками индикаторов.
        """
        return (
            cls.calculate_ema(df)
            .pipe(cls.calculate_rsi)
            .pipe(cls.calculate_stoch_rsi)
            .pipe(cls.calculate_macd)
            .pipe(cls.calculate_bollinger_bands)
            .pipe(cls.calculate_atr)
            .pipe(cls.calculate_adx)
            .pipe(cls.calculate_obv)
            .pipe(cls.calculate_vwap)
        )
