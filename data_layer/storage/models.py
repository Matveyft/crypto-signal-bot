"""SQLAlchemy модели. TimescaleDB hypertables создаются в TimescaleManager."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    Float,
    Identity,
    Index,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Базовый класс всех моделей."""


class OHLCV(Base):
    """Свеча OHLCV для пары и таймфрейма.

    Естественный PK (symbol, timeframe, timestamp): TimescaleDB требует,
    чтобы уникальные индексы hypertable включали колонку партиционирования.
    """

    __tablename__ = "ohlcv"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), nullable=False)
    symbol: Mapped[str] = mapped_column(String(30), primary_key=True)
    timeframe: Mapped[str] = mapped_column(String(5), primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), primary_key=True, server_default=func.now()
    )
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)

    def __repr__(self) -> str:
        return (
            f"OHLCV({self.symbol} {self.timeframe} {self.timestamp} "
            f"O={self.open} H={self.high} L={self.low} C={self.close} V={self.volume})"
        )


class FundingData(Base):
    """Funding rate, Open Interest и Long/Short ratio."""

    __tablename__ = "funding_data"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), nullable=False)
    symbol: Mapped[str] = mapped_column(String(30), primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), primary_key=True, server_default=func.now()
    )
    funding_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    open_interest: Mapped[float | None] = mapped_column(Float, nullable=True)
    long_short_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)

    def __repr__(self) -> str:
        return (
            f"FundingData({self.symbol} {self.timestamp} fr={self.funding_rate} "
            f"oi={self.open_interest} lsr={self.long_short_ratio})"
        )


class Signal(Base):
    """Сгенерированный торговый сигнал (заполняется на Этапе 3-4)."""

    __tablename__ = "signals"
    __table_args__ = (Index("ix_signals_symbol_ts", "symbol", "created_at"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(30), nullable=False)
    strategy: Mapped[str] = mapped_column(String(30), nullable=False)
    side: Mapped[str] = mapped_column(String(5), nullable=False)  # LONG | SHORT
    entry: Mapped[float] = mapped_column(Float, nullable=False)
    stop: Mapped[float] = mapped_column(Float, nullable=False)
    target1: Mapped[float] = mapped_column(Float, nullable=False)
    target2: Mapped[float | None] = mapped_column(Float, nullable=True)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    reason: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Position(Base):
    """Отслеживаемая виртуальная позиция (без execution layer)."""

    __tablename__ = "positions"
    __table_args__ = (Index("ix_positions_symbol_status", "symbol", "status"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    signal_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    symbol: Mapped[str] = mapped_column(String(30), nullable=False)
    side: Mapped[str] = mapped_column(String(5), nullable=False)
    limit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    entry: Mapped[float] = mapped_column(Float, nullable=False)
    stop: Mapped[float] = mapped_column(Float, nullable=False)
    target: Mapped[float] = mapped_column(Float, nullable=False)
    size: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    trailing_active: Mapped[bool] = mapped_column(default=False)
    status: Mapped[str] = mapped_column(String(10), nullable=False, default="open")
    opened_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    exit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    pnl_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
