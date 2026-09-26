"""Общие утилиты: конфигурация, логирование, константы таймфреймов."""

import logging
import logging.handlers
import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

TIMEFRAME_MINUTES: dict[str, int] = {
    "1m": 1, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "4h": 240, "1d": 1440,
}


def load_env() -> None:
    """Загружает .env из корня проекта (идемпотентно)."""
    load_dotenv(PROJECT_ROOT / ".env")


def load_config(name: str) -> dict[str, Any]:
    """Читает YAML из config/ и подставляет переменные окружения.

    Args:
        name: Имя файла без расширения, например "database".

    Returns:
        Словарь с конфигурацией.
    """
    load_env()
    path = PROJECT_ROOT / "config" / f"{name}.yaml"
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    # ${VAR} -> значение из окружения
    for key, value in os.environ.items():
        raw = raw.replace(f"${{{key}}}", str(value))
    return yaml.safe_load(raw)


def setup_logging(name: str = "crypto_signal_bot") -> logging.Logger:
    """Настраивает логгер: консоль + файл с daily rotation (JSON-ready формат).

    Args:
        name: Имя логгера.

    Returns:
        Настроенный логгер.
    """
    load_env()
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    log_dir = PROJECT_ROOT / os.getenv("LOG_DIR", "logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    if logger.handlers:  # уже настроен
        return logger
    logger.setLevel(level)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    )

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    file_handler = logging.handlers.TimedRotatingFileHandler(
        log_dir / "strategy.log", when="midnight", backupCount=14, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger


def timeframe_to_timedelta(tf: str) -> Any:
    """Возвращает timedelta для таймфрейма.

    Args:
        tf: Строка таймфрейма ('1m', '5m', '1h', '4h', '1d').

    Returns:
        datetime.timedelta длительности таймфрейма.

    Raises:
        ValueError: Неизвестный таймфрейм.
    """
    from datetime import timedelta

    if tf not in TIMEFRAME_MINUTES:
        raise ValueError(f"Unknown timeframe: {tf}")
    return timedelta(minutes=TIMEFRAME_MINUTES[tf])


def symbol_to_bybit(symbols_ccxt: str) -> str:
    """Конвертирует ccxt-символ 'BTC/USDT:USDT' в bybit-формат 'BTCUSDT'.

    Args:
        symbols_ccxt: Символ в формате ccxt.

    Returns:
        Символ в формате Bybit.
    """
    return symbols_ccxt.replace("/USDT:USDT", "USDT").replace("/", "")
