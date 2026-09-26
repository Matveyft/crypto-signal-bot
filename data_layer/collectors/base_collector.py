"""Абстрактный сборщик данных: lifecycle, реконнект с backoff, метрики."""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Any

from data_layer.utils import setup_logging


class BaseCollector(ABC):
    """Базовый класс для всех коллекторов.

    Наследники реализуют `_run()` — бесконечный цикл сбора данных.
    При исключении происходит реконнект с exponential backoff
    (1s, 2s, 4s, ... максимум 60s).
    """

    name: str = "base"

    def __init__(
        self,
        max_backoff_sec: float = 60.0,
        base_backoff_sec: float = 1.0,
    ) -> None:
        """Инициализирует коллектор.

        Args:
            max_backoff_sec: Верхняя граница паузы между реконнектами.
            base_backoff_sec: Начальная пауза (множитель 2 на каждую ошибку).
        """
        self.logger: logging.Logger = setup_logging(f"collector.{self.name}")
        self._max_backoff_sec = max_backoff_sec
        self._base_backoff_sec = base_backoff_sec
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        # Простые счётчики мониторинга
        self.stats: dict[str, Any] = {
            "messages_processed": 0,
            "errors": 0,
            "reconnects": 0,
            "last_message_at": 0.0,
        }

    # ------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        """Запускает коллектор как фоновую задачу."""
        self._stop_event.clear()
        self._task = asyncio.create_task(self._supervise(), name=f"{self.name}-task")
        self.logger.info("Collector '%s' started", self.name)

    async def stop(self) -> None:
        """Останавливает коллектор (graceful shutdown)."""
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self.cleanup()
        self.logger.info(
            "Collector '%s' stopped (msgs=%s, errors=%s, reconnects=%s)",
            self.name,
            self.stats["messages_processed"],
            self.stats["errors"],
            self.stats["reconnects"],
        )

    async def _supervise(self) -> None:
        """Супервизор: перезапускает `_run` с exponential backoff при сбоях."""
        backoff = self._base_backoff_sec
        while not self._stop_event.is_set():
            try:
                await self._run()
                # Нормальный выход из _run — только по stop_event
                backoff = self._base_backoff_sec
            except asyncio.CancelledError:
                raise
            except Exception:
                self.stats["errors"] += 1
                self.stats["reconnects"] += 1
                self.logger.exception("Collector '%s' crashed, reconnect in %.1fs",
                                      self.name, backoff)
                await self._sleep(backoff)
                backoff = min(backoff * 2, self._max_backoff_sec)
            else:
                await self._sleep(1.0)

    async def _sleep(self, seconds: float) -> None:
        """Сон, прерываемый stop_event.

        Args:
            seconds: Длительность сна.
        """
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    # ----------------------------------------------------------------- health
    @property
    def is_healthy(self) -> bool:
        """True, если за последние 2 минуты были сообщения."""
        return (time.time() - self.stats["last_message_at"]) < 120

    @abstractmethod
    async def _run(self) -> None:
        """Основной цикл сбора данных. Должен завершаться по stop_event."""

    async def cleanup(self) -> None:
        """Освобождение ресурсов (переопределяется при необходимости)."""
