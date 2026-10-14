"""Главный раннер: скан символов -> сигналы -> БД -> отслеживание позиций.

Запуск: python -m scripts.run_strategy
"""

from __future__ import annotations

import asyncio
import signal as sig_module
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text

from data_layer.storage.redis_cache import RedisCache
from data_layer.storage.timescale_manager import TimescaleManager
from data_layer.utils import load_config, setup_logging
from notifications.telegram_notifier import TelegramNotifier
from strategy.signal_generator import SignalGenerator

logger = setup_logging("run_strategy")

ACCOUNT_BALANCE = 5_000.0  # USDT; подключить реальный баланс при execution layer


class StrategyRunner:
    """Оркестратор цикла сканирования и менеджмента виртуальных позиций."""

    def __init__(self, account_balance: float = ACCOUNT_BALANCE) -> None:
        """Инициализирует раннер.

        Args:
            account_balance: Размер аккаунта для расчёта позиций.
        """
        self.cfg = load_config("symbols")
        self.params = load_config("strategy_params")
        self._db = TimescaleManager()
        self._cache = RedisCache()
        self.generator = SignalGenerator(self._db, self._cache, self.params)
        self.notifier = TelegramNotifier(self._db)
        self.notifier.attach_scans(self.generator)
        self.account_balance = account_balance
        self.scan_interval = self.params.get("signal", {}).get("scan_interval_sec", 60)
        self.min_confidence = self.params.get("signal", {}).get("min_confidence", 0.6)
        self.limit_ttl = self.params.get("signal", {}).get("limit_ttl_minutes", 30)
        self._running = True

    # ------------------------------------------------------------------ main
    async def run(self) -> None:
        """Бесконечный цикл: скан -> сигналы -> трекинг позиций."""
        loop = asyncio.get_running_loop()
        for sig in (sig_module.SIGINT, sig_module.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._shutdown)
            except NotImplementedError:  # Windows
                sig_module.signal(sig, lambda *_: self._shutdown())
        logger.info("Strategy runner started (scan every %ds)", self.scan_interval)
        polling = asyncio.create_task(self.notifier.poll_loop())
        try:
            while self._running:
                try:
                    await self.scan_all()
                    await self.track_positions()
                except Exception:
                    logger.exception("Scan cycle failed")
                await self._sleep(self.scan_interval)
        finally:
            polling.cancel()
            await self.notifier.close()
            await self._cache.close()
            await self._db.close()
            logger.info("Strategy runner stopped")

    def _shutdown(self) -> None:
        """Graceful shutdown флаг."""
        self._running = False
        logger.info("Shutdown requested")

    async def _sleep(self, seconds: float) -> None:
        """Сон с возможностью раннего выхода при shutdown."""
        for _ in range(int(seconds)):
            if not self._running:
                return
            await asyncio.sleep(1)

    # ----------------------------------------------------------------- scan
    async def scan_all(self) -> None:
        """Сканирует все символы, сохраняет сигналы, шлёт уведомления."""
        # Один сигнал/позиция на символ: не дублируем сетап, пока по символу
        # есть открытая или ждущая исполнения позиция (инцидент 19:04 —
        # повторный BTC-сигнал создал позицию, которая просто истекла по TTL)
        async with self._db.engine.begin() as conn:
            busy = set((await conn.execute(text(
                "SELECT DISTINCT symbol FROM positions"
                " WHERE status IN ('open', 'pending')"
            ))).scalars())
        for symbol in self.cfg["symbols"]:
            if not self._running:
                break
            if symbol in busy:
                # Телеметрия заморожена с последнего скана до закрытия
                # позиции — помечаем строку в /scan, чтобы застывшие
                # цифры не выглядели живыми данными
                scan = self.generator.last_scans.get(symbol)
                if scan is None:
                    # Рестарт при открытой позиции: телеметрии нет вовсе —
                    # строка пропадала из /scan целиком (инцидент XRP 29.09).
                    # Создаём заглушку с прочерками вместо метрик
                    scan = {"price": None, "trend": "?", "adx": None,
                            "rsi": None, "atr_pct": None, "vol_ok": None,
                            "time_ok": False, "imb": None, "cvd30": None,
                            "funding": None, "book_age": None}
                    self.generator.last_scans[symbol] = scan
                scan["in_position"] = True
                continue
            if self.generator.in_cooldown(symbol):
                # Телеметрия не обновляется во время cooldown — помечаем
                # строку в /scan, чтобы заморозка не выглядела сбоем данных
                scan = self.generator.last_scans.get(symbol)
                if scan is not None:
                    scan["cooldown"] = True
                continue
            try:
                signal = await self.generator.generate_signals(symbol)
                if signal is None or signal["confidence"] < self.min_confidence:
                    continue
                # Размер считаем от лимитки: на исполнении теряем ровно
                # risk_per_trade_pct (расчёт от рыночного входа занижал риск
                # на величину offset — $18.9 вместо $25 при offset 0.15%)
                signal["position_size"] = self.generator.risk.calculate_position_size(
                    account_balance=self.account_balance,
                    entry_price=signal.get("limit_price") or signal["entry"],
                    stop_loss=signal["stop"],
                )
                # Cooldown включаем только после успешной записи: сбой БД
                # не должен ослеплять символ на час (инцидент 2026-09-28,
                # ETH-сигнал conf 0.90 умер на INSERT, cooldown остался)
                await self._save_signal(signal)
                self.generator.mark_signaled(symbol)
                self._notify(signal)
            except Exception:
                logger.exception("Signal processing failed for %s", symbol)

    async def _save_signal(self, signal: dict[str, Any]) -> None:
        """Сохраняет сигнал и создаёт позицию в статусе pending (ждёт лимитку).

        Args:
            signal: Сигнал от SignalGenerator.
        """
        limit = signal.get("limit_price") or signal["entry"]
        query = text(
            "INSERT INTO signals (symbol, strategy, side, entry, stop, target1,"
            " target2, confidence, reason) VALUES"
            " (:symbol, :strategy, :side, :entry, :stop, :t1, :t2, :confidence, :reason)"
            " RETURNING id"
        )
        async with self._db.engine.begin() as conn:
            result = await conn.execute(query, {
                "symbol": signal["symbol"], "strategy": signal["strategy"],
                "side": signal["type"], "entry": signal["entry"],
                "stop": signal["stop"], "t1": signal["targets"][0],
                "t2": signal["targets"][1] if len(signal["targets"]) > 1 else None,
                "confidence": signal["confidence"], "reason": signal["reason"][:500],
            })
            signal_id = result.scalar_one()
            await conn.execute(text(
                "INSERT INTO positions (signal_id, symbol, side, limit_price,"
                " entry, stop, target, size, status, trailing_active) VALUES"
                " (:sid, :symbol, :side, :limit, :entry, :stop,"
                " :target, :size, 'pending', FALSE)"
            ), {
                "sid": signal_id, "symbol": signal["symbol"], "side": signal["type"],
                "limit": limit, "entry": limit,
                "stop": signal["stop"],
                "target": signal["targets"][-1],
                "size": signal["position_size"]["qty"],
            })

    def _send(self, coro: Any, what: str) -> None:
        """Планирует отправку уведомления, логируя ошибки задачи.

        Голый ensure_future молча проглатывает исключения — так терялись
        сообщения канала, пока это не нашли руками.

        Args:
            coro: Корутина отправки (notifier.send_channel / send_admin).
            what: Имя уведомления для лога.
        """
        task = asyncio.ensure_future(coro)

        def _on_done(t: asyncio.Task) -> None:
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                logger.error("Notify failed (%s): %s", what, exc, exc_info=exc)

        task.add_done_callback(_on_done)

    def _notify(self, signal: dict[str, Any]) -> None:
        """Уведомление о сигнале (пока print; сюда встраивается Telegram/webhook).

        Args:
            signal: Сигнал.
        """
        size = signal["position_size"]
        print("=" * 70)
        print(f"СИГНАЛ {signal['type']} | {signal['symbol']} | {signal['strategy']}"
              f" | confidence={signal['confidence']:.2f}")
        print(f"лимит={signal.get('limit_price', signal['entry']):.6g} "
              f"stop={signal['stop']:.6g} "
              f"targets={[round(t, 6) for t in signal['targets']]}")
        print(f"size={size['qty']:.6g} (risk ${size['risk_amount']}, "
              f"margin ${size['margin_usdt']})")
        print(f"reason: {signal['reason']}")
        print("=" * 70)
        html = self.notifier.format_signal(signal)
        self._send(self.notifier.send_channel(html), f"signal {signal['symbol']}")
        self._send(self.notifier.send_admin(html), f"signal-admin {signal['symbol']}")
        logger.info("Signal saved: %s %s %s conf=%.2f", signal["symbol"],
                    signal["type"], signal["strategy"], signal["confidence"])

    # --------------------------------------------------------------- tracking
    async def track_positions(self) -> None:
        """Проверяет позиции: исполнение лимиток, стоп/тейк/трейлинг."""
        async with self._db.engine.begin() as conn:
            rows = (await conn.execute(text(
                "SELECT id, symbol, side, entry, stop, target, limit_price,"
                " status, trailing_active, opened_at"
                " FROM positions WHERE status IN ('open', 'pending')"
            ))).mappings().all()

        for pos in rows:
            win = await self._get_price_window(pos["symbol"], pos["opened_at"])
            if win is None:
                continue
            if pos["status"] == "pending":
                await self._check_pending(dict(pos), win)
            else:
                await self._check_position(dict(pos), win)

    async def _check_pending(self, pos: dict[str, Any], win: dict[str, float]) -> None:
        """Pending-позиция: ждём касания лимитки или отменяем по таймауту.

        Args:
            pos: Строка позиции со статусом pending.
            win: Окно цены {'high', 'low', 'close'}.
        """
        limit = pos["limit_price"] or pos["entry"]
        now = datetime.now(tz=timezone.utc)
        age_min = (now - pos["opened_at"]).total_seconds() / 60

        filled = (pos["side"] == "LONG" and win["low"] <= limit) or \
                 (pos["side"] == "SHORT" and win["high"] >= limit)
        if filled:
            async with self._db.engine.begin() as conn:
                await conn.execute(text(
                    "UPDATE positions SET status = 'open', entry = :entry"
                    " WHERE id = :id"
                ), {"entry": limit, "id": pos["id"]})
            pos["entry"] = limit
            logger.info("Pending %s %s: filled at %.6g", pos["symbol"],
                        pos["side"], limit)
            self._send(self.notifier.send_channel(
                self.notifier.format_lifecycle(pos, "filled")),
                f"filled {pos['symbol']}")
        elif age_min > self.limit_ttl:
            async with self._db.engine.begin() as conn:
                await conn.execute(text(
                    "UPDATE positions SET status = 'cancelled',"
                    " closed_at = :now WHERE id = :id"
                ), {"now": now, "id": pos["id"]})
            logger.info("Pending %s %s: cancelled (TTL)", pos["symbol"], pos["side"])
            self._send(self.notifier.send_channel(
                self.notifier.format_lifecycle(pos, "cancelled")),
                f"cancelled {pos['symbol']}")

    async def _get_price_window(
        self, symbol: str, since: datetime
    ) -> dict[str, float] | None:
        """Окно экстремумов: текущая 1m-свеча (Redis) + закрытые после `since`.

        Проверка одной last-price пропускала хвосты свечей между сканами —
        тейки «недосчитывались» (реплей по свечам дал TP 8 против 3 у трекера).
        Уровни проверяются против high/low объединённого окна; свечи старше
        минуты создания позиции отсечены — лимитка не может «исполниться»
        хвостом, который был до сигнала (минус свеча самого сигнала остаётся).

        Args:
            symbol: Символ ccxt.
            since: Момент создания позиции (opened_at).

        Returns:
            dict {'high', 'low', 'close'} или None, если данных нет.
        """
        candle = await self._cache.get_candle(symbol, "1m")
        df = await self._db.fetch_ohlcv(symbol, "1m", limit=2)
        if not df.empty:
            df = df[df["timestamp"] >= since - timedelta(minutes=1)]
        highs, lows, close = [], [], None
        if candle is not None:
            highs.append(float(candle["high"]))
            lows.append(float(candle["low"]))
            close = float(candle["close"])
        if not df.empty:
            highs.append(float(df["high"].max()))
            lows.append(float(df["low"].min()))
            close = close if close is not None else float(df["close"].iloc[-1])
        if close is None:
            return None
        return {"high": max(highs), "low": min(lows), "close": close}

    async def _check_position(self, pos: dict[str, Any], win: dict[str, float]) -> None:
        """Применяет правила выхода к одной позиции.

        Уровни проверяются против high/low окна; при касании стопа и тейка
        в одном окне приоритет у стопа (консервативно, синхронно с
        analyze_quality). Оба выхода исполняются по цене уровня: стоп —
        рыночный (проскальзывание не моделируем), тейк — лимитный.

        Args:
            pos: Строка позиции (id, side, entry, stop, target, trailing_active).
            win: Окно цены {'high', 'low', 'close'}.
        """
        is_long = pos["side"] == "LONG"
        exit_price: float | None = None
        status = ""

        if is_long:
            if win["low"] <= pos["stop"]:
                exit_price, status = pos["stop"], "stopped"
            elif win["high"] >= pos["target"]:
                exit_price, status = pos["target"], "target"
        else:
            if win["high"] >= pos["stop"]:
                exit_price, status = pos["stop"], "stopped"
            elif win["low"] <= pos["target"]:
                exit_price, status = pos["target"], "target"

        if exit_price is not None:
            pnl_pct = (exit_price - pos["entry"]) / pos["entry"] * 100
            if not is_long:
                pnl_pct = -pnl_pct
            async with self._db.engine.begin() as conn:
                await conn.execute(text(
                    "UPDATE positions SET status = :status, closed_at = :now,"
                    " exit_price = :exit, pnl_pct = :pnl WHERE id = :id"
                ), {"status": status, "now": datetime.now(tz=timezone.utc),
                    "exit": exit_price, "pnl": pnl_pct, "id": pos["id"]})
            logger.info("Position %s %s closed: %s @ %.6g (pnl %.2f%%)",
                        pos["id"], pos["symbol"], status, exit_price, pnl_pct)
            pos["exit_price"] = exit_price
            pos["pnl_pct"] = pnl_pct
            event = "tp" if status == "target" else "sl"
            self._send(self.notifier.send_channel(
                self.notifier.format_lifecycle(pos, event)),
                f"{event} {pos['symbol']}")
            return

        # Trailing: при 1:1 переносим стоп в безубыток
        if not pos["trailing_active"]:
            activate = self.generator.risk.should_trail_stop(
                win["close"], pos["entry"], pos["stop"]
            )
            if activate:
                breakeven = pos["entry"]
                async with self._db.engine.begin() as conn:
                    await conn.execute(text(
                        "UPDATE positions SET trailing_active = TRUE,"
                        " stop = :stop WHERE id = :id"
                    ), {"stop": breakeven, "id": pos["id"]})
                logger.info("Position %s %s: trailing activated, stop -> breakeven %.6g",
                            pos["id"], pos["symbol"], breakeven)
                self._send(self.notifier.send_channel(
                    self.notifier.format_lifecycle(pos, "breakeven")),
                    f"breakeven {pos['symbol']}")


async def main() -> None:
    """Точка входа."""
    runner = StrategyRunner()
    await runner.run()


if __name__ == "__main__":
    asyncio.run(main())
