"""Telegram-нотификатор: канал сигналов + личный бот со статистикой.

Работает на чистом aiohttp (зависимость уже есть от ccxt) — long polling
getUpdates, sendMessage с HTML-разметкой. Токен и настройки из .env:
    TELEGRAM_BOT_TOKEN   — токен от @BotFather
    TELEGRAM_CHANNEL     — канал, например @matvey_crypto_signals
    TELEGRAM_ADMIN_CHAT_ID — ваш числовой chat_id (личка для команд)
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any

import aiohttp

API_BASE = "https://api.telegram.org/bot"


class TelegramNotifier:
    """Отправка сообщений и обработка команд личного бота."""

    def __init__(self, db: Any) -> None:
        """Инициализация нотификатора.

        Args:
            db: TimescaleManager — для команд статистики.
        """
        self.logger = logging.getLogger("crypto_signal_bot.telegram")
        self._db = db
        self._token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self._channel = os.getenv("TELEGRAM_CHANNEL", "")
        self._admin_chat = os.getenv("TELEGRAM_ADMIN_CHAT_ID", "")
        self._session: aiohttp.ClientSession | None = None
        self._offset = 0
        self._scans_provider: Any = None  # SignalGenerator.last_scans

    def attach_scans(self, generator: Any) -> None:
        """Подключает источник телеметрии сканов (SignalGenerator).

        Args:
            generator: SignalGenerator с атрибутом last_scans.
        """
        self._scans_provider = generator

    @property
    def enabled(self) -> bool:
        """Нотификации включены, если задан токен."""
        return bool(self._token)

    async def _api(self, method: str, **params: Any) -> dict[str, Any] | None:
        """Вызов метода Telegram Bot API.

        Args:
            method: Имя метода (sendMessage, getUpdates...).
            **params: Параметры метода.

        Returns:
            Ответ API или None при ошибке (не роняем стратегию).
        """
        if not self.enabled:
            return None
        try:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=35)
                )
            async with self._session.post(
                f"{API_BASE}{self._token}/{method}", json=params
            ) as resp:
                return await resp.json()
        except Exception as exc:
            self.logger.warning("Telegram API %s failed: %s", method, exc)
            return None

    # ------------------------------------------------------------ отправка
    async def send_channel(self, html: str) -> None:
        """Отправляет сообщение в канал сигналов.

        Args:
            html: Текст с HTML-разметкой.
        """
        if not self._channel:
            return
        await self._api(
            "sendMessage", chat_id=self._channel, text=html,
            parse_mode="HTML", disable_web_page_preview=True,
        )

    async def send_admin(self, html: str) -> None:
        """Отправляет сообщение в личку администратору.

        Args:
            html: Текст с HTML-разметкой.
        """
        if not self._admin_chat:
            return
        await self._api(
            "sendMessage", chat_id=self._admin_chat, text=html,
            parse_mode="HTML", disable_web_page_preview=True,
        )

    # ------------------------------------------------------------- формат
    def format_signal(self, signal: dict[str, Any]) -> str:
        """Форматирует сигнал для канала (HTML, лимитный вход).

        Args:
            signal: Сигнал от SignalGenerator (type, strategy, entry,
                limit_price, stop, targets, confidence, reason, position_size).

        Returns:
            Готовое HTML-сообщение.
        """
        side = signal["type"]
        icon = "🟢 LONG" if side == "LONG" else "🔴 SHORT"
        coin = signal["symbol"].split("/")[0]
        strat = {
            "trend_pullback": "тренд-откат", "mean_reversion": "разворот",
            "breakout_retest": "пробой-ретест",
        }.get(signal["strategy"], signal["strategy"])
        limit = signal.get("limit_price") or signal["entry"]
        entry = signal["entry"]
        offset_pct = (limit - entry) / entry * 100
        t1, t2 = signal["targets"][0], signal["targets"][1]
        risk = abs(entry - signal["stop"])
        rr1 = abs(t1 - entry) / risk
        size = signal.get("position_size", {})
        now = datetime.now(tz=timezone.utc).strftime("%H:%M UTC")
        return (
            f"📊 <b>{icon} • {signal['symbol']} • {strat}</b>\n\n"
            f"⚡ Лимит: <code>{limit:.6g}</code> ({offset_pct:+.2f}% от текущей)\n"
            f"🛑 Стоп: <code>{signal['stop']:.6g}</code>\n"
            f"🎯 TP1: <code>{t1:.6g}</code> (R:R 1:{rr1:.1f})\n"
            f"🎯 TP2: <code>{t2:.6g}</code>\n\n"
            f"💰 Объём: <b>{size.get('qty', 0):.6g} {coin}</b>"
            f" ≈ {size.get('notional_usdt', 0):.0f} USDT\n"
            f"🏦 Маржа 5x: {size.get('margin_usdt', 0):.0f} USDT\n"
            f"❗ Риск: {size.get('risk_amount', 0):.0f} USDT"
            f" ({size.get('risk_pct', 0):.1f}% депозита)\n\n"
            f"📈 {signal.get('reason', '')}\n"
            f"🎯 Уверенность: {signal['confidence']*100:.0f}%\n\n"
            f"🕐 {now}\n#{coin} #{side}"
        )

    def format_lifecycle(self, pos: dict[str, Any], event: str) -> str:
        """Форматирует событие жизненного цикла позиции.

        Args:
            pos: Строка позиции (symbol, side, entry, stop, target...).
            event: 'filled' | 'cancelled' | 'breakeven' | 'tp' | 'sl'.

        Returns:
            HTML-сообщение.
        """
        sym = pos["symbol"]
        side = pos["side"]
        if event == "filled":
            return (f"✅ <b>{sym} {side}: лимит исполнен</b>\n"
                    f"Вход: <code>{pos['entry']:.6g}</code> — позиция открыта")
        if event == "cancelled":
            return (f"🔌 <b>{sym} {side}: лимит не исполнен</b>\n"
                    f"Сигнал отменён (30 мин). Цена ушла без нас")
        if event == "breakeven":
            return (f"🔄 <b>{sym} {side}: 1:1 пройдено</b>\n"
                    f"Стоп перенесён в безубыток: <code>{pos['entry']:.6g}</code>")
        if event == "tp":
            return (f"🏁 <b>{sym} {side}: закрыт по TP</b>\n"
                    f"Выход: <code>{pos.get('exit_price', 0):.6g}</code>"
                    f" ({pos.get('pnl_pct', 0):+.2f}%)")
        if event == "sl":
            return (f"🛑 <b>{sym} {side}: закрыт по стопу</b>\n"
                    f"Выход: <code>{pos.get('exit_price', 0):.6g}</code>"
                    f" ({pos.get('pnl_pct', 0):+.2f}%)")
        return f"ℹ️ {sym} {side}: {event}"

    # -------------------------------------------------------------- команды
    async def poll_loop(self) -> None:
        """Бесконечный long polling: обрабатывает команды личного бота."""
        if not self.enabled:
            self.logger.info("Telegram: токен не задан — нотификации выключены")
            return
        self.logger.info("Telegram bot polling started")
        while True:
            try:
                resp = await self._api(
                    "getUpdates", offset=self._offset, timeout=25,
                    allowed_updates=["message"],
                )
                if not resp or not resp.get("ok"):
                    await asyncio.sleep(3)
                    continue
                for update in resp.get("result", []):
                    self._offset = update["update_id"] + 1
                    await self._handle_message(update.get("message", {}))
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception("Telegram poll error")
                await asyncio.sleep(5)

    async def _handle_message(self, message: dict[str, Any]) -> None:
        """Диспетчер входящих сообщений личного бота.

        Args:
            message: Объект message из update.
        """
        chat_id = str(message.get("chat", {}).get("id", ""))
        text = (message.get("text") or "").strip()
        if not text.startswith("/"):
            return
        command = text.split()[0].split("@")[0]

        # /start доступен всем: показывает chat_id для настройки
        if command == "/start":
            await self._api(
                "sendMessage", chat_id=chat_id,
                text=(f"Привет! Ваш chat_id: <code>{chat_id}</code>\n"
                      f"Впишите его в TELEGRAM_ADMIN_CHAT_ID (.env), "
                      f"затем доступны: /status /stats /positions"),
                parse_mode="HTML",
            )
            return

        if chat_id != self._admin_chat:
            await self._api("sendMessage", chat_id=chat_id,
                            text="⛔ Доступ только для администратора.")
            return

        if command == "/help":
            await self.send_admin(
                "Команды:\n/status — здоровье системы\n"
                "/scan — близость к сигналу по всем монетам\n"
                "/stats — статистика сигналов\n/positions — открытые позиции"
            )
        elif command == "/status":
            await self.send_admin(await self._build_status())
        elif command == "/scan":
            await self.send_admin(self._build_scan())
        elif command == "/stats":
            await self.send_admin(await self._build_stats())
        elif command == "/positions":
            await self.send_admin(await self._build_positions())

    # ------------------------------------------------------------ отчёты
    async def _build_status(self) -> str:
        """Сводка здоровья: лаг данных, счётчики, активность сервисов.

        Returns:
            HTML-текст.
        """
        from sqlalchemy import text as sql

        lag = None
        candles = signals = positions = 0
        try:
            async with self._db.engine.connect() as conn:
                r = await conn.execute(sql(
                    "SELECT round(EXTRACT(EPOCH FROM (now()-MAX(timestamp))))"
                    " FROM ohlcv WHERE timeframe='1m'"))
                lag = r.scalar()
                candles = (await conn.execute(
                    sql("SELECT count(*) FROM ohlcv"))).scalar()
                signals = (await conn.execute(
                    sql("SELECT count(*) FROM signals"))).scalar()
                positions = (await conn.execute(
                    sql("SELECT count(*) FROM positions WHERE status"
                        " IN ('open','pending')"))).scalar()
        except Exception:
            self.logger.exception("status query failed")
        return (
            f"🩺 <b>Статус системы</b>\n"
            f"Лаг данных: {lag if lag is not None else '?'} сек"
            f" {'✅' if lag is not None and lag < 120 else '⚠️'}\n"
            f"Свечей в БД: {candles}\n"
            f"Сигналов всего: {signals}\n"
            f"Активных позиций: {positions}\n"
            f"Время: {datetime.now(tz=timezone.utc).strftime('%H:%M UTC')}"
        )

    def _build_scan(self) -> str:
        """Телеметрия близости к сигналу по всем символам (для /scan).

        Показывает по каждой монете: цену, тренд D1, ADX, RSI, ATR%,
        статус пре-фильтров (vol/time) и микропоток (imb/cvd).
        Считает, сколько символов проходят все условия разом.

        Returns:
            HTML-текст с моноширинной таблицей.
        """
        if not self._scans_provider or not getattr(
            self._scans_provider, "last_scans", None
        ):
            return "⏳ Данных ещё нет — подождите первый цикл сканирования (1 мин)"
        scans = self._scans_provider.last_scans
        icon = {"uptrend": "▲", "downtrend": "▼", "sideways": "■"}
        lines = ["🔍 <b>Близость к сигналу</b>\n<pre>"]
        ready = 0
        for symbol in sorted(scans):
            s = scans[symbol]
            tr = icon.get(s["trend"], "?")
            vol = "✓" if s["vol_ok"] else "✗"
            imb = s["imb"]
            cvd = s["cvd30"]
            imb_s = f"{imb:.1f}" if imb is not None else "—"
            cvd_s = ("+" if cvd and cvd > 0 else "−") if cvd is not None else "—"
            # Полный набор пре-условий LONG-сетапа (тренд+ADX+фильтры+поток)
            if (s["trend"] == "uptrend" and s["adx"] > 25 and s["vol_ok"]
                    and s["time_ok"] and imb is not None and imb >= 1.8
                    and cvd is not None and cvd > 0):
                ready += 1
                mark = "⚡"
            else:
                mark = " "
            lines.append(
                f"{mark}{symbol.split('/')[0]:6}{s['price']:<9.6g}{tr} "
                f"ADX{s['adx']:<3.0f}RSI{s['rsi']:<3.0f}"
                f"vol{vol} imb{imb_s:<5}cvd{cvd_s}"
            )
        lines.append("</pre>")
        lines.append(
            f"Пре-условия сетапа выполняются у {ready} из {len(scans)} монет"
            f" (нужен ещё откат к уровню + свечное подтверждение M15)"
        )
        lines.append("⬤ ▲ тренд вверх ▼ вниз ■ боковик ⚡ поток подтверждён")
        return "\n".join(lines)

    async def _build_stats(self) -> str:
        """Статистика по закрытым позициям: win rate, PnL, по стратегиям.

        Returns:
            HTML-текст.
        """
        from sqlalchemy import text as sql

        lines = ["📊 <b>Статистика</b>"]
        try:
            async with self._db.engine.connect() as conn:
                r = (await conn.execute(sql(
                    "SELECT count(*) FILTER (WHERE status IN ('target','stopped')),"
                    " count(*) FILTER (WHERE pnl_pct > 0),"
                    " COALESCE(round(sum(pnl_pct)::numeric, 2), 0),"
                    " COALESCE(round(avg(pnl_pct)::numeric, 2), 0)"
                    " FROM positions"))).one()
                closed, wins, total_pnl, avg_pnl = r
                if closed:
                    wr = wins / closed * 100
                    lines.append(
                        f"Закрыто сделок: {closed} | Win rate: {wr:.0f}%\n"
                        f"Суммарный PnL: {total_pnl:+}% | Средний: {avg_pnl:+}%")
                else:
                    lines.append("Закрытых сделок пока нет.")
                r = (await conn.execute(sql(
                    "SELECT count(*), min(created_at), max(created_at)"
                    " FROM signals"))).one()
                lines.append(f"Сигналов всего: {r[0]}"
                             f" (первый: {r[1].strftime('%d.%m') if r[1] else '—'})")
                rows = (await conn.execute(sql(
                    "SELECT strategy, count(*),"
                    " count(*) FILTER (WHERE p.pnl_pct > 0)"
                    " FROM positions p JOIN signals s ON s.id = p.signal_id"
                    " WHERE p.status IN ('target','stopped')"
                    " GROUP BY strategy"))).all()
                for strat, total, win in rows:
                    lines.append(f"• {strat}: {total} сделок, {win} в плюс")
                rows = (await conn.execute(sql(
                    "SELECT status, count(*) FROM positions"
                    " GROUP BY status"))).all()
                lines.append("Позиции: " + ", ".join(f"{s}={n}" for s, n in rows))
        except Exception:
            self.logger.exception("stats query failed")
        return "\n".join(lines)

    async def _build_positions(self) -> str:
        """Список активных (pending/open) позиций.

        Returns:
            HTML-текст.
        """
        from sqlalchemy import text as sql

        lines = ["📌 <b>Активные позиции</b>"]
        try:
            async with self._db.engine.connect() as conn:
                rows = (await conn.execute(sql(
                    "SELECT symbol, side, status, entry, stop, target,"
                    " limit_price FROM positions"
                    " WHERE status IN ('open','pending')"
                    " ORDER BY opened_at DESC"))).all()
            if not rows:
                lines.append("Пусто.")
            for sym, side, status, entry, stop, target, limit in rows:
                price = f"лимит <code>{limit:.6g}</code>" if status == "pending" \
                    else f"вход <code>{entry:.6g}</code>"
                lines.append(
                    f"• {sym} {side} [{status}] — {price}, "
                    f"стоп <code>{stop:.6g}</code>, тейк <code>{target:.6g}</code>")
        except Exception:
            self.logger.exception("positions query failed")
        return "\n".join(lines)

    async def close(self) -> None:
        """Закрывает HTTP-сессию."""
        if self._session and not self._session.closed:
            await self._session.close()
