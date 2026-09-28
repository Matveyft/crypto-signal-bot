"""Bybit V5 WebSocket коллектор: klines (1m), publicTrade (CVD), orderbook.50.

Поток данных:
    kline.1.{symbol}     закрытые свечи -> TimescaleDB (батчами),
                         текущие -> Redis
    publicTrade.{symbol} дельта объёма (buy - sell) -> CVD -> Redis
    orderbook.50.{symbol} стакан -> imbalance + стены -> Redis
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

import websockets

from data_layer.collectors.base_collector import BaseCollector
from data_layer.storage.redis_cache import RedisCache
from data_layer.storage.timescale_manager import TimescaleManager
from data_layer.utils import load_config, symbol_to_bybit
from features.orderbook_features import (
    IMBALANCE_SANITY_MAX,
    IMBALANCE_SANITY_MIN,
)

WS_URL = "wss://stream.bybit.com/v5/public/linear"
SUBSCRIBE_CHUNK = 10  # Bybit: максимум 10 args на один subscribe-запрос
# Столько деградировавших orderbook-сообщений подряд -> форс-ресабскрайб
# топика (биржа отвечает свежим snapshot; kline/trade-потоки не рвутся)
ORDERBOOK_RESUBSCRIBE_AFTER = 200
ORDERBOOK_RESUBSCRIBE_COOLDOWN_SEC = 60.0


def _ms_to_dt(ms: int) -> datetime:
    """Конвертирует миллисекунды epoch в timezone-aware datetime UTC.

    Args:
        ms: Unix-время в миллисекундах.

    Returns:
        datetime с таймзоной UTC.
    """
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


class BybitCollector(BaseCollector):
    """Сбор real-time данных с Bybit через публичный WebSocket."""

    name = "bybit_ws"

    def __init__(
        self,
        symbols: list[str],
        db: TimescaleManager,
        cache: RedisCache,
    ) -> None:
        """Инициализирует коллектор.

        Args:
            symbols: Список символов в формате ccxt ('BTC/USDT:USDT').
            db: Менеджер TimescaleDB.
            cache: Redis-кэш.
        """
        super().__init__()
        self._symbols = symbols
        self._bybit_symbols = {symbol_to_bybit(s): s for s in symbols}
        self._db = db
        self._cache = cache
        self._ping_interval = int(os.getenv("WS_PING_INTERVAL_SEC", "20"))
        self._flush_interval = int(os.getenv("CANDLE_FLUSH_INTERVAL_SEC", "10"))
        # Буфер закрытых свечей на запись в БД
        self._candle_buffer: list[dict[str, Any]] = []
        # CVD: deque[(ts_ms, signed_volume)] на каждый символ
        self._cvd: dict[str, deque[tuple[int, float]]] = {
            s: deque(maxlen=200_000) for s in symbols
        }
        # Стакан в памяти: {symbol: {'b': [...], 'a': [...]}}
        self._books: dict[str, dict[str, list[list[str]]]] = {}
        # Счётчики подряд деградировавших стаканов (троттлинг warning'ов)
        self._degraded_books: dict[str, int] = {}
        # Последний форс-ресабскрайб топика стакана (cooldown per symbol)
        self._resubscribe_at: dict[str, float] = {}
        # Текущий WS (для ресабскрайба из обработчика стакана)
        self._ws: Any | None = None

    async def _run(self) -> None:
        """Основной цикл: connect -> subscribe -> receive loop."""
        flusher = asyncio.create_task(self._flush_loop())
        pinger: asyncio.Task[None] | None = None
        try:
            async with websockets.connect(
                WS_URL, ping_interval=None, max_queue=4096
            ) as ws:
                self._ws = ws
                await self._subscribe(ws)
                pinger = asyncio.create_task(self._ping_loop(ws))
                self.logger.info("WebSocket connected, receiving messages")
                async for raw in ws:
                    self.stats["messages_processed"] += 1
                    self.stats["last_message_at"] = time.time()
                    await self._handle_message(raw)
        finally:
            self._ws = None
            if pinger is not None:
                pinger.cancel()
            flusher.cancel()
            # Флашнем остатки буфера перед реконнектом
            await self._flush_candles()

    # ------------------------------------------------------------ subscribe
    async def _subscribe(self, ws: Any) -> None:
        """Отправляет подписки чанками по 10 аргументов.

        Args:
            ws: Активное WebSocket-соединение.
        """
        args: list[str] = []
        for b in self._bybit_symbols:
            # Внимание: интервал в WS-топиках теперь без суффикса 'm'
            # (kline.1 = минутки); старый формат kline.1m удалён биржей.
            args += [f"kline.1.{b}", f"publicTrade.{b}", f"orderbook.50.{b}"]
        for i in range(0, len(args), SUBSCRIBE_CHUNK):
            chunk = args[i : i + SUBSCRIBE_CHUNK]
            await ws.send(json.dumps({"op": "subscribe", "args": chunk}))
            await asyncio.sleep(0.05)  # мягкий rate limit
        self.logger.info("Subscribed to %d topics for %d symbols", len(args),
                         len(self._bybit_symbols))

    async def _ping_loop(self, ws: Any) -> None:
        """Отправляет ping-запросы для поддержания соединения.

        Args:
            ws: Активное WebSocket-соединение.
        """
        while True:
            await asyncio.sleep(self._ping_interval)
            await ws.send(json.dumps({"op": "ping"}))

    # ------------------------------------------------------------- messages
    async def _handle_message(self, raw: str | bytes) -> None:
        """Маршрутизирует сообщение по topic.

        Args:
            raw: Сырое WS-сообщение.
        """
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            self.logger.warning("Non-JSON WS message: %s", str(raw)[:100])
            return

        topic: str = msg.get("topic", "")
        if topic.startswith("kline."):
            await self._handle_kline(msg)
        elif topic.startswith("publicTrade."):
            self._handle_trades(msg)
        elif topic.startswith("orderbook."):
            await self._handle_orderbook(msg)
        # pong / subscribe-ack игнорируем

    async def _handle_kline(self, msg: dict[str, Any]) -> None:
        """Обрабатывает свечи: закрытые -> буфер БД, текущие -> Redis.

        Args:
            msg: Распарсенное сообщение kline.
        """
        topic = msg["topic"]  # kline.1m.BTCUSDT
        bybit_sym = topic.split(".")[-1]
        ccxt_sym = self._bybit_symbols.get(bybit_sym)
        if ccxt_sym is None:
            return
        for k in msg.get("data", []):
            candle = {
                "symbol": ccxt_sym,
                "timeframe": "1m",
                "timestamp": _ms_to_dt(int(k["start"])),
                "open": float(k["open"]),
                "high": float(k["high"]),
                "low": float(k["low"]),
                "close": float(k["close"]),
                "volume": float(k["volume"]),
            }
            if k.get("confirm", False):
                self._candle_buffer.append(candle)
            else:
                current = {k_: v for k_, v in candle.items()
                           if k_ not in ("symbol", "timeframe")}
                current["timestamp"] = int(k["start"])  # в Redis — ms int
                await self._cache.set_candle(ccxt_sym, "1m", current)

    def _handle_trades(self, msg: dict[str, Any]) -> None:
        """Накапливает signed volume для CVD и периодически пушит в Redis.

        Args:
            msg: Распарсенное сообщение publicTrade.
        """
        bybit_sym = msg["topic"].split(".")[-1]
        ccxt_sym = self._bybit_symbols.get(bybit_sym)
        if ccxt_sym is None:
            return
        dq = self._cvd[ccxt_sym]
        for trade in msg.get("data", []):
            signed = float(trade["v"]) if trade["S"] == "Buy" else -float(trade["v"])
            dq.append((int(trade["T"]), signed))

    async def _handle_orderbook(self, msg: dict[str, Any]) -> None:
        """Обновляет стакан, считает imbalance и стены, пушит в Redis.

        Деградировавший стакан не пушится (счётчик деградации растёт,
        при длительной деградации топик переподписывается). Режимы
        деградации: <10 уровней с любой стороны (рассинхрон snapshot/diff),
        нулевой объём стороны или imbalance вне sanity-границ — вторая
        сторона стакана «пустая» на вид, но по уровням формально есть.

        Args:
            msg: Распарсенное сообщение orderbook.50.
        """
        bybit_sym = msg["topic"].split(".")[-1]
        ccxt_sym = self._bybit_symbols.get(bybit_sym)
        if ccxt_sym is None:
            return
        data = msg.get("data", {})
        book = self._books.setdefault(ccxt_sym, {"b": [], "a": []})
        if msg.get("type") == "snapshot":
            book["b"] = list(data.get("b", []))
            book["a"] = list(data.get("a", []))
        else:  # delta: size == 0 -> удалить уровень
            for side in ("b", "a"):
                updates = {price: size for price, size in data.get(side, [])}
                merged = {p: s for p, s in book[side]}
                merged.update(updates)
                book[side] = [[p, s] for p, s in merged.items() if float(s) > 0]
        book["b"].sort(key=lambda x: -float(x[0]))  # bids по убыванию цены
        book["a"].sort(key=lambda x: float(x[0]))   # asks по возрастанию

        bids = [(float(p), float(s)) for p, s in book["b"][:50]]
        asks = [(float(p), float(s)) for p, s in book["a"][:50]]
        bid_vol = sum(s for _, s in bids[:10])
        ask_vol = sum(s for _, s in asks[:10])
        imbalance = (
            bid_vol / ask_vol
            if ask_vol > 0 and bid_vol > 0 else None
        )
        degraded = (
            len(bids) < 10 or len(asks) < 10
            or imbalance is None
            or not (IMBALANCE_SANITY_MIN <= imbalance <= IMBALANCE_SANITY_MAX)
        )
        if degraded:
            count = self._degraded_books.get(ccxt_sym, 0) + 1
            self._degraded_books[ccxt_sym] = count
            if count == 1 or count % 100 == 0:
                self.logger.warning(
                    "Orderbook degraded for %s (%d updates in row): "
                    "%d bid / %d ask levels, imb=%s, waiting for fresh snapshot",
                    ccxt_sym, count, len(bids), len(asks),
                    f"{imbalance:.1f}" if imbalance is not None else "n/a")
            if count % ORDERBOOK_RESUBSCRIBE_AFTER == 0:
                await self._resubscribe_orderbook(
                    ccxt_sym, msg["topic"])
            return
        self._degraded_books[ccxt_sym] = 0
        await self._cache.set_orderbook(ccxt_sym, {
            "bids": bids[:20],
            "asks": asks[:20],
            "bid_volume_top10": bid_vol,
            "ask_volume_top10": ask_vol,
            "imbalance": round(imbalance, 4),
            "updated_at": time.time(),
        })

    async def _resubscribe_orderbook(self, ccxt_sym: str, topic: str) -> None:
        """Переподписывает топик стакана, чтобы биржа прислала snapshot.

        Деградировавший после рассинхрона book восстанавливается только
        свежим snapshot, а Bybit шлёт его в ответ на подписку. Cooldown —
        не чаще раза в ORDERBOOK_RESUBSCRIBE_COOLDOWN_SEC на символ.

        Args:
            ccxt_sym: Символ ccxt (для cooldown-ключа).
            topic: Полный WS-топик ('orderbook.50.BTCUSDT').
        """
        now = time.time()
        if now - self._resubscribe_at.get(ccxt_sym, 0.0) \
                < ORDERBOOK_RESUBSCRIBE_COOLDOWN_SEC:
            return
        self._resubscribe_at[ccxt_sym] = now
        self._degraded_books[ccxt_sym] = 0
        if self._ws is None:
            return
        self.logger.warning(
            "Orderbook for %s degraded too long, resubscribing %s",
            ccxt_sym, topic)
        await self._ws.send(json.dumps({"op": "unsubscribe", "args": [topic]}))
        await self._ws.send(json.dumps({"op": "subscribe", "args": [topic]}))

    # ------------------------------------------------------------------ CVD
    async def _flush_loop(self) -> None:
        """Периодически пушит CVD в Redis и флашит свечи в БД."""
        while True:
            await asyncio.sleep(self._flush_interval)
            await self._flush_candles()
            await self._push_cvd()

    async def _push_cvd(self) -> None:
        """Считает CVD (total, 5m, 15m, 30m) и кладёт снапшот в Redis."""
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - 30 * 60 * 1000
        for sym, dq in self._cvd.items():
            while dq and dq[0][0] < cutoff:
                dq.popleft()  # чистим всё старше 30 минут
            if not dq:
                continue
            total = sum(v for _, v in dq)
            m5 = sum(v for ts, v in dq if ts >= now_ms - 5 * 60 * 1000)
            m15 = sum(v for ts, v in dq if ts >= now_ms - 15 * 60 * 1000)
            m30 = total
            await self._cache.set_cvd(sym, {
                "total": round(total, 6),
                "m5": round(m5, 6),
                "m15": round(m15, 6),
                "m30": round(m30, 6),
                "updated_at": time.time(),
            })

    async def _flush_candles(self) -> None:
        """Записывает буфер закрытых свечей в TimescaleDB батчем."""
        if not self._candle_buffer:
            return
        batch, self._candle_buffer = self._candle_buffer, []
        try:
            written = await self._db.upsert_ohlcv(batch)
            self.logger.debug("Flushed %d closed candles to DB", written)
        except Exception:
            self.stats["errors"] += 1
            self.logger.exception("Failed to flush %d candles, dropping", len(batch))
