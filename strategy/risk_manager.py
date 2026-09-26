"""Риск-менеджмент: stop-loss, take-profit, размер позиции, trailing."""

from __future__ import annotations

from typing import Any


class RiskManager:
    """Расчёт параметров сделки при риске 1-2% на позицию."""

    def __init__(self, params: dict[str, Any] | None = None) -> None:
        """Инициализирует менеджер.

        Args:
            params: Секция 'risk_management' из strategy_params.yaml.
        """
        params = params or {}
        self.risk_per_trade_pct = float(params.get("risk_per_trade_pct", 1.5))
        self.stop_atr_multiplier = float(params.get("stop_loss_atr_multiplier", 1.5))
        self.risk_reward_ratio = float(params.get("risk_reward_ratio", 2.0))
        self.trailing_activation_rr = float(params.get("trailing_stop_activation_rr", 1.0))
        self.leverage = float(params.get("leverage", 5))

    # ------------------------------------------------------------- stop loss
    def calculate_stop_loss(
        self,
        entry_price: float,
        atr: float,
        side: str,
        swing_level: float | None = None,
    ) -> float:
        """Стоп = max(1.5xATR, за свинг-уровнем) с запасом 0.3xATR.

        Логика: ATR-стоп — база; если свинг ближе ATR-стопа, но дальше
        0.5xATR — ставим за свинг (логичнее структурно).

        Args:
            entry_price: Цена входа.
            atr: Текущий ATR.
            side: 'LONG' | 'SHORT'.
            swing_level: Свинг-лоу (для LONG) или свинг-хай (для SHORT).

        Returns:
            Цена стоп-лосса.
        """
        atr_stop = atr * self.stop_atr_multiplier
        if side == "LONG":
            stop = entry_price - atr_stop
            if swing_level is not None and swing_level < entry_price:
                structural = swing_level - 0.3 * atr  # за свингом с запасом
                # Стоп за структурой, но не ближе 0.5 ATR и не дальше 2.5 ATR
                stop = max(structural, entry_price - 2.5 * atr)
                if entry_price - stop < 0.5 * atr:  # слишком близко — раздвигаем
                    stop = entry_price - atr_stop
        else:
            stop = entry_price + atr_stop
            if swing_level is not None and swing_level > entry_price:
                structural = swing_level + 0.3 * atr
                stop = min(structural, entry_price + 2.5 * atr)
                if stop - entry_price < 0.5 * atr:
                    stop = entry_price + atr_stop
        return round(stop, 8)

    # ------------------------------------------------------------ take profit
    def calculate_take_profit(
        self,
        entry_price: float,
        stop_loss: float,
        side: str,
        resistance_levels: list[float] | None = None,
        risk_reward: float | None = None,
    ) -> tuple[float, float]:
        """Две цели: ближайший уровень R/R 1.5 и R/R 2.0 (или сопротивление).

        Args:
            entry_price: Цена входа.
            stop_loss: Цена стопа.
            side: 'LONG' | 'SHORT'.
            resistance_levels: Уровни S/R по направлению сделки.
            risk_reward: Базовый R/R (по умолчанию из конфига).

        Returns:
            (target1, target2).
        """
        rr = risk_reward or self.risk_reward_ratio
        risk = abs(entry_price - stop_loss)
        if side == "LONG":
            tp1, tp2 = entry_price + 0.75 * rr * risk, entry_price + rr * risk
            # TP1 фиксируем перед ближайшим сопротивлением
            if resistance_levels:
                ahead = [lv for lv in resistance_levels if entry_price < lv < tp1 + risk]
                if ahead:
                    tp1 = max(min(ahead) - 0.1 * risk, entry_price + 0.5 * risk)
        else:
            tp1, tp2 = entry_price - 0.75 * rr * risk, entry_price - rr * risk
            if resistance_levels:
                ahead = [lv for lv in resistance_levels if tp2 - risk < lv < entry_price]
                if ahead:
                    tp1 = min(max(ahead) + 0.1 * risk, entry_price - 0.5 * risk)
        return round(tp1, 8), round(tp2, 8)

    # --------------------------------------------------------- position size
    def calculate_position_size(
        self,
        account_balance: float,
        entry_price: float,
        stop_loss: float,
        leverage: float | None = None,
    ) -> dict[str, float]:
        """Размер позиции при фиксированном риске на сделку.

        qty = balance * risk% / |entry - stop| — размер в монетах.
        Неional = qty * entry; проверяем маржу с учётом плеча.

        Args:
            account_balance: Баланс в USDT.
            entry_price: Цена входа.
            stop_loss: Цена стопа.
            leverage: Плечо (по умолчанию из конфига).

        Returns:
            dict: qty, notional_usdt, margin_usdt, risk_amount, risk_pct.

        Raises:
            ValueError: Некорректные входы (нулевой риск/цена).
        """
        if account_balance <= 0 or entry_price <= 0:
            raise ValueError(f"Invalid balance/price: {account_balance}, {entry_price}")
        risk_amount = account_balance * self.risk_per_trade_pct / 100
        risk_per_unit = abs(entry_price - stop_loss)
        if risk_per_unit <= 0:
            raise ValueError("Stop loss equals entry — zero risk distance")

        qty = risk_amount / risk_per_unit
        notional = qty * entry_price
        lev = leverage or self.leverage
        margin = notional / lev
        if margin > account_balance:  # не влезаем в маржу — режем размер
            qty = account_balance * lev / entry_price
            notional = qty * entry_price
            margin = account_balance
            risk_amount = qty * risk_per_unit
        return {
            "qty": round(qty, 8),
            "notional_usdt": round(notional, 2),
            "margin_usdt": round(margin, 2),
            "risk_amount": round(risk_amount, 2),
            "risk_pct": round(risk_amount / account_balance * 100, 4),
        }

    # --------------------------------------------------------------- trailing
    def should_trail_stop(
        self,
        current_price: float,
        entry_price: float,
        stop_loss: float,
    ) -> bool:
        """Активировать ли trailing: цена прошла 1:1 к риску.

        Args:
            current_price: Текущая цена.
            entry_price: Цена входа.
            stop_loss: Текущий стоп.

        Returns:
            True, если пора переводить стоп в безубыток/trailing.
        """
        risk = abs(entry_price - stop_loss)
        if risk <= 0:
            return False
        if stop_loss < entry_price:  # LONG: стоп ниже входа
            return current_price >= entry_price + self.trailing_activation_rr * risk
        # SHORT: стоп выше входа
        return current_price <= entry_price - self.trailing_activation_rr * risk

    @staticmethod
    def trailing_stop_price(
        current_price: float, atr: float, side: str, atr_multiplier: float = 1.0
    ) -> float:
        """Новая цена trailing-стопа (только в сторону прибыли).

        Args:
            current_price: Текущая цена.
            atr: Текущий ATR.
            side: 'LONG' | 'SHORT'.
            atr_multiplier: Отступ трейлинга в ATR.

        Returns:
            Цена нового стопа.
        """
        if side == "LONG":
            return round(current_price - atr_multiplier * atr, 8)
        return round(current_price + atr_multiplier * atr, 8)
