"""Utilities for matching trade lots when computing realized profit."""

from __future__ import annotations

from typing import Dict, List


Lot = Dict[str, float]
PositionState = Dict[str, List[Lot] | float | None]


def create_position() -> PositionState:
    """Return a fresh position state for tracking lots.

    The returned mapping stores open long and short lots along with summary
    information that mirrors the previous ``shares``/``avg_cost`` structure so
    existing calculations that rely on those keys continue to function.
    """

    return {
        "long_lots": [],
        "short_lots": [],
        "shares": 0.0,
        "avg_cost": 0.0,
        "last_price": None,
    }


def _validate_method(method: str | None) -> str:
    normalized = (method or "fifo").strip().lower()
    return "lifo" if normalized == "lifo" else "fifo"


def _total_quantity(lots: List[Lot]) -> float:
    return sum(lot["qty"] for lot in lots)


def _consume_lots(lots: List[Lot], quantity: float, *, price: float, method: str, closing_side: str) -> tuple[float, float]:
    """Consume lots and compute realized profit.

    Parameters
    ----------
    lots:
        Collection of open lots to draw down.
    quantity:
        Number of shares to close from the existing position.
    price:
        Execution price of the closing trade.
    method:
        Either ``"fifo"`` or ``"lifo"`` to determine consumption order.
    closing_side:
        ``"SELL"`` when closing long lots, ``"BUY"`` when covering shorts.

    Returns
    -------
    tuple[float, float]
        Realized profit (or loss) from the matched quantity and the total
        number of shares that were consumed.
    """

    realized = 0.0
    consumed = 0.0
    remaining = quantity

    while remaining > 0 and lots:
        index = 0 if method == "fifo" else -1
        lot = lots[index]
        lot_qty = lot["qty"]
        take = min(remaining, lot_qty)
        lot_price = lot["price"]

        if closing_side == "SELL":
            realized += (price - lot_price) * take
        else:  # BUY covering short lots
            realized += (lot_price - price) * take

        lot["qty"] = lot_qty - take
        consumed += take
        remaining -= take

        if lot["qty"] <= 1e-9:
            lots.pop(index)

    return realized, consumed


def _append_lot(lots: List[Lot], quantity: float, price: float) -> None:
    lots.append({"qty": quantity, "price": price})


def _update_position_summary(position: PositionState) -> None:
    long_lots: List[Lot] = position.get("long_lots", [])  # type: ignore[assignment]
    short_lots: List[Lot] = position.get("short_lots", [])  # type: ignore[assignment]

    total_long = _total_quantity(long_lots)
    total_short = _total_quantity(short_lots)

    net = total_long - total_short
    position["shares"] = net

    avg_cost = 0.0
    if total_long > 0 and net >= 0:
        avg_cost = sum(lot["qty"] * lot["price"] for lot in long_lots) / total_long
    elif total_short > 0 and net <= 0:
        avg_cost = sum(lot["qty"] * lot["price"] for lot in short_lots) / total_short

    position["avg_cost"] = avg_cost


def apply_trade(
    position: PositionState,
    side: str,
    qty: float,
    price: float,
    *,
    fee: float | None = None,
    method: str | None = None,
) -> float:
    """Apply a trade to ``position`` and return the realized profit/loss."""

    method_value = _validate_method(method)
    fee_value = abs(float(fee or 0.0))

    long_lots: List[Lot] = position.setdefault("long_lots", [])  # type: ignore[assignment]
    short_lots: List[Lot] = position.setdefault("short_lots", [])  # type: ignore[assignment]

    qty = float(qty)
    price = float(price)

    realized = 0.0

    if side == "BUY":
        total_short = _total_quantity(short_lots)
        qty_to_close = min(qty, total_short)
        closed_realized, consumed = _consume_lots(
            short_lots, qty_to_close, price=price, method=method_value, closing_side="BUY"
        )
        realized += closed_realized

        closed_fee = fee_value * (consumed / qty) if qty else 0.0
        realized -= closed_fee

        remaining = max(qty - consumed, 0.0)
        open_fee = fee_value - closed_fee
        if remaining > 0:
            effective_price = price
            if open_fee:
                effective_price += open_fee / remaining
            _append_lot(long_lots, remaining, effective_price)

    elif side == "SELL":
        total_long = _total_quantity(long_lots)
        qty_to_close = min(qty, total_long)
        closed_realized, consumed = _consume_lots(
            long_lots, qty_to_close, price=price, method=method_value, closing_side="SELL"
        )
        realized += closed_realized

        closed_fee = fee_value * (consumed / qty) if qty else 0.0
        realized -= closed_fee

        remaining = max(qty - consumed, 0.0)
        open_fee = fee_value - closed_fee
        if remaining > 0:
            effective_price = price
            if open_fee:
                effective_price -= open_fee / remaining
            _append_lot(short_lots, remaining, effective_price)

    position["last_price"] = price
    _update_position_summary(position)

    return realized

