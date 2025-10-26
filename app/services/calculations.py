from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Any, MutableMapping


def _apply_trade_to_position(
    position: MutableMapping[str, float], side: str, qty: float, price: float
) -> float:
    """Apply a trade to an in-memory position and return realized P/L."""

    shares = float(position.get("shares", 0.0) or 0.0)
    avg_cost = float(position.get("avg_cost", 0.0) or 0.0)
    realized = 0.0

    if side == "BUY":
        remaining = qty

        if shares < 0:
            cover_qty = min(remaining, -shares)
            realized += (avg_cost - price) * cover_qty
            shares += cover_qty
            remaining -= cover_qty
            if shares == 0:
                avg_cost = 0.0

        if remaining > 0:
            cost_basis = avg_cost * shares if shares > 0 else 0.0
            cost_basis += price * remaining
            shares += remaining
            avg_cost = cost_basis / shares if shares else 0.0

    elif side == "SELL":
        remaining = qty

        if shares > 0:
            sell_qty = min(remaining, shares)
            realized += (price - avg_cost) * sell_qty
            shares -= sell_qty
            remaining -= sell_qty
            if shares == 0:
                avg_cost = 0.0

        if remaining > 0:
            short_shares = -shares if shares < 0 else 0.0
            total_proceeds = avg_cost * short_shares if short_shares else 0.0
            total_proceeds += price * remaining
            short_shares += remaining
            if short_shares:
                avg_cost = total_proceeds / short_shares
                shares = -short_shares

    position["shares"] = shares
    position["avg_cost"] = avg_cost
    position["last_price"] = price

    return realized


class Ledger:
    def __init__(self):
        self.positions: Dict[str, Dict[str, float]] = defaultdict(
            lambda: {"shares": 0.0, "avg_cost": 0.0, "last_price": None}
        )
        self.realized_by_date = defaultdict(float)

    def apply(self, trades: List[Dict[str, Any]]):
        """Process trades and compute realized P/L per day."""

        if not trades:
            return self.realized_by_date

        # Trades are expected in chronological order; sort defensively by date.
        sorted_trades = sorted(
            trades,
            key=lambda row: (
                row.get("date"),
                row.get("datetime"),
            ),
        )

        for trade in sorted_trades:
            raw_date = trade.get("date")
            if isinstance(raw_date, datetime):
                day = raw_date.date().strftime("%Y-%m-%d")
            elif hasattr(raw_date, "strftime"):
                day = raw_date.strftime("%Y-%m-%d")
            else:
                day = str(raw_date)

            symbol = (trade.get("symbol") or "").strip().upper()
            side = (trade.get("action") or trade.get("side") or "").strip().upper()
            try:
                qty = float(trade.get("qty") or trade.get("quantity") or 0.0)
                price = float(trade.get("price") or 0.0)
            except (TypeError, ValueError):
                continue

            if not symbol or side not in {"BUY", "SELL"}:
                continue

            if qty <= 0 or price <= 0:
                continue

            position = self.positions[symbol]
            realized = _apply_trade_to_position(position, side, qty, price)
            if realized:
                self.realized_by_date[day] += realized

        return self.realized_by_date
