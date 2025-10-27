import math
from collections import defaultdict
from datetime import date, datetime
from typing import Iterable, List, Dict, Any, Optional, Tuple

from app.services.trade_matching import apply_trade, create_position


class Ledger:
    def __init__(self, *, method: str = "fifo"):
        normalized = (method or "fifo") if isinstance(method, str) else "fifo"
        self.method = "lifo" if str(normalized).strip().lower() == "lifo" else "fifo"
        self.positions: Dict[str, Dict[str, Any]] = defaultdict(create_position)
        self.realized_by_date = defaultdict(float)

    def apply(self, trades: List[Dict[str, Any]]):
        """Process trades and compute realized P/L per day."""

        if not trades:
            return self.realized_by_date

        sorted_trades = sorted(trades, key=_trade_sort_key)

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
            fee = float(trade.get("fee") or 0.0)
            realized = apply_trade(position, side, qty, price, fee=fee, method=self.method)
            if realized:
                self.realized_by_date[day] += realized

        return self.realized_by_date


def _normalize_trade_day(raw: Any) -> Optional[date]:
    """Best-effort conversion of a trade's date field to ``datetime.date``."""

    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    if hasattr(raw, "date"):
        try:
            return raw.date()
        except Exception:  # pragma: no cover - defensive
            pass
    if raw is None:
        return None
    try:
        return date.fromisoformat(str(raw))
    except ValueError:
        return None


def _trade_sort_key(trade: Dict[str, Any]) -> Tuple[str, int, str, float]:
    """Provide a deterministic sort key for trade records."""

    day = trade.get("date")
    if isinstance(day, datetime):
        day_key = day.strftime("%Y-%m-%d")
    elif hasattr(day, "strftime"):
        day_key = day.strftime("%Y-%m-%d")
    else:
        day_key = str(day)

    sequence = trade.get("sequence")
    try:
        sequence_key = int(sequence)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        sequence_key = 0

    stamp = trade.get("datetime")
    if isinstance(stamp, datetime):
        stamp_key = stamp.isoformat()
    elif hasattr(stamp, "strftime"):
        stamp_key = stamp.strftime("%H:%M:%S")
    elif stamp is None:
        stamp_key = ""
    else:
        stamp_key = str(stamp)

    identifier = trade.get("id") or trade.get("trade_id") or 0

    numeric_id: float
    try:
        numeric_id = float(identifier)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        numeric_id = 0.0

    return (day_key, sequence_key, stamp_key, numeric_id)


def count_trade_win_losses(
    trades: Iterable[Dict[str, Any]],
    *,
    start: Optional[date] = None,
    end: Optional[date] = None,
    method: str = "fifo",
) -> tuple[int, int]:
    """Count winning and losing days within an optional window.

    Trades are processed chronologically so that prior activity can establish
    position context for the target window. Realized profit and loss from
    trades that fall within the window are aggregated per day; a day with net
    positive realized value is a win while a day with net negative value is a
    loss.
    """

    normalized = (method or "fifo") if isinstance(method, str) else "fifo"
    method_value = "lifo" if str(normalized).strip().lower() == "lifo" else "fifo"
    positions: Dict[str, Dict[str, Any]] = defaultdict(create_position)
    daily_realized: Dict[date, float] = defaultdict(float)

    sorted_trades = sorted(trades, key=_trade_sort_key)

    for trade in sorted_trades:
        day = _normalize_trade_day(trade.get("date"))
        if day is None:
            continue

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

        position = positions[symbol]
        fee = float(trade.get("fee") or 0.0)
        realized = apply_trade(position, side, qty, price, fee=fee, method=method_value)

        if start and day < start:
            # Context-only trade; do not classify outcome but maintain position.
            continue
        if end and day > end:
            # Past the window of interest; no need to classify but positions may
            # still be relevant for subsequent ranges processed by the caller.
            continue

        if math.isclose(realized, 0.0, abs_tol=0.005):
            continue

        daily_realized[day] += realized

    wins = 0
    losses = 0
    for realized_total in daily_realized.values():
        if math.isclose(realized_total, 0.0, abs_tol=0.005):
            continue
        if realized_total > 0:
            wins += 1
        elif realized_total < 0:
            losses += 1

    return wins, losses
