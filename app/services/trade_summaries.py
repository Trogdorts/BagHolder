from __future__ import annotations

from datetime import datetime, date
from typing import Any, Dict, List, Mapping, Optional, Sequence

import math
from datetime import datetime
from sqlalchemy.orm import Session

from app.core.models import DailySummary, Trade
from app.services.pnl import compute_daily_pnl_records


def _coerce_number(value: Any) -> float:
    """Convert ``value`` to a finite float rounded to two decimals."""

    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0

    if not math.isfinite(number):
        return 0.0

    return round(number, 2)


def _normalize_date(value: Any) -> Optional[str]:
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, datetime):
        return value.date().strftime("%Y-%m-%d")
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d")
        except ValueError:
            return text
        return parsed.strftime("%Y-%m-%d")
    return None


def _trade_to_record(trade: Trade) -> Optional[Dict[str, Any]]:
    """Convert a :class:`Trade` ORM object into a calculation-friendly dict."""

    if trade is None:
        return None

    action = (trade.action or "").strip().upper()
    if action not in {"BUY", "SELL"}:
        return None

    symbol = (trade.symbol or "").strip().upper()
    if not symbol:
        return None

    try:
        quantity = float(trade.qty)
        price = float(trade.price)
    except (TypeError, ValueError):
        return None

    if quantity <= 0 or price <= 0:
        return None

    try:
        trade_date = datetime.strptime(trade.date, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None

    timestamp = None
    time_value = (getattr(trade, "time", "") or "").strip()
    if time_value:
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                parsed_time = datetime.strptime(time_value, fmt).time()
                timestamp = datetime.combine(trade_date, parsed_time)
                break
            except ValueError:
                continue

    return {
        "date": trade_date,
        "side": action,
        "symbol": symbol,
        "quantity": float(quantity),
        "price": float(price),
        "fee": float(getattr(trade, "fee", 0.0) or 0.0),
        "sequence": int(getattr(trade, "sequence", 0) or 0),
        "datetime": timestamp,
    }


def calculate_daily_trade_map(
    trades: Sequence[Trade], *, method: str = "fifo"
) -> Dict[str, Dict[str, float]]:
    """Compute realized profit/loss for each trading day.

    Parameters
    ----------
    trades:
        Iterable of :class:`Trade` ORM objects sorted by trading date.

    Returns
    -------
    Dict[str, Dict[str, float]]
        Mapping of ``YYYY-MM-DD`` date strings to calculated totals.
    """

    records: List[Dict[str, Any]] = []
    for trade in trades:
        record = _trade_to_record(trade)
        if record is not None:
            records.append(record)

    if not records:
        return {}

    daily_df = compute_daily_pnl_records(records, method=method)
    result: Dict[str, Dict[str, float]] = {}
    for row in daily_df.to_dict("records"):
        day_key = _normalize_date(row.get("date"))
        if not day_key:
            continue
        result[day_key] = {
            "realized": _coerce_number(row.get("realized_pl", 0.0)),
            "total_invested": _coerce_number(row.get("trade_value", 0.0)),
        }
    return result


def upsert_daily_summaries(
    db: Session, daily_map: Mapping[str, Mapping[str, float]], timestamp: Optional[str] = None
) -> None:
    """Persist computed daily totals into the ``daily_summary`` table."""

    if not daily_map:
        return

    if not timestamp:
        timestamp = datetime.utcnow().isoformat()

    existing_rows = (
        db.query(DailySummary)
        .filter(DailySummary.date.in_(list(daily_map.keys())))
        .all()
    )
    by_date = {row.date: row for row in existing_rows}

    for day, values in daily_map.items():
        realized = _coerce_number(values.get("realized", 0.0))
        total_invested = _coerce_number(values.get("total_invested", 0.0))
        row = by_date.get(day)
        if row is None:
            db.add(
                DailySummary(
                    date=day,
                    realized=realized,
                    total_invested=total_invested,
                    updated_at=timestamp,
                )
            )
            continue

        row.realized = realized
        row.total_invested = total_invested
        row.updated_at = timestamp


def recompute_daily_summaries(
    db: Session, *, method: str = "fifo"
) -> Dict[str, Dict[str, float]]:
    """Recalculate and persist daily summaries for all recorded trades."""

    trades = (
        db.query(Trade)
        .order_by(Trade.date.asc(), Trade.id.asc())
        .all()
    )

    daily_map = calculate_daily_trade_map(trades, method=method)
    upsert_daily_summaries(db, daily_map)
    return daily_map
