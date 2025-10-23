import csv
import io
import json
import math
from bisect import bisect_right
from datetime import date, datetime, timedelta
from typing import List, Optional, Set, Tuple

from fastapi import APIRouter, Request, Form, Depends, Query, HTTPException
from fastapi.responses import (
    HTMLResponse,
    RedirectResponse,
    StreamingResponse,
)
from sqlalchemy import tuple_
from sqlalchemy.orm import Session
import calendar
from app.core.database import get_session
from app.core.models import DailySummary, Meta, NoteDaily, NoteMonthly, NoteWeekly, Trade
from app.core.utils import month_bounds
from app.services.trade_summaries import recompute_daily_summaries
from pydantic import BaseModel, Field, field_validator

router = APIRouter()


def _coerce_bool(value, default: bool = True) -> bool:
    """Best-effort conversion of configuration values to booleans.

    The configuration file can occasionally contain string representations of
    truthy or falsy values (for example when edited manually). Jinja treats any
    non-empty string as truthy which would cause UI toggles such as
    ``show_trade_count`` to render incorrectly. This helper normalizes those
    values before they reach the template so the UI behaves as expected.
    """

    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
        return default
    if value is None:
        return default
    try:
        return bool(int(value))
    except (TypeError, ValueError):
        return bool(value) if value is not None else default


class TradeUpdate(BaseModel):
    id: Optional[int] = None
    symbol: str
    action: str
    qty: float
    price: float

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("Symbol is required")
        return normalized

    @field_validator("action")
    @classmethod
    def validate_action(cls, value: str) -> str:
        normalized = value.strip().upper()
        if normalized not in {"BUY", "SELL"}:
            raise ValueError("Action must be BUY or SELL")
        return normalized

    @field_validator("qty", "price", mode="before")
    @classmethod
    def validate_positive(cls, value: float) -> float:
        try:
            number = float(value)
        except (TypeError, ValueError) as exc:  # pragma: no cover - validation guard
            raise ValueError("Must be a number") from exc
        if number <= 0:
            raise ValueError("Must be greater than zero")
        return number


class TradeUpdatePayload(BaseModel):
    trades: List[TradeUpdate] = Field(default_factory=list)

@router.get("/", response_class=HTMLResponse)
def home(request: Request, db: Session = Depends(get_session)):
    # Decide which month to show
    cfg = request.app.state.config.raw
    default_view = cfg.get("view", {}).get("default", "latest")
    last_viewed = db.get(Meta, "last_viewed_month")
    today = date.today()
    if default_view == "latest" or not last_viewed or not last_viewed.value:
        y, m = today.year, today.month
    else:
        y, m = map(int, last_viewed.value.split("-"))
    return RedirectResponse(url=f"/calendar/{y}/{m}", status_code=302)

@router.get("/calendar/{year}/{month}", response_class=HTMLResponse)
def calendar_view(year: int, month: int, request: Request, db: Session = Depends(get_session)):
    # Save last viewed month
    last = db.get(Meta, "last_viewed_month")
    if last:
        last.value = f"{year}-{month}"
    else:
        db.add(Meta(key="last_viewed_month", value=f"{year}-{month}"))
    db.commit()

    cfg = request.app.state.config.raw
    ui_cfg = cfg.get("ui", {})
    fill_strategy = ui_cfg.get("unrealized_fill_strategy", "carry_forward")
    show_trade_badges = _coerce_bool(ui_cfg.get("show_trade_count", True), True)
    show_unrealized_default = _coerce_bool(ui_cfg.get("show_unrealized", True), True)
    show_text_default = _coerce_bool(ui_cfg.get("show_text", True), True)
    show_weekends_default = _coerce_bool(ui_cfg.get("show_weekends", False), False)
    notes_cfg = cfg.get("notes", {})
    notes_enabled = _coerce_bool(notes_cfg.get("enabled", True), True)
    today = date.today()

    start, end, days = month_bounds(year, month)
    # Pull daily summaries for month
    q = db.query(DailySummary).filter(DailySummary.date >= start, DailySummary.date <= end).all()
    by_day = {r.date: r for r in q}

    # Determine the unrealized value to carry forward for days without trades.
    prev_summary = (
        db.query(DailySummary)
        .filter(DailySummary.date < start)
        .order_by(DailySummary.date.desc())
        .first()
    )
    running_unrealized = float(prev_summary.unrealized) if prev_summary else 0.0
    has_running_unrealized = prev_summary is not None

    actual_unrealized_map = {}
    actual_unrealized_dates = []
    next_summary = None
    if fill_strategy == "average_neighbors":
        for row in q:
            actual_unrealized_map[date.fromisoformat(row.date)] = float(row.unrealized)
        if prev_summary:
            actual_unrealized_map[date.fromisoformat(prev_summary.date)] = float(prev_summary.unrealized)
        next_summary = (
            db.query(DailySummary)
            .filter(DailySummary.date > end)
            .order_by(DailySummary.date.asc())
            .first()
        )
        if next_summary:
            actual_unrealized_map[date.fromisoformat(next_summary.date)] = float(next_summary.unrealized)
        actual_unrealized_dates = sorted(actual_unrealized_map.keys())

        def get_prev_value(day: date):
            if not actual_unrealized_dates:
                return None
            idx = bisect_right(actual_unrealized_dates, day) - 1
            if idx >= 0:
                prev_day = actual_unrealized_dates[idx]
                if prev_day <= day:
                    return actual_unrealized_map[prev_day]
            return None

        def get_next_value(day: date):
            if not actual_unrealized_dates:
                return None
            idx = bisect_right(actual_unrealized_dates, day)
            if idx < len(actual_unrealized_dates):
                next_day = actual_unrealized_dates[idx]
                if next_day >= day:
                    return actual_unrealized_map[next_day]
            return None
    else:
        def get_prev_value(day: date):
            return None

        def get_next_value(day: date):
            return None

    note_rows = (
        db.query(NoteDaily)
        .filter(NoteDaily.date >= start, NoteDaily.date <= end)
        .all()
    )
    notes_by_day = {
        row.date: {
            "note": row.note or "",
            "updated_at": row.updated_at or "",
        }
        for row in note_rows
    }

    trade_rows = (
        db.query(Trade)
        .filter(Trade.date >= start, Trade.date <= end)
        .order_by(Trade.date.asc(), Trade.id.asc())
        .all()
    )
    trades_by_day = {}
    for tr in trade_rows:
        trades_by_day.setdefault(tr.date, []).append(
            {
                "id": tr.id,
                "symbol": tr.symbol,
                "action": tr.action,
                "qty": float(tr.qty),
                "price": float(tr.price),
            }
        )

    # Calculate weekly aggregates inline
    cal = calendar.Calendar(firstweekday=0)  # Monday=0 or Sunday=6; we'll keep 0
    weeks = []
    month_days = cal.monthdatescalendar(year, month)
    for week in month_days:
        iso_year, iso_week, _ = week[0].isocalendar()
        wk = []
        week_total_realized = 0.0
        last_unrealized_value = None
        for d in week:
            day_key = d.strftime("%Y-%m-%d")
            ds = by_day.get(day_key)
            note_entry = notes_by_day.get(day_key)
            note_text = note_entry["note"] if note_entry else ""
            note_updated_at = note_entry["updated_at"] if note_entry else ""
            is_weekend = d.weekday() >= 5
            is_future_day = d > today
            if ds:
                running_unrealized = float(ds.unrealized)
                has_running_unrealized = True
                day_unrealized = running_unrealized
            elif d.month == month:
                if is_future_day:
                    day_unrealized = 0.0
                elif fill_strategy == "average_neighbors":
                    prev_val = get_prev_value(d)
                    next_val = get_next_value(d)
                    if prev_val is not None and next_val is not None:
                        day_unrealized = (prev_val + next_val) / 2.0
                    elif has_running_unrealized:
                        day_unrealized = running_unrealized
                    else:
                        day_unrealized = 0.0
                elif has_running_unrealized:
                    day_unrealized = running_unrealized
                else:
                    day_unrealized = 0.0
            else:
                day_unrealized = 0.0
            day_trades = trades_by_day.get(day_key, [])
            has_trades = bool(day_trades)
            has_sell_trade = any(
                (trade.get("action") or "").upper() == "SELL" for trade in day_trades
            )
            realized_value = float(ds.realized) if ds else 0.0
            show_realized = bool(ds) and (
                not math.isclose(realized_value, 0.0, abs_tol=0.005) or has_sell_trade
            )
            wk.append({
                "date": d,
                "in_month": (d.month == month),
                "realized": realized_value,
                "unrealized": day_unrealized,
                "has_values": bool(ds),
                "show_realized": show_realized,
                "note": note_text,
                "note_updated_at": note_updated_at,
                "has_note": bool(note_text.strip()),
                "is_weekend": is_weekend,
                "trades": day_trades,
                "has_trades": has_trades,
            })
            if d.month == month and ds:
                week_total_realized += float(ds.realized)
            if d.month == month:
                if ds:
                    last_unrealized_value = float(ds.unrealized)
                elif has_running_unrealized:
                    last_unrealized_value = day_unrealized
        weeks.append({
            "days": wk,
            "week_realized": week_total_realized,
            "week_unrealized": last_unrealized_value if last_unrealized_value is not None else 0.0,
            "week_index": len(weeks) + 1,
            "week_year": iso_year,
            "week_number": iso_week,
            "show_week_realized": not math.isclose(week_total_realized, 0.0, abs_tol=0.005),
            "note": "",
            "note_updated_at": "",
            "has_note": False,
        })

    week_pairs = {(week_entry["week_year"], week_entry["week_number"]) for week_entry in weeks}
    weekly_note_rows = []
    if week_pairs:
        weekly_note_rows = (
            db.query(NoteWeekly)
            .filter(tuple_(NoteWeekly.year, NoteWeekly.week).in_(list(week_pairs)))
            .all()
        )
    weekly_notes = {
        (row.year, row.week): {
            "note": row.note or "",
            "updated_at": row.updated_at or "",
        }
        for row in weekly_note_rows
    }
    for week_entry in weeks:
        data = weekly_notes.get((week_entry["week_year"], week_entry["week_number"]))
        note_text = data["note"] if data else ""
        week_entry["note"] = note_text
        week_entry["note_updated_at"] = data["updated_at"] if data else ""
        week_entry["has_note"] = bool(note_text.strip())

    month_note_row = (
        db.query(NoteMonthly)
        .filter(NoteMonthly.year == year, NoteMonthly.month == month)
        .first()
    )
    month_note = {
        "note": month_note_row.note if month_note_row and month_note_row.note else "",
        "updated_at": month_note_row.updated_at if month_note_row and month_note_row.updated_at else "",
    }

    # Monthly totals
    month_realized = sum(float(r.realized) for r in q)
    month_unrealized = sum(float(r.unrealized) for r in q)

    # Yearly totals
    year_start = f"{year:04d}-01-01"
    year_end = f"{year:04d}-12-31"
    year_rows = (
        db.query(DailySummary)
        .filter(DailySummary.date >= year_start, DailySummary.date <= year_end)
        .all()
    )
    year_realized = sum(float(r.realized) for r in year_rows)
    year_unrealized = sum(float(r.unrealized) for r in year_rows)

    ctx = {
        "request": request,
        "year": year, "month": month,
        "weeks": weeks,
        "month_note": month_note,
        "month_realized": month_realized,
        "month_unrealized": month_unrealized,
        "year_realized": year_realized,
        "year_unrealized": year_unrealized,
        "cfg": request.app.state.config.raw,
        "show_trade_badges": show_trade_badges,
        "show_unrealized_flag": show_unrealized_default,
        "show_text_flag": show_text_default,
        "show_weekends_flag": show_weekends_default,
        "notes_enabled_flag": notes_enabled,
        "export_default_start": start,
        "export_default_end": end,
        "current_year": today.year,
        "current_month": today.month,
    }
    return request.app.state.templates.TemplateResponse("calendar.html", ctx)


@router.get("/api/trades/{date_str}")
def get_trades_for_day(date_str: str, db: Session = Depends(get_session)):
    trades = (
        db.query(Trade)
        .filter(Trade.date == date_str)
        .order_by(Trade.id.asc())
        .all()
    )
    return {
        "trades": [
            {
                "id": trade.id,
                "symbol": trade.symbol,
                "action": trade.action,
                "qty": float(trade.qty),
                "price": float(trade.price),
            }
            for trade in trades
        ]
    }


@router.post("/api/trades/{date_str}")
def save_trades_for_day(
    date_str: str,
    payload: TradeUpdatePayload,
    db: Session = Depends(get_session),
):
    existing = (
        db.query(Trade)
        .filter(Trade.date == date_str)
        .order_by(Trade.id.asc())
        .all()
    )
    existing_map = {trade.id: trade for trade in existing}
    seen_ids = set()
    had_existing_trades = bool(existing)

    for trade_update in payload.trades:
        trade = None
        if trade_update.id is not None:
            trade = existing_map.get(trade_update.id)
            if trade is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Trade {trade_update.id} was not found for {date_str}.",
                )
            seen_ids.add(trade_update.id)

        amount = trade_update.qty * trade_update.price
        signed_amount = amount if trade_update.action == "SELL" else -amount

        if trade is not None:
            trade.symbol = trade_update.symbol
            trade.action = trade_update.action
            trade.qty = trade_update.qty
            trade.price = trade_update.price
            trade.amount = signed_amount
        else:
            db.add(
                Trade(
                    date=date_str,
                    symbol=trade_update.symbol,
                    action=trade_update.action,
                    qty=trade_update.qty,
                    price=trade_update.price,
                    amount=signed_amount,
                )
            )

    for trade in existing:
        if trade.id not in seen_ids:
            db.delete(trade)

    db.flush()

    remaining = (
        db.query(Trade)
        .filter(Trade.date == date_str)
        .count()
    )
    if remaining == 0 and had_existing_trades:
        summary_row = db.get(DailySummary, date_str)
        if summary_row:
            db.delete(summary_row)

    db.flush()

    daily_map = recompute_daily_summaries(db)
    updated = (
        db.query(Trade)
        .filter(Trade.date == date_str)
        .order_by(Trade.id.asc())
        .all()
    )
    response_trades = [
        {
            "id": trade.id,
            "symbol": trade.symbol,
            "action": trade.action,
            "qty": float(trade.qty),
            "price": float(trade.price),
        }
        for trade in updated
    ]

    db.commit()

    return {
        "ok": True,
        "trades": response_trades,
        "summary": daily_map.get(date_str),
    }


@router.delete("/api/trades/{date_str}")
def clear_trades_for_day(date_str: str, db: Session = Depends(get_session)):
    trades = (
        db.query(Trade)
        .filter(Trade.date == date_str)
        .order_by(Trade.id.asc())
        .all()
    )
    deleted = 0
    for trade in trades:
        db.delete(trade)
        deleted += 1

    summary_row = db.get(DailySummary, date_str)
    if summary_row is not None:
        db.delete(summary_row)

    db.flush()

    daily_map = recompute_daily_summaries(db)

    db.commit()

    return {
        "ok": True,
        "deleted": deleted,
        "trades": [],
        "summary": daily_map.get(date_str),
    }


@router.post("/api/daily/{date_str}")
def overwrite_daily(date_str: str, realized: float = Form(...), unrealized: float = Form(...), db: Session = Depends(get_session)):
    from app.core.models import DailySummary
    now = datetime.utcnow().isoformat()
    ds = db.get(DailySummary, date_str)
    if ds:
        ds.realized = realized
        ds.unrealized = unrealized
        ds.total_invested = unrealized
        ds.updated_at = now
    else:
        db.add(DailySummary(date=date_str, realized=realized, unrealized=unrealized, total_invested=unrealized, updated_at=now))
    db.commit()
    return {"ok": True}


@router.get("/export")
def export_data(
    request: Request,
    start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    dataset: str = Query(
        "summaries",
        description="The dataset to export (summaries, trades, or notes)",
    ),
    db: Session = Depends(get_session),
):
    fmt = "%Y-%m-%d"
    today = date.today()

    cfg = getattr(request.app.state, "config", None)
    fill_empty_with_zero = True
    if cfg is not None:
        fill_empty_with_zero = cfg.raw.get("export", {}).get("fill_empty_with_zero", True)

    def parse(value: Optional[str], fallback: date) -> date:
        if value:
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.") from exc
        return fallback

    end_dt = parse(end, today)
    start_dt = parse(start, end_dt - timedelta(days=30))

    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    start_str = start_dt.strftime(fmt)
    end_str = end_dt.strftime(fmt)

    dataset_value = dataset
    if not isinstance(dataset_value, str):  # When called directly in tests
        dataset_value = getattr(dataset_value, "default", "summaries")
    dataset_key = (dataset_value or "").strip().lower()
    if dataset_key not in {"summaries", "trades", "notes"}:
        raise HTTPException(status_code=400, detail="Unknown export dataset")

    if dataset_key == "summaries":
        summaries = (
            db.query(DailySummary)
            .filter(DailySummary.date >= start_str, DailySummary.date <= end_str)
            .order_by(DailySummary.date.asc())
            .all()
        )

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["date", "realized", "unrealized"])

        def format_value(value: Optional[float]) -> str:
            if value is None:
                return "0.00" if fill_empty_with_zero else ""
            if isinstance(value, str):
                stripped = value.strip()
                if not stripped:
                    return "0.00" if fill_empty_with_zero else ""
                candidate = stripped
            else:
                candidate = value
            try:
                number = float(candidate)
            except (TypeError, ValueError):
                return "0.00" if fill_empty_with_zero else ""
            if math.isnan(number):  # pragma: no cover - defensive guard
                return "0.00" if fill_empty_with_zero else ""
            return f"{number:.2f}"

        for summary in summaries:
            writer.writerow(
                [
                    summary.date,
                    format_value(summary.realized),
                    format_value(summary.unrealized),
                ]
            )

        filename = f"bagholder_export_{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}.csv"
        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        content = buffer.getvalue().encode("utf-8")
        return StreamingResponse(iter([content]), media_type="text/csv", headers=headers)

    if dataset_key == "trades":
        trades = (
            db.query(Trade)
            .filter(Trade.date >= start_str, Trade.date <= end_str)
            .order_by(Trade.date.asc(), Trade.id.asc())
            .all()
        )

        def _format_decimal(value: Optional[float], precision: int = 2) -> str:
            if value is None:
                return ""
            text = f"{float(value):.{precision}f}"
            text = text.rstrip("0").rstrip(".")
            return text or "0"

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["date", "symbol", "action", "qty", "price", "amount"])
        for trade in trades:
            writer.writerow(
                [
                    trade.date,
                    trade.symbol,
                    trade.action,
                    _format_decimal(trade.qty, precision=8),
                    _format_decimal(trade.price, precision=4),
                    _format_decimal(trade.amount, precision=4),
                ]
            )

        filename = f"bagholder_trades_{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}.csv"
        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        content = buffer.getvalue().encode("utf-8")
        return StreamingResponse(iter([content]), media_type="text/csv", headers=headers)

    # dataset_key == "notes"
    day_cursor = start_dt
    week_pairs: Set[Tuple[int, int]] = set()
    month_pairs: Set[Tuple[int, int]] = set()
    while day_cursor <= end_dt:
        iso_year, iso_week, _ = day_cursor.isocalendar()
        week_pairs.add((iso_year, iso_week))
        month_pairs.add((day_cursor.year, day_cursor.month))
        day_cursor += timedelta(days=1)

    daily_notes = (
        db.query(NoteDaily)
        .filter(NoteDaily.date >= start_str, NoteDaily.date <= end_str)
        .order_by(NoteDaily.date.asc())
        .all()
    )

    weekly_notes = []
    if week_pairs:
        weekly_notes = (
            db.query(NoteWeekly)
            .filter(tuple_(NoteWeekly.year, NoteWeekly.week).in_(list(week_pairs)))
            .order_by(NoteWeekly.year.asc(), NoteWeekly.week.asc())
            .all()
        )

    monthly_notes = []
    if month_pairs:
        monthly_notes = (
            db.query(NoteMonthly)
            .filter(tuple_(NoteMonthly.year, NoteMonthly.month).in_(list(month_pairs)))
            .order_by(NoteMonthly.year.asc(), NoteMonthly.month.asc())
            .all()
        )

    payload = {
        "range": {"start": start_str, "end": end_str},
        "daily": [
            {
                "date": note.date,
                "note": note.note,
                "is_markdown": bool(note.is_markdown),
                "updated_at": note.updated_at,
            }
            for note in daily_notes
        ],
        "weekly": [
            {
                "year": note.year,
                "week": note.week,
                "note": note.note,
                "updated_at": note.updated_at,
            }
            for note in weekly_notes
        ],
        "monthly": [
            {
                "year": note.year,
                "month": note.month,
                "note": note.note,
                "updated_at": note.updated_at,
            }
            for note in monthly_notes
        ],
    }

    filename = f"bagholder_notes_{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}.json"
    headers = {"Content-Disposition": f"attachment; filename={filename}"}
    content = json.dumps(payload, indent=2).encode("utf-8")
    return StreamingResponse(iter([content]), media_type="application/json", headers=headers)
