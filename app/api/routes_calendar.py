import csv
import io
import math
from bisect import bisect_right
from datetime import date, datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, Request, Form, Depends, Query, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from sqlalchemy import tuple_
from sqlalchemy.orm import Session
import calendar
from app.core.database import get_session
from app.core.models import DailySummary, Meta, NoteDaily, NoteWeekly, Trade
from app.core.utils import month_bounds
from pydantic import BaseModel, Field, field_validator

router = APIRouter()


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
    from app.core.models import Meta
    last = db.get(Meta, "last_viewed_month")
    if last:
        last.value = f"{year}-{month}"
    else:
        db.add(Meta(key="last_viewed_month", value=f"{year}-{month}"))
    db.commit()

    cfg = request.app.state.config.raw
    fill_strategy = cfg.get("ui", {}).get("unrealized_fill_strategy", "carry_forward")
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
    notes_by_day = {r.date: r.note for r in note_rows}

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
            note_text = notes_by_day.get(day_key, "")
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
            wk.append({
                "date": d,
                "in_month": (d.month == month),
                "realized": float(ds.realized) if ds else 0.0,
                "unrealized": day_unrealized,
                "has_values": bool(ds),
                "note": note_text,
                "has_note": bool(note_text.strip()),
                "is_weekend": is_weekend,
                "trades": trades_by_day.get(day_key, []),
                "has_trades": bool(trades_by_day.get(day_key, [])),
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
            "note": "",
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
    weekly_notes = {(row.year, row.week): row.note for row in weekly_note_rows}
    for week_entry in weeks:
        note_text = weekly_notes.get((week_entry["week_year"], week_entry["week_number"]), "")
        week_entry["note"] = note_text
        week_entry["has_note"] = bool(note_text.strip())

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
        "month_realized": month_realized,
        "month_unrealized": month_unrealized,
        "year_realized": year_realized,
        "year_unrealized": year_unrealized,
        "cfg": request.app.state.config.raw,
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

    db.commit()
    return {"ok": True}


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
