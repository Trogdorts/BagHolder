import csv
import io
import math
from bisect import bisect_right
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

from fastapi import APIRouter, Request, Form, Depends, Query, HTTPException
from fastapi.responses import (
    HTMLResponse,
    RedirectResponse,
    StreamingResponse,
)
from sqlalchemy import tuple_
from sqlalchemy.orm import Session
import calendar
from app.core.config import AppConfig
from app.core.database import get_session
from app.core.models import (
    DailySummary,
    Meta,
    NoteDaily,
    NoteMonthly,
    NoteWeekly,
    Trade,
)
from app.core.utils import coerce_bool, month_bounds
from app.services.trade_summaries import recompute_daily_summaries
from pydantic import BaseModel, Field, field_validator

router = APIRouter()
class UIPreferencesUpdate(BaseModel):
    show_unrealized: Optional[bool] = None
    show_percentages: Optional[bool] = None
    show_weekends: Optional[bool] = None
    show_exclude_controls: Optional[bool] = None
    show_trade_count: Optional[bool] = None


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
    show_trade_badges = coerce_bool(ui_cfg.get("show_trade_count", False), False)
    show_unrealized_default = coerce_bool(ui_cfg.get("show_unrealized", True), True)
    show_text_default = coerce_bool(ui_cfg.get("show_text", True), True)
    show_percentages_default = coerce_bool(ui_cfg.get("show_percentages", True), True)
    show_weekends_default = coerce_bool(ui_cfg.get("show_weekends", True), True)
    show_exclude_controls_default = coerce_bool(
        ui_cfg.get("show_exclude_controls", True), True
    )
    notes_cfg = cfg.get("notes", {})
    notes_enabled = coerce_bool(notes_cfg.get("enabled", True), True)
    today = date.today()

    start, end, days = month_bounds(year, month)
    month_end_date = date.fromisoformat(end)
    year_start = f"{year:04d}-01-01"
    year_end = f"{year:04d}-12-31"
    months_to_subtract = 11
    total_months = year * 12 + month - 1 - months_to_subtract
    rolling_year = total_months // 12
    rolling_month = total_months % 12 + 1
    rolling_start = f"{rolling_year:04d}-{rolling_month:02d}-01"
    rolling_start_date = date.fromisoformat(rolling_start)
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
    def calculate_percentage(realized_value: float, invested_samples: List[float]) -> Optional[float]:
        total_invested = sum(
            abs(sample)
            for sample in invested_samples
            if sample is not None and not math.isclose(sample, 0.0, abs_tol=0.005)
        )
        if math.isclose(total_invested, 0.0, abs_tol=0.005):
            return None
        return round((realized_value / total_invested) * 100.0, 2)

    def invested_max(rows: List[DailySummary]) -> float:
        max_value = 0.0
        for row in rows:
            try:
                magnitude = abs(float(row.total_invested))
            except (TypeError, ValueError):
                continue
            if math.isclose(magnitude, 0.0, abs_tol=0.005):
                continue
            if magnitude > max_value:
                max_value = magnitude
        return max_value

    for week in month_days:
        iso_year, iso_week, _ = week[0].isocalendar()
        wk = []
        week_total_realized = 0.0
        week_invested_samples: List[float] = []
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
            invested_value = float(ds.total_invested) if ds else 0.0
            percent_value = (
                calculate_percentage(realized_value, [invested_value]) if ds else None
            )

            wk.append({
                "date": d,
                "in_month": (d.month == month),
                "realized": realized_value,
                "unrealized": day_unrealized,
                "has_values": bool(ds),
                "invested": invested_value,
                "show_realized": show_realized,
                "percent": percent_value,
                "note": note_text,
                "note_updated_at": note_updated_at,
                "has_note": bool(note_text.strip()),
                "is_weekend": is_weekend,
                "trades": day_trades,
                "has_trades": has_trades,
                "in_rolling": rolling_start_date <= d <= month_end_date,
                "belongs_to_year": d.year == year,
            })
            if d.month == month and ds:
                week_total_realized += float(ds.realized)
                week_invested_samples.append(invested_value)
            if d.month == month:
                if ds:
                    last_unrealized_value = float(ds.unrealized)
                elif has_running_unrealized:
                    last_unrealized_value = day_unrealized
        week_percent = calculate_percentage(week_total_realized, week_invested_samples)
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
            "week_percent": week_percent,
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
    month_percent = calculate_percentage(
        month_realized,
        [float(r.total_invested) for r in q],
    )

    # Yearly totals
    year_rows = (
        db.query(DailySummary)
        .filter(DailySummary.date >= year_start, DailySummary.date <= year_end)
        .all()
    )
    year_realized = sum(float(r.realized) for r in year_rows)
    year_unrealized = sum(float(r.unrealized) for r in year_rows)
    year_trading_days = sum(1 for r in year_rows if r)
    year_percent = calculate_percentage(
        year_realized,
        [float(r.total_invested) for r in year_rows],
    )

    # Rolling 12 month totals ending at the current month
    rolling_rows = (
        db.query(DailySummary)
        .filter(DailySummary.date >= rolling_start, DailySummary.date <= end)
        .all()
    )
    rolling_realized = sum(float(r.realized) for r in rolling_rows)
    rolling_unrealized = sum(float(r.unrealized) for r in rolling_rows)
    rolling_trading_days = sum(1 for r in rolling_rows if r)
    rolling_year_percent = calculate_percentage(
        rolling_realized,
        [float(r.total_invested) for r in rolling_rows],
    )

    year_other_rows = []
    rolling_other_rows = []
    for row in year_rows:
        row_date = date.fromisoformat(row.date)
        if row_date.year != year or row_date.month != month:
            year_other_rows.append(row)
    for row in rolling_rows:
        row_date = date.fromisoformat(row.date)
        if row_date.year != year or row_date.month != month:
            rolling_other_rows.append(row)

    year_other_invested_max = invested_max(year_other_rows)
    rolling_other_invested_max = invested_max(rolling_other_rows)

    ctx = {
        "request": request,
        "year": year, "month": month,
        "weeks": weeks,
        "month_note": month_note,
        "month_realized": month_realized,
        "month_unrealized": month_unrealized,
        "month_percent": month_percent,
        "year_realized": year_realized,
        "year_unrealized": year_unrealized,
        "year_trading_days": year_trading_days,
        "year_percent": year_percent,
        "rolling_year_realized": rolling_realized,
        "rolling_year_unrealized": rolling_unrealized,
        "rolling_year_trading_days": rolling_trading_days,
        "rolling_year_percent": rolling_year_percent,
        "year_other_invested_max": year_other_invested_max,
        "rolling_year_other_invested_max": rolling_other_invested_max,
        "cfg": request.app.state.config.raw,
        "show_trade_badges": show_trade_badges,
        "show_unrealized_flag": show_unrealized_default,
        "show_text_flag": show_text_default,
        "show_percentages_flag": show_percentages_default,
        "show_weekends_flag": show_weekends_default,
        "show_exclude_controls_flag": show_exclude_controls_default,
        "notes_enabled_flag": notes_enabled,
        "current_year": today.year,
        "current_month": today.month,
    }
    return request.app.state.templates.TemplateResponse(
        request,
        "calendar.html",
        ctx,
    )


@router.post("/api/ui/preferences")
def update_ui_preferences(payload: UIPreferencesUpdate, request: Request):
    cfg: AppConfig = request.app.state.config
    ui_section = cfg.raw.setdefault("ui", {})

    updates: Dict[str, bool] = {}
    for field, key in (
        ("show_unrealized", "show_unrealized"),
        ("show_percentages", "show_percentages"),
        ("show_weekends", "show_weekends"),
        ("show_exclude_controls", "show_exclude_controls"),
        ("show_trade_count", "show_trade_count"),
    ):
        value = getattr(payload, field)
        if value is not None:
            ui_section[key] = bool(value)
            updates[key] = bool(value)

    if not updates:
        raise HTTPException(status_code=400, detail="No preferences provided.")

    cfg.save()
    request.app.state.templates.env.globals["cfg"] = cfg.raw

    return {"status": "ok", "preferences": updates}


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
    if dataset_key not in {"summaries", "trades"}:
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

        notes_by_date = {
            note.date: note.note or ""
            for note in (
                db.query(NoteDaily)
                .filter(NoteDaily.date >= start_str, NoteDaily.date <= end_str)
                .all()
            )
        }

        def _format_decimal(value: Optional[float], precision: int = 2) -> str:
            if value is None:
                return ""
            text = f"{float(value):.{precision}f}"
            text = text.rstrip("0").rstrip(".")
            return text or "0"

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["date", "symbol", "action", "qty", "price", "amount", "notes"])
        for trade in trades:
            writer.writerow(
                [
                    trade.date,
                    trade.symbol,
                    trade.action,
                    _format_decimal(trade.qty, precision=8),
                    _format_decimal(trade.price, precision=4),
                    _format_decimal(trade.amount, precision=4),
                    notes_by_date.get(trade.date, ""),
                ]
            )

        filename = f"bagholder_trades_{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}.csv"
        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        content = buffer.getvalue().encode("utf-8")
        return StreamingResponse(iter([content]), media_type="text/csv", headers=headers)
