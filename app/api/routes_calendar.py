import csv
import io
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Request, Form, Depends, Query, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from sqlalchemy.orm import Session
import calendar
from app.core.database import get_session
from app.core.models import DailySummary, Meta, NoteDaily, Trade
from app.core.utils import month_bounds

router = APIRouter()

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
        wk = []
        week_total_realized = 0.0
        last_unrealized_value = None
        for d in week:
            day_key = d.strftime("%Y-%m-%d")
            ds = by_day.get(day_key)
            note_text = notes_by_day.get(day_key, "")
            is_weekend = d.weekday() >= 5
            if ds:
                running_unrealized = float(ds.unrealized)
                has_running_unrealized = True
                day_unrealized = running_unrealized
            elif d.month == month and has_running_unrealized:
                day_unrealized = running_unrealized
            else:
                day_unrealized = 0.0
            wk.append({
                "date": d,
                "in_month": (d.month == month),
                "realized": float(ds.realized) if ds else 0.0,
                "unrealized": day_unrealized,
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
        })

    # Monthly totals
    month_realized = sum(float(r.realized) for r in q)
    month_unrealized = sum(float(r.unrealized) for r in q)

    today = date.today()

    ctx = {
        "request": request,
        "year": year, "month": month,
        "weeks": weeks,
        "month_realized": month_realized,
        "month_unrealized": month_unrealized,
        "cfg": request.app.state.config.raw,
        "export_default_start": start,
        "export_default_end": end,
        "current_year": today.year,
        "current_month": today.month,
    }
    return request.app.state.templates.TemplateResponse("calendar.html", ctx)

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
    start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    db: Session = Depends(get_session),
):
    fmt = "%Y-%m-%d"
    today = date.today()

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
    writer.writerow(["date", "realized", "unrealized", "total_invested", "updated_at"])
    for summary in summaries:
        writer.writerow(
            [
                summary.date,
                f"{float(summary.realized):.2f}",
                f"{float(summary.unrealized):.2f}",
                f"{float(summary.total_invested):.2f}",
                summary.updated_at,
            ]
        )

    filename = f"bagholder_export_{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}.csv"
    headers = {"Content-Disposition": f"attachment; filename={filename}"}
    content = buffer.getvalue().encode("utf-8")
    return StreamingResponse(iter([content]), media_type="text/csv", headers=headers)
