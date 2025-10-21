from fastapi import APIRouter, Request, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session
from datetime import date, datetime
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
        week_total_unreal = 0.0
        for d in week:
            day_key = d.strftime("%Y-%m-%d")
            ds = by_day.get(day_key)
            note_text = notes_by_day.get(day_key, "")
            is_weekend = d.weekday() >= 5
            wk.append({
                "date": d,
                "in_month": (d.month == month),
                "realized": float(ds.realized) if ds else 0.0,
                "unrealized": float(ds.unrealized) if ds else 0.0,
                "note": note_text,
                "has_note": bool(note_text.strip()),
                "is_weekend": is_weekend,
                "trades": trades_by_day.get(day_key, []),
                "has_trades": bool(trades_by_day.get(day_key, [])),
            })
            if d.month == month and ds:
                week_total_realized += float(ds.realized)
                week_total_unreal += float(ds.unrealized)
        weeks.append({
            "days": wk,
            "week_realized": week_total_realized,
            "week_unrealized": week_total_unreal,
            "week_index": len(weeks) + 1,
        })

    # Monthly totals
    month_realized = sum(float(r.realized) for r in q)
    month_unrealized = sum(float(r.unrealized) for r in q)

    ctx = {
        "request": request,
        "year": year, "month": month,
        "weeks": weeks,
        "month_realized": month_realized,
        "month_unrealized": month_unrealized,
        "cfg": request.app.state.config.raw,
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
