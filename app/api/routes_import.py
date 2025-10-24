from datetime import datetime
from typing import Dict

from fastapi import APIRouter, Depends, File, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.core.database import get_session
from app.core.models import DailySummary, Trade
from app.services.import_thinkorswim import parse_thinkorswim_csv
from app.services.import_trades_csv import parse_trade_csv
from app.services.trade_summaries import calculate_daily_trade_map, upsert_daily_summaries

router = APIRouter()


def _is_close(a: float, b: float, tol: float = 0.01) -> bool:
    return abs(float(a) - float(b)) <= tol


def _is_missing_summary(summary: DailySummary) -> bool:
    """Return ``True`` when a summary appears to be an empty placeholder."""

    if summary is None:
        return True

    updated = (summary.updated_at or "").strip()
    if updated:
        return False

    realized = float(summary.realized or 0.0)
    unrealized = float(summary.unrealized or 0.0)
    invested = float(summary.total_invested or 0.0)
    return (
        _is_close(realized, 0.0)
        and _is_close(unrealized, 0.0)
        and _is_close(invested, 0.0)
    )


@router.get("/import", response_class=RedirectResponse)
def import_page(request: Request):
    return RedirectResponse("/settings#stock-data-import", status_code=307)


def _persist_trade_rows(db: Session, rows):
    if not rows:
        return 0

    deduped_rows = []
    seen = set()
    for row in rows:
        key = (
            row["date"],
            row["symbol"],
            row["action"],
            float(row["qty"]),
            float(row["price"]),
            float(row["amount"]),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped_rows.append(
            {
                "date": row["date"],
                "symbol": row["symbol"],
                "action": row["action"],
                "qty": float(row["qty"]),
                "price": float(row["price"]),
                "amount": float(row["amount"]),
            }
        )

    if not deduped_rows:
        return 0

    affected_dates = {row["date"] for row in deduped_rows}
    if affected_dates:
        existing_rows = (
            db.query(Trade)
            .filter(Trade.date.in_(affected_dates))
            .all()
        )
        for trade in existing_rows:
            db.delete(trade)
        if existing_rows:
            db.flush()

    inserted = 0
    for row in deduped_rows:
        db.add(Trade(**row))
        inserted += 1

    db.commit()
    return inserted


def _finalize_trade_import(request: Request, db: Session, inserted: int):
    all_trades = db.query(Trade).order_by(Trade.date.asc(), Trade.id.asc()).all()
    daily_map = calculate_daily_trade_map(all_trades)

    now = datetime.utcnow().isoformat()
    conflicts = []
    resolved: Dict[str, Dict[str, float]] = {}
    for day, values in daily_map.items():
        realized = values["realized"]
        unrealized = values["unrealized"]
        ds = db.get(DailySummary, day)
        if _is_missing_summary(ds):
            resolved[day] = values
            continue

        if _is_close(ds.realized, realized) and _is_close(ds.unrealized, unrealized):
            resolved[day] = values
        else:
            conflicts.append(
                {
                    "date": day,
                    "existing": {
                        "realized": float(ds.realized),
                        "unrealized": float(ds.unrealized),
                        "updated_at": ds.updated_at,
                    },
                    "new": {
                        "realized": realized,
                        "unrealized": unrealized,
                    },
                }
            )

    if resolved:
        upsert_daily_summaries(db, resolved, timestamp=now)
    db.commit()

    if conflicts:
        return request.app.state.templates.TemplateResponse(
            request,
            "import_conflicts.html",
            {
                "request": request,
                "conflicts": conflicts,
                "inserted": inserted,
            },
        )

    return RedirectResponse(url="/", status_code=303)


@router.post("/import/trades")
async def import_trades(
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_session),
):
    content = await file.read()
    rows = parse_trade_csv(content)

    if not rows:
        return RedirectResponse(
            "/settings?trade_csv_error=no_trades#stock-data-import",
            status_code=303,
        )

    inserted = _persist_trade_rows(db, rows)
    return _finalize_trade_import(request, db, inserted)


@router.post("/import/thinkorswim")
async def import_thinkorswim(
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_session),
):
    content = await file.read()
    rows = parse_thinkorswim_csv(content)

    if not rows:
        return RedirectResponse(
            "/settings?thinkorswim_error=no_trades#stock-data-import",
            status_code=303,
        )

    inserted = _persist_trade_rows(db, rows)
    return _finalize_trade_import(request, db, inserted)


@router.post("/import/thinkorswim/conflicts", response_class=HTMLResponse)
async def resolve_conflicts(
    request: Request,
    db: Session = Depends(get_session),
):
    form = await request.form()
    dates = form.getlist("date")
    now = datetime.utcnow().isoformat()

    for day in dates:
        choice = form.get(f"choice_{day}")
        if choice != "new":
            continue

        realized = float(form.get(f"new_realized_{day}", 0.0))
        unrealized = float(form.get(f"new_unrealized_{day}", 0.0))
        ds = db.get(DailySummary, day)
        if ds:
            ds.realized = realized
            ds.unrealized = unrealized
            ds.total_invested = unrealized
            ds.updated_at = now
        else:
            db.add(
                DailySummary(
                    date=day,
                    realized=realized,
                    unrealized=unrealized,
                    total_invested=unrealized,
                    updated_at=now,
                )
            )

    db.commit()
    return RedirectResponse(url="/", status_code=303)
