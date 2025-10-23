from datetime import datetime

from fastapi import APIRouter, Depends, File, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.core.database import get_session
from app.core.models import DailySummary, Trade
from app.services.import_daily_summaries import parse_daily_summary_csv
from app.services.import_thinkorswim import (
    compute_daily_pnl_records,
    parse_thinkorswim_csv,
)
from app.services.import_trades_csv import parse_trade_csv

router = APIRouter()


def _is_close(a: float, b: float, tol: float = 0.01) -> bool:
    return abs(float(a) - float(b)) <= tol


@router.get("/import", response_class=RedirectResponse)
def import_page(request: Request):
    return RedirectResponse("/settings#stock-data-import", status_code=307)


def _persist_trade_rows(db: Session, rows):
    inserted = 0
    for r in rows:
        exists = (
            db.query(Trade)
            .filter_by(
                date=r["date"],
                symbol=r["symbol"],
                action=r["action"],
                qty=r["qty"],
                price=r["price"],
                amount=r["amount"],
            )
            .first()
        )
        if exists:
            continue
        db.add(Trade(**r))
        inserted += 1
    db.commit()
    return inserted


def _finalize_trade_import(request: Request, db: Session, inserted: int):
    all_trades = db.query(Trade).order_by(Trade.date.asc(), Trade.id.asc()).all()
    trade_records = []
    for t in all_trades:
        try:
            dt = datetime.strptime(t.date, "%Y-%m-%d").date()
        except ValueError:
            continue
        trade_records.append(
            {
                "date": dt,
                "side": t.action.upper(),
                "symbol": t.symbol.upper(),
                "quantity": float(t.qty),
                "price": float(t.price),
            }
        )

    daily_df = compute_daily_pnl_records(trade_records)
    daily_map = {}
    for record in daily_df.to_dict("records"):
        date_value = record["date"]
        day_key = (
            date_value.strftime("%Y-%m-%d")
            if hasattr(date_value, "strftime")
            else str(date_value)
        )
        daily_map[day_key] = {
            "realized": float(record.get("realized_pl", 0.0)),
            "unrealized": float(record.get("unrealized_pl", 0.0)),
        }

    now = datetime.utcnow().isoformat()
    conflicts = []
    for day, values in daily_map.items():
        realized = values["realized"]
        unrealized = values["unrealized"]
        ds = db.get(DailySummary, day)
        if ds is None:
            db.add(
                DailySummary(
                    date=day,
                    realized=realized,
                    unrealized=unrealized,
                    total_invested=unrealized,
                    updated_at=now,
                )
            )
            continue

        if _is_close(ds.realized, realized) and _is_close(ds.unrealized, unrealized):
            ds.realized = realized
            ds.unrealized = unrealized
            ds.total_invested = unrealized
            ds.updated_at = now
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

    db.commit()

    if conflicts:
        return request.app.state.templates.TemplateResponse(
            "import_conflicts.html",
            {
                "request": request,
                "conflicts": conflicts,
                "inserted": inserted,
            },
        )

    return RedirectResponse(url="/", status_code=303)


@router.post("/import/daily-summaries")
async def import_daily_summaries(
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_session),
):
    content = await file.read()
    rows = parse_daily_summary_csv(content)

    if not rows:
        return RedirectResponse(
            "/settings?daily_summary_error=no_summaries#stock-data-import",
            status_code=303,
        )

    now = datetime.utcnow().isoformat()
    for row in rows:
        date = row["date"]
        realized = float(row.get("realized", 0.0))
        unrealized = float(row.get("unrealized", 0.0))
        total_invested = float(row.get("total_invested", unrealized))
        updated_at = row.get("updated_at") or now

        summary = db.get(DailySummary, date)
        if summary is None:
            db.add(
                DailySummary(
                    date=date,
                    realized=realized,
                    unrealized=unrealized,
                    total_invested=total_invested,
                    updated_at=updated_at,
                )
            )
            continue

        summary.realized = realized
        summary.unrealized = unrealized
        summary.total_invested = total_invested
        summary.updated_at = updated_at

    db.commit()

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
