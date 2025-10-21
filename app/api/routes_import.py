from fastapi import APIRouter, UploadFile, File, Request, Depends
from fastapi.responses import RedirectResponse, HTMLResponse
from sqlalchemy.orm import Session
from ..core.database import get_session
from ..core.models import Trade, DailySummary
from ..services.import_thinkorswim import parse_thinkorswim_csv
from ..services.calculations import Ledger
from datetime import datetime
import os

router = APIRouter()

@router.get("/import", response_class=HTMLResponse)
def import_page(request: Request):
    return request.app.state.templates.TemplateResponse("import.html", {"request": request})

@router.post("/import/thinkorswim")
async def import_thinkorswim(request: Request, file: UploadFile = File(...), db: Session = Depends(get_session)):
    content = await file.read()
    rows = parse_thinkorswim_csv(content)

    # Insert trades with dedup
    inserted = 0
    for r in rows:
        exists = db.query(Trade).filter_by(
            date=r["date"], symbol=r["symbol"], action=r["action"],
            qty=r["qty"], price=r["price"], amount=r["amount"]
        ).first()
        if exists:
            continue
        db.add(Trade(**r))
        inserted += 1
    db.commit()

    # Recompute summaries via FIFO ledger on all trades
    all_trades = [{
        "date": t.date, "symbol": t.symbol, "action": t.action,
        "qty": t.qty, "price": t.price, "amount": t.amount
    } for t in db.query(Trade).order_by(Trade.date.asc(), Trade.id.asc()).all()]
    ledger = Ledger()
    realized_by_date, unreal_by_date = ledger.apply(all_trades)

    # Persist daily_summary
    dates = set(list(realized_by_date.keys()) + list(unreal_by_date.keys()))
    now = datetime.utcnow().isoformat()
    for d in dates:
        ds = db.get(DailySummary, d)
        realized = float(realized_by_date.get(d, 0.0))
        unreal = float(unreal_by_date.get(d, 0.0))
        if ds:
            ds.realized = realized
            ds.unrealized = unreal
            ds.total_invested = unreal
            ds.updated_at = now
        else:
            db.add(DailySummary(date=d, realized=realized, unrealized=unreal, total_invested=unreal, updated_at=now))
    db.commit()

    return RedirectResponse(url="/", status_code=303)
