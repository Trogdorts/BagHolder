from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.core.database import get_session
from app.core.models import DailySummary
from app.services.summaries import group_by_week

router = APIRouter()

@router.get("/api/stats/monthly/{year}/{month}")
def stats_monthly(year: int, month: int, db: Session = Depends(get_session)):
    y = f"{year:04d}"; m = f"{month:02d}"
    rows = db.query(DailySummary).filter(DailySummary.date.like(f"{y}-{m}-%")).all()
    realized = sum(float(r.realized) for r in rows)
    unreal = sum(float(r.unrealized) for r in rows)
    return {"realized": realized, "unrealized": unreal}

@router.get("/api/stats/weekly")
def stats_weekly(db: Session = Depends(get_session)):
    # returns per-week aggregates across all data
    rows = db.query(DailySummary).all()
    daily = {r.date: {"realized": float(r.realized), "unrealized": float(r.unrealized), "total_invested": float(r.total_invested)} for r in rows}
    return group_by_week(daily)
