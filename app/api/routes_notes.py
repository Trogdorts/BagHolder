from fastapi import APIRouter, Request, Form, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from app.core.database import get_session
from app.services.notes_manager import (
    set_daily_note,
    get_daily_note,
    set_weekly_note,
    get_weekly_note,
    set_monthly_note,
    get_monthly_note,
)

router = APIRouter()

@router.get("/api/notes/daily/{date}")
def get_daily(date: str, db: Session = Depends(get_session)):
    return {"note": get_daily_note(db, date)}

@router.post("/api/notes/daily/{date}")
def set_daily(date: str, note: str = Form(""), db: Session = Depends(get_session)):
    set_daily_note(db, date, note)
    return {"ok": True}

@router.get("/api/notes/weekly/{year}/{week}")
def get_weekly(year: int, week: int, db: Session = Depends(get_session)):
    return {"note": get_weekly_note(db, year, week)}

@router.post("/api/notes/weekly/{year}/{week}")
def set_weekly(year: int, week: int, note: str = Form(""), db: Session = Depends(get_session)):
    set_weekly_note(db, year, week, note)
    return {"ok": True}

@router.get("/api/notes/monthly/{year}/{month}")
def get_monthly(year: int, month: int, db: Session = Depends(get_session)):
    return {"note": get_monthly_note(db, year, month)}

@router.post("/api/notes/monthly/{year}/{month}")
def set_monthly(year: int, month: int, note: str = Form(""), db: Session = Depends(get_session)):
    set_monthly_note(db, year, month, note)
    return {"ok": True}
