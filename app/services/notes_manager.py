from sqlalchemy.orm import Session
from app.core.models import NoteDaily, NoteWeekly, NoteMonthly
from datetime import datetime

def set_daily_note(db: Session, date_str: str, note: str):
    nd = db.get(NoteDaily, date_str)
    now = datetime.utcnow().isoformat()
    if nd:
        nd.note = note
        nd.updated_at = now
    else:
        db.add(NoteDaily(date=date_str, note=note, updated_at=now))
    db.commit()

def get_daily_note(db: Session, date_str: str) -> str:
    nd = db.get(NoteDaily, date_str)
    return nd.note if nd else ""

def set_weekly_note(db: Session, year: int, week: int, note: str):
    now = datetime.utcnow().isoformat()
    x = db.query(NoteWeekly).filter_by(year=year, week=week).first()
    if x:
        x.note = note
        x.updated_at = now
    else:
        db.add(NoteWeekly(year=year, week=week, note=note, updated_at=now))
    db.commit()

def get_weekly_note(db: Session, year: int, week: int) -> str:
    x = db.query(NoteWeekly).filter_by(year=year, week=week).first()
    return x.note if x else ""

def set_monthly_note(db: Session, year: int, month: int, note: str):
    now = datetime.utcnow().isoformat()
    x = db.query(NoteMonthly).filter_by(year=year, month=month).first()
    if x:
        x.note = note
        x.updated_at = now
    else:
        db.add(NoteMonthly(year=year, month=month, note=note, updated_at=now))
    db.commit()

def get_monthly_note(db: Session, year: int, month: int) -> str:
    x = db.query(NoteMonthly).filter_by(year=year, month=month).first()
    return x.note if x else ""
