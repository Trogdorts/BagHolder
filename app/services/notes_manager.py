from sqlalchemy.orm import Session
from app.core.models import NoteDaily, NoteWeekly, NoteMonthly
from datetime import datetime


def _current_timestamp() -> str:
    return datetime.utcnow().isoformat()


def set_daily_note(db: Session, date_str: str, note: str) -> str:
    nd = db.get(NoteDaily, date_str)
    now = _current_timestamp()
    if nd:
        nd.note = note
        nd.is_markdown = False
        nd.updated_at = now
    else:
        db.add(
            NoteDaily(
                date=date_str,
                note=note,
                is_markdown=False,
                updated_at=now,
            )
        )
    db.commit()
    return now


def get_daily_note(db: Session, date_str: str) -> tuple[str, str | None]:
    nd = db.get(NoteDaily, date_str)
    if not nd:
        return "", None
    return nd.note, nd.updated_at or None


def set_weekly_note(db: Session, year: int, week: int, note: str) -> str:
    now = _current_timestamp()
    record = db.query(NoteWeekly).filter_by(year=year, week=week).first()
    if record:
        record.note = note
        record.updated_at = now
    else:
        db.add(NoteWeekly(year=year, week=week, note=note, updated_at=now))
    db.commit()
    return now


def get_weekly_note(db: Session, year: int, week: int) -> tuple[str, str | None]:
    record = db.query(NoteWeekly).filter_by(year=year, week=week).first()
    if not record:
        return "", None
    return record.note, record.updated_at or None


def set_monthly_note(db: Session, year: int, month: int, note: str) -> str:
    now = _current_timestamp()
    record = db.query(NoteMonthly).filter_by(year=year, month=month).first()
    if record:
        record.note = note
        record.updated_at = now
    else:
        db.add(NoteMonthly(year=year, month=month, note=note, updated_at=now))
    db.commit()
    return now


def get_monthly_note(db: Session, year: int, month: int) -> tuple[str, str | None]:
    record = db.query(NoteMonthly).filter_by(year=year, month=month).first()
    if not record:
        return "", None
    return record.note, record.updated_at or None
