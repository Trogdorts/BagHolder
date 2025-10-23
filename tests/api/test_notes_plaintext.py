import sys
from datetime import date
from pathlib import Path

from starlette.requests import Request

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.main import create_app  # noqa: E402
from app.api import routes_notes  # noqa: E402
from app.api.routes_calendar import calendar_view  # noqa: E402
from app.core import database as db  # noqa: E402
from app.core.models import NoteDaily, NoteWeekly, NoteMonthly  # noqa: E402


def _build_request(app):
    return Request({"type": "http", "app": app, "method": "GET", "headers": []})


def _find_day(weeks, target_date: date):
    for week in weeks:
        for day in week["days"]:
            if day["date"] == target_date:
                return day
    raise AssertionError(f"Unable to locate {target_date} in calendar weeks")


def test_daily_note_defaults_to_plain_text(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()
    try:
        with db.SessionLocal() as session:
            payload = routes_notes.get_daily("2024-01-01", db=session)

        assert payload["note"] == ""
        assert payload["updated_at"] is None
    finally:
        db.dispose_engine()


def test_daily_note_update_sets_plaintext_flags(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()
    try:
        with db.SessionLocal() as session:
            result = routes_notes.set_daily("2024-02-10", note="plain entry", db=session)
            first_timestamp = result["updated_at"]
            assert isinstance(first_timestamp, str)

        with db.SessionLocal() as session:
            payload = routes_notes.get_daily("2024-02-10", db=session)
            assert payload["note"] == "plain entry"
            assert isinstance(payload["updated_at"], str)

        with db.SessionLocal() as session:
            routes_notes.set_daily("2024-02-10", note="**bold**", db=session)

        with db.SessionLocal() as session:
            final_payload = routes_notes.get_daily("2024-02-10", db=session)
            assert final_payload["note"] == "**bold**"
            assert isinstance(final_payload["updated_at"], str)

        with db.SessionLocal() as session:
            record = session.get(NoteDaily, "2024-02-10")
            assert record is not None
            assert record.is_markdown is False
    finally:
        db.dispose_engine()


def test_weekly_and_monthly_notes_include_updated_at(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()
    try:
        with db.SessionLocal() as session:
            weekly_result = routes_notes.set_weekly(2024, 8, note="weekly overview", db=session)
            monthly_result = routes_notes.set_monthly(2024, 3, note="monthly recap", db=session)

            assert isinstance(weekly_result["updated_at"], str)
            assert isinstance(monthly_result["updated_at"], str)

        with db.SessionLocal() as session:
            weekly_payload = routes_notes.get_weekly(2024, 8, db=session)
            monthly_payload = routes_notes.get_monthly(2024, 3, db=session)

            assert weekly_payload["note"] == "weekly overview"
            assert isinstance(weekly_payload["updated_at"], str)
            assert monthly_payload["note"] == "monthly recap"
            assert isinstance(monthly_payload["updated_at"], str)
    finally:
        db.dispose_engine()


def test_calendar_context_includes_note_metadata(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()
    try:
        with db.SessionLocal() as session:
            session.add(
                NoteDaily(
                    date="2024-03-05",
                    note="simple text",
                    updated_at="2024-03-05T12:00:00",
                )
            )
            session.add(
                NoteWeekly(
                    year=2024,
                    week=10,
                    note="weekly note",
                    updated_at="2024-03-09T08:30:00",
                )
            )
            session.add(
                NoteMonthly(
                    year=2024,
                    month=3,
                    note="monthly summary",
                    updated_at="2024-03-31T21:15:00",
                )
            )
            session.commit()

            request = _build_request(app)
            response = calendar_view(2024, 3, request, db=session)
            weeks = response.context["weeks"]

            march_fifth = _find_day(weeks, date(2024, 3, 5))
            assert march_fifth["note"] == "simple text"
            assert march_fifth["note_updated_at"] == "2024-03-05T12:00:00"

            week_entry = next(
                week
                for week in weeks
                if week["week_number"] == 10 and week["week_year"] == 2024
            )
            assert week_entry["note"] == "weekly note"
            assert week_entry["note_updated_at"] == "2024-03-09T08:30:00"

            month_note = response.context["month_note"]
            assert month_note["note"] == "monthly summary"
            assert month_note["updated_at"] == "2024-03-31T21:15:00"
    finally:
        db.dispose_engine()
