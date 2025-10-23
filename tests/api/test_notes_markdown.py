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
from app.core.models import NoteDaily  # noqa: E402


def _build_request(app):
    return Request({"type": "http", "app": app, "method": "GET", "headers": []})


def _find_day(weeks, target_date: date):
    for week in weeks:
        for day in week["days"]:
            if day["date"] == target_date:
                return day
    raise AssertionError(f"Unable to locate {target_date} in calendar weeks")


def test_daily_note_defaults_to_markdown(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()
    try:
        with db.SessionLocal() as session:
            payload = routes_notes.get_daily("2024-01-01", db=session)

        assert payload["note"] == ""
        assert payload["is_markdown"] is True
    finally:
        db.dispose_engine()


def test_daily_note_persists_markdown_toggle(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()
    try:
        with db.SessionLocal() as session:
            routes_notes.set_daily(
                "2024-02-10",
                note="plain entry",
                use_markdown=False,
                db=session,
            )

        with db.SessionLocal() as session:
            payload = routes_notes.get_daily("2024-02-10", db=session)
            assert payload["note"] == "plain entry"
            assert payload["is_markdown"] is False

        with db.SessionLocal() as session:
            routes_notes.set_daily(
                "2024-02-10",
                note="**bold**",
                use_markdown=True,
                db=session,
            )

        with db.SessionLocal() as session:
            final_payload = routes_notes.get_daily("2024-02-10", db=session)
            assert final_payload["note"] == "**bold**"
            assert final_payload["is_markdown"] is True

        with db.SessionLocal() as session:
            record = session.get(NoteDaily, "2024-02-10")
            assert record is not None
            assert record.is_markdown is True
    finally:
        db.dispose_engine()


def test_calendar_context_includes_markdown_flag(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()
    try:
        with db.SessionLocal() as session:
            session.add(
                NoteDaily(
                    date="2024-03-05",
                    note="simple text",
                    is_markdown=False,
                    updated_at="now",
                )
            )
            session.add(
                NoteDaily(
                    date="2024-03-06",
                    note="**formatted**",
                    is_markdown=True,
                    updated_at="now",
                )
            )
            session.commit()

            request = _build_request(app)
            response = calendar_view(2024, 3, request, db=session)
            weeks = response.context["weeks"]

            march_fifth = _find_day(weeks, date(2024, 3, 5))
            assert march_fifth["note"] == "simple text"
            assert march_fifth["note_is_markdown"] is False

            march_sixth = _find_day(weeks, date(2024, 3, 6))
            assert march_sixth["note"] == "**formatted**"
            assert march_sixth["note_is_markdown"] is True
    finally:
        db.dispose_engine()
