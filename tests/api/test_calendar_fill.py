import sys
from pathlib import Path
from datetime import date

import pytest
from starlette.requests import Request

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.main import create_app
from app.api.routes_calendar import calendar_view
from app.core import database as db
from app.core.models import DailySummary


def _build_request(app):
    return Request({"type": "http", "app": app, "method": "GET", "headers": []})


def _get_day(weeks, target_date):
    for week in weeks:
        for day in week["days"]:
            if day["date"] == target_date:
                return day
    raise AssertionError(f"Date {target_date} not found in weeks data")


def test_average_fill_for_missing_unrealized(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()
    app.state.config.raw["ui"]["unrealized_fill_strategy"] = "average_neighbors"

    with db.SessionLocal() as session:
        session.add(
            DailySummary(
                date="2024-01-01",
                realized=0.0,
                unrealized=100.0,
                total_invested=100.0,
                updated_at="now",
            )
        )
        session.add(
            DailySummary(
                date="2024-01-05",
                realized=0.0,
                unrealized=200.0,
                total_invested=200.0,
                updated_at="now",
            )
        )
        session.commit()

    with db.SessionLocal() as session:
        request = _build_request(app)
        response = calendar_view(2024, 1, request, db=session)
        weeks = response.context["weeks"]
        day = _get_day(weeks, date(2024, 1, 2))
        assert day["unrealized"] == pytest.approx(150.0)

    db.dispose_engine()


def test_average_fill_falls_back_without_future_value(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()
    app.state.config.raw["ui"]["unrealized_fill_strategy"] = "average_neighbors"

    with db.SessionLocal() as session:
        session.add(
            DailySummary(
                date="2024-02-01",
                realized=0.0,
                unrealized=75.0,
                total_invested=75.0,
                updated_at="now",
            )
        )
        session.commit()

    with db.SessionLocal() as session:
        request = _build_request(app)
        response = calendar_view(2024, 2, request, db=session)
        weeks = response.context["weeks"]
        day = _get_day(weeks, date(2024, 2, 2))
        assert day["unrealized"] == pytest.approx(75.0)

    db.dispose_engine()


def test_average_fill_uses_next_month_value(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()
    app.state.config.raw["ui"]["unrealized_fill_strategy"] = "average_neighbors"

    with db.SessionLocal() as session:
        session.add(
            DailySummary(
                date="2024-01-10",
                realized=0.0,
                unrealized=40.0,
                total_invested=40.0,
                updated_at="now",
            )
        )
        session.add(
            DailySummary(
                date="2024-02-05",
                realized=0.0,
                unrealized=80.0,
                total_invested=80.0,
                updated_at="now",
            )
        )
        session.commit()

    with db.SessionLocal() as session:
        request = _build_request(app)
        response = calendar_view(2024, 1, request, db=session)
        weeks = response.context["weeks"]
        day = _get_day(weeks, date(2024, 1, 15))
        assert day["unrealized"] == pytest.approx(60.0)

    db.dispose_engine()
