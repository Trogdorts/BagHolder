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
from app.core.models import DailySummary, NoteWeekly, Trade
from app.services.trade_summaries import recompute_daily_summaries


def _build_request(app):
    return Request({"type": "http", "app": app, "method": "GET", "path": "/", "headers": []})


def _get_day(weeks, target_date):
    for week in weeks:
        for day in week["days"]:
            if day["date"] == target_date:
                return day
    raise AssertionError(f"Date {target_date} not found in weeks data")


def _get_week(weeks, index):
    for week in weeks:
        if week["week_index"] == index:
            return week
    raise AssertionError(f"Week index {index} not found in weeks data")


def test_realized_hidden_for_buy_only_days(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    with db.SessionLocal() as session:
        session.add_all(
            [
                Trade(
                    date="2024-03-01",
                    symbol="AAPL",
                    action="BUY",
                    qty=1.0,
                    price=100.0,
                    amount=-100.0,
                ),
                Trade(
                    date="2024-03-02",
                    symbol="AAPL",
                    action="SELL",
                    qty=1.0,
                    price=100.0,
                    amount=100.0,
                ),
            ]
        )
        session.commit()
        recompute_daily_summaries(session)
        session.commit()

    with db.SessionLocal() as session:
        request = _build_request(app)
        response = calendar_view(2024, 3, request, db=session)
        weeks = response.context["weeks"]
        buy_day = _get_day(weeks, date(2024, 3, 1))
        sell_day = _get_day(weeks, date(2024, 3, 2))

        assert buy_day["show_realized"] is False
        assert sell_day["show_realized"] is True
        assert sell_day["realized"] == pytest.approx(0.0)

    db.dispose_engine()


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


def test_weekly_notes_follow_iso_week(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    with db.SessionLocal() as session:
        session.add(
            NoteWeekly(
                year=2024,
                week=1,
                note="Week one note",
                updated_at="now",
            )
        )
        session.commit()

    with db.SessionLocal() as session:
        request = _build_request(app)
        response = calendar_view(2024, 1, request, db=session)
        weeks = response.context["weeks"]
        jan_week_one = _get_week(weeks, 1)
        assert jan_week_one["week_year"] == 2024
        assert jan_week_one["week_number"] == 1
        assert jan_week_one["note"] == "Week one note"
        assert jan_week_one["has_note"] is True

    with db.SessionLocal() as session:
        request = _build_request(app)
        response = calendar_view(2024, 2, request, db=session)
        weeks = response.context["weeks"]
        feb_first_week = _get_week(weeks, 1)
        assert feb_first_week["week_number"] == 5
        assert feb_first_week["note"] == ""
        assert feb_first_week["has_note"] is False

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


def test_unrealized_not_extended_past_today(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

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
        session.commit()

    class FrozenDate(date):
        @classmethod
        def today(cls):
            return cls(2024, 1, 3)

    monkeypatch.setattr("app.api.routes_calendar.date", FrozenDate)

    with db.SessionLocal() as session:
        request = _build_request(app)
        response = calendar_view(2024, 1, request, db=session)
        weeks = response.context["weeks"]
        jan_second = _get_day(weeks, date(2024, 1, 2))
        assert jan_second["unrealized"] == pytest.approx(100.0)
        jan_fourth = _get_day(weeks, date(2024, 1, 4))
        assert jan_fourth["unrealized"] == 0.0

    db.dispose_engine()


def test_show_trade_badges_handles_string_values(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    app.state.config.raw["ui"]["show_trade_count"] = "false"
    with db.SessionLocal() as session:
        request = _build_request(app)
        response = calendar_view(2024, 1, request, db=session)
        assert response.context["show_trade_badges"] is False

    app.state.config.raw["ui"]["show_trade_count"] = "TrUe"
    with db.SessionLocal() as session:
        request = _build_request(app)
        response = calendar_view(2024, 1, request, db=session)
        assert response.context["show_trade_badges"] is True

    db.dispose_engine()
