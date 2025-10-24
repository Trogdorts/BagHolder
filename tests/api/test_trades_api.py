import sys
from pathlib import Path

import pytest
from fastapi import HTTPException
from starlette.requests import Request

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.main import create_app  # noqa: E402
from app.api.routes_calendar import (  # noqa: E402
    TradeUpdate,
    TradeUpdatePayload,
    clear_trades_for_day,
    get_trades_for_day,
    save_trades_for_day,
)
from app.api.routes_import import (  # noqa: E402
    _finalize_trade_import,
    _persist_trade_rows,
)
from app.core import database as db  # noqa: E402
from app.core.models import DailySummary, Trade  # noqa: E402
from app.services.import_trades_csv import parse_trade_csv  # noqa: E402


def _init_app(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    return create_app()


def test_get_trades_for_day_returns_sorted_list(tmp_path, monkeypatch):
    _init_app(tmp_path, monkeypatch)
    try:
        with db.SessionLocal() as session:
            session.add_all(
                [
                    Trade(date="2024-04-01", symbol="AAPL", action="BUY", qty=2.0, price=150.0, amount=-300.0),
                    Trade(date="2024-04-01", symbol="MSFT", action="SELL", qty=1.0, price=100.0, amount=100.0),
                    Trade(date="2024-04-02", symbol="TSLA", action="BUY", qty=1.0, price=200.0, amount=-200.0),
                ]
            )
            session.commit()

            payload = get_trades_for_day("2024-04-01", db=session)
            assert len(payload["trades"]) == 2
            assert [trade["symbol"] for trade in payload["trades"]] == ["AAPL", "MSFT"]
    finally:
        db.dispose_engine()


def test_save_trades_updates_existing_and_removes_missing(tmp_path, monkeypatch):
    _init_app(tmp_path, monkeypatch)
    try:
        with db.SessionLocal() as session:
            first = Trade(date="2024-05-01", symbol="AAPL", action="BUY", qty=1.0, price=120.0, amount=-120.0)
            second = Trade(date="2024-05-01", symbol="MSFT", action="SELL", qty=2.0, price=200.0, amount=400.0)
            session.add_all([first, second])
            session.commit()
            first_id = first.id

        with db.SessionLocal() as session:
            payload = TradeUpdatePayload(
                trades=[
                    TradeUpdate(id=first_id, symbol="aapl", action="buy", qty=3, price=110),
                    TradeUpdate(symbol="tsla", action="SELL", qty=1.5, price=250),
                ]
            )
            result = save_trades_for_day("2024-05-01", payload=payload, db=session)
            assert result["ok"] is True
            assert isinstance(result.get("summary"), dict)
            assert result["summary"]["realized"] == pytest.approx(0.0)
            assert result["summary"]["unrealized"] == pytest.approx(0.0)
            returned_symbols = [trade["symbol"] for trade in result["trades"]]
            assert returned_symbols == ["AAPL", "TSLA"]
            returned_qty = {trade["symbol"]: trade["qty"] for trade in result["trades"]}
            assert returned_qty["AAPL"] == pytest.approx(3.0)
            assert returned_qty["TSLA"] == pytest.approx(1.5)

        with db.SessionLocal() as session:
            rows = (
                session.query(Trade)
                .filter(Trade.date == "2024-05-01")
                .order_by(Trade.id.asc())
                .all()
            )
            assert len(rows) == 2
            symbols = {row.symbol for row in rows}
            assert symbols == {"AAPL", "TSLA"}
            updated = next(row for row in rows if row.symbol == "AAPL")
            assert updated.qty == pytest.approx(3.0)
            assert updated.price == pytest.approx(110.0)
            assert updated.amount == pytest.approx(-330.0)
            new_trade = next(row for row in rows if row.symbol == "TSLA")
            assert new_trade.action == "SELL"
            assert new_trade.amount == pytest.approx(375.0)
            summary_row = session.get(DailySummary, "2024-05-01")
            assert summary_row is not None
            assert summary_row.realized == pytest.approx(0.0)
            assert summary_row.unrealized == pytest.approx(0.0)
    finally:
        db.dispose_engine()


def test_save_trades_unknown_id_raises(tmp_path, monkeypatch):
    _init_app(tmp_path, monkeypatch)
    try:
        with db.SessionLocal() as session:
            session.add(Trade(date="2024-06-01", symbol="NVDA", action="BUY", qty=1.0, price=100.0, amount=-100.0))
            session.commit()

        with db.SessionLocal() as session:
            payload = TradeUpdatePayload(
                trades=[TradeUpdate(id=9999, symbol="NVDA", action="BUY", qty=1.0, price=120.0)]
            )
            with pytest.raises(HTTPException) as excinfo:
                save_trades_for_day("2024-06-01", payload=payload, db=session)
            assert excinfo.value.status_code == 404
    finally:
        db.dispose_engine()


def test_save_trades_recomputes_following_day_summary(tmp_path, monkeypatch):
    _init_app(tmp_path, monkeypatch)
    try:
        with db.SessionLocal() as session:
            buy = Trade(date="2024-01-02", symbol="AAPL", action="BUY", qty=1.0, price=100.0, amount=-100.0)
            sell = Trade(date="2024-01-03", symbol="AAPL", action="SELL", qty=1.0, price=120.0, amount=120.0)
            session.add_all([buy, sell])
            session.commit()
            buy_id = buy.id

        with db.SessionLocal() as session:
            payload = TradeUpdatePayload(
                trades=[TradeUpdate(id=buy_id, symbol="AAPL", action="BUY", qty=1.0, price=110.0)]
            )
            result = save_trades_for_day("2024-01-02", payload=payload, db=session)
            assert result["ok"] is True
            assert result["summary"]["realized"] == pytest.approx(0.0)
            assert result["summary"]["unrealized"] == pytest.approx(0.0)

        with db.SessionLocal() as session:
            day1 = session.get(DailySummary, "2024-01-02")
            day2 = session.get(DailySummary, "2024-01-03")
            assert day1 is not None
            assert day1.realized == pytest.approx(0.0)
            assert day1.unrealized == pytest.approx(0.0)
            assert day2 is not None
            assert day2.realized == pytest.approx(10.0)
            assert day2.unrealized == pytest.approx(0.0)
    finally:
        db.dispose_engine()


def test_clear_trades_for_day_removes_summary(tmp_path, monkeypatch):
    _init_app(tmp_path, monkeypatch)
    try:
        with db.SessionLocal() as session:
            buy = Trade(date="2024-03-05", symbol="AAPL", action="BUY", qty=2.0, price=100.0, amount=-200.0)
            sell = Trade(date="2024-03-06", symbol="AAPL", action="SELL", qty=2.0, price=120.0, amount=240.0)
            session.add_all([buy, sell])
            session.commit()
            buy_id = buy.id

        with db.SessionLocal() as session:
            payload = TradeUpdatePayload(
                trades=[TradeUpdate(id=buy_id, symbol="AAPL", action="BUY", qty=2.0, price=100.0)]
            )
            save_trades_for_day("2024-03-05", payload=payload, db=session)

        with db.SessionLocal() as session:
            result = clear_trades_for_day("2024-03-05", db=session)
            assert result["ok"] is True
            assert result["deleted"] == 1

        with db.SessionLocal() as session:
            remaining = session.query(Trade).filter(Trade.date == "2024-03-05").count()
            assert remaining == 0
            assert session.get(DailySummary, "2024-03-05") is None
            day2 = session.get(DailySummary, "2024-03-06")
            assert day2 is not None
            assert day2.realized == pytest.approx(0.0)
            assert day2.unrealized == pytest.approx(0.0)
    finally:
        db.dispose_engine()


def test_import_trades_overwrites_existing_rows(tmp_path, monkeypatch):
    _init_app(tmp_path, monkeypatch)
    try:
        with db.SessionLocal() as session:
            session.add_all(
                [
                    Trade(date="2025-10-16", symbol="ORCL", action="SELL", qty=100.0, price=310.0, amount=31000.0),
                    Trade(date="2025-10-16", symbol="MLTX", action="BUY", qty=50.0, price=8.0, amount=-400.0),
                    Trade(date="2025-10-15", symbol="MSFT", action="SELL", qty=1.0, price=100.0, amount=100.0),
                ]
            )
            session.commit()

        csv_content = """date,symbol,action,qty,price,amount\n2025-10-16,ORCL,SELL,100,320.17,32017\n2025-10-16,MLTX,BUY,100,10,-1000\n"""
        rows = parse_trade_csv(csv_content.encode("utf-8"))
        assert len(rows) == 2

        with db.SessionLocal() as session:
            inserted = _persist_trade_rows(session, rows)
            assert inserted == 2

        with db.SessionLocal() as session:
            day_rows = (
                session.query(Trade)
                .filter(Trade.date == "2025-10-16")
                .order_by(Trade.symbol.asc())
                .all()
            )
            assert len(day_rows) == 2
            assert {row.symbol for row in day_rows} == {"ORCL", "MLTX"}
            orcl = next(row for row in day_rows if row.symbol == "ORCL")
            assert orcl.price == pytest.approx(320.17)
            assert orcl.amount == pytest.approx(32017.0)
            mltx = next(row for row in day_rows if row.symbol == "MLTX")
            assert mltx.qty == pytest.approx(100.0)
            assert mltx.amount == pytest.approx(-1000.0)

            other_day = (
                session.query(Trade)
                .filter(Trade.date == "2025-10-15")
                .one()
            )
            assert other_day.symbol == "MSFT"
            assert other_day.price == pytest.approx(100.0)
    finally:
        db.dispose_engine()


def test_import_trades_auto_resolves_missing_summaries(tmp_path, monkeypatch):
    app = _init_app(tmp_path, monkeypatch)
    try:
        with db.SessionLocal() as session:
            session.add(
                DailySummary(
                    date="2024-01-02",
                    realized=0.0,
                    unrealized=0.0,
                    total_invested=0.0,
                    updated_at="",
                )
            )
            session.commit()

        rows = [
            {
                "date": "2024-01-02",
                "symbol": "AAPL",
                "action": "BUY",
                "qty": 1.0,
                "price": 100.0,
                "amount": -100.0,
            },
            {
                "date": "2024-01-02",
                "symbol": "AAPL",
                "action": "SELL",
                "qty": 1.0,
                "price": 130.0,
                "amount": 130.0,
            },
        ]

        with db.SessionLocal() as session:
            inserted = _persist_trade_rows(session, rows)
            request = Request({"type": "http", "method": "POST", "headers": [], "app": app})
            response = _finalize_trade_import(request, session, inserted)

            assert response.status_code == 303
            assert response.headers.get("location") == "/"

            summary = session.get(DailySummary, "2024-01-02")
            assert summary is not None
            assert summary.updated_at.strip() != ""
            assert summary.realized == pytest.approx(30.0)
            assert summary.unrealized == pytest.approx(0.0)
            assert summary.total_invested == pytest.approx(230.0)
    finally:
        db.dispose_engine()
