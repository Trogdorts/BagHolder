import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.main import create_app  # noqa: E402
from app.api.routes_calendar import (  # noqa: E402
    TradeUpdate,
    TradeUpdatePayload,
    get_trades_for_day,
    save_trades_for_day,
)
from app.core import database as db  # noqa: E402
from app.core.models import Trade  # noqa: E402


def _init_app(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    create_app()


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
