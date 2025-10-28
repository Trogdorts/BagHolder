from pathlib import Path
import sys

import pytest
from fastapi import HTTPException

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.main import create_app  # noqa: E402
from app.api.routes_calendar import (  # noqa: E402
    DividendUpdate,
    DividendUpdatePayload,
    clear_dividends_for_day,
    get_dividends_for_day,
    save_dividends_for_day,
)
from app.api.routes_import import _persist_dividend_rows  # noqa: E402
from app.core import database as db  # noqa: E402
from app.core.models import Dividend  # noqa: E402


def _init_app(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    return create_app()


def test_get_dividends_for_day_returns_sorted_list(tmp_path, monkeypatch):
    _init_app(tmp_path, monkeypatch)
    try:
        with db.SessionLocal() as session:
            session.add_all(
                [
                    Dividend(
                        date="2025-10-27",
                        symbol="CODX",
                        description="CO-DIAGNOSTICS INC",
                        action="Cash Dividend",
                        qty=0.0,
                        price=0.0,
                        fee=0.0,
                        amount=43.41,
                        sequence=2,
                    ),
                    Dividend(
                        date="2025-10-27",
                        symbol="GDXY",
                        description="YIELDMAX GOLD",
                        action="Qualified Dividend",
                        qty=0.0,
                        price=0.0,
                        fee=0.0,
                        amount=225.0,
                        sequence=1,
                    ),
                ]
            )
            session.commit()

            payload = get_dividends_for_day("2025-10-27", db=session)
            actions = [row["action"] for row in payload["dividends"]]
            assert actions == ["Qualified Dividend", "Cash Dividend"]
    finally:
        db.dispose_engine()


def test_save_dividends_updates_existing_and_removes_missing(tmp_path, monkeypatch):
    _init_app(tmp_path, monkeypatch)
    try:
        with db.SessionLocal() as session:
            first = Dividend(
                date="2025-10-24",
                symbol="GDXY",
                description="YIELDMAX",
                action="Cash Dividend",
                qty=0.0,
                price=0.0,
                fee=0.0,
                amount=43.41,
                sequence=0,
            )
            session.add(first)
            session.commit()
            first_id = first.id

        with db.SessionLocal() as session:
            payload = DividendUpdatePayload(
                dividends=[
                    DividendUpdate(
                        id=first_id,
                        action="Cash Dividend",
                        symbol="GDXY",
                        description="YIELDMAX GOLD",
                        qty=0.0,
                        price=0.0,
                        fee=0.0,
                        amount=50.00,
                        time="",
                    ),
                    DividendUpdate(
                        action="Qualified Dividend",
                        symbol="ORCL",
                        description="ORACLE CORP",
                        qty=0.0,
                        price=0.0,
                        fee=0.0,
                        amount=225.0,
                        time="",
                    ),
                ]
            )
            result = save_dividends_for_day("2025-10-24", payload=payload, db=session)
            assert result["ok"] is True
            returned = result["dividends"]
            assert len(returned) == 2
            amounts = {row["symbol"]: row["amount"] for row in returned}
            assert amounts["GDXY"] == pytest.approx(50.0)
            assert amounts["ORCL"] == pytest.approx(225.0)

        with db.SessionLocal() as session:
            rows = (
                session.query(Dividend)
                .filter(Dividend.date == "2025-10-24")
                .order_by(Dividend.sequence.asc())
                .all()
            )
            assert len(rows) == 2
            assert {row.symbol for row in rows} == {"GDXY", "ORCL"}
    finally:
        db.dispose_engine()


def test_save_dividends_unknown_id_raises(tmp_path, monkeypatch):
    _init_app(tmp_path, monkeypatch)
    try:
        with db.SessionLocal() as session:
            session.add(
                Dividend(
                    date="2025-10-22",
                    symbol="T",
                    description="AT&T",
                    action="Qualified Dividend",
                    qty=0.0,
                    price=0.0,
                    fee=0.0,
                    amount=25.0,
                )
            )
            session.commit()

        with db.SessionLocal() as session:
            payload = DividendUpdatePayload(
                dividends=[
                    DividendUpdate(
                        id=9999,
                        action="Cash Dividend",
                        symbol="T",
                        description="AT&T",
                        qty=0.0,
                        price=0.0,
                        fee=0.0,
                        amount=25.0,
                    )
                ]
            )
            with pytest.raises(HTTPException) as excinfo:
                save_dividends_for_day("2025-10-22", payload=payload, db=session)
            assert excinfo.value.status_code == 404
    finally:
        db.dispose_engine()


def test_clear_dividends_for_day_removes_rows(tmp_path, monkeypatch):
    _init_app(tmp_path, monkeypatch)
    try:
        with db.SessionLocal() as session:
            session.add_all(
                [
                    Dividend(
                        date="2025-10-21",
                        symbol="GDXY",
                        description="YIELDMAX",
                        action="Cash Dividend",
                        qty=0.0,
                        price=0.0,
                        fee=0.0,
                        amount=43.41,
                        sequence=0,
                    ),
                    Dividend(
                        date="2025-10-21",
                        symbol="ORCL",
                        description="ORACLE",
                        action="Qualified Dividend",
                        qty=0.0,
                        price=0.0,
                        fee=0.0,
                        amount=225.0,
                        sequence=1,
                    ),
                ]
            )
            session.commit()

        with db.SessionLocal() as session:
            result = clear_dividends_for_day("2025-10-21", db=session)
            assert result["ok"] is True
            assert result["deleted"] == 2

        with db.SessionLocal() as session:
            remaining = session.query(Dividend).filter(Dividend.date == "2025-10-21").count()
            assert remaining == 0
    finally:
        db.dispose_engine()


def test_persist_dividend_rows_skips_duplicates(tmp_path, monkeypatch):
    _init_app(tmp_path, monkeypatch)
    try:
        rows = [
            {
                "date": "2025-10-24",
                "symbol": "GDXY",
                "description": "YIELDMAX",
                "action": "Cash Dividend",
                "qty": 0.0,
                "price": 0.0,
                "fee": 0.0,
                "amount": 43.41,
                "time": "",
            },
            {
                "date": "2025-10-24",
                "symbol": "GDXY",
                "description": "YIELDMAX",
                "action": "Cash Dividend",
                "qty": 0.0,
                "price": 0.0,
                "fee": 0.0,
                "amount": 43.41,
                "time": "",
            },
        ]
        with db.SessionLocal() as session:
            inserted = _persist_dividend_rows(session, rows)
            assert inserted == 1

        with db.SessionLocal() as session:
            count = session.query(Dividend).filter(Dividend.date == "2025-10-24").count()
            assert count == 1
    finally:
        db.dispose_engine()
