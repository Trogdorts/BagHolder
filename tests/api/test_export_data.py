import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.main import create_app  # noqa: E402
from app.core import database as db  # noqa: E402
from app.core.models import (  # noqa: E402
    DailySummary,
    NoteDaily,
    Trade,
)
from app.api.routes_calendar import export_data  # noqa: E402


def test_export_data_excludes_total_and_updated_at(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))

    _app = create_app()

    with db.SessionLocal() as session:
        session.add(
            DailySummary(
                date="2024-03-01",
                realized=10.0,
                unrealized=5.0,
                total_invested=5.0,
                updated_at="now",
            )
        )
        session.add(
            DailySummary(
                date="2024-03-02",
                realized=-3.5,
                unrealized=0.0,
                total_invested=0.0,
                updated_at="then",
            )
        )
        session.commit()

    request = SimpleNamespace(app=_app)

    with db.SessionLocal() as session:
        response = export_data(
            request=request,
            start="2024-03-01",
            end="2024-03-02",
            db=session,
        )

    async def gather_body(resp):
        chunks = []
        async for chunk in resp.body_iterator:
            chunks.append(chunk)
        return b"".join(chunks)

    content = asyncio.run(gather_body(response))
    lines = content.decode("utf-8").strip().splitlines()

    assert lines[0] == "date,realized,unrealized"
    assert lines[1:] == [
        "2024-03-01,10.00,5.00",
        "2024-03-02,-3.50,0.00",
    ]

    db.dispose_engine()


def test_export_data_leaves_empty_values_when_configured(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))

    _app = create_app()

    _app.state.config.raw["export"]["fill_empty_with_zero"] = False

    request = SimpleNamespace(app=_app)

    class DummySummary:
        date = "2024-04-01"
        realized = None
        unrealized = None

    class DummyQuery:
        def filter(self, *args, **kwargs):
            return self

        def order_by(self, *args, **kwargs):
            return self

        def all(self):
            return [DummySummary()]

    class DummySession:
        def query(self, *args, **kwargs):
            return DummyQuery()

    response = export_data(
        request=request,
        start="2024-04-01",
        end="2024-04-01",
        db=DummySession(),
    )

    async def gather_body(resp):
        chunks = []
        async for chunk in resp.body_iterator:
            chunks.append(chunk)
        return b"".join(chunks)

    content = asyncio.run(gather_body(response))
    lines = content.decode("utf-8").strip().splitlines()

    assert lines[0] == "date,realized,unrealized"
    assert lines[1:] == [
        "2024-04-01,,",
    ]

    db.dispose_engine()


def test_export_trades_returns_csv(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))

    _app = create_app()

    with db.SessionLocal() as session:
        session.add_all(
            [
                Trade(
                    date="2024-05-01",
                    symbol="AAPL",
                    action="BUY",
                    qty=2.0,
                    price=100.0,
                    amount=-200.0,
                ),
                Trade(
                    date="2024-05-02",
                    symbol="TSLA",
                    action="SELL",
                    qty=1.5,
                    price=50.25,
                    amount=75.375,
                ),
                NoteDaily(
                    date="2024-05-01",
                    note="Strong earnings",
                    is_markdown=False,
                    updated_at="2024-05-01T08:30:00Z",
                ),
            ]
        )
        session.commit()

    request = SimpleNamespace(app=_app)

    with db.SessionLocal() as session:
        response = export_data(
            request=request,
            start="2024-05-01",
            end="2024-05-02",
            dataset="trades",
            db=session,
        )

    async def gather_body(resp):
        chunks = []
        async for chunk in resp.body_iterator:
            chunks.append(chunk)
        return b"".join(chunks)

    content = asyncio.run(gather_body(response))
    lines = content.decode("utf-8").strip().splitlines()

    assert lines[0] == "date,symbol,action,qty,price,amount,notes"
    assert lines[1:] == [
        "2024-05-01,AAPL,BUY,2,100,-200,Strong earnings",
        "2024-05-02,TSLA,SELL,1.5,50.25,75.375,",
    ]

    db.dispose_engine()
