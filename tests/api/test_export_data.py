import asyncio
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.main import create_app  # noqa: E402
from app.core import database as db  # noqa: E402
from app.core.models import DailySummary  # noqa: E402
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

    with db.SessionLocal() as session:
        response = export_data(
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
