import os

from app.core import database
from app.core.seed import ensure_seed
from app.core.database import init_db
from app.core.models import DailySummary, Meta, Trade
from app.services import data_reset
from app.services.data_reset import clear_all_data


def test_clear_all_data_resets_database(tmp_path):
    data_dir = tmp_path / "data"
    os.makedirs(data_dir, exist_ok=True)
    db_path = data_dir / "profitloss.db"

    ensure_seed(str(db_path))
    _, Session = init_db(str(db_path))

    with Session() as session:
        session.add(
            Trade(
                date="2024-01-01",
                symbol="AAPL",
                action="BUY",
                qty=1,
                price=100.0,
                amount=100.0,
            )
        )
        session.add(
            DailySummary(
                date="2024-01-01",
                realized=10.0,
                unrealized=5.0,
                total_invested=5.0,
                updated_at="now",
            )
        )
        session.commit()

        assert session.query(Trade).count() == 1
        assert session.query(DailySummary).count() == 1

    clear_all_data(str(data_dir))

    _, Session = init_db(str(db_path))
    with Session() as session:
        assert session.query(Trade).count() == 0
        assert session.query(DailySummary).count() == 0
        assert session.get(Meta, "schema_version") is not None
        assert session.get(Meta, "last_viewed_month") is not None


def test_clear_all_data_disposes_engine(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    os.makedirs(data_dir, exist_ok=True)
    db_path = data_dir / "profitloss.db"

    ensure_seed(str(db_path))
    init_db(str(db_path))

    assert database._engine is not None

    original_remove = data_reset.os.remove

    def checking_remove(path):
        # Engine should be disposed before the database file is deleted so
        # Windows can release its file handle.
        assert database._engine is None
        original_remove(path)

    monkeypatch.setattr(data_reset.os, "remove", checking_remove)

    clear_all_data(str(data_dir))

    # init_db is called again after clearing, so the engine should be
    # re-initialized for continued use.
    assert database._engine is not None
