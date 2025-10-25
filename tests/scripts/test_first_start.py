from pathlib import Path

import pytest

from app.core.bootstrap import BootstrapError, bootstrap_admin
from app.core.config import AppConfig
from app.core.database import dispose_engine, init_db
from app.core.models import User
from app.services.accounts import prepare_accounts


def _get_db_path(data_dir: Path) -> Path:
    cfg = AppConfig.load(str(data_dir))
    _, active = prepare_accounts(cfg, str(data_dir))
    return Path(active.path) / "profitloss.db"


def test_bootstrap_admin_creates_user(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    user = bootstrap_admin("AdminUser", "password123", data_dir=str(data_dir))
    assert user.username == "adminuser"
    assert user.is_admin is True

    db_path = _get_db_path(data_dir)
    _, session_factory = init_db(str(db_path))
    try:
        with session_factory() as session:
            db_user = session.query(User).one()
            assert db_user.username == "adminuser"
            assert db_user.is_admin is True
    finally:
        dispose_engine()


def test_bootstrap_admin_requires_empty_database(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    bootstrap_admin("first", "password123", data_dir=str(data_dir))

    with pytest.raises(BootstrapError):
        bootstrap_admin("second", "password123", data_dir=str(data_dir))
