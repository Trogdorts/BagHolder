from pathlib import Path

from app.core.bootstrap import bootstrap_admin, maybe_bootstrap_admin_from_env
from app.core.config import AppConfig
from app.core.database import dispose_engine, init_db
from app.core.models import User
from app.services.accounts import prepare_accounts


def _db_path(data_dir: Path) -> Path:
    cfg = AppConfig.load(str(data_dir))
    _, active = prepare_accounts(cfg, str(data_dir))
    return Path(active.path) / "profitloss.db"


def _list_users(data_dir: Path) -> list[str]:
    db_path = _db_path(data_dir)
    if not db_path.exists():
        return []
    _, session_factory = init_db(str(db_path))
    try:
        with session_factory() as session:
            return [user.username for user in session.query(User).all()]
    finally:
        dispose_engine()


def test_maybe_bootstrap_creates_user(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("BAGHOLDER_BOOTSTRAP_USERNAME", "AdminUser")
    monkeypatch.setenv("BAGHOLDER_BOOTSTRAP_PASSWORD", "password123")

    created = maybe_bootstrap_admin_from_env(data_dir=str(data_dir))
    assert created is True
    assert _list_users(data_dir) == ["adminuser"]


def test_maybe_bootstrap_noop_when_user_exists(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    bootstrap_admin("first", "password123", data_dir=str(data_dir))

    monkeypatch.setenv("BAGHOLDER_BOOTSTRAP_USERNAME", "second")
    monkeypatch.setenv("BAGHOLDER_BOOTSTRAP_PASSWORD", "password123")

    created = maybe_bootstrap_admin_from_env(data_dir=str(data_dir))
    assert created is False
    assert _list_users(data_dir) == ["first"]


def test_maybe_bootstrap_requires_both_env(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("BAGHOLDER_BOOTSTRAP_USERNAME", "onlyuser")
    monkeypatch.delenv("BAGHOLDER_BOOTSTRAP_PASSWORD", raising=False)

    created = maybe_bootstrap_admin_from_env(data_dir=str(data_dir))
    assert created is False
    assert _list_users(data_dir) == []
