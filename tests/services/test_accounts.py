import os
from pathlib import Path

import pytest

from app.core.config import AppConfig
from app.core import database
from app.core.database import dispose_engine
from app.core.seed import ensure_seed
from app.services.accounts import (
    create_account,
    prepare_accounts,
    rename_account,
    set_active_account,
)
from app.services.identity import IdentityService


def test_prepare_accounts_initializes_default(tmp_path):
    base_dir = tmp_path / "data"
    cfg = AppConfig.load(str(base_dir))

    records, active = prepare_accounts(cfg, str(base_dir))

    assert records and active.id == "primary"
    assert cfg.raw["accounts"]["active"] == "primary"
    assert Path(active.path).exists()
    assert Path(active.path) == base_dir.resolve()


def test_create_account_adds_new_entry(tmp_path):
    base_dir = tmp_path / "data"
    cfg = AppConfig.load(str(base_dir))
    prepare_accounts(cfg, str(base_dir))

    record = create_account(cfg, str(base_dir), "Trading Fund")

    assert record.name == "Trading Fund"
    assert Path(record.path).exists()
    assert record.storage.startswith("accounts/")
    assert cfg.raw["accounts"]["active"] == record.id
    assert set(cfg.raw["accounts"]["entries"].keys()) == {"primary", record.id}


def test_rename_account_validates_input(tmp_path):
    base_dir = tmp_path / "data"
    cfg = AppConfig.load(str(base_dir))
    prepare_accounts(cfg, str(base_dir))
    record = create_account(cfg, str(base_dir), "Alt")

    rename_account(cfg, str(base_dir), record.id, "Renamed")
    assert cfg.raw["accounts"]["entries"][record.id]["name"] == "Renamed"

    with pytest.raises(ValueError):
        rename_account(cfg, str(base_dir), record.id, "   ")

    with pytest.raises(ValueError):
        rename_account(cfg, str(base_dir), "missing", "Name")


def test_set_active_account_switches_and_validates(tmp_path):
    base_dir = tmp_path / "data"
    cfg = AppConfig.load(str(base_dir))
    prepare_accounts(cfg, str(base_dir))
    record = create_account(cfg, str(base_dir), "Alt")

    set_active_account(cfg, str(base_dir), "primary")
    assert cfg.raw["accounts"]["active"] == "primary"

    with pytest.raises(ValueError):
        set_active_account(cfg, str(base_dir), "unknown")


def test_create_account_copies_existing_users(tmp_path):
    base_dir = tmp_path / "data"
    cfg = AppConfig.load(str(base_dir))
    _, active = prepare_accounts(cfg, str(base_dir))

    try:
        primary_db = os.path.join(active.path, "profitloss.db")
        ensure_seed(primary_db)

        with database.SessionLocal() as session:
            identity = IdentityService(session)
            result = identity.create_user(
                "existing",
                "password123",
                confirm_password="password123",
                is_admin=True,
            )
            assert result.success

        create_account(cfg, str(base_dir), "Secondary")

        with database.SessionLocal() as session:
            identity = IdentityService(session)
            users = identity.list_users()

        assert len(users) == 1
        assert users[0].username == "existing"
        assert users[0].is_admin is True
    finally:
        dispose_engine()
