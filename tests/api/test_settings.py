import asyncio
import io
import json
import sys
from pathlib import Path
from unittest.mock import patch

import shutil
import zipfile
import yaml
from starlette.requests import Request
from types import SimpleNamespace

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.main import create_app  # noqa: E402
from app.api.routes_settings import (
    create_new_account,
    shutdown_application,
    export_settings_config,
    import_settings_config,
    export_full_backup,
    import_full_backup,
    export_debug_logs,
    rename_existing_account,
    save_settings,
    switch_active_account,
    update_account_password,
)  # noqa: E402
from app.core.auth import hash_password, verify_password  # noqa: E402
from app.core.models import User  # noqa: E402
from app.core import database as db  # noqa: E402
from app.services.data_backup import create_backup_archive  # noqa: E402


def _build_request(app, method: str = "POST"):
    request = Request({"type": "http", "app": app, "method": method, "path": "/", "headers": []})
    request.state.user = SimpleNamespace(id=0, username="admin", is_admin=True)
    return request


class DummyUploadFile:
    def __init__(self, filename: str, data: bytes):
        self.filename = filename
        self._data = data

    async def read(self) -> bytes:
        return self._data

    async def close(self) -> None:
        return None


def test_shutdown_application_schedules_process_signal(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()
    request = _build_request(app)

    with patch("app.api.routes_settings.os.kill") as mock_kill:
        response = shutdown_application(request)
        assert response.context["shutting_down"] is True
        assert response.context["cleared"] is False
        assert response.background is not None
        asyncio.run(response.background())
    
    mock_kill.assert_called_once()


def test_shutdown_application_rejects_non_admin(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()
    request = _build_request(app)
    request.state.user = SimpleNamespace(id=2, username="user", is_admin=False)

    response = shutdown_application(request)

    assert response.status_code == 303
    assert response.headers["location"] == "/settings?user_error=forbidden"


def test_export_settings_config_returns_current_state(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    request = _build_request(app, method="GET")
    response = export_settings_config(request)

    assert response.status_code == 200
    assert response.headers["content-disposition"].startswith("attachment; filename=bagholder-config.json")

    payload = json.loads(response.body.decode("utf-8"))
    assert payload["ui"]["theme"] == "dark"


def test_import_settings_config_updates_state_and_persists(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    export_request = _build_request(app, method="GET")
    export_response = export_settings_config(export_request)
    new_config = json.loads(export_response.body.decode("utf-8"))
    new_config["ui"]["theme"] = "light"
    new_config["ui"]["show_trade_count"] = False
    new_config["ui"]["show_percentages"] = False

    upload = DummyUploadFile("config.json", json.dumps(new_config).encode("utf-8"))
    request = _build_request(app)
    response = asyncio.run(import_settings_config(request, config_file=upload))

    assert response.status_code == 303
    assert response.headers["location"] == "/settings?config_imported=1"
    assert app.state.config.raw["ui"]["theme"] == "light"
    assert app.state.templates.env.globals["cfg"]["ui"]["theme"] == "light"
    assert app.state.config.raw["ui"]["show_trade_count"] is False
    assert app.state.templates.env.globals["cfg"]["ui"]["show_trade_count"] is False
    assert app.state.config.raw["ui"]["show_percentages"] is False
    assert app.state.templates.env.globals["cfg"]["ui"]["show_percentages"] is False

    cfg_path = Path(app.state.config.path)
    with cfg_path.open("r", encoding="utf-8") as handle:
        contents = yaml.safe_load(handle) or {}

    assert contents["ui"]["theme"] == "light"
    assert contents["ui"]["show_trade_count"] is False
    assert contents["ui"]["show_percentages"] is False


def test_import_settings_config_rejects_non_admin(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    new_config = app.state.config.as_dict()
    upload = DummyUploadFile("config.json", json.dumps(new_config).encode("utf-8"))
    request = _build_request(app)
    request.state.user = SimpleNamespace(id=3, username="member", is_admin=False)

    response = asyncio.run(import_settings_config(request, config_file=upload))

    assert response.status_code == 303
    assert response.headers["location"] == "/settings?config_error=forbidden"


def test_import_settings_config_with_invalid_json_sets_error(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    upload = DummyUploadFile("config.json", b"not-json")
    request = _build_request(app)
    response = asyncio.run(import_settings_config(request, config_file=upload))

    assert response.status_code == 303
    assert response.headers["location"].startswith("/settings?config_error=")
    assert app.state.config.raw["ui"]["theme"] == "dark"


def test_create_new_account_switches_active_and_creates_directory(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    request = _build_request(app)
    response = create_new_account(request, account_name="Swing", redirect_to="/settings")

    assert response.status_code == 303
    assert response.headers["location"] == "/settings?account_status=created"
    assert app.state.active_account.name == "Swing"
    account_path = Path(app.state.account_data_dir)
    assert account_path.exists()
    assert account_path.parent.name == "accounts"


def test_rename_existing_account_updates_name(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()
    request = _build_request(app)
    create_new_account(request, account_name="Growth", redirect_to="/settings")
    account_id = app.state.active_account.id

    response = rename_existing_account(
        request,
        account_id=account_id,
        account_name="Growth Fund",
        redirect_to="/settings",
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/settings?account_status=renamed"
    assert app.state.active_account.name == "Growth Fund"

    error_response = rename_existing_account(
        request,
        account_id=account_id,
        account_name="   ",
        redirect_to="/settings",
    )

    assert error_response.status_code == 303
    assert error_response.headers["location"] == "/settings?account_error=empty_name"


def test_switch_active_account_uses_redirect_target(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()
    request = _build_request(app)
    create_new_account(request, account_name="Short", redirect_to="/settings")
    secondary_id = app.state.active_account.id

    response = switch_active_account(request, account_id="primary", redirect_to="/calendar")

    assert response.status_code == 303
    assert response.headers["location"] == "/calendar"
    assert app.state.active_account.id == "primary"

    # Switching back to the secondary account should return to settings and flag the success message.
    response_back = switch_active_account(request, account_id=secondary_id, redirect_to="/settings")

    assert response_back.status_code == 303
    assert response_back.headers["location"] == "/settings?account_status=switched"
    assert app.state.active_account.id == secondary_id


def test_export_full_backup_includes_database_and_config(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    # Ensure there is an extra file to confirm arbitrary data is included.
    extra_file = Path(app.state.config.path).parent / "notes-cache.txt"
    extra_file.write_text("notes", encoding="utf-8")

    request = _build_request(app, method="GET")
    response = export_full_backup(request)

    assert response.status_code == 200
    assert response.headers["content-disposition"].startswith("attachment; filename=bagholder-backup-")

    with zipfile.ZipFile(io.BytesIO(response.body)) as archive:
        names = set(archive.namelist())

    assert "config.yaml" in names
    assert "profitloss.db" in names
    assert "notes-cache.txt" in names


def test_export_debug_logs_downloads_file(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    request = _build_request(app, method="GET")
    log_path = Path(app.state.log_path)
    log_path.write_text("diagnostic entry\n", encoding="utf-8")

    response = export_debug_logs(request)

    assert response.status_code == 200
    assert response.headers["content-disposition"].startswith("attachment; filename=bagholder-debug-")
    assert b"diagnostic entry" in response.body


def test_save_settings_updates_listening_port(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()
    request = _build_request(app)

    session = db.SessionLocal()
    try:
        response = save_settings(
            request,
            db=session,
            theme="dark",
            show_text="true",
            show_trade_count="true",
            show_percentages="true",
            show_weekends="false",
            default_view="latest",
            listening_port="8123",
            debug_logging="false",
            icon_color="#6b7280",
            primary_color="#2563eb",
            primary_hover_color="#1d4ed8",
            success_color="#22c55e",
            warning_color="#f59e0b",
            danger_color="#dc2626",
            danger_hover_color="#b91c1c",
            trade_badge_color="#34d399",
            trade_badge_text_color="#111827",
            note_icon_color="#80cbc4",
            export_empty_values="zero",
            pnl_method="fifo",
        )
    finally:
        session.close()

    assert response.status_code == 303
    assert response.headers["location"] == "/settings"
    assert app.state.config.raw["server"]["port"] == 8123

    cfg_path = Path(app.state.config.path)
    with cfg_path.open("r", encoding="utf-8") as handle:
        persisted = yaml.safe_load(handle) or {}

    assert persisted["server"]["port"] == 8123


def test_save_settings_invalid_port_preserves_existing_value(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()
    request = _build_request(app)

    original_port = app.state.config.raw["server"]["port"]
    session = db.SessionLocal()
    try:
        response = save_settings(
            request,
            db=session,
            theme="dark",
            show_text="true",
            show_trade_count="true",
            show_percentages="true",
            show_weekends="false",
            default_view="latest",
            listening_port="70000",
            debug_logging="false",
            icon_color="#6b7280",
            primary_color="#2563eb",
            primary_hover_color="#1d4ed8",
            success_color="#22c55e",
            warning_color="#f59e0b",
            danger_color="#dc2626",
            danger_hover_color="#b91c1c",
            trade_badge_color="#34d399",
            trade_badge_text_color="#111827",
            note_icon_color="#80cbc4",
            export_empty_values="zero",
            pnl_method="fifo",
        )
    finally:
        session.close()

    assert response.status_code == 303
    assert app.state.config.raw["server"]["port"] == original_port

    cfg_path = Path(app.state.config.path)
    with cfg_path.open("r", encoding="utf-8") as handle:
        persisted = yaml.safe_load(handle) or {}

    assert persisted["server"]["port"] == original_port


def test_import_full_backup_replaces_data_and_reloads_state(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    existing_dir = Path(app.state.config.path).parent
    old_file = existing_dir / "legacy.txt"
    old_file.write_text("old", encoding="utf-8")

    backup_source = tmp_path / "backup_src"
    backup_source.mkdir()

    config_data = app.state.config.as_dict()
    config_data["ui"]["theme"] = "light"
    with (backup_source / "config.yaml").open("w", encoding="utf-8") as handle:
        yaml.safe_dump(config_data, handle, sort_keys=False)

    shutil.copy2(existing_dir / "profitloss.db", backup_source / "profitloss.db")
    extra_path = backup_source / "extra.txt"
    extra_path.write_text("extra", encoding="utf-8")

    archive_bytes = create_backup_archive(str(backup_source))

    upload = DummyUploadFile("backup.zip", archive_bytes)
    request = _build_request(app)
    response = asyncio.run(import_full_backup(request, backup_file=upload))

    assert response.status_code == 303
    assert response.headers["location"] == "/settings?backup_restored=1"

    new_dir = Path(app.state.config.path).parent
    assert not (new_dir / "legacy.txt").exists()
    assert (new_dir / "extra.txt").read_text(encoding="utf-8") == "extra"
    assert app.state.config.raw["ui"]["theme"] == "light"
    assert app.state.templates.env.globals["cfg"]["ui"]["theme"] == "light"


def test_import_full_backup_rejects_non_admin(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    archive_bytes = create_backup_archive(Path(app.state.config.path).parent)
    upload = DummyUploadFile("backup.zip", archive_bytes)
    request = _build_request(app)
    request.state.user = SimpleNamespace(id=4, username="member", is_admin=False)

    response = asyncio.run(import_full_backup(request, backup_file=upload))

    assert response.status_code == 303
    assert response.headers["location"] == "/settings?backup_error=forbidden"


def test_update_account_password_changes_credentials(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    with db.SessionLocal() as session:
        salt, password_hash = hash_password("password123")
        user = User(username="admin", password_salt=salt, password_hash=password_hash)
        session.add(user)
        session.commit()
        session.refresh(user)

    request = _build_request(app)
    request.state.user = user

    with db.SessionLocal() as session:
        response = update_account_password(
            request,
            current_password="password123",
            new_password="newpassword456",
            confirm_password="newpassword456",
            redirect_to="/settings",
            db=session,
        )

    assert response.status_code == 303
    assert response.headers["location"] == "/settings?password_status=updated"

    with db.SessionLocal() as session:
        updated = session.get(User, user.id)
        assert updated is not None
        assert verify_password("newpassword456", updated.password_salt, updated.password_hash)

    db.dispose_engine()


def test_update_account_password_rejects_invalid_current(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    with db.SessionLocal() as session:
        salt, password_hash = hash_password("password123")
        user = User(username="admin", password_salt=salt, password_hash=password_hash)
        session.add(user)
        session.commit()
        session.refresh(user)

    request = _build_request(app)
    request.state.user = user

    with db.SessionLocal() as session:
        response = update_account_password(
            request,
            current_password="wrongpass",
            new_password="newpassword456",
            confirm_password="newpassword456",
            redirect_to="/settings",
            db=session,
        )

    assert response.status_code == 303
    assert response.headers["location"] == "/settings?password_error=invalid_current"

    with db.SessionLocal() as session:
        persisted = session.get(User, user.id)
        assert persisted is not None
        assert verify_password("password123", persisted.password_salt, persisted.password_hash)

    db.dispose_engine()
