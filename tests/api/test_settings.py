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

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.main import create_app  # noqa: E402
from app.api.routes_settings import (
    shutdown_application,
    export_settings_config,
    import_settings_config,
    export_full_backup,
    import_full_backup,
)  # noqa: E402
from app.services.data_backup import create_backup_archive  # noqa: E402


def _build_request(app, method: str = "POST"):
    return Request({"type": "http", "app": app, "method": method, "headers": []})


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

    upload = DummyUploadFile("config.json", json.dumps(new_config).encode("utf-8"))
    request = _build_request(app)
    response = asyncio.run(import_settings_config(request, config_file=upload))

    assert response.status_code == 303
    assert response.headers["location"] == "/settings?config_imported=1"
    assert app.state.config.raw["ui"]["theme"] == "light"
    assert app.state.templates.env.globals["cfg"]["ui"]["theme"] == "light"
    assert app.state.config.raw["ui"]["show_trade_count"] is False
    assert app.state.templates.env.globals["cfg"]["ui"]["show_trade_count"] is False

    cfg_path = Path(app.state.config.path)
    with cfg_path.open("r", encoding="utf-8") as handle:
        contents = yaml.safe_load(handle) or {}

    assert contents["ui"]["theme"] == "light"
    assert contents["ui"]["show_trade_count"] is False


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
