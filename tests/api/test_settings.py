import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import patch

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
)  # noqa: E402


def _build_request(app, method: str = "POST"):
    return Request({"type": "http", "app": app, "method": method, "headers": []})


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

    request = _build_request(app)
    response = import_settings_config(request, config_json=json.dumps(new_config))

    assert response.status_code == 303
    assert response.headers["location"] == "/settings?config_imported=1"
    assert app.state.config.raw["ui"]["theme"] == "light"
    assert app.state.templates.env.globals["cfg"]["ui"]["theme"] == "light"

    cfg_path = Path(app.state.config.path)
    with cfg_path.open("r", encoding="utf-8") as handle:
        contents = yaml.safe_load(handle) or {}

    assert contents["ui"]["theme"] == "light"


def test_import_settings_config_with_invalid_json_sets_error(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    request = _build_request(app)
    response = import_settings_config(request, config_json="not-json")

    assert response.status_code == 303
    assert response.headers["location"].startswith("/settings?config_error=")
    assert app.state.config.raw["ui"]["theme"] == "dark"
