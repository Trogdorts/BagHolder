import asyncio
import sys
from pathlib import Path
from unittest.mock import patch

from starlette.requests import Request

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.main import create_app  # noqa: E402
from app.api.routes_settings import shutdown_application  # noqa: E402


def _build_request(app):
    return Request({"type": "http", "app": app, "method": "POST", "headers": []})


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
