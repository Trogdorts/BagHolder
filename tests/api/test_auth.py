import sys
from pathlib import Path

from starlette.requests import Request

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.main import create_app  # noqa: E402
from app.main import _is_api_like_request  # noqa: E402
from app.api.routes_auth import (  # noqa: E402
    login_action,
    login_form,
    logout_action,
    register_action,
)
from app.core import database as db  # noqa: E402
from app.core.models import User  # noqa: E402


def _build_request(
    app,
    method: str = "GET",
    path: str = "/login",
    *,
    headers: list[tuple[bytes, bytes]] | None = None,
) -> Request:
    scope = {
        "type": "http",
        "http_version": "1.1",
        "asgi": {"version": "3.0", "spec_version": "2.1"},
        "method": method,
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "headers": headers or [],
        "client": ("test", 1234),
        "server": ("testserver", 80),
        "app": app,
        "session": {},
    }
    request = Request(scope)
    request.state.user = None
    return request


def test_register_creates_initial_user_and_logs_in(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    with db.SessionLocal() as session:
        request = _build_request(app, method="POST")
        response = register_action(
            request,
            username="Admin",
            password="supersecret",
            confirm_password="supersecret",
            db=session,
        )
        assert response.status_code == 303
        assert response.headers["location"] == "/"
        assert request.session.get("user_id") is not None

    with db.SessionLocal() as session:
        users = session.query(User).all()
        assert len(users) == 1
        assert users[0].username == "admin"
        assert users[0].is_admin is True

    db.dispose_engine()


def test_login_with_existing_user_sets_session(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    with db.SessionLocal() as session:
        request = _build_request(app, method="POST")
        register_action(
            request,
            username="trader",
            password="password123",
            confirm_password="password123",
            db=session,
        )

    with db.SessionLocal() as session:
        request = _build_request(app, method="POST")
        response = login_action(request, username="TRADER", password="password123", db=session)
        assert response.status_code == 303
        assert response.headers["location"] == "/"
        assert request.session.get("user_id") is not None

    with db.SessionLocal() as session:
        request = _build_request(app, method="POST")
        response = login_action(request, username="trader", password="wrong", db=session)
        assert response.status_code == 401
        assert response.context["login_error"] == "Invalid username or password."

    db.dispose_engine()


def test_api_request_detection(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    api_request = _build_request(
        app,
        method="POST",
        path="/api/ui/preferences",
        headers=[(b"content-type", b"application/json"), (b"accept", b"*/*")],
    )
    assert _is_api_like_request(api_request, "/api/ui/preferences") is True

    fetch_request = _build_request(
        app,
        method="POST",
        path="/dev/reload",
        headers=[(b"accept", b"*/*")],
    )
    assert _is_api_like_request(fetch_request, "/dev/reload") is True

    form_request = _build_request(
        app,
        method="POST",
        path="/settings",
        headers=[
            (
                b"accept",
                b"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,*/*;q=0.8",
            ),
            (b"content-type", b"application/x-www-form-urlencoded"),
        ],
    )
    assert _is_api_like_request(form_request, "/settings") is False

    get_request = _build_request(
        app,
        method="GET",
        path="/calendar/2024/1",
        headers=[
            (
                b"accept",
                b"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,*/*;q=0.8",
            )
        ],
    )
    assert _is_api_like_request(get_request, "/calendar/2024/1") is False

    db.dispose_engine()


def test_login_form_redirects_when_authenticated(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    with db.SessionLocal() as session:
        request = _build_request(app)
        register_action(
            request,
            username="admin",
            password="password123",
            confirm_password="password123",
            db=session,
        )

    with db.SessionLocal() as session:
        user = session.query(User).first()
        request = _build_request(app)
        request.state.user = user
        response = login_form(request, db=session)
        assert response.status_code == 303
        assert response.headers["location"] == "/"

    with db.SessionLocal() as session:
        request = _build_request(app)
        response = login_form(request, db=session)
        assert response.status_code == 200
        assert response.context["allow_registration"] is False

    db.dispose_engine()


def test_registration_disabled_after_first_user(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    with db.SessionLocal() as session:
        request = _build_request(app, method="POST")
        register_action(
            request,
            username="first",
            password="password123",
            confirm_password="password123",
            db=session,
        )

    with db.SessionLocal() as session:
        request = _build_request(app, method="POST")
        response = register_action(
            request,
            username="second",
            password="password123",
            confirm_password="password123",
            db=session,
        )
        assert response.status_code == 403
        assert response.context["registration_error"] == "Registration is disabled once an account exists."

    db.dispose_engine()


def test_logout_clears_session(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    app = create_app()

    request = _build_request(app, method="POST", path="/logout")
    request.session["user_id"] = 5
    response = logout_action(request)

    assert response.status_code == 303
    assert response.headers["location"] == "/login"
    assert "user_id" not in request.session
