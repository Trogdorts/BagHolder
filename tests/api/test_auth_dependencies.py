import sys
from pathlib import Path

import pytest
from fastapi import HTTPException
from starlette.requests import Request

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.core import database as db  # noqa: E402
from app.core.authentication import AuthContext, get_auth_context, require_user  # noqa: E402
from app.main import create_app  # noqa: E402
from app.services.identity import IdentityService  # noqa: E402


def _build_request(app, *, path: str = "/calendar", method: str = "GET", headers=None) -> Request:
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
    return request


@pytest.fixture
def app(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    application = create_app()
    try:
        yield application
    finally:
        db.dispose_engine()


def test_get_auth_context_resolves_user(app):
    with db.SessionLocal() as session:
        identity = IdentityService(session)
        result = identity.register(
            username="admin",
            password="password123",
            confirm_password="password123",
        )
        assert result.success and result.user is not None
        user_id = result.user.id

    with db.SessionLocal() as session:
        request = _build_request(app)
        request.session["user_id"] = user_id
        context = get_auth_context(request, db=session)
        assert context.user is not None
        assert context.user.id == user_id
        assert context.needs_setup is False
        assert request.session.get("user_id") == user_id


def test_get_auth_context_clears_stale_session(app):
    with db.SessionLocal() as session:
        # Ensure at least one user exists so allow_self_registration returns False
        identity = IdentityService(session)
        identity.register(
            username="existing",
            password="password123",
            confirm_password="password123",
        )

    with db.SessionLocal() as session:
        request = _build_request(app)
        request.session["user_id"] = 9999
        context = get_auth_context(request, db=session)
        assert context.user is None
        assert request.session.get("user_id") is None
        assert context.needs_setup is False


def test_require_user_redirects_to_setup(app):
    request = _build_request(app, path="/calendar")
    with pytest.raises(HTTPException) as exc_info:
        require_user(request, context=AuthContext(user=None, needs_setup=True))
    response = exc_info.value
    assert getattr(response, "status_code", None) in {302, 303}
    assert response.headers["Location"] == "/setup"


def test_require_user_returns_unauthorized_for_api(app):
    headers = [(b"accept", b"application/json")]
    request = _build_request(app, path="/api/data", headers=headers, method="POST")
    with pytest.raises(HTTPException) as exc_info:
        require_user(request, context=AuthContext(user=None, needs_setup=False))
    response = exc_info.value
    assert response.status_code == 401


def test_require_user_redirects_to_login_for_html(app):
    headers = [(b"accept", b"text/html")] 
    request = _build_request(app, path="/calendar", headers=headers)
    with pytest.raises(HTTPException) as exc_info:
        require_user(request, context=AuthContext(user=None, needs_setup=False))
    response = exc_info.value
    assert getattr(response, "status_code", None) in {302, 303}
    assert response.headers["Location"] == "/login"
