"""Authentication helpers and request guards."""

from __future__ import annotations

from dataclasses import dataclass
from typing import MutableMapping, Optional

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.core.database import get_session
from app.core.models import User
from app.services.identity import IdentityService


@dataclass(slots=True)
class AuthContext:
    """Represents authentication details resolved for a request."""

    user: Optional[User]
    needs_setup: bool


def _ensure_session(request: Request) -> MutableMapping[str, object]:
    session = request.scope.get("session")
    if not isinstance(session, MutableMapping):
        session = {}
        request.scope["session"] = session
    return session


def get_auth_context(request: Request, db: Session = Depends(get_session)) -> AuthContext:
    """Return the authenticated user (if any) and setup status for ``request``."""

    session = _ensure_session(request)
    identity = IdentityService(db)

    user: Optional[User] = None
    session_user_id = session.get("user_id")
    resolved_user_id: int | None = None
    if isinstance(session_user_id, int):
        resolved_user_id = session_user_id
    else:
        try:
            resolved_user_id = int(session_user_id)
        except (TypeError, ValueError):
            resolved_user_id = None

    if resolved_user_id is not None:
        user = identity.get_user_by_id(resolved_user_id)
        if user is None:
            session.pop("user_id", None)
    elif session_user_id is not None:
        session.pop("user_id", None)

    needs_setup = identity.allow_self_registration()

    request.state.user = user
    context = AuthContext(user=user, needs_setup=needs_setup)
    request.state.auth_context = context
    return context


def require_user(
    request: Request,
    context: AuthContext = Depends(get_auth_context),
) -> User:
    """Ensure the incoming request is authenticated.

    Returns
    -------
    User
        The authenticated user.

    Raises
    ------
    HTTPException
        When the request is not authenticated. The response mirrors the
        behaviour of the legacy login middleware by redirecting HTML clients
        and returning ``401`` for API consumers.
    """

    if context.user is not None:
        return context.user

    path = request.url.path
    method = request.method.upper()

    if context.needs_setup and path != "/setup":
        status_code = 303 if method != "GET" else 302
        raise HTTPException(status_code=status_code, headers={"Location": "/setup"})

    if _is_api_like_request(request, path):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required.",
        )

    if path not in {"/login", "/login/register"}:
        status_code = 303 if method != "GET" else 302
        raise HTTPException(status_code=status_code, headers={"Location": "/login"})

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication required.",
    )


def _is_api_like_request(request: Request, path: str) -> bool:
    accept_header = (request.headers.get("accept") or "").lower()
    content_type = (request.headers.get("content-type") or "").lower()
    return any(
        [
            path.startswith("/api"),
            "application/json" in accept_header,
            "application/json" in content_type,
            request.headers.get("hx-request", "").lower() == "true",
            request.headers.get("x-requested-with", "").lower() == "xmlhttprequest",
            request.method.upper() != "GET" and "text/html" not in accept_header,
        ]
    )
