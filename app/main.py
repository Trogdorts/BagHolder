import os
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from starlette.responses import JSONResponse, RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.types import ASGIApp

if __package__ in {None, ""}:
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.core.lifecycle import reload_application_state
from app.core import database
from app.core.session import SignedCookieSessionMiddleware
from app.services.identity import IdentityService


class LoginRequiredMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint):
        path = request.url.path

        if path.startswith("/static"):
            return await call_next(request)

        request.state.user = None

        session_data = request.scope.get("session")
        if not isinstance(session_data, dict):
            session_data = {}
            request.scope["session"] = session_data

        session_user_id = session_data.get("user_id")

        if session_user_id is not None and database.SessionLocal is not None:
            with database.SessionLocal() as db_session:
                identity = IdentityService(db_session)
                user = identity.get_user_by_id(session_user_id)
                if user is not None:
                    request.state.user = user
                else:
                    session_data.pop("user_id", None)

        public_paths = {"/login", "/login/register"}
        is_public = (
            path in public_paths
            or path.startswith("/docs")
            or path.startswith("/openapi")
            or path.startswith("/static")
        )

        if request.state.user is None and not is_public:
            if _is_api_like_request(request, path):
                return JSONResponse({"detail": "Authentication required."}, status_code=401)

            status_code = 303 if request.method.upper() != "GET" else 302
            return RedirectResponse(url="/login", status_code=status_code)

        response = await call_next(request)
        return response


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


def create_app():
    app = FastAPI(title="BagHolder")
    app.mount(
        "/static",
        StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")),
        name="static",
    )

    secret_key = os.environ.get("BAGHOLDER_SECRET_KEY", "bagholder-dev-secret")
    app.add_middleware(SignedCookieSessionMiddleware, secret_key=secret_key)
    app.add_middleware(LoginRequiredMiddleware)

    reload_application_state(app)

    from app.api.routes_import import router as import_router
    from app.api.routes_calendar import router as calendar_router
    from app.api.routes_settings import router as settings_router
    from app.api.routes_notes import router as notes_router
    from app.api.routes_stats import router as stats_router
    from app.api.routes_dev import router as dev_router
    from app.api.routes_auth import router as auth_router

    app.include_router(calendar_router)
    app.include_router(import_router)
    app.include_router(settings_router)
    app.include_router(notes_router)
    app.include_router(stats_router)
    app.include_router(dev_router)
    app.include_router(auth_router)

    return app


app = create_app()
