import os
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from starlette.responses import RedirectResponse

if __package__ in {None, ""}:
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.core.lifecycle import reload_application_state
from app.core.models import User
from app.core import database
from app.core.session import SignedCookieSessionMiddleware


def create_app():
    app = FastAPI(title="BagHolder")
    app.mount(
        "/static",
        StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")),
        name="static",
    )

    secret_key = os.environ.get("BAGHOLDER_SECRET_KEY", "bagholder-dev-secret")
    app.add_middleware(SignedCookieSessionMiddleware, secret_key=secret_key)

    reload_application_state(app)

    @app.middleware("http")
    async def enforce_login(request: Request, call_next):
        path = request.url.path

        if path.startswith("/static"):
            return await call_next(request)

        request.state.user = None
        session_user_id = request.session.get("user_id") if hasattr(request, "session") else None

        if session_user_id is not None and database.SessionLocal is not None:
            with database.SessionLocal() as db_session:
                user = db_session.get(User, session_user_id)
                if user is not None:
                    request.state.user = user
                else:
                    request.session.pop("user_id", None)

        public_paths = {"/login", "/login/register"}
        is_public = (
            path in public_paths
            or path.startswith("/docs")
            or path.startswith("/openapi")
            or path.startswith("/static")
        )

        if request.state.user is None and not is_public:
            status_code = 303 if request.method.upper() != "GET" else 302
            return RedirectResponse(url="/login", status_code=status_code)

        response = await call_next(request)
        return response

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
