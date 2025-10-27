import logging
import os
import secrets

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.core.bootstrap import maybe_bootstrap_admin_from_env
from app.core.lifecycle import reload_application_state
from app.core.session import SignedCookieSessionMiddleware
from app.core.utils import coerce_bool
from app.version import __version__


log = logging.getLogger(__name__)


def create_app():
    app = FastAPI(title="BagHolder")
    app.mount(
        "/static",
        StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")),
        name="static",
    )

    cfg = reload_application_state(app)
    if cfg.path:
        data_dir = os.path.dirname(cfg.path)
    else:
        data_dir = None
    maybe_bootstrap_admin_from_env(data_dir=data_dir)
    debug_logging_enabled = getattr(app.state, "debug_logging_enabled", False)
    secret_key = os.environ.get("BAGHOLDER_SECRET_KEY")
    if not secret_key:
        secret_key = secrets.token_urlsafe(32)
        log.warning(
            "BAGHOLDER_SECRET_KEY is not set; generated an ephemeral session secret. "
            "Set BAGHOLDER_SECRET_KEY to persist sessions across restarts."
        )

    secure_env = os.environ.get("BAGHOLDER_SESSION_SECURE")
    if secure_env is not None:
        https_only = coerce_bool(secure_env, False)
    else:
        https_only = False
        log.warning(
            "Session cookies are not marked Secure. Set BAGHOLDER_SESSION_SECURE=1 "
            "when deploying behind HTTPS."
        )

    max_age_env = os.environ.get("BAGHOLDER_SESSION_MAX_AGE")
    session_max_age = None
    if max_age_env:
        try:
            session_max_age = int(max_age_env)
        except ValueError:
            session_max_age = None

    samesite_env = (os.environ.get("BAGHOLDER_SESSION_SAMESITE") or "lax").lower()
    allowed_samesite = {"lax", "strict", "none"}
    samesite = samesite_env if samesite_env in allowed_samesite else "lax"
    if samesite == "none" and not https_only:
        https_only = True

    app.add_middleware(
        SignedCookieSessionMiddleware,
        secret_key=secret_key,
        https_only=https_only,
        max_age=session_max_age,
        samesite=samesite,
    )
    app.state.version = __version__
    app.version = __version__

    from app.api.routes_import import router as import_router
    from app.api.routes_calendar import router as calendar_router
    from app.api.routes_settings import router as settings_router
    from app.api.routes_notes import router as notes_router
    from app.api.routes_stats import router as stats_router
    from app.api.routes_dev import router as dev_router
    from app.api.routes_auth import router as auth_router
    from app.api.routes_setup import router as setup_router

    app.include_router(setup_router)
    app.include_router(calendar_router)
    app.include_router(import_router)
    app.include_router(settings_router)
    app.include_router(notes_router)
    app.include_router(stats_router)
    app.include_router(dev_router)
    app.include_router(auth_router)

    return app


app = create_app()
