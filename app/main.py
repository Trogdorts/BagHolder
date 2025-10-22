import os
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

if __package__ in {None, ""}:
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.core.lifecycle import reload_application_state

def create_app():
    app = FastAPI(title="BagHolder")
    app.mount(
        "/static",
        StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")),
        name="static",
    )

    reload_application_state(app)

    from app.api.routes_import import router as import_router
    from app.api.routes_calendar import router as calendar_router
    from app.api.routes_settings import router as settings_router
    from app.api.routes_notes import router as notes_router
    from app.api.routes_stats import router as stats_router
    from app.api.routes_dev import router as dev_router

    app.include_router(calendar_router)
    app.include_router(import_router)
    app.include_router(settings_router)
    app.include_router(notes_router)
    app.include_router(stats_router)
    app.include_router(dev_router)

    return app

app = create_app()
