import os
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
import sys

if __package__ in {None, ""}:
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from app.core.config import AppConfig
from app.core.seed import ensure_seed
from app.core.database import init_db

def create_app():
    data_dir = os.environ.get("BAGHOLDER_DATA", "/app/data")
    os.makedirs(data_dir, exist_ok=True)
    cfg = AppConfig.load(data_dir)
    db_path = os.path.join(data_dir, "profitloss.db")
    ensure_seed(db_path)
    engine, SessionLocal = init_db(db_path)

    app = FastAPI(title="BagHolder")
    app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")
    templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

    def format_money(value):
        try:
            number = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return "$0.00"

        quantized = number.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        sign = "-" if quantized < 0 else ""
        return f"{sign}${abs(quantized):,.2f}"

    templates.env.filters["money"] = format_money
    templates.env.globals["cfg"] = cfg.raw
    app.state.templates = templates
    app.state.config = cfg

    from app.api.routes_import import router as import_router
    from app.api.routes_calendar import router as calendar_router
    from app.api.routes_settings import router as settings_router
    from app.api.routes_notes import router as notes_router
    from app.api.routes_stats import router as stats_router

    app.include_router(calendar_router)
    app.include_router(import_router)
    app.include_router(settings_router)
    app.include_router(notes_router)
    app.include_router(stats_router)

    return app

app = create_app()
