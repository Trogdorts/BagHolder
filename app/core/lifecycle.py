"""Utilities for refreshing the FastAPI application state at runtime."""

from __future__ import annotations

import os
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from fastapi import FastAPI
from fastapi.templating import Jinja2Templates

from app.core.config import AppConfig
from app.core.database import dispose_engine, init_db
from app.core.seed import ensure_seed


def _build_templates(cfg: AppConfig) -> Jinja2Templates:
    """Create a ``Jinja2Templates`` instance configured with helpers.

    Parameters
    ----------
    cfg:
        The freshly loaded application configuration. The object is used to
        expose configuration values to templates.
    """

    templates = Jinja2Templates(
        directory=os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")
    )

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
    return templates


def reload_application_state(app: FastAPI, data_dir: str | None = None) -> AppConfig:
    """Reload configuration, templates and database connections in-place.

    The function mirrors the setup performed during the initial application
    creation. It reloads ``config.yaml`` from disk, rebuilds the Jinja2
    environment, and recreates the SQLAlchemy engine so that code or data
    changes on disk are immediately reflected without restarting the server.
    """

    data_dir = data_dir or os.environ.get("BAGHOLDER_DATA", "/app/data")
    os.makedirs(data_dir, exist_ok=True)

    cfg = AppConfig.load(data_dir)
    db_path = os.path.join(data_dir, "profitloss.db")

    # Recreate the engine to ensure SQLite reloads the database file.
    dispose_engine()
    ensure_seed(db_path)
    init_db(db_path)

    templates = _build_templates(cfg)
    app.state.templates = templates
    app.state.config = cfg

    return cfg

