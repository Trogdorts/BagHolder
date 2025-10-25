"""Utilities for refreshing the FastAPI application state at runtime."""

from __future__ import annotations

import logging
import os
import re
from dataclasses import asdict
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from fastapi import FastAPI
from fastapi.templating import Jinja2Templates

from app.core.config import AppConfig
from app.core.database import dispose_engine, init_db
from app.core.logger import configure_logging
from app.core.seed import ensure_seed
from app.core.utils import coerce_bool
from app.services.accounts import prepare_accounts, serialize_accounts


_HEX_COLOR_PATTERN = re.compile(r"^#[0-9a-fA-F]{6}$")


log = logging.getLogger(__name__)


def _parse_hex_color(color: str) -> tuple[int, int, int] | None:
    if not isinstance(color, str):
        return None
    candidate = color.strip()
    if not _HEX_COLOR_PATTERN.fullmatch(candidate):
        return None
    return int(candidate[1:3], 16), int(candidate[3:5], 16), int(candidate[5:7], 16)


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

    def hex_to_rgb(value: str) -> str:
        components = _parse_hex_color(value)
        if components is None:
            return "0, 0, 0"
        return f"{components[0]}, {components[1]}, {components[2]}"

    def hex_to_rgba(value: str, alpha: float = 1.0) -> str:
        components = _parse_hex_color(value)
        alpha = max(0.0, min(1.0, alpha))
        if components is None:
            return f"rgba(0, 0, 0, {alpha:.2f})"
        return f"rgba({components[0]}, {components[1]}, {components[2]}, {alpha:.2f})"

    def mix_with_white(value: str, blend: float = 0.5) -> str:
        components = _parse_hex_color(value)
        blend = max(0.0, min(1.0, blend))
        if components is None:
            return "#ffffff"
        r = round((1 - blend) * components[0] + blend * 255)
        g = round((1 - blend) * components[1] + blend * 255)
        b = round((1 - blend) * components[2] + blend * 255)
        return f"#{r:02x}{g:02x}{b:02x}"

    def pick_contrast(value: str) -> str:
        components = _parse_hex_color(value)
        if components is None:
            return "#ffffff"
        r, g, b = components
        # Relative luminance approximation (sRGB)
        luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255
        return "#000000" if luminance > 0.55 else "#ffffff"

    templates.env.filters["hex_to_rgb"] = hex_to_rgb
    templates.env.filters["hex_to_rgba"] = hex_to_rgba
    templates.env.filters["mix_with_white"] = mix_with_white
    templates.env.filters["pick_contrast"] = pick_contrast
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
    accounts, active_account = prepare_accounts(cfg, data_dir)
    diagnostics_cfg = cfg.raw.get("diagnostics", {}) if isinstance(cfg.raw, dict) else {}
    debug_logging = coerce_bool(diagnostics_cfg.get("debug_logging"), False)
    log_path = configure_logging(
        data_dir,
        debug_enabled=debug_logging,
        max_bytes=diagnostics_cfg.get("log_max_bytes", 1_048_576),
        retention=diagnostics_cfg.get("log_retention", 5),
    )

    db_path = os.path.join(active_account.path, "profitloss.db")

    # Recreate the engine to ensure SQLite reloads the database file.
    dispose_engine()
    ensure_seed(db_path)
    init_db(db_path)

    templates = _build_templates(cfg)
    app.state.templates = templates
    app.state.config = cfg
    app.state.accounts = accounts
    app.state.active_account = active_account
    app.state.account_data_dir = active_account.path
    app.state.log_path = str(log_path)
    app.state.debug_logging_enabled = debug_logging

    templates.env.globals["accounts"] = serialize_accounts(accounts, active_account)
    templates.env.globals["active_account"] = asdict(active_account)

    log.info(
        "Application state reloaded (debug_logging=%s, log_path=%s)",
        debug_logging,
        log_path,
    )

    return cfg
