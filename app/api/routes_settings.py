import json
import logging
import os
import re
import signal
from dataclasses import asdict
from datetime import datetime
from typing import Any
from urllib.parse import quote_plus
from zipfile import BadZipFile

from fastapi import APIRouter, Request, Form, UploadFile, File
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    Response,
)
from starlette.background import BackgroundTask

from app.core.config import AppConfig, DEFAULT_CONFIG
from app.core.lifecycle import reload_application_state
from app.core.logger import configure_logging
from app.core.utils import coerce_bool
from app.services.accounts import (
    create_account,
    prepare_accounts,
    rename_account,
    serialize_accounts,
    set_active_account,
)
from app.services.data_backup import create_backup_archive, restore_backup_archive
from app.services.data_reset import clear_all_data

router = APIRouter()


log = logging.getLogger(__name__)


def _resolve_data_directory(cfg: AppConfig) -> str:
    if cfg.path:
        return os.path.dirname(cfg.path)
    return os.environ.get("BAGHOLDER_DATA", "/app/data")


def _resolve_account_directory(request: Request, cfg: AppConfig) -> str:
    existing = getattr(request.app.state, "account_data_dir", None)
    if isinstance(existing, str) and existing:
        return existing
    base_dir = _resolve_data_directory(cfg)
    _, active = prepare_accounts(cfg, base_dir)
    return active.path


def _normalize_redirect_target(value: str | None) -> str:
    if isinstance(value, str):
        candidate = value.strip()
        if candidate.startswith("/"):
            return candidate
    return "/settings"


_HEX_COLOR_PATTERN = re.compile(r"^#[0-9a-fA-F]{6}$")


def _sanitize_hex_color(value: str, default: str, existing: str | None = None) -> str:
    """Return a normalized hex color from user input."""

    fallback = existing if isinstance(existing, str) and _HEX_COLOR_PATTERN.fullmatch(existing.strip()) else default
    if isinstance(value, str):
        candidate = value.strip()
        if _HEX_COLOR_PATTERN.fullmatch(candidate):
            return candidate.lower()
    return fallback.lower()


def _coerce_port(value: str | int, fallback: int) -> int:
    """Return a validated TCP port number.

    Values outside the valid range (1-65535) or non-integer inputs fall back to
    ``fallback`` so existing configuration values remain intact when invalid
    data is submitted.
    """

    try:
        port = int(value)
    except (TypeError, ValueError):
        return fallback
    if 1 <= port <= 65535:
        return port
    return fallback


_COLOR_GROUPS = [
    {
        "title": "Interface accents",
        "description": "Buttons, icons, and other reusable interface controls.",
        "fields": [
            {
                "name": "icon_color",
                "label": "Action icon color",
                "description": "Applied to interactive glyphs such as the calendar gear icon.",
                "config_path": ("ui", "icon_color"),
                "default": DEFAULT_CONFIG["ui"]["icon_color"].lower(),
            },
            {
                "name": "primary_color",
                "label": "Primary button color",
                "description": "The baseline accent for confirm buttons and focused controls.",
                "config_path": ("ui", "primary_color"),
                "default": DEFAULT_CONFIG["ui"]["primary_color"].lower(),
            },
            {
                "name": "primary_hover_color",
                "label": "Primary hover color",
                "description": "A darker shade used for hover and focus states on primary actions.",
                "config_path": ("ui", "primary_hover_color"),
                "default": DEFAULT_CONFIG["ui"]["primary_hover_color"].lower(),
            },
        ],
    },
    {
        "title": "Profit & risk feedback",
        "description": "Colors that communicate gains, warnings, and losses on the calendar.",
        "fields": [
            {
                "name": "success_color",
                "label": "Success accent color",
                "description": "Used for positive profit figures and confirming states.",
                "config_path": ("ui", "success_color"),
                "default": DEFAULT_CONFIG["ui"]["success_color"].lower(),
            },
            {
                "name": "warning_color",
                "label": "Warning accent color",
                "description": "Highlights cells that need attention without indicating a loss.",
                "config_path": ("ui", "warning_color"),
                "default": DEFAULT_CONFIG["ui"]["warning_color"].lower(),
            },
            {
                "name": "danger_color",
                "label": "Danger accent color",
                "description": "The base color for loss states and destructive actions.",
                "config_path": ("ui", "danger_color"),
                "default": DEFAULT_CONFIG["ui"]["danger_color"].lower(),
            },
            {
                "name": "danger_hover_color",
                "label": "Danger hover color",
                "description": "A deeper shade for hover and focus states on danger actions.",
                "config_path": ("ui", "danger_hover_color"),
                "default": DEFAULT_CONFIG["ui"]["danger_hover_color"].lower(),
            },
        ],
    },
    {
        "title": "Trade & note indicators",
        "description": "Badges and note indicators that supplement the calendar view.",
        "fields": [
            {
                "name": "trade_badge_color",
                "label": "Trade badge color",
                "description": "Background color for the trade count badge shown on each day.",
                "config_path": ("ui", "trade_badge_color"),
                "default": DEFAULT_CONFIG["ui"]["trade_badge_color"].lower(),
            },
            {
                "name": "trade_badge_text_color",
                "label": "Trade badge text color",
                "description": "Text color inside the trade count badge for contrast.",
                "config_path": ("ui", "trade_badge_text_color"),
                "default": DEFAULT_CONFIG["ui"]["trade_badge_text_color"].lower(),
            },
            {
                "name": "note_icon_color",
                "label": "Note icon highlight color",
                "description": "Applied when a day or week already contains a saved note.",
                "config_path": ("notes", "icon_has_note_color"),
                "default": DEFAULT_CONFIG["notes"]["icon_has_note_color"].lower(),
            },
        ],
    },
]

_COLOR_FIELD_INDEX = {
    field["name"]: field
    for group in _COLOR_GROUPS
    for field in group["fields"]
}


_CONFIG_IMPORT_ERRORS = {
    "invalid_json": "Unable to import configuration: the provided data was not valid JSON.",
    "invalid_type": "Unable to import configuration: the JSON must describe an object.",
    "apply_failed": "Unable to import configuration due to an unknown error.",
}

_BACKUP_IMPORT_ERRORS = {
    "no_file": "Unable to import backup: no file was provided.",
    "invalid_zip": "Unable to import backup: the uploaded file is not a valid ZIP archive.",
    "unsafe": "Unable to import backup: archive paths would write outside the data directory.",
    "apply_failed": "Unable to import backup due to an unknown error.",
}

_TRADE_IMPORT_ERRORS = {
    "no_trades": "No trades were detected in the uploaded file.",
}

_THINKORSWIM_IMPORT_ERRORS = {
    "no_trades": "No trades were detected in the uploaded statement.",
}

_LOG_EXPORT_ERRORS = {
    "missing": "No log file is available yet. Generate activity and try again.",
}


_ACCOUNT_STATUS_MESSAGES = {
    "created": "Account created successfully.",
    "renamed": "Account name updated.",
    "switched": "Active account changed.",
}

_ACCOUNT_ERROR_MESSAGES = {
    "missing": "The selected account could not be found.",
    "empty_name": "Account name cannot be empty.",
    "unknown": "Unable to update account due to an unexpected error.",
}


def _resolve_message(code: str | None, mapping: dict[str, str]) -> str | None:
    if not code:
        return None
    return mapping.get(code, code)


def _account_success_redirect(status: str, redirect_to: str | None) -> RedirectResponse:
    target = _normalize_redirect_target(redirect_to)
    if target == "/settings":
        return RedirectResponse(url=f"/settings?account_status={quote_plus(status, safe='')}", status_code=303)
    return RedirectResponse(url=target, status_code=303)


def _account_error_redirect(code: str, redirect_to: str | None) -> RedirectResponse:
    target = _normalize_redirect_target(redirect_to)
    if target == "/settings":
        return RedirectResponse(url=f"/settings?account_error={quote_plus(code, safe='')}", status_code=303)
    return RedirectResponse(url=target, status_code=303)


def _build_color_context(cfg: AppConfig) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Return structured color configuration data for the settings template."""

    color_groups: list[dict[str, Any]] = []
    for group in _COLOR_GROUPS:
        fields = []
        for field in group["fields"]:
            current_value: Any = cfg.raw
            for key in field["config_path"]:
                if isinstance(current_value, dict):
                    current_value = current_value.get(key)
                else:
                    current_value = None
                    break
            if isinstance(current_value, str) and _HEX_COLOR_PATTERN.fullmatch(current_value.strip()):
                current_value = current_value.strip().lower()
            if not isinstance(current_value, str) or not current_value:
                current_value = field["default"]
            fields.append({
                "name": field["name"],
                "label": field["label"],
                "description": field["description"],
                "config_path": field["config_path"],
                "default": field["default"],
                "current": current_value,
            })
        color_groups.append({
            "title": group["title"],
            "description": group["description"],
            "fields": fields,
        })

    color_defaults = {
        name: field["default"] for name, field in _COLOR_FIELD_INDEX.items()
    }
    return color_groups, color_defaults


@router.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request):
    cfg: AppConfig = request.app.state.config
    params = request.query_params
    cleared = params.get("cleared") is not None
    config_imported = params.get("config_imported") is not None
    backup_restored = params.get("backup_restored") is not None
    error_message = _resolve_message(params.get("config_error"), _CONFIG_IMPORT_ERRORS)
    backup_error_message = _resolve_message(params.get("backup_error"), _BACKUP_IMPORT_ERRORS)
    trade_csv_error_message = _resolve_message(
        params.get("trade_csv_error"), _TRADE_IMPORT_ERRORS
    )
    thinkorswim_error_message = _resolve_message(
        params.get("thinkorswim_error"), _THINKORSWIM_IMPORT_ERRORS
    )
    log_error_message = _resolve_message(params.get("log_error"), _LOG_EXPORT_ERRORS)
    account_status_message = _resolve_message(params.get("account_status"), _ACCOUNT_STATUS_MESSAGES)
    account_error_message = _resolve_message(params.get("account_error"), _ACCOUNT_ERROR_MESSAGES)
    color_groups, color_defaults = _build_color_context(cfg)
    diagnostics_cfg = cfg.raw.get("diagnostics", {}) if isinstance(cfg.raw, dict) else {}
    debug_logging_enabled = coerce_bool(diagnostics_cfg.get("debug_logging"), False)
    log_path = getattr(request.app.state, "log_path", None)
    log_export_available = bool(log_path and os.path.exists(log_path))

    accounts_records = getattr(request.app.state, "accounts", None)
    active_record = getattr(request.app.state, "active_account", None)
    if not accounts_records or active_record is None:
        base_dir = _resolve_data_directory(cfg)
        accounts_records, active_record = prepare_accounts(cfg, base_dir)

    serialized_accounts = serialize_accounts(accounts_records, active_record)
    active_account_payload = asdict(active_record)

    context = {
        "request": request,
        "cfg": cfg.raw,
        "cleared": cleared,
        "shutting_down": False,
        "config_imported": config_imported,
        "config_error_message": error_message,
        "backup_restored": backup_restored,
        "backup_error_message": backup_error_message,
        "trade_csv_error_message": trade_csv_error_message,
        "thinkorswim_error_message": thinkorswim_error_message,
        "color_groups": color_groups,
        "color_defaults": color_defaults,
        "debug_logging_enabled": debug_logging_enabled,
        "log_export_available": log_export_available,
        "log_error_message": log_error_message,
        "accounts": serialized_accounts,
        "active_account": active_account_payload,
        "account_status_message": account_status_message,
        "account_error_message": account_error_message,
    }
    return request.app.state.templates.TemplateResponse(
        request,
        "settings.html",
        context,
    )

@router.post("/settings", response_class=HTMLResponse)
def save_settings(
    request: Request,
    theme: str = Form(...),
    show_text: str = Form("true"),
    show_unrealized: str = Form("true"),
    show_trade_count: str = Form("true"),
    show_percentages: str = Form("true"),
    show_weekends: str = Form("false"),
    default_view: str = Form("latest"),
    listening_port: str = Form(str(DEFAULT_CONFIG["server"]["port"])),
    debug_logging: str = Form("false"),
    icon_color: str = Form("#6b7280"),
    primary_color: str = Form("#2563eb"),
    primary_hover_color: str = Form("#1d4ed8"),
    success_color: str = Form("#22c55e"),
    warning_color: str = Form("#f59e0b"),
    danger_color: str = Form("#dc2626"),
    danger_hover_color: str = Form("#b91c1c"),
    trade_badge_color: str = Form("#34d399"),
    trade_badge_text_color: str = Form("#111827"),
    note_icon_color: str = Form("#80cbc4"),
    unrealized_fill_strategy: str = Form("carry_forward"),
    export_empty_values: str = Form("zero"),
):
    cfg: AppConfig = request.app.state.config
    server_section = cfg.raw.setdefault("server", {})
    ui_section = cfg.raw.setdefault("ui", {})
    notes_section = cfg.raw.setdefault("notes", {})
    diagnostics_section = cfg.raw.setdefault("diagnostics", {})
    view_section = cfg.raw.setdefault("view", {})
    current_port = server_section.get("port", DEFAULT_CONFIG["server"]["port"])
    server_section["port"] = _coerce_port(listening_port, current_port)
    ui_section["theme"] = theme
    ui_section["show_text"] = coerce_bool(show_text, True)
    ui_section["show_unrealized"] = coerce_bool(show_unrealized, True)
    ui_section["show_trade_count"] = coerce_bool(show_trade_count, True)
    ui_section["show_percentages"] = coerce_bool(show_percentages, True)
    ui_section["show_weekends"] = coerce_bool(show_weekends, False)
    view_section["default"] = default_view
    color_sections = {
        "ui": ui_section,
        "notes": notes_section,
    }
    color_inputs = {name: locals().get(name) for name in _COLOR_FIELD_INDEX.keys()}
    for name, raw_value in color_inputs.items():
        field = _COLOR_FIELD_INDEX[name]
        section_key, option_key = field["config_path"]
        target_section = color_sections.get(section_key)
        if target_section is None:
            target_section = cfg.raw.setdefault(section_key, {})
            color_sections[section_key] = target_section
        target_section[option_key] = _sanitize_hex_color(
            raw_value,
            field["default"],
            target_section.get(option_key),
        )
    fill_options = {"carry_forward", "average_neighbors"}
    if unrealized_fill_strategy not in fill_options:
        unrealized_fill_strategy = "carry_forward"
    ui_section["unrealized_fill_strategy"] = unrealized_fill_strategy
    export_preference = export_empty_values.lower()
    cfg.raw.setdefault("export", {})
    cfg.raw["export"]["fill_empty_with_zero"] = export_preference != "empty"
    debug_logging_enabled = coerce_bool(debug_logging, diagnostics_section.get("debug_logging", False))
    diagnostics_section["debug_logging"] = debug_logging_enabled
    cfg.save()
    data_dir = _resolve_data_directory(cfg)
    log_path = configure_logging(
        data_dir,
        debug_enabled=debug_logging_enabled,
        max_bytes=diagnostics_section.get("log_max_bytes", 1_048_576),
        retention=diagnostics_section.get("log_retention", 5),
    )
    request.app.state.log_path = str(log_path)
    request.app.state.debug_logging_enabled = debug_logging_enabled
    log.info(
        "Settings updated (theme=%s, debug_logging=%s, port=%s)",
        theme,
        debug_logging_enabled,
        server_section["port"],
    )
    return RedirectResponse(url="/settings", status_code=303)


@router.post("/settings/accounts/create", response_class=HTMLResponse)
def create_new_account(
    request: Request,
    account_name: str = Form(""),
    redirect_to: str = Form("/settings"),
):
    cfg: AppConfig = request.app.state.config
    base_dir = _resolve_data_directory(cfg)
    try:
        record = create_account(cfg, base_dir, account_name)
    except Exception:  # pragma: no cover - defensive
        log.exception("Failed to create trading account")
        return _account_error_redirect("unknown", redirect_to)

    reload_application_state(request.app, data_dir=base_dir)
    log.info("Created account %s (%s)", record.id, record.name)
    return _account_success_redirect("created", redirect_to)


@router.post("/settings/accounts/rename", response_class=HTMLResponse)
def rename_existing_account(
    request: Request,
    account_id: str = Form(...),
    account_name: str = Form(...),
    redirect_to: str = Form("/settings"),
):
    cfg: AppConfig = request.app.state.config
    base_dir = _resolve_data_directory(cfg)
    try:
        rename_account(cfg, base_dir, account_id, account_name)
    except ValueError as exc:
        detail = (str(exc) or "").lower()
        if "empty" in detail:
            code = "empty_name"
        else:
            code = "missing"
        log.warning("Failed to rename account %s: %s", account_id, exc)
        return _account_error_redirect(code, redirect_to)
    except Exception:  # pragma: no cover - defensive
        log.exception("Unexpected error renaming account %s", account_id)
        return _account_error_redirect("unknown", redirect_to)

    reload_application_state(request.app, data_dir=base_dir)
    log.info("Renamed account %s", account_id)
    return _account_success_redirect("renamed", redirect_to)


@router.post("/settings/accounts/switch", response_class=HTMLResponse)
def switch_active_account(
    request: Request,
    account_id: str = Form(...),
    redirect_to: str = Form("/settings"),
):
    cfg: AppConfig = request.app.state.config
    base_dir = _resolve_data_directory(cfg)
    try:
        set_active_account(cfg, base_dir, account_id)
    except ValueError:
        log.warning("Attempted to switch to unknown account %s", account_id)
        return _account_error_redirect("missing", redirect_to)

    reload_application_state(request.app, data_dir=base_dir)
    log.info("Active account set to %s", account_id)
    return _account_success_redirect("switched", redirect_to)


@router.get("/settings/config/export")
def export_settings_config(request: Request):
    cfg: AppConfig = request.app.state.config
    log.info("Configuration exported as JSON")
    return JSONResponse(
        content=cfg.as_dict(),
        headers={"Content-Disposition": "attachment; filename=bagholder-config.json"},
    )


def _config_error_redirect(message: str) -> RedirectResponse:
    safe_message = quote_plus(message, safe="")
    log.warning("Configuration import failed: %s", message)
    return RedirectResponse(url=f"/settings?config_error={safe_message}", status_code=303)


@router.post("/settings/config/import", response_class=HTMLResponse)
async def import_settings_config(request: Request, config_file: UploadFile = File(None)):
    cfg: AppConfig = request.app.state.config

    if config_file is None or not config_file.filename:
        log.warning("Configuration import attempted without a file")
        if config_file is not None:
            await config_file.close()
        return _config_error_redirect(
            "Unable to import configuration: no file was provided."
        )

    try:
        payload_bytes = await config_file.read()
    finally:
        await config_file.close()

    if not payload_bytes:
        log.warning("Configuration import failed: uploaded file was empty")
        return _config_error_redirect(
            "Unable to import configuration: the uploaded file was empty."
        )

    try:
        payload = payload_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        log.warning("Configuration import failed: invalid encoding (%s)", exc)
        return _config_error_redirect(
            f"Unable to import configuration: the file is not valid UTF-8 ({exc})."
        )

    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as exc:
        detail = f"{exc.msg} at line {exc.lineno}, column {exc.colno}"
        log.warning("Configuration import failed: JSON parsing error (%s)", detail)
        return _config_error_redirect(
            f"Unable to import configuration: JSON parsing failed ({detail})."
        )

    try:
        cfg.update_from_dict(parsed)
    except ValueError as exc:
        detail = str(exc) or "the provided JSON does not describe a valid configuration"
        log.warning("Configuration import failed: %s", detail)
        return _config_error_redirect(
            f"Unable to import configuration: {detail}."
        )
    except Exception as exc:  # pragma: no cover - defensive guard
        detail = f"unexpected {exc.__class__.__name__}: {exc}".strip()
        log.exception("Configuration import failed unexpectedly")
        return _config_error_redirect(
            f"Unable to import configuration: {detail}."
        )

    base_dir = _resolve_data_directory(cfg)
    reload_application_state(request.app, data_dir=base_dir)

    log.info("Configuration imported successfully from %s", config_file.filename)
    return RedirectResponse(url="/settings?config_imported=1", status_code=303)


@router.post("/settings/clear-data", response_class=HTMLResponse)
def clear_settings_data(request: Request):
    cfg: AppConfig = request.app.state.config
    account_dir = _resolve_account_directory(request, cfg)
    clear_all_data(account_dir)
    log.warning("All application data cleared via settings page")
    return RedirectResponse(url="/settings?cleared=1", status_code=303)


@router.post("/settings/shutdown", response_class=HTMLResponse)
def shutdown_application(request: Request):
    cfg: AppConfig = request.app.state.config

    def _trigger_shutdown():
        sig = getattr(signal, "SIGTERM", signal.SIGINT)
        os.kill(os.getpid(), sig)

    log.info("Shutdown requested via settings page")
    color_groups, color_defaults = _build_color_context(cfg)
    context = {
        "request": request,
        "cfg": cfg.raw,
        "cleared": False,
        "shutting_down": True,
        "color_groups": color_groups,
        "color_defaults": color_defaults,
    }

    background = BackgroundTask(_trigger_shutdown)
    return request.app.state.templates.TemplateResponse(
        request,
        "settings.html",
        context,
        background=background,
    )


@router.get("/settings/backup/export")
def export_full_backup(request: Request):
    cfg: AppConfig = request.app.state.config
    data_dir = _resolve_data_directory(cfg)
    payload = create_backup_archive(data_dir)
    filename = f"bagholder-backup-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.zip"
    log.info("Full backup exported to %s", filename)
    return Response(
        content=payload,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.get("/settings/logs/export")
def export_debug_logs(request: Request):
    log_path = getattr(request.app.state, "log_path", None)
    if not log_path or not os.path.exists(log_path):
        log.warning("Log export requested but no log file is available")
        return RedirectResponse("/settings?log_error=missing", status_code=303)

    filename = f"bagholder-debug-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.log"
    log.info("Debug log exported to %s", filename)
    with open(log_path, "rb") as handle:
        payload = handle.read()
    return Response(
        content=payload,
        media_type="text/plain",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.post("/settings/backup/import", response_class=HTMLResponse)
async def import_full_backup(request: Request, backup_file: UploadFile = File(...)):
    cfg: AppConfig = request.app.state.config
    data_dir = _resolve_data_directory(cfg)

    if not backup_file.filename:
        await backup_file.close()
        log.warning("Backup import attempted without selecting a file")
        return RedirectResponse("/settings?backup_error=no_file", status_code=303)

    payload = await backup_file.read()
    await backup_file.close()

    if not payload:
        log.warning("Backup import failed: file was empty")
        return RedirectResponse("/settings?backup_error=invalid_zip", status_code=303)

    try:
        restore_backup_archive(data_dir, payload)
    except BadZipFile:
        log.warning("Backup import failed: invalid zip archive")
        return RedirectResponse("/settings?backup_error=invalid_zip", status_code=303)
    except ValueError:
        log.warning("Backup import failed: unsafe archive contents")
        return RedirectResponse("/settings?backup_error=unsafe", status_code=303)
    except Exception:
        log.exception("Backup import failed unexpectedly")
        return RedirectResponse("/settings?backup_error=apply_failed", status_code=303)

    reload_application_state(request.app, data_dir=data_dir)
    log.info("Backup imported successfully from %s", backup_file.filename)
    return RedirectResponse("/settings?backup_restored=1", status_code=303)
