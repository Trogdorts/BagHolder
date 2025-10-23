import json
import os
import re
import signal
from datetime import datetime
from urllib.parse import quote_plus
from zipfile import BadZipFile

from fastapi import APIRouter, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
from starlette.background import BackgroundTask

from app.core.config import AppConfig
from app.core.lifecycle import reload_application_state
from app.services.data_backup import create_backup_archive, restore_backup_archive
from app.services.data_reset import clear_all_data

router = APIRouter()


def _resolve_data_directory(cfg: AppConfig) -> str:
    if cfg.path:
        return os.path.dirname(cfg.path)
    return os.environ.get("BAGHOLDER_DATA", "/app/data")


_HEX_COLOR_PATTERN = re.compile(r"^#[0-9a-fA-F]{6}$")


def _sanitize_hex_color(value: str, default: str, existing: str | None = None) -> str:
    """Return a normalized hex color from user input."""

    fallback = existing if isinstance(existing, str) and _HEX_COLOR_PATTERN.fullmatch(existing.strip()) else default
    if isinstance(value, str):
        candidate = value.strip()
        if _HEX_COLOR_PATTERN.fullmatch(candidate):
            return candidate.lower()
    return fallback.lower()

@router.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request):
    cfg: AppConfig = request.app.state.config
    cleared = request.query_params.get("cleared") is not None
    config_imported = request.query_params.get("config_imported") is not None
    error_param = request.query_params.get("config_error")
    backup_restored = request.query_params.get("backup_restored") is not None
    backup_error_code = request.query_params.get("backup_error")
    daily_summary_error_code = request.query_params.get("daily_summary_error")
    trade_csv_error_code = request.query_params.get("trade_csv_error")
    thinkorswim_error_code = request.query_params.get("thinkorswim_error")
    error_message = None
    if error_param:
        if error_param == "invalid_json":
            error_message = "Unable to import configuration: the provided data was not valid JSON."
        elif error_param == "invalid_type":
            error_message = "Unable to import configuration: the JSON must describe an object."
        elif error_param == "apply_failed":
            error_message = "Unable to import configuration due to an unknown error."
        else:
            error_message = error_param
    backup_error_message = None
    if backup_error_code == "no_file":
        backup_error_message = "Unable to import backup: no file was provided."
    elif backup_error_code == "invalid_zip":
        backup_error_message = "Unable to import backup: the uploaded file is not a valid ZIP archive."
    elif backup_error_code == "unsafe":
        backup_error_message = "Unable to import backup: archive paths would write outside the data directory."
    elif backup_error_code == "apply_failed":
        backup_error_message = "Unable to import backup due to an unknown error."
    daily_summary_error_message = None
    if daily_summary_error_code == "no_summaries":
        daily_summary_error_message = "No daily summaries were detected in the uploaded file."
    trade_csv_error_message = None
    if trade_csv_error_code == "no_trades":
        trade_csv_error_message = "No trades were detected in the uploaded file."
    thinkorswim_error_message = None
    if thinkorswim_error_code == "no_trades":
        thinkorswim_error_message = "No trades were detected in the uploaded statement."
    context = {
        "request": request,
        "cfg": cfg.raw,
        "cleared": cleared,
        "shutting_down": False,
        "config_imported": config_imported,
        "config_error_message": error_message,
        "backup_restored": backup_restored,
        "backup_error_message": backup_error_message,
        "daily_summary_error_message": daily_summary_error_message,
        "trade_csv_error_message": trade_csv_error_message,
        "thinkorswim_error_message": thinkorswim_error_message,
    }
    return request.app.state.templates.TemplateResponse("settings.html", context)

@router.post("/settings", response_class=HTMLResponse)
def save_settings(request: Request,
                  theme: str = Form(...),
                  show_text: str = Form("true"),
                  show_unrealized: str = Form("true"),
                  show_trade_count: str = Form("true"),
                  show_weekends: str = Form("false"),
                  default_view: str = Form("latest"),
                  icon_color: str = Form("#6b7280"),
                  primary_color: str = Form("#2563eb"),
                  primary_hover_color: str = Form("#3b82f6"),
                  success_color: str = Form("#22c55e"),
                  warning_color: str = Form("#f59e0b"),
                  danger_color: str = Form("#dc2626"),
                  danger_hover_color: str = Form("#ef4444"),
                  trade_badge_color: str = Form("#34d399"),
                  trade_badge_text_color: str = Form("#111827"),
                  note_icon_color: str = Form("#80cbc4"),
                  unrealized_fill_strategy: str = Form("carry_forward"),
                  export_empty_values: str = Form("zero")):
    cfg: AppConfig = request.app.state.config
    ui_section = cfg.raw.setdefault("ui", {})
    ui_section["theme"] = theme
    ui_section["show_text"] = (show_text.lower() == "true")
    ui_section["show_unrealized"] = (show_unrealized.lower() == "true")
    ui_section["show_trade_count"] = (show_trade_count.lower() == "true")
    ui_section["show_weekends"] = (show_weekends.lower() == "true")
    cfg.raw["view"]["default"] = default_view
    ui_section["icon_color"] = _sanitize_hex_color(icon_color, "#6b7280", ui_section.get("icon_color"))
    ui_section["primary_color"] = _sanitize_hex_color(primary_color, "#2563eb", ui_section.get("primary_color"))
    ui_section["primary_hover_color"] = _sanitize_hex_color(primary_hover_color, "#3b82f6", ui_section.get("primary_hover_color"))
    ui_section["success_color"] = _sanitize_hex_color(success_color, "#22c55e", ui_section.get("success_color"))
    ui_section["warning_color"] = _sanitize_hex_color(warning_color, "#f59e0b", ui_section.get("warning_color"))
    ui_section["danger_color"] = _sanitize_hex_color(danger_color, "#dc2626", ui_section.get("danger_color"))
    ui_section["danger_hover_color"] = _sanitize_hex_color(danger_hover_color, "#ef4444", ui_section.get("danger_hover_color"))
    ui_section["trade_badge_color"] = _sanitize_hex_color(trade_badge_color, "#34d399", ui_section.get("trade_badge_color"))
    ui_section["trade_badge_text_color"] = _sanitize_hex_color(trade_badge_text_color, "#111827", ui_section.get("trade_badge_text_color"))
    fill_options = {"carry_forward", "average_neighbors"}
    if unrealized_fill_strategy not in fill_options:
        unrealized_fill_strategy = "carry_forward"
    ui_section["unrealized_fill_strategy"] = unrealized_fill_strategy
    export_preference = export_empty_values.lower()
    cfg.raw.setdefault("export", {})
    cfg.raw["export"]["fill_empty_with_zero"] = export_preference != "empty"
    notes_section = cfg.raw.setdefault("notes", {})
    notes_section["icon_has_note_color"] = _sanitize_hex_color(
        note_icon_color,
        "#80cbc4",
        notes_section.get("icon_has_note_color"),
    )
    cfg.save()
    return RedirectResponse(url="/settings", status_code=303)


@router.get("/settings/config/export")
def export_settings_config(request: Request):
    cfg: AppConfig = request.app.state.config
    return JSONResponse(
        content=cfg.as_dict(),
        headers={"Content-Disposition": "attachment; filename=bagholder-config.json"},
    )


def _config_error_redirect(message: str) -> RedirectResponse:
    safe_message = quote_plus(message, safe="")
    return RedirectResponse(url=f"/settings?config_error={safe_message}", status_code=303)


@router.post("/settings/config/import", response_class=HTMLResponse)
async def import_settings_config(request: Request, config_file: UploadFile = File(None)):
    cfg: AppConfig = request.app.state.config

    if config_file is None or not config_file.filename:
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
        return _config_error_redirect(
            "Unable to import configuration: the uploaded file was empty."
        )

    try:
        payload = payload_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        return _config_error_redirect(
            f"Unable to import configuration: the file is not valid UTF-8 ({exc})."
        )

    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as exc:
        detail = f"{exc.msg} at line {exc.lineno}, column {exc.colno}"
        return _config_error_redirect(
            f"Unable to import configuration: JSON parsing failed ({detail})."
        )

    try:
        cfg.update_from_dict(parsed)
    except ValueError as exc:
        detail = str(exc) or "the provided JSON does not describe a valid configuration"
        return _config_error_redirect(
            f"Unable to import configuration: {detail}."
        )
    except Exception as exc:  # pragma: no cover - defensive guard
        detail = f"unexpected {exc.__class__.__name__}: {exc}".strip()
        return _config_error_redirect(
            f"Unable to import configuration: {detail}."
        )

    templates = getattr(request.app.state, "templates", None)
    if templates is not None:
        templates.env.globals["cfg"] = cfg.raw

    return RedirectResponse(url="/settings?config_imported=1", status_code=303)


@router.post("/settings/clear-data", response_class=HTMLResponse)
def clear_settings_data(request: Request):
    cfg: AppConfig = request.app.state.config
    data_dir = _resolve_data_directory(cfg)
    clear_all_data(data_dir)
    return RedirectResponse(url="/settings?cleared=1", status_code=303)


@router.post("/settings/shutdown", response_class=HTMLResponse)
def shutdown_application(request: Request):
    cfg: AppConfig = request.app.state.config

    def _trigger_shutdown():
        sig = getattr(signal, "SIGTERM", signal.SIGINT)
        os.kill(os.getpid(), sig)

    context = {
        "request": request,
        "cfg": cfg.raw,
        "cleared": False,
        "shutting_down": True,
    }

    background = BackgroundTask(_trigger_shutdown)
    return request.app.state.templates.TemplateResponse(
        "settings.html", context, background=background
    )


@router.get("/settings/backup/export")
def export_full_backup(request: Request):
    cfg: AppConfig = request.app.state.config
    data_dir = _resolve_data_directory(cfg)
    payload = create_backup_archive(data_dir)
    filename = f"bagholder-backup-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.zip"
    return Response(
        content=payload,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.post("/settings/backup/import", response_class=HTMLResponse)
async def import_full_backup(request: Request, backup_file: UploadFile = File(...)):
    cfg: AppConfig = request.app.state.config
    data_dir = _resolve_data_directory(cfg)

    if not backup_file.filename:
        await backup_file.close()
        return RedirectResponse("/settings?backup_error=no_file", status_code=303)

    payload = await backup_file.read()
    await backup_file.close()

    if not payload:
        return RedirectResponse("/settings?backup_error=invalid_zip", status_code=303)

    try:
        restore_backup_archive(data_dir, payload)
    except BadZipFile:
        return RedirectResponse("/settings?backup_error=invalid_zip", status_code=303)
    except ValueError:
        return RedirectResponse("/settings?backup_error=unsafe", status_code=303)
    except Exception:
        return RedirectResponse("/settings?backup_error=apply_failed", status_code=303)

    reload_application_state(request.app, data_dir=data_dir)
    return RedirectResponse("/settings?backup_restored=1", status_code=303)
