import json
import os
import signal
from datetime import datetime
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

@router.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request):
    cfg: AppConfig = request.app.state.config
    cleared = request.query_params.get("cleared") is not None
    config_imported = request.query_params.get("config_imported") is not None
    error_code = request.query_params.get("config_error")
    backup_restored = request.query_params.get("backup_restored") is not None
    backup_error_code = request.query_params.get("backup_error")
    error_message = None
    if error_code == "invalid_json":
        error_message = "Unable to import configuration: the provided text is not valid JSON."
    elif error_code == "invalid_type":
        error_message = "Unable to import configuration: the JSON must describe an object."
    elif error_code == "apply_failed":
        error_message = "Unable to import configuration due to an unknown error."
    backup_error_message = None
    if backup_error_code == "no_file":
        backup_error_message = "Unable to import backup: no file was provided."
    elif backup_error_code == "invalid_zip":
        backup_error_message = "Unable to import backup: the uploaded file is not a valid ZIP archive."
    elif backup_error_code == "unsafe":
        backup_error_message = "Unable to import backup: archive paths would write outside the data directory."
    elif backup_error_code == "apply_failed":
        backup_error_message = "Unable to import backup due to an unknown error."
    context = {
        "request": request,
        "cfg": cfg.raw,
        "cleared": cleared,
        "shutting_down": False,
        "config_imported": config_imported,
        "config_error_message": error_message,
        "backup_restored": backup_restored,
        "backup_error_message": backup_error_message,
    }
    return request.app.state.templates.TemplateResponse("settings.html", context)

@router.post("/settings", response_class=HTMLResponse)
def save_settings(request: Request,
                  theme: str = Form(...),
                  show_text: str = Form("true"),
                  show_unrealized: str = Form("true"),
                  show_weekends: str = Form("false"),
                  default_view: str = Form("latest"),
                  icon_color: str = Form("#6b7280"),
                  unrealized_fill_strategy: str = Form("carry_forward"),
                  export_empty_values: str = Form("zero")):
    cfg: AppConfig = request.app.state.config
    cfg.raw["ui"]["theme"] = theme
    cfg.raw["ui"]["show_text"] = (show_text.lower() == "true")
    cfg.raw["ui"]["show_unrealized"] = (show_unrealized.lower() == "true")
    cfg.raw["ui"]["show_weekends"] = (show_weekends.lower() == "true")
    cfg.raw["view"]["default"] = default_view
    icon_color = icon_color.strip()
    if not icon_color:
        icon_color = "#6b7280"
    elif not (icon_color.startswith("#") and len(icon_color) == 7 and all(ch in "0123456789abcdefABCDEF" for ch in icon_color[1:])):
        icon_color = cfg.raw["ui"].get("icon_color", "#6b7280")
    cfg.raw["ui"]["icon_color"] = icon_color
    fill_options = {"carry_forward", "average_neighbors"}
    if unrealized_fill_strategy not in fill_options:
        unrealized_fill_strategy = "carry_forward"
    cfg.raw["ui"]["unrealized_fill_strategy"] = unrealized_fill_strategy
    export_preference = export_empty_values.lower()
    cfg.raw.setdefault("export", {})
    cfg.raw["export"]["fill_empty_with_zero"] = export_preference != "empty"
    cfg.save()
    return RedirectResponse(url="/settings", status_code=303)


@router.get("/settings/config/export")
def export_settings_config(request: Request):
    cfg: AppConfig = request.app.state.config
    return JSONResponse(
        content=cfg.as_dict(),
        headers={"Content-Disposition": "attachment; filename=bagholder-config.json"},
    )


@router.post("/settings/config/import", response_class=HTMLResponse)
def import_settings_config(request: Request, config_json: str = Form(...)):
    cfg: AppConfig = request.app.state.config
    payload = (config_json or "").strip()
    if not payload:
        return RedirectResponse(url="/settings?config_error=invalid_json", status_code=303)
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return RedirectResponse(url="/settings?config_error=invalid_json", status_code=303)
    try:
        cfg.update_from_dict(parsed)
    except ValueError:
        return RedirectResponse(url="/settings?config_error=invalid_type", status_code=303)
    except Exception:  # pragma: no cover - defensive guard
        return RedirectResponse(url="/settings?config_error=apply_failed", status_code=303)

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
