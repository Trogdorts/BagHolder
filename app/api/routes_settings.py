import os

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse

from app.core.config import AppConfig
from app.services.data_reset import clear_all_data

router = APIRouter()

@router.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request):
    cfg: AppConfig = request.app.state.config
    cleared = request.query_params.get("cleared") is not None
    context = {"request": request, "cfg": cfg.raw, "cleared": cleared}
    return request.app.state.templates.TemplateResponse("settings.html", context)

@router.post("/settings", response_class=HTMLResponse)
def save_settings(request: Request,
                  theme: str = Form(...),
                  show_text: str = Form("true"),
                  show_unrealized: str = Form("true"),
                  show_weekends: str = Form("false"),
                  default_view: str = Form("latest"),
                  icon_color: str = Form("#6b7280"),
                  unrealized_fill_strategy: str = Form("carry_forward")):
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
    cfg.save()
    return RedirectResponse(url="/settings", status_code=303)


@router.post("/settings/clear-data", response_class=HTMLResponse)
def clear_settings_data(request: Request):
    cfg: AppConfig = request.app.state.config
    data_dir = os.path.dirname(cfg.path) if cfg.path else os.environ.get("BAGHOLDER_DATA", "/app/data")
    clear_all_data(data_dir)
    return RedirectResponse(url="/settings?cleared=1", status_code=303)
