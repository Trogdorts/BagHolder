from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from app.core.config import AppConfig

router = APIRouter()

@router.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request):
    cfg: AppConfig = request.app.state.config
    return request.app.state.templates.TemplateResponse("settings.html", {"request": request, "cfg": cfg.raw})

@router.post("/settings", response_class=HTMLResponse)
def save_settings(request: Request,
                  theme: str = Form(...),
                  show_text: str = Form("true"),
                  show_weekends: str = Form("false"),
                  default_view: str = Form("latest"),
                  icon_color: str = Form("#6b7280")):
    cfg: AppConfig = request.app.state.config
    cfg.raw["ui"]["theme"] = theme
    cfg.raw["ui"]["show_text"] = (show_text.lower() == "true")
    cfg.raw["ui"]["show_weekends"] = (show_weekends.lower() == "true")
    cfg.raw["view"]["default"] = default_view
    icon_color = icon_color.strip()
    if not icon_color:
        icon_color = "#6b7280"
    elif not (icon_color.startswith("#") and len(icon_color) == 7 and all(ch in "0123456789abcdefABCDEF" for ch in icon_color[1:])):
        icon_color = cfg.raw["ui"].get("icon_color", "#6b7280")
    cfg.raw["ui"]["icon_color"] = icon_color
    cfg.save()
    return RedirectResponse(url="/settings", status_code=303)
