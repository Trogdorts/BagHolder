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
                  default_view: str = Form("latest")):
    cfg: AppConfig = request.app.state.config
    cfg.raw["ui"]["theme"] = theme
    cfg.raw["ui"]["show_text"] = (show_text.lower() == "true")
    cfg.raw["view"]["default"] = default_view
    cfg.save()
    return RedirectResponse(url="/settings", status_code=303)
