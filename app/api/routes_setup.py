from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.core.database import get_session
from app.services.identity import IdentityService

router = APIRouter()


@router.get("/setup", response_class=HTMLResponse)
def setup_wizard(request: Request, db: Session = Depends(get_session)):
    identity = IdentityService(db)
    if not identity.allow_self_registration():
        if getattr(request.state, "user", None) is not None:
            return RedirectResponse(url="/", status_code=303)
        return RedirectResponse(url="/login", status_code=303)

    context = {
        "request": request,
        "cfg": request.app.state.config.raw,
        "error_message": None,
        "submitted_username": "",
    }
    return request.app.state.templates.TemplateResponse(
        request,
        "setup_wizard.html",
        context,
    )


@router.post("/setup", response_class=HTMLResponse)
def setup_wizard_submit(
    request: Request,
    username: str = Form(""),
    password: str = Form(""),
    confirm_password: str = Form(""),
    db: Session = Depends(get_session),
):
    identity = IdentityService(db)

    if not identity.allow_self_registration():
        if getattr(request.state, "user", None) is not None:
            return RedirectResponse(url="/", status_code=303)
        return RedirectResponse(url="/login", status_code=303)

    result = identity.register(username=username, password=password, confirm_password=confirm_password)
    if not result.success or result.user is None:
        context = {
            "request": request,
            "cfg": request.app.state.config.raw,
            "error_message": result.error_message or "Failed to create the administrator account.",
            "submitted_username": username,
        }
        return request.app.state.templates.TemplateResponse(
            request,
            "setup_wizard.html",
            context,
            status_code=400,
        )

    request.session["user_id"] = result.user.id
    return RedirectResponse(url="/", status_code=303)
