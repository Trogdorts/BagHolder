from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.core.database import get_session
from app.services.identity import IdentityService

router = APIRouter()


@router.get("/login", response_class=HTMLResponse)
def login_form(request: Request, db: Session = Depends(get_session)):
    if getattr(request.state, "user", None) is not None:
        return RedirectResponse(url="/", status_code=303)

    identity = IdentityService(db)
    allow_registration = identity.allow_self_registration()
    if allow_registration:
        return RedirectResponse(url="/setup", status_code=303)

    context = {
        "request": request,
        "cfg": request.app.state.config.raw,
        "allow_registration": allow_registration,
        "login_error": None,
        "registration_error": None,
        "submitted_username": "",
    }
    return request.app.state.templates.TemplateResponse(request, "login.html", context)


@router.post("/login", response_class=HTMLResponse)
def login_action(
    request: Request,
    username: str = Form(""),
    password: str = Form(""),
    db: Session = Depends(get_session),
):
    identity = IdentityService(db)
    result = identity.authenticate(username=username, password=password)

    if not result.success or result.user is None:
        context = {
            "request": request,
            "cfg": request.app.state.config.raw,
            "allow_registration": identity.allow_self_registration(),
            "login_error": result.error_message or "Invalid username or password.",
            "registration_error": None,
            "submitted_username": username,
        }
        return request.app.state.templates.TemplateResponse(request, "login.html", context, status_code=401)

    request.session["user_id"] = result.user.id
    return RedirectResponse(url="/", status_code=303)


@router.post("/login/register", response_class=HTMLResponse)
def register_action(
    request: Request,
    username: str = Form(""),
    password: str = Form(""),
    confirm_password: str = Form(""),
    db: Session = Depends(get_session),
):
    identity = IdentityService(db)
    result = identity.register(username=username, password=password, confirm_password=confirm_password)

    if not result.success or result.user is None:
        allow_registration = identity.allow_self_registration()
        context = {
            "request": request,
            "cfg": request.app.state.config.raw,
            "allow_registration": allow_registration,
            "login_error": None,
            "registration_error": result.error_message,
            "submitted_username": username,
        }
        status_code = 400 if allow_registration else 403
        return request.app.state.templates.TemplateResponse(
            request,
            "login.html",
            context,
            status_code=status_code,
        )

    request.session["user_id"] = result.user.id
    return RedirectResponse(url="/", status_code=303)


@router.post("/logout")
def logout_action(request: Request):
    request.session.pop("user_id", None)
    return RedirectResponse(url="/login", status_code=303)
