from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.core.authentication import AuthContext, get_auth_context
from app.core.database import get_session
from app.core.templating import render_template
from app.services.identity import IdentityService

router = APIRouter()


@router.get("/login", response_class=HTMLResponse)
def login_form(request: Request, context: AuthContext = Depends(get_auth_context)):
    if context.user is not None:
        return RedirectResponse(url="/", status_code=303)

    if context.needs_setup:
        return RedirectResponse(url="/setup", status_code=303)

    return render_template(
        request,
        "login.html",
        allow_registration=False,
        login_error=None,
        registration_error=None,
        submitted_username="",
    )


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
        return render_template(
            request,
            "login.html",
            status_code=401,
            allow_registration=identity.allow_self_registration(),
            login_error=result.error_message or "Invalid username or password.",
            registration_error=None,
            submitted_username=username,
        )

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
        status_code = 400 if allow_registration else 403
        return render_template(
            request,
            "login.html",
            status_code=status_code,
            allow_registration=allow_registration,
            login_error=None,
            registration_error=result.error_message,
            submitted_username=username,
        )

    request.session["user_id"] = result.user.id
    return RedirectResponse(url="/", status_code=303)


@router.post("/logout")
def logout_action(request: Request):
    request.session.pop("user_id", None)
    return RedirectResponse(url="/login", status_code=303)
