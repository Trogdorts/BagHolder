from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.core.authentication import AuthContext, get_auth_context
from app.core.database import get_session
from app.core.templating import render_template
from app.services.identity import IdentityService

router = APIRouter()


@router.get("/setup", response_class=HTMLResponse)
def setup_wizard(request: Request, context: AuthContext = Depends(get_auth_context)):
    if not context.needs_setup:
        if context.user is not None:
            return RedirectResponse(url="/", status_code=303)
        return RedirectResponse(url="/login", status_code=303)

    return render_template(
        request,
        "setup_wizard.html",
        error_message=None,
        submitted_username="",
    )


@router.post("/setup", response_class=HTMLResponse)
def setup_wizard_submit(
    request: Request,
    username: str = Form(""),
    password: str = Form(""),
    confirm_password: str = Form(""),
    context: AuthContext = Depends(get_auth_context),
    db: Session = Depends(get_session),
):
    identity = IdentityService(db)

    if not identity.allow_self_registration():
        if context.user is not None:
            return RedirectResponse(url="/", status_code=303)
        return RedirectResponse(url="/login", status_code=303)

    result = identity.register(username=username, password=password, confirm_password=confirm_password)
    if not result.success or result.user is None:
        return render_template(
            request,
            "setup_wizard.html",
            status_code=400,
            error_message=result.error_message or "Failed to create the administrator account.",
            submitted_username=username,
        )

    request.session["user_id"] = result.user.id
    return RedirectResponse(url="/", status_code=303)
