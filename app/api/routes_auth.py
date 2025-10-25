from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.auth import hash_password, verify_password
from app.core.database import get_session
from app.core.models import User

router = APIRouter()


@router.get("/login", response_class=HTMLResponse)
def login_form(request: Request, db: Session = Depends(get_session)):
    if getattr(request.state, "user", None) is not None:
        return RedirectResponse(url="/", status_code=303)

    user_count = db.query(func.count(User.id)).scalar() or 0
    context = {
        "request": request,
        "cfg": request.app.state.config.raw,
        "allow_registration": user_count == 0,
        "login_error": None,
        "registration_error": None,
    }
    return request.app.state.templates.TemplateResponse(request, "login.html", context)


@router.post("/login", response_class=HTMLResponse)
def login_action(
    request: Request,
    username: str = Form(""),
    password: str = Form(""),
    db: Session = Depends(get_session),
):
    normalized = (username or "").strip().lower()
    user = None
    if normalized:
        user = db.query(User).filter(User.username == normalized).first()

    if user is None or not verify_password(password or "", user.password_salt, user.password_hash):
        user_count = db.query(func.count(User.id)).scalar() or 0
        context = {
            "request": request,
            "cfg": request.app.state.config.raw,
            "allow_registration": user_count == 0,
            "login_error": "Invalid username or password.",
            "registration_error": None,
            "submitted_username": username,
        }
        return request.app.state.templates.TemplateResponse(request, "login.html", context, status_code=401)

    request.session["user_id"] = user.id
    return RedirectResponse(url="/", status_code=303)


@router.post("/login/register", response_class=HTMLResponse)
def register_action(
    request: Request,
    username: str = Form(""),
    password: str = Form(""),
    confirm_password: str = Form(""),
    db: Session = Depends(get_session),
):
    existing_users = db.query(func.count(User.id)).scalar() or 0
    allow_registration = existing_users == 0
    normalized = (username or "").strip().lower()
    error = None

    if not allow_registration:
        error = "Registration is disabled once an account exists."
    elif not normalized:
        error = "Username is required."
    elif len(password or "") < 8:
        error = "Password must be at least 8 characters long."
    elif password != confirm_password:
        error = "Passwords do not match."
    elif db.query(User).filter(User.username == normalized).first() is not None:
        error = "Username is already in use."

    if error:
        context = {
            "request": request,
            "cfg": request.app.state.config.raw,
            "allow_registration": allow_registration,
            "login_error": None,
            "registration_error": error,
            "submitted_username": username,
        }
        status_code = 400 if allow_registration else 403
        return request.app.state.templates.TemplateResponse(
            request,
            "login.html",
            context,
            status_code=status_code,
        )

    salt, password_hash = hash_password(password)
    user = User(username=normalized, password_hash=password_hash, password_salt=salt)
    db.add(user)
    db.commit()

    request.session["user_id"] = user.id
    return RedirectResponse(url="/", status_code=303)


@router.post("/logout")
def logout_action(request: Request):
    request.session.pop("user_id", None)
    return RedirectResponse(url="/login", status_code=303)
