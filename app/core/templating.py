"""Template rendering helpers."""

from __future__ import annotations

from typing import Any

from fastapi import Request
from fastapi.responses import HTMLResponse


def render_template(
    request: Request,
    template_name: str,
    *,
    status_code: int = 200,
    **context: Any,
) -> HTMLResponse:
    """Render ``template_name`` with default context values."""

    templates = request.app.state.templates
    cfg = request.app.state.config.raw

    payload = {"request": request, "cfg": cfg}
    payload.update(context)
    return templates.TemplateResponse(request, template_name, payload, status_code=status_code)
