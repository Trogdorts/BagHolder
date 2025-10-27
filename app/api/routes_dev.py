"""Developer focused utility endpoints."""

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from app.core.authentication import require_user
from app.core.lifecycle import reload_application_state

router = APIRouter(dependencies=[Depends(require_user)])


@router.post("/dev/reload")
async def reload_application(request: Request):
    """Reload configuration, templates and database connections on demand."""

    reload_application_state(request.app)
    return JSONResponse({"status": "ok"})

