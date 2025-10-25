"""Deployment bootstrap helpers used by Docker and CLI tooling."""

from __future__ import annotations

import logging
import os
from typing import cast

from sqlalchemy.orm import Session

from app.core.config import AppConfig
from app.core.database import dispose_engine, init_db
from app.core.models import User
from app.core.seed import ensure_seed
from app.services.accounts import prepare_accounts
from app.services.identity import IdentityService

log = logging.getLogger(__name__)


class BootstrapError(RuntimeError):
    """Raised when the deployment bootstrap process fails."""


def _normalize_username(username: str) -> str:
    if not isinstance(username, str):
        raise BootstrapError("Username is required")
    normalized = username.strip().lower()
    if not normalized:
        raise BootstrapError("Username is required")
    return normalized


def _validate_password(password: str) -> str:
    if not isinstance(password, str) or len(password) < 8:
        raise BootstrapError("Password must be at least 8 characters long")
    return password


def bootstrap_admin(username: str, password: str, *, data_dir: str | None = None) -> User:
    """Create the initial administrator account.

    Parameters
    ----------
    username:
        Username for the administrator account. The value is normalized to
        lower-case to match the application's authentication behaviour.
    password:
        Password for the administrator account. Must satisfy the default
        password policy of eight characters or more.
    data_dir:
        Optional explicit data directory. Falls back to the ``BAGHOLDER_DATA``
        environment variable or ``/app/data``.
    """

    data_dir = data_dir or os.environ.get("BAGHOLDER_DATA", "/app/data")
    normalized_username = _normalize_username(username)
    validated_password = _validate_password(password)

    os.makedirs(data_dir, exist_ok=True)

    cfg = AppConfig.load(data_dir)
    _, active_account = prepare_accounts(cfg, data_dir)
    db_path = os.path.join(active_account.path, "profitloss.db")

    ensure_seed(db_path)
    _, session_factory = init_db(db_path)

    try:
        with session_factory() as session:
            session = cast(Session, session)
            identity = IdentityService(session)
            if not identity.allow_self_registration():
                raise BootstrapError(
                    "An account already exists. Use the web UI to manage users."
                )

            result = identity.register(
                username=normalized_username,
                password=validated_password,
                confirm_password=validated_password,
            )
            if not result.success or result.user is None:
                raise BootstrapError(
                    result.error_message or "Failed to create administrator account."
                )
            return result.user
    finally:
        dispose_engine()


def maybe_bootstrap_admin_from_env(*, data_dir: str | None = None) -> bool:
    """Create the administrator account from environment variables if provided.

    The function reads ``BAGHOLDER_BOOTSTRAP_USERNAME`` and
    ``BAGHOLDER_BOOTSTRAP_PASSWORD``. When both values are provided a best-effort
    attempt is made to create the first account. The function returns ``True`` if
    the account is created successfully, ``False`` otherwise.
    """

    username = os.getenv("BAGHOLDER_BOOTSTRAP_USERNAME")
    password = os.getenv("BAGHOLDER_BOOTSTRAP_PASSWORD")

    if not username and not password:
        return False

    if not username or not password:
        log.warning(
            "Ignoring bootstrap request: both BAGHOLDER_BOOTSTRAP_USERNAME and "
            "BAGHOLDER_BOOTSTRAP_PASSWORD must be set."
        )
        return False

    try:
        user = bootstrap_admin(username, password, data_dir=data_dir)
    except BootstrapError as exc:
        log.info("Admin bootstrap skipped: %s", exc)
        return False
    except Exception:  # pragma: no cover - defensive logging path
        log.exception("Admin bootstrap failed due to an unexpected error")
        return False

    log.info("Administrator account '%s' created via environment bootstrap", user.username)
    return True
