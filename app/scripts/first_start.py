"""CLI utility for creating the initial administrator account."""

from __future__ import annotations

import argparse
import getpass
import os
import sys
from typing import Iterable, cast

from sqlalchemy.orm import Session

from app.core.auth import hash_password
from app.core.config import AppConfig
from app.core.database import dispose_engine, init_db
from app.core.models import User
from app.core.seed import ensure_seed
from app.services.accounts import prepare_accounts


def bootstrap_admin(username: str, password: str, *, data_dir: str | None = None) -> User:
    """Create the first user account with administrator privileges."""

    data_dir = data_dir or os.environ.get("BAGHOLDER_DATA", "/app/data")
    if not isinstance(username, str) or not username.strip():
        raise ValueError("Username is required")
    if not isinstance(password, str) or len(password) < 8:
        raise ValueError("Password must be at least 8 characters long")

    os.makedirs(data_dir, exist_ok=True)

    cfg = AppConfig.load(data_dir)
    _, active_account = prepare_accounts(cfg, data_dir)
    db_path = os.path.join(active_account.path, "profitloss.db")

    ensure_seed(db_path)
    _, session_factory = init_db(db_path)

    normalized_username = username.strip().lower()
    try:
        with session_factory() as session:
            session = cast(Session, session)
            existing_users = session.query(User).count()
            if existing_users > 0:
                raise RuntimeError("An account already exists. Use the web UI to manage users.")

            salt, password_hash = hash_password(password)
            user = User(
                username=normalized_username,
                password_hash=password_hash,
                password_salt=salt,
                is_admin=True,
            )
            session.add(user)
            session.commit()
            session.refresh(user)
            return user
    finally:
        dispose_engine()


def _prompt(prompt: str) -> str:
    value = input(prompt)
    return value.strip()


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Bootstrap the BagHolder administrator account."
    )
    parser.add_argument("--username", "-u", help="Username for the administrator account.")
    parser.add_argument(
        "--password",
        "-p",
        help="Password for the administrator account. Provide interactively if omitted.",
    )
    parser.add_argument(
        "--data-dir",
        "-d",
        help="Directory containing BagHolder data. Defaults to $BAGHOLDER_DATA or /app/data.",
    )

    args = parser.parse_args(list(argv) if argv is not None else None)

    username = args.username or _prompt("Administrator username: ")
    if not username:
        print("Username is required.", file=sys.stderr)
        return 1

    password = args.password
    if not password:
        password = getpass.getpass("Administrator password: ")
        confirm = getpass.getpass("Confirm password: ")
        if password != confirm:
            print("Passwords do not match.", file=sys.stderr)
            return 1

    try:
        user = bootstrap_admin(username, password, data_dir=args.data_dir)
    except Exception as exc:  # pragma: no cover - CLI error path
        print(f"Failed to create administrator account: {exc}", file=sys.stderr)
        return 1

    print(f"Administrator account '{user.username}' created successfully.")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
