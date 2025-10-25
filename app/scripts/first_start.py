"""CLI utility for creating the initial administrator account."""

from __future__ import annotations

import argparse
import getpass
import sys
from typing import Iterable

from app.core.bootstrap import BootstrapError, bootstrap_admin


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
    except BootstrapError as exc:
        print(f"Failed to create administrator account: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # pragma: no cover - unexpected error path
        print(f"Failed to create administrator account: {exc}", file=sys.stderr)
        return 1

    print(f"Administrator account '{user.username}' created successfully.")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
