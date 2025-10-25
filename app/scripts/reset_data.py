"""Utility to wipe the application's data directory for manual testing."""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

from app.core.config import AppConfig
from app.core.lifecycle import get_default_data_dir


def _resolve_data_dir(explicit: str | None) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    env_value = os.environ.get("BAGHOLDER_DATA")
    if env_value:
        return Path(env_value).expanduser().resolve()
    return Path(get_default_data_dir()).resolve()


def _validate_target(path: Path) -> None:
    if path == Path("/"):
        raise ValueError("Refusing to erase the filesystem root")
    if path.is_symlink():
        raise ValueError("Data directory cannot be a symbolic link")


def reset_data_directory(target: Path) -> None:
    """Remove ``target`` and recreate an empty data directory."""

    _validate_target(target)

    if target.exists():
        if not target.is_dir():
            raise ValueError(f"Data directory {target} is not a directory")
        shutil.rmtree(target)

    target.mkdir(parents=True, exist_ok=True)
    AppConfig.load(str(target))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Reset BagHolder data directory")
    parser.add_argument(
        "--data-dir",
        help="Override the data directory to wipe. Defaults to BAGHOLDER_DATA or the project data folder.",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Do not prompt before deleting the directory.",
    )
    args = parser.parse_args(argv)

    target = _resolve_data_dir(args.data_dir)

    if not args.force:
        answer = input(f"This will permanently delete all data under {target}. Continue? [y/N] ")
        if answer.strip().lower() not in {"y", "yes"}:
            print("Aborted.")
            return 1

    try:
        reset_data_directory(target)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    print(f"Data directory reset. Fresh files will be created in {target} on next startup.")
    return 0


if __name__ == "__main__":  # pragma: no cover - manual entry point
    raise SystemExit(main())
