"""Utilities for creating and restoring full application backups."""

from __future__ import annotations

import io
import os
import shutil
import zipfile
from typing import Iterable

from sqlalchemy.orm.session import close_all_sessions

from app.core.database import SessionLocal, dispose_engine


def _iter_files(base_dir: str) -> Iterable[tuple[str, str]]:
    """Yield filesystem paths and their archive names for ``base_dir``.

    Parameters
    ----------
    base_dir:
        Directory whose contents should be added to the backup archive.

    Yields
    ------
    tuple[str, str]
        Tuples containing the absolute file path and the relative archive name.
    """

    for root, _, files in os.walk(base_dir):
        for filename in files:
            absolute_path = os.path.join(root, filename)
            archive_name = os.path.relpath(absolute_path, base_dir)
            yield absolute_path, archive_name


def create_backup_archive(data_dir: str) -> bytes:
    """Create a ZIP archive containing all persisted application data.

    Parameters
    ----------
    data_dir:
        Directory containing BagHolder's persisted state (database, config, notes).

    Returns
    -------
    bytes
        The binary contents of the generated ZIP archive.
    """

    data_dir = os.path.abspath(data_dir)
    os.makedirs(data_dir, exist_ok=True)

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for absolute_path, archive_name in _iter_files(data_dir):
            archive.write(absolute_path, arcname=archive_name)

    buffer.seek(0)
    return buffer.read()


def _ensure_within_directory(base_dir: str, target_path: str) -> bool:
    """Return ``True`` if ``target_path`` is contained within ``base_dir``."""

    base_dir = os.path.abspath(base_dir)
    target_path = os.path.abspath(target_path)
    return os.path.commonpath([base_dir]) == os.path.commonpath([base_dir, target_path])


def restore_backup_archive(data_dir: str, archive_bytes: bytes) -> None:
    """Replace ``data_dir`` contents with those stored in ``archive_bytes``.

    The function validates archive paths to prevent directory traversal, clears
    the existing data directory, and extracts the provided backup. Any open
    database sessions are closed before files are replaced so SQLite releases
    file handles on Windows.
    """

    data_dir = os.path.abspath(data_dir)
    os.makedirs(data_dir, exist_ok=True)

    try:
        zip_file = zipfile.ZipFile(io.BytesIO(archive_bytes))
    except zipfile.BadZipFile:
        raise

    with zip_file as archive:
        for member in archive.infolist():
            # Directory entries are allowed, but we still normalise their paths.
            normalized = os.path.normpath(member.filename)
            if normalized in {"", "."}:
                continue
            if normalized.startswith("..") or os.path.isabs(normalized):
                raise ValueError("Archive contains unsafe paths")
            destination = os.path.join(data_dir, normalized)
            if not _ensure_within_directory(data_dir, destination):
                raise ValueError("Archive contains unsafe paths")

        # Ensure SQLite releases file handles before deleting the database.
        if SessionLocal is not None:
            close_all_sessions()
        dispose_engine()

        # Remove existing contents so the backup fully replaces on-disk state.
        for entry in os.listdir(data_dir):
            path = os.path.join(data_dir, entry)
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)

        archive.extractall(data_dir)
