"""Utilities for clearing user data stored in the application database."""

from __future__ import annotations

import os
from contextlib import suppress

from sqlalchemy.orm.session import close_all_sessions

from app.core.database import SessionLocal, dispose_engine, init_db
from app.core.seed import ensure_seed


def clear_all_data(data_dir: str) -> None:
    """Remove all persisted trading and note data.

    Parameters
    ----------
    data_dir:
        The directory where the application's database is stored.

    The function closes any existing database sessions, deletes the SQLite
    database file if present, and re-seeds the database so the application can
    continue operating with a clean state.
    """

    os.makedirs(data_dir, exist_ok=True)
    db_path = os.path.join(data_dir, "profitloss.db")

    if SessionLocal is not None:
        # Ensure any live sessions release their file handles before deletion.
        close_all_sessions()

    # Dispose the active engine so SQLite releases the file handle before we
    # attempt to remove the database file.
    dispose_engine()

    if os.path.exists(db_path):
        with suppress(OSError):
            os.remove(db_path)

    # Recreate the database schema and seed meta information.
    ensure_seed(db_path)
    # ensure_seed already re-initialises the session factory via init_db, but
    # calling init_db here makes the intent explicit and guarantees future
    # changes keep SessionLocal pointing at the new database file.
    init_db(db_path)
