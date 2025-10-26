from sqlalchemy import text

from app.core.database import init_db
from sqlalchemy.orm import Session

from app.core.models import Base, Meta


SCHEMA_VERSION = 5


def _ensure_daily_summary_unrealized_column(engine) -> None:
    """Add the ``unrealized`` column to ``daily_summary`` if it is missing."""

    with engine.begin() as conn:
        columns = conn.execute(text("PRAGMA table_info(daily_summary)"))
        has_column = any(row[1] == "unrealized" for row in columns)
        if not has_column:
            conn.execute(
                text(
                    "ALTER TABLE daily_summary ADD COLUMN unrealized FLOAT NOT NULL DEFAULT 0.0"
                )
            )


def _ensure_daily_notes_markdown_column(engine) -> None:
    """Add the ``is_markdown`` column to ``notes_daily`` if it is missing."""

    with engine.begin() as conn:
        columns = conn.execute(text("PRAGMA table_info(notes_daily)"))
        has_column = any(row[1] == "is_markdown" for row in columns)
        if not has_column:
            conn.execute(
                text(
                    "ALTER TABLE notes_daily ADD COLUMN is_markdown INTEGER NOT NULL DEFAULT 0"
                )
            )


def _ensure_users_admin_column(engine) -> None:
    """Add the ``is_admin`` column to ``users`` and promote the first user."""

    with engine.begin() as conn:
        columns = conn.execute(text("PRAGMA table_info(users)")).fetchall()
        has_column = any(row[1] == "is_admin" for row in columns)
        if not has_column:
            conn.execute(
                text("ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0")
            )

        has_admin = conn.execute(
            text("SELECT 1 FROM users WHERE is_admin = 1 LIMIT 1")
        ).fetchone()
        if has_admin is None:
            first_user = conn.execute(
                text("SELECT id FROM users ORDER BY id ASC LIMIT 1")
            ).fetchone()
            if first_user is not None:
                conn.execute(
                    text("UPDATE users SET is_admin = 1 WHERE id = :user_id"),
                    {"user_id": first_user[0]},
                )


def ensure_seed(db_path: str):
    engine, _ = init_db(db_path)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        schema_meta = session.get(Meta, "schema_version")
        current_version = 0
        if schema_meta is None:
            schema_meta = Meta(key="schema_version", value=str(SCHEMA_VERSION))
            session.add(schema_meta)
        else:
            try:
                current_version = int(schema_meta.value)
            except (TypeError, ValueError):
                current_version = 0

        if current_version < 2:
            _ensure_daily_notes_markdown_column(engine)
            current_version = 2

        if current_version < 3:
            _ensure_users_admin_column(engine)
            current_version = 3

        if current_version < 5:
            _ensure_daily_summary_unrealized_column(engine)
            current_version = 5

        if current_version < SCHEMA_VERSION:
            schema_meta.value = str(SCHEMA_VERSION)

        if not session.get(Meta, "last_viewed_month"):
            session.add(Meta(key="last_viewed_month", value=""))

        session.commit()
