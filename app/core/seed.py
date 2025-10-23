from sqlalchemy import text

from app.core.database import init_db
from app.core.models import Base, Meta


SCHEMA_VERSION = 2


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


def ensure_seed(db_path: str):
    engine, _ = init_db(db_path)
    Base.metadata.create_all(engine)

    from sqlalchemy.orm import Session

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
            schema_meta.value = str(SCHEMA_VERSION)

        if not session.get(Meta, "last_viewed_month"):
            session.add(Meta(key="last_viewed_month", value=""))

        session.commit()
