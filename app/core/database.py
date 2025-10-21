import os
from typing import Generator, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

Base = declarative_base()
_engine = None
SessionLocal: Optional[sessionmaker] = None


def init_db(db_path: str):
    global _engine, SessionLocal
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    _engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)
    return _engine, SessionLocal


def get_session() -> Generator[Session, None, None]:
    if SessionLocal is None:
        raise RuntimeError("Database session factory is not initialized")
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def dispose_engine() -> None:
    """Dispose the active SQLAlchemy engine and clear the session factory.

    SQLite holds file handles open until the engine is disposed, which prevents
    deleting the database file on Windows. Explicitly disposing the engine makes
    sure the next call to :func:`init_db` recreates a fresh connection.
    """

    global _engine, SessionLocal

    if _engine is not None:
        _engine.dispose()
        _engine = None

    SessionLocal = None
