from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import os

Base = declarative_base()
_engine = None
SessionLocal = None

def init_db(db_path: str):
    global _engine, SessionLocal
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    _engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)
    return _engine, SessionLocal

def get_session():
    return SessionLocal()
