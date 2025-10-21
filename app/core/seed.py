import os, time
from datetime import datetime
from sqlalchemy import select
from .database import init_db
from .models import Base, Meta

def ensure_seed(db_path: str):
    engine, _ = init_db(db_path)
    Base.metadata.create_all(engine)
    # Seed meta
    from sqlalchemy.orm import Session
    with Session(engine) as s:
        if not s.get(Meta, "schema_version"):
            s.add(Meta(key="schema_version", value="1"))
        if not s.get(Meta, "last_viewed_month"):
            s.add(Meta(key="last_viewed_month", value=""))
        s.commit()
