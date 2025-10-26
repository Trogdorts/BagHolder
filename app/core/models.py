from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, Integer, Float, Text, UniqueConstraint, Boolean, DateTime
from sqlalchemy.sql import func
from sqlalchemy.types import Date
from sqlalchemy.orm import DeclarativeBase
from datetime import date, datetime

class Base(DeclarativeBase):
    pass

class Trade(Base):
    __tablename__ = "trades"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[str] = mapped_column(String, index=True)  # YYYY-MM-DD
    symbol: Mapped[str] = mapped_column(String, index=True)
    action: Mapped[str] = mapped_column(String)  # BUY/SELL
    qty: Mapped[float] = mapped_column(Float)
    price: Mapped[float] = mapped_column(Float)
    amount: Mapped[float] = mapped_column(Float)
    # Composite uniqueness to prevent duplicate imports
    __table_args__ = (UniqueConstraint("date", "symbol", "action", "qty", "price", "amount", name="uix_trade_dedup"),)

class DailySummary(Base):
    __tablename__ = "daily_summary"
    date: Mapped[str] = mapped_column(String, primary_key=True)  # YYYY-MM-DD
    realized: Mapped[float] = mapped_column(Float, default=0.0)
    unrealized: Mapped[float] = mapped_column(Float, default=0.0)
    total_invested: Mapped[float] = mapped_column(Float, default=0.0)
    updated_at: Mapped[str] = mapped_column(String, default="", nullable=False)

class Meta(Base):
    __tablename__ = "meta"
    key: Mapped[str] = mapped_column(String, primary_key=True)
    value: Mapped[str] = mapped_column(String)

class NoteDaily(Base):
    __tablename__ = "notes_daily"
    date: Mapped[str] = mapped_column(String, primary_key=True)
    note: Mapped[str] = mapped_column(Text, default="")
    is_markdown: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    updated_at: Mapped[str] = mapped_column(String, default="")

class NoteWeekly(Base):
    __tablename__ = "notes_weekly"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    year: Mapped[int] = mapped_column(Integer, index=True)
    week: Mapped[int] = mapped_column(Integer, index=True)
    note: Mapped[str] = mapped_column(Text, default="")
    updated_at: Mapped[str] = mapped_column(String, default="")
    __table_args__ = (UniqueConstraint("year", "week", name="uix_week"),)

class NoteMonthly(Base):
    __tablename__ = "notes_monthly"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    year: Mapped[int] = mapped_column(Integer, index=True)
    month: Mapped[int] = mapped_column(Integer, index=True)
    note: Mapped[str] = mapped_column(Text, default="")
    updated_at: Mapped[str] = mapped_column(String, default="")
    __table_args__ = (UniqueConstraint("year", "month", name="uix_month"),)


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(150), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(256), nullable=False)
    password_salt: Mapped[str] = mapped_column(String(256), nullable=False)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
