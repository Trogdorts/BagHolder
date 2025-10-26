import csv
import io
import logging
import math
import os
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

from fastapi import APIRouter, Request, Form, Depends, Query, HTTPException
from fastapi.responses import (
    HTMLResponse,
    RedirectResponse,
    StreamingResponse,
)
from sqlalchemy import tuple_
from sqlalchemy.orm import Session
import calendar
from app.core import database
from app.core.config import AppConfig
from app.core.database import get_session
from app.core.models import (
    DailySummary,
    Meta,
    NoteDaily,
    NoteMonthly,
    NoteWeekly,
    Trade,
)
from app.core.utils import coerce_bool, month_bounds
from app.services.trade_summaries import recompute_daily_summaries
from app.services.trade_simulator import SimulationError, SimulationOptions
from app.services.simulation_runner import import_simulated_trades
from pydantic import BaseModel, Field, field_validator

router = APIRouter()

log = logging.getLogger(__name__)
class UIPreferencesUpdate(BaseModel):
    show_market_value: Optional[bool] = None
    show_total: Optional[bool] = None  # Legacy support
    show_percentages: Optional[bool] = None
    show_weekends: Optional[bool] = None
    show_exclude_controls: Optional[bool] = None
    show_trade_count: Optional[bool] = None


class TradeUpdate(BaseModel):
    id: Optional[int] = None
    symbol: str
    action: str
    qty: float
    price: float

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("Symbol is required")
        return normalized

    @field_validator("action")
    @classmethod
    def validate_action(cls, value: str) -> str:
        normalized = value.strip().upper()
        if normalized not in {"BUY", "SELL"}:
            raise ValueError("Action must be BUY or SELL")
        return normalized

    @field_validator("qty", "price", mode="before")
    @classmethod
    def validate_positive(cls, value: float) -> float:
        try:
            number = float(value)
        except (TypeError, ValueError) as exc:  # pragma: no cover - validation guard
            raise ValueError("Must be a number") from exc
        if number <= 0:
            raise ValueError("Must be greater than zero")
        return number


class TradeUpdatePayload(BaseModel):
    trades: List[TradeUpdate] = Field(default_factory=list)


class SimulationRequest(BaseModel):
    years_back: float = Field(2.0, ge=0.25, le=20)
    start_balance: float = Field(10_000.0, gt=0)
    risk_level: float = Field(0.5, gt=0, le=1)
    profit_target: float = Field(0.05, gt=0)
    stop_loss: float = Field(0.03, gt=0)
    symbol_cache: str = Field("simulator/us_symbols.csv", min_length=1)
    price_cache_dir: str = Field("simulator/price_cache", min_length=1)
    output_dir: str = Field("simulator/output", min_length=1)
    output_name: str = Field("trades.csv", min_length=1)
    seed: int = Field(42)
    generate_only: bool = False

    @field_validator("symbol_cache", "price_cache_dir", "output_dir", "output_name")
    @classmethod
    def normalize_path(cls, value: str) -> str:
        return value.strip()

    def resolve(self, base_dir: str) -> SimulationOptions:
        def _resolve(path: str, is_file: bool = False) -> str:
            candidate = path or ""
            if not candidate:
                return ""
            joined = (
                candidate
                if os.path.isabs(candidate)
                else os.path.join(base_dir, candidate)
            )
            normalized = os.path.abspath(joined)
            base = os.path.abspath(base_dir)
            if not normalized.startswith(base):
                raise ValueError(
                    "Paths must stay within the active portfolio directory."
                )
            if is_file:
                directory = os.path.dirname(normalized)
                os.makedirs(directory, exist_ok=True)
            else:
                os.makedirs(normalized, exist_ok=True)
            return normalized

        symbol_cache = _resolve(self.symbol_cache, is_file=True)
        price_cache_dir = _resolve(self.price_cache_dir)
        output_dir = _resolve(self.output_dir)
        output_name = os.path.basename(self.output_name) or "trades.csv"

        return SimulationOptions(
            years_back=self.years_back,
            start_balance=self.start_balance,
            risk_level=self.risk_level,
            profit_target=self.profit_target,
            stop_loss=self.stop_loss,
            symbol_cache=symbol_cache,
            price_cache_dir=price_cache_dir,
            output_dir=output_dir,
            output_name=output_name,
            seed=self.seed,
            generate_only=self.generate_only,
        )

@router.get("/", response_class=HTMLResponse)
def home(request: Request, db: Session = Depends(get_session)):
    # Decide which month to show
    cfg = request.app.state.config.raw
    default_view = cfg.get("view", {}).get("default", "latest")
    last_viewed = db.get(Meta, "last_viewed_month")
    today = date.today()
    if default_view == "latest" or not last_viewed or not last_viewed.value:
        y, m = today.year, today.month
    else:
        y, m = map(int, last_viewed.value.split("-"))
    return RedirectResponse(url=f"/calendar/{y}/{m}", status_code=302)

@router.get("/calendar/{year}/{month}", response_class=HTMLResponse)
def calendar_view(year: int, month: int, request: Request, db: Session = Depends(get_session)):
    # Save last viewed month
    last = db.get(Meta, "last_viewed_month")
    if last:
        last.value = f"{year}-{month}"
    else:
        db.add(Meta(key="last_viewed_month", value=f"{year}-{month}"))
    db.commit()

    cfg = request.app.state.config.raw
    ui_cfg = cfg.get("ui", {})
    show_trade_badges = coerce_bool(ui_cfg.get("show_trade_count", False), False)
    show_market_value_default = coerce_bool(
        ui_cfg.get("show_market_value", ui_cfg.get("show_total", True)), True
    )
    show_text_default = coerce_bool(ui_cfg.get("show_text", True), True)
    show_percentages_default = coerce_bool(ui_cfg.get("show_percentages", True), True)
    show_weekends_default = coerce_bool(ui_cfg.get("show_weekends", True), True)
    show_exclude_controls_default = coerce_bool(
        ui_cfg.get("show_exclude_controls", True), True
    )
    notes_cfg = cfg.get("notes", {})
    notes_enabled = coerce_bool(notes_cfg.get("enabled", True), True)
    today = date.today()

    start, end, days = month_bounds(year, month)
    month_end_date = date.fromisoformat(end)
    year_start = f"{year:04d}-01-01"
    year_end = f"{year:04d}-12-31"
    months_to_subtract = 11
    total_months = year * 12 + month - 1 - months_to_subtract
    rolling_year = total_months // 12
    rolling_month = total_months % 12 + 1
    rolling_start = f"{rolling_year:04d}-{rolling_month:02d}-01"
    rolling_start_date = date.fromisoformat(rolling_start)
    # Pull daily summaries for month
    q = db.query(DailySummary).filter(DailySummary.date >= start, DailySummary.date <= end).all()
    by_day = {r.date: r for r in q}

    note_rows = (
        db.query(NoteDaily)
        .filter(NoteDaily.date >= start, NoteDaily.date <= end)
        .all()
    )
    notes_by_day = {
        row.date: {
            "note": row.note or "",
            "updated_at": row.updated_at or "",
        }
        for row in note_rows
    }

    trade_rows = (
        db.query(Trade)
        .filter(Trade.date >= start, Trade.date <= end)
        .order_by(Trade.date.asc(), Trade.id.asc())
        .all()
    )
    trades_by_day = {}
    for tr in trade_rows:
        trades_by_day.setdefault(tr.date, []).append(
            {
                "id": tr.id,
                "symbol": tr.symbol,
                "action": tr.action,
                "qty": float(tr.qty),
                "price": float(tr.price),
            }
        )

    # Calculate weekly aggregates inline
    cal = calendar.Calendar(firstweekday=0)  # Monday=0 or Sunday=6; we'll keep 0
    weeks = []
    month_days = cal.monthdatescalendar(year, month)
    def calculate_percentage(realized_value: float, invested_samples: List[float]) -> Optional[float]:
        total_invested = sum(
            abs(sample)
            for sample in invested_samples
            if sample is not None and not math.isclose(sample, 0.0, abs_tol=0.005)
        )
        if math.isclose(total_invested, 0.0, abs_tol=0.005):
            return None
        return round((realized_value / total_invested) * 100.0, 2)

    def invested_max(rows: List[DailySummary]) -> float:
        max_value = 0.0
        for row in rows:
            try:
                magnitude = abs(float(row.total_invested))
            except (TypeError, ValueError):
                continue
            if math.isclose(magnitude, 0.0, abs_tol=0.005):
                continue
            if magnitude > max_value:
                max_value = magnitude
        return max_value

    def win_loss_counts(rows: List[DailySummary]) -> tuple[int, int]:
        wins = 0
        losses = 0
        for row in rows:
            try:
                realized_value = float(row.realized)
            except (TypeError, ValueError):
                continue
            if math.isclose(realized_value, 0.0, abs_tol=0.005):
                continue
            if realized_value > 0:
                wins += 1
            elif realized_value < 0:
                losses += 1
        return wins, losses

    def win_ratio_from_counts(wins: int, losses: int) -> Optional[float]:
        total = wins + losses
        if total == 0:
            return None
        return round((wins / total) * 100.0, 1)

    # Monthly totals
    month_realized = sum(float(r.realized) for r in q)
    month_invested_samples: List[float] = []
    for row in q:
        try:
            magnitude = abs(float(row.total_invested))
        except (TypeError, ValueError):
            continue
        if math.isclose(magnitude, 0.0, abs_tol=0.005):
            continue
        month_invested_samples.append(magnitude)
    month_invested_average = (
        sum(month_invested_samples) / len(month_invested_samples)
        if month_invested_samples
        else 0.0
    )
    month_invested_count = len(month_invested_samples)
    month_percent = calculate_percentage(
        month_realized,
        [float(r.total_invested) for r in q],
    )

    month_wins, month_losses = win_loss_counts(q)
    month_win_ratio = win_ratio_from_counts(month_wins, month_losses)

    month_year_wins = 0
    month_year_losses = 0
    for row in q:
        try:
            row_date = date.fromisoformat(row.date)
        except (TypeError, ValueError):
            continue
        if row_date.year != year:
            continue
        try:
            realized_value = float(row.realized)
        except (TypeError, ValueError):
            continue
        if math.isclose(realized_value, 0.0, abs_tol=0.005):
            continue
        if realized_value > 0:
            month_year_wins += 1
        elif realized_value < 0:
            month_year_losses += 1

    for week in month_days:
        iso_year, iso_week, _ = week[0].isocalendar()
        wk = []
        week_total_realized = 0.0
        week_invested_samples: List[float] = []
        for d in week:
            day_key = d.strftime("%Y-%m-%d")
            ds = by_day.get(day_key)
            note_entry = notes_by_day.get(day_key)
            note_text = note_entry["note"] if note_entry else ""
            note_updated_at = note_entry["updated_at"] if note_entry else ""
            is_weekend = d.weekday() >= 5
            is_future_day = d > today
            invested_value = 0.0
            if ds:
                try:
                    invested_value = float(ds.total_invested)
                except (TypeError, ValueError):
                    invested_value = 0.0
            unrealized_value = 0.0
            if ds:
                try:
                    unrealized_value = float(getattr(ds, "unrealized", 0.0))
                except (TypeError, ValueError):
                    unrealized_value = 0.0
            else:
                unrealized_value = month_invested_average if month_invested_samples else 0.0
            day_trades = trades_by_day.get(day_key, [])
            has_trades = bool(day_trades)
            has_sell_trade = any(
                (trade.get("action") or "").upper() == "SELL" for trade in day_trades
            )
            realized_value = float(ds.realized) if ds else 0.0
            show_realized = bool(ds) and (
                not math.isclose(realized_value, 0.0, abs_tol=0.005) or has_sell_trade
            )
            percent_value = (
                calculate_percentage(realized_value, [invested_value]) if ds else None
            )
            if ds:
                market_value = invested_value
            elif is_future_day:
                market_value = 0.0
            elif month_invested_samples:
                estimated_market = month_invested_average * month_invested_count
                market_value = float(round(estimated_market, -1))
            else:
                market_value = 0.0

            wk.append({
                "date": d,
                "in_month": (d.month == month),
                "realized": realized_value,
                "has_values": bool(ds),
                "invested": invested_value,
                "show_realized": show_realized,
                "percent": percent_value,
                "market_value": market_value,
                "unrealized": unrealized_value,
                "note": note_text,
                "note_updated_at": note_updated_at,
                "has_note": bool(note_text.strip()),
                "is_weekend": is_weekend,
                "trades": day_trades,
                "has_trades": has_trades,
                "in_rolling": rolling_start_date <= d <= month_end_date,
                "belongs_to_year": d.year == year,
            })
            if d.month == month and ds:
                week_total_realized += float(ds.realized)
                week_invested_samples.append(invested_value)
        week_percent = calculate_percentage(week_total_realized, week_invested_samples)
        weeks.append({
            "days": wk,
            "week_realized": week_total_realized,
            "week_index": len(weeks) + 1,
            "week_year": iso_year,
            "week_number": iso_week,
            "show_week_realized": not math.isclose(week_total_realized, 0.0, abs_tol=0.005),
            "note": "",
            "note_updated_at": "",
            "has_note": False,
            "week_percent": week_percent,
        })

    week_pairs = {(week_entry["week_year"], week_entry["week_number"]) for week_entry in weeks}
    weekly_note_rows = []
    if week_pairs:
        weekly_note_rows = (
            db.query(NoteWeekly)
            .filter(tuple_(NoteWeekly.year, NoteWeekly.week).in_(list(week_pairs)))
            .all()
        )
    weekly_notes = {
        (row.year, row.week): {
            "note": row.note or "",
            "updated_at": row.updated_at or "",
        }
        for row in weekly_note_rows
    }
    for week_entry in weeks:
        data = weekly_notes.get((week_entry["week_year"], week_entry["week_number"]))
        note_text = data["note"] if data else ""
        week_entry["note"] = note_text
        week_entry["note_updated_at"] = data["updated_at"] if data else ""
        week_entry["has_note"] = bool(note_text.strip())

    month_note_row = (
        db.query(NoteMonthly)
        .filter(NoteMonthly.year == year, NoteMonthly.month == month)
        .first()
    )
    month_note = {
        "note": month_note_row.note if month_note_row and month_note_row.note else "",
        "updated_at": month_note_row.updated_at if month_note_row and month_note_row.updated_at else "",
    }

    # Yearly totals
    year_rows = (
        db.query(DailySummary)
        .filter(DailySummary.date >= year_start, DailySummary.date <= year_end)
        .all()
    )
    year_realized = sum(float(r.realized) for r in year_rows)
    year_trading_days = sum(1 for r in year_rows if r)
    year_percent = calculate_percentage(
        year_realized,
        [float(r.total_invested) for r in year_rows],
    )

    year_wins_total, year_losses_total = win_loss_counts(year_rows)
    year_win_ratio = win_ratio_from_counts(year_wins_total, year_losses_total)
    year_other_wins = max(year_wins_total - month_year_wins, 0)
    year_other_losses = max(year_losses_total - month_year_losses, 0)

    # Rolling 12 month totals ending at the current month
    rolling_rows = (
        db.query(DailySummary)
        .filter(DailySummary.date >= rolling_start, DailySummary.date <= end)
        .all()
    )
    rolling_realized = sum(float(r.realized) for r in rolling_rows)
    rolling_trading_days = sum(1 for r in rolling_rows if r)
    rolling_year_percent = calculate_percentage(
        rolling_realized,
        [float(r.total_invested) for r in rolling_rows],
    )

    year_other_rows = []
    rolling_other_rows = []
    for row in year_rows:
        row_date = date.fromisoformat(row.date)
        if row_date.year != year or row_date.month != month:
            year_other_rows.append(row)
    for row in rolling_rows:
        row_date = date.fromisoformat(row.date)
        if row_date.year != year or row_date.month != month:
            rolling_other_rows.append(row)

    year_other_invested_max = invested_max(year_other_rows)
    rolling_other_invested_max = invested_max(rolling_other_rows)

    account_dir = getattr(request.app.state, "account_data_dir", "")
    simulation_defaults = {
        "years_back": 2,
        "start_balance": 10_000.0,
        "risk_level": 0.5,
        "profit_target": 0.05,
        "stop_loss": 0.03,
        "symbol_cache": "simulator/us_symbols.csv",
        "price_cache_dir": "simulator/price_cache",
        "output_dir": "simulator/output",
        "output_name": "trades.csv",
        "seed": 42,
        "generate_only": False,
    }

    ctx = {
        "request": request,
        "year": year, "month": month,
        "weeks": weeks,
        "month_note": month_note,
        "month_realized": month_realized,
        "month_percent": month_percent,
        "year_realized": year_realized,
        "year_trading_days": year_trading_days,
        "year_percent": year_percent,
        "year_win_ratio": year_win_ratio,
        "year_wins": year_wins_total,
        "year_losses": year_losses_total,
        "rolling_year_realized": rolling_realized,
        "rolling_year_trading_days": rolling_trading_days,
        "rolling_year_percent": rolling_year_percent,
        "year_other_invested_max": year_other_invested_max,
        "rolling_year_other_invested_max": rolling_other_invested_max,
        "month_win_ratio": month_win_ratio,
        "month_wins": month_wins,
        "month_losses": month_losses,
        "month_year_wins": month_year_wins,
        "month_year_losses": month_year_losses,
        "year_other_wins": year_other_wins,
        "year_other_losses": year_other_losses,
        "cfg": request.app.state.config.raw,
        "show_trade_badges": show_trade_badges,
        "show_market_value_flag": show_market_value_default,
        "show_text_flag": show_text_default,
        "show_percentages_flag": show_percentages_default,
        "show_weekends_flag": show_weekends_default,
        "show_exclude_controls_flag": show_exclude_controls_default,
        "notes_enabled_flag": notes_enabled,
        "current_year": today.year,
        "current_month": today.month,
        "simulation_defaults": simulation_defaults,
        "simulation_account_path": account_dir,
    }
    return request.app.state.templates.TemplateResponse(
        request,
        "calendar.html",
        ctx,
    )


@router.post("/api/ui/preferences")
def update_ui_preferences(payload: UIPreferencesUpdate, request: Request):
    cfg: AppConfig = request.app.state.config
    ui_section = cfg.raw.setdefault("ui", {})

    updates: Dict[str, bool] = {}
    for field, key in (
        ("show_market_value", "show_market_value"),
        ("show_percentages", "show_percentages"),
        ("show_weekends", "show_weekends"),
        ("show_exclude_controls", "show_exclude_controls"),
        ("show_trade_count", "show_trade_count"),
    ):
        value = getattr(payload, field)
        if value is not None:
            ui_section[key] = bool(value)
            updates[key] = bool(value)

    if payload.show_total is not None and "show_market_value" not in updates:
        ui_section["show_market_value"] = bool(payload.show_total)
        updates["show_market_value"] = bool(payload.show_total)

    if "show_total" in ui_section:
        ui_section.pop("show_total")

    if not updates:
        raise HTTPException(status_code=400, detail="No preferences provided.")

    cfg.save()
    request.app.state.templates.env.globals["cfg"] = cfg.raw

    return {"status": "ok", "preferences": updates}


@router.get("/api/trades/{date_str}")
def get_trades_for_day(date_str: str, db: Session = Depends(get_session)):
    trades = (
        db.query(Trade)
        .filter(Trade.date == date_str)
        .order_by(Trade.id.asc())
        .all()
    )
    return {
        "trades": [
            {
                "id": trade.id,
                "symbol": trade.symbol,
                "action": trade.action,
                "qty": float(trade.qty),
                "price": float(trade.price),
            }
            for trade in trades
        ]
    }


@router.post("/api/trades/{date_str}")
def save_trades_for_day(
    date_str: str,
    payload: TradeUpdatePayload,
    db: Session = Depends(get_session),
):
    existing = (
        db.query(Trade)
        .filter(Trade.date == date_str)
        .order_by(Trade.id.asc())
        .all()
    )
    existing_map = {trade.id: trade for trade in existing}
    seen_ids = set()
    had_existing_trades = bool(existing)

    for trade_update in payload.trades:
        trade = None
        if trade_update.id is not None:
            trade = existing_map.get(trade_update.id)
            if trade is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Trade {trade_update.id} was not found for {date_str}.",
                )
            seen_ids.add(trade_update.id)

        amount = trade_update.qty * trade_update.price
        signed_amount = amount if trade_update.action == "SELL" else -amount

        if trade is not None:
            trade.symbol = trade_update.symbol
            trade.action = trade_update.action
            trade.qty = trade_update.qty
            trade.price = trade_update.price
            trade.amount = signed_amount
        else:
            db.add(
                Trade(
                    date=date_str,
                    symbol=trade_update.symbol,
                    action=trade_update.action,
                    qty=trade_update.qty,
                    price=trade_update.price,
                    amount=signed_amount,
                )
            )

    for trade in existing:
        if trade.id not in seen_ids:
            db.delete(trade)

    db.flush()

    remaining = (
        db.query(Trade)
        .filter(Trade.date == date_str)
        .count()
    )
    if remaining == 0 and had_existing_trades:
        summary_row = db.get(DailySummary, date_str)
        if summary_row:
            db.delete(summary_row)

    db.flush()

    daily_map = recompute_daily_summaries(db)
    updated = (
        db.query(Trade)
        .filter(Trade.date == date_str)
        .order_by(Trade.id.asc())
        .all()
    )
    response_trades = [
        {
            "id": trade.id,
            "symbol": trade.symbol,
            "action": trade.action,
            "qty": float(trade.qty),
            "price": float(trade.price),
        }
        for trade in updated
    ]

    db.commit()

    return {
        "ok": True,
        "trades": response_trades,
        "summary": daily_map.get(date_str),
    }


@router.post("/api/simulated-trades")
def generate_simulated_trades(request: Request, payload: SimulationRequest):
    account_dir = getattr(request.app.state, "account_data_dir", None)
    if not account_dir:
        raise HTTPException(
            status_code=500,
            detail="The active portfolio directory could not be determined.",
        )

    try:
        options = payload.resolve(account_dir)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        cfg: AppConfig = request.app.state.config
        data_dir = os.path.dirname(cfg.path) if cfg.path else None
        result = import_simulated_trades(
            request.app,
            account_dir,
            data_dir,
            options,
        )
    except SimulationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:  # pragma: no cover - defensive guard
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive logging
        log.exception("Unexpected error while generating simulated trades")
        raise HTTPException(
            status_code=500,
            detail="Failed to generate simulated trades.",
        ) from exc

    return result


@router.delete("/api/trades/{date_str}")
def clear_trades_for_day(date_str: str, db: Session = Depends(get_session)):
    trades = (
        db.query(Trade)
        .filter(Trade.date == date_str)
        .order_by(Trade.id.asc())
        .all()
    )
    deleted = 0
    for trade in trades:
        db.delete(trade)
        deleted += 1

    summary_row = db.get(DailySummary, date_str)
    if summary_row is not None:
        db.delete(summary_row)

    db.flush()

    daily_map = recompute_daily_summaries(db)

    db.commit()

    return {
        "ok": True,
        "deleted": deleted,
        "trades": [],
        "summary": daily_map.get(date_str),
    }


@router.post("/api/daily/{date_str}")
def overwrite_daily(
    date_str: str,
    realized: float = Form(...),
    invested: float = Form(...),
    db: Session = Depends(get_session),
):
    from app.core.models import DailySummary
    now = datetime.utcnow().isoformat()
    ds = db.get(DailySummary, date_str)
    if ds:
        ds.realized = realized
        ds.total_invested = invested
        ds.updated_at = now
    else:
        db.add(
            DailySummary(
                date=date_str,
                realized=realized,
                total_invested=invested,
                updated_at=now,
            )
        )
    db.commit()
    return {"ok": True}


@router.get("/export")
def export_data(
    request: Request,
    start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    dataset: str = Query(
        "summaries",
        description="The dataset to export (summaries, trades, or notes)",
    ),
    db: Session = Depends(get_session),
):
    fmt = "%Y-%m-%d"
    today = date.today()

    cfg = getattr(request.app.state, "config", None)
    fill_empty_with_zero = True
    if cfg is not None:
        fill_empty_with_zero = cfg.raw.get("export", {}).get("fill_empty_with_zero", True)

    def parse(value: Optional[str], fallback: date) -> date:
        if value:
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.") from exc
        return fallback

    end_dt = parse(end, today)
    start_dt = parse(start, end_dt - timedelta(days=30))

    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    start_str = start_dt.strftime(fmt)
    end_str = end_dt.strftime(fmt)

    dataset_value = dataset
    if not isinstance(dataset_value, str):  # When called directly in tests
        dataset_value = getattr(dataset_value, "default", "summaries")
    dataset_key = (dataset_value or "").strip().lower()
    if dataset_key not in {"summaries", "trades"}:
        raise HTTPException(status_code=400, detail="Unknown export dataset")

    if dataset_key == "summaries":
        summaries = (
            db.query(DailySummary)
            .filter(DailySummary.date >= start_str, DailySummary.date <= end_str)
            .order_by(DailySummary.date.asc())
            .all()
        )

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["date", "realized", "total_invested"])

        def format_value(value: Optional[float]) -> str:
            if value is None:
                return "0.00" if fill_empty_with_zero else ""
            if isinstance(value, str):
                stripped = value.strip()
                if not stripped:
                    return "0.00" if fill_empty_with_zero else ""
                candidate = stripped
            else:
                candidate = value
            try:
                number = float(candidate)
            except (TypeError, ValueError):
                return "0.00" if fill_empty_with_zero else ""
            if math.isnan(number):  # pragma: no cover - defensive guard
                return "0.00" if fill_empty_with_zero else ""
            return f"{number:.2f}"

        for summary in summaries:
            writer.writerow(
                [
                    summary.date,
                    format_value(summary.realized),
                    format_value(summary.total_invested),
                ]
            )

        filename = f"bagholder_export_{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}.csv"
        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        content = buffer.getvalue().encode("utf-8")
        return StreamingResponse(iter([content]), media_type="text/csv", headers=headers)

    if dataset_key == "trades":
        trades = (
            db.query(Trade)
            .filter(Trade.date >= start_str, Trade.date <= end_str)
            .order_by(Trade.date.asc(), Trade.id.asc())
            .all()
        )

        notes_by_date = {
            note.date: note.note or ""
            for note in (
                db.query(NoteDaily)
                .filter(NoteDaily.date >= start_str, NoteDaily.date <= end_str)
                .all()
            )
        }

        def _format_decimal(value: Optional[float], precision: int = 2) -> str:
            if value is None:
                return ""
            text = f"{float(value):.{precision}f}"
            text = text.rstrip("0").rstrip(".")
            return text or "0"

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["date", "symbol", "action", "qty", "price", "amount", "notes"])
        for trade in trades:
            writer.writerow(
                [
                    trade.date,
                    trade.symbol,
                    trade.action,
                    _format_decimal(trade.qty, precision=8),
                    _format_decimal(trade.price, precision=4),
                    _format_decimal(trade.amount, precision=4),
                    notes_by_date.get(trade.date, ""),
                ]
            )

        filename = f"bagholder_trades_{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}.csv"
        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        content = buffer.getvalue().encode("utf-8")
        return StreamingResponse(iter([content]), media_type="text/csv", headers=headers)
