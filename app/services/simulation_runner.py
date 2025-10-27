"""Utilities for executing trade simulations within a portfolio."""

from __future__ import annotations

import os
from datetime import datetime
import re
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Tuple

from fastapi import FastAPI

from app.core import database
from app.core.config import AppConfig
from app.core.lifecycle import reload_application_state
from app.core.models import NoteDaily, Trade
from app.services.data_reset import clear_all_data
from app.services.trade_simulator import (
    SimulationError,
    SimulationOptions,
    run_trade_simulation,
)
from app.services.trade_summaries import recompute_daily_summaries


def build_default_simulation_options(account_dir: str) -> SimulationOptions:
    """Return simulation options using the stock defaults for ``account_dir``."""

    simulator_root = os.path.join(account_dir, "simulator")
    symbol_cache = os.path.join(simulator_root, "us_symbols.csv")
    price_cache_dir = os.path.join(simulator_root, "price_cache")
    output_dir = os.path.join(simulator_root, "output")

    os.makedirs(os.path.dirname(symbol_cache), exist_ok=True)
    os.makedirs(price_cache_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    return SimulationOptions(
        years_back=2.0,
        months_back=24,
        start_balance=10_000.0,
        risk_level=0.5,
        profit_target=0.05,
        stop_loss=0.03,
        symbol_cache=symbol_cache,
        price_cache_dir=price_cache_dir,
        output_dir=output_dir,
        output_name="trades.csv",
        seed=42,
        generate_only=False,
    )


_NOTE_PREFIX_PATTERN = re.compile(r"^\[\s*(BUY|SELL)\b", re.IGNORECASE)


def _format_trade_note(action: str, qty: float, price: float, note: str) -> str:
    action_label = action.strip().upper() or "BUY"
    qty_value = float(qty)
    if qty_value.is_integer():
        qty_text = str(int(qty_value))
    else:
        qty_text = f"{qty_value:.2f}".rstrip("0").rstrip(".")
    price_text = f"{float(price):.2f}"
    prefix = f"[ {action_label} - {qty_text} x ${price_text} ]"
    cleaned_note = (note or "").replace("\r\n", "\n").strip()
    if not cleaned_note:
        return prefix
    if _NOTE_PREFIX_PATTERN.match(cleaned_note):
        return cleaned_note
    return f"{prefix} {cleaned_note}"


def _prepare_trade_records(
    records: Iterable[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], Dict[str, List[str]]]:
    prepared: list[Dict[str, Any]] = []
    note_lines_by_date: Dict[str, List[str]] = {}
    for row in records:
        raw_date = str(row.get("date", "")).strip()
        try:
            iso_date = datetime.strptime(raw_date, "%m/%d/%Y").strftime("%Y-%m-%d")
        except ValueError as exc:  # pragma: no cover - defensive parsing
            raise SimulationError(f"Simulator returned an invalid date: {raw_date}") from exc

        symbol = str(row.get("symbol", "")).strip().upper()
        action = str(row.get("action", "")).strip().upper()
        try:
            qty = float(row.get("qty", 0.0))
            price = float(row.get("price", 0.0))
            amount = float(row.get("amount", 0.0))
        except (TypeError, ValueError) as exc:  # pragma: no cover - defensive parsing
            raise SimulationError("Simulator produced an invalid trade record.") from exc

        if not symbol or action not in {"BUY", "SELL"} or qty <= 0 or price <= 0:
            raise SimulationError("Simulator produced an invalid trade record.")

        prepared.append(
            {
                "date": iso_date,
                "symbol": symbol,
                "action": action,
                "qty": qty,
                "price": price,
                "amount": amount,
            }
        )

        raw_note = row.get("notes") if "notes" in row else row.get("note")
        note_text = str(raw_note or "").strip()
        if note_text:
            formatted_note = _format_trade_note(action, qty, price, note_text)
            note_lines_by_date.setdefault(iso_date, []).append(formatted_note)

    return prepared, note_lines_by_date


def import_simulated_trades(
    app: FastAPI,
    account_dir: str,
    base_data_dir: str | None,
    options: SimulationOptions,
) -> Dict[str, Any]:
    """Execute a simulation and import the resulting trades into ``account_dir``."""

    result = run_trade_simulation(options)
    metadata = dict(result.metadata)

    if options.generate_only:
        metadata.setdefault("status", "cache_updated")
        return {
            "ok": True,
            "generate_only": True,
            "metadata": metadata,
            "message": "Symbol and price caches have been updated.",
            "reload": False,
        }

    if result.trades.empty:
        raise SimulationError("The simulator did not return any trades to import.")

    prepared, note_lines_by_date = _prepare_trade_records(
        result.trades.to_dict("records")
    )

    clear_all_data(account_dir)

    if database.SessionLocal is None:
        raise RuntimeError(
            "Database session factory is unavailable after reset."
        )

    with database.SessionLocal() as session:
        sequence_by_date: Dict[str, int] = defaultdict(int)
        for trade in prepared:
            sequence = sequence_by_date[trade["date"]]
            sequence_by_date[trade["date"]] = sequence + 1
            session.add(
                Trade(
                    date=trade["date"],
                    symbol=trade["symbol"],
                    action=trade["action"],
                    qty=trade["qty"],
                    price=trade["price"],
                    amount=trade["amount"],
                    time="",
                    fee=0.0,
                    sequence=sequence,
                )
            )
        if note_lines_by_date:
            timestamp = datetime.utcnow().isoformat()
            for date_str in sorted(note_lines_by_date):
                note_text = "\n\n".join(note_lines_by_date[date_str])
                record = session.get(NoteDaily, date_str)
                if record:
                    existing = (record.note or "").rstrip()
                    record.note = (
                        f"{existing}\n\n{note_text}".strip() if existing else note_text
                    )
                    record.is_markdown = False
                    record.updated_at = timestamp
                else:
                    session.add(
                        NoteDaily(
                            date=date_str,
                            note=note_text,
                            is_markdown=False,
                            updated_at=timestamp,
                        )
                    )
        session.flush()
        method = _resolve_method_from_app(app)
        recompute_daily_summaries(session, method=method)
        session.commit()

    reload_application_state(app, data_dir=base_data_dir)

    unique_days = {trade["date"] for trade in prepared}
    metadata.update(
        {
            "status": "trades_imported",
            "trades_imported": len(prepared),
            "days_with_trades": len(unique_days),
        }
    )

    return {
        "ok": True,
        "generate_only": False,
        "metadata": metadata,
        "trades_imported": len(prepared),
        "days_with_trades": len(unique_days),
        "reload": True,
    }
def _resolve_method_from_app(app: FastAPI | None) -> str:
    if app is None:
        return "fifo"
    cfg = getattr(app.state, "config", None)
    if isinstance(cfg, AppConfig):
        try:
            method = cfg.raw.get("trades", {}).get("pnl_method", "fifo")
        except AttributeError:  # pragma: no cover - defensive guard
            return "fifo"
        if isinstance(method, str) and method.strip().lower() == "lifo":
            return "lifo"
    return "fifo"
