"""Utilities for executing trade simulations within a portfolio."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, Iterable

from fastapi import FastAPI

from app.core import database
from app.core.lifecycle import reload_application_state
from app.core.models import Trade
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


def _prepare_trade_records(records: Iterable[Dict[str, Any]]) -> list[Dict[str, Any]]:
    prepared: list[Dict[str, Any]] = []
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
    return prepared


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

    prepared = _prepare_trade_records(result.trades.to_dict("records"))

    clear_all_data(account_dir)

    if database.SessionLocal is None:
        raise RuntimeError(
            "Database session factory is unavailable after reset."
        )

    with database.SessionLocal() as session:
        for trade in prepared:
            session.add(
                Trade(
                    date=trade["date"],
                    symbol=trade["symbol"],
                    action=trade["action"],
                    qty=trade["qty"],
                    price=trade["price"],
                    amount=trade["amount"],
                )
            )
        session.flush()
        recompute_daily_summaries(session)
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
