"""Utilities for calculating profit and loss summaries."""
from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from app.services.trade_matching import apply_trade, create_position


def compute_daily_pnl_records(
    records: List[Dict[str, Any]], *, method: str = "fifo"
) -> pd.DataFrame:
    """Aggregate trade records into daily profit/loss totals.

    Parameters
    ----------
    records:
        Iterable of trade dictionaries containing at least ``date``, ``side``,
        ``symbol``, ``quantity`` and ``price`` keys. Optional ``fee`` and
        ``sequence`` fields refine processing order.
    method:
        Matching algorithm passed to :func:`app.services.trade_matching.apply_trade`.

    Returns
    -------
    pandas.DataFrame
        DataFrame with ``date``, ``realized_pl``, ``trade_value`` and
        ``cumulative_pl`` columns summarizing daily results.
    """

    empty_df = pd.DataFrame(
        columns=[
            "date",
            "realized_pl",
            "trade_value",
            "total_pl",
            "cumulative_pl",
        ]
    )

    if not records:
        return empty_df

    df = pd.DataFrame(records)
    if df.empty:
        return empty_df

    if "date" not in df.columns or "side" not in df.columns:
        raise ValueError("records require 'date' and 'side' fields")

    required = {"symbol", "quantity", "price"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"records missing required fields: {', '.join(missing)}")

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["side"] = df["side"].str.upper()
    df["symbol"] = df["symbol"].str.upper()
    if "sequence" not in df.columns:
        df["sequence"] = 0
    else:
        df["sequence"] = (
            pd.to_numeric(df["sequence"], errors="coerce").fillna(0).astype(int)
        )
    if "datetime" in df.columns:
        df["_trade_dt"] = pd.to_datetime(df["datetime"], errors="coerce")
    else:
        df["_trade_dt"] = pd.NaT

    df = df.sort_values(["date", "sequence", "_trade_dt", "symbol"]).reset_index(
        drop=True
    )

    positions: Dict[str, Dict[str, Any]] = {}
    daily_records: List[Dict[str, Any]] = []

    for date_value, day_trades in df.groupby("date", sort=True):
        realized_total = 0.0
        trade_value_total = 0.0

        for trade in day_trades.itertuples(index=False):
            side = trade.side
            symbol = trade.symbol
            qty = float(trade.quantity)
            price = float(trade.price)
            if qty <= 0 or price is None:
                continue

            if side not in {"BUY", "SELL"}:
                continue

            position = positions.setdefault(symbol, create_position())
            fee = float(getattr(trade, "fee", 0.0) or 0.0)
            realized_total += apply_trade(
                position, side, qty, price, fee=fee, method=method
            )
            trade_value_total += qty * price

        total_value = realized_total

        daily_records.append(
            {
                "date": date_value,
                "realized_pl": round(realized_total, 2),
                "trade_value": round(trade_value_total, 2),
                "total_pl": round(total_value, 2),
            }
        )

    daily_df = pd.DataFrame(daily_records)
    daily_df["cumulative_pl"] = daily_df["total_pl"].cumsum()
    return daily_df
