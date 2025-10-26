"""Trade simulation utilities based on the standalone simulator script.

This module encapsulates the behaviour from the ``simulate_real_trades.py``
script that accompanies the project. The original script focused on running
from the command line; here we adapt it for in-app usage so the UI can trigger
simulations while keeping the logic and configurability intact.
"""

from __future__ import annotations

import logging
import os
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import StringIO
from typing import Dict, Iterable, List, Mapping, MutableMapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests
import yfinance as yf

log = logging.getLogger(__name__)

NASDAQ_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
NYSE_URL = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"
_MAX_FETCH_SYMBOLS = 100
_DEFAULT_WORKERS = 8


class SimulationError(RuntimeError):
    """Raised when the simulator cannot complete the requested run."""


@dataclass
class SimulationOptions:
    """Normalized options for a simulation run."""

    years_back: int = 2
    start_balance: float = 10_000.0
    risk_level: float = 0.5
    profit_target: float = 0.05
    stop_loss: float = 0.03
    symbol_cache: str = "us_symbols.csv"
    price_cache_dir: str = "price_cache"
    output_dir: str = "output"
    output_name: str = "trades.csv"
    seed: int = 42
    generate_only: bool = False
    max_workers: int = _DEFAULT_WORKERS

    def as_dict(self) -> Dict[str, object]:
        return {
            "years_back": self.years_back,
            "start_balance": self.start_balance,
            "risk_level": self.risk_level,
            "profit_target": self.profit_target,
            "stop_loss": self.stop_loss,
            "symbol_cache": self.symbol_cache,
            "price_cache_dir": self.price_cache_dir,
            "output_dir": self.output_dir,
            "output_name": self.output_name,
            "seed": self.seed,
            "generate_only": self.generate_only,
            "max_workers": self.max_workers,
        }


@dataclass
class SimulationResult:
    """Return value produced by :func:`run_trade_simulation`."""

    trades: pd.DataFrame
    metadata: MutableMapping[str, object]


def _write_csv(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)


def _calculate_rsi(series: pd.Series, window: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = (-1 * delta.clip(upper=0)).abs()
    ma_up = up.rolling(window).mean()
    ma_down = down.rolling(window).mean()
    rs = ma_up / ma_down
    return 100 - (100 / (1 + rs))


def _read_symbol_cache(path: str) -> List[str]:
    try:
        df = pd.read_csv(path)
    except Exception:  # pragma: no cover - graceful cache fallback
        return []
    symbols = df.get("Symbol")
    if symbols is None:
        return []
    result = (
        symbols.dropna()
        .astype(str)
        .str.upper()
        .str.strip()
        .tolist()
    )
    return [sym for sym in result if sym.isalpha() and 1 <= len(sym) <= 5]


def _download_symbol_cache(path: str) -> List[str]:
    log.info("Downloading U.S. ticker universe…")
    frames: List[pd.DataFrame] = []
    for url in (NASDAQ_URL, NYSE_URL):
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
        except requests.RequestException as exc:  # pragma: no cover - network error
            log.warning("Unable to download %s: %s", url, exc)
            continue
        try:
            frame = pd.read_csv(StringIO(response.text), sep="|")
        except Exception as exc:  # pragma: no cover - malformed data
            log.warning("Unable to parse symbol list from %s: %s", url, exc)
            continue
        column = "Symbol" if "Symbol" in frame.columns else "ACT Symbol"
        frames.append(frame[[column]].rename(columns={column: "Symbol"}))

    if not frames:
        raise SimulationError("Failed to download U.S. ticker symbols.")

    merged = pd.concat(frames).dropna().drop_duplicates()
    merged["Symbol"] = merged["Symbol"].astype(str).str.upper()
    merged = merged[merged["Symbol"].str.match(r"^[A-Z]{1,5}$")]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    merged.to_csv(path, index=False)
    log.info("Saved %s symbols to %s", len(merged), path)
    return merged["Symbol"].tolist()


def get_us_symbols(path: str) -> List[str]:
    symbols = _read_symbol_cache(path)
    if symbols:
        return symbols
    return _download_symbol_cache(path)


def update_price_cache(
    symbols: Sequence[str],
    years_back: int,
    cache_dir: str,
    max_workers: int = _DEFAULT_WORKERS,
) -> List[str]:
    os.makedirs(cache_dir, exist_ok=True)
    end = datetime.today()
    start = end - timedelta(days=365 * years_back)
    cached = [f[:-4] for f in os.listdir(cache_dir) if f.endswith(".csv")]
    candidates = [s for s in symbols if s.isalpha() and len(s) <= 5]
    to_fetch = [s for s in candidates if s not in cached][: _MAX_FETCH_SYMBOLS]

    if not to_fetch:
        log.info("Price cache is up to date (%s symbols).", len(cached))
        return cached

    log.info("Fetching %s symbols for price cache…", len(to_fetch))

    def fetch(sym: str) -> str | None:
        try:
            df = yf.download(
                sym,
                start=start,
                end=end,
                progress=False,
                auto_adjust=True,
                threads=False,
            )
        except Exception as exc:  # pragma: no cover - network error
            log.warning("Failed to download %s: %s", sym, exc)
            return None
        if df.empty:
            log.info("Skipping %s because no data was returned", sym)
            return None
        tidy = df[["Close"]].rename_axis("Date").reset_index()
        tidy.to_csv(os.path.join(cache_dir, f"{sym}.csv"), index=False)
        return sym

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch, sym) for sym in to_fetch]
        for completed in as_completed(futures):  # pragma: no branch - iteration only
            symbol = completed.result()
            if symbol:
                log.info("Cached %s", symbol)

    return [f[:-4] for f in os.listdir(cache_dir) if f.endswith(".csv")]


def load_prices(cache_dir: str) -> Dict[str, pd.DataFrame]:
    data: Dict[str, pd.DataFrame] = {}
    if not os.path.isdir(cache_dir):
        return data
    for filename in os.listdir(cache_dir):
        if not filename.endswith(".csv"):
            continue
        path = os.path.join(cache_dir, filename)
        try:
            df = pd.read_csv(path, parse_dates=["Date"])
        except Exception as exc:  # pragma: no cover - corrupted file
            log.warning("Skipping %s due to parse error: %s", filename, exc)
            continue
        if "Close" not in df.columns:
            continue
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
        df.dropna(subset=["Close"], inplace=True)
        if df.empty:
            continue
        data[filename[:-4]] = df
    log.info("Loaded %s symbols from cache for simulation.", len(data))
    return data


def _make_note_buy(symbol: str, reason: str, cash: float) -> str:
    return f"Bought {symbol} {reason}. Cash remaining: ${cash:,.2f}"


def _make_note_sell(symbol: str, reason: str, result: str, cash: float) -> str:
    outcome = "locked in profit" if result == "gain" else "cut the loss"
    return f"Sold {symbol} {reason} — {outcome}. Cash after sale: ${cash:,.2f}"


def simulate_trades(price_map: Mapping[str, pd.DataFrame], options: SimulationOptions) -> pd.DataFrame:
    rng = np.random.default_rng(options.seed)
    trades: List[MutableMapping[str, object]] = []
    cash = float(options.start_balance)
    symbols = list(price_map.items())
    rng.shuffle(symbols)

    log.info("Starting simulation with balance $%s", f"{cash:,.2f}")

    for symbol, raw_df in symbols:
        if len(raw_df) < 60:
            continue
        df = raw_df.sort_values("Date").copy()
        df["SMA_short"] = df["Close"].rolling(10).mean()
        df["SMA_long"] = df["Close"].rolling(30).mean()
        df["RSI"] = _calculate_rsi(df["Close"])
        df.dropna(inplace=True)
        if df.empty:
            continue

        holding = 0
        entry_price = 0.0

        for idx in range(1, len(df)):
            row = df.iloc[idx]
            prev = df.iloc[idx - 1]
            date = row["Date"].to_pydatetime()
            price = float(row["Close"])
            prev_short = float(prev["SMA_short"])
            prev_long = float(prev["SMA_long"])
            short = float(row["SMA_short"])
            long = float(row["SMA_long"])
            rsi = float(row["RSI"])

            if holding == 0 and cash > 0:
                ma_cross = prev_short < prev_long and short > long
                rsi_rebound = rsi < 35 and float(prev["RSI"]) < 30
                hold_bias = rng.uniform(0.85, 1.15)
                if ma_cross or rsi_rebound:
                    qty = int(((cash * 0.1 * options.risk_level) / price) * hold_bias)
                    if qty < 1:
                        continue
                    cost = qty * price
                    if cost > cash:
                        continue
                    cash -= cost
                    holding = qty
                    entry_price = price
                    reason = (
                        "on moving average crossover"
                        if ma_cross
                        else "as RSI rebounded from oversold"
                    )
                    trades.append(
                        {
                            "date": date.strftime("%m/%d/%Y"),
                            "symbol": symbol,
                            "action": "BUY",
                            "qty": qty,
                            "price": round(price, 2),
                            "amount": round(-cost, 2),
                            "cash_after": round(cash, 2),
                            "notes": _make_note_buy(symbol, reason, cash),
                        }
                    )
            elif holding > 0:
                stop_price = entry_price * (1 - options.stop_loss)
                target_price = entry_price * (1 + options.profit_target)
                ma_cross_down = prev_short > prev_long and short < long
                overbought = rsi > 70

                sell_reason = ""
                should_sell = False
                if price <= stop_price:
                    sell_reason = "after hitting stop-loss level"
                    should_sell = True
                elif price >= target_price:
                    sell_reason = "after reaching target profit area"
                    should_sell = True
                elif ma_cross_down:
                    sell_reason = "on bearish moving average crossover"
                    should_sell = True
                elif overbought:
                    sell_reason = "as RSI signaled overbought conditions"
                    should_sell = True

                if should_sell:
                    revenue = holding * price
                    cash += revenue
                    result = "gain" if price > entry_price else "loss"
                    trades.append(
                        {
                            "date": date.strftime("%m/%d/%Y"),
                            "symbol": symbol,
                            "action": "SELL",
                            "qty": holding,
                            "price": round(price, 2),
                            "amount": round(revenue, 2),
                            "cash_after": round(cash, 2),
                            "notes": _make_note_sell(symbol, sell_reason, result, cash),
                        }
                    )
                    holding = 0
                    entry_price = 0.0

    result = pd.DataFrame(trades)
    if result.empty:
        return result
    result["_date"] = pd.to_datetime(result["date"], format="%m/%d/%Y")
    result.sort_values(["_date", "symbol", "action"], inplace=True)
    result.drop(columns=["_date"], inplace=True)
    log.info(
        "Generated %s trades. Final cash balance $%s",
        len(result),
        f"{cash:,.2f}",
    )
    return result


def run_trade_simulation(options: SimulationOptions) -> SimulationResult:
    if options.years_back < 1:
        raise SimulationError("years_back must be at least 1")
    if not 0 < options.risk_level <= 1:
        raise SimulationError("risk_level must be between 0 and 1")
    if options.start_balance <= 0:
        raise SimulationError("start_balance must be positive")

    symbols = get_us_symbols(options.symbol_cache)
    if not symbols:
        raise SimulationError("No symbols available for simulation.")

    shuffled = symbols.copy()
    random.Random(options.seed).shuffle(shuffled)
    cached = update_price_cache(
        shuffled,
        options.years_back,
        options.price_cache_dir,
        max_workers=options.max_workers,
    )

    metadata: MutableMapping[str, object] = {
        "symbols_available": len(symbols),
        "symbols_shuffled": len(shuffled),
        "symbols_cached": len(cached),
        "price_cache_dir": options.price_cache_dir,
        "symbol_cache": options.symbol_cache,
    }

    if options.generate_only:
        metadata["status"] = "cache_updated"
        return SimulationResult(trades=pd.DataFrame(), metadata=metadata)

    price_map = load_prices(options.price_cache_dir)
    if not price_map:
        raise SimulationError("No cached price data is available for simulation.")

    trades = simulate_trades(price_map, options)
    if trades.empty:
        raise SimulationError("Simulation did not produce any trades.")

    if options.output_dir:
        try:
            os.makedirs(options.output_dir, exist_ok=True)
            destination = os.path.join(options.output_dir, options.output_name)
            _write_csv(trades, destination)
            metadata["output_file"] = destination
        except OSError as exc:  # pragma: no cover - filesystem error
            log.warning("Unable to write simulation CSV: %s", exc)

    metadata["status"] = "trades_generated"
    metadata["trades"] = len(trades)
    return SimulationResult(trades=trades, metadata=metadata)
