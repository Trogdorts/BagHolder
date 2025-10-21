#!/usr/bin/env python3
"""
ThinkOrSwim CSV Extractor — Fixed Version
Accurately tracks realized and unrealized gains/losses per day.
"""

import os, csv, logging
import pandas as pd
from datetime import datetime

# ---------------------------------------------------------------------------
# Setup logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger("tos_parser")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def latest_statement():
    d = os.path.expanduser("~/Downloads")
    files = [f for f in os.listdir(d) if f.lower().endswith(".csv")]
    if not files:
        raise FileNotFoundError("No CSV files found in Downloads.")
    files.sort(key=lambda x: os.path.getmtime(os.path.join(d, x)), reverse=True)
    path = os.path.join(d, files[0])
    log.debug(f"Using statement: {path}")
    return path

def read_text(path):
    with open(path, "rb") as f:
        raw = f.read()
    try:
        text = raw.decode("utf-8-sig")
        log.debug("Decoded as UTF-8.")
    except UnicodeDecodeError:
        text = raw.decode("utf-16", errors="ignore")
        log.debug("Decoded as UTF-16.")
    return text

def extract_trade_section(text):
    """Extract lines under 'Account Trade History'."""
    lines = text.splitlines()
    start = None
    for i, ln in enumerate(lines):
        if "Account Trade History" in ln:
            start = i + 2
            log.debug(f"Found 'Account Trade History' at line {i}")
            break
    if start is None:
        log.warning("No 'Account Trade History' section found.")
        return []
    section = []
    for ln in lines[start:]:
        if not ln.strip() or "Equities" in ln or "Profits" in ln:
            break
        section.append(ln.strip())
    log.debug(f"Captured {len(section)} trade lines.")
    return section

def parse_trades(lines):
    """Parse trade section lines into structured rows."""
    reader = csv.reader(lines)
    trades = []
    for row in reader:
        if len(row) < 12:
            continue
        try:
            dt = datetime.strptime(row[1].split(" ")[0], "%m/%d/%y").date()
            side = row[3].strip()
            qty = abs(float(row[4]))
            symbol = row[6].strip().upper()
            price = float(row[10])
            trades.append({
                "date": dt,
                "side": side,
                "symbol": symbol,
                "quantity": qty,
                "price": price
            })
        except Exception as e:
            log.debug(f"Skip {row}: {e}")
    log.debug(f"Parsed {len(trades)} trades.")
    return pd.DataFrame(trades)

def extract_equities_section(text):
    """Extract the 'Equities' section."""
    lines = text.splitlines()
    start = None
    for i, ln in enumerate(lines):
        if ln.startswith("Symbol,Description,Qty"):
            start = i + 1
            break
    if start is None:
        log.warning("No 'Equities' section found.")
        return []
    section = []
    for ln in lines[start:]:
        if not ln.strip() or "OVERALL TOTALS" in ln:
            break
        section.append(ln.strip())
    return section

def parse_equities(lines):
    """Parse current holdings for unrealized P/L."""
    reader = csv.reader(lines)
    holdings = []
    for row in reader:
        try:
            symbol = row[0].strip().upper()
            qty = int(row[2].replace("+", ""))
            trade_price = float(row[3])
            mark = float(row[4])
            holdings.append({
                "symbol": symbol,
                "quantity": qty,
                "avg_cost": trade_price,
                "mark": mark,
                "unrealized": (mark - trade_price) * qty
            })
        except Exception as e:
            log.debug(f"Skip equity {row}: {e}")
    return pd.DataFrame(holdings)

# ---------------------------------------------------------------------------
# Correct daily P/L logic
# ---------------------------------------------------------------------------

def compute_daily_pnl(trades: pd.DataFrame):
    positions = {}  # symbol → {"shares": int, "avg_cost": float}
    daily_records = []

    for date, day_trades in trades.sort_values("date").groupby("date"):
        realized_total = 0.0

        for _, t in day_trades.iterrows():
            sym, side, qty, price = t.symbol, t.side, int(t.quantity), float(t.price)
            pos = positions.setdefault(sym, {"shares": 0, "avg_cost": 0.0})

            if side == "BUY":
                total_cost = pos["avg_cost"] * pos["shares"] + price * qty
                pos["shares"] += qty
                if pos["shares"]:
                    pos["avg_cost"] = total_cost / pos["shares"]

            elif side == "SELL":
                if pos["shares"] > 0:
                    sold = min(qty, pos["shares"])
                    realized = (price - pos["avg_cost"]) * sold
                    realized_total += realized
                    pos["shares"] -= sold
                    if pos["shares"] < 0:
                        pos["shares"] = 0

        # unrealized from all open positions after processing day’s trades
        unrealized_total = 0.0
        for sym, p in positions.items():
            if p["shares"] > 0:
                last_price = (
                    day_trades.loc[day_trades["symbol"] == sym, "price"].iloc[-1]
                    if sym in day_trades.symbol.values
                    else p["avg_cost"]
                )
                unrealized_total += (last_price - p["avg_cost"]) * p["shares"]

        daily_records.append({
            "date": date,
            "realized_pl": round(realized_total, 2),
            "unrealized_pl": round(unrealized_total, 2),
            "total_pl": round(realized_total + unrealized_total, 2),
        })

    daily_df = pd.DataFrame(daily_records)
    daily_df["cumulative_pl"] = daily_df["total_pl"].cumsum()
    return daily_df

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    path = latest_statement()
    print(f"\nProcessing: {path}\n")
    text = read_text(path)

    trade_lines = extract_trade_section(text)
    trades = parse_trades(trade_lines)
    eq_lines = extract_equities_section(text)
    equities = parse_equities(eq_lines)

    if trades.empty:
        log.warning("No trades parsed.")
        return

    daily = compute_daily_pnl(trades)

    log.info("\n=== TRADES SAMPLE ===\n" + trades.head(20).to_string(index=False))
    log.info("\n=== DAILY SUMMARY ===\n" + daily.tail(20).to_string(index=False))

    if not equities.empty:
        log.info("\n=== END-OF-PERIOD HOLDINGS ===\n" + equities.to_string(index=False))
        total_unreal = equities["unrealized"].sum()
        log.info(f"\nCurrent Unrealized P/L: ${total_unreal:,.2f}")

    print("\nDone.")

if __name__ == "__main__":
    main()
