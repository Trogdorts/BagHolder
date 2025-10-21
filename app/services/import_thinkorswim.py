import pandas as pd
import io, re
from datetime import datetime
from typing import Tuple, List, Dict, Any

TRADE_ACTION_MAP = {
    "BUY": "BUY", "BOT": "BUY", "BTO": "BUY",
    "SELL": "SELL", "SLD": "SELL", "STC": "SELL",
}

def _canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
    # Common aliases
    aliases = {
        "trade_date": "date",
        "date_time": "date",
        "time": "time",
        "instrument": "symbol",
        "symbol": "symbol",
        "description": "description",
        "action": "action",
        "quantity": "qty",
        "qty": "qty",
        "price": "price",
        "amount": "amount",
        "proceeds": "amount",
        "realized_pl": "realized_pl",
    }
    for k, v in list(df.columns.map(lambda c: (c, aliases.get(c, c)))):
        pass
    renamed = {c: aliases.get(c, c) for c in df.columns}
    df = df.rename(columns=renamed)
    return df

def _extract_symbol(desc: str) -> str:
    # TOS descriptions often like "Bought 100 XYZ @ 10.00"
    m = re.search(r"\b([A-Z]{1,6})(?:\s|$|\.)", desc or "")
    return m.group(1) if m else ""

def parse_thinkorswim_csv(content: bytes) -> List[Dict[str, Any]]:
    df = pd.read_csv(io.BytesIO(content))
    df = _canonicalize_columns(df)

    # Try to identify trade rows
    # Keep rows that have either action or description indicating buy/sell
    rows = []
    for _, r in df.iterrows():
        action = str(r.get("action", "")).upper().strip()
        desc = str(r.get("description", "")).upper().strip()

        if not action and desc:
            if "BOUGHT" in desc or "BOT" in desc or "BTO" in desc:
                action = "BUY"
            elif "SOLD" in desc or "SLD" in desc or "STC" in desc:
                action = "SELL"

        if action not in ("BUY", "SELL", "BOT", "SLD", "BTO", "STC"):
            continue  # skip non-trade rows (fees, interest, etc.)

        qty = r.get("qty", r.get("quantity", None))
        price = r.get("price", None)
        amount = r.get("amount", None)

        # Symbol resolution: prefer explicit, else parse description
        symbol = str(r.get("symbol", "")).strip().upper()
        if not symbol:
            symbol = _extract_symbol(desc)

        # Date handling
        date_val = r.get("date", None) or r.get("trade_date", None)
        if pd.isna(date_val):
            continue
        try:
            # Support multiple formats
            dt = pd.to_datetime(date_val).date()
        except Exception:
            continue

        # Normalize action
        norm_action = TRADE_ACTION_MAP.get(action, action)

        try:
            qty_f = float(qty)
            price_f = float(price)
        except Exception:
            # If price missing, try to back out from amount/qty
            try:
                amount_f = float(str(amount).replace(",", ""))
                qty_f = float(qty)
                price_f = amount_f / qty_f if qty_f else 0.0
            except Exception:
                continue

        rows.append({
            "date": dt.strftime("%Y-%m-%d"),
            "symbol": symbol,
            "action": norm_action,
            "qty": qty_f,
            "price": price_f,
            "amount": float(str(amount).replace(",", "")) if amount is not None and str(amount).strip() != "" else qty_f * price_f if norm_action=="SELL" else -qty_f * price_f,
        })

    # Sort by date
    rows.sort(key=lambda x: x["date"])
    return rows
