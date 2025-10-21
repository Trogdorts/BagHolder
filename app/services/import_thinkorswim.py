import csv
import io
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

TRADE_ACTION_MAP = {
    "BUY": "BUY",
    "BOT": "BUY",
    "BTO": "BUY",
    "SELL": "SELL",
    "SLD": "SELL",
    "STC": "SELL",
}


def _canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
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
        "net_amount": "amount",
        "proceeds": "amount",
        "net_price": "price",
        "realized_pl": "realized_pl",
    }
    renamed = {c: aliases.get(c, c) for c in df.columns}
    df = df.rename(columns=renamed)
    return df


def _extract_symbol(desc: str) -> str:
    m = re.search(r"\b([A-Z]{1,6})(?:\s|$|\.)", desc or "")
    return m.group(1) if m else ""


def _normalize_header(label: str) -> str:
    label = (label or "").strip().lower()
    label = label.replace("#", "number")
    label = re.sub(r"[^a-z0-9]+", "_", label)
    return label.strip("_")


def _parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"~", "-", "--"}:
        return None
    text = text.replace("$", "").replace(",", "")
    if text.startswith("(") and text.endswith(")"):
        text = f"-{text[1:-1]}"
    try:
        return float(text)
    except ValueError:
        return None


_DATETIME_FORMATS = [
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y %I:%M:%S %p",
    "%m/%d/%Y %I:%M %p",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m/%d/%y %H:%M:%S",
    "%m/%d/%y %H:%M",
    "%m/%d/%y",
]


def _parse_datetime_guess(text: Optional[str]) -> Optional[datetime]:
    if not text:
        return None
    cleaned = re.sub(r"\b(ET|EST|EDT|CST|CDT|PST|PDT|MT|MDT|UTC|GMT)\b", "", text, flags=re.IGNORECASE)
    cleaned = cleaned.strip().replace("T", " ")
    cleaned = re.sub(r"\s+", " ", cleaned)
    for fmt in _DATETIME_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    return None


def _read_statement_rows(content: bytes) -> List[Dict[str, Any]]:
    text = content.decode("utf-8-sig", errors="ignore")
    delimiter = "\t" if text.count("\t") > text.count(",") else ","
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)

    rows_with_order: List[Tuple[datetime, int, Dict[str, Any]]] = []
    section: Optional[str] = None
    headers: Optional[List[str]] = None

    for index, raw_row in enumerate(reader):
        row = [cell.strip() for cell in raw_row]
        if not any(row):
            continue

        first_cell = row[0].lower()
        if first_cell.startswith("account "):
            section = "trade_history" if "trade history" in first_cell else None
            headers = None
            continue

        if section != "trade_history":
            continue

        if headers is None:
            headers = [_normalize_header(cell) for cell in row]
            continue

        data: Dict[str, str] = {}
        for i, header in enumerate(headers):
            if not header:
                continue
            if i < len(row):
                data[header] = row[i].strip()

        symbol = data.get("symbol", "") or data.get("instrument", "")
        description = data.get("description", "")
        side = data.get("side", data.get("action", "")).upper()

        if not symbol:
            symbol = _extract_symbol(description)
        symbol = symbol.strip().upper()

        if not symbol or not side:
            continue

        action = TRADE_ACTION_MAP.get(side)
        if not action:
            if "BUY" in side:
                action = "BUY"
            elif "SELL" in side:
                action = "SELL"
            else:
                continue

        qty_val = _parse_float(data.get("qty") or data.get("quantity"))
        if qty_val is None or abs(qty_val) < 1e-9:
            continue
        qty_val = abs(qty_val)

        price_val = _parse_float(data.get("price"))
        if price_val is None:
            price_val = _parse_float(data.get("net_price"))
        if price_val is None:
            amount_guess = _parse_float(data.get("amount") or data.get("net_amount"))
            if amount_guess is not None and abs(qty_val) > 1e-9:
                price_val = abs(amount_guess) / qty_val
        if price_val is None:
            continue

        exec_time = data.get("exec_time") or data.get("time")
        trade_date_text = data.get("trade_date") or data.get("date")
        dt_exec = _parse_datetime_guess(exec_time)
        if dt_exec is None:
            dt_exec = _parse_datetime_guess(trade_date_text)
        if dt_exec is None:
            continue

        day = dt_exec.strftime("%Y-%m-%d")

        amount_val = _parse_float(data.get("amount") or data.get("net_amount"))
        gross = abs(amount_val) if amount_val is not None else qty_val * price_val
        amount = gross if action == "SELL" else -gross

        trade = {
            "date": day,
            "symbol": symbol,
            "action": action,
            "qty": qty_val,
            "price": price_val,
            "amount": amount,
        }

        rows_with_order.append((dt_exec, index, trade))

    rows_with_order.sort(key=lambda x: (x[0], x[1]))
    return [row for _, _, row in rows_with_order]


def _parse_dataframe(content: bytes) -> List[Dict[str, Any]]:
    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception:
        return []

    if df.empty:
        return []

    df = _canonicalize_columns(df)
    if "symbol" not in df.columns:
        return []

    rows: List[Tuple[datetime, int, Dict[str, Any]]] = []
    for idx, r in df.iterrows():
        action = str(r.get("action", "")).upper().strip()
        desc = str(r.get("description", "")).upper().strip()

        if not action and desc:
            if any(token in desc for token in ("BOUGHT", "BOT", "BTO")):
                action = "BUY"
            elif any(token in desc for token in ("SOLD", "SLD", "STC")):
                action = "SELL"

        if action not in ("BUY", "SELL", "BOT", "SLD", "BTO", "STC"):
            continue

        qty = r.get("qty", r.get("quantity", None))
        price = r.get("price", r.get("net_price", None))
        amount = r.get("amount", r.get("net_amount", None))

        symbol = str(r.get("symbol", "")).strip().upper()
        if not symbol:
            symbol = _extract_symbol(desc)
        if not symbol:
            continue

        date_val = r.get("date", r.get("trade_date", None))
        if pd.isna(date_val):
            continue
        try:
            dt = pd.to_datetime(date_val)
        except Exception:
            continue
        dt_python = dt.to_pydatetime() if hasattr(dt, "to_pydatetime") else dt
        if not isinstance(dt_python, datetime):
            continue

        norm_action = TRADE_ACTION_MAP.get(action, action)

        qty_f = _parse_float(qty)
        if qty_f is None:
            continue
        qty_f = abs(qty_f)

        price_f = _parse_float(price)
        if price_f is None:
            amount_val = _parse_float(amount)
            if amount_val is not None and qty_f:
                price_f = abs(amount_val) / qty_f
        if price_f is None:
            continue

        amount_val = _parse_float(amount)
        gross = abs(amount_val) if amount_val is not None else qty_f * price_f
        amount_signed = gross if norm_action == "SELL" else -gross

        trade = {
            "date": dt_python.strftime("%Y-%m-%d"),
            "symbol": symbol,
            "action": norm_action,
            "qty": qty_f,
            "price": price_f,
            "amount": amount_signed,
        }

        rows.append((dt_python, idx, trade))

    rows.sort(key=lambda x: (x[0], x[1]))
    return [row for _, _, row in rows]


def parse_thinkorswim_csv(content: bytes) -> List[Dict[str, Any]]:
    rows = _read_statement_rows(content)
    if rows:
        return rows
    return _parse_dataframe(content)
