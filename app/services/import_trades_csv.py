"""Utilities for importing generic stock trade CSV data."""

from __future__ import annotations

import csv
import io
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

_DECODING_CANDIDATES = (
    "utf-8-sig",
    "utf-16",
    "utf-16le",
    "utf-16be",
    "utf-8",
)

_HEADER_ALIASES = {
    "trade_date": "date",
    "date_time": "date",
    "transaction_date": "date",
    "symbol": "symbol",
    "ticker": "symbol",
    "underlying": "symbol",
    "ticker_symbol": "symbol",
    "side": "action",
    "type": "action",
    "transaction_type": "action",
    "activity": "action",
    "action": "action",
    "trade_type": "action",
    "qty": "qty",
    "quantity": "qty",
    "shares": "qty",
    "contracts": "qty",
    "price": "price",
    "trade_price": "price",
    "execution_price": "price",
    "fill_price": "price",
    "amount": "amount",
    "value": "amount",
    "total": "amount",
    "net_amount": "amount",
    "proceeds": "amount",
}

_DATE_FORMATS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m/%d/%y",
)

_ACTION_ALIASES = {
    "BUY": "BUY",
    "BOT": "BUY",
    "B": "BUY",
    "BTO": "BUY",
    "BUY_TO_OPEN": "BUY",
    "BUY_TO_CLOSE": "BUY",
    "BUY_TO_COVER": "BUY",
    "SELL": "SELL",
    "SLD": "SELL",
    "S": "SELL",
    "STC": "SELL",
    "SELL_TO_CLOSE": "SELL",
    "SELL_TO_OPEN": "SELL",
    "SOLD": "SELL",
    "SELL_SHORT": "SELL",
}


def _decode_content(data: bytes) -> str:
    if not data:
        return ""
    for encoding in _DECODING_CANDIDATES:
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="ignore")


def _normalize_header(label: Optional[str]) -> str:
    text = (label or "").strip().lower()
    text = text.replace("#", "number")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def _canonical_header(label: Optional[str]) -> str:
    normalized = _normalize_header(label)
    return _HEADER_ALIASES.get(normalized, normalized)


def _parse_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = value.strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _parse_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "").replace("$", "")
    if text.startswith("(") and text.endswith(")"):
        text = f"-{text[1:-1]}"
    try:
        return float(text)
    except ValueError:
        return None


def _parse_action(value: Any) -> Optional[str]:
    if value is None:
        return None
    label = str(value).strip().upper()
    label = re.sub(r"[^A-Z0-9_ ]+", " ", label)
    label = re.sub(r"\s+", "_", label).strip("_")
    if not label:
        return None
    if label in _ACTION_ALIASES:
        return _ACTION_ALIASES[label]
    if "BUY" in label:
        return "BUY"
    if "SELL" in label or "SLD" in label:
        return "SELL"
    return None


def _sanitize_symbol(value: Optional[str]) -> str:
    if not value:
        return ""
    text = value.strip().upper()
    text = re.sub(r"[^A-Z0-9.]+", "", text)
    return text


def parse_trade_csv(content: bytes) -> List[Dict[str, Any]]:
    """Parse a generic trade CSV into rows consumable by the Trade model."""

    text = _decode_content(content)
    if not text.strip():
        return []

    reader = csv.reader(io.StringIO(text))
    try:
        headers = next(reader)
    except StopIteration:
        return []

    if not headers:
        return []

    canonical_headers: List[str] = []
    for header in headers:
        canonical_headers.append(_canonical_header(header))

    rows: List[Dict[str, Any]] = []
    for raw_row in reader:
        if not any((cell or "").strip() for cell in raw_row):
            continue
        values = {
            canonical_headers[i]: raw_row[i].strip() if i < len(raw_row) else ""
            for i in range(len(canonical_headers))
        }

        date_value = _parse_date(values.get("date"))
        if not date_value:
            continue

        symbol_value = _sanitize_symbol(values.get("symbol"))
        if not symbol_value:
            continue

        action_value = _parse_action(values.get("action"))
        if not action_value:
            continue

        qty_value = _parse_number(values.get("qty"))
        amount_value = _parse_number(values.get("amount"))
        price_value = _parse_number(values.get("price"))

        if qty_value is None or qty_value == 0:
            continue

        qty_abs = abs(qty_value)

        if price_value is None and amount_value is not None and qty_abs:
            price_value = abs(amount_value) / qty_abs if qty_abs else None

        if amount_value is None and price_value is not None:
            direction = -1 if action_value == "BUY" else 1
            amount_value = direction * qty_abs * price_value

        if price_value is None or amount_value is None:
            continue

        rows.append(
            {
                "date": date_value,
                "symbol": symbol_value,
                "action": action_value,
                "qty": float(qty_abs),
                "price": float(price_value),
                "amount": float(amount_value),
            }
        )

    return rows

