"""Parser for Charles Schwab transaction history exports."""

from __future__ import annotations

import csv
import io
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

_DECODING_CANDIDATES = (
    "utf-8-sig",
    "utf-16",
    "utf-16le",
    "utf-16be",
    "utf-8",
)

_HEADER_ALIASES = {
    "date": "date",
    "trade_date": "date",
    "action": "action",
    "transaction": "action",
    "transaction_type": "action",
    "type": "action",
    "symbol": "symbol",
    "ticker": "symbol",
    "symbol_description": "symbol_description",
    "symbol___description": "symbol_description",
    "description": "description",
    "quantity": "qty",
    "shares": "qty",
    "price": "price",
    "trade_price": "price",
    "amount": "amount",
    "total": "amount",
    "value": "amount",
    "fees": "fee",
    "fees___comm": "fee",
    "fees_comm": "fee",
    "fees_&_comm": "fee",
    "fees_and_comm": "fee",
    "fee": "fee",
}

_DATE_FORMATS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m/%d/%y",
)

_TRADE_ACTIONS = {
    "BUY": "BUY",
    "SELL": "SELL",
    "REINVEST SHARES": "BUY",
}

_DIVIDEND_ACTIONS = {
    "BANK INTEREST",
    "CASH DIVIDEND",
    "QUAL DIV REINVEST",
    "QUALIFIED DIVIDEND",
    "REINVEST DIVIDEND",
}

_IGNORED_ACTIONS = {
    "MONEYLINK TRANSFER",
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


def _normalize_action_key(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _format_action_label(key: str) -> str:
    parts = [part for part in key.split(" ") if part]
    formatted = []
    for part in parts:
        if len(part) <= 3:
            formatted.append(part.upper())
        else:
            formatted.append(part.capitalize())
    return " ".join(formatted)


def _sanitize_symbol(value: Optional[str]) -> str:
    if not value:
        return ""
    text = value.strip().upper()
    return re.sub(r"[^A-Z0-9.]+", "", text)


def _normalize_description(value: Optional[str]) -> str:
    if not value:
        return ""
    text = value.strip()
    return re.sub(r"\s+", " ", text)


def _split_symbol_description(value: Optional[str]) -> Tuple[str, str]:
    if not value:
        return "", ""
    text = value.strip()
    if not text:
        return "", ""
    parts = re.split(r"[\s\u00A0]+", text)
    if not parts:
        return "", ""
    symbol = _sanitize_symbol(parts[0])
    description = " ".join(part.strip() for part in parts[1:] if part.strip())
    return symbol, description


def parse_charles_schwab_csv(content: bytes) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Parse a Charles Schwab transaction history export.

    Returns a tuple of ``(trades, dividends)`` ready for persistence.
    """

    text = _decode_content(content)
    if not text.strip():
        return [], []

    reader = csv.reader(io.StringIO(text))
    try:
        headers = next(reader)
    except StopIteration:
        return [], []

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
        rows.append(values)

    trades: List[Dict[str, Any]] = []
    dividends: List[Dict[str, Any]] = []

    for values in rows:
        date_value = _parse_date(values.get("date"))
        if not date_value:
            continue

        action_key = _normalize_action_key(values.get("action"))
        if not action_key or action_key in _IGNORED_ACTIONS:
            continue

        action_label = _format_action_label(action_key)

        symbol_value = _sanitize_symbol(values.get("symbol"))
        description_value = _normalize_description(values.get("description"))
        if not symbol_value and values.get("symbol_description"):
            alt_symbol, alt_description = _split_symbol_description(values.get("symbol_description"))
            if alt_symbol:
                symbol_value = alt_symbol
            if not description_value and alt_description:
                description_value = _normalize_description(alt_description)
        elif symbol_value and not description_value and values.get("symbol_description"):
            _, alt_description = _split_symbol_description(values.get("symbol_description"))
            if alt_description:
                description_value = _normalize_description(alt_description)

        qty_value = _parse_number(values.get("qty")) or 0.0
        price_value = _parse_number(values.get("price")) or 0.0
        fee_value = _parse_number(values.get("fee")) or 0.0
        amount_value = _parse_number(values.get("amount"))

        if action_key in _TRADE_ACTIONS:
            if qty_value == 0:
                continue
            qty_abs = abs(qty_value)
            price_clean = price_value
            amount_clean = amount_value
            if price_clean == 0 and amount_clean is not None and qty_abs:
                price_clean = abs(amount_clean) / qty_abs
            if amount_clean is None and price_clean:
                direction = -1 if _TRADE_ACTIONS[action_key] == "BUY" else 1
                amount_clean = direction * qty_abs * price_clean
            if amount_clean is None or price_clean == 0:
                continue
            trades.append(
                {
                    "date": date_value,
                    "symbol": symbol_value,
                    "action": _TRADE_ACTIONS[action_key],
                    "qty": float(qty_abs),
                    "price": float(price_clean),
                    "amount": float(amount_clean),
                    "fee": float(abs(fee_value)),
                    "time": "",
                }
            )
            continue

        if action_key in _DIVIDEND_ACTIONS:
            if amount_value is None:
                continue
            dividends.append(
                {
                    "date": date_value,
                    "symbol": symbol_value,
                    "description": description_value,
                    "action": action_label,
                    "qty": float(qty_value or 0.0),
                    "price": float(price_value or 0.0),
                    "fee": float(abs(fee_value)),
                    "amount": float(amount_value),
                    "time": "",
                }
            )
            continue

        # Unknown actions are treated as dividends so they remain visible to the user.
        if amount_value is not None:
            dividends.append(
                {
                    "date": date_value,
                    "symbol": symbol_value,
                    "description": description_value,
                    "action": action_label,
                    "qty": float(qty_value or 0.0),
                    "price": float(price_value or 0.0),
                    "fee": float(abs(fee_value)),
                    "amount": float(amount_value),
                    "time": "",
                }
            )

    return trades, dividends
