"""Utilities for importing BagHolder-exported CSV data."""

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
    "realized_pl": "realized",
    "realizedpnl": "realized",
    "realized_pnl": "realized",
    "unrealized_pl": "unrealized",
    "unrealized_pnl": "unrealized",
    "total": "total_invested",
    "totalinvested": "total_invested",
    "total_investment": "total_invested",
}

_DATE_FORMATS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m/%d/%y",
)


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


def parse_bagholder_csv(content: bytes) -> List[Dict[str, Any]]:
    """Parse a BagHolder-exported CSV into normalized daily summary rows."""
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
        if not any(cell.strip() for cell in raw_row):
            continue
        values = {
            canonical_headers[i]: raw_row[i].strip() if i < len(raw_row) else ""
            for i in range(len(canonical_headers))
        }

        date_value = _parse_date(values.get("date"))
        if not date_value:
            continue

        realized = _parse_number(values.get("realized"))
        unrealized = _parse_number(values.get("unrealized"))
        total_invested = _parse_number(values.get("total_invested"))
        updated_at = (values.get("updated_at") or "").strip()

        if realized is None:
            realized = 0.0
        if unrealized is None:
            unrealized = 0.0
        if total_invested is None:
            total_invested = unrealized

        rows.append(
            {
                "date": date_value,
                "realized": float(realized),
                "unrealized": float(unrealized),
                "total_invested": float(total_invested),
                "updated_at": updated_at,
            }
        )

    return rows
