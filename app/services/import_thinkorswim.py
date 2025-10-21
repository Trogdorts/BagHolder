import csv
import io
import logging
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

log = logging.getLogger(__name__)


TRADE_ACTION_MAP = {
    "BUY": "BUY",
    "BOT": "BUY",
    "BTO": "BUY",
    "SELL": "SELL",
    "SLD": "SELL",
    "STC": "SELL",
}


_PLAINTEXT_TRADE_LINE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}.*\b(BOT|SOLD)\b", re.IGNORECASE)
_PLAINTEXT_TRADE_PATTERN = re.compile(
    r"(?P<date>\d{2}/\d{2}/\d{4}).*?\b(?P<action>BOT|SOLD)\b\s+(?P<qty>[+-]?\d+)\s+"
    r"(?P<symbol>[A-Z0-9.\s]+?)\s*@\s*(?P<price>[\d.,]+).*?(?P<amount>[-\d,]+\.\d{2})",
    re.IGNORECASE,
)


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


def _decode_text_content(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-16", "utf-16le", "utf-16be"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="ignore")


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


def _parse_plaintext_statement(content: bytes) -> List[Dict[str, Any]]:
    text = _decode_text_content(content)
    if not text:
        return []

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    trades: List[Tuple[datetime, int, Dict[str, Any]]] = []

    for idx, line in enumerate(lines):
        if not _PLAINTEXT_TRADE_LINE_RE.match(line):
            continue
        match = _PLAINTEXT_TRADE_PATTERN.search(line)
        if not match:
            continue

        date_text = match.group("date")
        try:
            trade_date = datetime.strptime(date_text, "%m/%d/%Y")
        except ValueError:
            continue

        action_raw = match.group("action").upper()
        action = "BUY" if "BOT" in action_raw else "SELL"

        qty = _parse_float(match.group("qty"))
        price = _parse_float(match.group("price"))
        amount_val = _parse_float(match.group("amount"))

        if qty is None or abs(qty) < 1e-9:
            continue
        if price is None:
            continue
        if amount_val is None:
            amount_val = abs(qty) * price

        qty = abs(qty)
        amount_signed = abs(amount_val) if action == "SELL" else -abs(amount_val)

        raw_symbol = match.group("symbol") or ""
        primary_symbol = raw_symbol.strip().upper().split()
        symbol = primary_symbol[0] if primary_symbol else ""
        symbol = re.sub(r"[^A-Z0-9.]+", "", symbol)
        if not symbol:
            symbol = _extract_symbol(raw_symbol)
        if not symbol:
            continue

        trades.append(
            (
                trade_date,
                idx,
                {
                    "date": trade_date.strftime("%Y-%m-%d"),
                    "symbol": symbol,
                    "action": action,
                    "qty": qty,
                    "price": price,
                    "amount": amount_signed,
                },
            )
        )

    trades.sort(key=lambda item: (item[0], item[1]))
    return [trade for _, _, trade in trades]


def _read_statement_rows(content: bytes) -> List[Dict[str, Any]]:
    text = _decode_text_content(content)
    delimiter = "\t" if text.count("\t") > text.count(",") else ","
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)

    rows_with_order: List[Tuple[datetime, int, Dict[str, Any]]] = []
    section: Optional[str] = None
    headers: Optional[List[str]] = None

    for index, raw_row in enumerate(reader):
        row = [cell.strip() for cell in raw_row]
        if not any(row):
            continue

        normalized_row = [_normalize_header(cell) for cell in row]
        first_cell = row[0].strip().lower()

        if any("trade_history" in cell for cell in normalized_row) or "trade history" in first_cell:
            section = "trade_history"
            headers = None
            header_candidates = set(normalized_row)
            if "symbol" in header_candidates and (
                "action" in header_candidates
                or "qty" in header_candidates
                or "quantity" in header_candidates
            ):
                headers = normalized_row
            continue

        if first_cell.startswith("account "):
            section = "trade_history" if "trade history" in first_cell else None
            headers = None
            continue

        if section != "trade_history":
            header_candidates = set(normalized_row)
            if "symbol" in header_candidates and (
                "action" in header_candidates
                or "qty" in header_candidates
                or "quantity" in header_candidates
            ):
                section = "trade_history"
                headers = normalized_row
            continue

        if headers is None:
            headers = normalized_row
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


def _extract_trade_section(text: str) -> List[str]:
    lines = text.splitlines()
    start = None
    for i, line in enumerate(lines):
        if "Account Trade History" in line:
            start = i + 2
            log.debug("Detected 'Account Trade History' section at line %s", i)
            break
    if start is None:
        return []

    section: List[str] = []
    for line in lines[start:]:
        if not line.strip() or "Equities" in line or "Profits" in line:
            break
        section.append(line.strip())
    return section


def _parse_trade_section(lines: Iterable[str]) -> pd.DataFrame:
    reader = csv.reader(lines)
    trades: List[Dict[str, Any]] = []
    for row in reader:
        if len(row) < 12:
            continue
        try:
            dt = datetime.strptime(row[1].split(" ")[0], "%m/%d/%y").date()
            side = row[3].strip().upper()
            qty = abs(float(row[4]))
            symbol = row[6].strip().upper()
            price = float(row[10])
        except Exception as exc:  # pragma: no cover - defensive parsing
            log.debug("Skipping trade row %s due to %s", row, exc)
            continue

        if not symbol or side not in {"BUY", "SELL"}:
            continue

        trades.append(
            {
                "date": dt,
                "side": side,
                "symbol": symbol,
                "quantity": qty,
                "price": price,
            }
        )

    return pd.DataFrame(trades)


def _parse_statement_trade_lines(content: bytes) -> List[Dict[str, Any]]:
    text = _decode_text_content(content)
    if not text:
        return []
    lines = _extract_trade_section(text)
    if not lines:
        return []
    df = _parse_trade_section(lines)
    if df.empty:
        return []

    results: List[Dict[str, Any]] = []
    for row in df.to_dict("records"):
        day = row["date"].strftime("%Y-%m-%d")
        side = row["side"].upper()
        qty = float(row["quantity"])
        price = float(row["price"])
        amount = qty * price
        amount_signed = amount if side == "SELL" else -amount
        results.append(
            {
                "date": day,
                "symbol": row["symbol"],
                "action": side,
                "qty": qty,
                "price": price,
                "amount": amount_signed,
            }
        )
    return results


def parse_thinkorswim_csv(content: bytes) -> List[Dict[str, Any]]:
    section_rows = _parse_statement_trade_lines(content)
    if section_rows:
        log.debug("Parsed %s trades from 'Account Trade History' section", len(section_rows))
        return section_rows

    plaintext_rows = _parse_plaintext_statement(content)
    if plaintext_rows:
        return plaintext_rows

    rows = _read_statement_rows(content)
    if rows:
        return rows

    return _parse_dataframe(content)


def compute_daily_pnl_records(records: List[Dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(
            columns=[
                "date",
                "realized_pl",
                "unrealized_pl",
                "total_pl",
                "cumulative_pl",
            ]
        )

    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "realized_pl",
                "unrealized_pl",
                "total_pl",
                "cumulative_pl",
            ]
        )

    if "date" not in df.columns or "side" not in df.columns:
        raise ValueError("records require 'date' and 'side' fields")

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["side"] = df["side"].str.upper()

    positions: Dict[str, Dict[str, float]] = {}
    daily_records: List[Dict[str, Any]] = []

    for date_value, day_trades in df.sort_values("date").groupby("date"):
        realized_total = 0.0

        for _, trade in day_trades.iterrows():
            sym = trade["symbol"].upper()
            side = trade["side"]
            qty = float(trade["quantity"])
            price = float(trade["price"])
            pos = positions.setdefault(sym, {"shares": 0.0, "avg_cost": 0.0})

            if side == "BUY":
                total_cost = pos["avg_cost"] * pos["shares"] + price * qty
                pos["shares"] += qty
                if pos["shares"]:
                    pos["avg_cost"] = total_cost / pos["shares"]
            elif side == "SELL":
                shares_available = pos["shares"]
                if shares_available > 0:
                    sold = min(qty, shares_available)
                    realized = (price - pos["avg_cost"]) * sold
                    realized_total += realized
                    pos["shares"] -= sold
                    if pos["shares"] < 0:
                        pos["shares"] = 0.0

        unrealized_total = 0.0
        for sym, p in positions.items():
            if p["shares"] > 0:
                last_price = (
                    day_trades.loc[day_trades["symbol"].str.upper() == sym, "price"].iloc[-1]
                    if sym in day_trades["symbol"].str.upper().values
                    else p["avg_cost"]
                )
                unrealized_total += (float(last_price) - p["avg_cost"]) * p["shares"]

        total_pl = realized_total + unrealized_total
        daily_records.append(
            {
                "date": date_value,
                "realized_pl": round(realized_total, 2),
                "unrealized_pl": round(unrealized_total, 2),
                "total_pl": round(total_pl, 2),
            }
        )

    daily_df = pd.DataFrame(daily_records)
    daily_df["cumulative_pl"] = daily_df["total_pl"].cumsum()
    return daily_df
