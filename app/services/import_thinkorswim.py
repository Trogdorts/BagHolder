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
    "BUY_TO_OPEN": "BUY",
    "BUY_TO_CLOSE": "BUY",
    "BUY_TO_COVER": "BUY",
    "BOUGHT": "BUY",
    "SELL": "SELL",
    "SLD": "SELL",
    "STC": "SELL",
    "SELL_TO_CLOSE": "SELL",
    "SELL_TO_OPEN": "SELL",
    "SOLD": "SELL",
    "SELL_SHORT": "SELL",
    "SELL_SHORT_TO_OPEN": "SELL",
    "SELL_SHORT_TO_CLOSE": "SELL",
}


COLUMN_ALIASES = {
    "trade_date": "date",
    "trade_date_et": "date",
    "transaction_date": "date",
    "date_time": "date",
    "time": "time",
    "trade_time": "time",
    "transaction_time": "time",
    "exec_time": "time",
    "exec_time_et": "time",
    "execution_time": "time",
    "execution_time_et": "time",
    "order_time": "time",
    "quantity": "qty",
    "qty": "qty",
    "quantity_executed": "qty",
    "shares": "qty",
    "trade_quantity": "qty",
    "filled_quantity": "qty",
    "instrument": "symbol",
    "ticker_symbol": "symbol",
    "underlying": "symbol",
    "underlying_symbol": "symbol",
    "symbol": "symbol",
    "description": "description",
    "side": "action",
    "type": "action",
    "trade_type": "action",
    "transaction_type": "action",
    "activity": "action",
    "price": "price",
    "trade_price": "price",
    "execution_price": "price",
    "fill_price": "price",
    "avg_price": "price",
    "average_price": "price",
    "net_price": "price",
    "amount": "amount",
    "net_amount": "amount",
    "trade_amount": "amount",
    "trade_value": "amount",
    "value": "amount",
    "gross_amount": "amount",
    "proceeds": "amount",
}


_PLAINTEXT_TRADE_LINE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}.*\b(BOT|SOLD)\b", re.IGNORECASE)
_PLAINTEXT_TRADE_PATTERN = re.compile(
    r"(?P<date>\d{2}/\d{2}/\d{4}).*?\b(?P<action>BOT|SOLD)\b\s+(?P<qty>[+-]?\d+)\s+"
    r"(?P<symbol>[A-Z0-9.\s]+?)\s*@\s*(?P<price>[\d.,]+).*?(?P<amount>[-\d,]+\.\d{2})",
    re.IGNORECASE,
)


def _canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_normalize_header(c) for c in df.columns]
    df.columns = [COLUMN_ALIASES.get(c, c) for c in df.columns]
    # Drop duplicate columns that may result from aliasing (prefer the first occurrence)
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def _extract_symbol(desc: str) -> str:
    m = re.search(r"\b([A-Z]{1,6})(?:\s|$|\.)", desc or "")
    return m.group(1) if m else ""


def _normalize_header(label: str) -> str:
    label = (label or "").strip().lower()
    label = label.replace("#", "number")
    label = re.sub(r"[^a-z0-9]+", "_", label)
    return label.strip("_")


def _normalize_action_label(text: Any) -> str:
    label = (text or "").strip()
    if not label:
        return ""
    label = label.upper()
    label = label.replace("&", "AND")
    label = re.sub(r"[^A-Z0-9]+", "_", label)
    return label.strip("_")


def _resolve_action(text: Any) -> Optional[str]:
    normalized = _normalize_action_label(text)
    if not normalized:
        return None
    action = TRADE_ACTION_MAP.get(normalized)
    if action:
        return action
    if "BUY" in normalized:
        return "BUY"
    if "SELL" in normalized or "SLD" in normalized:
        return "SELL"
    return None


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


def _looks_like_trade_header(headers: List[str]) -> bool:
    header_set = {h for h in headers if h}
    if not header_set:
        return False

    has_symbol = any(
        key in header_set
        for key in (
            "symbol",
            "instrument",
            "description",
            "underlying",
            "underlying_symbol",
        )
    )
    has_action = any(
        key in header_set
        for key in (
            "action",
            "type",
            "trade_type",
            "transaction_type",
            "side",
            "activity",
        )
    )
    has_quantity = any(
        key in header_set
        for key in (
            "qty",
            "quantity",
            "shares",
            "trade_quantity",
            "filled_quantity",
        )
    )
    has_price_or_amount = any(
        key in header_set
        for key in (
            "price",
            "trade_price",
            "execution_price",
            "fill_price",
            "avg_price",
            "average_price",
            "net_price",
            "amount",
            "net_amount",
            "trade_amount",
            "trade_value",
            "value",
            "gross_amount",
            "proceeds",
        )
    )

    return has_symbol and has_action and has_quantity and has_price_or_amount


def _prepare_header_mappings(row: List[str]) -> List[Tuple[str, str]]:
    mappings: List[Tuple[str, str]] = []
    for cell in row:
        normalized = _normalize_header(cell)
        canonical = COLUMN_ALIASES.get(normalized, normalized)
        mappings.append((canonical, normalized))
    return mappings


def _map_row_values(row: List[str], header_mappings: List[Tuple[str, str]]) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for idx, raw_value in enumerate(row):
        if idx >= len(header_mappings):
            break
        canonical, normalized = header_mappings[idx]
        text = raw_value.strip()
        if not text:
            continue
        if canonical:
            values.setdefault(canonical, text)
        if normalized and normalized != canonical:
            values.setdefault(normalized, text)
    return values


def _parse_datetime_from_row(data: Dict[str, str]) -> Optional[datetime]:
    date_candidates = [
        data.get("trade_date"),
        data.get("date"),
        data.get("transaction_date"),
        data.get("date_time"),
    ]
    time_candidates = [
        data.get("time"),
        data.get("trade_time"),
        data.get("exec_time"),
        data.get("execution_time"),
        data.get("transaction_time"),
        data.get("order_time"),
    ]

    for date_text in date_candidates:
        for time_text in time_candidates:
            if date_text and time_text:
                dt = _parse_datetime_guess(f"{date_text} {time_text}")
                if dt:
                    return dt

    for candidate in [*time_candidates, *date_candidates]:
        dt = _parse_datetime_guess(candidate)
        if dt:
            return dt

    for date_text in date_candidates:
        if not date_text:
            continue
        try:
            parsed = pd.to_datetime(date_text, errors="raise")
        except Exception:  # pragma: no cover - defensive fallback
            continue
        if pd.isna(parsed):
            continue
        dt = parsed.to_pydatetime() if hasattr(parsed, "to_pydatetime") else parsed
        if isinstance(dt, datetime):
            return dt

    return None


def _iter_trade_blocks(text: str) -> Iterable[Tuple[List[Tuple[str, str]], List[List[str]]]]:
    delimiter = "\t" if text.count("\t") > text.count(",") else ","
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)

    header_mappings: Optional[List[Tuple[str, str]]] = None
    rows: List[List[str]] = []

    def flush_current() -> Optional[Tuple[List[Tuple[str, str]], List[List[str]]]]:
        nonlocal header_mappings, rows
        if header_mappings and rows:
            result = (header_mappings, rows)
            header_mappings = None
            rows = []
            return result
        header_mappings = None
        rows = []
        return None

    for raw_row in reader:
        row = [cell.strip() for cell in raw_row]
        if not any(row):
            flushed = flush_current()
            if flushed:
                yield flushed
            continue

        first_cell = row[0].strip().lower() if row else ""
        if len(row) == 1 and (
            "trade history" in first_cell
            or first_cell.startswith("account ")
            or "account statement" in first_cell
        ):
            flushed = flush_current()
            if flushed:
                yield flushed
            continue

        header_candidate = _prepare_header_mappings(row)
        canonical_headers = [canonical for canonical, _ in header_candidate]
        if _looks_like_trade_header(canonical_headers):
            flushed = flush_current()
            if flushed:
                yield flushed
            header_mappings = header_candidate
            continue

        if header_mappings is None:
            continue

        rows.append(row)

    flushed = flush_current()
    if flushed:
        yield flushed

        canonical: Dict[str, str] = dict(data)
        for key, value in data.items():
            alias = COLUMN_ALIASES.get(key)
            if alias:
                if not canonical.get(alias):
                    canonical[alias] = value
        data = canonical

        symbol = data.get("symbol", "") or data.get("instrument", "")
        description = data.get("description", "")
        side_raw = data.get("action") or data.get("side") or data.get("type")
        action = _resolve_action(side_raw)

def _read_statement_rows(content: bytes) -> List[Dict[str, Any]]:
    text = _decode_text_content(content)
    if not text:
        return []

        if not symbol or not action:
            continue

        qty_val = _parse_float(
            data.get("qty")
            or data.get("quantity")
            or data.get("shares")
            or data.get("trade_quantity")
        )
        if qty_val is None or abs(qty_val) < 1e-9:
            continue
        qty_val = abs(qty_val)

        price_val = _parse_float(
            data.get("price")
            or data.get("trade_price")
            or data.get("execution_price")
            or data.get("avg_price")
            or data.get("average_price")
        )
        if price_val is None:
            price_val = _parse_float(data.get("net_price"))
        if price_val is None:
            amount_guess = _parse_float(
                data.get("amount")
                or data.get("net_amount")
                or data.get("trade_amount")
                or data.get("trade_value")
                or data.get("value")
                or data.get("gross_amount")
                or data.get("proceeds")
            )
            if amount_guess is not None and abs(qty_val) > 1e-9:
                price_val = abs(amount_guess) / qty_val
        if price_val is None:
            continue

        exec_time = (
            data.get("time")
            or data.get("exec_time")
            or data.get("trade_time")
            or data.get("transaction_time")
            or data.get("execution_time")
            or data.get("order_time")
        )
        trade_date_text = data.get("trade_date") or data.get("date")
        dt_exec = _parse_datetime_guess(exec_time)
        if dt_exec is None:
            dt_exec = _parse_datetime_guess(trade_date_text)
        if dt_exec is None:
            continue

            day = dt_exec.strftime("%Y-%m-%d")

        amount_val = _parse_float(
            data.get("amount")
            or data.get("net_amount")
            or data.get("trade_amount")
            or data.get("trade_value")
            or data.get("value")
            or data.get("gross_amount")
            or data.get("proceeds")
        )
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

            rows_with_order.append((dt_exec, order, trade))
            order += 1

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
        raw_action = r.get("action", "") or r.get("side", "") or r.get("type", "")
        action = _resolve_action(raw_action)
        desc = str(r.get("description", "")).upper().strip()

        if not action and desc:
            if any(token in desc for token in ("BOUGHT", "BOT", "BTO")):
                action = "BUY"
            elif any(token in desc for token in ("SOLD", "SLD", "STC")):
                action = "SELL"

        if not action:
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

        norm_action = _resolve_action(action)
        if not norm_action:
            norm_action = action

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

        amount_val = _parse_float(
            amount
            if amount is not None
            else r.get("trade_amount", None)
            or r.get("trade_value", None)
            or r.get("value", None)
            or r.get("gross_amount", None)
            or r.get("proceeds", None)
        )
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


def _deduplicate_trades(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return []

    seen = set()
    unique_rows: List[Dict[str, Any]] = []
    for trade in rows:
        key = (
            trade.get("date"),
            (trade.get("symbol") or "").upper(),
            (trade.get("action") or "").upper(),
            round(float(trade.get("qty", 0.0)), 8),
            round(float(trade.get("price", 0.0)), 8),
            round(float(trade.get("amount", 0.0)), 8),
        )
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append(trade)

    return unique_rows


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
        return _deduplicate_trades(section_rows)

    plaintext_rows = _parse_plaintext_statement(content)
    if plaintext_rows:
        return _deduplicate_trades(plaintext_rows)

    rows = _read_statement_rows(content)
    if rows:
        return _deduplicate_trades(rows)

    return _deduplicate_trades(_parse_dataframe(content))


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
