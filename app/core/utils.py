from datetime import datetime, date
from calendar import monthrange
from typing import Tuple


def coerce_bool(value, default: bool = True) -> bool:
    """Best-effort conversion of truthy configuration values to booleans."""

    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
        return default
    if value is None:
        return default
    try:
        return bool(int(value))
    except (TypeError, ValueError):
        return bool(value)

def ymd(dt):
    if isinstance(dt, (datetime,)):
        return dt.strftime("%Y-%m-%d")
    if isinstance(dt, date):
        return dt.strftime("%Y-%m-%d")
    return str(dt)

def month_bounds(year: int, month: int) -> Tuple[str, str, int]:
    _, days = monthrange(year, month)
    start = f"{year:04d}-{month:02d}-01"
    end = f"{year:04d}-{month:02d}-{days:02d}"
    return start, end, days
