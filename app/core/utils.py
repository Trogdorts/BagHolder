from datetime import datetime, date
from calendar import monthrange
from typing import Tuple

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
