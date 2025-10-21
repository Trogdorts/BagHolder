from datetime import datetime, date
from collections import defaultdict
from typing import Dict, List, Tuple

def group_by_week(daily: Dict[str, Dict[str, float]]) -> Dict[Tuple[int,int], Dict[str,float]]:
    # daily: date -> {realized, unrealized, invested}
    agg = defaultdict(lambda: {"realized":0.0,"unrealized":0.0,"total_invested":0.0})
    for d, vals in daily.items():
        y,m,day = map(int, d.split("-"))
        iso = date(y,m,int(day)).isocalendar()
        key = (iso[0], iso[1])
        agg[key]["realized"] += vals.get("realized",0.0)
        agg[key]["unrealized"] += vals.get("unrealized",0.0)
        agg[key]["total_invested"] += vals.get("total_invested",0.0)
    return dict(agg)
