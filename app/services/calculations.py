from collections import defaultdict, deque
from datetime import datetime
from typing import List, Dict, Any

class Ledger:
    def __init__(self):
        self.pos = defaultdict(deque)  # symbol -> deque of (qty_remaining, unit_cost)
        self.realized_by_date = defaultdict(float)
        self.unrealized_by_date = defaultdict(float)

    def apply(self, trades: List[Dict[str, Any]]):
        # trades sorted by datetime
        last_date = None
        for t in trades:
            d = t["date"]
            if isinstance(d, str):
                day = d
            else:
                day = d.strftime("%Y-%m-%d")
            action = t["action"].upper()
            symbol = t["symbol"].upper()
            qty = float(t["qty"])
            price = float(t["price"])
            if action == "BUY":
                self.pos[symbol].append([qty, price])
            elif action == "SELL":
                remaining = qty
                cost_total = 0.0
                proceeds = qty * price
                while remaining > 1e-9 and self.pos[symbol]:
                    lot_qty, lot_cost = self.pos[symbol][0]
                    take = min(remaining, lot_qty)
                    cost_total += take * lot_cost
                    lot_qty -= take
                    remaining -= take
                    if lot_qty <= 1e-9:
                        self.pos[symbol].popleft()
                    else:
                        self.pos[symbol][0][0] = lot_qty
                # If short sell without position, treat cost as 0 (realized equals proceeds)
                realized = proceeds - cost_total
                self.realized_by_date[day] += realized

            # compute unrealized as carried cost at end of the day
            # we will recompute per symbol after each trade within the day
            last_date = day
            self.unrealized_by_date[day] = sum(
                qty * cost for lots in self.pos.values() for qty, cost in lots
            )

        # Carry forward unrealized for days without trades if needed handled at summarization
        return self.realized_by_date, self.unrealized_by_date
