from pathlib import Path
import sys

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.services.trade_simulator import SimulationOptions, simulate_trades


def test_simulate_trades_handles_short_history():
    prices = [20] * 8 + [19, 18, 17, 16, 15, 14, 13] + [14, 15, 16, 17, 18, 19, 20, 21]
    dates = pd.bdate_range("2024-01-01", periods=len(prices))
    frame = pd.DataFrame({"Date": dates, "Close": prices})

    options = SimulationOptions(
        months_back=1,
        start_balance=1_000.0,
        risk_level=1.0,
        seed=1,
    )

    trades = simulate_trades({"TEST": frame}, options)

    assert not trades.empty
    assert {"BUY", "SELL"}.issubset(set(trades["action"]))
