import sys
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.services.calculations import Ledger


def test_ledger_tracks_realized_profit_with_remaining_position():
    ledger = Ledger()
    trades = [
        {"date": "2024-01-02", "symbol": "AAPL", "action": "BUY", "qty": 100, "price": 1.0},
        {"date": "2024-01-03", "symbol": "AAPL", "action": "SELL", "qty": 50, "price": 1.2},
    ]

    realized = ledger.apply(trades)

    assert realized["2024-01-03"] == pytest.approx(10.0)


def test_ledger_registers_loss_for_sell_transactions():
    ledger = Ledger()
    trades = [
        {"date": "2024-02-01", "symbol": "XYZ", "action": "BUY", "qty": 100, "price": 1.0},
        {"date": "2024-02-02", "symbol": "XYZ", "action": "SELL", "qty": 1, "price": 0.99},
    ]

    realized = ledger.apply(trades)

    assert realized["2024-02-02"] == pytest.approx(-0.01)
