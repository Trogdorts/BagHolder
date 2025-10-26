import sys
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.services.calculations import Ledger


def test_ledger_tracks_unrealized_profit_with_remaining_position():
    ledger = Ledger()
    trades = [
        {"date": "2024-01-02", "symbol": "AAPL", "action": "BUY", "qty": 100, "price": 1.0},
        {"date": "2024-01-03", "symbol": "AAPL", "action": "SELL", "qty": 50, "price": 1.2},
    ]

    realized, unrealized = ledger.apply(trades)

    assert realized["2024-01-03"] == pytest.approx(10.0)
    assert unrealized["2024-01-03"] == pytest.approx(10.0)
    assert unrealized["2024-01-02"] == pytest.approx(0.0)


def test_ledger_registers_loss_when_price_moves_against_remaining_shares():
    ledger = Ledger()
    trades = [
        {"date": "2024-02-01", "symbol": "XYZ", "action": "BUY", "qty": 100, "price": 1.0},
        {"date": "2024-02-02", "symbol": "XYZ", "action": "SELL", "qty": 1, "price": 0.99},
    ]

    realized, unrealized = ledger.apply(trades)

    assert realized["2024-02-02"] == pytest.approx(-0.01)
    assert unrealized["2024-02-02"] == pytest.approx(-0.99)
