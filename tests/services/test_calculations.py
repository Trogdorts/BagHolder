import sys
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from datetime import date

from app.services.calculations import Ledger, count_trade_win_losses


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


def test_ledger_respects_lifo_method():
    ledger = Ledger(method="lifo")
    trades = [
        {"date": "2024-06-01", "symbol": "ABC", "action": "BUY", "qty": 1, "price": 10.0},
        {"date": "2024-06-01", "symbol": "ABC", "action": "BUY", "qty": 1, "price": 20.0},
        {"date": "2024-06-02", "symbol": "ABC", "action": "SELL", "qty": 1, "price": 15.0},
        {"date": "2024-06-03", "symbol": "ABC", "action": "SELL", "qty": 1, "price": 25.0},
    ]

    realized = ledger.apply(trades)

    assert realized["2024-06-02"] == pytest.approx(-5.0)
    assert realized["2024-06-03"] == pytest.approx(15.0)


def test_ledger_accounts_for_fees():
    ledger = Ledger()
    trades = [
        {"date": "2024-07-01", "symbol": "FEE", "action": "BUY", "qty": 1, "price": 10.0, "fee": 1.0},
        {"date": "2024-07-02", "symbol": "FEE", "action": "SELL", "qty": 1, "price": 15.0, "fee": 2.0},
    ]

    realized = ledger.apply(trades)

    assert realized["2024-07-02"] == pytest.approx(2.0)


def test_count_trade_win_losses_classifies_days_by_net_realized_pnl():
    trades = [
        {"date": date(2024, 3, 1), "symbol": "ABC", "action": "BUY", "qty": 10, "price": 10.0},
        {"date": date(2024, 3, 2), "symbol": "ABC", "action": "SELL", "qty": 10, "price": 12.0},
        {"date": date(2024, 3, 3), "symbol": "ABC", "action": "BUY", "qty": 5, "price": 10.0},
        {"date": date(2024, 3, 4), "symbol": "ABC", "action": "SELL", "qty": 5, "price": 8.0},
    ]

    wins, losses = count_trade_win_losses(trades)

    assert wins == 1
    assert losses == 1


def test_count_trade_win_losses_ignores_days_with_offsetting_trades():
    trades = [
        {"date": date(2024, 4, 1), "symbol": "XYZ", "action": "BUY", "qty": 10, "price": 10.0},
        {"date": date(2024, 4, 2), "symbol": "XYZ", "action": "SELL", "qty": 5, "price": 12.0},
        {"date": date(2024, 4, 2), "symbol": "XYZ", "action": "SELL", "qty": 5, "price": 8.0},
    ]

    wins, losses = count_trade_win_losses(trades)

    assert wins == 0
    assert losses == 0


def test_count_trade_win_losses_respects_date_window():
    trades = [
        {"date": date(2024, 5, 1), "symbol": "LMN", "action": "BUY", "qty": 10, "price": 10.0},
        {"date": date(2024, 5, 2), "symbol": "LMN", "action": "SELL", "qty": 5, "price": 12.0},
        {"date": date(2024, 5, 3), "symbol": "LMN", "action": "SELL", "qty": 5, "price": 8.0},
    ]

    wins, losses = count_trade_win_losses(
        trades, start=date(2024, 5, 2), end=date(2024, 5, 2)
    )

    assert wins == 1
    assert losses == 0
