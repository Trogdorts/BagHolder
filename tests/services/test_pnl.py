import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from datetime import date

from app.services.pnl import compute_daily_pnl_records


def test_compute_daily_pnl_records_basic_long_flow():
    records = [
        {
            "date": "2024-02-05",
            "symbol": "AAPL",
            "side": "BUY",
            "quantity": 10,
            "price": 150,
        },
        {
            "date": "2024-02-05",
            "symbol": "AAPL",
            "side": "SELL",
            "quantity": 5,
            "price": 155,
        },
    ]

    daily = compute_daily_pnl_records(records)

    assert daily.to_dict("records") == [
        {
            "date": date(2024, 2, 5),
            "realized_pl": 25.0,
            "trade_value": 2275.0,
            "total_pl": 25.0,
            "cumulative_pl": 25.0,
        }
    ]


def test_compute_daily_pnl_records_handles_full_close():
    records = [
        {
            "date": "2024-02-05",
            "symbol": "AAPL",
            "side": "BUY",
            "quantity": 10,
            "price": 100,
        },
        {
            "date": "2024-02-06",
            "symbol": "AAPL",
            "side": "SELL",
            "quantity": 10,
            "price": 110,
        },
    ]

    daily = compute_daily_pnl_records(records)

    assert daily.to_dict("records") == [
        {
            "date": date(2024, 2, 5),
            "realized_pl": 0.0,
            "trade_value": 1000.0,
            "total_pl": 0.0,
            "cumulative_pl": 0.0,
        },
        {
            "date": date(2024, 2, 6),
            "realized_pl": 100.0,
            "trade_value": 1100.0,
            "total_pl": 100.0,
            "cumulative_pl": 100.0,
        },
    ]


def test_compute_daily_pnl_records_supports_short_positions():
    records = [
        {
            "date": "2024-02-05",
            "symbol": "TSLA",
            "side": "SELL",
            "quantity": 10,
            "price": 200,
        },
        {
            "date": "2024-02-06",
            "symbol": "TSLA",
            "side": "BUY",
            "quantity": 4,
            "price": 180,
        },
    ]

    daily = compute_daily_pnl_records(records)

    assert daily.to_dict("records") == [
        {
            "date": date(2024, 2, 5),
            "realized_pl": 0.0,
            "trade_value": 2000.0,
            "total_pl": 0.0,
            "cumulative_pl": 0.0,
        },
        {
            "date": date(2024, 2, 6),
            "realized_pl": 80.0,
            "trade_value": 720.0,
            "total_pl": 80.0,
            "cumulative_pl": 80.0,
        },
    ]
