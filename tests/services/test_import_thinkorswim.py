import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from datetime import date

import pytest

from app.services.import_thinkorswim import (
    compute_daily_pnl_records,
    parse_thinkorswim_csv,
)


def test_parse_statement_trade_history_section():
    content = """Account Statement,,,,,
Account: 12345678,,,,,
Trade History,,,,,
Trade Date,Time,Action,Quantity,Symbol,Description,Price,Amount
02/05/2024,09:31:00,Buy,1,AAPL,Apple Inc,$150.00,-$150.00
02/05/2024,10:15:00,Sell,1,AAPL,Apple Inc,$152.00,$152.00
""".encode("utf-8")

    rows = parse_thinkorswim_csv(content)

    assert rows == [
        {
            "date": "2024-02-05",
            "symbol": "AAPL",
            "action": "BUY",
            "qty": 1.0,
            "price": 150.0,
            "amount": -150.0,
        },
        {
            "date": "2024-02-05",
            "symbol": "AAPL",
            "action": "SELL",
            "qty": 1.0,
            "price": 152.0,
            "amount": 152.0,
        },
    ]


def test_parse_simple_csv_without_section_heading():
    content = """Trade Date,Action,Symbol,Qty,Price,Amount
02/06/2024,BUY,MSFT,10,315.50,-3155.00
02/07/2024,SELL,MSFT,5,320.00,1600.00
""".encode("utf-8")

    rows = parse_thinkorswim_csv(content)

    assert rows == [
        {
            "date": "2024-02-06",
            "symbol": "MSFT",
            "action": "BUY",
            "qty": 10.0,
            "price": 315.5,
            "amount": -3155.0,
        },
        {
            "date": "2024-02-07",
            "symbol": "MSFT",
            "action": "SELL",
            "qty": 5.0,
            "price": 320.0,
            "amount": 1600.0,
        },
    ]


def test_parse_plaintext_statement():
    content = (
        "10/21/2025 BOT +1 AAPL @190.00 -190.00 10,000.00\n"
        "10/22/2025 SOLD -1 AAPL @192.50 192.50 10,192.50\n"
    ).encode("utf-16")

    rows = parse_thinkorswim_csv(content)

    assert rows == [
        {
            "date": "2025-10-21",
            "symbol": "AAPL",
            "action": "BUY",
            "qty": 1.0,
            "price": 190.0,
            "amount": -190.0,
        },
        {
            "date": "2025-10-22",
            "symbol": "AAPL",
            "action": "SELL",
            "qty": 1.0,
            "price": 192.5,
            "amount": 192.5,
        },
    ]


def test_parse_trade_history_with_alternate_headers():
    content = """Account Statement,,,,,
Account: 12345678,,,,,
Trade History,,,,,
Trade Date,Trade Time,Type,Instrument,Quantity,Trade Price,Trade Amount
10/21/2025,14:32:00,Bought,NERV,200,8.8101,"-$1,762.02"
10/21/2025,15:10:00,Sold,NERV,200,8.8101,"$1,762.02"
""".encode("utf-8")

    rows = parse_thinkorswim_csv(content)

    assert rows == [
        {
            "date": "2025-10-21",
            "symbol": "NERV",
            "action": "BUY",
            "qty": 200.0,
            "price": 8.8101,
            "amount": -1762.02,
        },
        {
            "date": "2025-10-21",
            "symbol": "NERV",
            "action": "SELL",
            "qty": 200.0,
            "price": 8.8101,
            "amount": 1762.02,
        },
    ]


def test_parse_trade_history_section_with_blank_line():
    content = """Account Statement,,,,,
Account: 12345678,,,,,
Account Trade History,,,,,

Trade Date,Time,Action,Quantity,Symbol,Description,Price,Amount
02/05/2024,09:31:00,Buy,1,AAPL,Apple Inc,$150.00,-$150.00
""".encode("utf-8")

    rows = parse_thinkorswim_csv(content)

    assert rows == [
        {
            "date": "2024-02-05",
            "symbol": "AAPL",
            "action": "BUY",
            "qty": 1.0,
            "price": 150.0,
            "amount": -150.0,
        }
    ]


def test_parse_deduplicates_identical_rows():
    content = """Trade Date,Action,Symbol,Qty,Price,Amount
02/06/2024,BUY,MSFT,10,315.50,-3155.00
02/06/2024,BUY,MSFT,10,315.50,-3155.00
02/07/2024,SELL,MSFT,5,320.00,1600.00
02/07/2024,SELL,MSFT,5,320.00,1600.00
""".encode("utf-8")

    rows = parse_thinkorswim_csv(content)

    assert rows == [
        {
            "date": "2024-02-06",
            "symbol": "MSFT",
            "action": "BUY",
            "qty": 10.0,
            "price": 315.5,
            "amount": -3155.0,
        },
        {
            "date": "2024-02-07",
            "symbol": "MSFT",
            "action": "SELL",
            "qty": 5.0,
            "price": 320.0,
            "amount": 1600.0,
        },
    ]


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
