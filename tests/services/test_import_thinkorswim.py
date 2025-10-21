import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.services.import_thinkorswim import parse_thinkorswim_csv


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


def test_parse_multiple_trade_tables_in_single_file():
    content = """Account Statement,,,,,
Account: 99999999,,,,,
Trade History,,,,,
Trade Date,Time,Action,Quantity,Symbol,Description,Price,Amount
10/21/2025,09:31:00,Buy,1,AAPL,Apple Inc,$150.00,-$150.00
10/21/2025,10:15:00,Sell,1,AAPL,Apple Inc,$152.00,$152.00

Trade Date,Time,Action,Quantity,Symbol,Description,Price,Amount
10/22/2025,11:05:00,Buy,2,MSFT,Microsoft Corp,$310.00,-$620.00
10/22/2025,15:45:00,Sell,2,MSFT,Microsoft Corp,$315.50,$631.00
""".encode("utf-8")

    rows = parse_thinkorswim_csv(content)

    assert rows == [
        {
            "date": "2025-10-21",
            "symbol": "AAPL",
            "action": "BUY",
            "qty": 1.0,
            "price": 150.0,
            "amount": -150.0,
        },
        {
            "date": "2025-10-21",
            "symbol": "AAPL",
            "action": "SELL",
            "qty": 1.0,
            "price": 152.0,
            "amount": 152.0,
        },
        {
            "date": "2025-10-22",
            "symbol": "MSFT",
            "action": "BUY",
            "qty": 2.0,
            "price": 310.0,
            "amount": -620.0,
        },
        {
            "date": "2025-10-22",
            "symbol": "MSFT",
            "action": "SELL",
            "qty": 2.0,
            "price": 315.5,
            "amount": 631.0,
        },
    ]


def test_parse_header_with_additional_metadata_columns():
    content = """Section,,,
Trade Date,Order Number,Trade Time,Transaction Type,Instrument,Description,Quantity,Trade Price,Net Amount
10/23/2025,ABC123,13:05:00,Buy,GOOG,Alphabet Inc,3,2800.00,-8400.00
10/24/2025,XYZ999,09:42:00,Sell,GOOG,Alphabet Inc,3,2825.50,8476.50
""".encode("utf-8")

    rows = parse_thinkorswim_csv(content)

    assert rows == [
        {
            "date": "2025-10-23",
            "symbol": "GOOG",
            "action": "BUY",
            "qty": 3.0,
            "price": 2800.0,
            "amount": -8400.0,
        },
        {
            "date": "2025-10-24",
            "symbol": "GOOG",
            "action": "SELL",
            "qty": 3.0,
            "price": 2825.5,
            "amount": 8476.5,
        },
    ]
