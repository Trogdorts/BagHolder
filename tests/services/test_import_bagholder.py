import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.services.import_bagholder import parse_bagholder_csv


def test_parse_bagholder_csv_basic():
    content = """date,realized,unrealized,total_invested,updated_at
2024-02-01,100.50,50.25,75.00,2024-02-01T12:00:00
2024-02-02,-10.00,0,,
""".encode("utf-8")

    rows = parse_bagholder_csv(content)

    assert rows == [
        {
            "date": "2024-02-01",
            "realized": 100.5,
            "unrealized": 50.25,
            "total_invested": 75.0,
            "updated_at": "2024-02-01T12:00:00",
        },
        {
            "date": "2024-02-02",
            "realized": -10.0,
            "unrealized": 0.0,
            "total_invested": 0.0,
            "updated_at": "",
        },
    ]


def test_parse_bagholder_csv_handles_bom_and_aliases():
    content = "\ufeffDate,Realized PnL,Unrealized PnL,Total,Updated At\n02/03/2024,$25.00,$5.00,,2024-02-03T10:00:00\n".encode(
        "utf-8"
    )

    rows = parse_bagholder_csv(content)

    assert rows == [
        {
            "date": "2024-02-03",
            "realized": 25.0,
            "unrealized": 5.0,
            "total_invested": 5.0,
            "updated_at": "2024-02-03T10:00:00",
        }
    ]


def test_parse_bagholder_csv_skips_invalid_rows():
    content = """date,realized,unrealized,total_invested,updated_at
,10,20,30,
invalid,5,5,5,
""".encode("utf-8")

    rows = parse_bagholder_csv(content)

    assert rows == []
