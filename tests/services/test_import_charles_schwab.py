from pathlib import Path
import sys

import pytest

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.services.import_charles_schwab import parse_charles_schwab_csv  # noqa: E402


def test_parse_charles_schwab_csv_splits_trades_and_dividends():
    content = """Date,Action,Symbol / Description,Quantity,Price,Fees & Comm,Amount
10/27/2025,Buy,CODX CO-DIAGNOSTICS INC,100,1.25,0.12,($125.12)
10/27/2025,Sell,CODX CO-DIAGNOSTICS INC,50,1.40,0.05,$69.95
10/28/2025,Reinvest Shares,GDXY YIELDMAX GOLD,10,1.00,0.00,($10.00)
10/27/2025,Cash Dividend,GDXY YIELDMAX GOLD, , , ,$43.41
10/27/2025,MoneyLink Transfer,, , , ,($1000.00)
""".encode('utf-8')

    trades, dividends = parse_charles_schwab_csv(content)
    assert len(trades) == 3
    assert len(dividends) == 1

    buy_trade = next(row for row in trades if row['action'] == 'BUY')
    sell_trade = next(row for row in trades if row['action'] == 'SELL')
    reinvest_trade = next(row for row in trades if row['qty'] == 10.0)

    assert buy_trade['symbol'] == 'CODX'
    assert buy_trade['qty'] == 100.0
    assert buy_trade['price'] == 1.25
    assert buy_trade['fee'] == 0.12
    assert buy_trade['amount'] == pytest.approx(-125.12)

    assert sell_trade['amount'] == pytest.approx(69.95)
    assert reinvest_trade['action'] == 'BUY'
    assert reinvest_trade['amount'] == pytest.approx(-10.0)

    dividend = dividends[0]
    assert dividend['symbol'] == 'GDXY'
    assert dividend['action'] == 'Cash Dividend'
    assert dividend['amount'] == pytest.approx(43.41)
