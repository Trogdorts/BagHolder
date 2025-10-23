# BagHolder

FastAPI + Jinja2 + HTMX + Alpine.js app to visualize realized and unrealized P&L on a calendar.
Imports ThinkOrSwim (TOS) Account Statement CSVs. No personally identifying account fields are stored.

## Quick start

```bash
docker compose up --build
# open http://localhost:8012
```

## Manual dev

```bash
python -m pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8012
```

## Data

A volume is mounted at `/app/data` (or `./app/data` in local dev) containing:
- `profitloss.db` (SQLite)
- `config.yaml`

## Import

- Open **Settings → Stock data importing**
- Choose **ThinkOrSwim → Account Statement** CSV
- Only trade fields are persisted: date, symbol, action, qty, price, amount
- Account numbers or names are discarded in-memory after parsing

## Notes

Daily/weekly/monthly notes are accessible via faint note icons. Autosave enabled by default.

## Settings

Theme defaults to **dark**. Toggle numeric text visibility, default month view, and note settings.

## Support

If this project has been useful, consider [buying me a coffee](https://buymeacoffee.com/crissejdav6).
