# BagHolder (Alpha 1)

FastAPI + Jinja2 + HTMX + Alpine.js app to visualize realized performance on a calendar.
Imports ThinkOrSwim (TOS) Account Statement CSVs. No personally identifying account fields are stored.

The **Alpha 1** milestone focuses on containerized deployment and smoother
first-run experiences when provisioning the administrator account.

## Quick start

```bash
docker compose up --build
# open http://localhost:8012
```

### Docker deployment notes

The project ships with a Dockerfile optimized for production usage. Build
arguments allow matching the container UID/GID to the host so that bind-mounted
volumes remain writable:

```bash
APP_VERSION=0.1.0-alpha.1 BAGHOLDER_UID=$(id -u) BAGHOLDER_GID=$(id -g) \
  docker compose up --build -d
```

Environment variables can be stored in a local `.env` file that sits alongside
`docker-compose.yml`. Useful keys include:

```dotenv
BAGHOLDER_SECRET_KEY=change-me
BAGHOLDER_BOOTSTRAP_USERNAME=admin
BAGHOLDER_BOOTSTRAP_PASSWORD=super-secret-password
```

When both bootstrap variables are provided the container automatically creates
the initial administrator account on startup. If `BAGHOLDER_SECRET_KEY` is not
set the server generates a random, in-memory secret at launch; this allows
quick experiments but invalidates existing sessions whenever the process
restarts.

## Manual dev

```bash
python -m pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8012
```

## User accounts

- Visit `/login` to sign in. The application requires authentication for all pages and API endpoints.
- When no users exist you may create the first account directly on the login screen. That account is promoted to administrator.
- To provision the administrator ahead of time, run the bootstrap helper:

  ```bash
  python -m app.scripts.first_start --username admin --password "your-strong-password"
  ```

  Omitting `--password` launches an interactive prompt with confirmation.

## Data and diagnostics

By default the application stores its configuration, logs, and database under
`app/data` inside the repository. Override the location by setting the
`BAGHOLDER_DATA` environment variable before starting the server.

- `BAGHOLDER_SECRET_KEY` customizes the session signing key. Always set a strong
  value in production deployments to keep sessions valid across restarts.
- `BAGHOLDER_BOOTSTRAP_USERNAME` and `BAGHOLDER_BOOTSTRAP_PASSWORD` enable
  unattended administrator provisioning (e.g., Docker deployments).
- `BAGHOLDER_SESSION_SECURE` forces the session cookie to require HTTPS when set
  to a truthy value (recommended for TLS-enabled deployments).
- `profitloss.db` (SQLite)
- `config.yaml`
- `logs/bagholder.log`

Set `BAGHOLDER_DEBUG_LOGGING=true` to force verbose debug logging regardless of
the value stored in `config.yaml`.

To wipe the data directory for a fresh test environment run:

```bash
python -m app.scripts.reset_data --force
```

Add `--data-dir <path>` to target a custom location. The script recreates the
directory and writes a pristine `config.yaml` so the next server start behaves
like a first-time launch.

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

## Contact

- [Reddit: r/bagholder_dev](https://www.reddit.com/r/bagholder_dev)
