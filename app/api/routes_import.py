import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.core.config import DEFAULT_CONFIG
from app.core.database import get_session
from app.core.logger import get_logger
from app.core.models import DailySummary, NoteDaily, Trade
from app.services.import_thinkorswim import parse_thinkorswim_csv
from app.services.import_trades_csv import parse_trade_csv
from app.services.trade_summaries import calculate_daily_trade_map, upsert_daily_summaries

router = APIRouter()
log = get_logger(__name__)


_NOTE_PREFIX_PATTERN = re.compile(r"^\[\s*(BUY|SELL)\b", re.IGNORECASE)


def _is_close(a: float, b: float, tol: float = 0.01) -> bool:
    return abs(float(a) - float(b)) <= tol


def _is_missing_summary(summary: DailySummary) -> bool:
    """Return ``True`` when a summary appears to be an empty placeholder."""

    if summary is None:
        return True

    updated = (summary.updated_at or "").strip()
    if updated:
        return False

    realized = float(summary.realized or 0.0)
    invested = float(summary.total_invested or 0.0)
    return _is_close(realized, 0.0) and _is_close(invested, 0.0)


def _import_config(request: Request) -> Tuple[int, Set[str]]:
    cfg = getattr(request.app.state, "config", None)
    default_import_cfg = DEFAULT_CONFIG.get("import", {}) if isinstance(DEFAULT_CONFIG, dict) else {}

    raw_default_max = default_import_cfg.get("max_upload_bytes", 5_000_000)
    try:
        default_max_bytes = int(raw_default_max)
    except (TypeError, ValueError):
        default_max_bytes = 5_000_000
    default_max_bytes = max(1, default_max_bytes)

    default_formats_raw = default_import_cfg.get("accepted_formats", [".csv"])
    default_formats: Set[str] = set()
    if isinstance(default_formats_raw, (list, tuple, set)):
        for ext in default_formats_raw:
            if not isinstance(ext, str):
                continue
            normalized = ext.strip().lower()
            if not normalized:
                continue
            if not normalized.startswith("."):
                normalized = f".{normalized}"
            default_formats.add(normalized)
    if not default_formats:
        default_formats = {".csv"}

    if not cfg:
        return default_max_bytes, default_formats

    try:
        raw_cfg = cfg.raw  # type: ignore[attr-defined]
    except AttributeError:  # pragma: no cover - defensive fallback
        return default_max_bytes, default_formats

    import_cfg = raw_cfg.get("import", {}) if isinstance(raw_cfg, dict) else {}
    max_bytes = import_cfg.get("max_upload_bytes", default_max_bytes)
    try:
        max_bytes = int(max_bytes)
    except (TypeError, ValueError):
        max_bytes = default_max_bytes

    accepted = import_cfg.get("accepted_formats", default_formats)
    if isinstance(accepted, (list, tuple, set)):
        accepted_formats: Set[str] = set()
        for ext in accepted:
            if not isinstance(ext, str):
                continue
            normalized = ext.strip().lower()
            if not normalized:
                continue
            if not normalized.startswith("."):
                normalized = f".{normalized}"
            accepted_formats.add(normalized)
        if not accepted_formats:
            accepted_formats = default_formats
    else:
        accepted_formats = default_formats

    return max(1, max_bytes), accepted_formats


async def _read_upload(
    upload: UploadFile,
    allowed_formats: Iterable[str],
    max_bytes: int,
) -> bytes:
    filename = upload.filename or ""
    suffix = Path(filename).suffix.lower()
    allowed_set = {ext.lower() for ext in allowed_formats}
    if allowed_set and suffix not in allowed_set:
        log.warning(
            "Rejected import due to unsupported extension (filename=%s, allowed=%s)",
            filename,
            sorted(allowed_formats),
        )
        raise HTTPException(status_code=400, detail="Unsupported file format")

    try:
        await upload.seek(0)
    except Exception as exc:  # pragma: no cover - defensive fallback
        log.error("Failed to seek upload %s: %s", filename, exc)
        raise HTTPException(status_code=400, detail="Invalid upload state") from exc

    # ``UploadFile`` streams content to a SpooledTemporaryFile so reading more than
    # ``max_bytes + 1`` ensures we can detect oversized uploads without loading an
    # unbounded payload into memory.
    content = await upload.read(max_bytes + 1)
    if len(content) > max_bytes:
        log.warning(
            "Rejected import due to oversized payload (filename=%s, size=%s, limit=%s)",
            filename,
            len(content),
            max_bytes,
        )
        raise HTTPException(status_code=413, detail="Uploaded file is too large")
    return content


@router.get("/import", response_class=RedirectResponse)
def import_page(request: Request):
    return RedirectResponse("/settings#stock-data-import", status_code=307)


def _persist_trade_rows(db: Session, rows):
    if not rows:
        return 0

    notes_present = any("note" in row for row in rows)
    note_lines_by_date: Dict[str, List[str]] = {}
    empty_note_dates: Set[str] = set()
    if notes_present:
        for row in rows:
            if "note" not in row:
                continue
            date_str = row["date"]
            raw_note = row.get("note", "") or ""
            if raw_note.strip():
                action_label = str(row.get("action", "")).upper() or "BUY"
                qty_value = float(row.get("qty", 0) or 0)
                price_value = float(row.get("price", 0) or 0)
                qty_text = (
                    str(int(qty_value))
                    if qty_value.is_integer()
                    else f"{qty_value:.2f}".rstrip("0").rstrip(".")
                )
                price_text = f"{price_value:.2f}"
                note_prefix = f"[ {action_label} - {qty_text} x ${price_text} ]"
                cleaned_note = raw_note.replace("\r\n", "\n").strip()
                if cleaned_note:
                    if _NOTE_PREFIX_PATTERN.match(cleaned_note):
                        formatted_note = cleaned_note
                    else:
                        formatted_note = f"{note_prefix} {cleaned_note}"
                else:
                    formatted_note = note_prefix
                note_lines_by_date.setdefault(date_str, []).append(formatted_note)
                if date_str in empty_note_dates:
                    empty_note_dates.discard(date_str)
            elif date_str not in note_lines_by_date:
                empty_note_dates.add(date_str)

    deduped_rows = []
    seen = set()
    for row in rows:
        key = (
            row["date"],
            row["symbol"],
            row["action"],
            float(row["qty"]),
            float(row["price"]),
            float(row["amount"]),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped_rows.append(
            {
                "date": row["date"],
                "symbol": row["symbol"],
                "action": row["action"],
                "qty": float(row["qty"]),
                "price": float(row["price"]),
                "amount": float(row["amount"]),
            }
        )

    if not deduped_rows:
        return 0

    affected_dates = {row["date"] for row in deduped_rows}
    if affected_dates:
        existing_rows = (
            db.query(Trade)
            .filter(Trade.date.in_(affected_dates))
            .all()
        )
        for trade in existing_rows:
            db.delete(trade)
        if existing_rows:
            db.flush()

    inserted = 0
    for row in deduped_rows:
        db.add(Trade(**row))
        inserted += 1

    if note_lines_by_date or empty_note_dates:
        timestamp = datetime.utcnow().isoformat()
        for date_str in sorted(set(note_lines_by_date) | empty_note_dates):
            if date_str in note_lines_by_date:
                note_text = "\n\n".join(note_lines_by_date[date_str])
            else:
                note_text = ""
            record = db.get(NoteDaily, date_str)
            if record:
                if note_text:
                    existing = (record.note or "").rstrip()
                    record.note = f"{existing}\n\n{note_text}".strip() if existing else note_text
                else:
                    record.note = ""
                record.is_markdown = False
                record.updated_at = timestamp
            else:
                db.add(
                    NoteDaily(
                        date=date_str,
                        note=note_text,
                        is_markdown=False,
                        updated_at=timestamp,
                    )
                )

    db.commit()
    log.info("Persisted %s trades to database", inserted)
    return inserted


def _finalize_trade_import(request: Request, db: Session, inserted: int):
    all_trades = db.query(Trade).order_by(Trade.date.asc(), Trade.id.asc()).all()
    daily_map = calculate_daily_trade_map(all_trades)

    now = datetime.utcnow().isoformat()
    conflicts = []
    resolved: Dict[str, Dict[str, float]] = {}
    for day, values in daily_map.items():
        realized = values["realized"]
        invested = values.get("total_invested", 0.0)
        ds = db.get(DailySummary, day)
        if _is_missing_summary(ds):
            resolved[day] = values
            continue

        if _is_close(ds.realized, realized) and _is_close(ds.total_invested, invested):
            resolved[day] = values
        else:
            conflicts.append(
                {
                    "date": day,
                    "existing": {
                        "realized": float(ds.realized),
                        "invested": float(ds.total_invested),
                        "updated_at": ds.updated_at,
                    },
                    "new": {
                        "realized": realized,
                        "invested": invested,
                    },
                }
            )

    if resolved:
        upsert_daily_summaries(db, resolved, timestamp=now)
    db.commit()

    if conflicts:
        log.warning(
            "Import conflicts detected for %s day(s): %s", len(conflicts), [c["date"] for c in conflicts]
        )
        return request.app.state.templates.TemplateResponse(
            request,
            "import_conflicts.html",
            {
                "request": request,
                "conflicts": conflicts,
                "inserted": inserted,
            },
        )

    log.info(
        "Trade import finalized (inserted=%s, summaries_updated=%s)",
        inserted,
        len(resolved),
    )
    return RedirectResponse(url="/", status_code=303)


@router.post("/import/trades")
async def import_trades(
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_session),
):
    max_bytes, allowed_formats = _import_config(request)
    try:
        content = await _read_upload(file, allowed_formats, max_bytes)
    except HTTPException as exc:
        await file.close()
        query = "file_too_large" if exc.status_code == 413 else "invalid_format"
        return RedirectResponse(
            f"/settings?trade_csv_error={query}#stock-data-import",
            status_code=303,
        )

    rows = parse_trade_csv(content)
    await file.close()

    if not rows:
        log.warning(
            "Trade CSV upload produced no rows (filename=%s)",
            file.filename,
        )
        return RedirectResponse(
            "/settings?trade_csv_error=no_trades#stock-data-import",
            status_code=303,
        )

    inserted = _persist_trade_rows(db, rows)
    log.info(
        "Imported generic trade CSV (filename=%s, rows=%s, inserted=%s)",
        file.filename,
        len(rows),
        inserted,
    )
    return _finalize_trade_import(request, db, inserted)


@router.post("/import/thinkorswim")
async def import_thinkorswim(
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_session),
):
    max_bytes, allowed_formats = _import_config(request)
    try:
        content = await _read_upload(file, allowed_formats, max_bytes)
    except HTTPException as exc:
        await file.close()
        query = "file_too_large" if exc.status_code == 413 else "invalid_format"
        return RedirectResponse(
            f"/settings?thinkorswim_error={query}#stock-data-import",
            status_code=303,
        )

    rows = parse_thinkorswim_csv(content)
    await file.close()

    if not rows:
        log.warning(
            "thinkorswim CSV upload produced no rows (filename=%s)",
            file.filename,
        )
        return RedirectResponse(
            "/settings?thinkorswim_error=no_trades#stock-data-import",
            status_code=303,
        )

    inserted = _persist_trade_rows(db, rows)
    log.info(
        "Imported thinkorswim trade CSV (filename=%s, rows=%s, inserted=%s)",
        file.filename,
        len(rows),
        inserted,
    )
    return _finalize_trade_import(request, db, inserted)


@router.post("/import/thinkorswim/conflicts", response_class=HTMLResponse)
async def resolve_conflicts(
    request: Request,
    db: Session = Depends(get_session),
):
    form = await request.form()
    dates = form.getlist("date")
    now = datetime.utcnow().isoformat()
    updated_days = 0

    for day in dates:
        choice = form.get(f"choice_{day}")
        if choice != "new":
            continue

        realized = float(form.get(f"new_realized_{day}", 0.0))
        invested = float(form.get(f"new_invested_{day}", 0.0))
        ds = db.get(DailySummary, day)
        if ds:
            ds.realized = realized
            ds.total_invested = invested
            ds.updated_at = now
        else:
            db.add(
                DailySummary(
                    date=day,
                    realized=realized,
                    total_invested=invested,
                    updated_at=now,
                )
            )
        updated_days += 1

    db.commit()
    log.info("Resolved trade import conflicts for %s day(s)", updated_days)
    return RedirectResponse(url="/", status_code=303)
