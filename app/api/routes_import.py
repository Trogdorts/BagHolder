from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Set, Tuple

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.core.database import get_session
from app.core.logger import get_logger
from app.core.models import DailySummary, NoteDaily, Trade
from app.services.import_thinkorswim import parse_thinkorswim_csv
from app.services.import_trades_csv import parse_trade_csv
from app.services.trade_summaries import calculate_daily_trade_map, upsert_daily_summaries

router = APIRouter()
log = get_logger(__name__)


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
    unrealized = float(summary.unrealized or 0.0)
    invested = float(summary.total_invested or 0.0)
    return (
        _is_close(realized, 0.0)
        and _is_close(unrealized, 0.0)
        and _is_close(invested, 0.0)
    )


def _import_config(request: Request) -> Tuple[int, Set[str]]:
    cfg = getattr(request.app.state, "config", None)
    default_max_bytes = 5_000_000
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
        extensions = {str(ext).lower() for ext in accepted if isinstance(ext, str)}
        accepted_formats: Set[str] = extensions or default_formats
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
    notes_by_date: Dict[str, str] = {}
    notes_has_content: Dict[str, bool] = {}
    if notes_present:
        for row in rows:
            if "note" not in row:
                continue
            date_str = row["date"]
            note_text = row.get("note", "") or ""
            if date_str not in notes_by_date:
                notes_by_date[date_str] = note_text
                notes_has_content[date_str] = bool(note_text)
                continue
            if note_text and not notes_has_content.get(date_str, False):
                notes_by_date[date_str] = note_text
                notes_has_content[date_str] = True
            elif not note_text and not notes_has_content.get(date_str, False):
                notes_by_date[date_str] = ""

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

    if notes_by_date:
        timestamp = datetime.utcnow().isoformat()
        for date_str, note_text in notes_by_date.items():
            record = db.get(NoteDaily, date_str)
            if record:
                record.note = note_text
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
        unrealized = values["unrealized"]
        ds = db.get(DailySummary, day)
        if _is_missing_summary(ds):
            resolved[day] = values
            continue

        if _is_close(ds.realized, realized) and _is_close(ds.unrealized, unrealized):
            resolved[day] = values
        else:
            conflicts.append(
                {
                    "date": day,
                    "existing": {
                        "realized": float(ds.realized),
                        "unrealized": float(ds.unrealized),
                        "updated_at": ds.updated_at,
                    },
                    "new": {
                        "realized": realized,
                        "unrealized": unrealized,
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
        unrealized = float(form.get(f"new_unrealized_{day}", 0.0))
        ds = db.get(DailySummary, day)
        if ds:
            ds.realized = realized
            ds.unrealized = unrealized
            ds.total_invested = unrealized
            ds.updated_at = now
        else:
            db.add(
                DailySummary(
                    date=day,
                    realized=realized,
                    unrealized=unrealized,
                    total_invested=unrealized,
                    updated_at=now,
                )
            )
        updated_days += 1

    db.commit()
    log.info("Resolved trade import conflicts for %s day(s)", updated_days)
    return RedirectResponse(url="/", status_code=303)
