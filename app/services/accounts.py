from __future__ import annotations

import os
import re
import shutil
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.core.models import User
from app.core.seed import ensure_seed
from app.services.data_reset import clear_all_data

DEFAULT_ACCOUNT_ID = "primary"
DEFAULT_ACCOUNT_NAME = "Primary account"
DEFAULT_ACCOUNT_STORAGE = "."

_SLUG_RE = re.compile(r"[^a-z0-9]+")


class AccountOperationError(ValueError):
    """Represents an account operation failure with a machine-friendly code."""

    def __init__(self, code: str, message: str | None = None):
        super().__init__(message or code)
        self.code = code


@dataclass(frozen=True)
class AccountRecord:
    """Represents a configured trading account."""

    id: str
    name: str
    storage: str
    path: str


def _slugify(value: str) -> str:
    normalized = value.strip().lower()
    slug = _SLUG_RE.sub("-", normalized)
    slug = slug.strip("-")
    return slug or "account"


def _generate_unique_id(base: str, existing: Iterable[str]) -> str:
    slug = _slugify(base)
    candidate = slug
    index = 2
    existing_set = set(existing)
    while candidate in existing_set or candidate == DEFAULT_ACCOUNT_ID:
        candidate = f"{slug}-{index}"
        index += 1
    return candidate


def _capture_users_from_database(db_path: str) -> list[dict[str, Any]]:
    """Return serialized user records from ``db_path`` if available."""

    if not os.path.exists(db_path):
        return []

    engine = create_engine(
        f"sqlite:///{db_path}", connect_args={"check_same_thread": False}
    )
    try:
        with Session(engine) as session:
            users = session.query(User).order_by(User.id.asc()).all()
            return [
                {
                    "username": user.username,
                    "password_hash": user.password_hash,
                    "password_salt": user.password_salt,
                    "is_admin": bool(user.is_admin),
                    "created_at": user.created_at,
                }
                for user in users
            ]
    except SQLAlchemyError:
        return []
    finally:
        engine.dispose()


def _restore_users_to_database(db_path: str, payload: Sequence[dict[str, Any]]) -> None:
    """Populate ``db_path`` with provided user records if empty."""

    if not payload or not os.path.exists(db_path):
        return

    engine = create_engine(
        f"sqlite:///{db_path}", connect_args={"check_same_thread": False}
    )
    try:
        with Session(engine) as session:
            existing = {
                username
                for (username,) in session.query(User.username).all()
            }
            for item in payload:
                username = item.get("username")
                if not username or username in existing:
                    continue
                user = User(
                    username=username,
                    password_hash=item.get("password_hash", ""),
                    password_salt=item.get("password_salt", ""),
                    is_admin=bool(item.get("is_admin")),
                    created_at=item.get("created_at"),
                )
                session.add(user)
            session.commit()
    except SQLAlchemyError:
        return
    finally:
        engine.dispose()


def _ensure_account_sections(cfg) -> tuple[dict, dict]:
    accounts_section = cfg.raw.setdefault("accounts", {})
    if not isinstance(accounts_section, dict):
        accounts_section = {}
        cfg.raw["accounts"] = accounts_section
    entries = accounts_section.setdefault("entries", {})
    if not isinstance(entries, dict):
        entries = {}
        accounts_section["entries"] = entries
    if not entries:
        entries[DEFAULT_ACCOUNT_ID] = {
            "name": DEFAULT_ACCOUNT_NAME,
            "storage": DEFAULT_ACCOUNT_STORAGE,
        }
        accounts_section.setdefault("active", DEFAULT_ACCOUNT_ID)
    accounts_section.setdefault("active", DEFAULT_ACCOUNT_ID)
    return accounts_section, entries


def prepare_accounts(cfg, base_dir: str) -> tuple[list[AccountRecord], AccountRecord]:
    """Ensure account configuration is valid and directories exist."""

    accounts_section, entries = _ensure_account_sections(cfg)
    changed = False

    active_id = accounts_section.get("active", DEFAULT_ACCOUNT_ID)
    records: list[AccountRecord] = []
    active_record: AccountRecord | None = None

    for account_id, entry in list(entries.items()):
        if not isinstance(entry, dict):
            entry = {}
            entries[account_id] = entry
            changed = True

        name = entry.get("name")
        if not isinstance(name, str) or not name.strip():
            name = DEFAULT_ACCOUNT_NAME if account_id == DEFAULT_ACCOUNT_ID else account_id.title()
            entry["name"] = name
            changed = True
        else:
            normalized_name = name.strip()
            if normalized_name != name:
                entry["name"] = normalized_name
                name = normalized_name
                changed = True
            else:
                name = normalized_name

        storage = entry.get("storage")
        if not isinstance(storage, str) or not storage.strip():
            storage = DEFAULT_ACCOUNT_STORAGE if account_id == DEFAULT_ACCOUNT_ID else os.path.join("accounts", account_id)
            entry["storage"] = storage
            changed = True
        else:
            normalized_storage = storage.strip()
            if os.path.isabs(normalized_storage):
                normalized_storage = os.path.relpath(normalized_storage, base_dir)
                entry["storage"] = normalized_storage
                changed = True
            elif normalized_storage != storage:
                entry["storage"] = normalized_storage
                changed = True
            storage = normalized_storage

        account_path = os.path.abspath(os.path.join(base_dir, storage))
        os.makedirs(account_path, exist_ok=True)

        record = AccountRecord(
            id=account_id,
            name=name,
            storage=storage,
            path=account_path,
        )
        records.append(record)
        if account_id == active_id:
            active_record = record

    if not records:
        # Should not occur, but keep behaviour safe.
        default_storage = os.path.abspath(os.path.join(base_dir, DEFAULT_ACCOUNT_STORAGE))
        os.makedirs(default_storage, exist_ok=True)
        default_record = AccountRecord(
            id=DEFAULT_ACCOUNT_ID,
            name=DEFAULT_ACCOUNT_NAME,
            storage=DEFAULT_ACCOUNT_STORAGE,
            path=default_storage,
        )
        entries[DEFAULT_ACCOUNT_ID] = {
            "name": DEFAULT_ACCOUNT_NAME,
            "storage": DEFAULT_ACCOUNT_STORAGE,
        }
        records.append(default_record)
        accounts_section["active"] = DEFAULT_ACCOUNT_ID
        active_record = default_record
        changed = True

    if active_record is None:
        active_record = records[0]
        accounts_section["active"] = active_record.id
        changed = True

    if changed:
        cfg.save()

    return records, active_record


def create_account(cfg, base_dir: str, requested_name: str | None = None) -> AccountRecord:
    """Create a new trading account and make it active."""

    records, active_record = prepare_accounts(cfg, base_dir)
    source_db_path = None
    if active_record is not None:
        source_db_path = os.path.join(active_record.path, "profitloss.db")
    accounts_section, entries = _ensure_account_sections(cfg)
    existing_ids = entries.keys()

    sanitized_name = (requested_name or "").strip()
    if not sanitized_name:
        sanitized_name = f"Account {len(entries) + 1}"

    account_id = _generate_unique_id(sanitized_name, existing_ids)
    storage = os.path.join("accounts", account_id)

    entries[account_id] = {
        "name": sanitized_name,
        "storage": storage,
    }
    accounts_section["active"] = account_id
    cfg.save()

    account_path = os.path.abspath(os.path.join(base_dir, storage))
    os.makedirs(account_path, exist_ok=True)
    target_db_path = os.path.join(account_path, "profitloss.db")
    ensure_seed(target_db_path)
    if (
        source_db_path
        and os.path.abspath(source_db_path) != os.path.abspath(target_db_path)
    ):
        payload = _capture_users_from_database(source_db_path)
        _restore_users_to_database(target_db_path, payload)

    return AccountRecord(id=account_id, name=sanitized_name, storage=storage, path=account_path)


def rename_account(cfg, base_dir: str, account_id: str, new_name: str) -> None:
    """Rename the specified account."""

    prepare_accounts(cfg, base_dir)
    _, entries = _ensure_account_sections(cfg)
    entry = entries.get(account_id)
    if entry is None:
        raise ValueError("Account does not exist")

    sanitized = (new_name or "").strip()
    if not sanitized:
        raise ValueError("Account name cannot be empty")

    entry["name"] = sanitized
    cfg.save()


def set_active_account(cfg, base_dir: str, account_id: str) -> bool:
    """Activate the specified account."""

    prepare_accounts(cfg, base_dir)
    accounts_section, entries = _ensure_account_sections(cfg)
    if account_id not in entries:
        raise ValueError("Account does not exist")

    if accounts_section.get("active") == account_id:
        return False

    accounts_section["active"] = account_id
    cfg.save()
    return True


def serialize_accounts(accounts: Sequence[AccountRecord], active: AccountRecord) -> list[dict[str, str]]:
    return [
        {
            "id": account.id,
            "name": account.name,
            "storage": account.storage,
            "is_active": account.id == active.id,
        }
        for account in accounts
    ]


def clear_account(cfg, base_dir: str, account_id: str) -> AccountRecord:
    """Clear all stored data for the specified account."""

    prepare_accounts(cfg, base_dir)
    _, entries = _ensure_account_sections(cfg)
    entry = entries.get(account_id)
    if entry is None:
        raise AccountOperationError("missing", "Account does not exist")

    storage = entry.get("storage")
    if not isinstance(storage, str) or not storage.strip():
        storage = DEFAULT_ACCOUNT_STORAGE if account_id == DEFAULT_ACCOUNT_ID else os.path.join("accounts", account_id)
        entry["storage"] = storage
        cfg.save()

    account_path = os.path.abspath(os.path.join(base_dir, storage))
    os.makedirs(account_path, exist_ok=True)
    clear_all_data(account_path)

    name = entry.get("name")
    if not isinstance(name, str) or not name.strip():
        name = DEFAULT_ACCOUNT_NAME if account_id == DEFAULT_ACCOUNT_ID else account_id.title()

    return AccountRecord(id=account_id, name=name, storage=storage, path=account_path)


def delete_account(cfg, base_dir: str, account_id: str) -> AccountRecord:
    """Remove an account and its associated data directory."""

    prepare_accounts(cfg, base_dir)
    accounts_section, entries = _ensure_account_sections(cfg)

    if account_id == DEFAULT_ACCOUNT_ID:
        raise AccountOperationError("protected", "The primary account cannot be deleted")

    entry = entries.get(account_id)
    if entry is None:
        raise AccountOperationError("missing", "Account does not exist")

    if len(entries) <= 1:
        raise AccountOperationError("last_account", "At least one account must remain")

    storage = entry.get("storage")
    if not isinstance(storage, str) or not storage.strip():
        storage = os.path.join("accounts", account_id)

    name = entry.get("name")
    if not isinstance(name, str) or not name.strip():
        name = account_id.title()

    account_path = os.path.abspath(os.path.join(base_dir, storage))
    record = AccountRecord(id=account_id, name=name, storage=storage, path=account_path)

    del entries[account_id]

    if accounts_section.get("active") == account_id:
        fallback_id = DEFAULT_ACCOUNT_ID if DEFAULT_ACCOUNT_ID in entries else next(iter(entries.keys()))
        accounts_section["active"] = fallback_id

    cfg.save()

    if os.path.isdir(account_path):
        shutil.rmtree(account_path, ignore_errors=True)
    return record

