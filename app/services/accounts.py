from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Iterable, Sequence

from app.core.seed import ensure_seed

DEFAULT_ACCOUNT_ID = "primary"
DEFAULT_ACCOUNT_NAME = "Primary account"
DEFAULT_ACCOUNT_STORAGE = "."

_SLUG_RE = re.compile(r"[^a-z0-9]+")


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

    prepare_accounts(cfg, base_dir)
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
    ensure_seed(os.path.join(account_path, "profitloss.db"))

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
