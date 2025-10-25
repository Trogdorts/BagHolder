"""Utilities for password hashing and session helpers."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import secrets
from typing import Tuple

PBKDF2_ITERATIONS = 480_000
_SALT_BYTES = 16


def _encode(data: bytes) -> str:
    return base64.b64encode(data).decode("utf-8")


def _decode(data: str) -> bytes:
    return base64.b64decode(data.encode("utf-8"))


def hash_password(password: str, salt: bytes | None = None) -> Tuple[str, str]:
    """Return a tuple of ``(salt, password_hash)`` encoded as base64 strings.

    ``password`` is normalized to UTF-8 and hashed using PBKDF2-HMAC-SHA256.
    When ``salt`` is not provided a new 16 byte salt is generated using
    :func:`secrets.token_bytes`.
    """

    if not isinstance(password, str) or not password:
        raise ValueError("Password must be a non-empty string")

    salt_bytes = salt if isinstance(salt, (bytes, bytearray)) else secrets.token_bytes(_SALT_BYTES)
    dk = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt_bytes,
        PBKDF2_ITERATIONS,
    )
    return _encode(salt_bytes), _encode(dk)


def verify_password(password: str, salt_b64: str, hash_b64: str) -> bool:
    """Return ``True`` when ``password`` matches ``hash_b64`` using ``salt_b64``."""

    if not isinstance(password, str) or not password:
        return False

    try:
        salt = _decode(salt_b64)
        expected = _decode(hash_b64)
    except (binascii.Error, ValueError):
        return False

    candidate = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PBKDF2_ITERATIONS,
    )
    return hmac.compare_digest(candidate, expected)
