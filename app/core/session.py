"""Lightweight signed cookie session middleware."""

from __future__ import annotations

import base64
import json
import binascii
import hmac
import hashlib
from typing import Any, MutableMapping

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp


class SignedCookieSessionMiddleware(BaseHTTPMiddleware):
    """Persist ``request.session`` using an HMAC signed cookie."""

    def __init__(
        self,
        app: ASGIApp,
        secret_key: str,
        *,
        cookie_name: str = "bagholder_session",
        https_only: bool = False,
    ) -> None:
        if not secret_key:
            raise RuntimeError("A non-empty secret_key is required for session middleware")
        super().__init__(app)
        self._secret = secret_key.encode("utf-8")
        self._cookie_name = cookie_name
        self._https_only = https_only

    async def dispatch(self, request, call_next):
        session_data = self._load_cookie(request.cookies.get(self._cookie_name))
        request.scope["session"] = session_data

        response = await call_next(request)

        session_value = request.scope.get("session", {})
        if not isinstance(session_value, MutableMapping):
            try:
                session_value = dict(session_value)
            except TypeError:
                session_value = {}

        if session_value:
            payload = self._dump(session_value)
            response.set_cookie(
                self._cookie_name,
                payload,
                httponly=True,
                secure=self._https_only,
                samesite="lax",
                path="/",
            )
        else:
            if request.cookies.get(self._cookie_name):
                response.delete_cookie(self._cookie_name, path="/")

        return response

    def _load_cookie(self, value: str | None) -> dict[str, Any]:
        if not value:
            return {}
        try:
            signature_b64, payload_b64 = value.split(".", 1)
            signature = base64.urlsafe_b64decode(signature_b64.encode("utf-8"))
            payload = base64.urlsafe_b64decode(payload_b64.encode("utf-8"))
        except (ValueError, binascii.Error):
            return {}

        expected = hmac.new(self._secret, payload, hashlib.sha256).digest()
        if not hmac.compare_digest(signature, expected):
            return {}

        try:
            data = json.loads(payload.decode("utf-8"))
        except json.JSONDecodeError:
            return {}

        if not isinstance(data, dict):
            return {}
        return data

    def _dump(self, data: MutableMapping[str, Any]) -> str:
        payload = json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")
        signature = hmac.new(self._secret, payload, hashlib.sha256).digest()
        return ".".join(
            [
                base64.urlsafe_b64encode(signature).decode("utf-8"),
                base64.urlsafe_b64encode(payload).decode("utf-8"),
            ]
        )
