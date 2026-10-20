"""Typed exceptions keyed by the contract's closed error-code enum."""

from __future__ import annotations

import email.utils
import time
from typing import Any, Mapping, Optional


class VocalStackError(Exception):
    """Any failure surfaced by this SDK: an HTTP error envelope, a socket
    ``error`` frame, or one the transport synthesizes (``capacity_exceeded``).

    ``details`` is the envelope's ``data.details`` (for example ``metric``,
    ``balance``, ``required``, ``plan``, ``topUpUrl`` on ``insufficient_credit``);
    ``headers`` are the response headers (``Retry-After``, ``RateLimit-*``,
    ``WWW-Authenticate``) when the error came from an HTTP response.
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        status: Optional[int] = None,
        details: Any = None,
        headers: Optional[Mapping[str, str]] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.status = status
        self.details = details
        self.headers = headers

    @property
    def retry_after(self) -> Optional[float]:
        """Seconds the server asked the caller to wait (``Retry-After``), if it said."""
        v = self.headers.get("retry-after") if self.headers is not None else None
        if not v:
            return None
        try:
            return float(v)
        except ValueError:
            pass
        try:
            at = email.utils.parsedate_to_datetime(v).timestamp()
        except (TypeError, ValueError):
            return None
        return max(0.0, at - time.time())

    def __repr__(self) -> str:
        return (
            f"VocalStackError(code={self.code!r}, message={self.message!r}, "
            f"retryable={self.retryable}, status={self.status}, details={self.details!r})"
        )


def capacity_exceeded(message: str) -> VocalStackError:
    """Synthesized by the transport: a saturated fleet queues the socket
    upgrade at the proxy (it hangs) or answers a bare 503, and the wire
    never carries the envelope."""
    return VocalStackError("capacity_exceeded", message, retryable=True)


def from_envelope(
    body: Any, status: Optional[int] = None, headers: Optional[Mapping[str, str]] = None
) -> VocalStackError:
    if isinstance(body, dict) and isinstance(body.get("code"), str):
        data = body.get("data") or {}
        return VocalStackError(
            body["code"],
            body.get("message", body["code"]),
            retryable=bool(data.get("retryable", False)),
            status=status,
            details=data.get("details"),
            headers=headers,
        )
    return VocalStackError(
        "internal", f"unexpected error (status {status})", status=status, headers=headers
    )


def from_oauth(
    body: Any, status: Optional[int] = None, headers: Optional[Mapping[str, str]] = None
) -> VocalStackError:
    """A token endpoint's ``400 { error, error_description }`` (RFC 6749
    §5.2) as a :class:`VocalStackError` whose ``code`` is the ``error``
    (``invalid_request``, ``invalid_grant``, ``unsupported_grant_type``);
    any other answer goes through the envelope."""
    if status == 400 and isinstance(body, dict) and isinstance(body.get("error"), str):
        return VocalStackError(
            body["error"], body.get("error_description") or body["error"], status=status, headers=headers
        )
    return from_envelope(body, status, headers)
