"""The one HTTP path every resource method goes through."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Callable, Dict, Mapping, Optional, Union

import httpx

from .errors import from_envelope, from_oauth

DEFAULT_BASE_URL = "https://api.vocalstack.com"

Query = Mapping[str, Any]


def _clean(q: Optional[Query]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in (q or {}).items():
        if v is None or v == "":
            continue
        out[k] = ("true" if v else "false") if isinstance(v, bool) else str(v)
    return out


def listener_query(listener_token: Optional[str], share_code: Optional[str]) -> Dict[str, Any]:
    """The query of a listener-side read (a private broadcast's listener
    token, or a share link's code), named as on the wire."""
    return {"listenerToken": listener_token, "shareCode": share_code}


#: The wait before retrying ``503 unavailable`` when it names none.
UNAVAILABLE_WAIT_S = 2.0


def retries(status: int, code: str, method: str, idempotency_key: Optional[str] = None) -> bool:
    """Whether a failed call is retried after its ``Retry-After`` (up to
    ``max_retries`` times): ``429 rate_limited`` always (nothing ran), and
    ``503 unavailable`` (the database is waking) only when repeating the call
    cannot do the work twice: GET, HEAD and DELETE, or a call carrying an
    ``Idempotency-Key``."""
    if status == 429:
        return True
    if status != 503 or code != "unavailable":
        return False
    return method.upper() in ("GET", "HEAD", "DELETE") or bool(idempotency_key)


class Transport:
    def __init__(
        self,
        base_url: Optional[str],
        api_key: Optional[str],
        token: Union[None, str, Callable[[], Optional[str]]],
        *,
        http: Optional[httpx.AsyncClient],
        acting_principal: Optional[str],
        device_id: Optional[str],
        max_retries: int,
        timeout: float,
    ) -> None:
        self.base_url = (base_url or DEFAULT_BASE_URL).rstrip("/")
        if api_key and token:
            raise TypeError("VocalStack: pass api_key (a server) or token (a browser), not both")
        self._api_key = api_key
        self._token = token
        self.own_http = http is None
        self.http = http or httpx.AsyncClient(timeout=timeout)
        self.acting_principal = acting_principal
        self.device_id = device_id
        self.max_retries = max_retries

    def bearer(self) -> Optional[str]:
        """What goes in ``Authorization: Bearer``: the API key, else the token."""
        if self._api_key:
            return self._api_key
        t = self._token
        return t() if callable(t) else t

    def auth_headers(self) -> Dict[str, str]:
        """The credential as a header (sockets too: a key is refused in a URL)."""
        key = self.bearer()
        return {"authorization": f"Bearer {key}"} if key else {}

    def url(self, path: str) -> str:
        return self.base_url + path

    def ws_url(self, path: str, query: Optional[Query] = None) -> str:
        base = "ws" + self.base_url[4:] if self.base_url.startswith("http") else self.base_url
        q = _clean(query)
        return base + path + (("?" + str(httpx.QueryParams(q))) if q else "")

    async def raw(
        self,
        method: str,
        path: str,
        *,
        query: Optional[Query] = None,
        json_body: Any = None,
        files: Any = None,
        data: Any = None,
        headers: Optional[Mapping[str, str]] = None,
        idempotency_key: Optional[str] = None,
        anonymous: bool = False,
        no_retry: bool = False,
        oauth_errors: bool = False,
    ) -> httpx.Response:
        h: Dict[str, str] = {"accept": "application/json"}
        if headers:
            h.update(headers)
        key = None if anonymous else self.bearer()
        if key:
            h["authorization"] = f"Bearer {key}"
        if self.acting_principal:
            h["x-acting-principal"] = self.acting_principal
        if self.device_id:
            h["x-device-id"] = self.device_id
        if idempotency_key:
            h["idempotency-key"] = idempotency_key
        attempt = 0
        while True:
            resp = await self.http.request(
                method,
                self.url(path),
                params=_clean(query),
                json=json_body,
                files=files,
                data=data,
                headers=h,
                follow_redirects=False,
            )
            if resp.status_code < 400:
                return resp
            body = None
            try:
                body = resp.json()
            except (json.JSONDecodeError, ValueError):
                pass
            err = (from_oauth if oauth_errors else from_envelope)(body, resp.status_code, resp.headers)
            if not no_retry and retries(resp.status_code, err.code, method, idempotency_key) and attempt < self.max_retries:
                wait = err.retry_after
                if wait is None and err.code == "unavailable":
                    wait = UNAVAILABLE_WAIT_S
                if wait is not None and wait <= 60:
                    attempt += 1
                    await asyncio.sleep(wait)
                    continue
            raise err

    async def json(self, method: str, path: str, **kw: Any) -> Any:
        resp = await self.raw(method, path, **kw)
        if 300 <= resp.status_code < 400:
            return {"location": resp.headers.get("location")}
        return resp.json() if resp.content else None

    async def aclose(self) -> None:
        if self.own_http:
            await self.http.aclose()
