"""The VocalStack client: one class, grouped by resource.

    import asyncio, os
    from vocalstack import VocalStack

    async def main():
        async with VocalStack(api_key=os.environ["VOCALSTACK_API_KEY"]) as vs:
            tx = await vs.transcriptions.create(url="https://example.com/talk.mp3")
            job = await vs.jobs.wait(tx.job.recording_id)
            snap = await vs.recordings.transcript(job.recording_id)
            print(" ".join(s.text for s in snap.sentences))

    asyncio.run(main())

What is what: a realtime session is a signed token plus one socket,
nothing stored (``vs.realtime``); a broadcast is the link listeners open;
a transcription is a file, link, microphone or stream transcribed and
stored; a recording is one take inside either; a job is a file or link
recording's transcription work.
"""

from __future__ import annotations

from typing import Callable, Optional, Union

import httpx

from ._http import DEFAULT_BASE_URL, Transport
from .realtime import Realtime
from .resources import (
    Billing,
    Broadcasts,
    Corrections,
    Events,
    Jobs,
    Languages,
    Me,
    Meta,
    Recordings,
    Rtmp,
    Search,
    ShareLinks,
    Transcriptions,
    Voices,
)
from .realtime import DEFAULT_READY_DEADLINE_S


class VocalStack:
    """``api_key``: an API key (``vs_key_``), for a server. ``token``
    instead: a signed-in person's access token or a browser token, or a
    callable returning one (read per request); not with ``api_key``. ``base_url`` defaults to
    ``https://api.vocalstack.com``. A ``429`` is retried after its
    ``Retry-After`` (at most 60 s) up to ``max_retries`` times."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        *,
        api_key: Optional[str] = None,
        token: Union[None, str, Callable[[], Optional[str]]] = None,
        acting_principal: Optional[str] = None,
        device_id: Optional[str] = None,
        http: Optional[httpx.AsyncClient] = None,
        max_retries: int = 2,
        timeout: float = 300.0,
        ready_deadline_s: float = DEFAULT_READY_DEADLINE_S,
    ) -> None:
        t = Transport(
            base_url,
            api_key,
            token,
            http=http,
            acting_principal=acting_principal,
            device_id=device_id,
            max_retries=max_retries,
            timeout=timeout,
        )
        self._t = t
        self.base_url = t.base_url
        self.realtime = Realtime(t, ready_deadline_s)
        self.broadcasts = Broadcasts(t)
        self.transcriptions = Transcriptions(t)
        self.recordings = Recordings(t, ready_deadline_s)
        self.jobs = Jobs(t)
        self.corrections = Corrections(t)
        self.share_links = ShareLinks(t)
        self.voices = Voices(t)
        self.search = Search(t)
        self.me = Me(t)
        self.billing = Billing(t)
        self.languages = Languages(t)
        self.rtmp = Rtmp(t)
        self.events = Events(t)
        self.meta = Meta(t)

    async def aclose(self) -> None:
        await self._t.aclose()

    async def __aenter__(self) -> "VocalStack":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()


__all__ = ["DEFAULT_BASE_URL", "VocalStack"]
