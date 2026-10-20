"""The resource groups of :class:`vocalstack.VocalStack`, one per tag of
the API document (``/v1/openapi.json``)."""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import IO, TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, TypeVar, Union

import websockets
from pydantic import BaseModel

from . import _models as m
from ._http import Transport, listener_query
from .errors import VocalStackError

if TYPE_CHECKING:
    from .realtime import RealtimeSession

T = TypeVar("T", bound=BaseModel)

#: A file to upload: a path, bytes, or an open binary file.
Upload = Union[str, "os.PathLike[str]", bytes, bytearray, IO[bytes]]


def _body(model: Type[BaseModel], fields: Dict[str, Any]) -> Dict[str, Any]:
    """Validate keyword arguments (snake_case or the wire's camelCase)
    against a request model and dump them as the wire expects."""
    return model.model_validate(fields).model_dump(by_alias=True, exclude_none=True, mode="json")


def _one(model: Type[T], data: Any) -> T:
    return model.model_validate(data)


def _many(model: Type[T], data: Any, key: str) -> List[T]:
    return [model.model_validate(x) for x in ((data or {}).get(key) or [])]


async def _all_pages(page: Callable[[Optional[str]], Any], key: str) -> List[Any]:
    """Every item of a paged list: each page asked for with the previous
    page's ``nextCursor`` until one comes back without it."""
    out: List[Any] = []
    cursor: Optional[str] = None
    while True:
        data = (await page(cursor)) or {}
        out.extend(data.get(key) or [])
        cursor = data.get("nextCursor")
        if not cursor:
            return out


def _file_part(file: Upload, file_name: Optional[str], default: str) -> tuple:
    if isinstance(file, (bytes, bytearray)):
        return (file_name or default, bytes(file))
    if isinstance(file, (str, os.PathLike)):
        path = os.fspath(file)
        with open(path, "rb") as fh:
            return (file_name or os.path.basename(path), fh.read())
    name = file_name or os.path.basename(getattr(file, "name", "") or "") or default
    return (name, file.read())


def _p(segment: Any) -> str:
    from urllib.parse import quote

    return quote(str(segment), safe="")


class _Group:
    def __init__(self, t: Transport) -> None:
        self._t = t


class _ParentResource(_Group):
    """What broadcasts and transcriptions share: recordings, share links."""

    _base = ""
    #: The list's key: what it holds (``broadcasts`` or ``transcriptions``).
    _key = ""
    _model: Type[Union[m.Broadcast, m.Transcription]] = m.Broadcast
    _update: Type[Union[m.UpdateBroadcast, m.UpdateTranscription]] = m.UpdateBroadcast

    def _path(self, id: str, rest: str = "") -> str:
        return f"{self._base}/{_p(id)}{rest}"

    async def list(self, lifecycle: str = "active") -> List[Any]:
        """Every one, newest first: ``lifecycle`` is ``active``, ``archived``,
        or ``all`` (both in one list). A paged list is read to its last page
        (``transcriptions.list_page`` reads one)."""
        items = await _all_pages(lambda c: self._t.json("GET", self._base, query={"lifecycle": lifecycle, "cursor": c}), self._key)
        return [self._model.model_validate(x) for x in items]

    async def get(self, id: str) -> Any:
        return _one(self._model, await self._t.json("GET", self._path(id)))

    async def update(self, id: str, **fields: Any) -> Any:
        """Rename, or change the default settings (a broadcast: also its
        access, password or link, ``code``)."""
        body = _body(self._update, fields)
        return _one(self._model, await self._t.json("PATCH", self._path(id), json_body=body))

    async def archive(self, id: str) -> Any:
        return _one(self._model, await self._t.json("POST", self._path(id, "/archive")))

    async def restore(self, id: str) -> Any:
        return _one(self._model, await self._t.json("POST", self._path(id, "/restore")))

    async def purge(self, id: str) -> None:
        """Delete an archived one and everything in it, for good."""
        await self._t.json("POST", self._path(id, "/purge"))

    async def list_recordings(self, id: str, lifecycle: str = "active") -> List[m.Recording]:
        """Every recording, newest first (every page of ``list_recordings_page``)."""
        items = await _all_pages(
            lambda c: self._t.json("GET", self._path(id, "/recordings"), query={"lifecycle": lifecycle, "cursor": c}), "recordings"
        )
        return [m.Recording.model_validate(x) for x in items]

    async def list_recordings_page(
        self, id: str, *, lifecycle: str = "active", cursor: Optional[str] = None, limit: Optional[int] = None
    ) -> m.RecordingPage:
        """One page of recordings, newest first; ``cursor`` is the previous page's ``next_cursor``."""
        data = await self._t.json("GET", self._path(id, "/recordings"), query={"lifecycle": lifecycle, "cursor": cursor, "limit": limit})
        return _one(m.RecordingPage, data)

    async def start_recording(
        self, id: str, *, idempotency_key: Optional[str] = None, **fields: Any
    ) -> m.PublishGrant:
        """Start a live recording. The grant's ``stream_url`` is the publisher
        socket (``vs.recordings.publish(grant)``). A 409 ``conflict`` with
        ``details["takeover"]`` means another device is recording: pass
        ``takeover=True`` to record here instead."""
        body = _body(m.StartRecording, fields)
        data = await self._t.json(
            "POST", self._path(id, "/recordings"), json_body=body, idempotency_key=idempotency_key
        )
        return _one(m.PublishGrant, data)

    async def list_share_links(self, id: str) -> List[m.ShareLink]:
        return _many(m.ShareLink, await self._t.json("GET", self._path(id, "/share-links")), "links")

    async def create_share_link(self, id: str, **fields: Any) -> m.ShareLink:
        body = _body(m.CreateShareLink, fields)
        return _one(m.ShareLink, await self._t.json("POST", self._path(id, "/share-links"), json_body=body))

    async def events(
        self, id: str, *, listener_token: Optional[str] = None, share_code: Optional[str] = None
    ) -> Any:
        """The events socket a waiting screen needs (``recording.started``:
        it is live, ``recording.ended``, ``job.progress``,
        ``stream.ingest``; each names the transcription the recording is
        made in, ``transcriptionId``): a ``websockets`` connection yielding
        JSON text."""
        url = self._t.ws_url(self._path(id, "/events"), listener_query(listener_token, share_code))
        return await websockets.connect(url, additional_headers=self._t.auth_headers())


class Broadcasts(_ParentResource):
    """``vs.broadcasts``: the links listeners open."""

    _base = "/v1/broadcasts"
    _key = "broadcasts"
    _model = m.Broadcast
    _update = m.UpdateBroadcast

    async def create(self, *, idempotency_key: Optional[str] = None, **fields: Any) -> m.Broadcast:
        body = _body(m.CreateBroadcast, fields)
        data = await self._t.json("POST", self._base, json_body=body, idempotency_key=idempotency_key)
        return _one(m.Broadcast, data)

    async def add_audio_source(self, id: str, **fields: Any) -> m.Broadcast:
        """A stream the broadcast can record from (a YouTube live, a radio address)."""
        body = _body(m.CreateAudioSource, fields)
        return _one(m.Broadcast, await self._t.json("POST", self._path(id, "/audio-sources"), json_body=body))

    async def remove_audio_source(self, id: str, source_id: str) -> m.Broadcast:
        data = await self._t.json("DELETE", self._path(id, f"/audio-sources/{_p(source_id)}"))
        return _one(m.Broadcast, data)

    async def check_audio_source(self, id: str, source_id: str, *, fresh: bool = False) -> m.AudioSourceCheck:
        data = await self._t.json(
            "GET",
            self._path(id, f"/audio-sources/{_p(source_id)}/check"),
            query={"fresh": True if fresh else None},
        )
        return _one(m.AudioSourceCheck, data)


class Transcriptions(_ParentResource):
    """``vs.transcriptions``: a file, a link, a microphone or a stream, transcribed and stored."""

    _base = "/v1/transcriptions"
    _key = "transcriptions"
    _model = m.Transcription
    _update = m.UpdateTranscription

    async def list_page(
        self, *, lifecycle: str = "active", cursor: Optional[str] = None, limit: Optional[int] = None
    ) -> m.TranscriptionPage:
        """One page of transcriptions, newest first (``limit`` 1 to 200, 100
        by default); ``cursor`` is the previous page's ``next_cursor``, and
        the last page has none."""
        data = await self._t.json("GET", self._base, query={"lifecycle": lifecycle, "cursor": cursor, "limit": limit})
        return _one(m.TranscriptionPage, data)

    async def upload_recording(
        self, id: str, *, idempotency_key: Optional[str] = None, **fields: Any
    ) -> m.PublishGrant:
        """A file or link recording in this transcription: no live phase,
        transcribed at full speed.  It has no job: open the grant's socket
        with ``vs.recordings.publish(grant)`` (with ``url`` the server
        fetches the link) and read it until the session is done."""
        body = _body(m.UploadRecording, fields)
        data = await self._t.json(
            "POST", self._path(id, "/recordings/upload"), json_body=body, idempotency_key=idempotency_key
        )
        return _one(m.PublishGrant, data)

    async def create(
        self,
        *,
        file: Optional[Upload] = None,
        file_name: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        **fields: Any,
    ) -> m.Transcription:
        """From a file (``file``: a path, bytes or an open binary file) or a
        public link (``url=...``). The answer's ``transcription.recording_id``
        is the job: ``await vs.jobs.wait(recording_id)``, then
        ``await vs.recordings.transcript(recording_id)``."""
        options = _body(m.CreateTranscription, fields)
        if file is None:
            data = await self._t.json("POST", self._base, json_body=options, idempotency_key=idempotency_key)
        else:
            part = await asyncio.to_thread(_file_part, file, file_name, "upload")
            data = await self._t.json(
                "POST",
                self._base,
                data={"options": json.dumps(options)},
                files={"file": part},
                idempotency_key=idempotency_key,
            )
        return _one(m.Transcription, data)

    async def create_from_url(self, url: str, **fields: Any) -> m.Transcription:
        """``create(url=url, ...)``."""
        return await self.create(url=url, **fields)

    async def create_live(self, *, idempotency_key: Optional[str] = None, **fields: Any) -> m.Transcription:
        """Captured as it is spoken (a microphone, or a stream the server
        pulls). Created idle: ``start_recording(id)`` starts it."""
        body = _body(m.CreateLiveTranscription, fields)
        data = await self._t.json("POST", f"{self._base}/live", json_body=body, idempotency_key=idempotency_key)
        return _one(m.Transcription, data)

    async def probe(self, url: str) -> m.LinkProbe:
        """Whether a link is live (a live transcription) or a file."""
        return _one(m.LinkProbe, await self._t.json("GET", f"{self._base}/probe", query={"url": url}))

    async def attach_broadcast(self, id: str, broadcast_id: str) -> m.Transcription:
        data = await self._t.json("PUT", self._path(id, f"/broadcasts/{_p(broadcast_id)}"))
        return _one(m.Transcription, data)

    async def detach_broadcast(self, id: str, broadcast_id: str) -> m.Transcription:
        data = await self._t.json("DELETE", self._path(id, f"/broadcasts/{_p(broadcast_id)}"))
        return _one(m.Transcription, data)


class Recordings(_Group):
    """``vs.recordings``: one take, live or uploaded."""

    def __init__(self, t: Transport, ready_deadline_s: float) -> None:
        super().__init__(t)
        self._ready_deadline_s = ready_deadline_s

    @staticmethod
    def _path(id: str, rest: str = "") -> str:
        return f"/v1/recordings/{_p(id)}{rest}"

    async def get(
        self, id: str, *, listener_token: Optional[str] = None, share_code: Optional[str] = None
    ) -> m.Recording:
        return _one(m.Recording, await self._t.json("GET", self._path(id), query=listener_query(listener_token, share_code)))

    async def pause(self, id: str) -> m.Recording:
        return _one(m.Recording, await self._t.json("POST", self._path(id, "/pause")))

    async def resume(self, id: str) -> m.Recording:
        return _one(m.Recording, await self._t.json("POST", self._path(id, "/resume")))

    async def stop(self, id: str, reason: Optional[str] = None) -> m.Recording:
        """Only from paused."""
        data = await self._t.json("POST", self._path(id, "/stop"), json_body={"reason": reason})
        return _one(m.Recording, data)

    async def settings(self, id: str, **fields: Any) -> m.Recording:
        body = _body(m.SettingsPatch, fields)
        return _one(m.Recording, await self._t.json("PATCH", self._path(id, "/settings"), json_body=body))

    async def publish_token(self, id: str, *, takeover: bool = False) -> m.PublishGrant:
        data = await self._t.json("POST", self._path(id, "/publish-token"), json_body={"takeover": takeover})
        return _one(m.PublishGrant, data)

    async def publish(
        self,
        grant: m.PublishGrant,
        *,
        ready_deadline_s: Optional[float] = None,
        on_frame: Optional[Callable[[Any], None]] = None,
    ) -> "RealtimeSession":
        """Open the publisher socket a grant names and wait until it is
        ready: ``send_audio`` raw PCM (f32le, 16 kHz, mono), end with ``eof()``.
        ``on_frame`` sees every server frame, lifecycle included."""
        from .realtime import RealtimeSession

        if not grant.stream_url:
            raise VocalStackError("invalid_state", "this grant has no stream_url (an upload has no publisher socket)")
        return await RealtimeSession.connect(
            grant.session_id or grant.recording.id,
            grant.stream_url,
            ready_deadline_s=ready_deadline_s or self._ready_deadline_s,
            on_frame=on_frame,
        )

    async def archive(self, id: str) -> m.Recording:
        return _one(m.Recording, await self._t.json("POST", self._path(id, "/archive")))

    async def restore(self, id: str) -> m.Recording:
        return _one(m.Recording, await self._t.json("POST", self._path(id, "/restore")))

    async def purge(self, id: str) -> None:
        await self._t.json("POST", self._path(id, "/purge"))

    async def transcript(
        self,
        id: str,
        *,
        after: Optional[int] = None,
        lang: Optional[str] = None,
        listener_token: Optional[str] = None,
        share_code: Optional[str] = None,
    ) -> m.TranscriptSnapshot:
        """Segments (with word timings), sentences and speakers; ``lang`` reads it translated."""
        query = {**listener_query(listener_token, share_code), "after": after, "lang": lang}
        data = await self._t.json("GET", self._path(id, "/transcript"), query=query)
        return _one(m.TranscriptSnapshot, data)

    async def translation(
        self,
        id: str,
        lang: str,
        sentences: Optional[List[int]] = None,
        *,
        listener_token: Optional[str] = None,
        share_code: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Translate the given sentence ids (at most 40): ``{"lang", "translations"}``."""
        query = {**listener_query(listener_token, share_code), "lang": lang, "sentences": ",".join(map(str, sentences)) if sentences else None}
        return await self._t.json("GET", self._path(id, "/translation"), query=query)

    async def summarize(self, id: str, *, idempotency_key: Optional[str] = None) -> m.SummaryStatus:
        data = await self._t.json("POST", self._path(id, "/summarize"), idempotency_key=idempotency_key)
        return _one(m.SummaryStatus, data)

    async def summary(
        self, id: str, *, lang: Optional[str] = None, listener_token: Optional[str] = None, share_code: Optional[str] = None
    ) -> m.SummaryStatus:
        query = {**listener_query(listener_token, share_code), "lang": lang}
        data = await self._t.json("GET", self._path(id, "/summary"), query=query)
        return _one(m.SummaryStatus, data)

    async def export(
        self, id: str, format: str, *, lang: Optional[str] = None, share_code: Optional[str] = None, listener_token: Optional[str] = None
    ) -> bytes:
        """An export file: ``json``, ``srt``, ``xlsx`` or ``pdf``; ``lang`` makes it read in that language. ``listener_token``: a broadcast listener's, once the recording has ended."""
        resp = await self._t.raw(
            "GET", self._path(id, "/export"), headers={"accept": "*/*"}, query={"format": format, "lang": lang, "shareCode": share_code, "listenerToken": listener_token}
        )
        return resp.content

    async def audio_url(
        self,
        id: str,
        *,
        kind: Optional[str] = None,
        lang: Optional[str] = None,
        format: Optional[str] = None,
        listener_token: Optional[str] = None,
        share_code: Optional[str] = None,
    ) -> str:
        """The signed media URL the server redirects to (``kind``: delivery, master, source)."""
        query = {**listener_query(listener_token, share_code), "kind": kind, "lang": lang, "format": format}
        resp = await self._t.raw("GET", self._path(id, "/audio"), query=query)
        loc = resp.headers.get("location")
        if not loc:
            raise VocalStackError("not_found", "no signed URL in the redirect", status=resp.status_code)
        return loc

    async def clip(
        self, id: str, start_s: float, end_s: float, *, listener_token: Optional[str] = None, share_code: Optional[str] = None
    ) -> bytes:
        """A stretch of the recording's audio, ``start_s`` to ``end_s`` seconds (at most 120 s)."""
        query = {**listener_query(listener_token, share_code), "startS": start_s, "endS": end_s}
        resp = await self._t.raw("GET", self._path(id, "/audio/clip"), headers={"accept": "*/*"}, query=query)
        return resp.content

    async def voice(self, id: str, lang: Optional[str] = None) -> List[m.VoiceTrack]:
        data = await self._t.json("GET", self._path(id, "/voice"), query={"lang": lang})
        return [m.VoiceTrack.model_validate(x) for x in data or []]

    async def render_voice(self, id: str, langs: List[str], *, idempotency_key: Optional[str] = None) -> List[m.VoiceTrack]:
        data = await self._t.json("POST", self._path(id, "/voice"), json_body={"langs": langs}, idempotency_key=idempotency_key)
        return [m.VoiceTrack.model_validate(x) for x in data or []]

    async def voice_clip(self, id: str, sentence: int, lang: str, *, format: Optional[str] = None) -> bytes:
        resp = await self._t.raw(
            "GET", self._path(id, f"/voice/{sentence}"), headers={"accept": "*/*"}, query={"lang": lang, "format": format}
        )
        return resp.content

    async def media_token(self, id: str) -> Dict[str, Any]:
        return await self._t.json("GET", self._path(id, "/media-token"))

    async def name_speaker(self, id: str, speaker: int, name: Optional[str]) -> None:
        await self._t.json("PUT", self._path(id, f"/speakers/{speaker}"), json_body={"name": name})

    async def events(self, id: str, *, after: Optional[int] = None, limit: Optional[int] = None) -> List[m.RecordingEvent]:
        """The durable event log with wall times (owner only): every event
        after ``after`` (a ``seq``; the whole log without it), read ``limit`` a page."""

        def page(cursor: Optional[str]) -> Any:
            q = {"cursor": cursor, "limit": limit} if cursor else {"after": after, "limit": limit}
            return self._t.json("GET", self._path(id, "/events"), query=q)

        return [m.RecordingEvent.model_validate(x) for x in await _all_pages(page, "events")]

    async def events_page(
        self, id: str, *, cursor: Optional[str] = None, after: Optional[int] = None, limit: Optional[int] = None
    ) -> m.RecordingEvents:
        """One page of the event log (500 by default, 1000 at most): from
        ``after`` a ``seq``, or the previous page's ``next_cursor`` as ``cursor``."""
        data = await self._t.json("GET", self._path(id, "/events"), query={"cursor": cursor, "after": after, "limit": limit})
        return _one(m.RecordingEvents, data)

    async def subscribe(
        self,
        id: str,
        *,
        lang: Optional[str] = None,
        voice: Optional[str] = None,
        listener_token: Optional[str] = None,
        share_code: Optional[str] = None,
        as_owner: bool = True,
    ) -> Any:
        """The recording's live fan-out socket: a ``websockets`` connection
        whose first frame is ``subscribe.ready``; read the backfill with
        ``transcript(id, after=...)``."""
        url = self._t.ws_url(
            self._path(id, "/subscribe"),
            {
                "lang": lang,
                "voice": voice,
                **listener_query(listener_token, share_code),
                "role": None if as_owner else "listener",
                "device": self._t.device_id,
            },
        )
        headers = self._t.auth_headers() if as_owner else {}
        return await websockets.connect(url, max_size=None, additional_headers=headers)


_TERMINAL = {"done", "failed", "stopped", "cropping"}


class Jobs(_Group):
    """``vs.jobs``: a file or link recording's transcription job. States:
    queued, fetching, transcribing, done, failed, and cropping (waiting for ``start``)."""

    async def get(self, recording_id: str) -> m.TranscriptionJob:
        return _one(m.TranscriptionJob, await self._t.json("GET", f"/v1/recordings/{_p(recording_id)}/job"))

    async def start(self, recording_id: str, *, start_s: Optional[float] = None, end_s: Optional[float] = None) -> m.TranscriptionJob:
        """Release a job waiting in ``cropping``: the window to transcribe, in seconds."""
        body = _body(m.StartJob, {"start_s": start_s, "end_s": end_s})
        return _one(m.TranscriptionJob, await self._t.json("POST", f"/v1/recordings/{_p(recording_id)}/job/start", json_body=body))

    async def retry(self, recording_id: str) -> m.TranscriptionJob:
        """Re-queue a failed job from its stored source."""
        return _one(m.TranscriptionJob, await self._t.json("POST", f"/v1/recordings/{_p(recording_id)}/job/retry"))

    async def preview(self, recording_id: str) -> bytes:
        """The file the server fetched for a link job waiting in ``cropping``."""
        resp = await self._t.raw("GET", f"/v1/recordings/{_p(recording_id)}/job/preview", headers={"accept": "*/*"})
        return resp.content

    async def wait(
        self,
        recording_id: str,
        *,
        interval_s: float = 2.0,
        timeout_s: Optional[float] = None,
        on_progress: Optional[Callable[[m.TranscriptionJob], None]] = None,
    ) -> m.TranscriptionJob:
        """Poll until the job is ``done``, ``failed`` or ``cropping`` and
        return it. It does not raise on ``failed``: check ``job.state`` and ``job.error``.
        ``on_progress`` sees every job read, the last one included: its
        ``state`` and ``progress`` (``pct``, ``processed_s``, the fetch phase)."""
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        while True:
            job = await self.get(recording_id)
            if on_progress is not None:
                on_progress(job)
            if job.state in _TERMINAL:
                return job
            if deadline is not None and time.monotonic() + interval_s > deadline:
                raise VocalStackError("timeout", f"job still {job.state} after {timeout_s}s", retryable=True)
            await asyncio.sleep(interval_s)


class Corrections(_Group):
    """``vs.corrections``: edits to a finished transcript, each revertible."""

    async def list(self, recording_id: str) -> List[m.Correction]:
        """Every correction, newest first (every page of ``list_page``)."""
        path = f"/v1/recordings/{_p(recording_id)}/corrections"
        items = await _all_pages(lambda c: self._t.json("GET", path, query={"cursor": c}), "corrections")
        return [m.Correction.model_validate(x) for x in items]

    async def list_page(self, recording_id: str, *, cursor: Optional[str] = None, limit: Optional[int] = None) -> m.CorrectionPage:
        """One page of corrections, newest first; ``cursor`` is the previous page's ``next_cursor``."""
        data = await self._t.json("GET", f"/v1/recordings/{_p(recording_id)}/corrections", query={"cursor": cursor, "limit": limit})
        return _one(m.CorrectionPage, data)

    async def apply(self, recording_id: str, **fields: Any) -> List[m.Correction]:
        """Apply a staged batch atomically (``CorrectionBatch``)."""
        body = _body(m.CorrectionBatch, fields)
        data = await self._t.json("POST", f"/v1/recordings/{_p(recording_id)}/corrections", json_body=body)
        return _many(m.Correction, data, "corrections")

    async def retranscribe(self, recording_id: str, *, seqs: List[int], lang: str) -> List[m.Correction]:
        """Read lines again with the language forced."""
        data = await self._t.json(
            "POST", f"/v1/recordings/{_p(recording_id)}/retranscribe", json_body={"seqs": seqs, "lang": lang}
        )
        return _many(m.Correction, data, "corrections")

    async def revert(self, correction_id: str) -> m.Correction:
        return _one(m.Correction, await self._t.json("POST", f"/v1/corrections/{_p(correction_id)}/revert"))


class ShareLinks(_Group):
    """``vs.share_links``: named public links (create and list them on their owner)."""

    async def update(self, id: str, **fields: Any) -> m.ShareLink:
        body = _body(m.UpdateShareLink, fields)
        return _one(m.ShareLink, await self._t.json("PATCH", f"/v1/share-links/{_p(id)}", json_body=body))

    async def revoke(self, id: str) -> m.ShareLink:
        return _one(m.ShareLink, await self._t.json("DELETE", f"/v1/share-links/{_p(id)}"))


class Voices(_Group):
    """``vs.voices``: the account's cloned voices, and what a language offers."""

    async def list(self) -> List[m.Voice]:
        return _many(m.Voice, await self._t.json("GET", "/v1/voices"), "voices")

    async def get(self, id: str) -> m.Voice:
        return _one(m.Voice, await self._t.json("GET", f"/v1/voices/{_p(id)}"))

    async def create(
        self,
        name: str,
        *,
        sample: Optional[Upload] = None,
        sample_url: Optional[str] = None,
        file_name: Optional[str] = None,
        gender: Optional[str] = None,
        color: Optional[str] = None,
        draft: bool = False,
        idempotency_key: Optional[str] = None,
    ) -> m.Voice:
        """Build a voice from a sample or a link. Calling it asserts you have
        the speaker's permission, unless ``draft``."""
        meta = _body(
            m.CreateVoice,
            {"name": name, "sample_url": sample_url, "gender": gender, "color": color, "draft": draft or None, "consent": not draft},
        )
        # Multipart even without a sample: the meta part rides as a field.
        files: Dict[str, Any] = {"meta": (None, json.dumps(meta), "application/json")}
        if sample is not None:
            files["sample"] = await asyncio.to_thread(_file_part, sample, file_name, "sample.webm")
        data = await self._t.json("POST", "/v1/voices", files=files, idempotency_key=idempotency_key)
        return _one(m.Voice, data)

    async def create_from_speaker(
        self, *, recording_id: str, speaker: int, name: str, color: Optional[str] = None, idempotency_key: Optional[str] = None
    ) -> m.Voice:
        """Build a voice from one speaker of a recording (asserts permission)."""
        body = _body(
            m.CreateVoiceFromSpeaker,
            {"recording_id": recording_id, "speaker": speaker, "name": name, "color": color, "consent": True},
        )
        data = await self._t.json("POST", "/v1/voices/from-speaker", json_body=body, idempotency_key=idempotency_key)
        return _one(m.Voice, data)

    async def update(self, id: str, **fields: Any) -> m.Voice:
        body = _body(m.PatchVoice, fields)
        return _one(m.Voice, await self._t.json("PATCH", f"/v1/voices/{_p(id)}", json_body=body))

    async def delete(self, id: str) -> None:
        await self._t.json("DELETE", f"/v1/voices/{_p(id)}")

    async def available(
        self,
        lang: str,
        *,
        recording: Optional[str] = None,
        listener_token: Optional[str] = None,
        share_code: Optional[str] = None,
        as_owner: bool = True,
    ) -> m.VoiceCatalogue:
        """The voices that can speak ``lang``, best first (with a token, also
        the account's ready clones). ``recording`` also offers the cloned
        voices that recording uses, to whoever may read it: its owner, or a
        listener with ``listener_token`` or ``share_code``."""
        data = await self._t.json(
            "GET",
            "/v1/voices/available",
            query={**listener_query(listener_token, share_code), "lang": lang, "recording": recording},
            anonymous=not as_owner,
        )
        return _one(m.VoiceCatalogue, data)

    async def preview(self, voice: str, lang: str, *, format: Optional[str] = None) -> bytes:
        resp = await self._t.raw(
            "GET", "/v1/voices/preview", headers={"accept": "*/*"}, query={"voice": voice, "lang": lang, "format": format}
        )
        return resp.content

    async def clone_preview(
        self,
        id: str,
        *,
        lang: Optional[str] = None,
        format: Optional[str] = None,
        recording: Optional[str] = None,
        listener_token: Optional[str] = None,
        share_code: Optional[str] = None,
        as_owner: bool = True,
    ) -> bytes:
        """A cloned voice speaking ``lang``. With ``recording``, as it speaks
        in that recording, for whoever may read it (its owner, or a listener
        with ``listener_token`` or ``share_code``)."""
        resp = await self._t.raw(
            "GET",
            f"/v1/voices/{_p(id)}/preview",
            headers={"accept": "*/*"},
            query={**listener_query(listener_token, share_code), "lang": lang, "format": format, "recording": recording},
            anonymous=not as_owner,
        )
        return resp.content

    async def sample(self, id: str) -> bytes:
        resp = await self._t.raw("GET", f"/v1/voices/{_p(id)}/sample", headers={"accept": "*/*"})
        return resp.content


class Search(_Group):
    """``vs.search``: full-text search over the caller's transcripts."""

    async def query(
        self,
        q: str,
        *,
        broadcasts: Optional[List[str]] = None,
        listener_tokens: Optional[List[str]] = None,
        recording: Optional[str] = None,
        lang: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> m.SearchResults:
        """``q`` is 2 or more characters; ``broadcasts`` narrows to those links (codes)."""
        query = {
            "q": q,
            "broadcasts": ",".join(broadcasts) if broadcasts else None,
            "listenerTokens": ",".join(listener_tokens) if listener_tokens else None,
            "recording": recording,
            "lang": lang,
            "limit": limit,
        }
        return _one(m.SearchResults, await self._t.json("GET", "/v1/search", query=query))


class Me(_Group):
    """``vs.me``: who the credential is."""

    async def get(self) -> Dict[str, Any]:
        return await self._t.json("GET", "/v1/me")


class Connect(_Group):
    """``vs.connect``: one-click Connect, on the app's server."""

    async def token(self, *, code: str, code_verifier: str, redirect_uri: str) -> Dict[str, Any]:
        """Redeem the ``code`` the dashboard sent to the app's ``redirect_uri``
        for an API key (OAuth 2.0 authorization code + PKCE): ``code_verifier``
        is the verifier whose ``BASE64URL(SHA256(verifier))`` was the link's
        ``code_challenge``, ``redirect_uri`` exactly the link's. Returns the
        wire's ``{ access_token, token_type, key_id, scope, account }``;
        ``access_token`` is the key, shown once. No credential is sent, even
        when the client has one, and the call is never retried: the code
        works once. A refusal raises :class:`VocalStackError` whose ``code``
        is OAuth's ``error`` (``invalid_grant``: an unknown, spent or expired
        code, or a verifier or ``redirect_uri`` that does not match)."""
        form = {
            "grant_type": "authorization_code",
            "code": code,
            "code_verifier": code_verifier,
            "redirect_uri": redirect_uri,
        }
        return await self._t.json(
            "POST", "/v1/connect/token", data=form, anonymous=True, no_retry=True, oauth_errors=True
        )


class Billing(_Group):
    """``vs.billing``: the account's plan, credit balance and limits, the
    price list, the ledger and the per-recording charges."""

    async def get(self) -> m.BillingView:
        """The account: plan, rates, balance, limits, subscription, top-up URL."""
        return _one(m.BillingView, await self._t.json("GET", "/v1/billing"))

    async def plans(self) -> m.PlanList:
        """The visible plans with their current rates and limits (no credential needed)."""
        return _one(m.PlanList, await self._t.json("GET", "/v1/billing/plans"))

    async def ledger(self, *, cursor: Optional[str] = None, limit: Optional[int] = None) -> m.LedgerPage:
        """The ledger, newest first; ``cursor`` is the previous page's ``next_cursor``."""
        return _one(m.LedgerPage, await self._t.json("GET", "/v1/billing/ledger", query={"cursor": cursor, "limit": limit}))

    async def charges(self, *, cursor: Optional[str] = None, limit: Optional[int] = None) -> m.ChargePage:
        """The per-recording charges, newest first."""
        return _one(m.ChargePage, await self._t.json("GET", "/v1/billing/charges", query={"cursor": cursor, "limit": limit}))

    async def create_checkout(self, *, idempotency_key: Optional[str] = None, **fields: Any) -> m.CheckoutView:
        """Open a checkout at the payment provider (``kind="topup", amount=``
        or ``kind="subscribe", plan_id=, interval=``, with ``accept_terms=True``:
        the Terms and the request that the purchase start now); send the
        customer to ``url``.  A signed-in person only, never an API key."""
        body = _body(m.CreateCheckout, fields)
        return _one(m.CheckoutView, await self._t.json("POST", "/v1/billing/checkouts", json_body=body, idempotency_key=idempotency_key))

    async def get_checkout(self, id: str) -> m.CheckoutView:
        """A checkout's status (by its id or the provider's)."""
        return _one(m.CheckoutView, await self._t.json("GET", f"/v1/billing/checkouts/{id}"))

    async def cancel_subscription(self) -> m.SubscriptionView:
        """Cancel the subscription at the end of its period."""
        return _one(m.SubscriptionView, await self._t.json("POST", "/v1/billing/subscription/cancel"))

    async def uncancel_subscription(self) -> m.SubscriptionView:
        """Take back a scheduled cancellation."""
        return _one(m.SubscriptionView, await self._t.json("POST", "/v1/billing/subscription/uncancel"))

    async def set_auto_top_up(self, **fields: Any) -> m.AutoTopUpView:
        """Turn auto top-up on (``threshold`` and ``amount`` from the plan list's choices) or off."""
        body = _body(m.AutoTopUpSettings, fields)
        return _one(m.AutoTopUpView, await self._t.json("PUT", "/v1/billing/auto-top-up", json_body=body))

    async def portal(self) -> m.PortalLink:
        """A link to the provider's customer portal (payment methods, invoices, receipts)."""
        return _one(m.PortalLink, await self._t.json("GET", "/v1/billing/portal"))

    async def retry_auto_top_up(self) -> m.AutoTopUpView:
        """Charge the configured auto top-up amount now (under the day's cap)."""
        return _one(m.AutoTopUpView, await self._t.json("POST", "/v1/billing/auto-top-up/retry"))

    async def set_low_balance_alert(self, **fields: Any) -> m.LowBalanceAlertView:
        """Turn the low-balance email on or off (``enabled``) and set its line (``threshold``)."""
        body = _body(m.LowBalanceAlertSettings, fields)
        return _one(m.LowBalanceAlertView, await self._t.json("PUT", "/v1/billing/low-balance-alert", json_body=body))

    async def set_interval(self, **fields: Any) -> m.SubscriptionView:
        """Renew ``interval="month"`` or ``"year"`` from the next period, at
        ``subscription.switch_prices`` (needs ``accept_terms=True`` and
        ``no_new_withdrawal_period_shown=True``)."""
        body = _body(m.SetInterval, fields)
        return _one(m.SubscriptionView, await self._t.json("POST", "/v1/billing/subscription/interval", json_body=body))

    async def opt_in_yearly(self, **fields: Any) -> m.SubscriptionView:
        """Keep the subscription by switching to yearly when monthly billing
        is withdrawn (``subscription.yearly_opt_in``); needs
        ``accept_terms=True`` and ``no_new_withdrawal_period_shown=True``."""
        body = _body(m.YearlyOptIn, fields)
        return _one(m.SubscriptionView, await self._t.json("POST", "/v1/billing/subscription/yearly-opt-in", json_body=body))

    async def unset_interval(self) -> m.SubscriptionView:
        """Take back a scheduled interval switch."""
        return _one(m.SubscriptionView, await self._t.json("DELETE", "/v1/billing/subscription/interval"))

    async def usage(self, *, start_date: Optional[str] = None, end_date: Optional[str] = None, tz: Optional[str] = None) -> m.UsageReport:
        """What the account spent per day (``YYYY-MM-DD`` in ``tz``), source and kind."""
        return _one(m.UsageReport, await self._t.json("GET", "/v1/billing/usage", query={"startDate": start_date, "endDate": end_date, "tz": tz}))

    async def usage_charges(
        self,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        tz: Optional[str] = None,
        kind: Optional[str] = None,
        source: Optional[str] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> m.UsageChargePage:
        """The usage report's charges one by one, newest first; ``kind`` (``file``/``live``) and ``source`` (``dashboard`` or a key id) filter, ``count`` and ``total`` cover every match."""
        query = {"startDate": start_date, "endDate": end_date, "tz": tz, "kind": kind, "source": source, "cursor": cursor, "limit": limit}
        return _one(m.UsageChargePage, await self._t.json("GET", "/v1/billing/usage/charges", query=query))

    async def payments(self, *, cursor: Optional[str] = None, limit: Optional[int] = None) -> m.PaymentPage:
        """The payments history, newest first."""
        return _one(m.PaymentPage, await self._t.json("GET", "/v1/billing/payments", query={"cursor": cursor, "limit": limit}))

    async def payment_document(self, id: str) -> m.PaymentDocument:
        """A payment's invoice or receipt: ``url``, or ``pending`` while the provider makes it."""
        return _one(m.PaymentDocument, await self._t.json("GET", f"/v1/billing/payments/{id}/document"))

    async def payment_methods(self) -> m.PaymentMethodList:
        """The saved payment methods."""
        return _one(m.PaymentMethodList, await self._t.json("GET", "/v1/billing/payment-methods"))

    async def delete_payment_method(self, id: str) -> None:
        """Remove a saved payment method."""
        await self._t.json("DELETE", f"/v1/billing/payment-methods/{id}")

    async def manage_payment_methods(self) -> m.PortalLink:
        """A link to the provider's portal to add a card or choose the default."""
        return _one(m.PortalLink, await self._t.json("POST", "/v1/billing/payment-methods/manage"))

    async def details(self) -> m.BillingDetails:
        """The invoice details."""
        return _one(m.BillingDetails, await self._t.json("GET", "/v1/billing/details"))

    async def set_details(self, **fields: Any) -> m.BillingDetails:
        """Save the invoice details (``name``, ``email``, ``line1``, ``city``, ``postal_code``, ``country``, ``tax_id``)."""
        body = _body(m.BillingDetails, fields)
        return _one(m.BillingDetails, await self._t.json("PUT", "/v1/billing/details", json_body=body))

    async def check_promo_code(self, **fields: Any) -> m.PromoCodeView:
        """What a promo code gives (``code``, ``for_="topup"`` or ``"subscribe"``)."""
        body = _body(m.CheckPromoCode, fields)
        return _one(m.PromoCodeView, await self._t.json("POST", "/v1/billing/promo-codes/check", json_body=body))


class Languages(_Group):
    async def list(self) -> m.LanguageLists:
        """Spoken and translation languages."""
        return _one(m.LanguageLists, await self._t.json("GET", "/v1/languages"))


class Rtmp(_Group):
    async def status(self) -> m.RtmpStatus:
        """The account's RTMP ingest: server URL (RTMPS where available), the stream key (in full only in the answer that made it, masked_stream_key after), whether an encoder is sending."""
        return _one(m.RtmpStatus, await self._t.json("GET", "/v1/rtmp"))

    async def create_key(self, **fields: Any) -> m.CreatedAppStreamKey:
        """A stream key of the app's own, beside the account's (``name``): ``stream_key`` is in full in this answer only. It works while the API key that made it does."""
        body = _body(m.CreateAppStreamKey, fields)
        return _one(m.CreatedAppStreamKey, await self._t.json("POST", "/v1/rtmp/keys", json_body=body))

    async def list_keys(self) -> List[m.AppStreamKey]:
        """The account's app stream keys, oldest first: masked, each with whether an encoder is sending on it (``live``, ``audio``)."""
        return _many(m.AppStreamKey, await self._t.json("GET", "/v1/rtmp/keys"), "keys")

    async def delete_key(self, id: str) -> None:
        """Delete an app stream key: an encoder publishing on it is dropped within seconds."""
        await self._t.json("DELETE", f"/v1/rtmp/keys/{_p(id)}")


class Events(_Group):
    async def account(self) -> Any:
        """The account socket: ``events.ready``, then ``lists.changed``
        whenever a list of broadcasts or transcriptions is out of date."""
        return await websockets.connect(self._t.ws_url("/v1/events"), additional_headers=self._t.auth_headers())


class Meta(_Group):
    async def health(self) -> m.Health:
        return _one(m.Health, await self._t.json("GET", "/v1/health"))

    async def openapi(self) -> Dict[str, Any]:
        """The OpenAPI document the models are generated from."""
        return await self._t.json("GET", "/v1/openapi.json")
