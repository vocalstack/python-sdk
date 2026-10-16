"""``vs.realtime``: a realtime session is a signed token plus one socket
(``RealtimeSession``, over the public WS frame protocol; a recording's
publisher stream is one too).  Nothing is stored: the transcript is what
the socket sends."""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Union

import websockets
from websockets.asyncio.client import ClientConnection

from ._http import Transport
from ._models import (
    Capabilities,
    CreateSessionResponse,
    ErrorFrame,
    SessionDone,
    SessionOptions,
    SessionStats,
    TranscriptEvent,
    parse_server_frame,
)
from .errors import VocalStackError, capacity_exceeded, from_envelope

DEFAULT_READY_DEADLINE_S = 30.0


def upgrade_refused(status: int, body: Optional[bytes]) -> VocalStackError:
    """A refused socket upgrade as an error. A 503 carrying the error body
    with ``unavailable`` (the database is waking; retry shortly) says so;
    any other 503 (a bare one from a saturated proxy, or no body at all) is
    ``capacity_exceeded``. So is a 502 or 504 without the API's error body:
    the proxy in front of the engines found none free in time. Every other
    status is its own envelope."""
    parsed = None
    try:
        parsed = json.loads(body) if body else None
    except Exception:
        pass
    if status == 503:
        if isinstance(parsed, dict) and parsed.get("code") == "unavailable":
            return from_envelope(parsed, status)
        return capacity_exceeded("stream host answered 503 (fleet saturated)")
    ours = isinstance(parsed, dict) and isinstance(parsed.get("code"), str)
    if status in (502, 504) and not ours:
        return capacity_exceeded(f"stream host answered {status} (no engine answered in time)")
    return from_envelope(parsed, status)


class RealtimeSession:
    """Async-iterable stream of transcript events.

    Yields ``transcript.committed`` / ``transcript.draft`` /
    ``transcript.words`` model objects; lifecycle frames are handled
    internally (``session.loading``/``ready`` before iteration begins,
    ``session.done``/``error`` ending it). Unknown frame types are
    ignored.
    """

    def __init__(
        self, session_id: str, ws: ClientConnection, on_frame: Optional[Callable[[Any], None]] = None
    ) -> None:
        self.session_id = session_id
        self._ws = ws
        self._on_frame = on_frame
        self.done_reason: Optional[str] = None
        self.stats: Optional[SessionStats] = None
        self._error: Optional[VocalStackError] = None
        self._closed = False


    @classmethod
    async def connect(
        cls,
        session_id: str,
        stream_url: str,
        *,
        ready_deadline_s: float = DEFAULT_READY_DEADLINE_S,
        on_frame: Optional[Callable[[Any], None]] = None,
    ) -> "RealtimeSession":
        """Open the WS and wait for ``session.ready``.

        ``on_frame`` sees every server text frame as it arrives, parsed:
        the lifecycle frames the iterator keeps to itself
        (``session.loading``, ``session.ready``, ``session.done``,
        ``error``) and the transcript frames alike. For logs and protocol
        views; iterate the session to read the transcript.

        The deadline covers the gap since the LAST server frame: expiry
        with no frames seen synthesizes ``capacity_exceeded`` (the
        upgrade hung in the proxy's queue); once
        ``session.loading`` heartbeats flow, expiry maps to
        ``engine_unavailable`` instead.
        """
        try:
            # The upgrade waits in the proxy's queue while every engine is
            # busy: the ready deadline covers it, not websockets' own 10 s.
            ws = await websockets.connect(stream_url, max_size=None, open_timeout=ready_deadline_s)
        except websockets.InvalidStatus as e:
            raise upgrade_refused(e.response.status_code, e.response.body) from e
        except TimeoutError as e:
            raise VocalStackError(
                "capacity_exceeded", f"the upgrade was not answered within {ready_deadline_s}s", retryable=True
            ) from e
        except OSError as e:
            raise VocalStackError("engine_unavailable", f"connect failed: {e}", retryable=True) from e

        saw_frame = False
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=ready_deadline_s)
            except asyncio.TimeoutError:
                await ws.close()
                if saw_frame:
                    raise VocalStackError(
                        "engine_unavailable",
                        f"engine did not become ready within {ready_deadline_s}s",
                        retryable=True,
                    ) from None
                raise capacity_exceeded(
                    f"no response from stream host within {ready_deadline_s}s "
                    "(fleet may be saturated)"
                ) from None
            except websockets.ConnectionClosed as e:
                raise VocalStackError(
                    "engine_unavailable", "connection closed before ready", retryable=True
                ) from e
            if isinstance(raw, bytes):
                continue
            saw_frame = True
            frame = parse_server_frame(raw)
            if frame is None:
                continue
            _hand(on_frame, frame)
            if frame.type == "session.ready":
                return cls(session_id, ws, on_frame)
            if frame.type == "error":
                await ws.close()
                raise VocalStackError(frame.code, frame.message, retryable=frame.retryable)


    async def send_audio(self, pcm: Union[bytes, bytearray, memoryview]) -> None:
        """Raw PCM per the session's negotiated format (f32le 16 kHz mono).

        Sends racing the end of the session are silently dropped — the
        event iterator is the authority on how the session ended.
        """
        try:
            await self._ws.send(pcm)
        except websockets.ConnectionClosed:
            pass

    async def send_file(
        self,
        source: Union[str, bytes, bytearray, "os.PathLike[str]"],
        *,
        chunk_bytes: int = 256 * 1024,
    ) -> None:
        """Upload an encoded audio file (file sessions only), then ``eof()``.

        A path is streamed from disk rather than read whole — uploads run
        to hundreds of megabytes.
        """
        if isinstance(source, (bytes, bytearray)):
            for off in range(0, len(source), chunk_bytes):
                await self.send_audio(bytes(source[off : off + chunk_bytes]))
        else:
            with open(source, "rb") as fh:
                while True:
                    chunk = await asyncio.to_thread(fh.read, chunk_bytes)
                    if not chunk:
                        break
                    await self.send_audio(chunk)
        await self.eof()

    async def eof(self) -> None:
        """End of source: server flushes, emits final commits, then
        ``session.done`` — after which iteration completes."""
        try:
            await self._ws.send(json.dumps({"type": "eof"}))
        except websockets.ConnectionClosed:
            pass

    async def update(
        self,
        *,
        latency_mode: Optional[str] = None,
        spoken_language: Optional[str] = None,
        speaker_labels: Optional[str] = None,
        translate: Optional[List[str]] = None,
    ) -> None:
        """Options update during the realtime session; omitted fields are unchanged.

        ``latency_mode`` (``"auto"`` | ``"fast"`` | ``"accuracy"``)
        switches the decode model policy from the next engine step;
        ``spoken_language`` changes the transcription language from the next
        engine step (``""`` or ``"auto"`` = auto-detect); ``speaker_labels``
        turns speaker labelling on (``"on"`` or ``"auto"``) or off (``"off"``) from
        the next engine step; ``translate`` replaces the whole
        target-language list (``[]`` turns translation off). Applies to
        subsequent transcript — already-delivered segments are not
        re-transcribed, re-attributed or re-translated.
        """
        frame: Dict[str, Any] = {"type": "session.update"}
        if latency_mode is not None:
            frame["latencyMode"] = latency_mode
        if spoken_language is not None:
            frame["spokenLanguage"] = spoken_language
        if speaker_labels is not None:
            frame["speakerLabels"] = speaker_labels
        if translate is not None:
            frame["translate"] = translate
        try:
            await self._ws.send(json.dumps(frame))
        except websockets.ConnectionClosed:
            pass

    async def cancel(self) -> None:
        """Stop now, abandoning the rest of the audio.

        The server ends the session with ``session.done`` /
        ``cancelled`` and frees the machine immediately; everything
        committed before the stop stays valid. Prefer this to
        ``close()`` for a deliberate stop — it is a clean end that still
        reports stats.
        """
        try:
            await self._ws.send(json.dumps({"type": "cancel"}))
        except websockets.ConnectionClosed:
            pass

    async def close(self) -> None:
        """Hard-close (aborts the run server-side; no resume, rule 7).
        Prefer ``eof()`` to get final commits."""
        self._closed = True
        await self._ws.close()


    def __aiter__(self) -> AsyncIterator[TranscriptEvent]:
        return self._events()

    async def _events(self) -> AsyncIterator[TranscriptEvent]:
        while True:
            if self._error:
                raise self._error
            if self.done_reason is not None or self._closed:
                return
            try:
                raw = await self._ws.recv()
            except websockets.ConnectionClosed:
                return
            if isinstance(raw, bytes):
                continue
            frame = parse_server_frame(raw)
            if frame is None:
                continue
            _hand(self._on_frame, frame)
            if isinstance(frame, SessionDone):
                self.done_reason = frame.reason
                self.stats = frame.stats
                return
            if isinstance(frame, ErrorFrame):
                self._error = VocalStackError(
                    frame.code, frame.message, retryable=frame.retryable
                )
                raise self._error
            if frame.type.startswith("transcript."):
                yield frame


    async def __aenter__(self) -> "RealtimeSession":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


def _hand(on_frame: Optional[Callable[[Any], None]], frame: Any) -> None:
    """Hands a frame to ``on_frame``; a raising callback never breaks the socket."""
    if on_frame is None:
        return
    try:
        on_frame(frame)
    except Exception:
        pass


async def pace_realtime(
    session: RealtimeSession,
    pcm: bytes,
    *,
    chunk_seconds: float = 0.25,
    speed: float = 1.0,
) -> None:
    """Feed a whole PCM buffer at ``speed``× realtime, then ``eof()``.
    Convenience for file/batch sources against the realtime API."""
    bytes_per_second = 16000 * 4
    chunk = max(4, int(bytes_per_second * chunk_seconds) & ~3)
    start = time.monotonic()
    sent_s = 0.0
    for off in range(0, len(pcm), chunk):
        await session.send_audio(pcm[off : off + chunk])
        sent_s += len(pcm[off : off + chunk]) / bytes_per_second
        target = start + sent_s / speed
        delay = target - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)
    await session.eof()


class RealtimeTranscript:
    """Result of :meth:`Realtime.transcribe_file` / :meth:`Realtime.transcribe_url`."""

    __slots__ = ("segments", "done_reason", "stats")

    def __init__(self, segments: list, done_reason: Optional[str], stats: Any = None) -> None:
        self.segments = segments
        self.done_reason = done_reason
        self.stats = stats

    @property
    def text(self) -> str:
        """All committed text, space-joined."""
        return " ".join(s.text for s in self.segments)

    def __repr__(self) -> str:
        rt = f", realtime={self.stats.realtime_factor}x" if self.stats else ""
        return f"RealtimeTranscript(segments={len(self.segments)}, done_reason={self.done_reason!r}{rt})"


class Realtime:
    def __init__(self, t: Transport, ready_deadline_s: float) -> None:
        self._t = t
        self._ready_deadline_s = ready_deadline_s

    async def create_session(self, **options: Any) -> CreateSessionResponse:
        """``POST /v1/realtime/sessions``: validate the options, get a signed
        ``stream_url``. Keyword args mirror SessionOptions (``spoken_language``,
        ``speaker_labels``, ``latency_mode``, ``events``, ``input``, ...)."""
        body = SessionOptions.model_validate(options).model_dump(by_alias=True, exclude_none=True, mode="json")
        data = await self._t.json("POST", "/v1/realtime/sessions", json_body=body)
        return CreateSessionResponse.model_validate(data)

    async def connect(
        self,
        *,
        ready_deadline_s: Optional[float] = None,
        on_frame: Optional[Callable[[Any], None]] = None,
        **options: Any,
    ) -> RealtimeSession:
        """``create_session``, then open the socket and wait for ``session.ready``.
        ``on_frame`` sees every server frame, lifecycle included (see
        :meth:`RealtimeSession.connect`)."""
        created = await self.create_session(**options)
        return await RealtimeSession.connect(
            created.session_id,
            created.stream_url,
            ready_deadline_s=ready_deadline_s or self._ready_deadline_s,
            on_frame=on_frame,
        )

    async def capabilities(self) -> Capabilities:
        """Languages, audio formats and limits."""
        return Capabilities.model_validate(await self._t.json("GET", "/v1/realtime/capabilities"))

    async def transcribe_file(
        self,
        source: Any,
        *,
        on_progress: Optional[Callable[[Any], None]] = None,
        on_commit: Optional[Callable[[Any], None]] = None,
        **options: Any,
    ) -> RealtimeTranscript:
        """Stream an encoded file (mp3, wav, flac, ogg, m4a...; a path or
        bytes) through a realtime session and collect the transcript.
        Nothing is stored; for a stored, shareable transcription use
        ``vs.transcriptions.create(file=...)``."""
        session = await self.connect(input={"kind": "file"}, **options)
        async with session:
            await session.send_file(source)
            return await _collect(session, on_progress, on_commit)

    async def transcribe_url(
        self,
        url: str,
        *,
        on_progress: Optional[Callable[[Any], None]] = None,
        on_commit: Optional[Callable[[Any], None]] = None,
        **options: Any,
    ) -> RealtimeTranscript:
        """The same for a link the engine fetches itself."""
        session = await self.connect(input={"kind": "url", "url": url}, **options)
        async with session:
            return await _collect(session, on_progress, on_commit)


async def _collect(session: RealtimeSession, on_progress: Any, on_commit: Any) -> RealtimeTranscript:
    segments: List[Any] = []
    async for event in session:
        if event.type == "transcript.committed":
            segments.append(event.segment)
            if on_commit is not None:
                on_commit(event.segment)
        elif event.type == "transcript.progress" and on_progress is not None:
            on_progress(event)
    return RealtimeTranscript(segments, session.done_reason, session.stats)
