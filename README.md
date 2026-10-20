# vocalstack

The VocalStack speech API for Python (3.10 or newer, asyncio): one client,
`VocalStack`, with one group per resource. Requests go through `httpx`; answers
are pydantic models generated from the API's one OpenAPI document
(`/v1/openapi.json`), with snake_case attributes.

```python
import asyncio, os
from vocalstack import VocalStack

async def main():
    async with VocalStack(api_key=os.environ["VOCALSTACK_API_KEY"]) as vs:  # base_url defaults to https://api.vocalstack.com
        tx = await vs.transcriptions.create(url="https://example.com/talk.mp3")  # or file="talk.mp3"
        job = await vs.jobs.wait(tx.job.recording_id)                 # done | failed | cropping
        snap = await vs.recordings.transcript(job.recording_id)
        print(" ".join(s.text for s in snap.sentences))
        open("talk.srt", "wb").write(await vs.recordings.export(job.recording_id, "srt"))

asyncio.run(main())
```

<!-- sdk-docs: generated from the VocalStack API guides; edits here are overwritten -->

## Install

```sh
pip install vocalstack
```

## Guides

- [Quickstart](https://github.com/vocalstack/python-sdk/blob/main/docs/quickstart.md): An API key, the SDK, and your first transcript in a dozen lines.
- [Transcribe a file](https://github.com/vocalstack/python-sdk/blob/main/docs/transcribe-a-file.md): Upload a file, wait for its job, read the transcript and export it. Asynchronous; nothing to keep open.
- [Transcribe a link](https://github.com/vocalstack/python-sdk/blob/main/docs/transcribe-a-link.md): The server fetches a public URL or a YouTube video, then transcribes it like a file. Asynchronous, with a fetch phase first.
- [Stream a microphone](https://github.com/vocalstack/python-sdk/blob/main/docs/stream-a-microphone.md): Live audio in, transcript events out, over one WebSocket. Drafts replace the live tail; commits append.
- [Stream a file at full speed](https://github.com/vocalstack/python-sdk/blob/main/docs/stream-a-file.md): Send a file's bytes through a realtime session and read the transcript as fast as the server makes it. Nothing is stored.
- [Go live](https://github.com/vocalstack/python-sdk/blob/main/docs/go-live.md): Publish live audio to a stored transcription, and put it on a broadcast link listeners open. Control is HTTP; audio is one WebSocket.
- [Listen](https://github.com/vocalstack/python-sdk/blob/main/docs/listen.md): Follow a recording as it happens, in any language: subscribe, catch up from a snapshot, then apply live frames in order.
- [Live translation and spoken translations](https://github.com/vocalstack/python-sdk/blob/main/docs/translation-and-voices.md): Translate a stream as it is spoken, and have each translated sentence spoken back in a voice. Text frames and audio clips, over the same socket.
- [Connect an app with one click](https://github.com/vocalstack/python-sdk/blob/main/docs/connect-an-app.md): Let the people who use your app connect their VocalStack account with one Authenticate click — OAuth 2.0 authorization code with PKCE, where the access token is an API key — instead of pasting a key.
- [Errors, retries and limits](https://github.com/vocalstack/python-sdk/blob/main/docs/errors-and-limits.md): One error envelope everywhere; what to retry (429, 503 unavailable) and how to make a retry safe (Idempotency-Key); credit, plan limits and the 402s.

<!-- /sdk-docs -->

## What is what

| Name | What it is | Group |
|---|---|---|
| realtime session | a signed token plus one socket; nothing stored | `vs.realtime` |
| broadcast | the link listeners open | `vs.broadcasts` |
| transcription | a file, link, microphone or stream, transcribed and stored | `vs.transcriptions` |
| recording | one take inside a broadcast or a transcription | `vs.recordings` |
| job | a file or link recording's transcription work | `vs.jobs` |

The other groups: `vs.corrections`, `vs.share_links`, `vs.voices`,
`vs.search`, `vs.me`, `vs.languages`, `vs.rtmp`, `vs.events`, `vs.meta`.
Method names are the document's `operationId`s in snake_case
(`broadcasts.start_recording` is `POST /v1/broadcasts/{id}/recordings`).
Request fields are keyword arguments, in snake_case or the wire's camelCase
(`settings={"spokenLanguage": "en"}`); lists return lists; binary answers
(`recordings.export`, `recordings.clip`, `voices.sample`, ...) return bytes.

```python
VocalStack(
    base_url=None,          # default https://api.vocalstack.com
    api_key=None,           # an API key (vs_key_...), for a server
    token=None,             # or: an access token / browser token, or a callable returning one
    acting_principal=None,  # act for an organization (X-Acting-Principal)
    device_id=None,
    max_retries=2,          # 429s, and 503 unavailable on safe calls, retried after Retry-After
    timeout=300.0,
    ready_deadline_s=30.0,  # how long a socket may take to report ready
)
```

## Realtime sessions and live recordings

```python
async with VocalStack(api_key=key) as vs:
    # Nothing stored: a file through one socket.
    result = await vs.realtime.transcribe_file("meeting.mp3", spoken_language="en")
    print(result.text)

    # A stored live transcription: start a recording, stream raw PCM
    # (f32le, 16 kHz, mono) into the publisher socket.
    tx = await vs.transcriptions.create_live(name="Standup")
    grant = await vs.transcriptions.start_recording(tx.id)
    async with await vs.recordings.publish(grant) as stream:
        asyncio.create_task(pace_realtime(stream, pcm))  # then eof()
        async for event in stream:
            if event.type == "transcript.committed":
                print(event.segment.speaker, event.segment.text)
```

- `transcript.draft` frames **fully replace** the live tail; never append them.
- `transcript.committed` segments are immutable, gapless by `segment.seq`,
  and carry word timings.
- A dropped connection ends the socket (no resume): start a new one.

## Errors

`VocalStackError` carries `code` (the contract's closed enum), `message`,
`status`, `retryable`, `details` (quota numbers, code suggestions), `headers`
and `retry_after`.

Verified against a language-agnostic conformance suite shared by every
VocalStack SDK and against a running API. For agents: `SKILL.md`.
