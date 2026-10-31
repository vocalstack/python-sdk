---
name: vocalstack
description: Use when writing Python code that calls the VocalStack speech API with the vocalstack package, for example to transcribe an audio or video URL or a local file, stream live audio for realtime transcription, fetch or export a transcript (JSON, SRT, XLSX, PDF), or search transcripts. Covers installing, API keys, base URLs, five copy-paste programs and error handling (401, 402, 403, 429).
---

# VocalStack Python SDK

The VocalStack API transcribes speech (files, links, live audio), with speaker
labels and translation. The `vocalstack` package has one asyncio client,
`VocalStack`, with one group per resource: `vs.transcriptions`, `vs.jobs`,
`vs.recordings`, `vs.broadcasts`, `vs.search`, `vs.realtime` and a few more
(listed at the end). Answers are pydantic models with snake_case attributes.

## What is what

- A **transcription** is a file, a link, a microphone or a stream, transcribed
  and stored. Create one with `await vs.transcriptions.create(...)`.
- A **recording** is one take inside it. Its id (`recording_id`) is what you
  read the transcript and exports with: `await vs.recordings.transcript(recording_id)`.
- A **job** is a file or link recording's transcription work:
  `await vs.jobs.wait(recording_id)` polls it until it is `done` or `failed`
  (`on_progress=lambda job: ...` sees each read: `job.state`, `job.progress.pct`).
- A **batch** is many files transcribed together for less than one
  transcription each: `vs.batches.create()`, `add_file`/`add_url` per file,
  `start`, then `wait` (section 6).
- A **broadcast** is a link listeners open to follow a live recording.
- A **realtime session** (`vs.realtime`) is a socket you stream audio into and
  read the transcript from. Nothing is stored. "Session" means only this.

## Install

```sh
python3 -m venv .venv && . .venv/bin/activate   # system Pythons refuse a bare pip install
pip install vocalstack                           # Python 3.10 or newer
```

Live transcription (program 4) also needs `ffmpeg` on PATH.

Speaker numbers start at 0. A speaker of `-1` means speaker labels were off
for that transcription, so print the text without a speaker.

## Auth and base URL

- Read the key from `VOCALSTACK_API_KEY` and pass it as `api_key`. It goes
  out as `Authorization: Bearer <key>`, also on sockets.
- An API key starts with `vs_key_`. There is one kind of key: every call is
  real usage on the account's tier and quotas.
- Keys are for servers. Never put a key in browser code or in a URL. The API
  refuses a key sent from a web page (`Origin` header) or in a query string.
- Scopes: `read` (read transcripts, search, export), `write` and
  `recordings:publish` (create transcriptions, stream live audio), or `*`.
- Base URL: `https://api.vocalstack.com` (the default). Override it with
  `VOCALSTACK_BASE_URL` (for example, a local or staging server).

Every program below opens the client the same way, inside `async def main()`
run with `asyncio.run(main())` (a `None` base URL means the default):

```python
async with VocalStack(os.environ.get("VOCALSTACK_BASE_URL"), api_key=os.environ["VOCALSTACK_API_KEY"]) as vs:
    ...
```

## 1. Transcribe a URL and wait for the result

The server fetches the link itself, so it must be a public http(s) address.
Private, loopback and cloud-metadata addresses are refused.

```python
import asyncio, os
from vocalstack import VocalStack

async def main():
    async with VocalStack(os.environ.get("VOCALSTACK_BASE_URL"), api_key=os.environ["VOCALSTACK_API_KEY"]) as vs:
        tx = await vs.transcriptions.create(
            url="https://raw.githubusercontent.com/openai/whisper/main/tests/jfk.flac",
            name="JFK sample",
        )
        rec = tx.job.recording_id
        job = await vs.jobs.wait(rec)  # queued -> fetching -> transcribing -> done | failed
        if job.state != "done":
            raise SystemExit(f"transcription {job.state}: {job.error}")
        snap = await vs.recordings.transcript(rec)
        for s in snap.sentences:
            print(f"[speaker {s.speaker}] {s.text}" if s.speaker >= 0 else s.text)
        print("recording_id", rec)

asyncio.run(main())
```

## 2. Transcribe a local file

```python
import asyncio, os, sys
from vocalstack import VocalStack

async def main(path):  # mp3, wav, flac, ogg, m4a, mp4 ...
    async with VocalStack(os.environ.get("VOCALSTACK_BASE_URL"), api_key=os.environ["VOCALSTACK_API_KEY"]) as vs:
        tx = await vs.transcriptions.create(
            file=path,  # a path, bytes, or an open binary file
            settings={"spokenLanguage": "en", "speakerLabels": "on"},  # both optional; "auto" (default) | "on" | "off"
            # Off unless asked for: settings summary, title, summaryTranslations,
            # voice (spoken translations), and share_link=True (a public share link).
        )
        job = await vs.jobs.wait(tx.job.recording_id)
        if job.state != "done":
            raise SystemExit(f"transcription {job.state}: {job.error}")
        snap = await vs.recordings.transcript(job.recording_id)
        print(" ".join(s.text for s in snap.sentences))

asyncio.run(main(sys.argv[1]))
```

## 3. Fetch a transcript or an export file

The transcript holds segments (with word timings), sentences and speakers.
Export formats are `json`, `srt`, `xlsx` and `pdf`; add `lang="<code>"` for a
translated file. `export` returns the file's bytes.

```python
import asyncio, os, sys
from vocalstack import VocalStack

async def main(rec):
    async with VocalStack(os.environ.get("VOCALSTACK_BASE_URL"), api_key=os.environ["VOCALSTACK_API_KEY"]) as vs:
        snap = await vs.recordings.transcript(rec)
        print(len(snap.segments), "segments,", len(snap.sentences), "sentences, state", snap.state)
        with open("transcript.srt", "wb") as f:
            f.write(await vs.recordings.export(rec, "srt"))

asyncio.run(main(sys.argv[1]))
```

## 4. Live transcription from a microphone or a file

A live transcription takes raw PCM: 32-bit float little-endian, 16 kHz, mono.
Create it, start a recording, and open the publisher socket the grant names.
Here ffmpeg decodes a file, and `pace_realtime` sends it at realtime speed and
then calls `eof()`. For a microphone, read ffmpeg's stdout as it arrives
(`-f avfoundation -i :0` on macOS, `-f pulse -i default` on Linux) and call
`await stream.send_audio(chunk)` for each chunk.

```python
import asyncio, os, subprocess, sys
from vocalstack import VocalStack, pace_realtime

async def main(path):
    async with VocalStack(os.environ.get("VOCALSTACK_BASE_URL"), api_key=os.environ["VOCALSTACK_API_KEY"]) as vs:
        tx = await vs.transcriptions.create_live(name="Live demo", settings={"spokenLanguage": "en"})
        grant = await vs.transcriptions.start_recording(tx.id)

        pcm = subprocess.run(["ffmpeg", "-loglevel", "error", "-i", path,
                              "-f", "f32le", "-ac", "1", "-ar", "16000", "-"],
                             check=True, capture_output=True).stdout

        # A cold engine can take a while to load, so allow a long ready deadline.
        async with await vs.recordings.publish(grant, ready_deadline_s=180) as stream:
            feeder = asyncio.create_task(pace_realtime(stream, pcm))  # then eof(), which ends the recording
            async for event in stream:
                if event.type == "transcript.committed":
                    print(f"[speaker {event.segment.speaker}] {event.segment.text}", flush=True)
            await feeder

        # To end a recording without eof(): vs.recordings.pause(id), then vs.recordings.stop(id).
        snap = await vs.recordings.transcript(grant.recording.id)
        print("final:", " ".join(s.text for s in snap.sentences))

asyncio.run(main(sys.argv[1]))
```

To transcribe without storing anything, use a realtime session instead:
`(await vs.realtime.transcribe_file("talk.mp3")).text`.

## 5. Search transcripts

```python
import asyncio, os, sys
from vocalstack import VocalStack

async def main(q):  # 2+ characters
    async with VocalStack(os.environ.get("VOCALSTACK_BASE_URL"), api_key=os.environ["VOCALSTACK_API_KEY"]) as vs:
        res = await vs.search.query(q, limit=5)
        for h in res.hits:
            print(f"{h.link_name} @ {h.media_start:.1f}s [speaker {h.speaker}]: {h.text} (recording {h.recording_id})")

asyncio.run(main(sys.argv[1]))
```

## 6. A batch of files

Many files at once cost less as a batch: one create, one minimum charge for
the whole batch. Its files get transcripts, exports and speaker labels (when
`settings.speakerLabels` asks), never a summary, title, share link or voices.

```python
import asyncio, os, sys
from vocalstack import VocalStack

async def main(paths):
    async with VocalStack(os.environ.get("VOCALSTACK_BASE_URL"), api_key=os.environ["VOCALSTACK_API_KEY"]) as vs:
        batch = await vs.batches.create(name="Interviews", settings={"spokenLanguage": "en"})
        for path in paths:
            await vs.batches.add_file(batch.id, file=path)  # or add_url(batch.id, url)
        await vs.batches.start(batch.id)
        done = await vs.batches.wait(batch.id)  # done (or stopped); check done.counts.failed
        for tx in done.files or []:
            if tx.job and tx.job.state == "done":
                snap = await vs.recordings.transcript(tx.job.recording_id)
                print(tx.name, " ".join(s.text for s in snap.sentences))
        # vs.batches.export(batch.id, "srt") is every finished transcript in one zip.

asyncio.run(main(sys.argv[1:]))
```

## Errors

A failed call raises `vocalstack.VocalStackError` with `.code`, `.message`,
`.status`, `.retryable`, `.details` (the body's `data.details`), `.headers`
and `.retry_after` (seconds, from `Retry-After`). A `429`, and a
`503 unavailable` on a call that is safe to repeat, is retried for you after
its `Retry-After`, twice by default (`max_retries` on the client).

| Status | `code` | What to do |
|---|---|---|
| 401 | `invalid_token`, `key_expired`, `key_revoked` | Check `VOCALSTACK_API_KEY`. Use the replacement key, or create a new one. |
| 403 | `forbidden` | Usually a missing scope: the message names it, and `e.headers["www-authenticate"]` lists the scopes the route needs. A suspended key or account gets `suspended`. |
| 403 | `terms_acceptance_required` | The account has not accepted the current Terms of Service and Privacy Policy: someone signs in to the Dashboard and accepts them (an organisation's keys work once an owner or admin has). Not retryable until then. |
| 402 | `insufficient_credit` | The account's credit is out, or does not cover the transcription: every transcription is billed at least the plan's minimum (`rates.minimumSeconds`, 5 minutes). `details`: `balance`, `plan`, `topUpUrl`, and `required` when the cost was known up front. Add credit; `await vs.billing.get()` shows the balance. |
| 409 | `payment_method_required` | Auto top-up (`vs.billing.set_auto_top_up`) and the portal (`vs.billing.portal()`) need a payment first (`vs.billing.create_checkout(kind="topup", amount=25, accept_terms=True)` opens one; a signed-in person's token, never an API key). |
| 402 | `quota_exceeded` | The plan's transcriptions-at-once limit is reached. `details`: `metric` (`concurrent_transcriptions`), `limit`, `used`, `plan`, `topUpUrl`. |
| 429 | `rate_limited` | Retried automatically; if it still fails, wait `e.retry_after` seconds. Every response carries `RateLimit-Limit`, `RateLimit-Remaining` and `RateLimit-Reset`. |
| 503 | `unavailable` | The service is waking (its database was asleep). Retried automatically after `Retry-After` (2 s) on calls that are safe to repeat: GET, HEAD, DELETE, or any call with `idempotency_key=`. A create without a key is not retried: retry it yourself, with an `idempotency_key` so it cannot run twice. A realtime socket refused this way raises `unavailable`; a bare 503 there is `capacity_exceeded`. |

```python
from vocalstack import VocalStackError
try:
    await vs.search.query("country")
except VocalStackError as e:
    if e.code in ("insufficient_credit", "quota_exceeded"):
        print("billing:", e.message, e.details)
    else:
        print(e.status, e.code, e.message)
```

## More

- The other groups: `vs.transcriptions` (`list`, `get`, `update`, `archive`,
  `restore`, `purge`, `list_share_links`, `create_share_link`), `vs.recordings`
  (`get`, `summarize`, `summary`, `translation`, `audio_url`, `subscribe`),
  `vs.batches` (`create`, `add_file`, `add_url`, `start`, `wait`, `get`, `list`,
  `export`, `stop`, `delete`), `vs.corrections`, `vs.share_links`, `vs.voices`, `vs.broadcasts`,
  `vs.me` (`get`, `usage`), `vs.languages`, `vs.rtmp`, `vs.events`, `vs.meta`.
  Each method is its operation's name in the OpenAPI document, in snake_case
  (`transcriptions.create` is `POST /v1/transcriptions`).
- `<base>/llms.txt` lists every operation in a form written for agents.
  `<base>/llms-full.txt` adds parameters, auth mode, tier and scopes.
- `<base>/docs` is the interactive reference. `<base>/v1/openapi.json` is
  the OpenAPI document; the SDK's models are generated from it.
