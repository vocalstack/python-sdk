<!-- Generated from https://api.vocalstack.com/docs/guides/transcribe-a-link.md; edits here are overwritten. -->

# Transcribe a link

> The server fetches a public URL, a YouTube video or a post on a video or audio site, then transcribes it like a file. Asynchronous, with a fetch phase first.

| Endpoint | What it does |
|---|---|
| `GET /v1/transcriptions/probe` | Probe a link |
| `POST /v1/transcriptions` | Create a transcription |
| `GET /v1/transcriptions/{id}/job` | Get a job |
| `GET /v1/transcriptions/{id}/transcript` | Get a transcript |

Give the API a link instead of a file and it fetches the media itself: a public `http(s)` address (an mp3, a video, a podcast episode), a YouTube video, or a public video or audio post on Twitch, Facebook, Instagram, Vimeo, Dailymotion, TikTok, X, SoundCloud, Kick, Mixcloud, Bandcamp, LinkedIn, TED, Internet Archive, Streamable, Odysee, Niconico, VK, BitChute, Wistia, Vidyard, Kaltura, JW Player, Pinterest, Bluesky, Google Drive, Spreaker, iHeartRadio, Audioboom, CNN or Loom (only its audio is downloaded when the site offers it alone). The rest is [Transcribe a file](transcribe-a-file.md): a job to wait on, then the transcript.

## 1. Check the link

`GET /v1/transcriptions/probe` says what a link is before anything is stored: `kind` is `file` (transcribe it here), `live` (a YouTube or Twitch live, an endless radio stream, an HLS playlist with no end) or `unknown`, and `ok` is `false` with a `problem` sentence when the link did not open as media (a web page, an error). For a link from a video or audio site, `siteName` names the site and `title`, `durationS` and `thumbnailUrl` describe it where the site gives them; a private post, or one that needs a sign-in, is `ok: false` with `reason` `forbidden`. A live link never ends, so it is not a file: record it with a live transcription whose source is the link instead ([Go live](go-live.md)).

The probe is optional, and limited to 60 calls a minute. Links to private, loopback or cloud metadata addresses are refused.

## 2. Create the transcription

`POST /v1/transcriptions` with JSON `{ "url": "…" }` (and optional `name` and `settings`, as for a file) answers `201` at once; the server fetches the link afterwards. `vs.transcriptions.createFromUrl(url)` is the same call (`$vs->transcriptions->createFromUrl($url)` in PHP). An `Idempotency-Key` makes a retried create return the first transcription instead of a second one.

## 3. Wait for the fetch and the transcription

A link job starts in `fetching`, then moves to `transcribing` and `done` (or `failed`, with `error`). While it fetches, `progress.phase` is `fetching` (bytes arriving: `progress.fetchedBytes` of `progress.fetchTotalBytes`, when the other end says) or `extracting` (the audio is being taken out of a video), and `progress.phasePct` is how far that phase is. A long video takes a while to fetch; poll `GET /v1/transcriptions/{id}/job` every few seconds (`vs.transcriptions.waitForJob` does) rather than in a tight loop.

## 4. Read the transcript

`GET /v1/transcriptions/{id}/transcript`, as for a file: `sentences`, `segments` with word timings, `speakers`, and `lang=` for a translation. The transcription keeps the link it came from (`job.source`).

## Complete code

Each program runs as it is once `VOCALSTACK_API_KEY` holds one of your API keys.

### `link.py`

```python
import asyncio
import os
import sys
import uuid

from vocalstack import VocalStack

URL = sys.argv[1] if len(sys.argv) > 1 else "https://raw.githubusercontent.com/openai/whisper/main/tests/jfk.flac"


def show(job) -> None:
    p = job.progress
    if job.state == "fetching":
        print(f"{p.phase or 'fetching'} {p.phase_pct or 0}%")
    else:
        print(f"{job.state} {p.pct}%")


async def main() -> None:
    async with VocalStack("https://api.vocalstack.com", api_key=os.environ["VOCALSTACK_API_KEY"]) as vs:
        probe = await vs.transcriptions.probe(URL)
        if not probe.ok:
            raise SystemExit(f"that link did not open: {probe.problem}")
        if probe.kind == "live":
            raise SystemExit("a live link: record it with a live transcription instead")

        tx = await vs.transcriptions.create(url=URL, name="From a link", idempotency_key=str(uuid.uuid4()))

        job = await vs.transcriptions.wait_for_job(tx.id, interval_s=3, on_progress=show)
        if job.state != "done":
            raise SystemExit(f"transcription {job.state}: {job.error}")

        snapshot = await vs.transcriptions.transcript(tx.id)
        print(" ".join(s.text for s in snapshot.sentences))


asyncio.run(main())
```

---

The same guide for every SDK and for plain HTTP + WebSocket: https://api.vocalstack.com/docs/guides/transcribe-a-link.md. Every guide: [docs/](./).
