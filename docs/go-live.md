<!-- Generated from https://api.vocalstack.com/docs/guides/go-live.md; edits here are overwritten. -->

# Go live

> Publish live audio to a stored transcription, and put it on a broadcast link listeners open. Control is HTTP; audio is one WebSocket.

| Endpoint | What it does |
|---|---|
| `POST /v1/transcriptions/live` | Create a live transcription |
| `POST /v1/broadcasts` | Create a broadcast |
| `PUT /v1/transcriptions/{id}/broadcasts/{broadcast}` | Attach a broadcast |
| `POST /v1/transcriptions/{id}/start` | Start a live transcription |
| `POST /v1/transcriptions/{id}/pause` | Pause a transcription |
| `POST /v1/transcriptions/{id}/resume` | Resume a transcription |
| `POST /v1/transcriptions/{id}/stop` | Stop a transcription |
| `WS /v1/realtime/sessions/{token}/socket` | Connect to the `streamUrl` a realtime session returned (treat it as opaque). |

A realtime session is gone when its socket closes. To keep what is said, and let other people follow it as it happens, publish to a **live transcription**: it is stored as it is spoken, listed under Transcriptions, and can be read, translated, exported and shared afterwards. A **broadcast** is the link listeners open (`code`, `shareUrl`); it is fed by the live transcriptions attached to it. The order: create the live transcription, optionally put it on a broadcast, start it, publish audio to its `streamUrl`, and control it over HTTP.

## 1. Create a live transcription

`POST /v1/transcriptions/live` with a `name` and optional `settings` (`spokenLanguage`, `speakerLabels`, `latencyMode`…) creates it idle (`state: "idle"`). Nothing is recorded until you start it. Its `source` can also be a stream the server pulls itself (a YouTube, Twitch or Kick live, an internet radio address), in which case there is no audio for you to send. Or an RTMP encoder (OBS, vMix…): the `source.url` is the `serverUrl` of `GET /v1/rtmp`, and the encoder publishes there with a stream key. An app that drives the encoder (a plugin in it, say) makes a key of its own with `POST /v1/rtmp/keys` rather than taking the account's, which another encoder may be using: every key of the account feeds the same source.

## 2. Put it on a broadcast

`POST /v1/broadcasts` creates the link (`code`, and `shareUrl` to hand out; `access: "private"` and a `password` keep it to the people you give them). `PUT /v1/transcriptions/{id}/broadcasts/{broadcast}` feeds it from the live transcription. A transcription feeds one broadcast at a time (attaching it to another moves it there), and a broadcast can be fed by several transcriptions. Listeners follow it with the subscribe socket ([Listen](listen.md)).

## 3. Start it

`POST /v1/transcriptions/{id}/start` starts it and answers with a publish grant: `transcription` (the same transcription, now `pending`), `sessionId` and `streamUrl`. A transcription runs once: it can be paused and resumed as often as you like, but once stopped it cannot be started again (create a new one). A `409 conflict` with `details.takeover` means another device is sending its audio now; repeat with `{ "takeover": true }` to send it from here instead.

## 4. Publish audio

Connect to `streamUrl` and publish exactly as on a realtime session ([Stream a microphone](stream-a-microphone.md)): wait for `session.ready`, send raw PCM (`pcm_f32le`, 16 kHz, mono) as binary frames, and read commits back. The difference is that every commit is also stored and fanned out to the transcription's subscribers, translated into the languages they read. A cold machine can take a minute to become ready: give `connect` a long ready deadline (`readyDeadlineMs`, `ready_deadline_s`, PHP's `readyDeadline:` in seconds).

## 5. Pause, resume and stop

Control is HTTP, never a frame on the socket: `pause` (idempotent), `resume`, and `stop`, which is allowed only from paused. Sending `eof` on the publisher socket also ends it. A transcription moves `live` ⇄ `paused` → `ended` → `finalized` (its transcript settled); paused and resumed as often as you like, it stays one transcript. Listeners stay connected through `ended`. One left paused longer than `settings.maxPauseS` ends by itself.

## Complete code

Each program runs as it is once `VOCALSTACK_API_KEY` holds one of your API keys.

### `live.py`

```python
import asyncio
import os

from vocalstack import VocalStack, pace_realtime

# -re is not needed here: pace_realtime sends the PCM at its own pace.
DECODE = ["ffmpeg", "-loglevel", "error", "-i", "./talk.mp3", "-f", "f32le", "-ac", "1", "-ar", "16000", "-"]


async def main() -> None:
    async with VocalStack("https://api.vocalstack.com", api_key=os.environ["VOCALSTACK_API_KEY"]) as vs:
        tx = await vs.transcriptions.create_live(name="Town hall", settings={"spokenLanguage": "en"})

        broadcast = await vs.broadcasts.create(name="Town hall")
        await vs.transcriptions.attach_broadcast(tx.id, broadcast.id)
        print("listeners open", broadcast.share_url)

        grant = await vs.transcriptions.start(tx.id)

        ffmpeg = await asyncio.create_subprocess_exec(*DECODE, stdout=asyncio.subprocess.PIPE)
        pcm, _ = await ffmpeg.communicate()
        async with await vs.transcriptions.publish(grant, ready_deadline_s=180) as stream:
            feeder = asyncio.create_task(pace_realtime(stream, pcm))  # real time, then eof(): the transcription ends
            async for event in stream:
                if event.type == "transcript.committed":
                    print(f"#{event.segment.seq} {event.segment.text}", flush=True)
            await feeder

        # To end without eof: await vs.transcriptions.pause(tx.id), then stop(tx.id).
        print("ended:", (await vs.transcriptions.get(tx.id)).state)


asyncio.run(main())
```

---

The same guide for every SDK and for plain HTTP + WebSocket: https://api.vocalstack.com/docs/guides/go-live.md. Every guide: [docs/](./).
