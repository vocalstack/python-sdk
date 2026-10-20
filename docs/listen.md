<!-- Generated from https://api.vocalstack.com/docs/guides/listen.md; edits here are overwritten. -->

# Listen

> Follow a recording as it happens, in any language: subscribe, catch up from a snapshot, then apply live frames in order.

| Endpoint | What it does |
|---|---|
| `GET /v1/recordings/{id}/transcript` | Get a transcript |
| `GET /v1/recordings/{id}` | Get a recording |
| `WS /v1/recordings/{id}/subscribe` | Follow a recording: first frame `subscribe.ready {lastSeq}`, then `GET /v1/recordings/{id}/transcript` and discard buffered frames with `seq <= lastSeq`. |

Anyone allowed to read a recording can follow it live over its subscribe socket: the owner with an API key, a reader with a share link's code, anyone at all on a public broadcast. The socket sends what changes from the moment it opens; what was said before is read once, from a snapshot. Getting the two to meet without a gap or a duplicate is the whole trick, and the cursor makes it exact.

## 1. Find the recording

Subscribing takes a recording id. The owner has it already: the publish grant's `recording.id`, a transcription's or broadcast's `currentRecording.recordingId`, or its list of recordings. A screen waiting for something to go live opens the events socket of the transcription or the broadcast (`WS /v1/transcriptions/{id}/events`, `/v1/broadcasts/{id}/events`) and gets `recording.started` with the `recordingId` the moment it does.

## 2. Subscribe

Open `WS /v1/recordings/{id}/subscribe`. The owner sends the API key as `Authorization: Bearer …` (never in the URL); a share link's reader adds `shareCode=`; a public broadcast's listener sends nothing. `lang=es` reads it translated (through a share link, the link must allow translation), and `voice=` picks who speaks it ([Live translation and spoken translations](translation-and-voices.md)). The first frame is `subscribe.ready`: the recording's `state` and `lastSeq`, the cursor the live sequence is at.

## 3. Catch up

Read `GET /v1/recordings/{id}/transcript` (same `lang`, same credential): every sentence and segment up to the snapshot's own `lastSeq`. Every durable frame on the socket carries a `cursor`, gapless per recording; skip the frames whose `cursor` is at or below the snapshot's `lastSeq` (they are in it already) and apply the rest in order. A listener gets a `noop` frame in place of one withheld from it, so the cursor never skips. If it does jump (a dropped frame, `subscribe.gap`), read the transcript again with `after=<cursor>` and carry on from its `lastSeq`. `vs.recordings.subscribe` in the TypeScript SDK does all of this and hands you the snapshot first.

```text
+0.0s {"type":"subscribe.ready","recordingId":"5c0d…","lastSeq":41,"state":"live","instance":"api-1"}
+0.2s {"type":"(snapshot)","lastSeq":42,"text":"Good morning, everyone. Thanks for coming in today."}
+0.6s {"type":"transcript.committed","cursor":42,"seq":42,"segment":{"seq":17,"start":5.1,"end":6.0,"text":"Thanks for coming in today.","speaker":0,"words":[]}}
+1.9s {"type":"transcript.committed","cursor":43,"seq":43,"segment":{"seq":18,"start":6.4,"end":8.2,"text":"Let us start with the budget.","speaker":0,"words":[]}}
+2.2s {"type":"transcript.translated","cursor":44,"seq":18,"translations":{"es":"Empecemos por el presupuesto."}}
+3.0s {"type":"noop","cursor":45,"seq":45}
+4.4s {"type":"recording.state","cursor":46,"seq":46,"recordingId":"5c0d…","state":"paused"}
```

## 4. Follow it live

From there, frames arrive as the recording goes: `transcript.committed` appends a segment (a subscriber gets no drafts), `transcript.sentence` replaces a whole sentence by `sentenceId`, `transcript.translated` fills a segment's translations, `transcript.amended` (the engine) and `transcript.corrected` (a person) rewrite one in place, and `recording.state` says `live`, `paused`, `ended`, `finalized`. Change language without reopening: send `{"type":"subscribe.update","lang":"fr"}` (`setLanguage` in the SDK), then read the snapshot again in it.

## 5. When it ends

At `ended` the socket stays open: corrections and the last translations can still arrive until `finalized`. Close it when you are done. Afterwards, the snapshot is the whole transcript, and `GET /v1/recordings/{id}` says how it ended.

## Complete code

Each program runs as it is once `VOCALSTACK_API_KEY` holds one of your API keys.

### `listen.py`

```python
import asyncio
import json
import os
import sys

from vocalstack import VocalStack


async def main() -> None:
    async with VocalStack("https://api.vocalstack.com", api_key=os.environ["VOCALSTACK_API_KEY"]) as vs:
        recording_id = sys.argv[1]  # a grant's recording.id, or a transcription's current_recording

        # The owner, with the key. A share link's reader: share_code="CODE", as_owner=False.
        socket = await vs.recordings.subscribe(recording_id, lang="es")
        ready = json.loads(await socket.recv())  # subscribe.ready {lastSeq, state}
        print("state:", ready["state"])

        snapshot = await vs.recordings.transcript(recording_id, lang="es")
        for s in snapshot.sentences:
            print(s.text, (s.translations or {}).get("es", ""))
        cursor = snapshot.last_seq

        async for message in socket:
            if isinstance(message, bytes):
                continue  # live audio, when asked for
            frame = json.loads(message)
            at = frame.get("cursor")
            if at is not None:
                if at <= cursor:
                    continue  # already in the snapshot
                if at > cursor + 1:  # a gap: read what was missed, then go on
                    missed = await vs.recordings.transcript(recording_id, lang="es", after=cursor)
                    for seg in missed.segments:
                        print(seg.text)
                    cursor = max(cursor, missed.last_seq)
                    if at <= cursor:
                        continue
                cursor = at
            if frame["type"] == "transcript.committed":
                print(frame["segment"]["text"], flush=True)
            elif frame["type"] == "transcript.translated":
                print("  es:", frame["translations"].get("es"))
            elif frame["type"] == "recording.state":
                print(f"[{frame['state']}]")
                if frame["state"] == "finalized":
                    await socket.close()


asyncio.run(main())
```

---

The same guide for every SDK and for plain HTTP + WebSocket: https://api.vocalstack.com/docs/guides/listen.md. Every guide: [docs/](./).
