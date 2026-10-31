"""The helpers the reference's guides lean on: ``vs.jobs.wait``'s
``on_progress`` (against httpx.MockTransport) and a realtime socket's
``on_frame`` (against a local WebSocket server). No network:

    python -m unittest discover -s tests
"""

import asyncio
import json
import unittest

import httpx
import websockets

from vocalstack import VocalStack
from vocalstack.errors import VocalStackError
from vocalstack.realtime import RealtimeSession


def run(coro):
    return asyncio.run(coro)


class JobsWait(unittest.TestCase):
    def test_every_job_read_reaches_on_progress_the_last_one_included(self):
        states = [("queued", 0), ("transcribing", 40), ("done", 100)]
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            state, pct = states[min(len(calls) - 1, len(states) - 1)]
            body = {
                "recordingId": "r1",
                "transcriptionId": "t1",
                "state": state,
                "attempts": 0,
                "createdAt": "2026-10-08T00:00:00Z",
                "hasAudio": state == "done",
                "progress": {"pct": pct, "processedS": 0.0, "segmentsDone": 0},
            }
            return httpx.Response(200, json=body)

        async def go():
            http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            vs = VocalStack("http://api.test", api_key="vs_key_x", http=http)
            seen = []
            job = await vs.jobs.wait("r1", interval_s=0, on_progress=lambda j: seen.append(f"{j.state}:{j.progress.pct}"))
            return job, seen

        job, seen = run(go())
        self.assertEqual(job.state, "done")
        self.assertEqual(seen, ["queued:0", "transcribing:40", "done:100"])


class OnFrame(unittest.TestCase):
    def test_every_frame_reaches_on_frame_lifecycle_frames_included(self):
        async def serve(ws):
            await ws.send(json.dumps({"type": "session.loading", "stage": "model"}))
            await ws.send(json.dumps({"type": "session.ready", "sessionId": "s1"}))
            async for m in ws:
                if isinstance(m, bytes) or json.loads(m)["type"] != "eof":
                    continue
                await ws.send(json.dumps({"type": "transcript.draft", "segments": [{"start": 0, "text": "hel", "speaker": 0, "tentative": True}]}))
                await ws.send(json.dumps({"type": "transcript.committed", "segment": {"seq": 0, "start": 0, "end": 1, "text": "hello", "speaker": 0, "words": []}}))
                await ws.send(json.dumps({"type": "session.done", "reason": "eof", "stats": {"audioS": 1, "processingS": 0.5, "realtimeFactor": 2}}))
                await ws.close()
                return

        async def go():
            frames, events = [], []
            async with websockets.serve(serve, "127.0.0.1", 0) as server:
                port = server.sockets[0].getsockname()[1]
                url = f"ws://127.0.0.1:{port}/v1/realtime/sessions/t/socket"
                session = await RealtimeSession.connect("s1", url, on_frame=lambda f: frames.append(f.type))
                before = list(frames)
                await session.eof()
                async for ev in session:
                    events.append(ev.type)
                return before, frames, events, session.done_reason

        before, frames, events, reason = run(go())
        self.assertEqual(before, ["session.loading", "session.ready"])
        self.assertEqual(events, ["transcript.draft", "transcript.committed"])
        self.assertEqual(frames, ["session.loading", "session.ready", "transcript.draft", "transcript.committed", "session.done"])
        self.assertEqual(reason, "eof")



class EngineLost(unittest.TestCase):
    def test_a_socket_that_closes_with_no_session_done_is_engine_lost_not_a_quiet_end(self):
        async def serve(ws):
            await ws.send(json.dumps({"type": "session.ready", "sessionId": "s1"}))
            await ws.send(json.dumps({"type": "transcript.committed", "segment": {"seq": 0, "start": 0, "end": 1, "text": "before the loss", "speaker": 0, "words": []}}))
            await asyncio.sleep(0.02)
            ws.transport.abort()

        async def go():
            events = []
            async with websockets.serve(serve, "127.0.0.1", 0) as server:
                port = server.sockets[0].getsockname()[1]
                session = await RealtimeSession.connect("s1", f"ws://127.0.0.1:{port}/v1/realtime/sessions/t/socket")
                try:
                    async for ev in session:
                        events.append(ev.type)
                except VocalStackError as e:
                    return events, e, session.done_reason
            return events, None, session.done_reason

        events, err, reason = run(go())
        self.assertEqual(events, ["transcript.committed"], "what came before the loss is still read")
        self.assertIsNotNone(err, "a lost engine raises")
        self.assertEqual(err.code, "engine_lost")
        self.assertTrue(err.retryable)
        self.assertIsNone(reason)

    def test_a_socket_closed_by_close_ends_quietly(self):
        async def serve(ws):
            await ws.send(json.dumps({"type": "session.ready", "sessionId": "s1"}))
            await asyncio.sleep(1)

        async def go():
            async with websockets.serve(serve, "127.0.0.1", 0) as server:
                port = server.sockets[0].getsockname()[1]
                session = await RealtimeSession.connect("s1", f"ws://127.0.0.1:{port}/v1/realtime/sessions/t/socket")
                await session.close()
                return [ev async for ev in session]

        self.assertEqual(run(go()), [])


if __name__ == "__main__":
    unittest.main()
