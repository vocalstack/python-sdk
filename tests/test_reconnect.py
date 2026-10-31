"""A realtime session through the relay's restart close (1012), against a
local WebSocket server. No network:

    python -m unittest discover -s tests
"""

import asyncio
import json
import unittest

import websockets

from vocalstack.errors import VocalStackError
from vocalstack.realtime import RESTART_CLOSE, RealtimeSession

DONE = {"type": "session.done", "reason": "eof", "stats": {"audioS": 1, "processingS": 0.5, "realtimeFactor": 2}}


def run(coro):
    return asyncio.run(coro)


def stream_server(script):
    """Each connection says ready; ``script[i]`` = (after_bytes, close code)
    for connection i, or none: commit one segment on ``eof`` and say done."""
    conns = []

    async def serve(ws):
        i = len(conns)
        c = {"bytes": 0, "texts": []}
        conns.append(c)
        step = script[i] if i < len(script) else None
        await ws.send(json.dumps({"type": "session.ready", "sessionId": "s"}))
        async for m in ws:
            if isinstance(m, bytes):
                c["bytes"] += len(m)
                if step and c["bytes"] >= step[0]:
                    await ws.close(step[1], "the relay is restarting; reconnect")
                    return
                continue
            f = json.loads(m)
            c["texts"].append(f["type"])
            if f["type"] == "eof" and not step:
                await ws.send(json.dumps({"type": "transcript.committed", "segment": {"seq": 0, "start": 0, "end": 1, "text": "carried on", "speaker": 0, "words": []}}))
                await ws.send(json.dumps(DONE))
                await ws.close()
                return

    return serve, conns


async def collect(session):
    got, err = [], None
    try:
        async for ev in session:
            got.append(ev.type)
    except VocalStackError as e:
        err = e
    return got, err


class RestartClose(unittest.TestCase):
    def test_a_restart_close_reconnects_to_the_same_stream_url_and_the_session_goes_on(self):
        serve, conns = stream_server([(3200, RESTART_CLOSE)])

        async def go():
            async with websockets.serve(serve, "127.0.0.1", 0) as server:
                port = server.sockets[0].getsockname()[1]
                session = await RealtimeSession.connect("s", f"ws://127.0.0.1:{port}/x")
                reader = asyncio.create_task(collect(session))
                await session.send_audio(b"\0" * 3200)
                await asyncio.sleep(0.2)
                await session.send_audio(b"\0" * 1600)
                await session.eof()
                got, err = await asyncio.wait_for(reader, 20)
                return got, err, session.done_reason

        got, err, reason = run(go())
        self.assertIsNone(err, err)
        self.assertEqual(len(conns), 2, "one reconnect")
        self.assertEqual(conns[1]["bytes"], 1600, "the audio sent meanwhile reached the next socket")
        self.assertEqual(conns[1]["texts"], ["eof"])
        self.assertEqual(got, ["transcript.committed"])
        self.assertEqual(reason, "eof")

    def test_restart_closes_past_the_tries_end_the_session_as_engine_lost(self):
        serve, conns = stream_server([(1, RESTART_CLOSE)] * 4)

        async def go():
            async with websockets.serve(serve, "127.0.0.1", 0) as server:
                port = server.sockets[0].getsockname()[1]
                session = await RealtimeSession.connect("s", f"ws://127.0.0.1:{port}/x", ready_deadline_s=2)
                reader = asyncio.create_task(collect(session))

                async def feed():
                    while not reader.done():
                        await session.send_audio(b"\0" * 64)
                        await asyncio.sleep(0.05)

                feeder = asyncio.create_task(feed())
                got, err = await asyncio.wait_for(reader, 30)
                feeder.cancel()
                return err

        err = run(go())
        self.assertIsNotNone(err)
        self.assertEqual(err.code, "engine_lost")
        self.assertIn("1012", str(err))
        self.assertEqual(len(conns), 4, "the first socket and three reconnects")

    def test_any_other_close_is_engine_lost_at_once_no_reconnect(self):
        serve, conns = stream_server([(1, 1011)])

        async def go():
            async with websockets.serve(serve, "127.0.0.1", 0) as server:
                port = server.sockets[0].getsockname()[1]
                session = await RealtimeSession.connect("s", f"ws://127.0.0.1:{port}/x")
                reader = asyncio.create_task(collect(session))
                await session.send_audio(b"\0" * 64)
                got, err = await asyncio.wait_for(reader, 10)
                return err

        err = run(go())
        self.assertEqual(err.code, "engine_lost")
        self.assertEqual(len(conns), 1)


if __name__ == "__main__":
    unittest.main()
