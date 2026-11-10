"""A realtime session through the relay's restart close (1012), against a
local WebSocket server. No network:

    python -m unittest discover -s tests
"""

import asyncio
import json
import unittest
from unittest import mock

import websockets

from vocalstack import realtime
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

    def test_sockets_that_stayed_up_start_the_tries_anew(self):
        serve, conns = stream_server([(1, RESTART_CLOSE)] * 5)

        async def attached(n):
            while len(conns) < n:
                await asyncio.sleep(0.02)
            await asyncio.sleep(0.4)

        async def go():
            async with websockets.serve(serve, "127.0.0.1", 0) as server:
                port = server.sockets[0].getsockname()[1]
                session = await RealtimeSession.connect("s", f"ws://127.0.0.1:{port}/x", ready_deadline_s=2)
                reader = asyncio.create_task(collect(session))
                for i in range(1, 6):
                    await attached(i)
                    await session.send_audio(b"\0" * 64)
                await attached(6)
                await session.eof()
                got, err = await asyncio.wait_for(reader, 30)
                return err, session.done_reason

        with mock.patch.object(realtime, "STEADY_S", 0.3):
            err, reason = run(go())
        self.assertIsNone(err, err)
        self.assertEqual(len(conns), 6, "five restart closes, each after a steady socket")
        self.assertEqual(reason, "eof")

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


def lost_server(answers):
    """The first socket drops with no close frame once audio arrives; each
    later upgrade gets ``answers[i]``: ``again`` (503, try again), ``gone``
    (410), ``silent`` (the connection is cut), else it is served to its end."""
    seen = {"upgrades": 0, "bytes": {}}

    def answer(connection, request):
        n = seen["upgrades"]
        seen["upgrades"] += 1
        connection.nth = n
        how = "drop" if n == 0 else (answers[n - 1] if n - 1 < len(answers) else "serve")
        connection.how = how
        if how == "again":
            return connection.respond(503, json.dumps({"code": "engine_unavailable", "message": "The connection to this session was lost and is being restored. Try again in a moment.", "data": {"retryable": True}}))
        if how == "gone":
            return connection.respond(410, json.dumps({"code": "session_expired", "message": "session token connect-by deadline has passed", "data": {"retryable": False}}))
        if how == "silent":
            connection.transport.abort()
        return None

    async def serve(ws):
        seen["bytes"][ws.nth] = 0
        await ws.send(json.dumps({"type": "session.ready", "sessionId": "s"}))
        async for m in ws:
            if isinstance(m, bytes):
                seen["bytes"][ws.nth] += len(m)
                if ws.how == "drop":
                    ws.transport.abort()
                    return
                continue
            if json.loads(m)["type"] == "eof":
                await ws.send(json.dumps(DONE))
                await ws.close()
                return

    return serve, answer, seen


class LostSocket(unittest.TestCase):
    def lose(self, answers, audio_after=None, steady_s=0.0):
        serve, answer, seen = lost_server(answers)

        async def go():
            async with websockets.serve(serve, "127.0.0.1", 0, process_request=answer) as server:
                port = server.sockets[0].getsockname()[1]
                session = await RealtimeSession.connect("s", f"ws://127.0.0.1:{port}/x", ready_deadline_s=2)
                reader = asyncio.create_task(collect(session))
                await session.send_audio(b"\0" * 3200)
                if audio_after:
                    await asyncio.sleep(0.1)
                    await session.send_audio(b"\0" * audio_after)
                    await session.eof()
                got, err = await asyncio.wait_for(reader, 20)
                return err, session.done_reason

        with mock.patch.object(realtime, "LOST_WAITS_S", [0.05, 0.05, 0.05, 0.05]), mock.patch.object(realtime, "STEADY_S", steady_s):
            err, reason = run(go())
        return err, reason, seen

    def test_a_socket_lost_with_no_close_frame_soon_after_it_opened_is_engine_lost_with_no_try(self):
        err, _, seen = self.lose(["serve"], steady_s=30.0)
        self.assertEqual(err.code, "engine_lost")
        self.assertEqual(seen["upgrades"], 1, "no reconnect for a socket that never stayed up")

    def test_a_socket_lost_with_no_close_frame_is_opened_again_while_the_server_says_try_again(self):
        err, reason, seen = self.lose(["again", "again"], audio_after=1600)
        self.assertIsNone(err, err)
        self.assertEqual(seen["upgrades"], 4, "two tries told to come back, then let in")
        self.assertEqual(seen["bytes"][3], 1600, "the audio sent meanwhile reached the next socket")
        self.assertEqual(reason, "eof")

    def test_a_socket_lost_with_no_close_frame_ends_as_engine_lost_when_it_cannot_go_on(self):
        for answers, upgrades, why in (
            (["gone"], 2, "refused for good: the session ended on the server"),
            (["silent"], 2, "no answer from the server"),
            (["again"] * 4, 5, "told to come back past the last try"),
        ):
            with self.subTest(why):
                err, _, seen = self.lose(answers)
                self.assertIsNotNone(err, why)
                self.assertEqual(err.code, "engine_lost", why)
                self.assertEqual(seen["upgrades"], upgrades, why)


if __name__ == "__main__":
    unittest.main()
