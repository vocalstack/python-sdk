import asyncio
import json
import unittest

import httpx

from vocalstack import VocalStack

SPEAKER = {
    "id": "s1",
    "name": "Ana",
    "consent": "pending",
    "draft": False,
    "createdAt": "2026-11-07T00:00:00Z",
    "updatedAt": "2026-11-07T00:00:00Z",
    "voice": None,
}


def client(body, status=200):
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(status, json=body)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return VocalStack("http://api.test", api_key="vs_key_test", http=http), calls


def run(coro):
    return asyncio.run(coro)


def meta_of(request: httpx.Request) -> dict:
    body = request.content.decode("utf-8", "replace")
    start = body.index("{")
    return json.loads(body[start : body.index("}", start) + 1])


class CreateSpeaker(unittest.TestCase):
    def test_without_consent_sends_none(self):
        vs, calls = client(SPEAKER, 201)
        speaker = run(vs.speakers.create("Ana", sample=b"RIFF", idempotency_key="k1"))
        self.assertIsNone(speaker.voice)
        self.assertEqual(str(calls[0].url), "http://api.test/v1/speakers")
        self.assertEqual(calls[0].headers["idempotency-key"], "k1")
        self.assertEqual(meta_of(calls[0]), {"name": "Ana"})

    def test_consent_is_sent_when_given(self):
        vs, calls = client(SPEAKER, 201)
        run(vs.speakers.create("Ana", sample=b"RIFF", consent=True))
        self.assertEqual(meta_of(calls[0]), {"name": "Ana", "consent": True})


class CreateFromAppearance(unittest.TestCase):
    def test_sends_the_appearance_without_consent(self):
        vs, calls = client(SPEAKER, 201)
        run(vs.speakers.create_from_appearance(transcription_id="t1", appearance=2, name="Ana"))
        self.assertEqual(str(calls[0].url), "http://api.test/v1/speakers/from-appearance")
        self.assertEqual(json.loads(calls[0].content), {"transcriptionId": "t1", "appearance": 2, "name": "Ana"})


class NameAppearance(unittest.TestCase):
    def test_puts_the_name_on_the_transcription(self):
        vs, calls = client({"ok": True})
        run(vs.transcriptions.name_appearance("t1", 3, "Ana"))
        self.assertEqual(calls[0].method, "PUT")
        self.assertEqual(str(calls[0].url), "http://api.test/v1/transcriptions/t1/appearances/3")
        self.assertEqual(json.loads(calls[0].content), {"name": "Ana"})


if __name__ == "__main__":
    unittest.main()
