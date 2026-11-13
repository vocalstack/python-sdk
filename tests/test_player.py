import asyncio
import json
import unittest
import unittest.mock

import httpx

from vocalstack import VocalStack


def client(body, **kw):
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json=body)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return VocalStack("http://api.test", http=http, **kw), calls


def run(coro):
    return asyncio.run(coro)


class RequestVoice(unittest.TestCase):
    def test_langs_alone_still_work(self):
        vs, calls = client([])
        run(vs.transcriptions.request_voice("t1", ["es"]))
        self.assertEqual(str(calls[0].url), "http://api.test/v1/transcriptions/t1/voice")
        self.assertEqual(json.loads(calls[0].content), {"langs": ["es"]})

    def test_a_listener_asks_with_a_cast(self):
        vs, calls = client([])
        run(vs.transcriptions.request_voice("t1", ["es"], voice_by_speaker={0: "stock-a"}, near=4, share_code="abc"))
        self.assertEqual(str(calls[0].url), "http://api.test/v1/transcriptions/t1/voice?shareCode=abc")
        self.assertEqual(json.loads(calls[0].content), {"langs": ["es"], "voiceBySpeaker": {"0": "stock-a"}, "near": 4})

    def test_a_cloned_voice_is_asked_with_consent(self):
        vs, calls = client([])
        run(vs.transcriptions.request_voice("t1", ["es"], voice_by_speaker={0: "clone:abc"}, consent=True))
        self.assertEqual(json.loads(calls[0].content), {"langs": ["es"], "consent": True, "voiceBySpeaker": {"0": "clone:abc"}})


class OpenShare(unittest.TestCase):
    def test_reads_a_code_with_no_credential(self):
        vs, calls = client(None, token="session")
        with unittest.mock.patch("vocalstack.resources._one", lambda model, data: data):
            run(vs.share_links.open("abc"))
        self.assertEqual(str(calls[0].url), "http://api.test/v1/shares/abc")
        self.assertNotIn("authorization", calls[0].headers)


class Lookup(unittest.TestCase):
    def test_none_when_the_link_has_no_transcription(self):
        vs, calls = client({"transcription": None}, token="session")
        self.assertIsNone(run(vs.transcriptions.lookup("https://youtu.be/dQw4w9WgXcQ")))
        self.assertEqual(calls[0].url.path, "/v1/transcriptions/lookup")
        self.assertEqual(calls[0].url.params["url"], "https://youtu.be/dQw4w9WgXcQ")


if __name__ == "__main__":
    unittest.main()
