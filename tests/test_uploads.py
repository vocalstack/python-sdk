"""``vs.uploads.send()`` against httpx.MockTransport (no network)."""

import asyncio
import json
import unittest

import httpx

from vocalstack import VocalStack, VocalStackError


class SendTest(unittest.TestCase):
    def test_parts_go_to_their_links_then_complete(self):
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            url = str(request.url)
            if url == "http://api.test/v1/uploads":
                assert json.loads(request.content) == {"name": "a.mp3", "bytes": 10}
                return httpx.Response(201, json={"id": "up-1", "state": "uploading", "name": "a.mp3", "bytes": 10, "partBytes": 6, "expiresAt": "x",
                                                 "parts": [{"number": 1, "bytes": 6, "url": "https://store.test/p1"}, {"number": 2, "bytes": 4, "url": "https://store.test/p2"}]})
            if url.startswith("https://store.test/"):
                n = 1 if url.endswith("p1") else 2
                assert len(request.content) == (6 if n == 1 else 4)
                assert "authorization" not in request.headers
                return httpx.Response(200, headers={"etag": f'"e{n}"'})
            if url == "http://api.test/v1/uploads/up-1/complete":
                assert json.loads(request.content) == {"parts": [{"number": 1, "etag": '"e1"'}, {"number": 2, "etag": '"e2"'}]}
                return httpx.Response(200, json={"id": "up-1", "state": "complete", "name": "a.mp3", "bytes": 10, "partBytes": 6, "expiresAt": "x"})
            raise AssertionError(url)

        async def run():
            seen = []
            async with VocalStack("http://api.test", api_key="k", http=httpx.AsyncClient(transport=httpx.MockTransport(handler))) as vs:
                up = await vs.uploads.send(bytes(10), file_name="a.mp3", on_progress=lambda s, t: seen.append((s, t)))
            return up, seen

        up, seen = asyncio.run(run())
        self.assertEqual(up.state.value if hasattr(up.state, "value") else up.state, "complete")
        self.assertEqual(seen[-1], (10, 10))

    def test_switched_off_is_the_servers_error(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(409, json={"code": "invalid_state", "status": 409, "message": "Direct uploads are not available yet; send the file with POST /v1/transcriptions."})

        async def run():
            async with VocalStack("http://api.test", api_key="k", http=httpx.AsyncClient(transport=httpx.MockTransport(handler))) as vs:
                await vs.uploads.send(bytes(3), file_name="a.mp3")

        with self.assertRaises(VocalStackError) as cx:
            asyncio.run(run())
        self.assertEqual(cx.exception.code, "invalid_state")


if __name__ == "__main__":
    unittest.main()
