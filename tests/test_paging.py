"""The paged lists against httpx.MockTransport: the plain methods read every
page through ``nextCursor``, the ``*_page`` methods ask for one. No network:

    python -m unittest discover -s tests
"""

import asyncio
import unittest

import httpx

from vocalstack import VocalStack


def run(coro):
    return asyncio.run(coro)


def client(pages):
    """A client whose server answers ``pages`` in turn; the requests land in the list returned."""
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json=pages[min(len(calls) - 1, len(pages) - 1)])

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return VocalStack("http://api.test", api_key="vs_key_x", http=http), calls


def tx(id):
    return {
        "id": id, "code": "tx-1", "name": id, "kind": "transcription", "source": {"kind": "mic"}, "lifecycle": "active",
        "ownerPrincipalId": "p", "shareUrl": "", "settings": {}, "recordingCount": 0, "createdAt": "2026-10-12T00:00:00Z",
        "updatedAt": "2026-10-12T00:00:00Z",
    }


class Paging(unittest.TestCase):
    def test_list_follows_next_cursor_to_the_last_page(self):
        vs, calls = client([
            {"transcriptions": [tx("t1"), tx("t2")], "nextCursor": "c1"},
            {"transcriptions": [tx("t3")]},
        ])
        got = run(vs.transcriptions.list("all"))
        self.assertEqual([t.id for t in got], ["t1", "t2", "t3"])
        self.assertEqual([r.url.params.get("cursor") for r in calls], [None, "c1"])
        self.assertTrue(all(r.url.params.get("lifecycle") == "all" for r in calls))

    def test_list_page_asks_for_one_page(self):
        vs, calls = client([{"transcriptions": [tx("t1")], "nextCursor": "c9"}])
        page = run(vs.transcriptions.list_page(cursor="c8", limit=100))
        self.assertEqual(page.next_cursor, "c9")
        self.assertEqual(len(calls), 1)
        self.assertEqual((calls[0].url.params.get("cursor"), calls[0].url.params.get("limit")), ("c8", "100"))

    def test_events_start_after_a_seq_then_follow_the_cursor(self):
        ev = lambda s: {"seq": s, "kind": "transcript.final", "at": "2026-10-12T00:00:00Z", "payload": {}}
        vs, calls = client([{"events": [ev(4), ev(5)], "nextCursor": "5"}, {"events": [ev(6)]}])
        got = run(vs.recordings.events("r1", after=3, limit=2))
        self.assertEqual([e.seq for e in got], [4, 5, 6])
        self.assertEqual(calls[0].url.params.get("after"), "3")
        self.assertIsNone(calls[1].url.params.get("after"), "never cursor and after together")
        self.assertEqual(calls[1].url.params.get("cursor"), "5")


if __name__ == "__main__":
    unittest.main()
