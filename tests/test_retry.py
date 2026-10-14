"""The transport's retries and the socket upgrade's refusals, against
httpx.MockTransport (no network):

    python -m unittest discover -s sdks/python/tests
"""

import asyncio
import json
import unittest

import httpx

from vocalstack import VocalStack, VocalStackError
from vocalstack._http import retries
from vocalstack.realtime import upgrade_refused


def envelope(code, status, retryable):
    return {"defined": True, "code": code, "status": status, "message": code, "data": {"retryable": retryable}}


UNAVAILABLE = (503, envelope("unavailable", 503, True), {"retry-after": "0"})


def client(answers, **kw):
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        status, body, headers = answers[min(len(calls) - 1, len(answers) - 1)]
        return httpx.Response(status, json=body, headers=headers)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return VocalStack("http://api.test", api_key="vs_key_x", http=http, **kw), calls


def run(coro):
    return asyncio.run(coro)


class Retries(unittest.TestCase):
    def test_a_get_answered_503_unavailable_is_retried(self):
        vs, calls = client([UNAVAILABLE, (200, {"status": "ok"}, {})])
        health = run(vs.meta.health())
        self.assertEqual(health.status, "ok")
        self.assertEqual(len(calls), 2)

    def test_the_retries_are_bounded_then_the_error_surfaces(self):
        vs, calls = client([UNAVAILABLE], max_retries=2)
        with self.assertRaises(VocalStackError) as cm:
            run(vs.meta.health())
        self.assertEqual(cm.exception.code, "unavailable")
        self.assertEqual(cm.exception.status, 503)
        self.assertTrue(cm.exception.retryable)
        self.assertEqual(len(calls), 3)

    def test_a_post_without_an_idempotency_key_is_not_retried(self):
        vs, calls = client([UNAVAILABLE, (201, {"id": "b1"}, {})])
        with self.assertRaises(VocalStackError) as cm:
            run(vs.broadcasts.create(name="x"))
        self.assertEqual(cm.exception.code, "unavailable")
        self.assertEqual(len(calls), 1)

    def test_a_post_with_an_idempotency_key_is_retried_with_the_same_key(self):
        broadcast = {"id": "b1", "code": "x", "name": "x"}
        vs, calls = client([UNAVAILABLE, (201, broadcast, {})])
        try:
            run(vs.broadcasts.create(name="x", idempotency_key="k1"))
        except Exception:
            pass  # the canned answer may not be a whole Broadcast; the requests are what is checked
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1].headers["idempotency-key"], "k1")

    def test_which_answers_are_retried(self):
        self.assertTrue(retries(503, "unavailable", "DELETE"))
        self.assertTrue(retries(503, "unavailable", "HEAD"))
        self.assertTrue(retries(503, "unavailable", "POST", "k1"))
        self.assertFalse(retries(503, "unavailable", "PATCH"))
        self.assertFalse(retries(503, "capacity_exceeded", "GET"))
        self.assertFalse(retries(500, "internal", "GET"))
        self.assertTrue(retries(429, "rate_limited", "POST"))


class UpgradeRefused(unittest.TestCase):
    def test_503_with_an_unavailable_body_is_unavailable(self):
        e = upgrade_refused(503, json.dumps(envelope("unavailable", 503, True)).encode())
        self.assertEqual(e.code, "unavailable")
        self.assertTrue(e.retryable)

    def test_a_bare_503_stays_capacity_exceeded(self):
        self.assertEqual(upgrade_refused(503, b"").code, "capacity_exceeded")
        self.assertEqual(upgrade_refused(503, None).code, "capacity_exceeded")
        self.assertEqual(upgrade_refused(503, b"<html>busy</html>").code, "capacity_exceeded")
        self.assertEqual(upgrade_refused(503, json.dumps(envelope("internal", 503, False)).encode()).code, "capacity_exceeded")

    def test_a_bare_502_or_504_is_capacity_exceeded(self):
        for status in (502, 504):
            e = upgrade_refused(status, b"")
            self.assertEqual(e.code, "capacity_exceeded")
            self.assertTrue(e.retryable)
            self.assertEqual(upgrade_refused(status, b"<html>bad gateway</html>").code, "capacity_exceeded")
        self.assertEqual(upgrade_refused(502, json.dumps(envelope("internal", 502, False)).encode()).code, "internal")

    def test_other_statuses_are_their_own_envelope(self):
        e = upgrade_refused(410, json.dumps(envelope("session_expired", 410, False)).encode())
        self.assertEqual(e.code, "session_expired")


if __name__ == "__main__":
    unittest.main()
