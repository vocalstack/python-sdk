"""``vs.connect.token()`` against httpx.MockTransport (no network):

    python -m unittest discover -s tests
"""

import asyncio
import unittest
from urllib.parse import parse_qs

import httpx

from vocalstack import VocalStack, VocalStackError

ISSUED = {
    "access_token": "issued-key",
    "token_type": "api_key",
    "key_id": "00000000-0000-4000-8000-000000000001",
    "scope": "read write",
    "account": {"name": "Acme"},
}
ARGS = {"code": "c0de", "code_verifier": "v" * 43, "redirect_uri": "https://app.test/cb?x=1"}


def client(answers, **kw):
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        status, body, headers = answers[min(len(calls) - 1, len(answers) - 1)]
        return httpx.Response(status, json=body, headers=headers)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return VocalStack("http://api.test", http=http, **kw), calls


def run(coro):
    return asyncio.run(coro)


class ConnectToken(unittest.TestCase):
    def test_posts_a_form_with_no_credential_even_when_the_client_has_one(self):
        vs, calls = client([(200, ISSUED, {})], api_key="configured-key")
        self.assertEqual(run(vs.connect.token(**ARGS)), ISSUED)
        self.assertEqual(len(calls), 1)
        req = calls[0]
        self.assertEqual(req.method, "POST")
        self.assertEqual(str(req.url), "http://api.test/v1/connect/token")
        self.assertTrue(req.headers["content-type"].startswith("application/x-www-form-urlencoded"))
        self.assertNotIn("authorization", req.headers)
        self.assertNotIn("origin", req.headers)
        self.assertNotIn("sec-fetch-site", req.headers)
        form = {k: v[0] for k, v in parse_qs(req.content.decode()).items()}
        self.assertEqual(
            form,
            {
                "grant_type": "authorization_code",
                "code": "c0de",
                "code_verifier": "v" * 43,
                "redirect_uri": "https://app.test/cb?x=1",
            },
        )

    def test_a_client_with_no_credential_can_redeem_a_code(self):
        vs, calls = client([(200, ISSUED, {})])
        self.assertEqual(run(vs.connect.token(**ARGS))["key_id"], ISSUED["key_id"])
        self.assertNotIn("authorization", calls[0].headers)

    def test_a_400_is_a_vocalstack_error_with_the_oauth_code_not_retried(self):
        refused = {"error": "invalid_grant", "error_description": "the code was already used"}
        vs, calls = client([(400, refused, {}), (200, ISSUED, {})])
        with self.assertRaises(VocalStackError) as cm:
            run(vs.connect.token(**ARGS))
        self.assertEqual(cm.exception.code, "invalid_grant")
        self.assertEqual(cm.exception.message, "the code was already used")
        self.assertEqual(cm.exception.status, 400)
        self.assertFalse(cm.exception.retryable)
        self.assertEqual(len(calls), 1)

    def test_a_429_is_never_retried_either(self):
        limited = {"defined": True, "code": "rate_limited", "status": 429, "message": "slow down", "data": {"retryable": True}}
        vs, calls = client([(429, limited, {"retry-after": "0"}), (200, ISSUED, {})])
        with self.assertRaises(VocalStackError) as cm:
            run(vs.connect.token(**ARGS))
        self.assertEqual(cm.exception.code, "rate_limited")
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
