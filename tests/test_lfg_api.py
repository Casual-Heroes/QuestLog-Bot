import unittest

from utils.lfg_api import LFGAPIClient, LFGAPIError


class FakeResponse:
    def __init__(self, status, body, headers=None):
        self.status = status
        self.body = body
        self.headers = headers or {}

    async def json(self, content_type=None):
        return self.body


class FakeRequestContext:
    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class FakeSession:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []
        self.closed = False

    def request(self, method, url, **kwargs):
        self.requests.append((method, url, kwargs))
        return FakeRequestContext(self.responses.pop(0))

    async def close(self):
        self.closed = True


class LFGAPIClientTests(unittest.IsolatedAsyncioTestCase):
    def make_client(self, *responses):
        client = LFGAPIClient(
            "https://questlog.example/api/v1/lfg",
            "secret-token",
            max_attempts=1,
        )
        client._session = FakeSession(*responses)
        return client

    async def test_create_sends_actor_and_idempotency_headers(self):
        client = self.make_client(FakeResponse(
            201,
            {"id": 42, "share_token": "public-token"},
            {"Idempotent-Replay": "true"},
        ))

        result = await client.create_group(
            {"title": "Raid night"},
            actor_user_id=17,
            idempotency_key="discord:123:create",
        )

        method, url, request = client._session.requests[0]
        self.assertEqual((method, url), ("POST", "https://questlog.example/api/v1/lfg/"))
        self.assertEqual(request["headers"]["Authorization"], "Bearer secret-token")
        self.assertEqual(request["headers"]["X-QuestLog-Actor-User"], "17")
        self.assertEqual(request["headers"]["Idempotency-Key"], "discord:123:create")
        self.assertEqual(result.data["id"], 42)
        self.assertTrue(result.idempotent_replay)

    async def test_delete_group_sends_actor_and_idempotency_headers(self):
        client = self.make_client(FakeResponse(
            200,
            {"data": {"id": 42, "status": "cancelled"}},
        ))

        await client.delete_group(
            42,
            actor_user_id=17,
            idempotency_key="discord:125:lfg:delete",
        )

        method, url, request = client._session.requests[0]
        self.assertEqual(
            (method, url),
            ("DELETE", "https://questlog.example/api/v1/lfg/42/"),
        )
        self.assertEqual(request["headers"]["X-QuestLog-Actor-User"], "17")
        self.assertEqual(
            request["headers"]["Idempotency-Key"],
            "discord:125:lfg:delete",
        )

    async def test_normalizes_api_error_envelope(self):
        client = self.make_client(FakeResponse(
            409,
            {"error": {"code": "lfg_full", "message": "Group is full", "details": {}}},
        ))

        with self.assertRaises(LFGAPIError) as raised:
            await client.join_group(
                42,
                {},
                actor_user_id=17,
                idempotency_key="discord:124:join",
            )

        self.assertEqual(raised.exception.code, "lfg_full")
        self.assertEqual(raised.exception.status, 409)
        self.assertFalse(raised.exception.retryable)

    async def test_write_requires_idempotency_key(self):
        client = self.make_client()

        with self.assertRaises(LFGAPIError) as raised:
            await client._request(
                "POST",
                "https://questlog.example/api/v1/lfg/",
                payload={},
            )

        self.assertEqual(raised.exception.code, "idempotency_key_required")

    async def test_client_identifier_is_not_treated_as_a_bearer_token(self):
        client = LFGAPIClient(
            "https://questlog.example/api/v1/lfg",
            "qlc_public_client_identifier",
            max_attempts=1,
        )
        self.assertFalse(client.configured)
        with self.assertRaises(LFGAPIError) as raised:
            await client._request(
                "POST",
                "https://questlog.example/api/v1/lfg/",
                payload={},
                idempotency_key="test:invalid-client-id",
            )
        self.assertEqual(raised.exception.code, "invalid_token_format")

    async def test_client_label_is_rejected_before_http(self):
        client = LFGAPIClient(
            "https://questlog.example/api/v1/lfg",
            "client: qlp_secret",
            max_attempts=1,
        )
        self.assertFalse(client.configured)
        self.assertIn("without a 'client:' label", client.configuration_error)

    async def test_rejects_delivery_callback_on_another_host(self):
        client = self.make_client()

        with self.assertRaises(LFGAPIError) as raised:
            await client.acknowledge_delivery(
                "job-1",
                "delivered",
                idempotency_key="discord:delivery:job-1:delivered",
                callback_url="https://attacker.example/callback",
            )

        self.assertEqual(raised.exception.code, "invalid_callback_url")

    async def test_accepts_delivery_callback_on_configured_questlog_host(self):
        client = self.make_client(FakeResponse(200, {"status": "delivered"}))
        callback_url = (
            "https://questlog.example/api/v1/lfg/deliveries/job-1/"
        )

        await client.acknowledge_delivery(
            "job-1",
            "delivered",
            idempotency_key="discord:delivery:job-1:delivered",
            callback_url=callback_url,
        )

        method, url, _request = client._session.requests[0]
        self.assertEqual((method, url), ("POST", callback_url))


if __name__ == "__main__":
    unittest.main()
