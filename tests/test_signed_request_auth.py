import base64
import json
import time
import unittest

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from utils.signed_request_auth import (
    AuthenticationError,
    ReplayCache,
    canonical_request,
    load_trusted_signers,
    validate_required_scopes,
    verify_signed_request,
)


def b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


class SignedRequestAuthenticationTests(unittest.TestCase):
    def setUp(self):
        self.private_key = Ed25519PrivateKey.generate()
        public_key = self.private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        config = json.dumps({
            "questlog-web-2026-10": {
                "public_key": b64url(public_key),
                "scopes": ["moderation.write", "guilds.read"],
            }
        })
        self.signers = load_trusted_signers(config)
        self.timestamp = str(int(time.time()))
        self.nonce = b64url(b"0123456789abcdef")
        self.body = b'{"guild_id":123,"requester_id":"456"}'

    def signed_headers(self, **overrides):
        values = {
            "key_id": "questlog-web-2026-10",
            "timestamp": self.timestamp,
            "nonce": self.nonce,
            "scope": "moderation.write",
            "actor_id": "456",
            "method": "POST",
            "path": "/mod/ban",
            "body": self.body,
        }
        values.update(overrides)
        signature = self.private_key.sign(canonical_request(**values))
        return {
            "X-Warden-Auth-Version": "1",
            "X-Warden-Key-Id": values["key_id"],
            "X-Warden-Timestamp": values["timestamp"],
            "X-Warden-Nonce": values["nonce"],
            "X-Warden-Scope": values["scope"],
            "X-Warden-Actor-Id": values["actor_id"],
            "X-Warden-Signature": b64url(signature),
        }

    def verify(self, headers, replay_cache=None, **overrides):
        values = {
            "headers": headers,
            "method": "POST",
            "path": "/mod/ban",
            "body": self.body,
            "expected_scope": "moderation.write",
            "actor_required": True,
            "signers": self.signers,
            "replay_cache": replay_cache or ReplayCache(),
            "now": int(self.timestamp),
        }
        values.update(overrides)
        return verify_signed_request(**values)

    def test_valid_signature_returns_bound_actor_and_scope(self):
        context = self.verify(self.signed_headers())

        self.assertEqual(context.actor_id, "456")
        self.assertEqual(context.scope, "moderation.write")
        self.assertEqual(context.key_id, "questlog-web-2026-10")

    def test_body_method_path_actor_and_scope_are_cryptographically_bound(self):
        headers = self.signed_headers()
        tampered_requests = (
            {"body": b'{"guild_id":123,"requester_id":"999"}'},
            {"method": "GET"},
            {"path": "/mod/kick"},
            {"expected_scope": "guilds.read"},
        )
        for tampering in tampered_requests:
            with self.subTest(tampering=tampering):
                with self.assertRaises(AuthenticationError):
                    self.verify(headers, **tampering)

        actor_headers = dict(headers)
        actor_headers["X-Warden-Actor-Id"] = "999"
        with self.assertRaises(AuthenticationError):
            self.verify(actor_headers)

    def test_replay_is_rejected_after_successful_verification(self):
        cache = ReplayCache()
        headers = self.signed_headers()
        self.verify(headers, replay_cache=cache)

        with self.assertRaisesRegex(AuthenticationError, "replay"):
            self.verify(headers, replay_cache=cache)

    def test_stale_and_far_future_requests_are_rejected(self):
        for now in (int(self.timestamp) - 61, int(self.timestamp) + 61):
            with self.subTest(now=now):
                with self.assertRaisesRegex(AuthenticationError, "timestamp"):
                    self.verify(self.signed_headers(), now=now)

    def test_scope_authorization_is_enforced_even_with_a_valid_signature(self):
        headers = self.signed_headers(scope="creator.write")
        with self.assertRaisesRegex(AuthenticationError, "not authorized"):
            self.verify(headers, expected_scope="creator.write")

    def test_actor_is_required_on_privileged_routes(self):
        headers = self.signed_headers(actor_id="")
        with self.assertRaisesRegex(AuthenticationError, "actor"):
            self.verify(headers)

    def test_configuration_is_strict_and_required_scopes_are_checked(self):
        with self.assertRaises(RuntimeError):
            load_trusted_signers('{"bad key!": {}}')
        with self.assertRaisesRegex(RuntimeError, "creator.write"):
            validate_required_scopes(self.signers, {"moderation.write", "creator.write"})


if __name__ == "__main__":
    unittest.main()
