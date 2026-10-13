import importlib
import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


class _MatchInfo(dict):
    def __init__(self, route_name):
        super().__init__()
        self.route = SimpleNamespace(name=route_name)


class _Request:
    def __init__(self, route_name, path):
        self.path = path
        self.remote = "127.0.0.1"
        self.headers = {"Authorization": "Bearer " + "x" * 32}
        self.match_info = _MatchInfo(route_name)
        self.state = {}

    def __setitem__(self, key, value):
        self.state[key] = value


class APIAuthorizationPolicyTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.env = patch.dict(os.environ, {
            "WARDEN_API_AUTH_MODE": "dual",
            "DISCORD_BOT_API_TOKEN": "x" * 32,
            "DISCOVERY_APPROVERS": "123",
            "BOT_OWNER_ID": "456",
        })
        cls.env.start()
        sys.modules.pop("api_server", None)
        cls.api = importlib.import_module("api_server")

    @classmethod
    def tearDownClass(cls):
        sys.modules.pop("api_server", None)
        cls.env.stop()

    async def test_legacy_bearer_cannot_reach_actor_required_route(self):
        handler = AsyncMock(return_value="handler result")
        response = await self.api.auth_middleware(
            _Request("mod-ban", "/mod/ban"),
            handler,
        )

        self.assertEqual(response.status, 403)
        handler.assert_not_awaited()

    async def test_legacy_bearer_keeps_low_risk_guild_discovery(self):
        handler = AsyncMock(return_value="handler result")
        result = await self.api.auth_middleware(
            _Request("guilds-list", "/api/guilds"),
            handler,
        )

        self.assertEqual(result, "handler result")
        handler.assert_awaited_once()

    async def test_legacy_bearer_cannot_trigger_sync(self):
        handler = AsyncMock(return_value="handler result")
        response = await self.api.auth_middleware(
            _Request("guild-sync-all", "/api/sync"),
            handler,
        )

        self.assertEqual(response.status, 403)
        handler.assert_not_awaited()

    def test_network_creator_actor_must_be_current_approver(self):
        request = SimpleNamespace(match_info={})
        allowed, _ = self.api._signed_actor_is_authorized(
            request, "creator.network.write", "123", b"{}"
        )
        rejected, reason = self.api._signed_actor_is_authorized(
            request, "creator.network.write", "999", b"{}"
        )

        self.assertTrue(allowed)
        self.assertFalse(rejected)
        self.assertIn("approver", reason)

    def test_signed_guild_sync_requires_current_administrator(self):
        request = SimpleNamespace(match_info={"guild_id": "789"})
        member = SimpleNamespace(
            id=123,
            guild_permissions=SimpleNamespace(administrator=False),
        )
        guild = SimpleNamespace(get_member=lambda member_id: member)

        with patch.object(
            self.api,
            "bot_instance",
            SimpleNamespace(get_guild=lambda guild_id: guild),
        ):
            allowed, reason = self.api._signed_actor_is_authorized(
                request, "guilds.sync", "123", b"{}"
            )

        self.assertFalse(allowed)
        self.assertIn("administrator", reason)


if __name__ == "__main__":
    unittest.main()
