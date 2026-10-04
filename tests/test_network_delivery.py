import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import discord

from cogs.network_broadcasts import (
    CanonicalLFGControls,
    DeliveryOutcome,
    NetworkBroadcastsCog,
    _discord_http_url,
)
from utils.lfg_api import LFGAPIError


class FakeDeliveryClient:
    configured = True

    def __init__(self, error=None):
        self.error = error
        self.calls = []

    async def acknowledge_delivery(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.error:
            raise self.error


class NetworkDeliveryTests(unittest.IsolatedAsyncioTestCase):
    def make_cog(self, client):
        cog = NetworkBroadcastsCog.__new__(NetworkBroadcastsCog)
        cog.bot = SimpleNamespace(
            get_cog=lambda name: SimpleNamespace(client=client)
            if name == "LFGAPICog" else None,
            get_guild=lambda guild_id: None,
        )
        return cog

    async def test_bot_not_installed_is_destination_missing(self):
        cog = self.make_cog(FakeDeliveryClient())

        outcome = await cog._post_embed(1, 99, 100, {})

        self.assertEqual(outcome.status, "destination_missing")
        self.assertEqual(outcome.error_code, "bot_not_installed")

    def test_discord_http_url_accepts_absolute_web_urls(self):
        self.assertEqual(
            _discord_http_url(" https://cdn.example.com/game%20cover.png "),
            "https://cdn.example.com/game%20cover.png",
        )
        self.assertEqual(
            _discord_http_url("http://example.com/cover.png"),
            "http://example.com/cover.png",
        )
        self.assertEqual(
            _discord_http_url("//images.igdb.com/igdb/cover.jpg"),
            "https://images.igdb.com/igdb/cover.jpg",
        )

    def test_discord_http_url_rejects_malformed_and_relative_values(self):
        for value in (
            "/media/game-covers/cover.png",
            "not a url",
            "javascript:alert(1)",
            "https://example.com/bad path.png",
            "https://example.com:invalid/cover.png",
            {"url": "https://example.com/cover.png"},
        ):
            with self.subTest(value=value):
                self.assertIsNone(_discord_http_url(value))

    def test_build_embed_omits_bad_optional_urls(self):
        cog = self.make_cog(FakeDeliveryClient())

        embed = cog._build_embed({
            "title": "Test LFG",
            "url": "/ql/lfg/27",
            "thumbnail": "/media/game-covers/cover.png",
        })

        payload = embed.to_dict()
        self.assertNotIn("url", payload)
        self.assertNotIn("thumbnail", payload)

    def test_build_embed_keeps_valid_thumbnail(self):
        cog = self.make_cog(FakeDeliveryClient())

        embed = cog._build_embed({
            "thumbnail": "https://cdn.example.com/cover.png",
        })

        self.assertEqual(
            embed.to_dict()["thumbnail"]["url"],
            "https://cdn.example.com/cover.png",
        )

    async def test_missing_bound_channel_is_destination_missing(self):
        guild = SimpleNamespace(
            name="Test Guild",
            get_channel=lambda channel_id: None,
            fetch_channel=AsyncMock(side_effect=self.discord_error(
                discord.NotFound, 404
            )),
        )
        cog = self.make_cog(FakeDeliveryClient())
        cog.bot.get_guild = lambda guild_id: guild
        cog._find_canonical_binding = Mock(return_value=(123, 456, 100))

        outcome = await cog._post_embed(
            1, 99, 100, {"action": "edit", "track_group_id": 7}
        )

        self.assertEqual(outcome.status, "destination_missing")
        self.assertEqual(outcome.error_code, "bound_message_not_found")

    async def test_discord_forbidden_is_permission_missing(self):
        target = SimpleNamespace(fetch_message=AsyncMock(
            side_effect=self.discord_error(discord.Forbidden, 403)
        ))
        guild = SimpleNamespace(
            name="Test Guild",
            get_channel=lambda channel_id: target,
        )
        cog = self.make_cog(FakeDeliveryClient())
        cog.bot.get_guild = lambda guild_id: guild
        cog._find_canonical_binding = Mock(return_value=(123, 456, 100))

        outcome = await cog._post_embed(
            1, 99, 100, {"action": "edit", "track_group_id": 7}
        )

        self.assertEqual(outcome.status, "permission_missing")
        self.assertEqual(outcome.error_code, "discord_permission_missing")

    async def test_discord_rate_limit_is_retrying(self):
        target = SimpleNamespace(fetch_message=AsyncMock(
            side_effect=self.discord_error(discord.HTTPException, 429)
        ))
        guild = SimpleNamespace(
            name="Test Guild",
            get_channel=lambda channel_id: target,
        )
        cog = self.make_cog(FakeDeliveryClient())
        cog.bot.get_guild = lambda guild_id: guild
        cog._find_canonical_binding = Mock(return_value=(123, 456, 100))

        outcome = await cog._post_embed(
            1, 99, 100, {"action": "edit", "track_group_id": 7}
        )

        self.assertEqual(outcome.status, "retrying")
        self.assertEqual(outcome.error_code, "discord_http_429")

    async def test_delete_removes_bound_thread(self):
        thread = SimpleNamespace(delete=AsyncMock())
        message = SimpleNamespace(delete=AsyncMock())
        parent = SimpleNamespace(fetch_message=AsyncMock(return_value=message))
        guild = SimpleNamespace(
            name="Test Guild",
            get_thread=lambda thread_id: thread,
            get_channel=lambda channel_id: parent if channel_id == 100 else None,
        )
        cog = self.make_cog(FakeDeliveryClient())
        cog.bot.get_guild = lambda guild_id: guild
        cog._find_canonical_binding = Mock(return_value=(123, 456, 100))

        with patch(
            "cogs.network_broadcasts.LFG_LEGACY_WRITES_ENABLED", False
        ):
            outcome = await cog._post_embed(
                1, 99, 100, {"action": "delete", "track_group_id": 7}
            )

        self.assertEqual(outcome.status, "delivered")
        thread.delete.assert_awaited_once_with()
        message.delete.assert_awaited_once()

    async def test_failed_receipt_is_terminal_and_does_not_resend(self):
        client = FakeDeliveryClient()
        cog = self.make_cog(client)
        cog._load_delivery_receipt = Mock(return_value={
            "status": "failed",
            "payload_hash": cog._payload_hash("{}"),
            "message_id": None,
            "thread_id": None,
            "error_code": "unexpected_error",
            "error_message": "old adapter failure",
        })
        cog._claim_delivery = Mock(return_value=True)
        cog._save_delivery_receipt = Mock()
        cog._post_embed = AsyncMock(return_value=DeliveryOutcome("delivered"))

        acknowledged = await cog._process_canonical_delivery(
            1, "job-retry", 99, 100, {}, None, "{}"
        )

        self.assertTrue(acknowledged)
        cog._post_embed.assert_not_awaited()
        self.assertEqual(client.calls[0][0][:2], ("job-retry", "failed"))
        self.assertEqual(
            client.calls[0][1]["idempotency_key"],
            "discord:delivery:job-retry:failed",
        )

    async def test_callback_conflict_drains_already_finalized_job(self):
        client = FakeDeliveryClient(LFGAPIError(
            "idempotency_conflict",
            "This delivery job already has a terminal result",
            status=409,
        ))
        cog = self.make_cog(client)
        cog._load_delivery_receipt = Mock(return_value={
            "status": "delivered",
            "payload_hash": cog._payload_hash("{}"),
            "message_id": 123,
            "thread_id": 456,
            "error_code": None,
            "error_message": None,
        })
        cog._post_embed = AsyncMock()

        acknowledged = await cog._process_canonical_delivery(
            1, "job-finalized", 99, 100, {}, None, "{}"
        )

        self.assertTrue(acknowledged)
        cog._post_embed.assert_not_awaited()
        self.assertEqual(len(client.calls), 1)

    async def test_retrying_callback_conflict_keeps_job_queued(self):
        client = FakeDeliveryClient(LFGAPIError(
            "idempotency_conflict",
            "Retry details changed",
            status=409,
        ))
        cog = self.make_cog(client)
        cog._load_delivery_receipt = Mock(return_value=None)
        cog._claim_delivery = Mock(return_value=True)
        cog._save_delivery_receipt = Mock()
        cog._post_embed = AsyncMock(return_value=DeliveryOutcome(
            "retrying",
            error_code="discord_http_429",
        ))

        acknowledged = await cog._process_canonical_delivery(
            1, "job-retrying", 99, 100, {}, None, "{}"
        )

        self.assertFalse(acknowledged)
        cog._post_embed.assert_awaited_once()

    async def test_new_lfg_posts_visible_card_then_attaches_thread(self):
        thread = SimpleNamespace(id=456, name="Visible LFG thread")
        sent = SimpleNamespace(
            id=123,
            create_thread=AsyncMock(return_value=thread),
            pin=AsyncMock(),
        )
        channel = Mock(spec=discord.TextChannel)
        channel.id = 100
        channel.name = "lfg"
        channel.send = AsyncMock(return_value=sent)
        guild = SimpleNamespace(
            name="Test Guild",
            get_channel=lambda channel_id: channel if channel_id == 100 else None,
        )
        cog = self.make_cog(FakeDeliveryClient())
        cog.bot.get_guild = lambda guild_id: guild
        cog._build_embed = Mock(return_value=discord.Embed(title="Test LFG"))
        cog._record_delivery_progress = Mock()

        outcome = await cog._post_embed(
            1,
            99,
            100,
            {"title": "Test LFG", "thread_name": "Test LFG thread"},
        )

        self.assertEqual(outcome.status, "delivered")
        channel.send.assert_awaited_once()
        sent.create_thread.assert_awaited_once_with(
            name="Test LFG thread",
            auto_archive_duration=10080,
        )
        sent.pin.assert_awaited_once()
        self.assertEqual(outcome.message_id, 123)
        self.assertEqual(outcome.thread_id, 456)

    async def test_canonical_card_has_all_management_controls(self):
        view = CanonicalLFGControls(
            SimpleNamespace(),
            42,
            role_schema=[{"slot": "tank", "label": "Tank"}],
            use_roles=True,
        )

        self.assertEqual(
            [item.label for item in view.children],
            ["Join", "Leave", "Update My Setup", "Delete"],
        )
        self.assertEqual(view.timeout, None)
        self.assertEqual(
            {item.custom_id for item in view.children},
            {
                "questlog_lfg_join_42",
                "questlog_lfg_leave_42",
                "questlog_lfg_setup_42",
                "questlog_lfg_delete_42",
            },
        )

    async def test_delete_without_binding_is_idempotent_success(self):
        guild = SimpleNamespace(name="Test Guild")
        cog = self.make_cog(FakeDeliveryClient())
        cog.bot.get_guild = lambda guild_id: guild
        cog._find_canonical_binding = Mock(return_value=None)

        with patch(
            "cogs.network_broadcasts.LFG_LEGACY_WRITES_ENABLED", False
        ):
            outcome = await cog._post_embed(
                1, 99, 100, {"action": "delete", "track_group_id": 7}
            )

        self.assertEqual(outcome.status, "delivered")

    async def test_terminal_receipt_retries_callback_without_resending(self):
        client = FakeDeliveryClient()
        cog = self.make_cog(client)
        cog._load_delivery_receipt = Mock(return_value={
            "status": "delivered",
            "payload_hash": cog._payload_hash("{}"),
            "message_id": 123,
            "thread_id": 456,
            "error_code": None,
            "error_message": None,
        })
        cog._post_embed = AsyncMock()

        acknowledged = await cog._process_canonical_delivery(
            1, "job-1", 99, 100, {}, None, "{}"
        )

        self.assertTrue(acknowledged)
        cog._post_embed.assert_not_awaited()
        self.assertEqual(client.calls[0][0][:2], ("job-1", "delivered"))

    async def test_terminal_receipt_wins_over_a_refreshed_retry_payload(self):
        client = FakeDeliveryClient()
        cog = self.make_cog(client)
        cog._load_delivery_receipt = Mock(return_value={
            "status": "delivered",
            "payload_hash": cog._payload_hash('{"attempt":1}'),
            "message_id": 123,
            "thread_id": 456,
            "error_code": None,
            "error_message": None,
        })
        cog._post_embed = AsyncMock()

        acknowledged = await cog._process_canonical_delivery(
            1, "job-refreshed", 99, 100, {}, None, '{"attempt":2}'
        )

        self.assertTrue(acknowledged)
        cog._post_embed.assert_not_awaited()
        self.assertEqual(
            client.calls[0][0][:2], ("job-refreshed", "delivered")
        )

    async def test_new_result_is_saved_before_callback(self):
        client = FakeDeliveryClient()
        cog = self.make_cog(client)
        cog._load_delivery_receipt = Mock(return_value=None)
        cog._claim_delivery = Mock(return_value=True)
        cog._save_delivery_receipt = Mock()
        cog._post_embed = AsyncMock(return_value=DeliveryOutcome(
            "permission_missing",
            error_code="discord_permission_missing",
        ))

        acknowledged = await cog._process_canonical_delivery(
            1, "job-2", 99, 100, {}, None, "{}"
        )

        self.assertTrue(acknowledged)
        cog._save_delivery_receipt.assert_called_once()
        self.assertEqual(client.calls[0][0][:2], ("job-2", "permission_missing"))

    async def test_stale_partial_claim_resumes_existing_thread(self):
        client = FakeDeliveryClient()
        cog = self.make_cog(client)
        cog._load_delivery_receipt = Mock(return_value={
            "status": "processing",
            "payload_hash": cog._payload_hash("{}"),
            "message_id": None,
            "thread_id": 456,
            "error_code": None,
            "error_message": None,
        })
        cog._claim_delivery = Mock(return_value=True)
        cog._save_delivery_receipt = Mock()
        cog._post_embed = AsyncMock(return_value=DeliveryOutcome(
            "delivered", message_id=123, thread_id=456
        ))

        acknowledged = await cog._process_canonical_delivery(
            1, "job-restart", 99, 100, {}, None, "{}"
        )

        self.assertTrue(acknowledged)
        self.assertEqual(
            cog._post_embed.await_args.kwargs["resume_thread_id"], 456
        )
        self.assertEqual(
            cog._post_embed.await_args.kwargs["delivery_job_id"], "job-restart"
        )

    async def test_failed_callback_keeps_queue_row(self):
        client = FakeDeliveryClient(LFGAPIError(
            "transport_error", "offline", retryable=True
        ))
        cog = self.make_cog(client)
        cog._load_delivery_receipt = Mock(return_value={
            "status": "delivered",
            "payload_hash": cog._payload_hash("{}"),
            "message_id": 123,
            "thread_id": None,
            "error_code": None,
            "error_message": None,
        })
        cog._post_embed = AsyncMock()

        acknowledged = await cog._process_canonical_delivery(
            1, "job-3", 99, 100, {}, None, "{}"
        )

        self.assertFalse(acknowledged)
        cog._post_embed.assert_not_awaited()

    @staticmethod
    def discord_error(error_type, status):
        response = SimpleNamespace(
            status=status,
            reason="Discord test response",
            headers={},
        )
        return error_type(response, "test failure")


if __name__ == "__main__":
    unittest.main()
