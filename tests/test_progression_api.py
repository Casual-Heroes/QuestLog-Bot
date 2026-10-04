import sys
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from models import ProgressionOutboxEvent
from utils.progression_api import (
    ProgressionAPIClient,
    ProgressionAPIError,
    ProgressionAPIResult,
)
from utils.progression_outbox import ProgressionOutbox


class ProgressionAPIClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_rejects_client_id_in_token_field(self):
        client = ProgressionAPIClient(
            "https://questlog.example/api/v1/progression/events",
            "qlc_not_a_token",
        )
        with self.assertRaises(ProgressionAPIError) as context:
            await client.record_discord_event(
                user_id=123456789012345678,
                guild_id=234567890123456789,
                event_type="discord_message",
                evidence_id="message:1",
                occurred_at=1_786_737_600,
            )
        self.assertEqual(context.exception.code, "invalid_token_format")

    async def test_sends_evidence_without_an_xp_amount(self):
        client = ProgressionAPIClient(
            "https://questlog.example/api/v1/progression/events",
            "qlp_valid_service_token",
            max_attempts=1,
        )
        response = MagicMock()
        response.status = 200
        response.headers = {}
        response.json = AsyncMock(return_value={
            "data": {
                "status": "awarded",
                "awarded_xp": 2,
                "identity_linked": True,
                "current_xp": 100,
                "previous_level": 3,
                "current_level": 4,
                "hero_points": 45,
                "level_changed": True,
            }
        })
        context = AsyncMock()
        context.__aenter__.return_value = response
        session = MagicMock()
        session.post.return_value = context
        client._get_session = AsyncMock(return_value=session)

        result = await client.record_discord_event(
            user_id=123456789012345678,
            guild_id=234567890123456789,
            event_type="discord_message",
            evidence_id="message:345678901234567890",
            occurred_at=1_786_737_600,
        )

        self.assertEqual(result.awarded_xp, 2)
        self.assertEqual(result.awarded_legacy, 0)
        self.assertTrue(result.identity_linked)
        self.assertEqual(result.current_xp, 100)
        self.assertEqual(result.previous_level, 3)
        self.assertEqual(result.current_level, 4)
        self.assertEqual(result.hero_points, 45)
        self.assertTrue(result.level_changed)
        payload = session.post.call_args.kwargs["json"]
        self.assertNotIn("xp", payload)
        self.assertNotIn("hero_points", payload)
        self.assertEqual(payload["evidence_id"], "message:345678901234567890")


class ProgressionOutboxTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        ProgressionOutboxEvent.__table__.create(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

        @contextmanager
        def session_scope():
            session = self.Session()
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        self.session_scope = session_scope
        self.client = MagicMock()
        self.client.record_discord_event = AsyncMock()
        self.logger = MagicMock()
        self.outbox = ProgressionOutbox(
            self.client, self.session_scope, self.logger
        )
        self.event = {
            "user_id": 123456789012345678,
            "guild_id": 234567890123456789,
            "event_type": "discord_message",
            "evidence_id": "message:345678901234567890",
            "occurred_at": 1_786_737_600,
        }

    def tearDown(self):
        self.engine.dispose()

    async def test_retryable_failure_is_durable_then_retried(self):
        expected = ProgressionAPIResult(
            status="awarded",
            awarded_xp=2,
            identity_linked=True,
            current_xp=102,
            current_level=4,
        )
        self.client.record_discord_event.side_effect = [
            ProgressionAPIError(
                "transport_error", "offline", retryable=True
            ),
            expected,
        ]

        first = await self.outbox.submit(**self.event)
        self.assertIsNone(first)
        with self.session_scope() as session:
            row = session.query(ProgressionOutboxEvent).one()
            self.assertEqual(row.status, "retry")
            self.assertEqual(row.attempt_count, 1)
            self.assertEqual(row.last_error_code, "transport_error")
            row.next_attempt_at = 0

        keys = self.outbox.due_event_keys()
        self.assertEqual(len(keys), 1)
        result = await self.outbox.deliver(keys[0])

        self.assertEqual(result, expected)
        self.assertEqual(self.client.record_discord_event.await_count, 2)
        with self.session_scope() as session:
            row = session.query(ProgressionOutboxEvent).one()
            self.assertEqual(row.status, "delivered")
            self.assertEqual(row.attempt_count, 2)
            self.assertIsNotNone(row.delivered_at)

    async def test_duplicate_submission_reuses_delivered_result(self):
        expected = ProgressionAPIResult(
            status="identity_not_linked",
            awarded_xp=0,
            identity_linked=False,
        )
        self.client.record_discord_event.return_value = expected

        first = await self.outbox.submit(**self.event)
        replay = dict(self.event)
        replay["occurred_at"] += 10
        second = await self.outbox.submit(**replay)

        self.assertEqual(first, expected)
        self.assertEqual(second, expected)
        self.assertEqual(self.client.record_discord_event.await_count, 1)
        with self.session_scope() as session:
            self.assertEqual(session.query(ProgressionOutboxEvent).count(), 1)


class ProgressionRoutingContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = (
            Path(__file__).resolve().parents[1] / "cogs" / "xp.py"
        ).read_text(encoding="utf-8")

    def test_canonical_mode_prevents_legacy_direct_dual_write(self):
        self.assertIn(
            'if QUESTLOG_PROGRESSION_API_ENABLED:',
            self.source,
        )
        self.assertIn('raise Exception("canonical_progression_enabled")', self.source)

    def test_discord_level_and_roles_follow_authoritative_questlog_result(self):
        self.assertIn("def _apply_authoritative_progression", self.source)
        self.assertIn("result.current_level", self.source)
        self.assertIn("result.level_changed", self.source)
        self.assertIn("sync_canonical_progression_mirrors", self.source)
        self.assertIn("QuestLog owns XP and levels", self.source)

    def test_unlinked_members_remain_on_local_xp_until_merge(self):
        self.assertIn("def member_uses_canonical_progression", self.source)
        self.assertIn("SELECT 1 FROM web_users", self.source)
        # add_xp plus all four supported activity listeners route per member.
        self.assertGreaterEqual(
            self.source.count("member_uses_canonical_progression("), 6
        )
        self.assertIn("unlinked_rows", self.source)
        self.assertIn("Warden XP until account link", self.source)

    def test_supported_events_use_provider_evidence(self):
        for action in (
            "discord_message",
            "discord_media",
            "discord_reaction",
            "discord_voice",
            "discord_gaming",
        ):
            self.assertIn(action, self.source)
        self.assertIn('f"message:{message.id}"', self.source)
        self.assertIn('f"reaction:{payload.message_id}', self.source)

    def test_legacy_cog_uses_the_same_canonical_adapter(self):
        legacy = (
            Path(__file__).resolve().parents[1] / "cogs" / "legacy.py"
        ).read_text(encoding="utf-8")
        self.assertIn("QUESTLOG_PROGRESSION_API_ENABLED", legacy)
        self.assertIn("submit_event", legacy)
        self.assertIn("result.awarded_legacy", legacy)


if __name__ == "__main__":
    unittest.main()
