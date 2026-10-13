import unittest
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import discord

from cogs.audit import AuditCog
from models import AuditAction, AuditLog


class RawMessageDeleteTests(unittest.IsolatedAsyncioTestCase):
    async def test_uncached_delete_is_recorded_with_available_metadata(self):
        cog = AuditCog.__new__(AuditCog)
        cog.log_event = AsyncMock()
        payload = SimpleNamespace(
            guild_id=248806476150210571,
            channel_id=1281720517921079356,
            message_id=123456789,
            cached_message=None,
        )

        await AuditCog.on_raw_message_delete(cog, payload)

        cog.log_event.assert_awaited_once_with(
            guild_id=248806476150210571,
            action=AuditAction.MESSAGE_DELETE,
            actor_name="Unknown (self, moderator, or bot)",
            target_id=123456789,
            target_name="Deleted message (author unavailable)",
            target_type="message",
            details=(
                "Channel: <#1281720517921079356>\n"
                "Message ID: 123456789\n"
                "Content: unavailable (message predates this bot session or left the bounded in-memory cache)"
            ),
            category="message",
        )

    async def test_cached_raw_delete_defers_to_richer_message_listener(self):
        cog = AuditCog.__new__(AuditCog)
        cog.log_event = AsyncMock()
        payload = SimpleNamespace(
            guild_id=248806476150210571,
            channel_id=1281720517921079356,
            message_id=123456789,
            cached_message=object(),
        )

        await AuditCog.on_raw_message_delete(cog, payload)

        cog.log_event.assert_not_awaited()


class MessageDeleteActorTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _message(entries):
        async def audit_logs(**_kwargs):
            for entry in entries:
                yield entry

        author = SimpleNamespace(id=141890558045061120)
        author.__str__ = lambda _self: "ryven (Ryven)"
        guild = SimpleNamespace(id=248806476150210571, audit_logs=audit_logs)
        channel = SimpleNamespace(id=1281720517921079356)
        return SimpleNamespace(author=author, guild=guild, channel=channel)

    async def test_successful_empty_audit_log_does_not_guess_self_delete(self):
        cog = AuditCog.__new__(AuditCog)
        message = self._message([])

        with patch("cogs.audit.asyncio.sleep", new=AsyncMock()):
            actor_id, actor_name = await cog._resolve_message_delete_actor(message)

        self.assertIsNone(actor_id)
        self.assertIsNone(actor_name)

    async def test_recent_matching_audit_entry_identifies_moderator(self):
        moderator = SimpleNamespace(id=987654321)
        moderator.__str__ = lambda _self: "Moderator"
        target = SimpleNamespace(id=141890558045061120)
        channel = SimpleNamespace(id=1281720517921079356)
        entry = SimpleNamespace(
            target=target,
            user=moderator,
            extra=SimpleNamespace(channel=channel),
            created_at=discord.utils.utcnow() - timedelta(seconds=2),
        )
        cog = AuditCog.__new__(AuditCog)
        message = self._message([entry])

        with patch("cogs.audit.asyncio.sleep", new=AsyncMock()):
            actor_id, actor_name = await cog._resolve_message_delete_actor(message)

        self.assertEqual(actor_id, moderator.id)
        self.assertEqual(actor_name, str(moderator))

    async def test_failed_audit_log_read_does_not_guess_self_delete(self):
        async def forbidden_audit_logs(**_kwargs):
            raise discord.Forbidden(SimpleNamespace(status=403, reason="Forbidden"), "missing")
            yield  # pragma: no cover

        cog = AuditCog.__new__(AuditCog)
        message = self._message([])
        message.guild.audit_logs = forbidden_audit_logs

        with patch("cogs.audit.asyncio.sleep", new=AsyncMock()):
            actor_id, actor_name = await cog._resolve_message_delete_actor(message)

        self.assertIsNone(actor_id)
        self.assertIsNone(actor_name)


class AuditEnumCompatibilityTests(unittest.TestCase):
    def test_audit_action_uses_shared_database_values(self):
        enum_values = AuditLog.__table__.c.action.type.enums

        self.assertIn("message_delete", enum_values)
        self.assertIn("role_add", enum_values)
        self.assertNotIn("MESSAGE_DELETE", enum_values)


if __name__ == "__main__":
    unittest.main()
