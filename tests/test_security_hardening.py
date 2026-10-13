import socket
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import discord

from cogs.audit import _deleted_message_details
from cogs.roles import _requestable_role_error
from cogs.rss_feeds import _resolve_rss_target
from cogs.welcome import _resolve_auto_role_approver, _welcome_allowed_mentions
from utils.role_security import (
    automatic_role_error,
    automatic_role_execution_error,
    delegated_role_grant_error,
)


class FakeRole:
    def __init__(
        self,
        guild,
        role_id,
        position,
        *,
        permissions=None,
        managed=False,
        default=False,
    ):
        self.guild = guild
        self.id = role_id
        self.position = position
        self.permissions = permissions or discord.Permissions.none()
        self.managed = managed
        self._default = default

    def is_default(self):
        return self._default

    def __ge__(self, other):
        return self.position >= other.position


class FakeMember:
    def __init__(self, user_id, top_role):
        self.id = user_id
        self.top_role = top_role


class FakeGuild:
    def __init__(self, guild_id=1, owner_id=10):
        self.id = guild_id
        self.owner_id = owner_id
        self.me = None


class WelcomeMentionSecurityTests(unittest.TestCase):
    def test_welcome_can_ping_only_the_joining_member(self):
        member = SimpleNamespace(id=123456789)

        mentions = _welcome_allowed_mentions(member).to_dict()

        self.assertEqual(mentions, {"users": [123456789], "parse": []})


class RoleRequestSecurityTests(unittest.TestCase):
    def setUp(self):
        self.guild = FakeGuild()
        self.bot_role = FakeRole(self.guild, 99, 100)
        self.guild.me = FakeMember(999, self.bot_role)

    def test_privileged_role_is_never_self_service_requestable(self):
        permissions = discord.Permissions.none()
        permissions.administrator = True
        role = FakeRole(self.guild, 20, 20, permissions=permissions)

        self.assertIn("Privileged", _requestable_role_error(self.guild, role))

    def test_reviewer_must_outrank_role_and_target(self):
        role = FakeRole(self.guild, 20, 60)
        reviewer = FakeMember(30, FakeRole(self.guild, 30, 50))
        target = FakeMember(40, FakeRole(self.guild, 40, 10))

        self.assertIn(
            "at or above",
            _requestable_role_error(
                self.guild,
                role,
                reviewer=reviewer,
                target=target,
            ),
        )

    def test_safe_lower_role_is_requestable(self):
        role = FakeRole(self.guild, 20, 20)
        reviewer = FakeMember(30, FakeRole(self.guild, 30, 50))
        target = FakeMember(40, FakeRole(self.guild, 40, 10))

        self.assertIsNone(
            _requestable_role_error(
                self.guild,
                role,
                reviewer=reviewer,
                target=target,
            )
        )


class AutomaticRoleSecurityTests(unittest.TestCase):
    def setUp(self):
        self.guild = FakeGuild()
        self.guild.me = FakeMember(999, FakeRole(self.guild, 99, 100))
        self.approver = FakeMember(30, FakeRole(self.guild, 30, 50))

    def test_approver_cannot_delegate_higher_role(self):
        role = FakeRole(self.guild, 20, 60)
        self.assertIn("cannot delegate", automatic_role_error(
            self.guild, role, approver=self.approver
        ))

    def test_dangerous_role_is_never_automatic(self):
        dangerous_permissions = (
            "manage_roles",
            "manage_expressions",
            "mute_members",
            "deafen_members",
            "move_members",
            "manage_nicknames",
            "manage_events",
            "view_audit_log",
            "view_guild_insights",
            "manage_threads",
        )
        for permission_name in dangerous_permissions:
            with self.subTest(permission=permission_name):
                permissions = SimpleNamespace(**{permission_name: True})
                role = FakeRole(self.guild, 20, 20, permissions=permissions)
                self.assertIn("Privileged", automatic_role_error(
                    self.guild, role, approver=self.approver
                ))

    def test_missing_approver_quarantines_legacy_mapping(self):
        role = FakeRole(self.guild, 20, 20)
        self.assertIn("no longer a member", automatic_role_error(self.guild, role))

    def test_safe_lower_role_remains_assignable(self):
        role = FakeRole(self.guild, 20, 20)
        self.assertIsNone(automatic_role_error(
            self.guild, role, approver=self.approver
        ))

    def test_permission_drift_blocks_automatic_execution(self):
        permissions = discord.Permissions.none()
        permissions.manage_guild = True
        role = FakeRole(self.guild, 20, 20, permissions=permissions)
        self.assertIn(
            "Privileged",
            automatic_role_execution_error(self.guild, role),
        )

    def test_delegated_grant_respects_actor_and_target_hierarchy(self):
        actor = FakeMember(30, FakeRole(self.guild, 30, 50))
        higher_role = FakeRole(self.guild, 20, 60)
        higher_target = FakeMember(40, FakeRole(self.guild, 40, 50))

        self.assertIn(
            "highest role",
            delegated_role_grant_error(
                self.guild, higher_role, actor=actor, target=FakeMember(41, FakeRole(self.guild, 41, 10))
            ),
        )
        self.assertIn(
            "equal or higher",
            delegated_role_grant_error(
                self.guild, FakeRole(self.guild, 21, 20), actor=actor, target=higher_target
            ),
        )

    def test_guild_owner_can_delegate_a_safe_bot_manageable_role(self):
        owner = FakeMember(self.guild.owner_id, FakeRole(self.guild, 30, 1))
        role = FakeRole(self.guild, 20, 20)
        self.assertIsNone(delegated_role_grant_error(
            self.guild, role, actor=owner, target=self.approver
        ))

    def test_delegated_grant_requires_bot_to_outrank_target(self):
        owner = FakeMember(self.guild.owner_id, FakeRole(self.guild, 30, 1))
        target = FakeMember(40, FakeRole(self.guild, 40, 100))
        self.assertIn("target member", delegated_role_grant_error(
            self.guild, FakeRole(self.guild, 20, 20), actor=owner, target=target
        ))


class AutoRoleApproverResolutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_cached_approver_is_used_without_api_request(self):
        approver = object()
        guild = SimpleNamespace(
            get_member=lambda member_id: approver,
            fetch_member=None,
        )

        self.assertIs(approver, await _resolve_auto_role_approver(guild, 30))

    async def test_cache_miss_fetches_current_member(self):
        approver = object()

        async def fetch_member(member_id):
            self.assertEqual(30, member_id)
            return approver

        guild = SimpleNamespace(
            get_member=lambda member_id: None,
            fetch_member=fetch_member,
        )

        self.assertIs(approver, await _resolve_auto_role_approver(guild, 30))

    async def test_missing_approver_still_fails_closed(self):
        async def fetch_member(member_id):
            raise discord.NotFound(SimpleNamespace(status=404, reason="Not Found"), "missing")

        guild = SimpleNamespace(
            get_member=lambda member_id: None,
            fetch_member=fetch_member,
        )

        self.assertIsNone(await _resolve_auto_role_approver(guild, 30))


class DeletedMessageContextTests(unittest.TestCase):
    def test_metadata_is_preserved_when_content_capture_is_disabled(self):
        message = SimpleNamespace(
            id=123,
            channel=SimpleNamespace(id=456),
            content="private text",
            attachments=[SimpleNamespace(filename="evidence.png")],
            embeds=[object()],
            stickers=[],
        )

        details = _deleted_message_details(message, include_content=False)

        self.assertIn("Channel: <#456>", details)
        self.assertIn("Message ID: 123", details)
        self.assertIn("evidence.png", details)
        self.assertIn("Embeds: 1", details)
        self.assertNotIn("private text", details)

    def test_enabled_content_capture_includes_text(self):
        message = SimpleNamespace(
            id=123,
            channel=SimpleNamespace(id=456),
            content="deleted context",
            attachments=[],
            embeds=[],
            stickers=[],
        )

        details = _deleted_message_details(message, include_content=True)

        self.assertIn("Content: deleted context", details)


class RSSFetchSecurityTests(unittest.TestCase):
    def test_dns_failure_is_fail_closed(self):
        with patch(
            "cogs.rss_feeds.socket.getaddrinfo",
            side_effect=socket.gaierror(),
        ):
            target, error = _resolve_rss_target("https://missing.example/feed")

        self.assertIsNone(target)
        self.assertIn("could not be resolved", error)

    def test_any_private_dns_answer_blocks_the_host(self):
        answers = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443)),
        ]
        with patch("cogs.rss_feeds.socket.getaddrinfo", return_value=answers):
            target, error = _resolve_rss_target("https://example.com/feed")

        self.assertIsNone(target)
        self.assertIn("Non-public", error)

    def test_public_answer_produces_a_pinned_target(self):
        answers = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 8443)),
        ]
        with patch("cogs.rss_feeds.socket.getaddrinfo", return_value=answers):
            target, error = _resolve_rss_target(
                "https://example.com:8443/feed.xml?source=test"
            )

        self.assertIsNone(error)
        self.assertEqual("93.184.216.34", target["ip"])
        self.assertEqual("example.com:8443", target["host_header"])
        self.assertEqual("/feed.xml?source=test", target["request_target"])

    def test_url_credentials_are_rejected(self):
        target, error = _resolve_rss_target("https://user:secret@example.com/feed")
        self.assertIsNone(target)
        self.assertIn("Credentials", error)

    def test_cleartext_http_feed_is_rejected(self):
        target, error = _resolve_rss_target("http://example.com/feed")
        self.assertIsNone(target)
        self.assertIn("HTTPS", error)


if __name__ == "__main__":
    unittest.main()
