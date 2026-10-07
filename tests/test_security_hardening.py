import socket
import unittest
from unittest.mock import patch

import discord

from cogs.roles import _requestable_role_error
from cogs.rss_feeds import _resolve_rss_target


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
