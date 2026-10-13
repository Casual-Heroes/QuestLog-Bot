import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import cogs.gameserver as gameserver
from cogs.gameserver import (
    GameServerCog,
    _amp_config_is_authorized,
    _amp_public_password_is_authorized,
    _authorized_amp_log_dir,
    _current_amp_guild_bindings,
    _load_amp_guild_bindings,
    _resolve_server_password,
)
from utils.amp_registry import (
    AmpRegistryPolicy,
    parse_amp_guild_registry,
    parse_amp_registry_policy,
    read_secure_amp_guild_registry,
    registry_document,
)
from cogs.scheduled_messages import ScheduledMessageProcessor
from utils.guild_resources import resolve_guild_channel, resource_belongs_to_guild


class _GuildRef:
    def __init__(self, guild_id):
        self.id = guild_id


class _Channel:
    def __init__(self, guild_id, channel_id=20):
        self.guild = _GuildRef(guild_id)
        self.id = channel_id
        self.sent = []

    async def send(self, **kwargs):
        self.sent.append(kwargs)


class _Guild:
    def __init__(self, guild_id, *, cached=None, fetched=None):
        self.id = guild_id
        self.cached = cached
        self.fetched = fetched
        self.fetch_calls = []

    def get_channel_or_thread(self, channel_id):
        return self.cached

    async def fetch_channel(self, channel_id):
        self.fetch_calls.append(channel_id)
        return self.fetched


class _Bot:
    def __init__(self, guild):
        self.guild = guild

    def get_guild(self, guild_id):
        if self.guild and self.guild.id == guild_id:
            return self.guild
        return None


class GuildResourceTests(unittest.IsolatedAsyncioTestCase):
    def test_resource_ownership_is_fail_closed(self):
        self.assertTrue(resource_belongs_to_guild(_Channel(10), 10))
        self.assertFalse(resource_belongs_to_guild(_Channel(11), 10))
        self.assertFalse(resource_belongs_to_guild(_Channel(10, 21), 10, 20))
        self.assertFalse(resource_belongs_to_guild(object(), 10))

    async def test_cached_channel_must_belong_to_expected_guild(self):
        guild = _Guild(10, cached=_Channel(11))
        channel = await resolve_guild_channel(_Bot(guild), 10, 20)
        self.assertIsNone(channel)

    async def test_fetched_channel_must_belong_to_expected_guild(self):
        guild = _Guild(10, fetched=_Channel(11))
        channel = await resolve_guild_channel(_Bot(guild), 10, 20)
        self.assertIsNone(channel)
        self.assertEqual(guild.fetch_calls, [20])

    async def test_legitimate_cached_channel_is_preserved(self):
        expected = _Channel(10)
        guild = _Guild(10, cached=expected)
        channel = await resolve_guild_channel(_Bot(guild), "10", "20")
        self.assertIs(channel, expected)

    async def test_unknown_guild_is_rejected_without_global_lookup(self):
        channel = await resolve_guild_channel(_Bot(None), 10, 20)
        self.assertIsNone(channel)

    async def test_scheduled_message_rejects_cross_guild_destination(self):
        foreign = _Channel(11)
        processor = object.__new__(ScheduledMessageProcessor)
        processor.bot = _Bot(_Guild(10, cached=foreign))
        msg = SimpleNamespace(guild_id=10, channel_id=20)

        with self.assertRaisesRegex(ValueError, "not available in guild 10"):
            await processor.send_message(msg, {"content": "blocked"})
        self.assertEqual(foreign.sent, [])

    async def test_scheduled_message_preserves_same_guild_delivery(self):
        destination = _Channel(10)
        processor = object.__new__(ScheduledMessageProcessor)
        processor.bot = _Bot(_Guild(10, cached=destination))
        msg = SimpleNamespace(guild_id=10, channel_id=20)

        await processor.send_message(msg, {"content": "allowed", "silent": True})
        self.assertEqual(len(destination.sent), 1)
        sent = destination.sent[0]
        mentions = sent.pop("allowed_mentions")
        self.assertEqual(sent, {"content": "allowed", "silent": True})
        self.assertEqual(mentions.to_dict(), {"parse": []})

    async def test_game_status_rejects_foreign_channel_before_sensitive_lookup(self):
        cog = object.__new__(GameServerCog)
        cog.bot = _Bot(_Guild(10, cached=_Channel(11)))
        cog._last_fingerprint = {}
        cfg = {
            "instance_name": "foreign-instance",
            "discord_guild_id": 10,
            "discord_stats_channel_id": "20",
            "discord_stats_message_id": None,
        }

        with (
            patch(
                "cogs.gameserver._current_amp_guild_bindings",
                return_value={"foreign-instance": frozenset({10})},
            ),
            patch("cogs.gameserver.build_serverinfo_embed", new_callable=AsyncMock) as build,
        ):
            await cog._refresh_one(cfg)
        build.assert_not_awaited()

    def test_amp_binding_parser_and_authorization_fail_closed(self):
        bindings = _load_amp_guild_bindings('{"instance-a":[10,11]}')
        self.assertEqual(bindings, {"instance-a": frozenset({10, 11})})
        with patch("cogs.gameserver._current_amp_guild_bindings", return_value=bindings):
            self.assertTrue(_amp_config_is_authorized({
                "instance_name": "instance-a", "discord_guild_id": 10,
            }))
            self.assertFalse(_amp_config_is_authorized({
                "instance_name": "instance-a", "discord_guild_id": 12,
            }))
            self.assertFalse(_amp_config_is_authorized({
                "instance_name": "instance-b", "discord_guild_id": 10,
            }))

        for invalid in ('not-json', '[]', '{"../escape":10}', '{"ok":[]}'):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    _load_amp_guild_bindings(invalid)

        for invalid in ('{"ok":true}', '{"ok":10.5}', '{"ok":0}'):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    _load_amp_guild_bindings(invalid)

    def test_versioned_amp_registry_is_strict(self):
        bindings = parse_amp_guild_registry(
            '{"version":1,"bindings":{"instance-a":[10]}}'
        )
        self.assertEqual(bindings, {"instance-a": frozenset({10})})
        invalid_documents = (
            '{"version":1,"bindings":{"instance-a":10}}',
            '{"version":2,"bindings":{"instance-a":[10]}}',
            '{"version":1,"bindings":{},"extra":true}',
            '{"version":1,"version":1,"bindings":{}}',
            '{"version":1,"bindings":{"instance-a":[true]}}',
        )
        for document in invalid_documents:
            with self.subTest(document=document):
                with self.assertRaises(ValueError):
                    parse_amp_guild_registry(document)

        policy = parse_amp_registry_policy(
            '{"version":1,"bindings":{"instance-a":[10]},'
            '"public_passwords":{"instance-a":[10]}}'
        )
        self.assertEqual(policy.public_passwords, {"instance-a": frozenset({10})})
        with self.assertRaisesRegex(ValueError, "subset"):
            parse_amp_registry_policy(
                '{"version":1,"bindings":{"instance-a":[10]},'
                '"public_passwords":{"instance-a":[11]}}'
            )

    def test_secure_registry_reloads_without_restart_and_rejects_writable_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bindings.json"
            path.write_text(
                registry_document({"instance-a": [10]}), encoding="utf-8"
            )
            path.chmod(0o600)
            self.assertEqual(
                read_secure_amp_guild_registry(path, required_owner_uid=os.getuid()),
                {"instance-a": frozenset({10})},
            )
            path.write_text(
                registry_document({"instance-a": [11]}), encoding="utf-8"
            )
            self.assertEqual(
                read_secure_amp_guild_registry(path, required_owner_uid=os.getuid()),
                {"instance-a": frozenset({11})},
            )
            path.chmod(0o620)
            with self.assertRaisesRegex(ValueError, "writable"):
                read_secure_amp_guild_registry(path, required_owner_uid=os.getuid())

    def test_secure_registry_rejects_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "target.json"
            target.write_text(registry_document({"instance-a": [10]}), encoding="utf-8")
            target.chmod(0o600)
            link = Path(directory) / "bindings.json"
            link.symlink_to(target)
            with self.assertRaises(ValueError):
                read_secure_amp_guild_registry(link, required_owner_uid=os.getuid())

    def test_registry_overrides_fallback_and_disappearance_fails_closed(self):
        old_seen = gameserver._AMP_REGISTRY_SEEN
        old_error = gameserver._AMP_REGISTRY_LAST_ERROR
        try:
            gameserver._AMP_REGISTRY_SEEN = False
            gameserver._AMP_REGISTRY_LAST_ERROR = None
            with (
                patch.object(
                    gameserver,
                    "AMP_GUILD_BINDINGS",
                    {"legacy-instance": frozenset({99})},
                ),
                patch.object(
                    gameserver,
                    "read_secure_amp_registry_policy",
                    side_effect=[
                        AmpRegistryPolicy(
                            {"registry-instance": frozenset({10})}, {}
                        ),
                        None,
                    ],
                ),
            ):
                self.assertEqual(
                    _current_amp_guild_bindings(),
                    {"registry-instance": frozenset({10})},
                )
                self.assertEqual(_current_amp_guild_bindings(), {})
        finally:
            gameserver._AMP_REGISTRY_SEEN = old_seen
            gameserver._AMP_REGISTRY_LAST_ERROR = old_error

    def test_public_password_requires_root_owned_registry_grant(self):
        cfg = {"instance_name": "instance-a", "discord_guild_id": 10}
        with patch.object(
            gameserver,
            "read_secure_amp_registry_policy",
            return_value=AmpRegistryPolicy(
                {"instance-a": frozenset({10})},
                {"instance-a": frozenset({10})},
            ),
        ):
            self.assertTrue(_amp_public_password_is_authorized(cfg))
        with patch.object(
            gameserver,
            "read_secure_amp_registry_policy",
            return_value=AmpRegistryPolicy(
                {"instance-a": frozenset({10})}, {}
            ),
        ):
            self.assertFalse(_amp_public_password_is_authorized(cfg))

    async def test_password_is_discarded_if_binding_is_revoked_during_lookup(self):
        cfg = {
            "instance_name": "instance-a",
            "discord_guild_id": 10,
            "show_password": True,
        }
        with (
            patch.object(gameserver, "_amp_config_is_authorized", side_effect=[True, False]),
            patch.object(
                gameserver,
                "_get_amp_server_password",
                new_callable=AsyncMock,
                return_value="secret",
            ),
        ):
            self.assertIsNone(await _resolve_server_password(cfg))

    def test_amp_log_directory_must_stay_inside_bound_instance(self):
        cfg = {
            "instance_name": "instance-a",
            "discord_guild_id": 10,
            "amp_log_dir": "/srv/amp/instance-a/logs",
        }
        with (
            patch(
                "cogs.gameserver._current_amp_guild_bindings",
                return_value={"instance-a": frozenset({10})},
            ),
            patch("cogs.gameserver.AMP_INSTANCES_PATHS", [Path("/srv/amp")]),
        ):
            self.assertEqual(
                _authorized_amp_log_dir(cfg),
                Path("/srv/amp/instance-a/logs"),
            )
            cfg["amp_log_dir"] = "/etc"
            self.assertIsNone(_authorized_amp_log_dir(cfg))


if __name__ == "__main__":
    unittest.main()
