import unittest
from types import SimpleNamespace

from utils.guild_presence import mark_departed_guilds


class GuildPresenceTests(unittest.TestCase):
    def test_marks_only_guilds_absent_from_live_gateway_list(self):
        active = SimpleNamespace(
            guild_id=10,
            guild_name="Active",
            bot_present=True,
            left_at=None,
        )
        departed = SimpleNamespace(
            guild_id=20,
            guild_name="Departed",
            bot_present=True,
            left_at=None,
        )

        changed = mark_departed_guilds(
            [active, departed],
            [10],
            left_at=1_800_000_000,
        )

        self.assertEqual(changed, [departed])
        self.assertIs(active.bot_present, True)
        self.assertIsNone(active.left_at)
        self.assertIs(departed.bot_present, False)
        self.assertEqual(departed.left_at, 1_800_000_000)

    def test_empty_live_gateway_list_marks_every_active_record_departed(self):
        records = [
            SimpleNamespace(guild_id=10, bot_present=True, left_at=None),
            SimpleNamespace(guild_id=20, bot_present=True, left_at=None),
        ]

        changed = mark_departed_guilds(records, [], left_at=123)

        self.assertEqual(changed, records)
        self.assertTrue(all(record.bot_present is False for record in records))
        self.assertTrue(all(record.left_at == 123 for record in records))


if __name__ == "__main__":
    unittest.main()
