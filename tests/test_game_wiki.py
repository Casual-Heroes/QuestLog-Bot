import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import discord

from cogs.game_wiki import GAME_INFO, GameWikiCog, _directory_embed, _help_embed
from cogs.game_wiki_er import BOSS_ICON_MEDIA_ID, _boss_icon_url
from cogs.game_wiki_hubs import _questlog_fetch_url, record_embed


class GameWikiCommandTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.bot = discord.Bot(intents=discord.Intents.none())
        self.cog = GameWikiCog(self.bot)

    def test_all_game_groups_have_search_and_commands(self):
        commands = {command.name: command for command in self.cog.get_commands()}
        self.assertIn("games", commands)
        self.assertEqual(set(GAME_INFO) | {"ff", "ens"}, set(commands) - {"games"})
        for game in GAME_INFO:
            self.assertEqual(
                {"search", "commands"},
                {command.name for command in commands[game].subcommands},
            )
        self.assertEqual({
            "commands", "collection", "deepdungeon", "fieldops", "tripletriad", "fc",
            "gathering", "ocean", "status", "housing", "market", "resets",
            "jobs", "hunts", "partyfinder", "apply",
        }, {command.name for command in commands["ff"].subcommands})
        self.assertEqual({
            "commands", "server", "status", "uptime", "settings", "lastbackup", "whois",
        }, {command.name for command in commands["ens"].subcommands})

    def test_category_choices_fit_discord_limit(self):
        for game, (_, _, categories, _) in GAME_INFO.items():
            with self.subTest(game=game):
                self.assertLessEqual(len(categories), 25)
                self.assertEqual(len(categories), len(set(categories)))

    def test_directory_is_compact_and_slash_only(self):
        embed = _directory_embed()
        self.assertIn("/dd2 search name:Cyclops", embed.description)
        self.assertIn("/err commands", embed.description)
        self.assertIn("/ff commands", embed.description)
        self.assertIn("/ens commands", embed.description)
        self.assertNotIn("!dd2", embed.description)
        self.assertNotIn("—", embed.description)
        self.assertLessEqual(len(embed.description), 4096)

    def test_each_help_embed_has_categories_and_slash_examples(self):
        for game in GAME_INFO:
            with self.subTest(game=game):
                embed = _help_embed(game)
                rendered = "\n".join(field.value for field in embed.fields)
                self.assertIn(f"/{game} search", rendered)
                self.assertNotIn(f"!{game}", rendered)
                self.assertNotIn("—", rendered)

    async def test_dd2_filtered_search_routes_to_dd2_service(self):
        ctx = SimpleNamespace(defer=AsyncMock(), followup=SimpleNamespace(send=AsyncMock()))
        self.cog._dd2._lookup = AsyncMock()
        await self.cog._search(ctx, "dd2", "Cyclops", "boss")
        ctx.defer.assert_awaited_once()
        self.cog._dd2._lookup.assert_awaited_once()
        args = self.cog._dd2._lookup.await_args.args
        self.assertEqual(("boss", "Cyclops"), args[1:])

    async def test_err_unfiltered_search_stays_in_reforged_catalog(self):
        ctx = SimpleNamespace(defer=AsyncMock(), followup=SimpleNamespace(send=AsyncMock()))
        self.cog._er._search = AsyncMock()
        await self.cog._search(ctx, "err", "flamelost greatblades", None)
        args = self.cog._er._search.await_args.args
        self.assertEqual(("err", "flamelost greatblades"), args[1:])

    async def test_hub_filtered_search_preserves_category(self):
        ctx = SimpleNamespace(defer=AsyncMock(), followup=SimpleNamespace(send=AsyncMock()))
        self.cog._hubs._run = AsyncMock()
        await self.cog._search(ctx, "ms2", "Axatana", "weapons")
        args = self.cog._hubs._run.await_args.args
        self.assertEqual(("ms2", "weapons Axatana"), args[1:])

    async def test_ff_gathering_routes_to_live_service(self):
        ctx = SimpleNamespace(
            defer=AsyncMock(), followup=SimpleNamespace(send=AsyncMock()),
            author=SimpleNamespace(id=42), guild=SimpleNamespace(id=99),
        )
        self.cog._hubs._ff_gathering = AsyncMock()
        await self.cog._ff_call(ctx, "_ff_gathering", "cotton")
        ctx.defer.assert_awaited_once()
        args = self.cog._hubs._ff_gathering.await_args.args
        self.assertEqual("cotton", args[1])

    def test_ff_and_ens_help_are_live_data_guides(self):
        ff_text = "\n".join(field.value for field in self.cog._ff_help_embed().fields)
        ens_text = "\n".join(field.value for field in self.cog._ens_help_embed().fields)
        self.assertIn("/ff gathering", ff_text)
        self.assertIn("/ff collection", ff_text)
        self.assertIn("/ens settings", ens_text)
        self.assertIn("/ens whois", ens_text)
        self.assertNotIn("—", ff_text + ens_text)

    def test_linked_ffxiv_profile_uses_discord_identity(self):
        source = Path("cogs/game_wiki_hubs.py").read_text()
        self.assertIn("WHERE discord_id=:did", source)
        self.assertNotIn("WHERE fluxer_id=:fid", source)

    def test_ens_status_never_requests_server_password(self):
        source = Path("cogs/game_wiki.py").read_text()
        self.assertIn("build_serverinfo_embed(cfg, include_password=False)", source)


    def test_hub_fetches_are_restricted_to_questlog_https(self):
        self.assertEqual(
            "https://questlog.casual-heroes.com/api/example/",
            _questlog_fetch_url("api/example/"),
        )
        with self.assertRaises(ValueError):
            _questlog_fetch_url("http://questlog.casual-heroes.com/api/example/")
        with self.assertRaises(ValueError):
            _questlog_fetch_url("https://127.0.0.1/private")


class GameWikiPresentationTests(unittest.TestCase):
    def test_boss_icon_is_stable_for_all_elden_ring_bosses(self):
        self.assertEqual("c50ea0523444ce124ad4", BOSS_ICON_MEDIA_ID)
        self.assertIn(BOSS_ICON_MEDIA_ID, _boss_icon_url("er"))
        self.assertIn(BOSS_ICON_MEDIA_ID, _boss_icon_url("err"))

    def test_ms2_weapon_progression_is_split_into_readable_fields(self):
        embed = record_embed("ms2", "weapons", {
            "name": "Axatana",
            "max_level": 20,
            "enabled_metrics": [
                {"label": "Damage", "level_0": 1, "level_max": 2},
                {"label": "Guard meter", "level_0": 1, "level_max": 1.3125},
            ],
            "compatible_upgrades": [{"name": "Grudge Stone", "description": "Guaranteed Critical Hit"}],
        })
        fields = {field.name: field.value for field in embed.fields}
        self.assertIn("Damage progression", fields)
        self.assertIn("Guard meter progression", fields)
        self.assertIn("**Base (+0):**", fields["Damage progression"])
        self.assertIn("Grudge Stone", fields["Works with this weapon"])

    def test_cd_refinement_steps_are_separate_fields(self):
        embed = record_embed("cd", "weapons", {
            "name": "Antumbra's Eye",
            "recipes": [
                {"from": 0, "to": 1, "materials": [{"name": "Iron Ore", "count": 3}]},
                {"from": 1, "to": 2, "materials": [{"name": "Iron Ore +1", "count": 4}]},
            ],
        })
        names = [field.name for field in embed.fields]
        self.assertIn("Refinement +0 to +1", names)
        self.assertIn("Refinement +1 to +2", names)


if __name__ == "__main__":
    unittest.main()
