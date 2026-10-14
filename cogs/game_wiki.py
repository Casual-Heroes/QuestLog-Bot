"""Native Discord slash commands for QuestLog's supported game databases."""

from __future__ import annotations

import discord
from discord.ext import commands
from datetime import datetime
from pathlib import Path

from config import logger
from cogs.game_wiki_dd2 import CATEGORIES as DD2_CATEGORIES, DD2Cog
from cogs.game_wiki_er import EldenRingCog, categories_for
from cogs.game_wiki_hubs import (
    GAMES as HUB_GAMES,
    GameHubsCog,
    _add,
    _absolute,
    _ens_challenge_groups,
)


QUESTLOG_URL = "https://questlog.casual-heroes.com"

DD2_CHOICES = list(DD2_CATEGORIES)
ER_CHOICES = list(categories_for("er"))
ERR_CHOICES = list(categories_for("err"))
MS2_CHOICES = list(HUB_GAMES["ms2"]["categories"])
R2_CHOICES = list(HUB_GAMES["r2"]["categories"])
NR_CHOICES = list(HUB_GAMES["nr"]["categories"])
CD_CHOICES = list(HUB_GAMES["cd"]["categories"])

GAME_INFO = {
    "dd2": ("Dragon's Dogma 2", 0xC9A227, DD2_CHOICES, "dragons-dogma-2"),
    "er": ("Elden Ring", 0xC6A75B, ER_CHOICES, "elden-ring"),
    "err": ("Elden Ring Reforged", 0x8B5CF6, ERR_CHOICES, "elden-ring-reforged"),
    "ms2": ("Mortal Shell II", 0x8A8175, MS2_CHOICES, "mortal-shell-2"),
    "r2": ("Remnant 2", 0xB91C1C, R2_CHOICES, "remnant-2"),
    "nr": ("No Rest for the Wicked", 0x9A6A3A, NR_CHOICES, "no-rest-for-the-wicked"),
    "cd": ("Crimson Desert", 0xB4533C, CD_CHOICES, "crimson-desert"),
}


class _SlashReply:
    """Give the proven prefix-command services a minimal reply interface."""

    def __init__(self, ctx: discord.ApplicationContext):
        self._ctx = ctx
        self.author = getattr(ctx, "author", None)
        self.guild = getattr(ctx, "guild", None)

    async def reply(self, content=None, *, embed=None):
        await self._ctx.followup.send(content=content, embed=embed)


def _help_embed(game: str) -> discord.Embed:
    label, color, categories, slug = GAME_INFO[game]
    category_text = ", ".join(f"`{value}`" for value in categories)
    embed = discord.Embed(
        title=f"{label} commands",
        description=(
            f"Use `/{game} search` to search all {label} data. "
            "Choose a category only when you want to narrow the result."
        ),
        color=color,
        url=f"{QUESTLOG_URL}/soulslike/database/{slug}/",
    )
    embed.add_field(
        name="Search",
        value=(
            f"`/{game} search name:<name>` - search every category\n"
            f"`/{game} search name:<name> category:<category>` - filtered search"
        ),
        inline=False,
    )
    embed.add_field(name="Available categories", value=category_text[:1024], inline=False)
    embed.set_footer(text=f"QuestLog - {label} Data")
    return embed


def _directory_embed() -> discord.Embed:
    embed = discord.Embed(
        title="QuestLog Game Command Directory",
        description=(
            "Search by name across a game's database, or choose a category to narrow the result.\n\n"
            "Example: `/dd2 search name:Cyclops`"
        ),
        color=0x5865F2,
        url=QUESTLOG_URL,
    )
    rows = []
    for key, (label, _, categories, _) in GAME_INFO.items():
        rows.append(
            f"**{label}** - `/{key}`\n"
            f"`/{key} search name:<name>` - {len(categories)} searchable categories\n"
            f"`/{key} commands` - categories and examples"
        )
    rows.extend([
        "**Final Fantasy XIV** - `/ff`\nLive timers, housing, market data, and linked character progress\n`/ff commands` - complete tool guide",
        "**Enshrouded** - `/ens`\nLive ShroudQuest status, settings, uptime, backups, and players\n`/ens commands` - complete server guide",
    ])
    embed.description += "\n\n" + "\n\n".join(rows)
    embed.set_footer(text="QuestLog - Game data and live maps")
    return embed


class GameWikiCog(commands.Cog):
    """QuestLog game-database commands exposed as native Discord interactions."""

    dd2 = discord.SlashCommandGroup("dd2", "Search Dragon's Dogma 2 data")
    er = discord.SlashCommandGroup("er", "Search Elden Ring data")
    err = discord.SlashCommandGroup("err", "Search Elden Ring Reforged data")
    ms2 = discord.SlashCommandGroup("ms2", "Search Mortal Shell II data")
    r2 = discord.SlashCommandGroup("r2", "Search Remnant 2 data")
    nr = discord.SlashCommandGroup("nr", "Search No Rest for the Wicked data")
    cd = discord.SlashCommandGroup("cd", "Search Crimson Desert data")
    ff = discord.SlashCommandGroup("ff", "Final Fantasy XIV QuestLog tools")
    ens = discord.SlashCommandGroup("ens", "ShroudQuest Enshrouded server tools")

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._dd2 = DD2Cog(bot)
        self._er = EldenRingCog(bot)
        self._hubs = GameHubsCog(bot)

    async def _search(self, ctx, game: str, name: str, category: str | None):
        await ctx.defer()
        reply = _SlashReply(ctx)
        try:
            if game == "dd2":
                if category:
                    await self._dd2._lookup(reply, category, name)
                else:
                    await self._dd2._search(reply, name)
            elif game in {"er", "err"}:
                if category:
                    await self._er._lookup(reply, game, category, name)
                else:
                    await self._er._search(reply, game, name)
            else:
                raw = f"{category} {name}" if category else name
                await self._hubs._run(reply, game, raw)
        except Exception:
            logger.exception("Slash game lookup failed: game=%s category=%s", game, category)
            embed = discord.Embed(
                title=f"{GAME_INFO[game][0]} lookup unavailable",
                description="QuestLog could not load this game data right now. Please try again shortly.",
                color=0xED4245,
            )
            await ctx.followup.send(embed=embed)

    async def _ff_call(self, ctx, method: str, *args):
        await ctx.defer()
        try:
            await getattr(self._hubs, method)(_SlashReply(ctx), *args)
        except Exception:
            logger.exception("FFXIV slash command failed: method=%s", method)
            await ctx.followup.send(embed=discord.Embed(
                title="FFXIV data unavailable",
                description="QuestLog could not load this FFXIV data right now. Please try again shortly.",
                color=0xED4245,
            ))

    @staticmethod
    def _ff_help_embed():
        embed = discord.Embed(
            title="Final Fantasy XIV commands",
            description="Live QuestLog tools and progress from your linked Discord and Lodestone accounts.",
            color=0x6D5ACF,
            url=_absolute("/ffxiv/tools/"),
        )
        _add(embed, "Live data", [
            "`/ff gathering [query]` - current and upcoming gathering nodes",
            "`/ff ocean` - upcoming Ocean Fishing departures",
            "`/ff status [world]` - world and datacenter status",
            "`/ff housing [world]` - a balanced mix of open housing plots",
            "`/ff market <item> [world]` - current Market Board listings",
            "`/ff resets` - daily and weekly reset countdowns",
        ])
        _add(embed, "Linked progress", [
            "`/ff collection [view]` - mounts, minions, achievements, and clears",
            "`/ff deepdungeon` - personal Deep Dungeon bests",
            "`/ff fieldops` - Eureka, Bozja, and Occult Crescent progress",
            "`/ff tripletriad` - card collection progress",
            "`/ff fc` - tracked FC progression",
        ])
        _add(embed, "Community tools", [
            "`/ff jobs` - job guides and builds", "`/ff hunts` - community hunt board",
            "`/ff partyfinder` - QuestLog Party Finder", "`/ff apply` - FC application",
        ])
        embed.set_footer(text="QuestLog - Final Fantasy XIV Data")
        return embed

    @staticmethod
    def _ens_help_embed():
        embed = discord.Embed(
            title="Enshrouded commands",
            description="Live ShroudQuest server data and public challenge settings.",
            color=0xD97706,
            url=_absolute("/survival/enshrouded/"),
        )
        _add(embed, "Server tools", [
            "`/ens server` - ShroudQuest server hub",
            "`/ens status` - live server information",
            "`/ens settings` - gameplay and challenge settings",
            "`/ens uptime` - current server uptime",
            "`/ens lastbackup` - newest completed backup",
            "`/ens whois <player>` - online player lookup",
        ])
        embed.set_footer(text="QuestLog - Enshrouded Server Data")
        return embed

    @staticmethod
    def _ens_config(ctx):
        if not ctx.guild:
            return None
        from cogs.gameserver import _load_all_configs
        configs = [
            cfg for cfg in _load_all_configs()
            if str(cfg.get("discord_guild_id")) == str(ctx.guild.id)
            and "enshrouded" in str(cfg.get("game_type") or "").casefold()
        ]
        return configs[0] if len(configs) == 1 else None

    async def _ens_unavailable(self, ctx):
        await ctx.respond(embed=discord.Embed(
            title="Enshrouded server unavailable",
            description="No single authorized Enshrouded server is configured for this Discord community.",
            color=0xED4245,
            url=_absolute("/survival/enshrouded/"),
        ), ephemeral=True)

    @discord.slash_command(name="games", description="Show every QuestLog game command")
    async def games(self, ctx: discord.ApplicationContext):
        await ctx.respond(embed=_directory_embed())

    @dd2.command(name="search", description="Search Dragon's Dogma 2 data")
    @discord.option("name", str, description="Boss, enemy, item, skill, location, or other name")
    @discord.option("category", str, description="Optional category filter", choices=DD2_CHOICES, required=False)
    async def dd2_search(self, ctx, name: str, category: str | None = None):
        await self._search(ctx, "dd2", name, category)

    @dd2.command(name="commands", description="Show Dragon's Dogma 2 categories and examples")
    async def dd2_commands(self, ctx):
        await ctx.respond(embed=_help_embed("dd2"))

    @er.command(name="search", description="Search Elden Ring data")
    @discord.option("name", str, description="Weapon, boss, spell, location, or other name")
    @discord.option("category", str, description="Optional category filter", choices=ER_CHOICES, required=False)
    async def er_search(self, ctx, name: str, category: str | None = None):
        await self._search(ctx, "er", name, category)

    @er.command(name="commands", description="Show Elden Ring categories and examples")
    async def er_commands(self, ctx):
        await ctx.respond(embed=_help_embed("er"))

    @err.command(name="search", description="Search Elden Ring Reforged data")
    @discord.option("name", str, description="Weapon, boss, system, location, or other name")
    @discord.option("category", str, description="Optional category filter", choices=ERR_CHOICES, required=False)
    async def err_search(self, ctx, name: str, category: str | None = None):
        await self._search(ctx, "err", name, category)

    @err.command(name="commands", description="Show Elden Ring Reforged categories and examples")
    async def err_commands(self, ctx):
        await ctx.respond(embed=_help_embed("err"))

    @ms2.command(name="search", description="Search Mortal Shell II data")
    @discord.option("name", str, description="Weapon, shell, Tarstone, item, or boss name")
    @discord.option("category", str, description="Optional category filter", choices=MS2_CHOICES, required=False)
    async def ms2_search(self, ctx, name: str, category: str | None = None):
        await self._search(ctx, "ms2", name, category)

    @ms2.command(name="commands", description="Show Mortal Shell II categories and examples")
    async def ms2_commands(self, ctx):
        await ctx.respond(embed=_help_embed("ms2"))

    @r2.command(name="search", description="Search Remnant 2 data")
    @discord.option("name", str, description="Equipment, archetype, trait, boss, or other name")
    @discord.option("category", str, description="Optional category filter", choices=R2_CHOICES, required=False)
    async def r2_search(self, ctx, name: str, category: str | None = None):
        await self._search(ctx, "r2", name, category)

    @r2.command(name="commands", description="Show Remnant 2 categories and examples")
    async def r2_commands(self, ctx):
        await ctx.respond(embed=_help_embed("r2"))

    @nr.command(name="search", description="Search No Rest for the Wicked data")
    @discord.option("name", str, description="Weapon, armor, rune, boss, or other name")
    @discord.option("category", str, description="Optional category filter", choices=NR_CHOICES, required=False)
    async def nr_search(self, ctx, name: str, category: str | None = None):
        await self._search(ctx, "nr", name, category)

    @nr.command(name="commands", description="Show No Rest for the Wicked categories and examples")
    async def nr_commands(self, ctx):
        await ctx.respond(embed=_help_embed("nr"))

    @cd.command(name="search", description="Search Crimson Desert data")
    @discord.option("name", str, description="Weapon, armor, companion, mount, item, or other name")
    @discord.option("category", str, description="Optional category filter", choices=CD_CHOICES, required=False)
    async def cd_search(self, ctx, name: str, category: str | None = None):
        await self._search(ctx, "cd", name, category)

    @cd.command(name="commands", description="Show Crimson Desert categories and examples")
    async def cd_commands(self, ctx):
        await ctx.respond(embed=_help_embed("cd"))

    @ff.command(name="commands", description="Show every Final Fantasy XIV QuestLog command")
    async def ff_commands(self, ctx):
        await ctx.respond(embed=self._ff_help_embed())

    @ff.command(name="collection", description="Show your linked FFXIV collection progress")
    @discord.option("view", str, choices=["summary", "mounts", "minions", "achievements"], default="summary")
    async def ff_collection(self, ctx, view: str = "summary"):
        await self._ff_call(ctx, "_ff_collection", "collection" if view == "summary" else view)

    @ff.command(name="deepdungeon", description="Show your linked Deep Dungeon personal bests")
    async def ff_deepdungeon(self, ctx):
        await self._ff_call(ctx, "_ff_personal_tracker", "deepdungeon")

    @ff.command(name="fieldops", description="Show your linked Field Operations progress")
    async def ff_fieldops(self, ctx):
        await self._ff_call(ctx, "_ff_personal_tracker", "fieldops")

    @ff.command(name="tripletriad", description="Show your linked Triple Triad collection")
    async def ff_tripletriad(self, ctx):
        await self._ff_call(ctx, "_ff_personal_tracker", "tripletriad")

    @ff.command(name="fc", description="Show your tracked FC progression")
    async def ff_fc(self, ctx):
        await self._ff_call(ctx, "_ff_personal_tracker", "fc")

    @ff.command(name="gathering", description="Show active and upcoming timed gathering nodes")
    @discord.option("query", str, description="Optional item, node, or zone", required=False)
    async def ff_gathering(self, ctx, query: str = ""):
        await self._ff_call(ctx, "_ff_gathering", query)

    @ff.command(name="ocean", description="Show upcoming Ocean Fishing departures")
    async def ff_ocean(self, ctx):
        await self._ff_call(ctx, "_ff_ocean")

    @ff.command(name="status", description="Show FFXIV world status")
    @discord.option("query", str, description="Optional world or datacenter", required=False)
    async def ff_status(self, ctx, query: str = ""):
        await self._ff_call(ctx, "_ff_world_status", query)

    @ff.command(name="housing", description="Show a balanced mix of currently open housing plots")
    @discord.option("world", str, description="World; uses your linked character when omitted", required=False)
    async def ff_housing(self, ctx, world: str = ""):
        await self._ff_call(ctx, "_ff_housing", world)

    @ff.command(name="market", description="Show current Market Board listings and sale activity")
    @discord.option("item", str, description="Item name")
    @discord.option("world", str, description="World; uses your linked character when omitted", required=False)
    async def ff_market(self, ctx, item: str, world: str = ""):
        argument = f"{item} | {world}" if world else item
        await self._ff_call(ctx, "_ff_market_board", argument)

    @ff.command(name="resets", description="Show daily and weekly FFXIV reset countdowns")
    async def ff_resets(self, ctx):
        await self._ff_call(ctx, "_ff_resets")

    async def _ff_page(self, ctx, label, path):
        url = _absolute(path)
        await ctx.respond(embed=discord.Embed(
            title=f"FFXIV - {label}", description=f"[Open {label} on QuestLog]({url})",
            color=0x6D5ACF, url=url,
        ))

    @ff.command(name="jobs", description="Open QuestLog FFXIV job guides")
    async def ff_jobs(self, ctx):
        await self._ff_page(ctx, "Job Guides", "/ffxiv/tools/job-guides/")

    @ff.command(name="hunts", description="Open the QuestLog community Hunt Board")
    async def ff_hunts(self, ctx):
        await self._ff_page(ctx, "Hunt Board", "/ffxiv/tools/hunt-board/")

    @ff.command(name="partyfinder", description="Open the QuestLog Party Finder")
    async def ff_partyfinder(self, ctx):
        await self._ff_page(ctx, "Party Finder", "/lfg/")

    @ff.command(name="apply", description="Open the Casual Heroes FC application")
    async def ff_apply(self, ctx):
        await self._ff_page(ctx, "Apply to the FC", "/ffxiv/apply/")

    @ens.command(name="commands", description="Show every Enshrouded server command")
    async def ens_commands(self, ctx):
        await ctx.respond(embed=self._ens_help_embed())

    @ens.command(name="server", description="Open the ShroudQuest Enshrouded server hub")
    async def ens_server(self, ctx):
        url = _absolute("/survival/enshrouded/")
        await ctx.respond(embed=discord.Embed(
            title="ShroudQuest Enshrouded Server",
            description=f"[Open the ShroudQuest Enshrouded hub on QuestLog]({url})",
            color=0xD97706, url=url,
        ))

    @ens.command(name="status", description="Show live ShroudQuest server information")
    async def ens_status(self, ctx):
        cfg = self._ens_config(ctx)
        if not cfg:
            await self._ens_unavailable(ctx); return
        await ctx.defer()
        try:
            from cogs.gameserver import build_serverinfo_embed
            embed = await build_serverinfo_embed(cfg, include_password=False)
            embed.url = _absolute("/survival/enshrouded/")
            await ctx.followup.send(embed=embed)
        except Exception:
            logger.exception("Enshrouded status lookup failed")
            await ctx.followup.send(embed=discord.Embed(title="Enshrouded status unavailable", color=0xED4245))

    @ens.command(name="uptime", description="Show current ShroudQuest uptime")
    async def ens_uptime(self, ctx):
        cfg = self._ens_config(ctx)
        if not cfg:
            await self._ens_unavailable(ctx); return
        await ctx.defer()
        from cogs.gameserver import _get_server_status
        status = await _get_server_status(cfg["instance_name"], public_ip=cfg.get("public_ip") or None)
        online = bool(status.get("is_running"))
        await ctx.followup.send(embed=discord.Embed(
            title="ShroudQuest Uptime",
            description=f"**Status:** {'Online' if online else 'Offline'}\n**Uptime:** {status.get('uptime') or 'Unavailable'}",
            color=0xD97706, url=_absolute("/survival/enshrouded/"),
        ))

    @ens.command(name="settings", description="Show current ShroudQuest gameplay settings")
    async def ens_settings(self, ctx):
        await ctx.defer()
        url = _absolute("/survival/enshrouded/")
        try:
            status, source = await self._hubs._request_text(url)
            groups = _ens_challenge_groups(source) if status == 200 else []
        except Exception:
            groups = []
        embed = discord.Embed(
            title="ShroudQuest Challenge Profile",
            description="Current public gameplay rules from the dedicated-server configuration.",
            color=0xD97706, url=url,
        )
        if groups:
            for title, settings in groups:
                _add(embed, title, [f"• **{label}:** {value}" for label, value in settings])
        else:
            embed.description = f"The gameplay settings could not be loaded right now. [View them on QuestLog]({url})."
        embed.set_footer(text="QuestLog - Enshrouded Server Data")
        await ctx.followup.send(embed=embed)

    @ens.command(name="lastbackup", description="Show the newest completed ShroudQuest backup")
    async def ens_lastbackup(self, ctx):
        cfg = self._ens_config(ctx)
        if not cfg:
            await self._ens_unavailable(ctx); return
        from cogs.gameserver import _authorized_amp_log_dir
        log_dir = _authorized_amp_log_dir(cfg)
        snapshot = None
        if log_dir:
            try:
                backups = sorted((Path(log_dir).parent / "Backups").glob("*.zip"), key=lambda item: item.stat().st_mtime_ns)
                if backups:
                    latest = backups[-1]
                    snapshot = (latest.name, latest.stat())
            except OSError:
                snapshot = None
        if snapshot:
            name, stat = snapshot
            modified = datetime.fromtimestamp(stat.st_mtime).astimezone()
            description = f"**Created:** {modified:%Y-%m-%d %I:%M %p %Z}\n**Size:** {stat.st_size / (1024 ** 2):,.1f} MB\n**Archive:** `{name}`"
        else:
            description = "No completed Enshrouded backup archive is currently available."
        await ctx.respond(embed=discord.Embed(
            title="Enshrouded - Latest Backup", description=description,
            color=0xD97706, url=_absolute("/survival/enshrouded/"),
        ))

    @ens.command(name="whois", description="Find an online Enshrouded player")
    @discord.option("player", str, description="Player name")
    async def ens_whois(self, ctx, player: str):
        cfg = self._ens_config(ctx)
        if not cfg:
            await self._ens_unavailable(ctx); return
        from cogs.gameserver import _get_online_players
        target = " ".join(player.casefold().split())
        matches = [name for name in _get_online_players(cfg["instance_name"]) if target in name.casefold()]
        description = "\n".join(f"• {name}" for name in matches[:20]) or f"No online Enshrouded player matched `{player}`."
        await ctx.respond(embed=discord.Embed(
            title="Enshrouded - Player Lookup", description=description,
            color=0xD97706, url=_absolute("/survival/enshrouded/"),
        ))


def setup(bot):
    bot.add_cog(GameWikiCog(bot))
