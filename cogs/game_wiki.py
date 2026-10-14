"""Native Discord slash commands for QuestLog's supported game databases."""

from __future__ import annotations

import discord
from discord.ext import commands

from config import logger
from cogs.game_wiki_dd2 import CATEGORIES as DD2_CATEGORIES, DD2Cog
from cogs.game_wiki_er import EldenRingCog, categories_for
from cogs.game_wiki_hubs import GAMES as HUB_GAMES, GameHubsCog


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


def setup(bot):
    bot.add_cog(GameWikiCog(bot))
