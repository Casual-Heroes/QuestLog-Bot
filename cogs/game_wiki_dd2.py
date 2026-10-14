"""Dragon's Dogma 2 lookups backed by QuestLog's public authority."""

from __future__ import annotations

import asyncio

import aiohttp
import discord
from discord.ext import commands
Cog = commands.Cog

from config import logger

QUESTLOG_PUBLIC_URL = "https://questlog.casual-heroes.com"


DD2_COLOR = 0xC8A85A
ERROR_COLOR = 0xED4245
DD2_BASE_PATH = "/soulslike/database/dragons-dogma-2"

CATEGORIES = {
    "enemy": {"label": "enemy", "plural": "Enemies", "emoji": "🐉", "path": "enemy"},
    "boss": {"label": "boss", "plural": "Bosses", "emoji": "💀", "path": "boss"},
    "weapons": {"label": "weapon", "plural": "Weapons", "emoji": "⚔️", "path": "?category=weapons&view=detailed"},
    "armor": {"label": "armor", "plural": "Armor", "emoji": "🛡️", "path": "armor"},
    "rings": {"label": "ring", "plural": "Rings", "emoji": "💍", "path": "ring"},
    "vocations": {"label": "vocation", "plural": "Vocations", "emoji": "🏹", "path": "vocation"},
    "quests": {"label": "quest", "plural": "Quests", "emoji": "📜", "path": "quest"},
    "skills": {"label": "skill", "plural": "Skills", "emoji": "✨", "path": "skill"},
    "items": {"label": "item or material", "plural": "Items and materials", "emoji": "🎒", "path": "item"},
    "merchants": {"label": "merchant", "plural": "Merchants", "emoji": "🏪", "path": "merchant"},
    "pawn": {"label": "Pawn inclination or specialization", "plural": "Pawn guides", "emoji": "♟️", "path": "pawn_inclination"},
    "locations": {"label": "location", "plural": "Locations", "emoji": "🗺️", "path": "location"},
}

CATEGORY_ALIASES = {
    "enemy": "enemy", "enemies": "enemy", "boss": "boss", "bosses": "boss",
    "weapon": "weapons", "weapons": "weapons", "armour": "armor", "armor": "armor",
    "ring": "rings", "rings": "rings", "vocation": "vocations", "vocations": "vocations",
    "quest": "quests", "quests": "quests", "skill": "skills", "skills": "skills",
    "item": "items", "items": "items", "material": "items", "materials": "items",
    "merchant": "merchants", "merchants": "merchants", "pawn": "pawn",
    "location": "locations", "locations": "locations",
}


def _number(value):
    return f"{value:,.0f}" if isinstance(value, (int, float)) else "-"


def _clean_embed_text(value):
    return (
        str(value or "")
        .replace(" — ", " - ")
        .replace(" – ", " - ")
        .replace(" · ", " - ")
        .replace("—", "-")
        .replace("–", "-")
    )


def _bounded_lines(lines, limit=1024):
    output = []
    length = 0
    for line in lines:
        line = _clean_embed_text(line).strip()
        if not line:
            continue
        added = len(line) + (1 if output else 0)
        if length + added > limit:
            if output:
                output.append("…")
            break
        output.append(line)
        length += added
    return "\n".join(output)


def _absolute_url(value):
    value = str(value or "").strip()
    if not value:
        return ""
    if value.startswith(("https://", "http://")):
        return value
    return f"{QUESTLOG_PUBLIC_URL.rstrip('/')}/{value.lstrip('/')}"


def _category_url(category):
    path = CATEGORIES[category]["path"]
    if path.startswith("?"):
        return f"{QUESTLOG_PUBLIC_URL.rstrip('/')}{DD2_BASE_PATH}/{path}"
    return f"{QUESTLOG_PUBLIC_URL.rstrip('/')}{DD2_BASE_PATH}/{path}/"


def _parse_dd2_query(raw):
    value = " ".join(str(raw or "").split())[:140]
    if not value:
        return "", ""
    command, _, query = value.partition(" ")
    if command.casefold() == "commands":
        return "commands", ""
    category = CATEGORY_ALIASES.get(command.casefold())
    if category:
        return category, query[:100]
    return "search", value[:100]


def _combat_lines(data):
    combat = data.get("combat") or {}
    hp = combat.get("hp")
    if combat.get("health_varies") and combat.get("health_min") is not None:
        hp_label = f"{_number(combat.get('health_min'))}-{_number(combat.get('health_max'))}"
    else:
        hp_label = _number(hp)
    lines = []
    if hp_label != "-":
        lines.append(f"**HP:** {hp_label}")
    defenses = []
    if combat.get("physical_defense") is not None:
        defenses.append(f"Physical {_number(combat['physical_defense'])}")
    if combat.get("magick_defense") is not None:
        defenses.append(f"Magick {_number(combat['magick_defense'])}")
    if defenses:
        lines.append(f"**Defense:** {' · '.join(defenses)}")
    weaknesses = data.get("weaknesses") or []
    resistances = data.get("resistances") or []
    if weaknesses:
        lines.append(f"**Weak to:** {', '.join(weaknesses)}")
    if resistances:
        lines.append(f"**Resists:** {', '.join(resistances)}")
    return lines


def _drop_lines(data):
    lines = []
    seen = set()
    for drop in data.get("drops") or []:
        key = (
            str(drop.get("name") or "").casefold(),
            drop.get("rate"), drop.get("quantity"),
        )
        if key in seen:
            continue
        seen.add(key)
        details = []
        if isinstance(drop.get("rate"), (int, float)):
            details.append(f"{drop['rate']:g}%")
        if isinstance(drop.get("quantity"), int):
            details.append(f"×{drop['quantity']}")
        suffix = f" - {' - '.join(details)}" if details else ""
        lines.append(f"• {drop.get('name', 'Item')}{suffix}")
    return lines


def build_dd2_embed(data):
    """Build the enemy/boss encounter embed used by both command forms."""
    placement_count = int(data.get("placement_count") or 0)
    entity_type = "boss" if data.get("entity_type") == "boss" else "enemy"
    meta = CATEGORIES[entity_type]
    embed = discord.Embed(
        title=_clean_embed_text(
            f"{meta['emoji']} {data.get('name', 'Dragon’s Dogma 2 encounter')}"
        ),
        description="Dragon's Dogma 2 encounter data.",
        color=DD2_COLOR,
        url=data.get("detail_url") or None,
    )
    combat_lines = _combat_lines(data)
    if combat_lines:
        embed.add_field(name="Combat", value=_bounded_lines(combat_lines), inline=False)
    drop_lines = _drop_lines(data)
    if drop_lines:
        embed.add_field(name="Drops", value=_bounded_lines(drop_lines), inline=False)
    locations = data.get("locations") or []
    if locations or placement_count:
        location_lines = []
        if locations:
            location_lines.append(", ".join(locations))
        if placement_count:
            location_lines.append(
                f"**{placement_count:,} verified map placement"
                f"{'s' if placement_count != 1 else ''}** - [Open live map]({data.get('map_url')})"
            )
        embed.add_field(name="Where to find it", value=_bounded_lines(location_lines), inline=False)
    if data.get("map_image_url"):
        embed.set_image(url=data["map_image_url"])
    if data.get("icon_url"):
        embed.set_thumbnail(url=data["icon_url"])
    embed.set_footer(text="QuestLog - Dragon's Dogma 2 Data")
    return embed


def build_dd2_record_embed(data, category):
    meta = CATEGORIES[category]
    description = str(data.get("description") or "").strip()
    if category == "quests":
        description = description or "Quest objective from Dragon's Dogma 2."
    map_url = _absolute_url(data.get("map_url"))
    primary_url = (
        map_url
        if category == "locations" and map_url
        else _absolute_url(data.get("detail_url"))
    )
    embed = discord.Embed(
        title=_clean_embed_text(f"{meta['emoji']} {data.get('name', meta['plural'])}"),
        description=_bounded_lines([description], 700) or f"Dragon's Dogma 2 {meta['label']} data.",
        color=DD2_COLOR,
        url=primary_url or None,
    )
    field_labels = {
        "details": "Details", "stats": "Stats", "acquisition": "Where to find it",
        "objectives": "Objective", "inventory": "Sells", "locations": "Where to find them",
        "weapon_skills": "Weapon skills", "core_skills": "Core skills",
        "maister_skills": "Maister skills",
        "dark_arisen_skills": "Dark Arisen skills",
        "augments": "Augments", "starting_equipment": "Starting equipment",
        "monsters": "Nearby enemies", "bosses": "Nearby bosses",
        "loot": "Nearby treasure", "npcs": "NPCs",
        "shops": "Merchants", "nearby": "Nearby",
    }
    order = (
        "objectives", "details", "weapon_skills", "maister_skills",
        "dark_arisen_skills", "core_skills", "augments",
        "starting_equipment", "stats", "acquisition", "locations", "inventory",
        "monsters", "bosses", "loot", "npcs", "shops", "nearby",
    )
    for key in order:
        values = data.get(key) or []
        if not isinstance(values, list):
            continue
        lines = [f"• {value}" for value in values if str(value).strip()]
        if lines:
            embed.add_field(name=field_labels[key], value=_bounded_lines(lines), inline=False)
    if category == "locations" and map_url:
        embed.add_field(name="Map", value=f"[Open this location on the live map]({map_url})", inline=False)
    if data.get("icon_url"):
        embed.set_thumbnail(url=_absolute_url(data["icon_url"]))
    embed.set_footer(text="QuestLog - Dragon's Dogma 2 Data")
    return embed


def build_category_fallback(category):
    meta = CATEGORIES[category]
    command = f"!dd2 {category} <name>"
    embed = discord.Embed(
        title=f"{meta['emoji']} Dragon's Dogma 2 {meta['plural']}",
        description=(
            f"Use `{command}` to look up a {meta['label']}.\n\n"
            f"[Browse all {meta['plural'].lower()} on QuestLog]({_category_url(category)})"
        ),
        color=DD2_COLOR,
    )
    if category == "pawn":
        specializations = f"{QUESTLOG_PUBLIC_URL.rstrip('/')}{DD2_BASE_PATH}/pawn_specialization/"
        embed.description = (
            "Use `!dd2 pawn <name>` to look up a Pawn inclination or specialization.\n\n"
            f"[Browse Pawn inclinations]({_category_url(category)}) - "
            f"[Browse Pawn specializations]({specializations})"
        )
    embed.set_footer(text="QuestLog - Dragon's Dogma 2 Data")
    return embed


def build_dd2_help_embed():
    embed = discord.Embed(
        title="🐉 Dragon's Dogma 2 commands",
        description=(
            "Search all QuestLog Dragon's Dogma 2 data with `!dd2 <name>`.\n"
            "Add a category only when you want to filter the search."
        ),
        color=DD2_COLOR,
        url=f"{QUESTLOG_PUBLIC_URL.rstrip('/')}{DD2_BASE_PATH}/",
    )
    embed.add_field(
        name="🔎 Search anything",
        value=(
            "`!dd2 cyclops` - finds the Cyclops boss\n"
            "`!dd2 goblin` - finds the Goblin enemy\n"
            "`!dd2 trusty sword` - finds the weapon\n"
            "`!dd2 cloudfall strike` - finds the skill"
        ),
        inline=False,
    )
    embed.add_field(
        name="🐲 Encounter filters",
        value=(
            "`!dd2 enemy <name>` - search only enemies\n"
            "`!dd2 boss <name>` - search only bosses"
        ),
        inline=False,
    )
    embed.add_field(
        name="⚔️ Equipment",
        value=(
            "`!dd2 weapons <name>` - image, stats, sellers, and locations\n"
            "`!dd2 armor <name>` - image, defenses, sellers, and locations\n"
            "`!dd2 rings <name>` - image, effects, sellers, and locations"
        ),
        inline=False,
    )
    embed.add_field(
        name="🏹 Builds and Pawns",
        value=(
            "`!dd2 vocations <name>` - vocation details and availability\n"
            "`!dd2 skills <name>` - skills, augments, and unlock methods\n"
            "`!dd2 pawn <name>` - Pawn inclinations and specializations"
        ),
        inline=False,
    )
    embed.add_field(
        name="🗺️ World and adventure",
        value=(
            "`!dd2 quests <name>` - spoiler-safe objective with full guide link\n"
            "`!dd2 items <name>` - items, materials, drops, sellers, and locations\n"
            "`!dd2 merchants <name>` - inventory, prices, and location\n"
            "`!dd2 locations <name>` - enemies, bosses, loot, NPCs, merchants, and map"
        ),
        inline=False,
    )
    embed.add_field(
        name="Quick tips",
        value=(
            "• `!dd2 <name>` searches every supported DD2 category.\n"
            "• Add a category to filter an ambiguous or broad search.\n"
            "• Run a category without a name to open its QuestLog browse link.\n"
            "• Use `!dd2 commands` to show this guide again."
        ),
        inline=False,
    )
    embed.set_footer(text="QuestLog - Dragon's Dogma 2 Data")
    return embed


class DD2Cog(Cog):
    def __init__(self, bot):
        self.bot = bot

    async def _request(self, url, query):
        timeout = aiohttp.ClientTimeout(total=12)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params={"q": query}) as response:
                return response.status, await response.json(content_type=None)

    async def _lookup(self, ctx, category, query):
        if not query:
            await ctx.reply(embed=build_category_fallback(category))
            return
        if len(query) < 2:
            await ctx.reply(f"Enter at least two characters for the {CATEGORIES[category]['label']} name.")
            return
        endpoint = "enemy" if category in {"enemy", "boss"} else f"embed/{category}"
        url = f"{QUESTLOG_PUBLIC_URL.rstrip('/')}/api/soulslike/dragons-dogma-2/{endpoint}/"
        try:
            status, data = await self._request(url, query)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as error:
            logger.warning("DD2 %s lookup failed for %r: %s", category, query, error)
            embed = discord.Embed(
                title="DD2 lookup unavailable",
                description="QuestLog could not load the DD2 data right now. Please try again shortly.",
                color=ERROR_COLOR,
            )
            await ctx.reply(embed=embed)
            return
        if status != 200 or (category == "boss" and data.get("entity_type") != "boss"):
            suggestions = data.get("suggestions") or []
            suggestion_text = (
                f"\n\nTry: {', '.join(f'`{value}`' for value in suggestions[:6])}"
                if suggestions else ""
            )
            embed = discord.Embed(
                title=f"DD2 {CATEGORIES[category]['label']} not found",
                description=f"{data.get('error', 'No matching record was found.')}{suggestion_text}",
                color=ERROR_COLOR,
                url=_category_url(category),
            )
            await ctx.reply(embed=embed)
            return
        embed = build_dd2_embed(data) if category in {"enemy", "boss"} else build_dd2_record_embed(data, category)
        await ctx.reply(embed=embed)

    async def _search(self, ctx, query):
        if len(query) < 2:
            await ctx.reply("Enter at least two characters to search DD2 data.")
            return
        url = f"{QUESTLOG_PUBLIC_URL.rstrip('/')}/api/soulslike/dragons-dogma-2/lookup/"
        try:
            status, data = await self._request(url, query)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as error:
            logger.warning("DD2 universal lookup failed for %r: %s", query, error)
            await ctx.reply(embed=discord.Embed(
                title="DD2 lookup unavailable",
                description="QuestLog could not load the DD2 data right now. Please try again shortly.",
                color=ERROR_COLOR,
            ))
            return
        category = data.get("category")
        if status == 200 and category in CATEGORIES:
            await self._lookup(ctx, category, data.get("name") or query)
            return
        choices = data.get("matches") or data.get("suggestions") or []
        lines = []
        for choice in choices[:8]:
            if not isinstance(choice, dict):
                continue
            choice_category = choice.get("category")
            name = choice.get("name")
            if choice_category in CATEGORIES and name:
                lines.append(f"• `/dd2 search name:{name} category:{choice_category}`")
        description = data.get("error") or "No matching DD2 record was found."
        if lines:
            description += "\n\n" + "\n".join(lines)
            description += "\n\nAdd a category to filter the search."
        await ctx.reply(embed=discord.Embed(
            title="DD2 search results",
            description=_bounded_lines([description], 1800),
            color=ERROR_COLOR,
            url=f"{QUESTLOG_PUBLIC_URL.rstrip('/')}{DD2_BASE_PATH}/",
        ))

    async def dd2(self, ctx, *, lookup: str = ""):
        """!dd2 <name> or !dd2 <category> <name> - search DD2 data."""
        category, query = _parse_dd2_query(lookup)
        if not category or category == "commands":
            await ctx.reply(embed=build_dd2_help_embed())
            return
        if category == "search":
            await self._search(ctx, query)
            return
        await self._lookup(ctx, category, query)
