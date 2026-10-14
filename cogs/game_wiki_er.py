"""Elden Ring and Elden Ring Reforged QuestLog database commands."""

from __future__ import annotations

import asyncio
import re
import time
from difflib import SequenceMatcher
from urllib.parse import quote, urlencode

import aiohttp
import discord
from discord.ext import commands
Cog = commands.Cog

from config import logger

QUESTLOG_PUBLIC_URL = "https://questlog.casual-heroes.com"


ERROR_COLOR = 0xED4245
CACHE_SECONDS = 15 * 60
MAP_LAYERS = ("lands_between", "underground", "shadow_realm")
ELDEN_RING_BOSS_API = "https://eldenring.fanapis.com/api/bosses"
BOSS_ICON_MEDIA_ID = "c50ea0523444ce124ad4"

GAMES = {
    "er": {
        "api_game": "elden_ring",
        "label": "Elden Ring",
        "color": 0xC6A75B,
        "slug": "elden-ring",
    },
    "err": {
        "api_game": "err",
        "label": "Elden Ring Reforged",
        "color": 0x8B5CF6,
        "slug": "elden-ring-reforged",
    },
}

COMMON_CATEGORIES = {
    "weapons": {"label": "weapon", "plural": "Weapons", "emoji": "⚔️", "endpoint": "weapons", "key": "weapons"},
    "armor": {"label": "armor", "plural": "Armor", "emoji": "🛡️", "endpoint": "armor", "key": "armor"},
    "talismans": {"label": "talisman", "plural": "Talismans", "emoji": "💠", "endpoint": "talismans", "key": "talismans"},
    "affinities": {"label": "affinity", "plural": "Affinities", "emoji": "🔥", "endpoint": "affinities", "key": "affinities"},
    "spells": {"label": "spell", "plural": "Magic", "emoji": "✨", "endpoint": "spells", "key": "spells"},
    "ashes": {"label": "Ash of War", "plural": "Ashes of War", "emoji": "💥", "endpoint": "aow", "key": "aow"},
    "classes": {"label": "starting class", "plural": "Starting Classes", "emoji": "🧑‍🚀", "endpoint": "classes", "key": "classes"},
    "spirits": {"label": "Spirit Ash", "plural": "Spirit Ashes", "emoji": "👻", "endpoint": "spirit-ashes", "key": "ashes"},
    "tears": {"label": "Crystal Tear", "plural": "Crystal Tears", "emoji": "🧪", "endpoint": "crystal-tears", "key": "tears"},
    "bosses": {"label": "boss", "plural": "Bosses", "emoji": "💀", "endpoint": "bosses", "key": "bosses"},
    "locations": {"label": "map location", "plural": "Locations and Map", "emoji": "🗺️", "map": True},
}

ERR_CATEGORIES = {
    "items": {"label": "quest item", "plural": "Quest Items", "emoji": "📦", "endpoint": "err/items", "key": "items", "no_game": True},
    "difficulty": {"label": "difficulty or challenge", "plural": "Difficulty and Challenges", "emoji": "🎚️", "endpoint": "err/difficulty-challenges", "transform": "difficulty", "no_game": True},
    "enkindling": {"label": "Enkindling affix", "plural": "Enkindling", "emoji": "🌟", "endpoint": "err/enkindling", "key": "affixes", "no_game": True},
    "fortunes": {"label": "Fortune", "plural": "Fortunes", "emoji": "🍀", "endpoint": "err/fortunes", "key": "fortunes", "no_game": True},
    "curios": {"label": "Curio", "plural": "Curios", "emoji": "👁️", "endpoint": "err/curios", "key": "curios", "no_game": True},
    "runeforging": {"label": "Binding Rune", "plural": "Runeforging", "emoji": "🔨", "endpoint": "err/runeforging", "key": "categories", "flatten": "binding_runes", "no_game": True},
}

CATEGORY_ALIASES = {
    "weapon": "weapons", "weapons": "weapons",
    "armour": "armor", "armor": "armor",
    "talisman": "talismans", "talismans": "talismans",
    "spell": "spells", "spells": "spells", "magic": "spells",
    "ash": "ashes", "ashes": "ashes", "aow": "ashes", "skill": "ashes", "skills": "ashes",
    "class": "classes", "classes": "classes",
    "spirit": "spirits", "spirits": "spirits", "summon": "spirits", "summons": "spirits",
    "tear": "tears", "tears": "tears", "physick": "tears",
    "boss": "bosses", "bosses": "bosses",
    "location": "locations", "locations": "locations", "map": "locations",
    "item": "items", "items": "items",
    "affinity": "affinities", "affinities": "affinities",
    "difficulty": "difficulty", "difficulties": "difficulty", "challenge": "difficulty", "challenges": "difficulty",
    "enkindling": "enkindling", "affix": "enkindling", "affixes": "enkindling",
    "fortune": "fortunes", "fortunes": "fortunes",
    "curio": "curios", "curios": "curios",
    "rune": "runeforging", "runes": "runeforging", "runeforging": "runeforging",
}

MAP_KIND_LABELS = {
    "grace": "Site of Grace", "boss": "Boss", "npc": "NPC",
    "dungeon": "Dungeon", "weapon": "Weapon", "talisman": "Talisman",
    "armor": "Armor", "spell": "Spell", "ash_of_war": "Ash of War",
    "spirit": "Spirit Ash", "golden": "Golden Seed", "tear": "Sacred Tear",
    "crystal": "Crystal Tear", "greatrune": "Great Rune",
    "larval": "Larval Tear", "memory": "Memory Stone",
    "whetblade": "Whetblade", "region": "Region", "item": "Loot",
}


def categories_for(game_key):
    categories = dict(COMMON_CATEGORIES)
    if game_key == "err":
        categories.update(ERR_CATEGORIES)
    return categories


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
    for raw_line in lines:
        line = _clean_embed_text(raw_line).strip()
        if not line:
            continue
        added = len(line) + bool(output)
        if length + added > limit:
            if output:
                output.append("...")
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


def _boss_icon_url(game_key):
    game = GAMES[game_key]["api_game"]
    return _absolute_url(
        f"/api/soulslike/site-media/{game}/icons/item/{BOSS_ICON_MEDIA_ID}.webp/"
    )


def _normalized(value):
    return " ".join(re.sub(r"[^a-z0-9]+", " ", str(value or "").casefold()).split())


def _parse_query(game_key, raw):
    value = " ".join(str(raw or "").split())[:140]
    if not value:
        return "", ""
    command, _, query = value.partition(" ")
    if command.casefold() == "commands":
        return "commands", ""
    category = CATEGORY_ALIASES.get(command.casefold())
    if category in categories_for(game_key):
        return category, query[:100]
    return "search", value[:100]


def _database_url(game_key, category=None, query=""):
    game = GAMES[game_key]
    base = f"{QUESTLOG_PUBLIC_URL.rstrip('/')}/soulslike/database/{game['slug']}/"
    if category == "locations":
        base += "map/"
        params = {"q": query} if query else {}
    else:
        params = {}
        if category:
            params["category"] = category
        if query:
            params["q"] = query
    return f"{base}?{urlencode(params)}" if params else base


def _record_url(game_key, category, record):
    name = record.get("name") or record.get("label") or ""
    if category == "weapons":
        return (
            f"{QUESTLOG_PUBLIC_URL.rstrip('/')}/soulslike/database/weapons/"
            f"{quote(name, safe='')}/?{urlencode({'game': GAMES[game_key]['api_game']})}"
        )
    if category == "bosses" and record.get("key"):
        return (
            f"{QUESTLOG_PUBLIC_URL.rstrip('/')}/soulslike/database/bosses/"
            f"{quote(str(record['key']), safe='/')}/?"
            f"{urlencode({'game': GAMES[game_key]['api_game']})}"
        )
    return _database_url(game_key, category, name)


def _number(value):
    if value is None or value == "":
        return "-"
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return f"{value:,}" if isinstance(value, (int, float)) else str(value)


def _named_values(values, names=None):
    if isinstance(values, dict):
        return ", ".join(
            f"{str(key).upper()} {_number(value)}"
            for key, value in values.items() if value not in (None, "", 0, "-", 0.0)
        )
    if isinstance(values, list):
        labels = names or ("Physical", "Strike", "Slash", "Pierce", "Magic", "Fire", "Lightning", "Holy")
        return ", ".join(
            f"{labels[index] if index < len(labels) else index + 1} {_number(value)}"
            for index, value in enumerate(values) if value not in (None, "", 0, 0.0)
        )
    return str(values or "")


def _add_field(embed, name, lines):
    if isinstance(lines, str):
        lines = [lines]
    value = _bounded_lines(lines)
    if value:
        embed.add_field(name=name, value=value, inline=False)


def _detail_fields(category, record):
    fields = []
    if category == "weapons":
        fields.append(("Details", [
            f"**Type:** {record.get('type')}" if record.get("type") else "",
            f"**Weight:** {_number(record.get('weight'))}" if record.get("weight") is not None else "",
            f"**Upgrade:** {'Somber' if record.get('is_somber') else 'Standard'} - maximum +{record.get('max_upgrade')}" if record.get("max_upgrade") is not None else "",
            f"**Skill:** {record.get('special')}" if record.get("special") else "",
        ]))
        fields.append(("Attack", [_named_values(record.get("damage"))]))
        fields.append(("Requirements", [_named_values(record.get("requirements"))]))
        fields.append(("Scaling", [_named_values(record.get("scaling"))]))
        affinities = record.get("available_affinities") or record.get("affinities") or []
        if affinities:
            fields.append(("Affinities", [", ".join(map(str, affinities))]))
    elif category == "armor":
        fields.append(("Details", [
            f"**Type:** {record.get('type')}" if record.get("type") else "",
            f"**Poise:** {_number(record.get('poise'))}" if record.get("poise") is not None else "",
            f"**Weight:** {_number(record.get('weight'))}" if record.get("weight") is not None else "",
        ]))
        fields.append(("Defense", [_named_values(record.get("defense"))]))
        fields.append(("Resistances", [_named_values(record.get("resistances"))]))
        passives = record.get("passive_effects") or []
        fields.append(("Passive effects", [f"• {item}" for item in passives] if isinstance(passives, list) else [passives]))
    elif category == "talismans":
        fields.extend((("Effect", [record.get("effect")]), ("Weight", [_number(record.get("weight"))])))
    elif category == "spells":
        fields.append(("Details", [
            f"**Type:** {record.get('type')}" if record.get("type") else "",
            f"**School:** {record.get('school')}" if record.get("school") else "",
            f"**FP:** {_number(record.get('fp_cost'))}",
            f"**Stamina:** {_number(record.get('stamina_cost'))}" if record.get("stamina_cost") is not None else "",
            f"**Slots:** {_number(record.get('slots'))}",
        ]))
        fields.append(("Requirements", [_named_values(record.get("requirements"))]))
        fields.append(("Effect", [record.get("description") or record.get("effect")]))
    elif category == "ashes":
        fields.append(("Details", [
            f"**Affinity:** {record.get('affinity')}" if record.get("affinity") else "",
            f"**FP cost:** {_number(record.get('fp_cost'))}" if record.get("fp_cost") is not None else "",
            f"**Stamina cost:** {_number(record.get('stamina_cost'))}" if record.get("stamina_cost") is not None else "",
            f"**Compatible:** {record.get('compatibility_label') or record.get('compatible') or record.get('armaments')}" if record.get("compatibility_label") or record.get("compatible") or record.get("armaments") else "",
        ]))
        fields.append(("Effect", [record.get("description") or record.get("effect")]))
        fields.append(("How to get it", [record.get("acquisition")]))
    elif category == "classes":
        fields.append(("Details", [f"**Starting level:** {_number(record.get('level'))}"]))
        fields.append(("Attributes", [
            "VIG {vigor}, MND {mind}, END {endurance}, STR {strength}, DEX {dexterity}, INT {intelligence}, FAI {faith}, ARC {arcane}".format(
                **{key: _number(record.get(key)) for key in ("vigor", "mind", "endurance", "strength", "dexterity", "intelligence", "faith", "arcane")}
            )
        ]))
        for key, label in (("starting_equipment", "Starting equipment"), ("starting_items", "Starting items"), ("starting_spells", "Starting spells")):
            values = record.get(key) or []
            if values:
                fields.append((label, [f"• {value.get('name', value) if isinstance(value, dict) else value}" for value in values]))
    elif category == "spirits":
        fields.append(("Details", [
            f"**Type:** {record.get('summon_type')}" if record.get("summon_type") else "",
            f"**FP cost:** {_number(record.get('fp_cost'))}" if record.get("fp_cost") else "",
            f"**HP cost:** {_number(record.get('hp_cost'))}" if record.get("hp_cost") else "",
        ]))
        fields.append(("Behavior", [record.get("passive_behavior"), record.get("enraged_behavior")]))
        fields.append(("How to get it", [record.get("acquisition")]))
    elif category == "tears":
        fields.extend((("Effect", [record.get("effect")]), ("Duration", [_number(record.get("duration"))]), ("Where to find it", [record.get("location")])))
    elif category == "bosses":
        fields.append(("Encounter", [
            f"**Region:** {record.get('region')}" if record.get("region") else "",
            f"**Location:** {record.get('location')}" if record.get("location") else "",
            f"**Tier:** {str(record.get('tier')).replace('_', ' ').title()}" if record.get("tier") else "",
        ]))
        fields.append(("Rewards", [f"• {reward}" for reward in record.get("rewards") or []]))
    elif category == "items":
        fields.extend((("Description", [record.get("description")]), ("Details", [f"**Maximum held:** {_number(record.get('max_quantity'))}" if record.get("max_quantity") is not None else ""])))
    elif category == "affinities":
        fields.append(("Details", [
            f"**Whetblade:** {record.get('whetblade')}" if record.get("whetblade") else "",
            f"**Scaling:** {record.get('scaling_stat')}" if record.get("scaling_stat") else "",
            record.get("damage_type_change"), record.get("effect"),
        ]))
    elif category == "difficulty":
        if record.get("record_type") == "challenge":
            fields.append(("Summary", [record.get("summary")]))
            fields.append(("Rules", [f"• {value}" for value in record.get("details") or []]))
            fields.append(("Calculation", [record.get("calculation_effect")]))
        else:
            fields.append(("Status buildup", [
                f"**Received:** x{_number(record.get('status_buildup_received'))}",
                f"**Change:** {_number(record.get('status_percent_change'))}%",
                record.get("behavior_note"),
            ]))
            fields.append(("Affected statuses", [", ".join(record.get("affected_statuses") or [])]))
    elif category == "enkindling":
        fields.append(("Affinity", [record.get("affinity")]))
        fields.append(("Tiers", [f"• {tier.get('star')} star - {tier.get('text')}" for tier in record.get("tiers") or []]))
    elif category == "fortunes":
        fields.append(("Description", [record.get("description")]))
        fields.append(("Benefits", [f"• {value}" for value in record.get("buffs") or []] + [record.get("unique_effects")]))
        fields.append(("Drawbacks", [f"• {value}" for value in record.get("drawbacks") or []]))
        fields.append(("How to unlock", [record.get("how_to_unlock")]))
        fields.append(("Minor Fortune", [record.get("minor_effects")]))
    elif category == "curios":
        fields.append(("Trigger", [record.get("trigger")]))
        fields.append(("Choices", [f"• {choice.get('name')} - {choice.get('description')}" for choice in record.get("choices") or []]))
        fields.append(("How to get it", [record.get("acquisition")]))
    elif category == "runeforging":
        fields.append(("Effect", [record.get("effect")]))
        fields.append(("Details", [
            f"**Great Rune:** {record.get('great_rune')}" if record.get("great_rune") else "",
            f"**Category:** {record.get('rune_category')}" if record.get("rune_category") else "",
            f"**Maximum forge level:** {_number(record.get('max_forge_level'))}" if record.get("max_forge_level") is not None else "",
            "New Game Plus only" if record.get("ng_plus_only") else "",
        ]))
    return fields


def build_record_embed(game_key, category, record, placements=None):
    game = GAMES[game_key]
    meta = categories_for(game_key)[category]
    name = record.get("name") or record.get("label") or meta["plural"]
    embed = discord.Embed(
        title=_clean_embed_text(f"{meta['emoji']} {name}"),
        description=_clean_embed_text(record.get("description") or f"{game['label']} {meta['label']} data."),
        color=game["color"],
        url=_record_url(game_key, category, record),
    )
    for field_name, lines in _detail_fields(category, record):
        _add_field(embed, field_name, lines)

    placements = placements or []
    if placements and category != "bosses":
        location_lines = []
        for placement in placements[:6]:
            detail = placement.get("description") or MAP_KIND_LABELS.get(placement.get("kind"), "Map marker")
            location_lines.append(f"• {detail} - [Open map]({placement['map_url']})")
        if len(placements) > 6:
            location_lines.append(f"• + {len(placements) - 6} more on QuestLog")
        _add_field(embed, "Where to find it", location_lines)
        if not record.get("image_url"):
            record = {**record, "image_url": next((row.get("image_url") for row in placements if row.get("image_url")), "")}
    if category == "locations":
        loot = record.get("loot") or []
        _add_field(embed, "Loot", [f"• {item.get('name')}" for item in loot[:12]])
        _add_field(embed, "Map", [f"[Open this location on the live map]({record.get('map_url')})"])
    elif category == "bosses" and placements:
        boss_placements = [row for row in placements if row.get("kind") == "boss"] or placements
        links = []
        seen_links = set()
        for placement in boss_placements:
            map_url = placement.get("map_url")
            if not map_url or map_url in seen_links:
                continue
            seen_links.add(map_url)
            links.append(f"• [Open encounter on the live map]({map_url})")
        _add_field(embed, "Map", links[:4])
    image_url = _boss_icon_url(game_key) if category == "bosses" else _absolute_url(record.get("image_url"))
    if image_url:
        embed.set_thumbnail(url=image_url)
    embed.set_footer(text=f"QuestLog - {game['label']} Data")
    return embed


def build_category_fallback(game_key, category):
    game = GAMES[game_key]
    meta = categories_for(game_key)[category]
    embed = discord.Embed(
        title=f"{meta['emoji']} {game['label']} {meta['plural']}",
        description=(
            f"Use `!{game_key} {category} <name>` to look up a {meta['label']}.\n\n"
            f"[Browse {meta['plural'].lower()} on QuestLog]({_database_url(game_key, category)})"
        ),
        color=game["color"],
    )
    embed.set_footer(text=f"QuestLog - {game['label']} Data")
    return embed


def build_help_embed(game_key):
    game = GAMES[game_key]
    embed = discord.Embed(
        title=f"⚔️ {game['label']} commands",
        description=(
            f"Search all QuestLog {game['label']} data with `!{game_key} <name>`.\n"
            "Add a category only when you want to filter the search."
        ),
        color=game["color"],
        url=_database_url(game_key),
    )
    embed.add_field(name="🔎 Search anything", value=(
        f"`!{game_key} uchigatana` - finds the weapon\n"
        f"`!{game_key} comet azur` - finds the spell\n"
        f"`!{game_key} malenia` - finds the boss\n"
        f"`!{game_key} limgrave` - finds the map location"
    ), inline=False)
    embed.add_field(name="⚔️ Equipment and combat", value=(
        f"`!{game_key} weapons <name>` - stats, image, skill, and map locations\n"
        f"`!{game_key} armor <name>` - defenses, resistance, image, and map locations\n"
        f"`!{game_key} talismans <name>` - effects, image, and map locations\n"
        f"`!{game_key} affinities <name>` - damage types and supported scaling\n"
        f"`!{game_key} spells <name>` - magic costs, requirements, image, and map locations\n"
        f"`!{game_key} ashes <name>` - Ashes of War, costs, compatibility, and map locations"
    ), inline=False)
    embed.add_field(name="🧭 Journey and world", value=(
        f"`!{game_key} classes <name>` - starting stats and gear\n"
        f"`!{game_key} spirits <name>` - summon costs, behavior, and acquisition\n"
        f"`!{game_key} tears <name>` - Crystal Tear effects and locations\n"
        f"`!{game_key} bosses <name>` - encounter and region\n"
        f"`!{game_key} locations <name>` - map markers, nearby loot, and live map"
    ), inline=False)
    if game_key == "err":
        embed.add_field(name="🟣 Reforged systems", value=(
            "`!err items <name>` - Reforged quest items\n"
            "`!err difficulty <name>` - difficulty modes and challenge options\n"
            "`!err enkindling <name>` - affixes and rarity tiers\n"
            "`!err fortunes <name>` - benefits, drawbacks, and unlocks\n"
            "`!err curios <name>` - triggers, choices, and acquisition\n"
            "`!err runeforging <name>` - Binding Runes and forge rules"
        ), inline=False)
    embed.add_field(name="Quick tips", value=(
        f"• `!{game_key} <name>` searches every supported category.\n"
        "• Add a category to filter an ambiguous search.\n"
        "• Run a category without a name to open its QuestLog page.\n"
        f"• Use `!{game_key} commands` to show this guide again."
    ), inline=False)
    embed.set_footer(text=f"QuestLog - {game['label']} Data")
    return embed


class EldenRingCog(Cog):
    def __init__(self, bot):
        self.bot = bot
        self._cache = {}

    async def _request(self, path, params=None):
        timeout = aiohttp.ClientTimeout(total=15)
        url = f"{QUESTLOG_PUBLIC_URL.rstrip('/')}/{path.lstrip('/')}"
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params or {}) as response:
                return response.status, await response.json(content_type=None)

    async def _public_request(self, url, params=None):
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params or {}) as response:
                return response.status, await response.json(content_type=None)

    async def _cached(self, key, loader):
        cached = self._cache.get(key)
        now = time.monotonic()
        if cached and now - cached[0] < CACHE_SECONDS:
            return cached[1]
        value = await loader()
        self._cache[key] = (now, value)
        return value

    async def _catalog(self, game_key, category):
        meta = categories_for(game_key)[category]
        if meta.get("map"):
            return await self._map_catalog(game_key)

        async def load():
            params = {"limit": 1000}
            if not meta.get("no_game"):
                params["game"] = GAMES[game_key]["api_game"]
            if category == "bosses":
                params.update({"game": "elden_ring", "mode": "err" if game_key == "err" else "vanilla"})
            status, data = await self._request(f"api/soulslike/{meta['endpoint']}/", params)
            if status != 200:
                raise RuntimeError(f"{category} returned HTTP {status}")
            if meta.get("transform") == "difficulty":
                rows = [
                    {**row, "name": row.get("label"), "record_type": "difficulty"}
                    for row in data.get("difficulty_modes") or []
                ] + [
                    {**row, "name": row.get("label"), "record_type": "challenge"}
                    for row in data.get("challenges") or []
                ]
            else:
                rows = data.get(meta["key"]) or []
            if meta.get("flatten"):
                flattened = []
                for group in rows:
                    for row in group.get(meta["flatten"]) or []:
                        flattened.append({
                            **row,
                            "great_rune": group.get("great_rune"),
                            "rune_category": group.get("category"),
                            "cost_pieces": group.get("cost_pieces"),
                        })
                rows = flattened
            return [row for row in rows if isinstance(row, dict) and row.get("name")]

        return await self._cached((game_key, category), load)

    async def _map_catalog(self, game_key):
        async def load():
            game = GAMES[game_key]["api_game"]
            calls = [self._request(f"api/soulslike/site-map/{game}/", {"layer": layer}) for layer in MAP_LAYERS]
            results = await asyncio.gather(*calls, return_exceptions=True)
            markers = []
            for layer, result in zip(MAP_LAYERS, results):
                if isinstance(result, Exception):
                    logger.warning("%s map layer %s failed: %s", game_key.upper(), layer, result)
                    continue
                status, data = result
                if status != 200:
                    continue
                for marker in data.get("markers") or []:
                    if not isinstance(marker, dict):
                        continue
                    presented = dict(marker)
                    presented["name"] = marker.get("label")
                    presented["map_url"] = self._map_url(game_key, marker)
                    markers.append(presented)
            if not markers:
                raise RuntimeError("map catalog unavailable")
            return markers

        return await self._cached((game_key, "_map"), load)

    @staticmethod
    def _map_url(game_key, marker):
        params = {
            "layer": marker.get("layer") or "lands_between",
            "x": round(float(marker.get("x") or 0)),
            "y": round(float(marker.get("y") or 0)),
            "zoom": 6,
            "q": marker.get("label") or "",
        }
        return f"{_database_url(game_key, 'locations').split('?')[0]}?{urlencode(params)}"

    async def _placements(self, game_key, name):
        try:
            markers = await self._map_catalog(game_key)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, RuntimeError):
            return []
        target = _normalized(name)
        found = []
        seen = set()
        for marker in markers:
            marker_match = _normalized(marker.get("label")) == target
            loot_match = any(_normalized(item.get("name")) == target for item in marker.get("loot") or [])
            if not marker_match and not loot_match:
                continue
            key = marker.get("id") or (marker.get("layer"), marker.get("x"), marker.get("y"))
            if key in seen:
                continue
            seen.add(key)
            found.append(marker)
        return found

    async def _boss_drop_catalog(self):
        """Load names only; QuestLog's map remains the placement authority."""
        async def load():
            status, data = await self._public_request(
                ELDEN_RING_BOSS_API, {"limit": 200, "page": 0},
            )
            if status != 200:
                raise RuntimeError(f"boss reward catalog returned HTTP {status}")
            return [
                row for row in data.get("data") or []
                if isinstance(row, dict) and row.get("name")
            ]

        return await self._cached(("er", "_boss_drops"), load)

    async def _boss_rewards(self, game_key, record, placements):
        rewards = []
        seen = set()

        def add(value):
            value = _clean_embed_text(value).strip()
            key = _normalized(value)
            if value and key not in seen:
                seen.add(key)
                rewards.append(value)

        for placement in placements:
            description = str(placement.get("description") or "")
            rune_match = re.search(r"([\d,.]+)\s+runes?", description, re.IGNORECASE)
            if rune_match:
                add(f"{rune_match.group(1)} runes")
            for item in placement.get("loot") or []:
                if item.get("name"):
                    add(item["name"])

        # The map stores boss rune rewards on the boss marker and item rewards
        # as separate Event reward markers. Use the public boss catalog only as
        # a name whitelist, then require matching QuestLog map evidence near the
        # selected encounter before showing an item as a drop.
        try:
            catalog = await self._boss_drop_catalog()
            exact = next(
                (row for row in catalog if _normalized(row.get("name")) == _normalized(record.get("name"))),
                None,
            )
            allowed_items = {
                _normalized(value) for value in (exact or {}).get("drops") or []
                if "rune" not in str(value).casefold()
            }
            if allowed_items and placements:
                markers = await self._map_catalog(game_key)
                for marker in markers:
                    if (
                        _normalized(marker.get("label")) not in allowed_items
                        or "event reward" not in str(marker.get("description") or "").casefold()
                    ):
                        continue
                    for boss_marker in placements:
                        if marker.get("layer") != boss_marker.get("layer"):
                            continue
                        distance = (
                            (float(marker.get("x") or 0) - float(boss_marker.get("x") or 0)) ** 2
                            + (float(marker.get("y") or 0) - float(boss_marker.get("y") or 0)) ** 2
                        ) ** 0.5
                        if distance <= 30:
                            add(marker.get("label"))
                            break
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, RuntimeError) as error:
            logger.warning("Boss item reward enrichment failed for %r: %s", record.get("name"), error)
        return rewards

    async def _reply_record(self, ctx, game_key, category, record):
        placements = [] if category == "locations" else await self._placements(game_key, record.get("name"))
        if category == "bosses":
            record = {**record, "rewards": await self._boss_rewards(game_key, record, placements)}
        await ctx.reply(embed=build_record_embed(game_key, category, record, placements))

    async def _lookup(self, ctx, game_key, category, query):
        if not query:
            await ctx.reply(embed=build_category_fallback(game_key, category))
            return
        if len(query) < 2:
            await ctx.reply(f"Enter at least two characters for the {categories_for(game_key)[category]['label']} name.")
            return
        try:
            rows = await self._catalog(game_key, category)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, RuntimeError) as error:
            logger.warning("%s %s lookup failed for %r: %s", game_key.upper(), category, query, error)
            await self._unavailable(ctx, game_key)
            return
        matches = self._rank(rows, query)
        exact = [row for row in matches if _normalized(row.get("name")) == _normalized(query)]
        if len(exact) == 1:
            record = exact[0]
            await self._reply_record(ctx, game_key, category, record)
            return
        if not exact and len(matches) == 1 and _normalized(query) in _normalized(matches[0].get("name")):
            record = matches[0]
            await self._reply_record(ctx, game_key, category, record)
            return
        await self._results(ctx, game_key, query, [(category, row) for row in (exact or matches[:8])])

    @staticmethod
    def _rank(rows, query):
        target = _normalized(query)
        scored = []
        for row in rows:
            name = _normalized(row.get("name"))
            if not name:
                continue
            ratio = SequenceMatcher(None, target, name).ratio()
            if name == target:
                score = 4
            elif name.startswith(target):
                score = 3 + ratio
            elif target in name:
                score = 2 + ratio
            elif ratio >= 0.62:
                score = ratio
            else:
                continue
            scored.append((score, row))
        return [row for _, row in sorted(scored, key=lambda value: (-value[0], str(value[1].get("name")).casefold()))]

    async def _search(self, ctx, game_key, query):
        if len(query) < 2:
            await ctx.reply(f"Enter at least two characters to search {GAMES[game_key]['label']} data.")
            return
        categories = categories_for(game_key)
        results = await asyncio.gather(
            *(self._catalog(game_key, category) for category in categories),
            return_exceptions=True,
        )
        available = 0
        matches = []
        for category, result in zip(categories, results):
            if isinstance(result, Exception):
                logger.warning("%s universal %s catalog failed: %s", game_key.upper(), category, result)
                continue
            available += 1
            matches.extend((category, row) for row in self._rank(result, query)[:8])
        if not available:
            await self._unavailable(ctx, game_key)
            return

        target = _normalized(query)
        exact = [(category, row) for category, row in matches if _normalized(row.get("name")) == target]
        if exact:
            non_map = [match for match in exact if match[0] != "locations"]
            exact = non_map or exact
        unique_exact = {(category, _normalized(row.get("name"))) for category, row in exact}
        if len(unique_exact) == 1:
            category, record = exact[0]
            await self._reply_record(ctx, game_key, category, record)
            return

        if exact:
            await self._results(ctx, game_key, query, exact)
            return

        boss_matches = []
        boss_names = set()
        for category, record in matches:
            name = _normalized(record.get("name"))
            if category != "bosses" or not name.startswith(target) or name in boss_names:
                continue
            boss_names.add(name)
            boss_matches.append(record)
        if len(boss_matches) == 1:
            record = boss_matches[0]
            await self._reply_record(ctx, game_key, "bosses", record)
            return

        # A short player-facing name such as "Malenia" should open the one
        # matching catalog record even when its canonical name includes a
        # title. Ignore map duplicates here; the selected record is enriched
        # with those placements below.
        contained = []
        contained_keys = set()
        for category, record in matches:
            name = _normalized(record.get("name"))
            key = (category, name)
            if category == "locations" or target not in name or key in contained_keys:
                continue
            contained_keys.add(key)
            contained.append((category, record))
        if len(contained) == 1:
            category, record = contained[0]
            await self._reply_record(ctx, game_key, category, record)
            return

        ranked = sorted(matches, key=lambda item: SequenceMatcher(None, target, _normalized(item[1].get("name"))).ratio(), reverse=True)
        await self._results(ctx, game_key, query, ranked[:8])

    async def _results(self, ctx, game_key, query, matches):
        game = GAMES[game_key]
        lines = []
        seen = set()
        for category, record in matches:
            name = record.get("name")
            key = (category, _normalized(name))
            if not name or key in seen:
                continue
            seen.add(key)
            lines.append(f"• `/{game_key} search name:{name} category:{category}`")
            if len(lines) == 8:
                break
        description = f"No exact {game['label']} match was found for `{_clean_embed_text(query)}`."
        if lines:
            description += "\n\nClosest matches:\n" + "\n".join(lines)
            description += "\n\nAdd a category to filter the search."
        await ctx.reply(embed=discord.Embed(
            title=f"{game['label']} search results",
            description=_bounded_lines([description], 1800),
            color=game["color"],
            url=_database_url(game_key, query=query),
        ))

    async def _unavailable(self, ctx, game_key):
        game = GAMES[game_key]
        await ctx.reply(embed=discord.Embed(
            title=f"{game['label']} lookup unavailable",
            description=f"QuestLog could not load the {game['label']} data right now. Please try again shortly.",
            color=ERROR_COLOR,
        ))

    async def _command(self, ctx, game_key, lookup):
        category, query = _parse_query(game_key, lookup)
        if not category or category == "commands":
            await ctx.reply(embed=build_help_embed(game_key))
        elif category == "search":
            await self._search(ctx, game_key, query)
        else:
            await self._lookup(ctx, game_key, category, query)

    async def er(self, ctx, *, lookup: str = ""):
        """!er <name> or !er <category> <name> - search Elden Ring data."""
        await self._command(ctx, "er", lookup)

    async def err(self, ctx, *, lookup: str = ""):
        """!err <name> or !err <category> <name> - search Elden Ring Reforged data."""
        await self._command(ctx, "err", lookup)
