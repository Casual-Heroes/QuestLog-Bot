"""QuestLog lookups for the remaining game databases and tool hubs."""

from __future__ import annotations

import asyncio
import html
import json
import re
import time
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from urllib.parse import quote, urlencode, urlparse

import aiohttp
import discord
from discord.ext import commands
Cog = commands.Cog
from sqlalchemy import text

from config import db_session_scope, logger

QUESTLOG_PUBLIC_URL = "https://questlog.casual-heroes.com"


CACHE_SECONDS = 15 * 60
ERROR_COLOR = 0xED4245

GAMES = {
    "ms2": {"label": "Mortal Shell II", "color": 0x8A8175, "categories": {
        "weapons": "Weapons", "shells": "Shells", "tarstones": "Tarstones",
        "items": "Items", "bosses": "Bosses",
    }},
    "r2": {"label": "Remnant 2", "color": 0xB91C1C, "categories": {
        "weapons": "Weapons", "armor": "Armor", "rings": "Rings",
        "amulets": "Amulets", "archetypes": "Archetypes", "skills": "Skills",
        "perks": "Perks", "traits": "Traits", "mods_mutators": "Mods & Mutators",
        "relics": "Relics", "relic_fragments": "Relic Fragments", "engrams": "Engrams",
        "consumables": "Consumables", "materials": "Materials", "quest_items": "Quest Items",
        "bosses": "Bosses", "difficulties": "Difficulties", "aberrations": "Aberrations",
        "affixes": "Affixes", "boss_rush": "Boss Rush", "prisms": "Prisms",
    }},
    "nr": {"label": "No Rest for the Wicked", "color": 0x9A6A3A, "categories": {
        "weapons": "Weapons", "armor": "Armor", "rings": "Rings", "quivers": "Quivers",
        "runes": "Runes", "gems": "Gems", "bosses": "Bosses", "world": "World",
    }},
    "cd": {"label": "Crimson Desert", "color": 0xB4533C, "categories": {
        "weapons": "Weapons", "armor": "Armor", "accessories": "Accessories",
        "skills": "Skills", "companions": "Companions", "mounts": "Mounts",
        "bosses": "Bosses", "world": "World", "items": "Items",
    }},
}

CATEGORY_ALIASES = {
    "weapon": "weapons", "weapons": "weapons", "armour": "armor", "armor": "armor",
    "ring": "rings", "rings": "rings", "amulet": "amulets", "amulets": "amulets",
    "shell": "shells", "shells": "shells", "tarstone": "tarstones", "tarstones": "tarstones",
    "item": "items", "items": "items", "boss": "bosses", "bosses": "bosses",
    "archetype": "archetypes", "archetypes": "archetypes", "skill": "skills", "skills": "skills",
    "perk": "perks", "perks": "perks", "trait": "traits", "traits": "traits",
    "mod": "mods_mutators", "mods": "mods_mutators", "mutator": "mods_mutators",
    "mutators": "mods_mutators", "mods_mutators": "mods_mutators",
    "relic": "relics", "relics": "relics", "fragment": "relic_fragments",
    "fragments": "relic_fragments", "relic_fragments": "relic_fragments",
    "engram": "engrams", "engrams": "engrams", "consumable": "consumables",
    "consumables": "consumables", "material": "materials", "materials": "materials",
    "quest_item": "quest_items", "quest_items": "quest_items", "difficulty": "difficulties",
    "difficulties": "difficulties", "aberration": "aberrations", "aberrations": "aberrations",
    "affix": "affixes", "affixes": "affixes", "boss_rush": "boss_rush", "prism": "prisms",
    "prisms": "prisms", "quiver": "quivers", "quivers": "quivers", "rune": "runes",
    "runes": "runes", "gem": "gems", "gems": "gems", "world": "world",
    "accessory": "accessories", "accessories": "accessories", "companion": "companions",
    "companions": "companions", "mount": "mounts", "mounts": "mounts",
}

R2_ENDPOINTS = {
    "weapons": ("api/r2/weapons/", "weapons"), "armor": ("api/r2/armor/", "armor"),
    "rings": ("api/r2/rings/", "rings"), "amulets": ("api/r2/amulets/", "amulets"),
    "archetypes": ("api/r2/archetypes/", "archetypes"), "traits": ("api/r2/traits/", "traits"),
    "relics": ("api/r2/relics/", "relics"),
    "relic_fragments": ("api/r2/relic-fragments/", "fragments"),
    "bosses": ("api/r2/bosses/", "bosses"), "aberrations": ("api/r2/aberrations/", "aberrations"),
}


def _clean(value):
    return (
        str(value or "")
        .replace("—", "-")
        .replace("–", "-")
        .replace("·", "-")
        .replace("{R} Resolve", "Resolve")
        .replace("{R}", "Resolve")
    )


def _norm(value):
    value = re.sub(r"['’]s\b", "", str(value or "").casefold())
    return " ".join(re.sub(r"[^a-z0-9]+", " ", value).split())


def _absolute(value):
    value = str(value or "").strip()
    if not value:
        return ""
    return value if value.startswith(("http://", "https://")) else f"{QUESTLOG_PUBLIC_URL.rstrip('/')}/{value.lstrip('/')}"


def _category_url(game, category=None, query=""):
    direct_category = game == "ms2" and bool(category)
    if game == "nr":
        base = f"{QUESTLOG_PUBLIC_URL.rstrip('/')}/soulslike/database/"
        params = {"game": "nrftw"}
    elif game == "ms2" and category:
        base = f"{QUESTLOG_PUBLIC_URL.rstrip('/')}/soulslike/mortal-shell-2/{category}/"
        params = {}
    else:
        slug = {"ms2": "mortal-shell-2", "r2": "remnant-2", "cd": "crimson-desert"}[game]
        base = f"{QUESTLOG_PUBLIC_URL.rstrip('/')}/soulslike/database/{slug}/"
        params = {}
    if category and not direct_category:
        params["category"] = category
    if query:
        params["q"] = query
    return f"{base}?{urlencode(params)}" if params else base


def _slug(value):
    value = re.sub(r"[^a-z0-9]+", "-", str(value or "").casefold()).strip("-")
    return value or "item"


def _display_label(value):
    text = str(value or "").replace("_", " ").replace("-", " ").strip()
    text = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", text)
    text = re.sub(r"(?i)\b(two|one)hand", r"\1-hand ", text)
    return " ".join(text.split()).title()


def _record_url(game, category, row):
    if row.get("detail_url"):
        return _absolute(row["detail_url"])
    if game == "nr" and row.get("id") and category not in {"bosses", "world"}:
        identifier = quote(str(row["id"]).strip(), safe="")
        return (
            f"{QUESTLOG_PUBLIC_URL.rstrip('/')}/soulslike/database/no-rest-for-the-wicked/"
            f"items/{_slug(row.get('name'))}-{identifier}/"
        )
    if game == "ms2" and category == "weapons":
        identifier = row.get("source_id") or row.get("public_weapon_id") or row.get("name")
        if identifier:
            return (
                f"{QUESTLOG_PUBLIC_URL.rstrip('/')}/soulslike/mortal-shell-2/weapons/"
                f"{quote(_slug(identifier), safe='')}/?game=mortal_shell_2"
            )
    if game == "ms2" and category == "bosses":
        identifier = row.get("source_id") or row.get("public_target_id")
        if identifier:
            return (
                f"{QUESTLOG_PUBLIC_URL.rstrip('/')}/soulslike/mortal-shell-2/bosses/"
                f"{quote(str(identifier), safe='')}/"
            )
    return _category_url(game, category, row.get("name") or row.get("label") or "")


def _parse(game, raw):
    value = " ".join(str(raw or "").split())[:140]
    if not value:
        return "commands", ""
    head, _, tail = value.partition(" ")
    if head.casefold() == "commands":
        return "commands", ""
    category = CATEGORY_ALIASES.get(head.casefold())
    if category in GAMES[game]["categories"]:
        return category, tail[:100]
    return "search", value[:100]


def _lines(values, limit=1024):
    output = []
    used = 0
    for value in values:
        line = _clean(value).strip()
        if not line:
            continue
        if used + len(line) + 1 > limit:
            output.append("...")
            break
        output.append(line)
        used += len(line) + 1
    return "\n".join(output)


def _add(embed, name, values):
    value = _lines(values if isinstance(values, list) else [values])
    if value:
        embed.add_field(name=name, value=value, inline=False)


def _list_labels(values):
    labels = []
    for value in values or []:
        if isinstance(value, dict):
            label = value.get("name") or value.get("label") or value.get("value")
        else:
            label = value
        if label not in (None, ""):
            labels.append(str(label))
    return labels


def _price_text(prices):
    output = []
    for price in prices or []:
        if not isinstance(price, dict):
            output.append(str(price))
            continue
        amount = price.get("amount", price.get("value", price.get("price")))
        currency = price.get("currency") or price.get("name") or price.get("type") or ""
        if amount not in (None, ""):
            output.append(f"{amount:,} {currency}" if isinstance(amount, (int, float)) else f"{amount} {currency}")
    return ", ".join(part.strip() for part in output if part.strip())


def _number_text(value):
    return f"{value:g}" if isinstance(value, (int, float)) and not isinstance(value, bool) else str(value)


def _plain_html(value):
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", value or "")).split())


def _ms2_weapon_upgrades_from_html(source):
    """Read the player-facing compatible upgrade cards from a weapon page."""
    marker = re.search(r"Works with this weapon", source or "", re.IGNORECASE)
    if not marker:
        return []
    section_end = re.search(r"</section>", source[marker.end():], re.IGNORECASE)
    section = source[marker.start():marker.end() + section_end.end()] if section_end else source[marker.start():]
    upgrades = []
    for article in re.findall(r"<article\b[^>]*>(.*?)</article>", section, re.IGNORECASE | re.DOTALL):
        heading = re.search(r"<h3\b[^>]*>(.*?)</h3>", article, re.IGNORECASE | re.DOTALL)
        if not heading:
            continue
        description = re.search(r"<p\b[^>]*>(.*?)</p>", article, re.IGNORECASE | re.DOTALL)
        upgrades.append({
            "name": _plain_html(heading.group(1)),
            "description": _clean(_plain_html(description.group(1))) if description else "",
        })
    return upgrades


def _ens_challenge_groups(source):
    """Extract the public, allowlisted challenge profile rendered by QuestLog."""
    marker = re.search(r"ShroudQuest challenge profile", source or "", re.IGNORECASE)
    if not marker:
        return []
    section_end = re.search(r"</section>", source[marker.end():], re.IGNORECASE)
    section = source[marker.start():marker.end() + section_end.end()] if section_end else source[marker.start():]
    groups = []
    for article in re.findall(r"<article\b[^>]*>(.*?)</article>", section, re.IGNORECASE | re.DOTALL):
        heading = re.search(r"<h3\b[^>]*>(.*?)</h3>", article, re.IGNORECASE | re.DOTALL)
        if not heading:
            continue
        settings = []
        pairs = re.findall(
            r'<p\b[^>]*class="[^"]*uppercase[^"]*"[^>]*>(.*?)</p>\s*'
            r'<p\b[^>]*>(.*?)</p>',
            article,
            re.IGNORECASE | re.DOTALL,
        )
        for label, value in pairs:
            label, value = _plain_html(label), _plain_html(value)
            if label and value:
                settings.append((label, value))
        if settings:
            groups.append((_plain_html(heading.group(1)), settings))
    return groups


def _ms2_weapon_fields(embed, row):
    details = []
    if row.get("max_level") not in (None, ""):
        details.append(f"**Upgrade range:** +0 to +{row['max_level']}")
    _add(embed, "Weapon details", details)

    maximum = row.get("max_level", 20)
    for metric in (row.get("enabled_metrics") or [])[:12]:
        if not isinstance(metric, dict):
            continue
        start = metric.get("display_level_0", metric.get("level_0"))
        end = metric.get("display_level_max", metric.get("level_max"))
        if start is None or end is None:
            continue
        suffix = str(metric.get("suffix") or "").strip()
        if metric.get("display_mode") == "percent_of_base":
            start_text = f"{_number_text(start)}% of base cost"
            end_text = f"{_number_text(end)}% of base cost"
        elif suffix:
            start_text = f"{_number_text(start)}{suffix}"
            end_text = f"{_number_text(end)}{suffix}"
        else:
            start_text, end_text = _number_text(start), _number_text(end)
        lines = [
            f"**Base (+0):** {start_text}",
            f"**Maximum (+{maximum}):** {end_text}",
        ]
        if isinstance(start, (int, float)) and isinstance(end, (int, float)) and start:
            change = ((end / start) - 1) * 100
            direction = "increase" if change > 0 else "decrease" if change < 0 else "change"
            lines.append(f"**Total {direction}:** {abs(change):g}%")
        label = metric.get("label") or metric.get("key") or "Stat"
        _add(embed, f"{label} progression", lines)

    upgrades = []
    for option in row.get("compatible_upgrades") or []:
        if not isinstance(option, dict) or not option.get("name"):
            continue
        description = _clean(option.get("description"))
        upgrades.append(f"• **{option['name']}**{f' - {description}' if description else ''}")
    _add(embed, "Works with this weapon", upgrades)


def _ms2_shell_fields(embed, row):
    details = []
    if row.get("full_name") and row.get("full_name") != row.get("name"):
        details.append(f"**Full name:** {row['full_name']}")
    if row.get("shell_health_display"):
        health = row["shell_health_display"]
        if row.get("shell_health_max_display"):
            health += f" at base - {row['shell_health_max_display']} at maximum progression"
        details.append(f"**Health:** {health}")
    if row.get("shell_point_capacity"):
        details.append(f"**Shell point capacity:** {row['shell_point_capacity']}")
    if row.get("memory_count"):
        details.append(f"**Memories:** {row['memory_count']}")
    if row.get("total_memory_cost"):
        details.append(f"**Memory cost:** {row['total_memory_cost']:,}")
    _add(embed, "Shell details", details)

    combat = [
        f"• {stat.get('label')} - {stat.get('display')}"
        for stat in row.get("combat_stats") or []
        if isinstance(stat, dict) and stat.get("label") and stat.get("display") not in (None, "")
    ]
    _add(embed, "Abilities", combat)
    resistances = [
        f"• {stat.get('label')} - {stat.get('display')}"
        for stat in row.get("resistances") or []
        if isinstance(stat, dict) and stat.get("label") and stat.get("display") not in (None, "")
    ]
    _add(embed, "Resistances", resistances)
    _add(embed, "Strengths", [f"• {edge}" for edge in row.get("stat_edges") or []])

    tiers = {}
    for option in row.get("upgrades") or []:
        if not isinstance(option, dict) or not option.get("name"):
            continue
        tier = option.get("bond_tier_display") or option.get("bond_tier") or ""
        description = _clean(option.get("description"))
        if not description and option.get("levels"):
            description = _clean((option["levels"][0] or {}).get("effect"))
        tiers.setdefault(str(tier), []).append(
            f"• **{option['name']}**{f' - {description}' if description else ''}"
        )
    for tier, options in sorted(tiers.items()):
        _add(embed, f"Bond Tier {tier} upgrades", options)


def _ms2_tarstone_fields(embed, row):
    details = []
    for key, label in (("category", "Weapon family"), ("slot", "Slot"), ("color", "Color"), ("max_level", "Maximum level")):
        if row.get(key) not in (None, "", []):
            details.append(f"**{label}:** {_display_label(row[key])}")
    if row.get("system_class_label"):
        details.append(f"**Class:** {row['system_class_label']}")
    _add(embed, "Tarstone details", details)
    levels = []
    for level in row.get("levels") or []:
        if isinstance(level, dict) and level.get("effect"):
            levels.append(f"• **Level {level.get('level')}** - {_clean(level['effect'])}")
    _add(embed, "Upgrade effects", levels)


def _ms2_item_fields(embed, row):
    details = []
    for key, label in (("type", "Type"), ("category", "Category"), ("rarity", "Rarity")):
        if row.get(key) not in (None, "", []):
            details.append(f"**{label}:** {_display_label(row[key])}")
    if row.get("purchase_cost") not in (None, "", []):
        details.append(f"**Purchase cost:** {row['purchase_cost']:,}" if isinstance(row["purchase_cost"], (int, float)) else f"**Purchase cost:** {row['purchase_cost']}")
    _add(embed, "Item details", details)
    if row.get("effect"):
        _add(embed, "Effect", [_clean(row["effect"])])
    stages = []
    for stage in row.get("stages") or []:
        if not isinstance(stage, dict):
            continue
        effects = [stage.get("active_text"), stage.get("passive_text")]
        effect = " - ".join(_clean(value) for value in effects if value)
        required = stage.get("found_count_required")
        label = f"Familiarity {stage.get('stage')}"
        if required not in (None, ""):
            label += f" ({required} found)"
        if effect:
            stages.append(f"• **{label}** - {effect}")
    _add(embed, "Familiarity effects", stages)
    locations = []
    for location in row.get("acquisition_locations") or []:
        if not isinstance(location, dict):
            continue
        place = " - ".join(filter(None, [location.get("region"), location.get("landmark")]))
        route = _clean(location.get("route"))
        locations.append(f"• **{place}**{f' - {route}' if route else ''}")
    _add(embed, "How to get it", locations)


def _ms2_boss_fields(embed, row):
    encounter = []
    for key, label in (("region", "Region"), ("location", "Location"), ("tier", "Tier")):
        if row.get(key) not in (None, "", []):
            encounter.append(f"**{label}:** {row[key]}")
    for key, label in (("health_display", "Health"), ("poise_display", "Poise"), ("break_resistance_display", "Break resistance")):
        if row.get(key) not in (None, "", []):
            encounter.append(f"**{label}:** {row[key]}")
    _add(embed, "Encounter", encounter)

    weaknesses = []
    if row.get("weakness_summary"):
        weaknesses.append(f"• {_clean(row['weakness_summary'])}")
    lowest = row.get("lowest_resistance") or {}
    if lowest.get("label"):
        weaknesses.append(f"• Lowest resistance - {lowest['label']} ({lowest.get('display') or lowest.get('value')})")
    if row.get("immune_statuses"):
        weaknesses.append(f"• Immune - {', '.join(map(str, row['immune_statuses']))}")
    _add(embed, "Weaknesses and resistances", weaknesses)

    rewards = []
    if row.get("gloom_reward"):
        rewards.append(f"• {row.get('gloom_reward_display') or row['gloom_reward']} Gloom")
    if row.get("gold_reward"):
        rewards.append(f"• {row.get('gold_reward_display') or row['gold_reward']} Gold")
    for drop in row.get("drops") or []:
        if isinstance(drop, dict) and (drop.get("name") or drop.get("label")):
            rewards.append(f"• {drop.get('name') or drop.get('label')}")
        elif drop:
            rewards.append(f"• {drop}")
    _add(embed, "Rewards", rewards)


def _nr_weapon_fields(embed, row):
    details = []
    for key, label in (("weapon_class", "Class"), ("weapon_type", "Weapon type"),
                       ("hand_type", "Hand type"), ("damage_school", "Damage school"),
                       ("default_rarity", "Default rarity"), ("weight", "Weight"),
                       ("drop_level", "Drop level")):
        if row.get(key) not in (None, "", []):
            if key != "weapon_type" or not isinstance(row[key], (int, float)):
                details.append(f"**{label}:** {_display_label(row[key])}")
    if row.get("is_unique"):
        details.append("**Unique:** Yes")
    rarities = _list_labels(row.get("rarity_options"))
    if rarities:
        details.append(f"**Rarity options:** {', '.join(_display_label(value) for value in rarities)}")
    _add(embed, "Weapon details", details)
    runes = _list_labels(row.get("default_runes"))
    if runes:
        _add(embed, "Default runes", [f"• {rune}" for rune in runes])


def _cd_weapon_fields(embed, row):
    details = []
    for key, label in (("equip_type", "Type"), ("rarity", "Rarity")):
        if row.get(key) not in (None, "", []):
            details.append(f"• **{label}:** {_display_label(row[key])}")
    slots = _list_labels(row.get("slots"))
    if slots:
        details.append(f"• **Slots:** {', '.join(_display_label(value) for value in slots)}")
    if row.get("dyeable"):
        details.append("• **Dyeable:** Yes")
    if row.get("socket_enabled") and row.get("socket_count"):
        details.append(f"• **Socket options:** {row.get('socket_count') or 0}")
    _add(embed, "Weapon details", details)

    tiers = row.get("stats_by_tier") or []
    if tiers:
        index = min(max(int(row.get("default_tier_index") or 0), 0), len(tiers) - 1)
        tier = tiers[index] if isinstance(tiers[index], dict) else {}
        stats = [
            f"• {stat.get('name') or stat.get('label')} - {stat.get('display_value', stat.get('value'))}"
            for stat in (tier.get("stats") or [])[:12] if isinstance(stat, dict)
        ]
        _add(embed, f"Stats at {tier.get('name') or 'default enhancement'}", stats)

    sockets = _list_labels(row.get("socket_defaults"))
    if sockets:
        _add(embed, "Default sockets", [f"• {socket}" for socket in sockets])
    passives = []
    for skill in (row.get("passive_skills") or [])[:10]:
        if isinstance(skill, dict):
            name = skill.get("skill") or skill.get("name")
            if name:
                level = f" - Level {skill['level']}" if skill.get("level") else ""
                passives.append(f"• {name}{level}")
    _add(embed, "Passive skills", passives)

    purchase_prices = [
        f"• {_price_text([price])}"
        for price in row.get("prices") or [] if _price_text([price])
    ]
    _add(embed, "Purchase price", purchase_prices)
    merchants = []
    for store in (row.get("stores") or [])[:8]:
        if not isinstance(store, dict) or not store.get("name"):
            continue
        lines = [f"• **{store['name']}**"]
        if store.get("refills"):
            lines.append(f"  Stock per restock: {store['refills']}")
        merchants.append("\n".join(lines))
    _add(embed, "Sold by", merchants)

    for recipe in (row.get("recipes") or [])[:6]:
        if not isinstance(recipe, dict):
            continue
        target = "N/A" if recipe.get("to") is None else f"+{recipe['to']}"
        lines = ["**Materials**"]
        materials = [
            f"• {item.get('count')}× {item.get('name')}" + (f" +{item['level']}" if item.get("level") else "")
            for item in (recipe.get("materials") or [])
            if isinstance(item, dict) and item.get("name")
        ]
        lines.extend(materials or ["• No materials listed"])
        costs = [
            f"• {_price_text([price])}"
            for price in recipe.get("prices") or [] if _price_text([price])
        ]
        if costs:
            lines.extend(["**Cost**", *costs])
        _add(embed, f"Refinement +{recipe.get('from', 0)} to {target}", lines)


def _r2_player_fields(embed, category, row):
    if category == "weapons":
        progression = []
        if row.get("max_upgrade_level") not in (None, ""):
            progression.append(f"**Maximum upgrade:** +{row['max_upgrade_level']}")
        if row.get("max_upgrade_damage") not in (None, ""):
            progression.append(f"**Damage at maximum upgrade:** {row['max_upgrade_damage']:g}")
        if row.get("has_fixed_mod") and row.get("fixed_mod_name"):
            progression.append(f"**Fixed mod:** {row['fixed_mod_name']}")
        _add(embed, "Upgrade progression", progression)
    elif category == "archetypes":
        kit = []
        tags = _list_labels(row.get("flavor_tags"))
        if tags:
            kit.append(f"**Playstyle:** {', '.join(tags)}")
        if row.get("trait_name"):
            kit.append(f"**Archetype trait:** {row['trait_name']}")
        _add(embed, "Archetype kit", kit)
        _add(embed, "Skills", [
            f"• **{skill.get('name')}** - unlock level {skill.get('unlock_level')}"
            for skill in row.get("skills") or [] if isinstance(skill, dict) and skill.get("name")
        ])
        perks = []
        for perk in row.get("perks") or []:
            if not isinstance(perk, dict) or not perk.get("name"):
                continue
            label = f" - {perk['perk_label']}" if perk.get("perk_label") else ""
            perks.append(f"• **{perk['name']}**{label}")
        _add(embed, "Perks", perks)
    elif category in {"skills", "perks"}:
        unlock = []
        if row.get("archetype"):
            unlock.append(f"**Archetype:** {row['archetype']}")
        if row.get("unlock_level") not in (None, ""):
            unlock.append(f"**Unlock level:** {row['unlock_level']}")
        if category == "perks" and row.get("perk_label"):
            unlock.append(f"**Perk type:** {row['perk_label']}")
        _add(embed, "Unlock", unlock)
    elif category == "traits":
        details = []
        if row.get("trait_type"):
            details.append(f"**Trait type:** {_display_label(row['trait_type'])}")
        if row.get("max_points") not in (None, ""):
            details.append(f"**Maximum points:** {row['max_points']}")
        _add(embed, "Trait details", details)
    elif category == "mods_mutators":
        details = []
        if row.get("kind"):
            details.append(f"**Kind:** {_display_label(row['kind'])}")
        if row.get("is_fixed"):
            weapons = _list_labels(row.get("fixed_weapon_names"))
            details.append(f"**Fixed to:** {', '.join(weapons) if weapons else 'A specific weapon'}")
        elif row.get("is_equippable"):
            details.append("**Equippable:** Yes")
        _add(embed, "Mod details", details)
    elif category == "bosses":
        defenses = row.get("base_defenses") or {}
        lines = []
        if defenses.get("HealthMax") not in (None, ""):
            lines.append(f"• Health - {defenses['HealthMax']:g}")
        for key, value in defenses.items():
            if not key.endswith("Resistance") or not value:
                continue
            label = _display_label(key.removesuffix("Resistance"))
            note = "weakness" if value < 0 else "resistance"
            lines.append(f"• {label} - {value:g}% {note}")
        _add(embed, "Defenses", lines)
    elif category == "difficulties":
        values = row.get("multipliers") or {}
        labels = (
            ("enemy_health_scalar", "Enemy health"), ("enemy_damage_scalar", "Enemy damage"),
            ("world_boss_health_scalar", "World boss health"), ("world_boss_damage_scalar", "World boss damage"),
            ("experience_scalar", "Experience"), ("currency_scalar", "Currency"),
            ("friendly_fire_damage_scalar", "Friendly fire damage"),
        )
        _add(embed, "Difficulty modifiers", [
            f"• {label} - {values[key]:g}×" for key, label in labels if values.get(key) is not None
        ])
    elif category == "boss_rush":
        rewards = []
        if row.get("boss_kill_xp"):
            rewards.append(f"• XP per boss - {row['boss_kill_xp']:,}")
        if row.get("completion_xp"):
            rewards.append(f"• Completion XP - {row['completion_xp']:,}")
        _add(embed, "Rewards", rewards)
    elif category == "relic_fragments":
        values = []
        if row.get("min_value") is not None:
            values.append(f"**Minimum:** {row['min_value']:g}")
        if row.get("max_value") is not None:
            values.append(f"**Maximum:** {row['max_value']:g}")
        if row.get("effect_value") is not None:
            values.append(f"**Mythic:** {row['effect_value']:g}")
        _add(embed, "Fragment values", values)


def _nr_player_fields(embed, category, row):
    if category == "armor":
        details = []
        for key, label in (("armor_class", "Armor class"), ("weight_class", "Weight class"), ("default_rarity", "Default rarity")):
            if row.get(key) not in (None, "", []):
                details.append(f"**{label}:** {_display_label(row[key])}")
        rarities = _list_labels(row.get("rarity_options"))
        if rarities:
            details.append(f"**Available rarities:** {', '.join(_display_label(value) for value in rarities)}")
        if row.get("is_unique"):
            details.append("**Unique:** Yes")
        _add(embed, "Armor details", details)
    elif category == "runes":
        classes = _list_labels(row.get("compatible_weapon_class_names"))
        _add(embed, "Compatible weapons", [f"• {name}" for name in classes])
    elif category == "gems":
        effects = []
        for effect in row.get("effects") or []:
            if not isinstance(effect, dict) or not effect.get("description"):
                continue
            slot = effect.get("slot") or "Equipment"
            effects.append(f"• **{slot}** - {_clean(effect['description'])}")
        _add(embed, "Socket effects", effects)
    elif category == "bosses" and row.get("variant_count"):
        _add(embed, "Encounter variants", [f"{row['variant_count']} known variants"])


def help_embed(game):
    meta = GAMES[game]
    embed = discord.Embed(
        title=f"🎮 {meta['label']} commands",
        description=f"Use `!{game} <name>` to search all available {meta['label']} data. Add a category to filter it.",
        color=meta["color"], url=_category_url(game),
    )
    lines = [f"`!{game} {category} <name>` - {label}" for category, label in meta["categories"].items()]
    for index in range(0, len(lines), 8):
        _add(embed, "Categories" if index == 0 else "More categories", lines[index:index + 8])
    _add(embed, "Examples", [f"`!{game} <name>` - search everything", f"`!{game} commands` - show this guide"])
    embed.set_footer(text=f"QuestLog - {meta['label']} Data")
    return embed


def command_directory_embed():
    embed = discord.Embed(
        title="🎮 QuestLog Game Commands",
        description=(
            "Choose a game prefix below, then search for what you need.\n"
            "Each game has its own detailed command guide."
        ),
        color=0x6366F1,
        url=_absolute("/soulslike/database/"),
    )

    _add(embed, "Game databases", [
        "🐉 **Dragon's Dogma 2** - `!dd2`",
        "⚔️ **Elden Ring** - `!er`",
        "🔥 **Elden Ring Reforged** - `!err`",
        "🐚 **Mortal Shell II** - `!ms2`",
        "🔫 **Remnant 2** - `!r2`",
        "🏹 **No Rest for the Wicked** - `!nr`",
        "🌵 **Crimson Desert** - `!cd`",
    ])
    _add(embed, "Live tools", [
        "✨ **Final Fantasy XIV** - `/ff`",
        "🔥 **Enshrouded** - `!ens`",
    ])
    _add(embed, "How to use", [
        "**Search everything:** `!dd2 Cyclops`",
        "**Filter a search:** `!dd2 boss Cyclops`",
        "**See every filter:** `!dd2 commands`",
        "Use the matching prefix for any other game.",
    ])
    _add(embed, "More QuestLog commands", [
        "Run `!help` for XP, LFG, community, and game-server commands.",
    ])
    embed.set_footer(text="QuestLog - Search less. Play more.")
    return embed


def fallback_embed(game, category):
    label = GAMES[game]["categories"][category]
    return discord.Embed(
        title=f"{GAMES[game]['label']} - {label}",
        description=f"Enter a name to search, or [browse {label} on QuestLog]({_category_url(game, category)}).",
        color=GAMES[game]["color"], url=_category_url(game, category),
    )


def record_embed(game, category, row):
    meta = GAMES[game]
    name = row.get("name") or row.get("label") or "Record"
    embed = discord.Embed(
        title=f"{name}", description=_clean(row.get("description") or row.get("flavor_text") or f"{meta['label']} {meta['categories'][category].lower()} data."),
        color=meta["color"], url=_record_url(game, category, row),
    )
    specialized_weapon = category == "weapons" and game in {"ms2", "nr", "cd"}
    specialized_ms2 = game == "ms2" and category in {"weapons", "shells", "tarstones", "items", "bosses"}
    if game == "ms2" and category == "weapons":
        _ms2_weapon_fields(embed, row)
    elif game == "ms2" and category == "shells":
        _ms2_shell_fields(embed, row)
    elif game == "ms2" and category == "tarstones":
        _ms2_tarstone_fields(embed, row)
    elif game == "ms2" and category == "items":
        _ms2_item_fields(embed, row)
    elif game == "ms2" and category == "bosses":
        _ms2_boss_fields(embed, row)
    elif game == "nr" and category == "weapons":
        _nr_weapon_fields(embed, row)
    elif game == "cd" and category == "weapons":
        _cd_weapon_fields(embed, row)

    if game == "r2":
        _r2_player_fields(embed, category, row)
    elif game == "nr":
        _nr_player_fields(embed, category, row)

    if not specialized_weapon and not specialized_ms2:
        details = []
        for key, label in (("type", "Type"), ("kind", "Type"), ("category", "Category"), ("slot", "Slot"),
                           ("archetype", "Archetype"), ("world", "World"), ("region", "Region"),
                           ("zone", "Zone"), ("location", "Location"), ("tier", "Tier"),
                           ("dlc_label", "Release"), ("drop_level", "Drop level"), ("max_level", "Maximum level")):
            if row.get(key) not in (None, "", []):
                details.append(f"**{label}:** {row[key]}")
        _add(embed, "Details", details)
    stats = row.get("stats") or []
    if isinstance(stats, list):
        _add(embed, "Stats", [f"• {item.get('label') or item.get('key')} - {item.get('display_value', item.get('value'))}{item.get('suffix') or ''}" for item in stats[:12] if isinstance(item, dict)])
    elif isinstance(stats, dict):
        _add(embed, "Stats", [f"• {key.replace('_', ' ').title()} - {value}" for key, value in list(stats.items())[:12]])
    requirements = row.get("requirements") or []
    if isinstance(requirements, list):
        _add(embed, "Requirements", [f"• {item.get('attribute')} - {item.get('value')}" for item in requirements if isinstance(item, dict)])
    rewards = row.get("rewards") or []
    if isinstance(rewards, (str, int, float)):
        rewards = [rewards]
    _add(embed, "Rewards", [f"• {item.get('name', item) if isinstance(item, dict) else item}" for item in rewards])
    if not specialized_ms2:
        effects = []
        for key in ("effect", "bonus", "ability", "perk", "passive"):
            value = row.get(key)
            if value not in (None, "", []):
                effects.extend(_list_labels(value) if isinstance(value, list) else [_clean(value)])
        _add(embed, "Effects", [f"• {value}" for value in effects])
    _add(embed, "Acquisition", [row.get("acquisition"), row.get("drop_note"), row.get("material_use")])
    crafting = row.get("crafting") or {}
    if isinstance(crafting, dict) and crafting.get("craftable"):
        ingredients = [f"{item.get('name')} x{item.get('quantity')}" for item in crafting.get("ingredients") or []]
        _add(embed, "Crafting", [", ".join(crafting.get("stations") or []), ", ".join(ingredients)])
    image = _absolute(row.get("image_url") or row.get("image_path") or row.get("icon_url"))
    if image:
        embed.set_thumbnail(url=image)
    embed.set_footer(text=f"QuestLog - {meta['label']} Data")
    return embed


FF_TOOLS = {
    "jobs": ("Job Guides", "/ffxiv/tools/job-guides/"), "job": ("Job Guides", "/ffxiv/tools/job-guides/"),
    "jobguides": ("Job Guides", "/ffxiv/tools/job-guides/"),
    "housing": ("Housing Tracker", "/ffxiv/tools/housing/"), "deepdungeon": ("Deep Dungeon Tracker", "/ffxiv/tools/deep-dungeon/"),
    "housingtracker": ("Housing Tracker", "/ffxiv/tools/housing/"),
    "fieldops": ("Field Operations Tracker", "/ffxiv/tools/field-ops/"), "fieldoperations": ("Field Operations Tracker", "/ffxiv/tools/field-ops/"), "gatheringtimes": ("Gathering Timers", "/ffxiv/tools/gathering/"),
    "gathering": ("Gathering Timers", "/ffxiv/tools/gathering/"), "hunts": ("Hunt Board", "/ffxiv/tools/hunt-board/"),
    "mb": ("Market Board", "/ffxiv/tools/market-board/"), "ocean": ("Ocean Fishing", "/ffxiv/tools/ocean-fishing/"),
    "pf": ("Party Finder", "/lfg/"), "tripletriad": ("Triple Triad", "/ffxiv/tools/triple-triad/"), "tripletraid": ("Triple Triad", "/ffxiv/tools/triple-triad/"),
    "collection": ("Collection Tracker", "/ffxiv/tools/collection/"), "mounts": ("Mount Collection", "/ffxiv/tools/collection/"),
    "minions": ("Minion Collection", "/ffxiv/tools/collection/"), "achievements": ("Achievement Collection", "/ffxiv/tools/collection/"),
    "fc": ("FC Progression", "/ffxiv/tools/fc/"), "resets": ("Reset Checklist", "/ffxiv/tools/resets/"),
    "apply": ("Apply to the FC", "/ffxiv/apply/"),
}


def _json_value(value, default):
    try:
        parsed = json.loads(value or "")
        return parsed if isinstance(parsed, type(default)) else default
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _ffxiv_profile_for_discord(discord_user_id):
    """Return the requesting user's linked FFXIV data; never accepts another user."""
    if not str(discord_user_id or "").strip():
        return None
    try:
        with db_session_scope() as db:
            user = db.execute(text(
                "SELECT id, username FROM web_users "
                "WHERE discord_id=:did AND is_banned=0 AND is_disabled=0 LIMIT 1"
            ), {"did": str(discord_user_id)}).fetchone()
            if not user:
                return None
            character = db.execute(text(
                "SELECT lodestone_id, character_name, world, datacenter, avatar_url, active_job, "
                "mount_count, minion_count, mounts_json, minions_json, achievements_json, "
                "sync_status, last_synced_at FROM web_ffxiv_characters "
                "WHERE user_id=:uid ORDER BY is_primary DESC, id ASC LIMIT 1"
            ), {"uid": user.id}).fetchone()
            if not character:
                return {"account": user.username, "character": None}

            mounts = _json_value(character.mounts_json, [])
            minions = _json_value(character.minions_json, [])
            achievements = _json_value(character.achievements_json, {})
            achievement_total = sum(len(values) for values in achievements.values() if isinstance(values, list))
            achievement_count = sum(
                1 for values in achievements.values() if isinstance(values, list)
                for item in values if isinstance(item, dict) and item.get("completed")
            )
            triad_count = db.execute(text(
                "SELECT COUNT(*) FROM web_ffxiv_triad_cards WHERE user_id=:uid"
            ), {"uid": user.id}).scalar() or 0
            clear_count = db.execute(text(
                "SELECT COUNT(*) FROM web_ffxiv_clears WHERE user_id=:uid"
            ), {"uid": user.id}).scalar() or 0
            deep_dungeon = db.execute(text(
                "SELECT dungeon, floor_end, kos, job, party_size, is_clear, run_at "
                "FROM web_ffxiv_dd_pb WHERE user_id=:uid ORDER BY dungeon"
            ), {"uid": user.id}).fetchall()
            field_ops = db.execute(text(
                "SELECT zone, elemental_level, resistance_rank, knowledge_level, relic_stage, "
                "phantom_unlocked, phantom_mastered, forked_clears, updated_at "
                "FROM web_ffxiv_field_ops WHERE user_id=:uid ORDER BY zone"
            ), {"uid": user.id}).fetchall()
            return {
                "account": user.username,
                "character": {
                    "lodestone_id": character.lodestone_id,
                    "name": character.character_name,
                    "world": character.world,
                    "datacenter": character.datacenter,
                    "avatar": character.avatar_url,
                    "active_job": character.active_job,
                    "mount_count": max(character.mount_count or 0, len(mounts)),
                    "minion_count": max(character.minion_count or 0, len(minions)),
                    "mounts": mounts,
                    "minions": minions,
                    "achievement_count": achievement_count,
                    "achievement_total": achievement_total,
                    "triad_count": triad_count,
                    "clear_count": clear_count,
                    "sync_status": character.sync_status,
                    "last_synced_at": character.last_synced_at,
                },
                "deep_dungeon": [dict(row._mapping) for row in deep_dungeon],
                "field_ops": [dict(row._mapping) for row in field_ops],
            }
    except Exception as exc:
        logger.warning("FFXIV linked profile lookup failed for Discord user %s: %s", discord_user_id, exc)
        return None


def _ffxiv_node_status(node, now=None):
    """Return live Eorzea-time state and real-world seconds until the next change."""
    now = float(now if now is not None else time.time())
    et_total_minutes = int(now * (3600 / 175) / 60)
    et_minute = et_total_minutes % 1440
    uptime = int(node.get("uptime") or 0)
    best = None
    for spawn_hour in node.get("times") or []:
        spawn = int(spawn_hour) * 60
        despawn = (spawn + uptime) % 1440
        active = spawn <= et_minute < despawn if despawn > spawn else et_minute >= spawn or et_minute < despawn
        if active:
            remaining = (despawn - et_minute) % 1440 or 1440
            candidate = (0, remaining, int(spawn_hour))
        else:
            candidate = ((spawn - et_minute) % 1440 or 1440, 0, int(spawn_hour))
        if best is None or (candidate[0] == 0 and best[0] != 0) or (candidate[0] == best[0] == 0 and candidate[1] < best[1]) or (candidate[0] and best[0] and candidate[0] < best[0]):
            best = candidate
    wait_et, remaining_et, spawn_hour = best or (1440, 0, 0)
    return {
        "active": wait_et == 0,
        "wait_et": wait_et,
        "remaining_et": remaining_et,
        "spawn_hour": spawn_hour,
        "event_ts": int(now + (remaining_et if wait_et == 0 else wait_et) * 175 / 60),
        "et_hour": et_minute // 60,
        "et_minute": et_minute % 60,
    }


def _featured_housing_plots(districts, limit=8):
    """Choose a deterministic preview balanced across districts and plot sizes."""
    candidates = []
    for district_index, district in enumerate(districts or []):
        district_name = str(district.get("name") or "Unknown district")
        for plot_index, plot in enumerate(district.get("plots") or []):
            candidates.append({
                **plot,
                "district_name": district_name,
                "_order": (district_index, plot_index),
            })

    chosen = []
    district_counts = {}
    size_counts = {}
    remaining = list(candidates)
    while remaining and len(chosen) < limit:
        best = max(
            remaining,
            key=lambda plot: (
                district_counts.get(plot["district_name"], 0) == 0,
                size_counts.get(str(plot.get("size") or "?").upper(), 0) == 0,
                -district_counts.get(plot["district_name"], 0),
                -size_counts.get(str(plot.get("size") or "?").upper(), 0),
                -plot["_order"][0],
                -plot["_order"][1],
            ),
        )
        remaining.remove(best)
        chosen.append(best)
        district = best["district_name"]
        size = str(best.get("size") or "?").upper()
        district_counts[district] = district_counts.get(district, 0) + 1
        size_counts[size] = size_counts.get(size, 0) + 1
    return chosen


def _questlog_fetch_url(path):
    base = QUESTLOG_PUBLIC_URL.rstrip("/")
    path_text = str(path)
    url = path_text if path_text.startswith("http") else f"{base}/{path_text.lstrip(chr(47))}"
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != "questlog.casual-heroes.com" or parsed.username or parsed.password:
        raise ValueError("QuestLog data URL is not an approved HTTPS endpoint")
    return url


class GameHubsCog(Cog):
    def __init__(self, bot):
        self.bot = bot
        self._cache = {}

    async def _request(self, path, params=None):
        url = _questlog_fetch_url(path)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            async with session.get(url, params=params or {}) as response:
                return response.status, await response.json(content_type=None)

    async def _request_text(self, path, params=None):
        url = _questlog_fetch_url(path)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            async with session.get(url, params=params or {}) as response:
                return response.status, await response.text()

    @staticmethod
    def _html_cards(source, card_class):
        rows = []
        pattern = rf'<article[^>]*class="[^"]*{re.escape(card_class)}[^"]*"[^>]*>(.*?)</article>'
        for card in re.findall(pattern, source, re.IGNORECASE | re.DOTALL):
            name_match = re.search(r'<h3[^>]*>(.*?)</h3>', card, re.IGNORECASE | re.DOTALL)
            if not name_match:
                continue
            def plain(value):
                return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", value or "")).split())
            name = plain(name_match.group(1))
            image_match = re.search(r'<img[^>]+src="([^"]+)"', card, re.IGNORECASE)
            id_match = re.search(r'data-cd-item-details="([^"]+)"', card, re.IGNORECASE)
            descriptions = [plain(value) for value in re.findall(r'<p[^>]*class="[^"]*(?:leading-5|line-clamp-3)[^"]*"[^>]*>(.*?)</p>', card, re.IGNORECASE | re.DOTALL)]
            rows.append({
                "name": name,
                "description": next((value for value in descriptions if value and value != name), ""),
                "image_url": html.unescape(image_match.group(1)) if image_match else "",
                "id": id_match.group(1) if id_match else "",
            })
        return rows

    async def _cached(self, key, loader):
        cached = self._cache.get(key)
        now = time.monotonic()
        if cached and now - cached[0] < CACHE_SECONDS:
            return cached[1]
        value = await loader()
        self._cache[key] = (now, value)
        return value

    async def _ms2(self, category):
        async def load():
            status, manifest = await self._request("api/soulslike/data/manifest/")
            dataset = (manifest.get("datasets") or {}).get("ms2_reference") or {}
            if status != 200 or not dataset.get("url"):
                raise RuntimeError("MS2 snapshot unavailable")
            status, payload = await self._request(dataset["url"])
            if status != 200:
                raise RuntimeError("MS2 dataset unavailable")
            rows = list(payload.get(category) or [])
            if category == "items":
                permanent = (payload.get("permanent_pickups") or {}).get("items") or []
                seen = {_norm(row.get("name")) for row in rows if isinstance(row, dict)}
                rows.extend(
                    row for row in permanent
                    if isinstance(row, dict) and _norm(row.get("name")) not in seen
                )
            return rows
        return await self._cached(("ms2", category), load)

    async def _enrich_record(self, game, category, row):
        """Load detail-only player data after a single record has been selected."""
        if game != "ms2" or category != "weapons" or row.get("compatible_upgrades"):
            return row
        detail_url = _record_url(game, category, row)

        async def load():
            status, source = await self._request_text(detail_url)
            return _ms2_weapon_upgrades_from_html(source) if status == 200 else []

        enriched = dict(row)
        try:
            enriched["compatible_upgrades"] = await self._cached(
                ("ms2", "weapon-upgrades", row.get("source_id") or detail_url), load,
            )
        except Exception as exc:
            logger.warning("MS2 weapon detail enrichment failed for %s: %s", row.get("name"), exc)
        return enriched

    async def _nr(self, category):
        async def load():
            if category == "bosses":
                status, payload = await self._request("api/soulslike/bosses/", {"game": "nrftw"})
                return payload.get("bosses") or [] if status == 200 else []
            if category == "world":
                return []
            status, payload = await self._request("api/soulslike/nrftw/builder/catalog/")
            if status != 200:
                raise RuntimeError("NRFTW catalog unavailable")
            return payload.get(category) or []
        return await self._cached(("nr", category), load)

    async def _r2(self, category):
        async def load():
            if category in R2_ENDPOINTS:
                path, key = R2_ENDPOINTS[category]
                status, payload = await self._request(path)
                if status != 200:
                    raise RuntimeError("R2 catalog unavailable")
                return payload.get(key) or []
            if category in {"skills", "perks"}:
                archetypes = await self._r2("archetypes")
                rows, seen = [], set()
                for archetype in archetypes:
                    for row in archetype.get(category) or []:
                        key = row.get("source_id") or row.get("id")
                        if key not in seen:
                            seen.add(key); rows.append(row)
                return rows
            if category == "mods_mutators":
                responses = await asyncio.gather(self._request("api/r2/mods/"), self._request("api/r2/mutators/"))
                return (responses[0][1].get("mods") or []) + (responses[1][1].get("mutators") or [])
            if category == "prisms":
                status, payload = await self._request("api/r2/prisms/")
                return payload.get("prisms") or [] if status == 200 else []
            if category in {"difficulties", "affixes", "boss_rush"}:
                status, payload = await self._request("api/r2/systems/", {"include": "affix-pools"})
                if status != 200:
                    raise RuntimeError("R2 systems unavailable")
                if category == "difficulties":
                    return (payload.get("difficulty") or {}).get("difficulties") or []
                if category == "affixes":
                    return (payload.get("affixes") or {}).get("modifiers") or []
                system = payload.get("boss_rush") or {}
                return (system.get("modes") or []) + (system.get("upgrades") or [])
            if category in {"engrams", "consumables", "materials", "quest_items"}:
                status, source = await self._request_text(
                    "soulslike/database/remnant-2/", {"category": category},
                )
                return self._html_cards(source, "r2-catalog-card") if status == 200 else []
            return []
        return await self._cached(("r2", category), load)

    async def _cd(self, category, query=""):
        live = {"weapons", "armor", "accessories", "companions", "mounts", "items"}
        if category not in live:
            return []
        async def load():
            status, source = await self._request_text(
                f"soulslike/database/crimson-desert/{category}/", {"q": query},
            )
            if status != 200:
                return []
            rows = self._html_cards(source, "cd-card")
            if category != "items":
                details = []
                for row in rows:
                    if not row.get("id"):
                        details.append(row); continue
                    item_status, payload = await self._request(
                        "api/soulslike/crimson-desert/builder/catalog/", {"id": row["id"]},
                    )
                    details.append(payload.get("item") or row if item_status == 200 else row)
                rows = details
            return rows
        return await self._cached(("cd", category, _norm(query)), load)

    async def _catalog(self, game, category, query=""):
        if game == "cd":
            return await self._cd(category, query)
        return await {"ms2": self._ms2, "nr": self._nr, "r2": self._r2}[game](category)

    @staticmethod
    def _rank(rows, query):
        target = _norm(query)
        scored = []
        for row in rows:
            name = _norm(row.get("name") or row.get("label"))
            if not name:
                continue
            ratio = SequenceMatcher(None, target, name).ratio()
            score = 4 if name == target else 3 + ratio if name.startswith(target) else 2 + ratio if target in name else ratio if ratio >= .64 else -1
            if score >= 0:
                scored.append((score, row))
        return [row for _, row in sorted(scored, key=lambda value: (-value[0], _norm(value[1].get("name"))))]

    async def _results(self, ctx, game, query, matches):
        lines, seen = [], set()
        for category, row in matches:
            name = row.get("name") or row.get("label")
            key = (category, _norm(name))
            if not name or key in seen:
                continue
            seen.add(key); lines.append(f"• `!{game} {category} {name}`")
            if len(lines) == 8:
                break
        description = f"No exact {GAMES[game]['label']} match was found for `{_clean(query)}`."
        if lines:
            description += "\n\nClosest matches:\n" + "\n".join(lines)
        await ctx.reply(embed=discord.Embed(title=f"{GAMES[game]['label']} search results", description=description, color=GAMES[game]["color"], url=_category_url(game, query=query)))

    async def _run(self, ctx, game, raw):
        category, query = _parse(game, raw)
        if category == "commands":
            await ctx.reply(embed=help_embed(game)); return
        if category != "search" and not query:
            await ctx.reply(embed=fallback_embed(game, category)); return
        categories = [category] if category != "search" else list(GAMES[game]["categories"])
        results = await asyncio.gather(*(self._catalog(game, key, query) for key in categories), return_exceptions=True)
        available, matches = 0, []
        for key, rows in zip(categories, results):
            if isinstance(rows, Exception):
                logger.warning("%s %s lookup failed: %s", game.upper(), key, rows); continue
            available += 1
            matches.extend((key, row) for row in self._rank(rows, query)[:8])
        if not available:
            await ctx.reply(embed=discord.Embed(title=f"{GAMES[game]['label']} lookup unavailable", description="QuestLog could not load this game data right now.", color=ERROR_COLOR)); return
        exact = [(key, row) for key, row in matches if _norm(row.get("name") or row.get("label")) == _norm(query)]
        if len(exact) == 1:
            key, row = exact[0]
            row = await self._enrich_record(game, key, row)
            await ctx.reply(embed=record_embed(game, key, row)); return
        if category != "search" and len(matches) == 1:
            row = await self._enrich_record(game, category, matches[0][1])
            await ctx.reply(embed=record_embed(game, category, row)); return
        await self._results(ctx, game, query, exact or matches)

    async def commands(self, ctx):
        """!commands - Show the complete player-facing game command directory."""
        await ctx.reply(embed=command_directory_embed())

    async def ms2(self, ctx, *, lookup: str = ""):
        await self._run(ctx, "ms2", lookup)

    async def r2(self, ctx, *, lookup: str = ""):
        await self._run(ctx, "r2", lookup)

    async def nr(self, ctx, *, lookup: str = ""):
        await self._run(ctx, "nr", lookup)

    async def cd(self, ctx, *, lookup: str = ""):
        await self._run(ctx, "cd", lookup)

    async def _ff_collection(self, ctx, topic):
        url = _absolute("/ffxiv/tools/collection/")
        profile = _ffxiv_profile_for_discord(getattr(getattr(ctx, "author", None), "id", ""))
        if not profile:
            await ctx.reply(embed=discord.Embed(
                title="FFXIV Collection Tracker",
                description=(
                    "Your Discord account is not linked to a QuestLog account. "
                    f"[Log in and connect it on QuestLog]({url}) to view your collections here."
                ),
                color=0x6D5ACF, url=url,
            ))
            return
        character = profile.get("character")
        if not character:
            await ctx.reply(embed=discord.Embed(
                title="FFXIV Collection Tracker",
                description=f"Your QuestLog account is connected, but it has no linked Lodestone character. [Link one here]({url}).",
                color=0x6D5ACF, url=url,
            ))
            return

        embed = discord.Embed(
            title=f"✨ {character['name']} - FFXIV Progress",
            description=f"**World:** {character.get('world') or 'Unknown'} ({character.get('datacenter') or 'Unknown DC'})",
            color=0x6D5ACF, url=url,
        )
        if character.get("avatar"):
            embed.set_thumbnail(url=character["avatar"])
        summary = [
            f"• Mounts - **{character['mount_count']:,}**",
            f"• Minions - **{character['minion_count']:,}**",
            f"• Achievements - **{character['achievement_count']:,}/{character['achievement_total']:,}**",
            f"• Triple Triad cards - **{character['triad_count']:,}/473**",
            f"• Tracked raid clears - **{character['clear_count']:,}**",
        ]
        _add(embed, "Collection summary", summary)
        if topic in {"mounts", "minions"}:
            names = _list_labels(character.get(topic))
            _add(embed, f"Synced {topic}", [f"• {name}" for name in names[:15]] or ["No synced entries are available."])
        if character.get("active_job"):
            _add(embed, "Character", [f"**Active job:** {character['active_job']}"])
        sync = character.get("last_synced_at")
        if sync:
            synced = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(int(sync)))
            embed.set_footer(text=f"Last synced {synced} - QuestLog FFXIV Data")
        else:
            embed.set_footer(text=f"Sync status: {character.get('sync_status') or 'unknown'} - QuestLog FFXIV Data")
        await ctx.reply(embed=embed)

    async def _ff_personal_tracker(self, ctx, topic):
        paths = {
            "deepdungeon": "/ffxiv/tools/deep-dungeon/",
            "fieldops": "/ffxiv/tools/field-ops/",
            "tripletriad": "/ffxiv/tools/triple-triad/",
            "fc": "/ffxiv/tools/fc/",
        }
        url = _absolute(paths[topic])
        profile = _ffxiv_profile_for_discord(getattr(getattr(ctx, "author", None), "id", ""))
        character = (profile or {}).get("character")
        if not profile or not character:
            await ctx.reply(embed=discord.Embed(
                title="FFXIV Tracker",
                description=f"Connect your QuestLog account and Lodestone character to show personal progress here. [Open this tracker]({url}).",
                color=0x6D5ACF, url=url,
            ))
            return
        embed = discord.Embed(title=f"{character['name']} - FFXIV Progress", color=0x6D5ACF, url=url)
        if character.get("avatar"):
            embed.set_thumbnail(url=character["avatar"])
        if topic == "deepdungeon":
            names = {"potd": "Palace of the Dead", "hoh": "Heaven-on-High", "eo": "Eureka Orthos"}
            rows = [
                f"• **{names.get(row['dungeon'], _display_label(row['dungeon']))}:** Floor {row['floor_end']} - {row.get('job') or 'Unknown job'} - {row.get('party_size') or 1} player"
                for row in profile.get("deep_dungeon") or []
            ]
            _add(embed, "Personal bests", rows or ["No Deep Dungeon runs have been logged yet."])
        elif topic == "fieldops":
            rows = []
            for row in profile.get("field_ops") or []:
                progress = next((value for value in (
                    f"Elemental level {row['elemental_level']}" if row.get("elemental_level") else "",
                    f"Resistance rank {row['resistance_rank']}" if row.get("resistance_rank") else "",
                    f"Knowledge level {row['knowledge_level']}" if row.get("knowledge_level") else "",
                ) if value), "Progress recorded")
                rows.append(f"• **{_display_label(row['zone'])}:** {progress}")
            _add(embed, "Field Operations", rows or ["No Field Operations progress has been logged yet."])
        elif topic == "tripletriad":
            _add(embed, "Triple Triad collection", [f"**{character['triad_count']:,}/473 cards collected**"])
        else:
            _add(embed, "FC progression", [f"**Tracked raid clears:** {character['clear_count']:,}"])
        embed.set_footer(text="QuestLog - Personal FFXIV Data")
        await ctx.reply(embed=embed)

    async def _ff_gathering(self, ctx, query):
        url = _absolute("/ffxiv/tools/gathering/")
        status, nodes = await self._request("static/data/ffxiv_gathering_nodes.json")
        if status != 200 or not isinstance(nodes, list):
            await ctx.reply(embed=discord.Embed(title="FFXIV Gathering Timers", description=f"Timers are unavailable right now. [Open the live tracker]({url}).", color=ERROR_COLOR, url=url))
            return
        target = _norm(query)
        if target:
            nodes = [node for node in nodes if target in _norm(" ".join([
                str(node.get("name") or ""), str(node.get("zone") or ""), " ".join(node.get("items") or []),
            ]))]
        ranked = []
        for node in nodes:
            state = _ffxiv_node_status(node)
            ranked.append((0 if state["active"] else 1, state["event_ts"], node, state))
        ranked.sort(key=lambda value: (value[0], value[1], str(value[2].get("name") or "")))
        embed = discord.Embed(
            title="🌿 FFXIV Gathering Timers",
            description=(f"Results for **{_clean(query)}**" if target else "Active and upcoming timed gathering nodes."),
            color=0x16A34A, url=url,
        )
        lines = []
        for _, _, node, state in ranked[:10]:
            timing = f"active until <t:{state['event_ts']}:R>" if state["active"] else f"spawns <t:{state['event_ts']}:R>"
            items = ", ".join((node.get("items") or [])[:3])
            lines.append(f"• **{node.get('name')}** - {node.get('zone')} - {timing}\n  {node.get('job')} · {items}")
        _add(embed, "Nodes", lines or ["No gathering nodes matched that search."])
        if ranked:
            state = ranked[0][3]
            embed.set_footer(text=f"Eorzea time {state['et_hour']:02d}:{state['et_minute']:02d} - QuestLog FFXIV Data")
        await ctx.reply(embed=embed)

    async def _ff_ocean(self, ctx):
        url = _absolute("/ffxiv/tools/ocean-fishing/")
        status, payload = await self._request("api/ffxiv/ocean-fishing/schedule/", {"count": 4})
        windows = payload.get("windows") or [] if status == 200 else []
        embed = discord.Embed(title="🎣 FFXIV Ocean Fishing", description="Upcoming Ocean Fishing departures.", color=0x2563EB, url=url)
        lines = []
        for window in windows[:4]:
            timing = "Active now" if window.get("is_active") else f"Departs <t:{window.get('departure_ts')}:R>"
            fish = ", ".join(window.get("blue_fish") or []) or "No blue fish listed"
            lines.append(f"• **{window.get('route')}** - {timing}\n  Blue fish: {fish}")
        _add(embed, "Schedule", lines or ["The Ocean Fishing schedule is unavailable right now."])
        embed.set_footer(text="QuestLog - FFXIV Ocean Fishing Data")
        await ctx.reply(embed=embed)

    async def _ff_world_status(self, ctx, query):
        url = _absolute("/ffxiv/")
        status, payload = await self._request("api/ffxiv/world-status/")
        worlds = []
        for region in payload.get("regions") or [] if status == 200 else []:
            for dc in region.get("dcs") or []:
                for world in dc.get("worlds") or []:
                    worlds.append((region.get("name"), dc.get("name"), world))
        target = _norm(query)
        if target:
            worlds = [row for row in worlds if target in {_norm(row[1]), _norm(row[2].get("name"))}]
        embed = discord.Embed(title="🌐 FFXIV World Status", color=0x6D5ACF, url=url)
        lines = [
            f"• {'🟢' if world.get('online') else '🔴'} **{world.get('name')}** ({dc}) - {world.get('label') or _display_label(world.get('status'))}"
            for _, dc, world in worlds[:20]
        ]
        _add(embed, "Worlds", lines or ["No world status matched that search."])
        await ctx.reply(embed=embed)

    async def _ff_housing(self, ctx, requested_world):
        url = _absolute("/ffxiv/tools/housing/")
        world_name = requested_world.strip()
        if not world_name:
            profile = _ffxiv_profile_for_discord(getattr(getattr(ctx, "author", None), "id", ""))
            world_name = str(((profile or {}).get("character") or {}).get("world") or "")
        if not world_name:
            await ctx.reply(embed=discord.Embed(
                title="🏠 FFXIV Housing Tracker",
                description=f"Usage: `/ff housing <world>`\n\nLink a Lodestone character on [QuestLog]({url}) to use your character's world automatically.",
                color=0x6D5ACF, url=url,
            ))
            return
        status, payload = await self._request("api/ffxiv/housing/worlds/")
        worlds = [world for dc in payload.get("datacenters") or [] for world in dc.get("worlds") or []] if status == 200 else []
        world = next((item for item in worlds if _norm(item.get("name")) == _norm(world_name)), None)
        if not world:
            await ctx.reply(embed=discord.Embed(title="FFXIV Housing Tracker", description=f"QuestLog could not find the world `{_clean(world_name)}`.", color=ERROR_COLOR, url=url))
            return
        status, plots = await self._request(f"api/ffxiv/housing/plots/{world['id']}/")
        if status != 200:
            plots = {}
        embed = discord.Embed(
            title=f"🏠 {world['name']} Housing",
            description=f"**{int(plots.get('total_open') or 0):,} open plots** currently tracked.",
            color=0x6D5ACF, url=url,
        )
        districts = []
        plot_districts = plots.get("districts") or []
        for district in plot_districts:
            districts.append(f"• **{district.get('name')}:** {district.get('open_count') or 0} open")
        featured = [
            f"• {plot['district_name']} - Ward {plot.get('ward')}, Plot {plot.get('plot')} "
            f"({plot.get('size')}) - {int(plot.get('price') or 0):,} gil"
            for plot in _featured_housing_plots(plot_districts)
        ]
        _add(embed, "Districts", districts)
        _add(embed, "Available plots", featured)
        cached = plots.get("cached_at")
        if cached:
            embed.set_footer(text=f"Updated {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(int(cached)))} - QuestLog Housing Data")
        await ctx.reply(embed=embed)

    async def _ff_market_board(self, ctx, argument):
        url = _absolute("/ffxiv/tools/market-board/")
        item_query, separator, requested_world = argument.rpartition("|")
        if not separator:
            item_query, requested_world = argument, ""
        item_query, requested_world = item_query.strip(), requested_world.strip()
        profile = _ffxiv_profile_for_discord(getattr(getattr(ctx, "author", None), "id", ""))
        world = requested_world or str(((profile or {}).get("character") or {}).get("world") or "")
        if not item_query or not world:
            await ctx.reply(embed=discord.Embed(
                title="💰 FFXIV Market Board",
                description=f"Usage: `/ff mb <item> | <world>`\n\nA linked Lodestone character supplies the world automatically. [Open Market Board]({url})",
                color=0x6D5ACF, url=url,
            ))
            return
        status, search = await self._request("api/ffxiv/mb/search/", {"q": item_query})
        results = search.get("results") or [] if status == 200 else []
        item = next((row for row in results if _norm(row.get("name")) == _norm(item_query)), results[0] if results else None)
        if not item:
            await ctx.reply(embed=discord.Embed(title="FFXIV Market Board", description=f"No item matched `{_clean(item_query)}`.", color=ERROR_COLOR, url=url)); return
        status, prices = await self._request("api/ffxiv/mb/prices/", {"item_id": item["id"], "world": world})
        if status != 200 or prices.get("error"):
            await ctx.reply(embed=discord.Embed(title=f"💰 {item['name']}", description=prices.get("error") or "Market data is unavailable right now.", color=ERROR_COLOR, url=url)); return
        embed = discord.Embed(
            title=f"💰 {item['name']} - {world}",
            description=f"**NQ minimum:** {int(prices.get('min_price_nq') or 0):,} gil\n**HQ minimum:** {int(prices.get('min_price_hq') or 0):,} gil",
            color=0x6D5ACF, url=url,
        )
        if item.get("icon"):
            embed.set_thumbnail(url=item["icon"])
        listings = [
            f"• {int(row.get('price') or 0):,} gil × {row.get('quantity') or 1}{' - HQ' if row.get('hq') else ''} - {row.get('retainer') or 'Unknown retainer'}"
            for row in (prices.get("listings") or [])[:8]
        ]
        _add(embed, "Lowest listings", listings or ["No active listings."])
        _add(embed, "Market activity", [
            f"**Units for sale:** {int(prices.get('units_for_sale') or 0):,}",
            f"**NQ sale velocity:** {prices.get('sale_velocity_nq') or 0}/day",
            f"**HQ sale velocity:** {prices.get('sale_velocity_hq') or 0}/day",
        ])
        embed.set_footer(text="QuestLog - Universalis Market Data")
        await ctx.reply(embed=embed)

    async def _ff_resets(self, ctx):
        url = _absolute("/ffxiv/tools/resets/")
        now = datetime.now(timezone.utc)
        daily = now.replace(hour=15, minute=0, second=0, microsecond=0)
        if daily <= now:
            daily += timedelta(days=1)
        days_until_tuesday = (1 - now.weekday()) % 7
        weekly = (now + timedelta(days=days_until_tuesday)).replace(hour=8, minute=0, second=0, microsecond=0)
        if weekly <= now:
            weekly += timedelta(days=7)
        embed = discord.Embed(
            title="⏱️ FFXIV Reset Timers",
            description=(
                f"**Daily reset:** <t:{int(daily.timestamp())}:R>\n"
                f"**Weekly reset:** <t:{int(weekly.timestamp())}:R>"
            ),
            color=0x6D5ACF, url=url,
        )
        _add(embed, "Tracked tasks", ["Daily roulettes and tribal quests", "Weekly raids and currencies", "Wondrous Tails and Custom Deliveries"])
        embed.set_footer(text="Daily: 15:00 UTC - Weekly: Tuesday 08:00 UTC")
        await ctx.reply(embed=embed)

    async def ff(self, ctx, *, topic: str = ""):
        head, _, argument = " ".join(str(topic or "").split()).partition(" ")
        key = _norm(head).replace(" ", "")
        if key in {"collection", "mounts", "minions", "achievements"}:
            await self._ff_collection(ctx, key); return
        if key in {"gathering", "gatheringtimes", "timers"}:
            await self._ff_gathering(ctx, argument); return
        if key in {"ocean", "oceanfishing"}:
            await self._ff_ocean(ctx); return
        if key in {"status", "world", "worlds"}:
            await self._ff_world_status(ctx, argument); return
        if key in {"housing", "housingtracker"}:
            await self._ff_housing(ctx, argument); return
        if key in {"mb", "market", "marketboard"}:
            await self._ff_market_board(ctx, argument); return
        if key in {"resets", "reset"}:
            await self._ff_resets(ctx); return
        if key in {"deepdungeon", "fieldops", "tripletriad", "tripletraid", "fc"}:
            canonical = "tripletriad" if key == "tripletraid" else key
            await self._ff_personal_tracker(ctx, canonical); return
        normalized_topic = _norm(topic).replace(" ", "")
        if normalized_topic in FF_TOOLS:
            label, path = FF_TOOLS[normalized_topic]
            url = _absolute(path)
            await ctx.reply(embed=discord.Embed(title=f"FFXIV - {label}", description=f"[Open {label} on QuestLog]({url})", color=0x6D5ACF, url=url)); return
        unique = []
        interactive_paths = {
            "/ffxiv/tools/collection/", "/ffxiv/tools/gathering/", "/ffxiv/tools/resets/",
            "/ffxiv/tools/fc/", "/ffxiv/tools/deep-dungeon/", "/ffxiv/tools/field-ops/",
            "/ffxiv/tools/market-board/", "/ffxiv/tools/ocean-fishing/",
            "/ffxiv/tools/triple-triad/", "/ffxiv/tools/housing/",
        }
        for label, path in FF_TOOLS.values():
            if path not in interactive_paths and (label, path) not in unique:
                unique.append((label, path))
        embed = discord.Embed(title="Final Fantasy XIV commands", description="Use `/ff <tool>` for live QuestLog data. Personal trackers use your connected Discord and Lodestone accounts.", color=0x6D5ACF, url=_absolute("/ffxiv/tools/"))
        _add(embed, "Live data", ["`/ff gathering [item or zone]` - live gathering nodes", "`/ff ocean` - upcoming Ocean Fishing departures", "`/ff status [world or DC]` - world status", "`/ff housing [world]` - open housing plots", "`/ff mb <item> | [world]` - live market prices", "`/ff resets` - daily and weekly reset timers"])
        _add(embed, "Personal progress", ["`/ff collection` - collection summary", "`/ff mounts` / `/ff minions` - synced collections", "`/ff deepdungeon` - personal bests", "`/ff fieldops` - zone progress", "`/ff tripletriad` - card progress", "`/ff fc` - tracked clears"])
        _add(embed, "QuestLog pages", [f"`/ff {next(key for key, value in FF_TOOLS.items() if value == (label, path))}` - [{label}]({_absolute(path)})" for label, path in unique])
        await ctx.reply(embed=embed)
