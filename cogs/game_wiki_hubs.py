"""QuestLog lookups for the remaining game databases and tool hubs."""

from __future__ import annotations

import asyncio
import html
import re
import time
from difflib import SequenceMatcher
from urllib.parse import quote, urlencode, urlparse

import aiohttp
import discord
from discord.ext import commands
Cog = commands.Cog

from config import logger

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
        "✨ **Final Fantasy XIV** - `!ff`",
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



def _questlog_fetch_url(path):
    base = QUESTLOG_PUBLIC_URL.rstrip("/")
    path_text = str(path)
    url = path_text if path_text.startswith("http") else f"{base}/{path_text.lstrip('/')}"
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
            seen.add(key); lines.append(f"• `/{game} search name:{name} category:{category}`")
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
