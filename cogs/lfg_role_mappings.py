"""
LFG Role Mappings - Built-in role detection for popular MMOs

This module contains hardcoded role mappings for games with well-defined
class/spec systems. These mappings have the highest priority and cannot
be overridden by admin custom mappings.

Role Detection Priority:
1. BUILTIN - Games listed here with hardcoded mappings
2. CUSTOM  - Admin-defined role tags in custom_options
3. GENERIC - User selects Tank/Healer/DPS/Support/Flex manually

Supported roles: 'tank', 'healer', 'dps', 'support', 'flex'
"""

from typing import Optional, Dict, List, Set

# =============================================================================
# BUILT-IN GAME IDENTIFIERS
# These are matched against game_name (case-insensitive, partial match)
# =============================================================================

BUILTIN_GAMES = {
    'wow': ['world of warcraft', 'wow'],
    'ffxiv': ['final fantasy xiv', 'final fantasy 14', 'ffxiv', 'ff14'],
    'pantheon': ['pantheon', 'pantheon: rise of the fallen'],
    'gw2': ['guild wars 2', 'gw2'],
    'eso': ['elder scrolls online', 'eso', 'teso'],
}


# =============================================================================
# WORLD OF WARCRAFT - Class + Specialization System
# =============================================================================

WOW_ROLE_MAPPINGS = {
    # Tank Specs
    'tank': {
        # Death Knight
        'blood',
        # Demon Hunter
        'vengeance',
        # Druid
        'guardian',
        # Monk
        'brewmaster',
        # Paladin
        'protection',
        # Warrior
        'protection',  # Same name as Paladin
    },
    'healer': {
        # Druid
        'restoration',
        # Evoker
        'preservation',
        # Monk
        'mistweaver',
        # Paladin
        'holy',
        # Priest
        'discipline', 'holy',  # Holy appears twice (Paladin + Priest)
        # Shaman
        'restoration',  # Same name as Druid
    },
    'dps': {
        # Death Knight
        'frost', 'unholy',
        # Demon Hunter
        'havoc',
        # Druid
        'balance', 'feral',
        # Evoker
        'devastation',
        # Hunter
        'beast mastery', 'marksmanship', 'survival',
        # Mage
        'arcane', 'fire', 'frost',  # Frost same as DK
        # Monk
        'windwalker',
        # Paladin
        'retribution',
        # Priest
        'shadow',
        # Rogue
        'assassination', 'outlaw', 'subtlety',
        # Shaman
        'elemental', 'enhancement',
        # Warlock
        'affliction', 'demonology', 'destruction',
        # Warrior
        'arms', 'fury',
    },
    'support': {
        # Evoker - Augmentation is a support spec that buffs the group
        'augmentation',
    },
}

# WoW class-to-possible-roles mapping (for validation)
WOW_CLASS_ROLES = {
    'death knight': ['tank', 'dps'],
    'demon hunter': ['tank', 'dps'],
    'druid': ['tank', 'healer', 'dps'],
    'evoker': ['healer', 'dps', 'support'],
    'hunter': ['dps'],
    'mage': ['dps'],
    'monk': ['tank', 'healer', 'dps'],
    'paladin': ['tank', 'healer', 'dps'],
    'priest': ['healer', 'dps'],
    'rogue': ['dps'],
    'shaman': ['healer', 'dps'],
    'warlock': ['dps'],
    'warrior': ['tank', 'dps'],
}


# =============================================================================
# FINAL FANTASY XIV - Job System (no specs, job = role)
# =============================================================================

FFXIV_ROLE_MAPPINGS = {
    'tank': {
        'paladin', 'pld',
        'warrior', 'war',
        'dark knight', 'drk',
        'gunbreaker', 'gnb',
    },
    'healer': {
        'white mage', 'whm',
        'scholar', 'sch',
        'astrologian', 'ast',
        'sage', 'sge',
    },
    'dps': {
        # Melee DPS
        'monk', 'mnk',
        'dragoon', 'drg',
        'ninja', 'nin',
        'samurai', 'sam',
        'reaper', 'rpr',
        'viper', 'vpr',
        # Physical Ranged DPS
        'bard', 'brd',
        'machinist', 'mch',
        'dancer', 'dnc',
        # Magical Ranged DPS
        'black mage', 'blm',
        'summoner', 'smn',
        'red mage', 'rdm',
        'pictomancer', 'pct',
    },
}


# =============================================================================
# PANTHEON: RISE OF THE FALLEN - Traditional Class Trinity
# =============================================================================

PANTHEON_ROLE_MAPPINGS = {
    'tank': {
        'warrior',
        'dire lord',
        'paladin',
    },
    'healer': {
        'cleric',
        'shaman',
        'druid',
    },
    'dps': {
        'wizard',
        'enchanter',
        'summoner',
        'rogue',
        'ranger',
        'monk',
    },
    'support': {
        # Pantheon has strong support/CC classes
        'bard',
        'enchanter',  # Can be DPS or Support
    },
}


# =============================================================================
# GUILD WARS 2 - Flexible Class System
# GW2 classes can fill any role depending on build, so role is user-selected
# =============================================================================

GW2_CLASSES = [
    'warrior', 'guardian', 'revenant',
    'ranger', 'thief', 'engineer',
    'necromancer', 'elementalist', 'mesmer'
]

# GW2 doesn't have fixed role mappings - any class can be any role
# Role is determined by user selection, not class
GW2_ROLE_MAPPINGS = {}  # Empty - role comes from user's Role field selection


# =============================================================================
# ELDER SCROLLS ONLINE - Flexible Class System
# ESO classes can fill any role depending on build, so role is user-selected
# =============================================================================

ESO_CLASSES = [
    'dragonknight', 'sorcerer', 'nightblade', 'templar',
    'warden', 'necromancer', 'arcanist'
]

# ESO doesn't have fixed role mappings - any class can be any role
# Role is determined by user selection, not class
ESO_ROLE_MAPPINGS = {}  # Empty - role comes from user's Role field selection


# =============================================================================
# ROLE DETECTION FUNCTIONS
# =============================================================================

def get_builtin_game_type(game_name: str) -> Optional[str]:
    """
    Check if a game name matches a built-in game type.

    Args:
        game_name: The game name to check (case-insensitive)

    Returns:
        Game type key ('wow', 'ffxiv', 'pantheon') or None if not built-in
    """
    if not game_name:
        return None

    game_lower = game_name.lower().strip()

    for game_type, aliases in BUILTIN_GAMES.items():
        for alias in aliases:
            if alias in game_lower or game_lower in alias:
                return game_type

    return None


def get_role_from_builtin(game_type: str, spec_or_class: str) -> Optional[str]:
    """
    Get the role for a spec/class using built-in mappings.

    Args:
        game_type: The game type ('wow', 'ffxiv', 'pantheon')
        spec_or_class: The specialization or class name to look up

    Returns:
        Role string ('tank', 'healer', 'dps', 'support') or None if not found
    """
    if not spec_or_class:
        return None

    spec_lower = spec_or_class.lower().strip()

    # Select the appropriate mapping
    if game_type == 'wow':
        mappings = WOW_ROLE_MAPPINGS
    elif game_type == 'ffxiv':
        mappings = FFXIV_ROLE_MAPPINGS
    elif game_type == 'pantheon':
        mappings = PANTHEON_ROLE_MAPPINGS
    else:
        return None

    # Check each role's specs
    for role, specs in mappings.items():
        if spec_lower in specs:
            return role
        # Also check partial matches for multi-word specs
        for spec in specs:
            if spec in spec_lower or spec_lower in spec:
                return role

    return None


def detect_role(
    game_name: str,
    role_detection_mode: str,
    member_selections: Dict,
    selected_role: Optional[str],
    custom_options: Optional[List[Dict]] = None
) -> Optional[str]:
    """
    Detect a member's role using the priority system.

    Priority:
    1. Built-in mappings (for WoW, FFXIV, Pantheon - class/spec determines role)
    2. GW2 special case (class is flexible, user selects role separately)
    3. Custom role tags in options (if mode is 'custom')
    4. Explicit selected_role (if mode is 'generic')

    Args:
        game_name: The game name
        role_detection_mode: 'builtin', 'custom', or 'generic'
        member_selections: Dict of member's option selections
        selected_role: Explicitly selected role (tank/healer/dps/support/flex)
        custom_options: Game's custom options with potential role tags

    Returns:
        Role string or None if unassigned
    """
    # Step 1: Check for built-in game mappings (highest priority)
    game_type = get_builtin_game_type(game_name)

    if game_type:
        # GW2 and ESO special handling - classes are flexible, role is selected separately
        if game_type in ('gw2', 'eso'):
            # Check for Role field selection first
            for key in ['Role', 'role']:
                if key in member_selections:
                    val = member_selections[key]
                    role_val = val[0] if isinstance(val, list) else val
                    if role_val:
                        role_lower = role_val.lower()
                        if role_lower in ('tank', 'healer', 'dps', 'support', 'flex'):
                            return role_lower
            # For GW2/ESO, fall through to selected_role if no Role field
            if selected_role and selected_role in ('tank', 'healer', 'dps', 'support', 'flex'):
                return selected_role
            return None

        # WoW, FFXIV, Pantheon - class/spec determines role automatically
        # Extract spec/class from selections
        spec = None
        cls = None

        # Try common field names
        for key in ['Specialization', 'specialization', 'Spec', 'spec']:
            if key in member_selections:
                val = member_selections[key]
                spec = val[0] if isinstance(val, list) else val
                break

        for key in ['Class', 'class', 'Job', 'job']:
            if key in member_selections:
                val = member_selections[key]
                cls = val[0] if isinstance(val, list) else val
                break

        # Try spec first, then class
        if spec:
            role = get_role_from_builtin(game_type, spec)
            if role:
                return role

        if cls:
            role = get_role_from_builtin(game_type, cls)
            if role:
                return role

        # Built-in game but no matching spec/class = unassigned
        # Don't fall through to other methods for built-in games
        return None

    # Step 2: Check custom role tags from options (works with any mode)
    # This allows admins to define role-tagged choices in their custom options
    if custom_options:
        role = _get_role_from_custom_options(member_selections, custom_options)
        if role:
            return role

    # Step 3: Use explicit selected_role (generic mode or fallback)
    if selected_role and selected_role in ('tank', 'healer', 'dps', 'support', 'flex'):
        return selected_role

    return None


def _get_role_from_custom_options(
    member_selections: Dict,
    custom_options: List[Dict]
) -> Optional[str]:
    """
    Extract role from custom options that have role tags.

    Custom options format with role tags:
    {
        "name": "Build",
        "choices": [
            {"value": "Tank Build", "role": "tank"},
            {"value": "Healer Build", "role": "healer"},
            {"value": "DPS Build", "role": "dps"}
        ]
    }

    Args:
        member_selections: Member's option selections
        custom_options: Game's custom options config

    Returns:
        Role string or None
    """
    if not custom_options or not member_selections:
        return None

    for option in custom_options:
        option_name = option.get('name')
        choices = option.get('choices', [])

        if not option_name or not choices:
            continue

        # Get member's selection for this option
        member_value = member_selections.get(option_name)
        if not member_value:
            continue

        # Handle list values
        if isinstance(member_value, list):
            member_value = member_value[0] if member_value else None

        if not member_value:
            continue

        # Check if choices have role tags
        for choice in choices:
            if isinstance(choice, dict):
                if choice.get('value') == member_value and choice.get('role'):
                    return choice['role']
            elif isinstance(choice, str):
                # Simple string choices don't have role tags
                continue

    return None


# =============================================================================
# GENERIC ROLE OPTIONS
# =============================================================================

GENERIC_ROLE_CHOICES = [
    {'value': 'tank', 'label': '🛡️ Tank', 'emoji': '🛡️'},
    {'value': 'healer', 'label': '💚 Healer', 'emoji': '💚'},
    {'value': 'dps', 'label': '⚔️ DPS', 'emoji': '⚔️'},
    {'value': 'support', 'label': '🎵 Support', 'emoji': '🎵'},
    {'value': 'flex', 'label': '🔄 Flex', 'emoji': '🔄'},
]


def get_role_emoji(role: str) -> str:
    """Get the emoji for a role."""
    emoji_map = {
        'tank': '🛡️',
        'healer': '💚',
        'dps': '⚔️',
        'support': '🎵',
        'flex': '🔄',
    }
    return emoji_map.get(role, '❓')


def get_role_label(role: str) -> str:
    """Get a formatted label for a role."""
    label_map = {
        'tank': 'Tank',
        'healer': 'Healer',
        'dps': 'DPS',
        'support': 'Support',
        'flex': 'Flex',
    }
    return label_map.get(role, 'Unknown')
