"""
7 Days to Die Player Stats Parser
Reads XML save files and extracts gameplay statistics for XP rewards
"""

import logging
import xml.etree.ElementTree as ET
from typing import Dict, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime

logger = logging.getLogger("warden.sdtd_stats")


@dataclass
class SDTDPlayerStats:
    """7 Days to Die player statistics extracted from XML save file."""

    # Core player info
    steam_id: str
    entity_id: int
    player_name: str

    # Progression
    level: int = 0
    experience: int = 0
    skill_points: int = 0

    # Combat stats
    zombies_killed: int = 0
    players_killed: int = 0
    deaths: int = 0
    score: int = 0

    # Survival stats
    total_playtime_seconds: int = 0
    total_item_crafts: int = 0
    distance_walked: float = 0.0
    distance_run: float = 0.0

    # Position
    position_x: float = 0.0
    position_y: float = 0.0
    position_z: float = 0.0

    # Timestamps
    last_spawn_position_x: float = 0.0
    last_spawn_position_y: float = 0.0
    last_spawn_position_z: float = 0.0

    # Health/Stamina
    health: int = 100
    stamina: int = 100

    # Achievements (custom tracking)
    blood_moons_survived: int = 0
    legendary_items_crafted: int = 0
    pois_explored: int = 0  # Points of Interest

    # Meta
    last_updated: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON storage."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SDTDPlayerStats':
        """Create from dictionary."""
        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})


class SDTDStatsParser:
    """
    Parser for 7 Days to Die player save files (.ttp XML format).

    File location examples:
    - /saves/Navezgane/Player/{steam_id}.ttp
    - /saves/Random Gen/Player/{steam_id}.ttp
    - /saves/MyWorld/Player/{steam_id}.ttp
    """

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        """Safely convert to int."""
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        """Safely convert to float."""
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def parse_xml(xml_content: str, steam_id: str) -> Optional[SDTDPlayerStats]:
        """
        Parse 7DTD player XML save file.

        Args:
            xml_content: Raw XML file contents
            steam_id: Player's Steam ID

        Returns:
            SDTDPlayerStats object or None if parsing fails
        """
        try:
            root = ET.fromstring(xml_content)

            # Find player entity (usually first entity)
            entity = root.find('.//entity[@class="EntityPlayer"]')
            if entity is None:
                logger.warning(f"No EntityPlayer found in XML for {steam_id}")
                return None

            # Helper to get attribute value
            def get_attr(path: str, attr: str = "value", default: Any = 0):
                elem = entity.find(path)
                if elem is not None:
                    return elem.get(attr, default)
                return default

            # Parse basic info
            stats = SDTDPlayerStats(
                steam_id=steam_id,
                entity_id=SDTDStatsParser._safe_int(entity.get("id", 0)),
                player_name=entity.get("name", "Unknown")
            )

            # Progression
            stats.level = SDTDStatsParser._safe_int(get_attr('.//stat[@name="Level"]'))
            stats.experience = SDTDStatsParser._safe_int(get_attr('.//stat[@name="Experience"]'))
            stats.skill_points = SDTDStatsParser._safe_int(get_attr('.//stat[@name="SkillPoints"]'))

            # Combat
            stats.zombies_killed = SDTDStatsParser._safe_int(get_attr('.//stat[@name="ZombieKills"]'))
            stats.players_killed = SDTDStatsParser._safe_int(get_attr('.//stat[@name="PlayerKills"]'))
            stats.deaths = SDTDStatsParser._safe_int(get_attr('.//stat[@name="Deaths"]'))
            stats.score = SDTDStatsParser._safe_int(get_attr('.//stat[@name="Score"]'))

            # Survival
            stats.total_playtime_seconds = SDTDStatsParser._safe_int(get_attr('.//stat[@name="LongestLife"]'))
            stats.total_item_crafts = SDTDStatsParser._safe_int(get_attr('.//stat[@name="ItemsCrafted"]'))
            stats.distance_walked = SDTDStatsParser._safe_float(get_attr('.//stat[@name="DistanceWalked"]'))
            stats.distance_run = SDTDStatsParser._safe_float(get_attr('.//stat[@name="DistanceRun"]'))

            # Position
            pos = entity.find('.//property[@name="position"]')
            if pos is not None:
                pos_str = pos.get("value", "0,0,0")
                try:
                    x, y, z = map(float, pos_str.split(','))
                    stats.position_x = x
                    stats.position_y = y
                    stats.position_z = z
                except (ValueError, IndexError):
                    logger.warning(f"Failed to parse position: {pos_str}")

            # Last spawn position
            spawn_pos = entity.find('.//property[@name="spawnPosition"]')
            if spawn_pos is not None:
                spawn_str = spawn_pos.get("value", "0,0,0")
                try:
                    x, y, z = map(float, spawn_str.split(','))
                    stats.last_spawn_position_x = x
                    stats.last_spawn_position_y = y
                    stats.last_spawn_position_z = z
                except (ValueError, IndexError):
                    pass

            # Health/Stamina
            stats.health = SDTDStatsParser._safe_int(get_attr('.//stat[@name="Health"]', default=100))
            stats.stamina = SDTDStatsParser._safe_int(get_attr('.//stat[@name="Stamina"]', default=100))

            # Custom tracking (these may not exist in vanilla XML, track separately)
            # We'll calculate these from game events instead
            stats.blood_moons_survived = 0  # Track via event system
            stats.legendary_items_crafted = 0  # Track via crafting log
            stats.pois_explored = 0  # Track via position tracking

            stats.last_updated = datetime.utcnow().isoformat()

            logger.debug(f"✅ Parsed stats for {stats.player_name} (L{stats.level}, {stats.zombies_killed} kills)")
            return stats

        except ET.ParseError as e:
            logger.error(f"❌ XML parse error for {steam_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error parsing stats for {steam_id}: {e}", exc_info=True)
            return None

    @staticmethod
    async def fetch_stats_from_amp(
        amp_session,
        instance_id: str,
        steam_id: str,
        world_name: str = "Navezgane"
    ) -> Optional[SDTDPlayerStats]:
        """
        Fetch player stats from AMP-managed server.

        Args:
            amp_session: Authenticated AMPSession instance
            instance_id: AMP instance ID
            steam_id: Player's Steam ID (e.g., "76561198012345678")
            world_name: World save name (default: "Navezgane")

        Returns:
            SDTDPlayerStats or None if file not found
        """
        # Try common save file locations
        possible_paths = [
            f"saves/{world_name}/Player/{steam_id}.ttp",
            f"Saves/{world_name}/Player/{steam_id}.ttp",
            f"7DaysToDie/Saves/{world_name}/Player/{steam_id}.ttp",
        ]

        for file_path in possible_paths:
            try:
                xml_content = await amp_session.get_file_contents(instance_id, file_path)
                if xml_content:
                    stats = SDTDStatsParser.parse_xml(xml_content, steam_id)
                    if stats:
                        logger.info(f"✅ Loaded stats from {file_path}")
                        return stats
            except Exception as e:
                logger.debug(f"Failed to load {file_path}: {e}")
                continue

        logger.warning(f"⚠️ Could not find player save file for Steam ID {steam_id}")
        return None

    @staticmethod
    def calculate_stat_delta(previous: SDTDPlayerStats, current: SDTDPlayerStats) -> Dict[str, int]:
        """
        Calculate what changed between two stat snapshots.

        Returns:
            Dict of stat changes (e.g., {"zombies_killed": 15, "level": 1})
        """
        delta = {}

        # Track numeric stat changes
        numeric_fields = [
            'level', 'experience', 'zombies_killed', 'players_killed',
            'deaths', 'score', 'total_item_crafts', 'skill_points'
        ]

        for field in numeric_fields:
            prev_val = getattr(previous, field, 0)
            curr_val = getattr(current, field, 0)
            diff = curr_val - prev_val

            if diff != 0:
                delta[field] = diff

        return delta


# Example usage:
"""
from utils.amp_auth import get_amp_session
from utils.game_parsers.sdtd_stats import SDTDStatsParser

async def example():
    # Get AMP session
    amp = await get_amp_session("http://192.168.1.154:8080", "admin", "password")

    # Fetch player stats
    stats = await SDTDStatsParser.fetch_stats_from_amp(
        amp_session=amp,
        instance_id="your-instance-id",
        steam_id="76561198012345678",
        world_name="Navezgane"
    )

    if stats:
        print(f"Player: {stats.player_name}")
        print(f"Level: {stats.level}")
        print(f"Zombies Killed: {stats.zombies_killed}")
        print(f"Position: ({stats.position_x}, {stats.position_y}, {stats.position_z})")
"""
