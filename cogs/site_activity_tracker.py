"""
Site Activity Tracker - Discord Game Activity Monitor (Database-Driven)
Tracks Discord members playing specific games and writes data to JSON for website display.

Configuration is fully managed via the QuestLog admin panel (bot owner only).
No more hardcoded configs - everything is pulled from the database!
"""

import discord
from discord.ext import commands, tasks
import json
from pathlib import Path
import logging
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Data file path - shared with Django website
DATA_FILE = Path("/srv/ch-webserver/gamingactivity/activity_data.json")

# Database connection details from environment variables
# Use the warden database where site_activity_games tables are stored
DB_CONFIG = {
    'database': os.getenv('DB_NAME', 'warden'),
    'user': os.getenv('DB_USER', 'warden'),
    'password': os.getenv('DB_PASSWORD'),
    'unix_socket': os.getenv('DB_SOCKET', '/var/run/mysqld/mysqld.sock')
}

# Logger
logger = logging.getLogger(__name__)


class SiteActivityTracker(commands.Cog):
    """
    Tracks Discord game activity and writes to JSON for website display.

    PHASE 2: Fully database-driven configuration.
    Reads game config from site_activity_games and site_activity_guild_roles tables.
    """

    def __init__(self, bot):
        self.bot = bot

        # Configuration loaded from database
        self.games_config = {}  # {game_key: {"keywords": [...], "roles": [(guild_id, role_id), ...]}}

        self.player_counts = {}
        self.load_config_from_db()  # Initial load
        self.track_activity.start()
        logger.info("[SiteActivityTracker] Initialized with database-driven config.")

    def cog_unload(self):
        """Stop the tracking loop when cog is unloaded."""
        self.track_activity.cancel()
        logger.info("[SiteActivityTracker] Unloaded and stopped tracking loop.")

    def load_config_from_db(self):
        """
        Load game tracking configuration from database.

        Reads from:
        - site_activity_games: game metadata, activity keywords
        - site_activity_guild_roles: Discord guild/role mappings

        Only loads games with game_type 'discord' or 'both' and is_active=True.
        """
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            # Query active Discord games and their role mappings
            query = """
                SELECT
                    g.game_key,
                    g.activity_keywords,
                    r.guild_id,
                    r.role_id
                FROM site_activity_games g
                LEFT JOIN site_activity_guild_roles r ON g.id = r.game_id AND r.is_active = 1
                WHERE g.is_active = 1
                  AND g.game_type IN ('discord', 'both')
                ORDER BY g.game_key
            """

            cursor.execute(query)
            rows = cursor.fetchall()

            # Build configuration
            config = {}
            for row in rows:
                game_key = row['game_key']

                if game_key not in config:
                    # Parse activity keywords from JSON
                    try:
                        keywords = json.loads(row['activity_keywords']) if row['activity_keywords'] else []
                    except json.JSONDecodeError:
                        logger.warning(f"[SiteActivityTracker] Invalid JSON for {game_key} keywords: {row['activity_keywords']}")
                        keywords = []

                    config[game_key] = {
                        "keywords": keywords,
                        "roles": []
                    }

                # Add role mapping if exists
                if row['guild_id'] and row['role_id']:
                    config[game_key]["roles"].append((
                        int(row['guild_id']),
                        int(row['role_id'])
                    ))

            self.games_config = config

            cursor.close()
            conn.close()

            logger.info(f"[SiteActivityTracker] Loaded config for {len(config)} games from database")
            for game_key, data in config.items():
                logger.info(f"  - {game_key}: {len(data['keywords'])} keywords, {len(data['roles'])} role mappings")

        except Error as e:
            logger.error(f"[SiteActivityTracker] Database error loading config: {e}", exc_info=True)
            # Keep existing config if load fails
        except Exception as e:
            logger.error(f"[SiteActivityTracker] Unexpected error loading config: {e}", exc_info=True)

    @tasks.loop(seconds=30)
    async def track_activity(self):
        """
        Main tracking loop - runs every 30 seconds.

        Counts members for each tracked game role and checks their activity status.
        Writes results to JSON file for website consumption.
        """
        logger.debug("[SiteActivityTracker] Loop tick - starting activity scan")

        # Reload config from database every iteration (hot reload!)
        self.load_config_from_db()

        counts = {}

        for game_key, config in self.games_config.items():
            keywords = config["keywords"]
            roles = config["roles"]

            if not roles:
                logger.debug(f"[SiteActivityTracker] {game_key} has no role mappings - skipping")
                continue

            game_total = 0
            game_online = 0
            game_active = 0

            # Track across all role mappings for this game
            for guild_id, role_id in roles:
                guild = self.bot.get_guild(guild_id)
                if not guild:
                    # Debug: show what guilds we CAN see
                    available_guilds = [(g.id, g.name) for g in self.bot.guilds]
                    logger.warning(f"[SiteActivityTracker] Guild {guild_id} not found for {game_key}. Bot is in {len(self.bot.guilds)} guilds: {available_guilds}")
                    continue

                role = guild.get_role(role_id)
                if not role:
                    logger.warning(f"[SiteActivityTracker] Role {role_id} not found in guild {guild.name} ({guild_id}) for {game_key}")
                    continue

                # Count members
                total = len(role.members)
                online = 0
                active = 0

                for member in role.members:
                    # Count online members (not offline/invisible)
                    if member.status != discord.Status.offline:
                        online += 1

                        # Check if they're actively playing this game
                        if member.activity and hasattr(member.activity, 'name') and member.activity.name:
                            activity_name = member.activity.name

                            # Check against activity keywords for this game
                            if any(keyword.lower() in activity_name.lower() for keyword in keywords):
                                active += 1
                                logger.debug(f"[SiteActivityTracker] {member.name} is actively playing {game_key} (activity: {activity_name})")

                game_total += total
                game_online += online
                game_active += active

                logger.debug(f"[SiteActivityTracker] {game_key} in {guild.name}: {total} total, {online} online, {active} active")

            # Store aggregated counts for this game
            counts[game_key] = {
                "total": game_total,
                "online": game_online,
                "active": game_active
            }

        # Save to JSON file
        self.player_counts = counts
        try:
            # Ensure directory exists
            DATA_FILE.parent.mkdir(parents=True, exist_ok=True)

            with DATA_FILE.open("w") as f:
                json.dump(counts, f, indent=2)

            logger.info(f"[SiteActivityTracker] Saved counts to {DATA_FILE}: {counts}")
        except Exception as e:
            logger.error(f"[SiteActivityTracker] Failed to write to {DATA_FILE}: {e}", exc_info=True)

    @track_activity.before_loop
    async def before_track(self):
        """Wait for bot to be ready before starting the tracking loop."""
        logger.info("[SiteActivityTracker] Waiting for bot to be ready...")
        await self.bot.wait_until_ready()
        logger.info("[SiteActivityTracker] Bot ready. Starting database-driven activity tracking.")

    @commands.command(name="activitystatus")
    @commands.is_owner()
    async def activity_status(self, ctx):
        """
        Show current activity tracking status (bot owner only).

        Displays the latest player counts for all tracked games.
        """
        if not self.player_counts:
            await ctx.send("⚠️ No activity data available yet. Tracker may still be initializing or no games configured.")
            return

        embed = discord.Embed(
            title="🎮 Site Activity Tracker Status",
            description=f"Tracking {len(self.player_counts)} games from database config",
            color=discord.Color.green()
        )

        for game, stats in sorted(self.player_counts.items()):
            embed.add_field(
                name=f"**{game}**",
                value=f"👥 Total: {stats['total']}\n🟢 Online: {stats['online']}\n🎮 Active: {stats['active']}",
                inline=True
            )

        embed.set_footer(text=f"✅ Database-driven | Data file: {DATA_FILE}")
        await ctx.send(embed=embed)

    @commands.command(name="reloadtracker")
    @commands.is_owner()
    async def reload_tracker(self, ctx):
        """
        Manually reload tracker configuration from database (bot owner only).

        Forces an immediate reload of game config from the database.
        """
        await ctx.send("🔄 Reloading configuration from database...")

        old_count = len(self.games_config)
        self.load_config_from_db()
        new_count = len(self.games_config)

        embed = discord.Embed(
            title="✅ Configuration Reloaded",
            description=f"Loaded {new_count} games from database (was {old_count})",
            color=discord.Color.green()
        )

        for game_key, config in self.games_config.items():
            embed.add_field(
                name=game_key,
                value=f"Keywords: {len(config['keywords'])}\nRoles: {len(config['roles'])}",
                inline=True
            )

        await ctx.send(embed=embed)


def setup(bot):
    """Setup function for loading the cog."""
    bot.add_cog(SiteActivityTracker(bot))
    logger.info("[SiteActivityTracker] Database-driven cog loaded successfully.")
