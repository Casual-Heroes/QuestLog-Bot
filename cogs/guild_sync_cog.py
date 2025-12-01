"""
Guild Sync Cog - Syncs Discord guild data to database
Periodically updates member counts and online status for all guilds.
"""
import discord
from discord.ext import commands, tasks
import logging
import sys

sys.path.insert(0, '..')
from db import get_db_session
from models import Guild

logger = logging.getLogger("guild_sync")


class GuildSyncCog(commands.Cog):
    """
    Background task to sync Discord guild data to the database.
    Runs every 5 minutes to update member counts and online status.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sync_guild_stats.start()
        logger.info("✅ Guild Sync Cog loaded - will sync member stats every 5 minutes")

    def cog_unload(self):
        """Stop background tasks when cog unloads."""
        self.sync_guild_stats.cancel()

    @tasks.loop(minutes=5)
    async def sync_guild_stats(self):
        """
        Sync member counts and online status for all guilds.
        Runs every 5 minutes with rate limiting built-in.
        """
        try:
            logger.info(f"🔄 Starting guild stats sync for {len(self.bot.guilds)} guilds")

            for guild in self.bot.guilds:
                try:
                    # Count total members (excluding bots)
                    member_count = sum(1 for m in guild.members if not m.bot)

                    # Count online members (excluding bots)
                    # Online includes: online, idle, dnd (not offline/invisible)
                    online_count = sum(
                        1 for m in guild.members
                        if not m.bot and m.status != discord.Status.offline
                    )

                    # Update database
                    with get_db_session() as db:
                        guild_record = db.query(Guild).filter_by(guild_id=guild.id).first()
                        if guild_record:
                            guild_record.member_count = member_count
                            guild_record.online_count = online_count
                            db.commit()
                            logger.debug(
                                f"Updated {guild.name} ({guild.id}): "
                                f"{member_count} members, {online_count} online"
                            )
                        else:
                            logger.warning(f"Guild {guild.id} not found in database")

                except Exception as e:
                    logger.error(f"Failed to sync stats for guild {guild.id}: {e}")
                    continue

            logger.info("✅ Guild stats sync completed")

        except Exception as e:
            logger.error(f"Error in guild stats sync task: {e}", exc_info=True)

    @sync_guild_stats.before_loop
    async def before_sync_guild_stats(self):
        """Wait for bot to be ready before starting sync task."""
        await self.bot.wait_until_ready()
        logger.info("Bot is ready, starting guild stats sync task")

    async def sync_single_guild(self, guild: discord.Guild):
        """
        Sync a single guild's stats on-demand.
        Called by API when user clicks "Refresh Data" button.
        """
        try:
            # Count total members (excluding bots)
            member_count = sum(1 for m in guild.members if not m.bot)

            # Count online members (excluding bots)
            online_count = sum(
                1 for m in guild.members
                if not m.bot and m.status != discord.Status.offline
            )

            # Update database
            with get_db_session() as db:
                guild_record = db.query(Guild).filter_by(guild_id=guild.id).first()
                if guild_record:
                    guild_record.member_count = member_count
                    guild_record.online_count = online_count
                    db.commit()
                    logger.info(
                        f"✅ Synced {guild.name} ({guild.id}): "
                        f"{member_count} members, {online_count} online"
                    )
                else:
                    logger.warning(f"Guild {guild.id} not found in database")

        except Exception as e:
            logger.error(f"Failed to sync stats for guild {guild.id}: {e}")
            raise

    async def sync_all_guilds(self):
        """Sync all guilds on-demand (same as scheduled task)."""
        await self.sync_guild_stats()


def setup(bot: commands.Bot):
    bot.add_cog(GuildSyncCog(bot))
