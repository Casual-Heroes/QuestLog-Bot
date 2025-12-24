"""
Guild Sync Cog - Syncs Discord guild data to database
Periodically updates member counts and online status for all guilds.
"""
import discord
from discord.ext import commands, tasks
import logging
import sys
import asyncio

sys.path.insert(0, '..')
from db import get_db_session
from models import Guild, GuildMember

logger = logging.getLogger("guild_sync")


class GuildSyncCog(commands.Cog):
    """
    Background task to sync Discord guild data to the database.
    Runs every 15 minutes to update member counts, online status, and member avatars.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sync_guild_stats.start()
        logger.info("✅ Guild Sync Cog loaded - will sync member stats and avatars every 15 minutes")

    def cog_unload(self):
        """Stop background tasks when cog unloads."""
        self.sync_guild_stats.cancel()

    @tasks.loop(minutes=30)
    async def sync_guild_stats(self):
        """
        Sync member counts, online status, and member avatars for all guilds.
        Runs every 15 minutes with rate limiting built-in.
        """
        try:
            logger.info(f"🔄 Starting guild stats sync for {len(self.bot.guilds)} guilds")

            for i, guild in enumerate(self.bot.guilds, 1):
                try:
                    # Count total members (excluding bots)
                    member_count = sum(1 for m in guild.members if not m.bot)

                    # Count online members (excluding bots)
                    # Online includes: online, idle, dnd (not offline/invisible)
                    online_count = sum(
                        1 for m in guild.members
                        if not m.bot and m.status != discord.Status.offline
                    )

                    # Get guild icon hash (for Discord CDN URLs)
                    guild_icon_hash = guild.icon.key if guild.icon else None

                    # Update database
                    with get_db_session() as db:
                        guild_record = db.query(Guild).filter_by(guild_id=guild.id).first()
                        if guild_record:
                            guild_record.member_count = member_count
                            guild_record.online_count = online_count
                            guild_record.guild_icon_hash = guild_icon_hash
                            db.commit()
                            logger.debug(
                                f"Updated {guild.name} ({guild.id}): "
                                f"{member_count} members, {online_count} online"
                            )
                        else:
                            logger.warning(f"Guild {guild.id} not found in database")

                    # Sync member avatars (only for non-bot members in the database)
                    with get_db_session() as db:
                        synced_count = 0
                        for member in guild.members:
                            if member.bot:
                                continue

                            # Check if member exists in database
                            guild_member = db.query(GuildMember).filter_by(
                                guild_id=guild.id,
                                user_id=member.id
                            ).first()

                            if guild_member:
                                # Get avatar hash (None if using default avatar)
                                avatar_hash = member.avatar.key if member.avatar else None

                                # Only update if avatar changed or was not set
                                if guild_member.avatar_hash != avatar_hash:
                                    guild_member.avatar_hash = avatar_hash
                                    guild_member.display_name = member.display_name
                                    guild_member.username = member.name
                                    synced_count += 1

                        db.commit()
                        logger.debug(f"Synced {synced_count} member avatars for {guild.name}")

                    # Add delay between guilds to avoid rate limiting (0.5s per guild)
                    if i < len(self.bot.guilds):
                        await asyncio.sleep(0.5)

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

            # Get guild icon hash (for Discord CDN URLs)
            guild_icon_hash = guild.icon.key if guild.icon else None

            # Update database
            with get_db_session() as db:
                guild_record = db.query(Guild).filter_by(guild_id=guild.id).first()
                if guild_record:
                    guild_record.member_count = member_count
                    guild_record.online_count = online_count
                    guild_record.guild_icon_hash = guild_icon_hash
                    db.commit()
                    logger.info(
                        f"✅ Synced {guild.name} ({guild.id}): "
                        f"{member_count} members, {online_count} online"
                    )
                else:
                    logger.warning(f"Guild {guild.id} not found in database")

            # Sync member avatars (only for non-bot members in the database)
            with get_db_session() as db:
                synced_count = 0
                for member in guild.members:
                    if member.bot:
                        continue

                    # Check if member exists in database
                    guild_member = db.query(GuildMember).filter_by(
                        guild_id=guild.id,
                        user_id=member.id
                    ).first()

                    if guild_member:
                        # Get avatar hash (None if using default avatar)
                        avatar_hash = member.avatar.key if member.avatar else None

                        # Only update if avatar changed or was not set
                        if guild_member.avatar_hash != avatar_hash:
                            guild_member.avatar_hash = avatar_hash
                            guild_member.display_name = member.display_name
                            guild_member.username = member.name
                            synced_count += 1

                db.commit()
                logger.info(f"Synced {synced_count} member avatars for {guild.name}")

        except Exception as e:
            logger.error(f"Failed to sync stats for guild {guild.id}: {e}")
            raise

    async def sync_all_guilds(self):
        """Sync all guilds on-demand (same as scheduled task)."""
        await self.sync_guild_stats()


def setup(bot: commands.Bot):
    bot.add_cog(GuildSyncCog(bot))
