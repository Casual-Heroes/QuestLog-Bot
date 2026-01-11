"""
Guild Sync Cog - Automatically syncs guild data when changes occur
"""

import asyncio
import time
import os
from dotenv import load_dotenv
from discord.ext import commands
import discord
from config import logger

# Load environment variables
load_dotenv()


class AutoGuildSyncCog(commands.Cog):
    """Handles automatic syncing of guild data (roles, channels) when changes occur."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sync_cooldowns = {}  # guild_id: last_sync_timestamp
        self.sync_queue = set()  # Set of guild_ids pending sync
        self.COOLDOWN_SECONDS = 5  # Reduced from 60 to 5 seconds for faster updates

    async def queue_sync(self, guild_id: int):
        """
        Queue a guild for syncing with cooldown protection.

        If a sync was done less than 60 seconds ago, the sync will be queued
        and executed after the cooldown period.
        """
        current_time = time.time()
        last_sync = self.sync_cooldowns.get(guild_id, 0)
        time_since_sync = current_time - last_sync

        if time_since_sync < self.COOLDOWN_SECONDS:
            # Too soon - add to queue
            if guild_id not in self.sync_queue:
                self.sync_queue.add(guild_id)
                wait_time = self.COOLDOWN_SECONDS - time_since_sync
                logger.debug(f"Queueing sync for guild {guild_id}, will sync in {wait_time:.1f}s")

                # Schedule sync after cooldown
                await asyncio.sleep(wait_time)
                if guild_id in self.sync_queue:
                    self.sync_queue.remove(guild_id)
                    await self._perform_sync(guild_id)
        else:
            # Cooldown passed - sync now
            await self._perform_sync(guild_id)

    async def _perform_sync(self, guild_id: int):
        """Perform the actual guild sync via API."""
        try:
            import aiohttp
            import os

            bot_api_port = int(os.getenv('BOT_API_PORT', 8001))
            api_token = os.getenv('DISCORD_BOT_API_TOKEN')
            url = f"http://localhost:{bot_api_port}/api/sync/{guild_id}"

            # Prepare headers with Bearer token for authentication
            headers = {}
            if api_token:
                headers['Authorization'] = f'Bearer {api_token}'

            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('success'):
                            self.sync_cooldowns[guild_id] = time.time()
                            logger.info(f"✅ Auto-synced guild {guild_id} - roles and channels updated")
                        else:
                            logger.error(f"Failed to sync guild {guild_id}: {data.get('error', 'Unknown error')}")
                    else:
                        logger.error(f"Failed to sync guild {guild_id}: HTTP {response.status}")
        except Exception as e:
            logger.error(f"Error syncing guild {guild_id}: {e}", exc_info=True)

    # ===== Role Events =====

    @commands.Cog.listener()
    async def on_guild_role_create(self, role: discord.Role):
        """Triggered when a role is created."""
        logger.debug(f"Role created in {role.guild.name}: {role.name}")
        await self.queue_sync(role.guild.id)

    @commands.Cog.listener()
    async def on_guild_role_delete(self, role: discord.Role):
        """Triggered when a role is deleted."""
        logger.debug(f"Role deleted in {role.guild.name}: {role.name}")
        await self.queue_sync(role.guild.id)

    @commands.Cog.listener()
    async def on_guild_role_update(self, before: discord.Role, after: discord.Role):
        """Triggered when a role is updated (name, permissions, color, etc)."""
        # Only sync if name changed (avoid syncing on every permission change)
        if before.name != after.name:
            logger.debug(f"Role renamed in {after.guild.name}: {before.name} → {after.name}")
            await self.queue_sync(after.guild.id)

    # ===== Channel Events =====

    @commands.Cog.listener()
    async def on_guild_channel_create(self, channel: discord.abc.GuildChannel):
        """Triggered when a channel is created."""
        logger.debug(f"Channel created in {channel.guild.name}: #{channel.name}")
        await self.queue_sync(channel.guild.id)

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel):
        """Triggered when a channel is deleted."""
        logger.debug(f"Channel deleted in {channel.guild.name}: #{channel.name}")
        await self.queue_sync(channel.guild.id)

    @commands.Cog.listener()
    async def on_guild_channel_update(self, before: discord.abc.GuildChannel, after: discord.abc.GuildChannel):
        """Triggered when a channel is updated (name, permissions, category, etc)."""
        # Only sync if name or category changed
        if before.name != after.name or getattr(before, 'category', None) != getattr(after, 'category', None):
            logger.debug(f"Channel updated in {after.guild.name}: {before.name} → {after.name}")
            await self.queue_sync(after.guild.id)


def setup(bot: commands.Bot):
    bot.add_cog(AutoGuildSyncCog(bot))
