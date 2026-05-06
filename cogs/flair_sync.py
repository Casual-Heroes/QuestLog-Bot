# cogs/flair_sync.py - QuestLog flair -> Discord role sync
#
# Polls discord_pending_role_updates every 10 seconds.
# When a QuestLog user equips or unequips a flair on the site, this cog:
#   1. Looks up their discord_id from web_users
#   2. For every guild the bot shares with that user that has flair_sync_enabled=True:
#      - Removes all roles whose name starts with "Flair: "
#      - If action='set_flair': finds or creates "Flair: {emoji} {name}" role and assigns it
# Guild must opt in via the QuestLog Discord dashboard (flair_sync_enabled toggle).

import asyncio
import time
import discord
from discord.ext import commands
from sqlalchemy import text
from config import db_session_scope, logger

POLL_INTERVAL = 10    # seconds between polls
FLAIR_ROLE_PREFIX = 'Flair: '
FLAIR_ROLE_COLOR  = discord.Color.default()


class FlairSyncCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._sync_task = None

    @commands.Cog.listener()
    async def on_ready(self):
        if not self._sync_task or self._sync_task.done():
            self._sync_task = asyncio.ensure_future(self._poll_loop())
            logger.info('FlairSyncCog: poll loop started')

    async def _poll_loop(self):
        await asyncio.sleep(5)  # brief startup delay
        while True:
            try:
                await self._process_pending_updates()
            except Exception as e:
                logger.error(f'FlairSync poll loop error: {e}', exc_info=True)
            await asyncio.sleep(POLL_INTERVAL)

    async def _process_pending_updates(self):
        with db_session_scope() as db:
            rows = db.execute(text(
                "SELECT id, web_user_id, action, flair_emoji, flair_name "
                "FROM discord_pending_role_updates "
                "WHERE processed_at IS NULL "
                "ORDER BY created_at ASC LIMIT 20"
            )).fetchall()

            if not rows:
                return

            for row in rows:
                row_id, web_user_id, action, flair_emoji, flair_name = row
                try:
                    await self._apply_flair_update(web_user_id, action, flair_emoji or '', flair_name or '')
                except Exception as e:
                    logger.warning(f'FlairSync: error processing row {row_id} for user {web_user_id}: {e}')

                # Mark processed regardless - avoid infinite retry on hard failures
                db.execute(text(
                    "UPDATE discord_pending_role_updates SET processed_at = :now WHERE id = :id"
                ), {'now': int(time.time()), 'id': row_id})

            db.commit()

    async def _apply_flair_update(self, web_user_id: int, action: str, flair_emoji: str, flair_name: str):
        """Apply flair role change across all Discord guilds that have opted in."""
        with db_session_scope() as db:
            result = db.execute(text(
                "SELECT discord_id FROM web_users WHERE id = :uid AND discord_id IS NOT NULL"
            ), {'uid': web_user_id}).fetchone()

            if not result or not result[0]:
                return  # User hasn't linked their Discord account

            opted_in = db.execute(text(
                "SELECT guild_id FROM guilds WHERE flair_sync_enabled = 1 AND bot_present = 1"
            )).fetchall()
            opted_in_ids = {int(row[0]) for row in opted_in}

        if not opted_in_ids:
            return

        discord_user_id = int(result[0])

        for guild in self.bot.guilds:
            if guild.id not in opted_in_ids:
                continue  # Guild has not opted in - skip
            try:
                await self._sync_guild_flair(guild, discord_user_id, action, flair_emoji, flair_name)
            except Exception as e:
                logger.debug(f'FlairSync: skipped guild {guild.id} for user {discord_user_id}: {e}')

    async def _sync_guild_flair(self, guild: discord.Guild, user_id: int,
                                 action: str, flair_emoji: str, flair_name: str):
        """Update flair role for user in a single Discord guild."""
        member = guild.get_member(user_id)
        if not member:
            return  # User not in this guild

        # Remove all current flair roles
        old_flair_roles = [r for r in member.roles if r.name.startswith(FLAIR_ROLE_PREFIX)]
        if old_flair_roles:
            await member.remove_roles(*old_flair_roles, reason='QuestLog flair update')

        if action == 'set_flair' and (flair_emoji or flair_name):
            target_name = f'{FLAIR_ROLE_PREFIX}{flair_emoji} {flair_name}'.strip()

            # Find or create the flair role
            role = discord.utils.get(guild.roles, name=target_name)
            if not role:
                # Create role just below the bot's highest role
                bot_member = guild.get_member(self.bot.user.id)
                bot_top = max((r.position for r in bot_member.roles if not r.is_default()), default=1)
                role = await guild.create_role(
                    name=target_name,
                    color=FLAIR_ROLE_COLOR,
                    reason='QuestLog flair role auto-created',
                )
                try:
                    await role.edit(position=max(1, bot_top - 1))
                except discord.HTTPException:
                    pass  # Position edit is best-effort
                logger.info(f'FlairSync: created role "{target_name}" in guild {guild.name}')

            await member.add_roles(role, reason='QuestLog flair update')


def setup(bot: commands.Bot):
    bot.add_cog(FlairSyncCog(bot))
