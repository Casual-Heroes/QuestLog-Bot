# cogs/flair_sync.py - QuestLog flair -> Discord role sync
#
# Polls the compatibility source table while recording one durable receipt per
# target community. Only role IDs created and bound by Warden are managed.

import asyncio
import hashlib
import time
import discord
from discord.ext import commands
from sqlalchemy import text
from config import db_session_scope, logger

POLL_INTERVAL = 10    # seconds between polls
FLAIR_ROLE_PREFIX = 'Flair: '
FLAIR_ROLE_COLOR  = discord.Color.default()
TERMINAL_RECEIPT_STATUSES = frozenset({'delivered', 'skipped_not_member'})


class FlairSyncCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._sync_task = None
        self._retry_after = {}

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
        now = int(time.time())
        with db_session_scope() as db:
            rows = db.execute(text(
                "SELECT id, web_user_id, action, flair_emoji, flair_name "
                "FROM discord_pending_role_updates "
                "WHERE processed_at IS NULL "
                "ORDER BY created_at ASC LIMIT 20"
            )).fetchall()

        for row in rows:
            row_id, web_user_id, action, flair_emoji, flair_name = row
            if self._retry_after.get(row_id, 0) > now:
                continue

            try:
                await self._apply_flair_update(
                    row_id,
                    web_user_id,
                    action,
                    flair_emoji or '',
                    flair_name or '',
                )
            except Exception as e:
                # Do not acknowledge a failed Discord mutation. The operation
                # is idempotent and will be retried after a bounded delay.
                self._retry_after[row_id] = now + 60
                logger.warning(
                    f'FlairSync: row {row_id} retained for retry for user '
                    f'{web_user_id}: {e}'
                )
                continue

            with db_session_scope() as db:
                db.execute(text(
                    "UPDATE discord_pending_role_updates SET processed_at = :now WHERE id = :id"
                ), {'now': now, 'id': row_id})
                db.commit()
            self._retry_after.pop(row_id, None)

    async def _apply_flair_update(self, source_update_id: int, web_user_id: int,
                                  action: str, flair_emoji: str, flair_name: str):
        """Deliver one source update independently to every opted-in guild."""
        with db_session_scope() as db:
            result = db.execute(text(
                "SELECT discord_id FROM web_users WHERE id = :uid AND discord_id IS NOT NULL"
            ), {'uid': web_user_id}).fetchone()

            if not result or not result[0]:
                return  # User hasn't linked their Discord account

            opted_in = db.execute(text(
                "SELECT guild_id FROM guilds WHERE flair_sync_enabled = 1 AND bot_present = 1"
            )).fetchall()
            opted_in_ids = sorted({int(row[0]) for row in opted_in})

        if not opted_in_ids:
            return

        discord_user_id = int(result[0])

        failures = []
        for guild_id in opted_in_ids:
            if self._receipt_status(source_update_id, guild_id) in TERMINAL_RECEIPT_STATUSES:
                continue

            guild = self.bot.get_guild(guild_id)
            if guild is None:
                message = 'Bot is not currently connected to the opted-in guild'
                self._record_receipt(source_update_id, guild_id, 'retryable_error', error=message)
                failures.append(f'{guild_id}: {message}')
                continue
            try:
                status, role_id = await self._sync_guild_flair(
                    guild, discord_user_id, action, flair_emoji, flair_name
                )
            except Exception as e:
                self._record_receipt(
                    source_update_id,
                    guild_id,
                    'retryable_error',
                    error=str(e),
                )
                failures.append(f'{guild.id}: {e}')
            else:
                self._record_receipt(
                    source_update_id,
                    guild_id,
                    status,
                    role_id=role_id,
                    processed=True,
                )

        if failures:
            raise RuntimeError('; '.join(failures))

    def _receipt_status(self, source_update_id: int, guild_id: int):
        with db_session_scope() as db:
            row = db.execute(text(
                "SELECT status FROM warden_flair_delivery_receipts "
                "WHERE source_update_id=:source AND guild_id=:guild LIMIT 1"
            ), {'source': source_update_id, 'guild': guild_id}).fetchone()
        return row[0] if row else None

    def _record_receipt(self, source_update_id: int, guild_id: int, status: str,
                        *, role_id=None, error=None, processed=False):
        now = int(time.time())
        with db_session_scope() as db:
            db.execute(text(
                "INSERT INTO warden_flair_delivery_receipts "
                "(source_update_id,guild_id,status,attempts,role_id,last_error,created_at,updated_at,processed_at) "
                "VALUES (:source,:guild,:status,1,:role,:error,:now,:now,:processed) "
                "ON DUPLICATE KEY UPDATE status=VALUES(status),attempts=attempts+1,"
                "role_id=VALUES(role_id),last_error=VALUES(last_error),"
                "updated_at=VALUES(updated_at),processed_at=VALUES(processed_at)"
            ), {
                'source': source_update_id,
                'guild': guild_id,
                'status': status,
                'role': role_id,
                'error': (error or '')[:1000] or None,
                'now': now,
                'processed': now if processed else None,
            })

    def _owned_role_ids(self, guild_id: int) -> set[int]:
        with db_session_scope() as db:
            rows = db.execute(text(
                "SELECT role_id FROM warden_flair_role_bindings WHERE guild_id=:guild"
            ), {'guild': guild_id}).fetchall()
        return {int(row[0]) for row in rows}

    def _bound_role_id(self, guild_id: int, flair_key: str):
        with db_session_scope() as db:
            row = db.execute(text(
                "SELECT role_id FROM warden_flair_role_bindings "
                "WHERE guild_id=:guild AND flair_key=:flair LIMIT 1"
            ), {'guild': guild_id, 'flair': flair_key}).fetchone()
        return int(row[0]) if row else None

    def _bind_role(self, guild_id: int, flair_key: str, role: discord.Role):
        now = int(time.time())
        with db_session_scope() as db:
            db.execute(text(
                "INSERT INTO warden_flair_role_bindings "
                "(guild_id,flair_key,role_id,role_name,created_at,updated_at) "
                "VALUES (:guild,:flair,:role,:name,:now,:now) "
                "ON DUPLICATE KEY UPDATE role_id=VALUES(role_id),"
                "role_name=VALUES(role_name),updated_at=VALUES(updated_at)"
            ), {
                'guild': guild_id,
                'flair': flair_key,
                'role': role.id,
                'name': role.name[:100],
                'now': now,
            })

    @staticmethod
    def _safe_role_name(flair_emoji: str, flair_name: str) -> str:
        visible = ' '.join(
            ''.join(ch for ch in f'{flair_emoji} {flair_name}' if ch.isprintable()).split()
        )
        return f'{FLAIR_ROLE_PREFIX}{visible}'[:100].rstrip()

    async def _target_role(self, guild: discord.Guild, flair_emoji: str,
                           flair_name: str):
        target_name = self._safe_role_name(flair_emoji, flair_name)
        if target_name == FLAIR_ROLE_PREFIX.strip():
            return None

        flair_key = hashlib.sha256(
            f'{flair_emoji.strip()}\0{flair_name.strip()}'.encode('utf-8')
        ).hexdigest()
        bound_role_id = self._bound_role_id(guild.id, flair_key)
        role = guild.get_role(bound_role_id) if bound_role_id else None
        if role:
            if role.name != target_name:
                await role.edit(name=target_name, reason='QuestLog flair name refresh')
            return role

        # Never adopt a same-named administrator-created role. Discord permits
        # duplicate role names, and the stored role ID is the ownership proof.
        role = await guild.create_role(
            name=target_name,
            color=FLAIR_ROLE_COLOR,
            permissions=discord.Permissions.none(),
            hoist=False,
            mentionable=False,
            reason='QuestLog flair role auto-created',
        )
        bot_member = guild.me
        bot_top = max(
            (r.position for r in bot_member.roles if not r.is_default()),
            default=1,
        )
        try:
            await role.edit(position=max(1, bot_top - 1))
        except discord.HTTPException:
            pass
        self._bind_role(guild.id, flair_key, role)
        logger.info('FlairSync: created owned role %s in guild %s', role.id, guild.id)
        return role

    async def _sync_guild_flair(self, guild: discord.Guild, user_id: int,
                                 action: str, flair_emoji: str, flair_name: str):
        """Update flair role for user in a single Discord guild."""
        member = guild.get_member(user_id)
        if not member:
            return 'skipped_not_member', None

        target_role = None
        if action == 'set_flair' and (flair_emoji or flair_name):
            target_role = await self._target_role(guild, flair_emoji, flair_name)

        owned_role_ids = self._owned_role_ids(guild.id)
        old_flair_roles = [
            role for role in member.roles
            if role.id in owned_role_ids and (target_role is None or role.id != target_role.id)
        ]
        if old_flair_roles:
            await member.remove_roles(*old_flair_roles, reason='QuestLog flair update')

        if target_role and target_role not in member.roles:
            await member.add_roles(target_role, reason='QuestLog flair update')

        return 'delivered', target_role.id if target_role else None


def setup(bot: commands.Bot):
    bot.add_cog(FlairSyncCog(bot))
