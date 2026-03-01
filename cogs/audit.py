# cogs/audit.py - Full Audit Logging System
"""
Complete audit system for QuestLog.
Tracks and logs all server events for accountability and security.

RETENTION BY TIER:
- FREE: 7 days
- PREMIUM: 30 days
- PRO: 90 days

TRACKED EVENTS:
- Member: join, leave, ban, unban, kick, timeout, nickname change
- Roles: add, remove, create, delete, update
- Channels: create, delete, update
- Messages: delete, bulk delete
- Server: settings changes, permission updates
- Security: raid detection, lockdowns, verifications
"""

import time
import io
import csv
import asyncio
import json
import discord
from discord.ext import commands, tasks
from discord import SlashCommandGroup
from datetime import datetime, timedelta

from config import db_session_scope, logger, get_debug_guilds, FeatureLimits
from models import Guild, GuildModule, GuildMember, AuditLog, AuditAction


def get_guild_tier(session, guild_id: int) -> str:
    """Get the subscription tier for a guild."""
    db_guild = session.get(Guild, guild_id)
    if not db_guild:
        return "FREE"
    if db_guild.is_vip:
        return "PRO"
    return db_guild.subscription_tier.upper() if db_guild.subscription_tier else "FREE"


def has_moderation_access(session, guild_id: int) -> bool:
    """All guilds have full moderation access."""
    db_guild = session.get(Guild, guild_id)
    return db_guild is not None


def get_retention_days(session, guild_id: int) -> int:
    """Audit log retention is unlimited for all guilds."""
    return None  # None = no retention limit


# Action type to emoji mapping for display
ACTION_EMOJIS = {
    AuditAction.MEMBER_JOIN: "📥",
    AuditAction.MEMBER_LEAVE: "📤",
    AuditAction.MEMBER_BAN: "🔨",
    AuditAction.MEMBER_UNBAN: "🔓",
    AuditAction.MEMBER_KICK: "👢",
    AuditAction.MEMBER_TIMEOUT: "🔇",
    AuditAction.ROLE_ADD: "➕",
    AuditAction.ROLE_REMOVE: "➖",
    AuditAction.ROLE_CREATE: "🏷️",
    AuditAction.ROLE_DELETE: "🗑️",
    AuditAction.CHANNEL_CREATE: "📁",
    AuditAction.CHANNEL_DELETE: "🗑️",
    AuditAction.CHANNEL_UPDATE: "✏️",
    AuditAction.PERMISSION_UPDATE: "🔐",
    AuditAction.MESSAGE_DELETE: "🗑️",
    AuditAction.MESSAGE_BULK_DELETE: "🗑️",
    AuditAction.RAID_DETECTED: "🚨",
    AuditAction.LOCKDOWN_ACTIVATED: "🔒",
    AuditAction.LOCKDOWN_DEACTIVATED: "🔓",
    AuditAction.VERIFICATION_PASSED: "✅",
    AuditAction.VERIFICATION_FAILED: "❌",
}

# Action categories for filtering
ACTION_CATEGORIES = {
    "member": [
        AuditAction.MEMBER_JOIN, AuditAction.MEMBER_LEAVE,
        AuditAction.MEMBER_BAN, AuditAction.MEMBER_UNBAN,
        AuditAction.MEMBER_KICK, AuditAction.MEMBER_TIMEOUT
    ],
    "role": [
        AuditAction.ROLE_ADD, AuditAction.ROLE_REMOVE,
        AuditAction.ROLE_CREATE, AuditAction.ROLE_DELETE
    ],
    "channel": [
        AuditAction.CHANNEL_CREATE, AuditAction.CHANNEL_DELETE,
        AuditAction.CHANNEL_UPDATE, AuditAction.PERMISSION_UPDATE
    ],
    "message": [
        AuditAction.MESSAGE_DELETE, AuditAction.MESSAGE_BULK_DELETE
    ],
    "security": [
        AuditAction.RAID_DETECTED, AuditAction.LOCKDOWN_ACTIVATED,
        AuditAction.LOCKDOWN_DEACTIVATED, AuditAction.VERIFICATION_PASSED,
        AuditAction.VERIFICATION_FAILED
    ],
}


class AuditCog(commands.Cog):
    """Full audit logging system."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.cleanup_old_logs_task.start()

    def cog_unload(self):
        self.cleanup_old_logs_task.cancel()

    # ==================== HELPER METHODS ====================

    async def log_event(
        self,
        guild_id: int,
        action: AuditAction,
        actor_id: int = None,
        actor_name: str = None,
        target_id: int = None,
        target_name: str = None,
        target_type: str = None,
        reason: str = None,
        details: str = None,
        category: str = None
    ):
        """Log an audit event to the database and optionally to a channel."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, guild_id)
            if not db_guild or not db_guild.audit_logging_enabled:
                return
            if not self._event_allowed(db_guild, action):
                return

            # Create audit log entry
            log_entry = AuditLog(
                guild_id=guild_id,
                action=action,
                action_category=category,
                actor_id=actor_id,
                actor_name=actor_name,
                target_id=target_id,
                target_name=target_name,
                target_type=target_type,
                reason=reason,
                details=details,
            )
            session.add(log_entry)

            log_channel_id = db_guild.log_channel_id

        # Send to log channel if configured
        if log_channel_id:
            await self._send_log_embed(
                log_channel_id, action, actor_id, actor_name,
                target_id, target_name, target_type, reason, details
            )

    def _event_allowed(self, db_guild: Guild, action: AuditAction) -> bool:
        """Check per-event toggles; defaults to True if no config set."""
        if not db_guild.audit_logging_enabled:
            return False
        if not db_guild.audit_event_config:
            return True
        try:
            cfg = json.loads(db_guild.audit_event_config)
            return cfg.get(action.value, True)
        except Exception:
            return True

    async def _send_log_embed(
        self,
        channel_id: int,
        action: AuditAction,
        actor_id: int,
        actor_name: str,
        target_id: int,
        target_name: str,
        target_type: str,
        reason: str,
        details: str
    ):
        """Send a formatted embed to the log channel."""
        channel = self.bot.get_channel(channel_id)
        if not channel:
            return

        emoji = ACTION_EMOJIS.get(action, "📋")
        action_name = action.value.replace("_", " ").title()

        # Color based on action severity
        if action in [AuditAction.MEMBER_BAN, AuditAction.MEMBER_KICK,
                      AuditAction.RAID_DETECTED, AuditAction.LOCKDOWN_ACTIVATED]:
            color = discord.Color.red()
        elif action in [AuditAction.MEMBER_JOIN, AuditAction.VERIFICATION_PASSED,
                        AuditAction.MEMBER_UNBAN, AuditAction.LOCKDOWN_DEACTIVATED]:
            color = discord.Color.green()
        elif action in [AuditAction.MEMBER_LEAVE, AuditAction.MESSAGE_DELETE]:
            color = discord.Color.orange()
        else:
            color = discord.Color.blue()

        embed = discord.Embed(
            title=f"{emoji} {action_name}",
            color=color,
            timestamp=discord.utils.utcnow()
        )

        if actor_name:
            embed.add_field(
                name="Actor",
                value=f"{actor_name}\n(<@{actor_id}>)" if actor_id else actor_name,
                inline=True
            )

        if target_name:
            if target_type == "user":
                target_display = f"{target_name}\n(<@{target_id}>)" if target_id else target_name
            elif target_type == "role":
                target_display = f"{target_name}\n(<@&{target_id}>)" if target_id else target_name
            elif target_type == "channel":
                target_display = f"{target_name}\n(<#{target_id}>)" if target_id else target_name
            else:
                target_display = target_name
            embed.add_field(name="Target", value=target_display, inline=True)

        if reason:
            embed.add_field(name="Reason", value=reason[:1024], inline=False)

        if details:
            embed.add_field(name="Details", value=details[:1024], inline=False)

        # Add target user's avatar as thumbnail if target is a user
        if target_type == "user" and target_id:
            try:
                target_user = await self.bot.fetch_user(target_id)
                if target_user and target_user.avatar:
                    embed.set_thumbnail(url=target_user.avatar.url)
            except (discord.NotFound, discord.HTTPException):
                # User not found or error fetching, skip thumbnail
                pass

        try:
            await channel.send(embed=embed)
        except discord.Forbidden:
            logger.warning(f"Cannot send audit log to channel {channel_id}")

    # ==================== BACKGROUND TASKS ====================

    @tasks.loop(hours=24)
    async def cleanup_old_logs_task(self):
        """Clean up audit logs older than retention period."""
        logger.info("Running audit log cleanup task...")

        with db_session_scope() as session:
            guilds = session.query(Guild).all()

            for guild in guilds:
                try:
                    retention_days = get_retention_days(session, guild.guild_id)
                    cutoff_time = int(time.time()) - (retention_days * 86400)

                    # Delete old logs
                    deleted = (
                        session.query(AuditLog)
                        .filter(
                            AuditLog.guild_id == guild.guild_id,
                            AuditLog.timestamp < cutoff_time
                        )
                        .delete()
                    )

                    if deleted > 0:
                        logger.debug(f"Cleaned up {deleted} old audit logs for guild {guild.guild_id}")

                except Exception as e:
                    logger.error(f"Error cleaning up logs for guild {guild.guild_id}: {e}")

        logger.info("Audit log cleanup completed")

    @cleanup_old_logs_task.before_loop
    async def before_cleanup(self):
        await self.bot.wait_until_ready()

    # ==================== EVENT LISTENERS ====================

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """Log member join."""
        if member.bot:
            return

        await self.log_event(
            guild_id=member.guild.id,
            action=AuditAction.MEMBER_JOIN,
            target_id=member.id,
            target_name=str(member),
            target_type="user",
            details=f"Account created: <t:{int(member.created_at.timestamp())}:R>",
            category="member"
        )

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Log member leave/kick/ban."""
        if member.bot:
            return

        # Check audit log to determine if it was a kick or ban
        action = AuditAction.MEMBER_LEAVE
        actor_id = None
        actor_name = None
        reason = None

        try:
            await asyncio.sleep(1)  # Wait for audit log entry
            async for entry in member.guild.audit_logs(limit=5):
                if entry.target and entry.target.id == member.id:
                    if entry.action == discord.AuditLogAction.kick:
                        action = AuditAction.MEMBER_KICK
                        actor_id = entry.user.id
                        actor_name = str(entry.user)
                        reason = entry.reason
                        break
                    elif entry.action == discord.AuditLogAction.ban:
                        # Don't log here - on_member_ban handles it
                        return
        except (discord.Forbidden, discord.NotFound):
            pass

        await self.log_event(
            guild_id=member.guild.id,
            action=action,
            actor_id=actor_id,
            actor_name=actor_name,
            target_id=member.id,
            target_name=str(member),
            target_type="user",
            reason=reason,
            category="member"
        )

    @commands.Cog.listener()
    async def on_member_ban(self, guild: discord.Guild, user: discord.User):
        """Log member ban."""
        actor_id = None
        actor_name = None
        reason = None

        try:
            await asyncio.sleep(0.5)
            async for entry in guild.audit_logs(action=discord.AuditLogAction.ban, limit=5):
                if entry.target and entry.target.id == user.id:
                    actor_id = entry.user.id
                    actor_name = str(entry.user)
                    reason = entry.reason
                    break
        except (discord.Forbidden, discord.NotFound):
            pass

        await self.log_event(
            guild_id=guild.id,
            action=AuditAction.MEMBER_BAN,
            actor_id=actor_id,
            actor_name=actor_name,
            target_id=user.id,
            target_name=str(user),
            target_type="user",
            reason=reason,
            category="member"
        )

    @commands.Cog.listener()
    async def on_member_unban(self, guild: discord.Guild, user: discord.User):
        """Log member unban."""
        actor_id = None
        actor_name = None
        reason = None

        try:
            await asyncio.sleep(0.5)
            async for entry in guild.audit_logs(action=discord.AuditLogAction.unban, limit=5):
                if entry.target and entry.target.id == user.id:
                    actor_id = entry.user.id
                    actor_name = str(entry.user)
                    reason = entry.reason
                    break
        except (discord.Forbidden, discord.NotFound):
            pass

        await self.log_event(
            guild_id=guild.id,
            action=AuditAction.MEMBER_UNBAN,
            actor_id=actor_id,
            actor_name=actor_name,
            target_id=user.id,
            target_name=str(user),
            target_type="user",
            reason=reason,
            category="member"
        )

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """Log role changes and timeouts."""
        if before.bot:
            return

        # Check for role changes
        added_roles = set(after.roles) - set(before.roles)
        removed_roles = set(before.roles) - set(after.roles)

        # Update cached_members when roles change (for dashboard admin role checks)
        if added_roles or removed_roles:
            try:
                with db_session_scope() as session:
                    db_guild = session.get(Guild, after.guild.id)
                    if db_guild and db_guild.cached_members:
                        cached_members = json.loads(db_guild.cached_members)
                        # Find and update this member's roles in the cache
                        member_found = False
                        for member in cached_members:
                            if str(member.get('id')) == str(after.id):
                                member['roles'] = [str(role.id) for role in after.roles if role.name != "@everyone"]
                                member['display_name'] = after.display_name
                                member_found = True
                                break
                        # If member not in cache, add them
                        if not member_found:
                            cached_members.append({
                                'id': str(after.id),
                                'username': after.name,
                                'discriminator': after.discriminator,
                                'display_name': after.display_name,
                                'avatar': after.avatar.url if after.avatar else None,
                                'roles': [str(role.id) for role in after.roles if role.name != "@everyone"],
                                'joined_at': after.joined_at.isoformat() if after.joined_at else None
                            })
                        db_guild.cached_members = json.dumps(cached_members)
                        logger.debug(f"Updated cached_members for {after} in {after.guild.name} (role change)")
            except Exception as e:
                logger.error(f"Failed to update cached_members for role change: {e}")

        for role in added_roles:
            if role.is_default():
                continue

            actor_id = None
            actor_name = None

            try:
                await asyncio.sleep(0.5)
                async for entry in after.guild.audit_logs(action=discord.AuditLogAction.member_role_update, limit=5):
                    if entry.target and entry.target.id == after.id:
                        actor_id = entry.user.id
                        actor_name = str(entry.user)
                        break
            except (discord.Forbidden, discord.NotFound):
                pass

            await self.log_event(
                guild_id=after.guild.id,
                action=AuditAction.ROLE_ADD,
                actor_id=actor_id,
                actor_name=actor_name,
                target_id=after.id,
                target_name=str(after),
                target_type="user",
                details=f"Role: {role.name} ({role.id})",
                category="role"
            )

        for role in removed_roles:
            if role.is_default():
                continue

            actor_id = None
            actor_name = None

            try:
                await asyncio.sleep(0.5)
                async for entry in after.guild.audit_logs(action=discord.AuditLogAction.member_role_update, limit=5):
                    if entry.target and entry.target.id == after.id:
                        actor_id = entry.user.id
                        actor_name = str(entry.user)
                        break
            except (discord.Forbidden, discord.NotFound):
                pass

            await self.log_event(
                guild_id=after.guild.id,
                action=AuditAction.ROLE_REMOVE,
                actor_id=actor_id,
                actor_name=actor_name,
                target_id=after.id,
                target_name=str(after),
                target_type="user",
                details=f"Role: {role.name} ({role.id})",
                category="role"
            )

        # Check for timeout (py-cord uses communication_disabled_until)
        if before.communication_disabled_until != after.communication_disabled_until:
            if after.communication_disabled_until and after.communication_disabled_until > discord.utils.utcnow():
                actor_id = None
                actor_name = None
                reason = None

                try:
                    await asyncio.sleep(0.5)
                    async for entry in after.guild.audit_logs(action=discord.AuditLogAction.member_update, limit=5):
                        if entry.target and entry.target.id == after.id:
                            actor_id = entry.user.id
                            actor_name = str(entry.user)
                            reason = entry.reason
                            break
                except discord.Forbidden:
                    pass

                await self.log_event(
                    guild_id=after.guild.id,
                    action=AuditAction.MEMBER_TIMEOUT,
                    actor_id=actor_id,
                    actor_name=actor_name,
                    target_id=after.id,
                    target_name=str(after),
                    target_type="user",
                    reason=reason,
                    details=f"Until: <t:{int(after.communication_disabled_until.timestamp())}:R>",
                    category="member"
                )

    @commands.Cog.listener()
    async def on_guild_role_create(self, role: discord.Role):
        """Log role creation."""
        actor_id = None
        actor_name = None

        try:
            await asyncio.sleep(0.5)
            async for entry in role.guild.audit_logs(action=discord.AuditLogAction.role_create, limit=5):
                if entry.target and entry.target.id == role.id:
                    actor_id = entry.user.id
                    actor_name = str(entry.user)
                    break
        except (discord.Forbidden, discord.NotFound):
            pass

        await self.log_event(
            guild_id=role.guild.id,
            action=AuditAction.ROLE_CREATE,
            actor_id=actor_id,
            actor_name=actor_name,
            target_id=role.id,
            target_name=role.name,
            target_type="role",
            details=f"Color: {role.color} | Hoisted: {role.hoist}",
            category="role"
        )

    @commands.Cog.listener()
    async def on_guild_role_delete(self, role: discord.Role):
        """Log role deletion."""
        actor_id = None
        actor_name = None

        try:
            await asyncio.sleep(0.5)
            async for entry in role.guild.audit_logs(action=discord.AuditLogAction.role_delete, limit=5):
                actor_id = entry.user.id
                actor_name = str(entry.user)
                break
        except (discord.Forbidden, discord.NotFound):
            # NotFound can occur if bot left the guild
            pass

        await self.log_event(
            guild_id=role.guild.id,
            action=AuditAction.ROLE_DELETE,
            actor_id=actor_id,
            actor_name=actor_name,
            target_id=role.id,
            target_name=role.name,
            target_type="role",
            category="role"
        )

    @commands.Cog.listener()
    async def on_guild_channel_create(self, channel: discord.abc.GuildChannel):
        """Log channel creation."""
        actor_id = None
        actor_name = None

        try:
            await asyncio.sleep(0.5)
            async for entry in channel.guild.audit_logs(action=discord.AuditLogAction.channel_create, limit=5):
                if entry.target and entry.target.id == channel.id:
                    actor_id = entry.user.id
                    actor_name = str(entry.user)
                    break
        except (discord.Forbidden, discord.NotFound):
            pass

        channel_type = type(channel).__name__.replace("Channel", "")

        await self.log_event(
            guild_id=channel.guild.id,
            action=AuditAction.CHANNEL_CREATE,
            actor_id=actor_id,
            actor_name=actor_name,
            target_id=channel.id,
            target_name=channel.name,
            target_type="channel",
            details=f"Type: {channel_type}",
            category="channel"
        )

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel):
        """Log channel deletion."""
        actor_id = None
        actor_name = None

        try:
            await asyncio.sleep(0.5)
            async for entry in channel.guild.audit_logs(action=discord.AuditLogAction.channel_delete, limit=5):
                actor_id = entry.user.id
                actor_name = str(entry.user)
                break
        except (discord.Forbidden, discord.NotFound):
            pass

        await self.log_event(
            guild_id=channel.guild.id,
            action=AuditAction.CHANNEL_DELETE,
            actor_id=actor_id,
            actor_name=actor_name,
            target_id=channel.id,
            target_name=channel.name,
            target_type="channel",
            category="channel"
        )

    @commands.Cog.listener()
    async def on_guild_channel_update(self, before: discord.abc.GuildChannel, after: discord.abc.GuildChannel):
        """Log channel updates."""
        changes = []

        if before.name != after.name:
            changes.append(f"Name: {before.name} → {after.name}")

        if hasattr(before, 'topic') and hasattr(after, 'topic'):
            if before.topic != after.topic:
                changes.append("Topic changed")

        if hasattr(before, 'slowmode_delay') and hasattr(after, 'slowmode_delay'):
            if before.slowmode_delay != after.slowmode_delay:
                changes.append(f"Slowmode: {before.slowmode_delay}s → {after.slowmode_delay}s")

        if not changes:
            return

        actor_id = None
        actor_name = None

        try:
            await asyncio.sleep(0.5)
            async for entry in after.guild.audit_logs(action=discord.AuditLogAction.channel_update, limit=5):
                if entry.target and entry.target.id == after.id:
                    actor_id = entry.user.id
                    actor_name = str(entry.user)
                    break
        except (discord.Forbidden, discord.NotFound):
            pass

        await self.log_event(
            guild_id=after.guild.id,
            action=AuditAction.CHANNEL_UPDATE,
            actor_id=actor_id,
            actor_name=actor_name,
            target_id=after.id,
            target_name=after.name,
            target_type="channel",
            details="\n".join(changes),
            category="channel"
        )

    @commands.Cog.listener()
    async def on_message_delete(self, message: discord.Message):
        """Log message deletion."""
        if not message.guild or message.author.bot:
            return

        # Don't log if content is empty (embeds only, etc.)
        if not message.content:
            return

        actor_id = None
        actor_name = None

        try:
            await asyncio.sleep(0.5)
            async for entry in message.guild.audit_logs(action=discord.AuditLogAction.message_delete, limit=5):
                if entry.target and entry.target.id == message.author.id:
                    actor_id = entry.user.id
                    actor_name = str(entry.user)
                    break
        except (discord.Forbidden, discord.NotFound):
            pass

        # Truncate content for storage
        content = message.content[:500] + "..." if len(message.content) > 500 else message.content

        await self.log_event(
            guild_id=message.guild.id,
            action=AuditAction.MESSAGE_DELETE,
            actor_id=actor_id,
            actor_name=actor_name or "Unknown (self or bot)",
            target_id=message.author.id,
            target_name=str(message.author),
            target_type="user",
            details=f"Channel: <#{message.channel.id}>\nContent: {content}",
            category="message"
        )

    @commands.Cog.listener()
    async def on_bulk_message_delete(self, messages: list[discord.Message]):
        """Log bulk message deletion."""
        if not messages or not messages[0].guild:
            return

        guild = messages[0].guild
        channel = messages[0].channel

        actor_id = None
        actor_name = None

        try:
            await asyncio.sleep(0.5)
            async for entry in guild.audit_logs(action=discord.AuditLogAction.message_bulk_delete, limit=5):
                actor_id = entry.user.id
                actor_name = str(entry.user)
                break
        except (discord.Forbidden, discord.NotFound):
            pass

        await self.log_event(
            guild_id=guild.id,
            action=AuditAction.MESSAGE_BULK_DELETE,
            actor_id=actor_id,
            actor_name=actor_name,
            target_id=channel.id,
            target_name=channel.name,
            target_type="channel",
            details=f"Messages deleted: {len(messages)}",
            category="message"
        )

    # ==================== SLASH COMMANDS ====================

    audit = SlashCommandGroup(
        name="audit",
        description="Audit log commands",
        
    )

    @audit.command(name="search", description="Search audit logs")
    @discord.default_permissions(manage_guild=True)
    @commands.has_permissions(manage_guild=True)
    @discord.option("user", discord.Member, description="Filter by user (actor or target)", required=False)
    @discord.option(
        "category",
        str,
        description="Filter by category",
        required=False,
        choices=["member", "role", "channel", "message", "security"]
    )
    @discord.option(
        "action",
        str,
        description="Filter by specific action",
        required=False,
        choices=[a.value for a in AuditAction]
    )
    @discord.option("limit", int, description="Number of results (max 50)", required=False)
    async def audit_search(
        self,
        ctx: discord.ApplicationContext,
        user: discord.Member = None,
        category: str = None,
        action: str = None,
        limit: int = 25
    ):
        """Search audit logs with filters."""
        limit = min(limit, 50)

        with db_session_scope() as session:
            tier = get_guild_tier(session, ctx.guild.id)
            retention_days = get_retention_days(session, ctx.guild.id)
            cutoff_time = int(time.time()) - (retention_days * 86400)

            query = session.query(AuditLog).filter(
                AuditLog.guild_id == ctx.guild.id,
                AuditLog.timestamp >= cutoff_time
            )

            if user:
                query = query.filter(
                    (AuditLog.actor_id == user.id) | (AuditLog.target_id == user.id)
                )

            if category:
                actions = ACTION_CATEGORIES.get(category, [])
                if actions:
                    query = query.filter(AuditLog.action.in_(actions))

            if action:
                query = query.filter(AuditLog.action == AuditAction(action))

            logs = query.order_by(AuditLog.timestamp.desc()).limit(limit).all()

            if not logs:
                await ctx.respond(
                    f"No audit logs found matching your criteria (last {retention_days} days).",
                    ephemeral=True
                )
                return

            embed = discord.Embed(
                title="Audit Log Search Results",
                color=discord.Color.blue()
            )
            embed.set_footer(text=f"{tier} tier: {retention_days}-day retention | Showing {len(logs)} results")

            for log in logs[:15]:  # Show max 15 in embed
                emoji = ACTION_EMOJIS.get(log.action, "📋")
                action_name = log.action.value.replace("_", " ").title()
                time_str = f"<t:{log.timestamp}:R>"

                value_parts = []
                if log.actor_name:
                    value_parts.append(f"By: {log.actor_name}")
                if log.target_name:
                    value_parts.append(f"Target: {log.target_name}")
                if log.reason:
                    value_parts.append(f"Reason: {log.reason[:50]}")

                embed.add_field(
                    name=f"{emoji} {action_name} - {time_str}",
                    value="\n".join(value_parts) if value_parts else "No details",
                    inline=False
                )

            if len(logs) > 15:
                embed.description = f"*Showing 15 of {len(logs)} results. Use `/audit export` for full data.*"

        await ctx.respond(embed=embed, ephemeral=True)

    @audit.command(name="recent", description="View recent audit logs")
    @discord.default_permissions(manage_guild=True)
    @commands.has_permissions(manage_guild=True)
    @discord.option("limit", int, description="Number of logs to show (max 25)", required=False)
    async def audit_recent(
        self,
        ctx: discord.ApplicationContext,
        limit: int = 15
    ):
        """View most recent audit logs."""
        limit = min(limit, 25)

        with db_session_scope() as session:
            tier = get_guild_tier(session, ctx.guild.id)
            retention_days = get_retention_days(session, ctx.guild.id)

            logs = (
                session.query(AuditLog)
                .filter(AuditLog.guild_id == ctx.guild.id)
                .order_by(AuditLog.timestamp.desc())
                .limit(limit)
                .all()
            )

            if not logs:
                await ctx.respond("No audit logs found.", ephemeral=True)
                return

            embed = discord.Embed(
                title="Recent Audit Logs",
                color=discord.Color.blue()
            )
            embed.set_footer(text=f"{tier} tier: {retention_days}-day retention")

            for log in logs:
                emoji = ACTION_EMOJIS.get(log.action, "📋")
                action_name = log.action.value.replace("_", " ").title()
                time_str = f"<t:{log.timestamp}:R>"

                value = ""
                if log.actor_name:
                    value += f"By: {log.actor_name}\n"
                if log.target_name:
                    value += f"Target: {log.target_name}"

                embed.add_field(
                    name=f"{emoji} {action_name} - {time_str}",
                    value=value or "No details",
                    inline=False
                )

        await ctx.respond(embed=embed, ephemeral=True)

    @audit.command(name="stats", description="View audit log statistics")
    @discord.default_permissions(manage_guild=True)
    @commands.has_permissions(manage_guild=True)
    async def audit_stats(self, ctx: discord.ApplicationContext):
        """View audit log statistics."""
        with db_session_scope() as session:
            tier = get_guild_tier(session, ctx.guild.id)
            retention_days = get_retention_days(session, ctx.guild.id)
            cutoff_time = int(time.time()) - (retention_days * 86400)

            # Total logs
            total = (
                session.query(AuditLog)
                .filter(
                    AuditLog.guild_id == ctx.guild.id,
                    AuditLog.timestamp >= cutoff_time
                )
                .count()
            )

            # Logs today
            today_start = int(time.time()) - (int(time.time()) % 86400)
            today_count = (
                session.query(AuditLog)
                .filter(
                    AuditLog.guild_id == ctx.guild.id,
                    AuditLog.timestamp >= today_start
                )
                .count()
            )

            # Count by category
            category_counts = {}
            for cat_name, actions in ACTION_CATEGORIES.items():
                count = (
                    session.query(AuditLog)
                    .filter(
                        AuditLog.guild_id == ctx.guild.id,
                        AuditLog.timestamp >= cutoff_time,
                        AuditLog.action.in_(actions)
                    )
                    .count()
                )
                category_counts[cat_name] = count

            # Top actors
            from sqlalchemy import func
            top_actors = (
                session.query(AuditLog.actor_name, func.count(AuditLog.id))
                .filter(
                    AuditLog.guild_id == ctx.guild.id,
                    AuditLog.timestamp >= cutoff_time,
                    AuditLog.actor_name.isnot(None)
                )
                .group_by(AuditLog.actor_name)
                .order_by(func.count(AuditLog.id).desc())
                .limit(5)
                .all()
            )

        embed = discord.Embed(
            title="Audit Log Statistics",
            color=discord.Color.purple()
        )

        embed.add_field(
            name="Overview",
            value=f"**Total Logs:** {total:,}\n**Today:** {today_count:,}\n**Retention:** {retention_days} days",
            inline=False
        )

        category_text = "\n".join([
            f"**{cat.title()}:** {count:,}" for cat, count in category_counts.items()
        ])
        embed.add_field(name="By Category", value=category_text, inline=True)

        if top_actors:
            actors_text = "\n".join([
                f"{name}: {count}" for name, count in top_actors
            ])
            embed.add_field(name="Top Actors", value=actors_text, inline=True)

        embed.set_footer(text=f"{tier} tier | Upgrade for longer retention")

        await ctx.respond(embed=embed, ephemeral=True)

    @audit.command(name="export", description="Export audit logs to CSV")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    @discord.option(
        "category",
        str,
        description="Filter by category",
        required=False,
        choices=["member", "role", "channel", "message", "security", "all"]
    )
    @discord.option("days", int, description="Number of days to export", required=False)
    async def audit_export(
        self,
        ctx: discord.ApplicationContext,
        category: str = "all",
        days: int = None
    ):
        """Export audit logs to CSV."""
        await ctx.defer(ephemeral=True)

        with db_session_scope() as session:
            tier = get_guild_tier(session, ctx.guild.id)
            retention_days = get_retention_days(session, ctx.guild.id)
            export_days = min(days or retention_days, retention_days)
            cutoff_time = int(time.time()) - (export_days * 86400)

            query = session.query(AuditLog).filter(
                AuditLog.guild_id == ctx.guild.id,
                AuditLog.timestamp >= cutoff_time
            )

            if category and category != "all":
                actions = ACTION_CATEGORIES.get(category, [])
                if actions:
                    query = query.filter(AuditLog.action.in_(actions))

            logs = query.order_by(AuditLog.timestamp.desc()).all()

            if not logs:
                await ctx.followup.send("No logs to export.", ephemeral=True)
                return

            # Create CSV
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow([
                "Timestamp", "Action", "Category", "Actor ID", "Actor Name",
                "Target ID", "Target Name", "Target Type", "Reason", "Details"
            ])

            for log in logs:
                writer.writerow([
                    datetime.fromtimestamp(log.timestamp).isoformat(),
                    log.action.value,
                    log.action_category or "",
                    log.actor_id or "",
                    log.actor_name or "",
                    log.target_id or "",
                    log.target_name or "",
                    log.target_type or "",
                    log.reason or "",
                    log.details or ""
                ])

            output.seek(0)
            filename = f"audit_logs_{ctx.guild.id}_{category}_{export_days}d.csv"
            file = discord.File(io.BytesIO(output.getvalue().encode()), filename=filename)

        await ctx.followup.send(
            f"Exported **{len(logs)}** audit logs ({export_days} days, {category}):",
            file=file,
            ephemeral=True
        )

    @audit.command(name="user", description="View audit history for a specific user")
    @discord.default_permissions(manage_guild=True)
    @commands.has_permissions(manage_guild=True)
    @discord.option("member", discord.Member, description="User to look up")
    async def audit_user(
        self,
        ctx: discord.ApplicationContext,
        member: discord.Member
    ):
        """View all audit logs for a specific user."""
        with db_session_scope() as session:
            tier = get_guild_tier(session, ctx.guild.id)
            retention_days = get_retention_days(session, ctx.guild.id)
            cutoff_time = int(time.time()) - (retention_days * 86400)

            # Logs where user is actor
            as_actor = (
                session.query(AuditLog)
                .filter(
                    AuditLog.guild_id == ctx.guild.id,
                    AuditLog.actor_id == member.id,
                    AuditLog.timestamp >= cutoff_time
                )
                .count()
            )

            # Logs where user is target
            as_target = (
                session.query(AuditLog)
                .filter(
                    AuditLog.guild_id == ctx.guild.id,
                    AuditLog.target_id == member.id,
                    AuditLog.timestamp >= cutoff_time
                )
                .count()
            )

            # Recent actions by user
            recent_actions = (
                session.query(AuditLog)
                .filter(
                    AuditLog.guild_id == ctx.guild.id,
                    AuditLog.actor_id == member.id,
                    AuditLog.timestamp >= cutoff_time
                )
                .order_by(AuditLog.timestamp.desc())
                .limit(5)
                .all()
            )

            # Recent actions targeting user
            recent_targeted = (
                session.query(AuditLog)
                .filter(
                    AuditLog.guild_id == ctx.guild.id,
                    AuditLog.target_id == member.id,
                    AuditLog.timestamp >= cutoff_time
                )
                .order_by(AuditLog.timestamp.desc())
                .limit(5)
                .all()
            )

        embed = discord.Embed(
            title=f"Audit History: {member.display_name}",
            color=member.color
        )
        embed.set_thumbnail(url=member.display_avatar.url)

        embed.add_field(
            name="Summary",
            value=f"**Actions taken:** {as_actor}\n**Actions received:** {as_target}",
            inline=False
        )

        if recent_actions:
            actions_text = "\n".join([
                f"{ACTION_EMOJIS.get(log.action, '📋')} {log.action.value.replace('_', ' ').title()} <t:{log.timestamp}:R>"
                for log in recent_actions
            ])
            embed.add_field(name="Recent Actions", value=actions_text, inline=True)

        if recent_targeted:
            targeted_text = "\n".join([
                f"{ACTION_EMOJIS.get(log.action, '📋')} {log.action.value.replace('_', ' ').title()} <t:{log.timestamp}:R>"
                for log in recent_targeted
            ])
            embed.add_field(name="Recent Events", value=targeted_text, inline=True)

        embed.set_footer(text=f"{tier} tier: {retention_days}-day retention")

        await ctx.respond(embed=embed, ephemeral=True)

    @audit.command(name="config", description="Configure audit logging")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    @discord.option("enabled", bool, description="Enable/disable audit logging", required=False)
    @discord.option("channel", discord.TextChannel, description="Channel for live audit logs", required=False)
    async def audit_config(
        self,
        ctx: discord.ApplicationContext,
        enabled: bool = None,
        channel: discord.TextChannel = None
    ):
        """Configure audit logging settings."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)
            if not db_guild:
                await ctx.respond("Run `/questlog setup` first.", ephemeral=True)
                return

            tier = get_guild_tier(session, ctx.guild.id)
            retention_days = get_retention_days(session, ctx.guild.id)

            changes = []

            if enabled is not None:
                db_guild.audit_logging_enabled = enabled
                changes.append(f"Audit logging: **{'Enabled' if enabled else 'Disabled'}**")

            if channel is not None:
                db_guild.log_channel_id = channel.id
                changes.append(f"Log channel: {channel.mention}")

            if not changes:
                # Show current config
                log_ch = ctx.guild.get_channel(db_guild.log_channel_id) if db_guild.log_channel_id else None
                embed = discord.Embed(
                    title="Audit Configuration",
                    color=discord.Color.blue()
                )
                embed.add_field(
                    name="Status",
                    value="Enabled" if db_guild.audit_logging_enabled else "Disabled",
                    inline=True
                )
                embed.add_field(
                    name="Log Channel",
                    value=log_ch.mention if log_ch else "Not set",
                    inline=True
                )
                embed.add_field(
                    name="Retention",
                    value=f"{retention_days} days ({tier})",
                    inline=True
                )
                await ctx.respond(embed=embed, ephemeral=True)
            else:
                await ctx.respond(
                    "**Audit settings updated:**\n" + "\n".join(changes),
                    ephemeral=True
                )


def setup(bot: commands.Bot):
    bot.add_cog(AuditCog(bot))
