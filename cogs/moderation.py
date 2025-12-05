# cogs/moderation.py - Moderation System
"""
Comprehensive moderation for Warden bot.

Features:
- Manual mod commands: kick, ban, timeout, warn, jail, mute
- Warning system with database tracking
- Auto-mod for slurs/isms with regex patterns
- Escalation: 3 warnings = timeout, 5 warnings = jail (NEVER auto-ban)
- Jail system: Removes all channel access, only sees jail channel
- Muted role: Removes send message permissions everywhere
"""

import re
import time
import json
from datetime import datetime, timezone, timedelta

import discord
from discord.ext import commands, tasks

from config import db_session_scope, logger, get_debug_guilds
from models import (
    Guild, GuildMember, Warning, WarningType,
    ModerationConfig, ModAction, AuditLog, AuditAction
)


# Slur/ism patterns - comprehensive list of offensive terms
# These are regex patterns to catch variations
# NOTE: These are HIGH CONFIDENCE patterns - they require specific character sequences
# that are very unlikely to occur in innocent messages
SLUR_PATTERNS = [
    # Racism patterns - high confidence
    r"\bn[i1!|]gg[e3]r",      # n-word with -er
    r"\bn[i1!|]gg[a@]\b",     # n-word with -a (word boundary to avoid "niggle")
    r"\bn[i1!|]g+l[e3]t",     # diminutive form
    r"\bsp[i1!|]ck?\b",       # anti-Hispanic slur
    r"\bch[i1!|]nk\b",        # anti-Asian slur
    r"\bg[o0][o0]k\b",        # anti-Asian slur
    r"\bw[e3]tb[a@]ck",       # anti-Hispanic slur
    r"\bk[i1!|]k[e3]\b",      # antisemitic slur
    r"\br[a@]gh[e3][a@]d",    # anti-Middle Eastern slur
    r"\bs[a@]nd.?n[i1!|]gg",  # compound slur
    r"\btow[e3]l.?h[e3][a@]d", # anti-Middle Eastern slur

    # Homophobia patterns - high confidence
    r"\bf[a@4]gg[o0]t",       # full slur only (not short form to avoid UK "fag" for cigarette)
    r"\bd[y!1]k[e3]\b",       # anti-lesbian slur
    r"\btr[a@]nn[y!1i]",      # anti-trans slur
    r"\bsh[e3].?m[a@]l[e3]",  # anti-trans slur

    # Ableism patterns - high confidence
    r"\br[e3]t[a@]rd",        # ableist slur

    # Hate speech - high confidence
    r"\bk[i1!|]ll\s*y[o0][u!]rs[e3]lf",  # suicide baiting
    r"\bkys\b",                           # abbreviated suicide baiting
]

# Lower confidence patterns - these may have false positives
# Admins can enable these separately if desired
OPTIONAL_SLUR_PATTERNS = [
    r"\bc[o0][o0]n\b",        # Could match in "raccoon" context discussions
    r"\bf[a@4]g\b",           # UK slang for cigarette
    r"\bcr[a@]ck[e3]r\b",     # Could be food-related
    r"\bj[a@]p\b",            # Abbreviation sometimes used innocently
    r"\bqu[e3][e3]r\b",       # Sometimes reclaimed/used positively
    r"\bb[i1!|]tch\b",        # Very common, may be too broad
    r"\bwh[o0]r[e3]\b",       # Sometimes used casually
    r"\bsl[u!]t\b",           # Sometimes used casually
    r"\bc[u!]nt\b",           # Common in UK/AU English
    r"\btw[a@]t\b",           # Common in UK English
    r"\bsp[a@]z\b",           # UK term for overreacting
]

# Compiled patterns for performance
COMPILED_SLUR_PATTERNS = [re.compile(p, re.IGNORECASE) for p in SLUR_PATTERNS]
COMPILED_OPTIONAL_PATTERNS = [re.compile(p, re.IGNORECASE) for p in OPTIONAL_SLUR_PATTERNS]


def get_guild_tier(session, guild_id: int) -> str:
    """Get the subscription tier for a guild."""
    db_guild = session.get(Guild, guild_id)
    if not db_guild:
        return "FREE"
    if db_guild.is_vip:
        return "PRO"
    return db_guild.subscription_tier.upper() if db_guild.subscription_tier else "FREE"


def check_for_slurs(content: str, strict_mode: bool = False) -> tuple[bool, str | None]:
    """
    Check message content for slurs.

    Args:
        content: Message content to check
        strict_mode: If True, also checks optional patterns that may have false positives

    Returns: (contains_slur, matched_pattern)
    """
    # Always check high-confidence patterns
    for i, pattern in enumerate(COMPILED_SLUR_PATTERNS):
        if pattern.search(content):
            return (True, SLUR_PATTERNS[i])

    # Only check optional patterns in strict mode
    if strict_mode:
        for i, pattern in enumerate(COMPILED_OPTIONAL_PATTERNS):
            if pattern.search(content):
                return (True, OPTIONAL_SLUR_PATTERNS[i])

    return (False, None)


class ModerationCog(commands.Cog):
    """Moderation system with auto-mod and escalation."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # Start background tasks
        self.warning_decay_task.start()

    def cog_unload(self):
        self.warning_decay_task.cancel()

    # Slash command groups
    mod = discord.SlashCommandGroup(
        name="mod",
        description="Moderation commands",
        
    )

    automod = discord.SlashCommandGroup(
        name="automod",
        description="Auto-moderation settings",
        
    )

    # Helper methods

    def get_mod_config(self, session, guild_id: int) -> ModerationConfig:
        """Get or create moderation config for a guild."""
        config = session.get(ModerationConfig, guild_id)
        if not config:
            config = ModerationConfig(guild_id=guild_id)
            session.add(config)
            session.flush()
        return config

    def get_active_warning_count(self, session, guild_id: int, user_id: int, decay_days: int = 30) -> int:
        """Get count of active warnings within decay period."""
        cutoff = int(time.time()) - (decay_days * 86400)
        count = (
            session.query(Warning)
            .filter(
                Warning.guild_id == guild_id,
                Warning.user_id == user_id,
                Warning.is_active == True,
                Warning.pardoned == False,
                Warning.issued_at >= cutoff
            )
            .count()
        )
        return count

    async def log_mod_action(self, guild_id: int, mod: discord.Member, action_type: str,
                              target: discord.Member | discord.User = None, reason: str = None,
                              details: str = None, duration: int = None):
        """Log a moderation action to the database and mod log channel."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, guild_id)

            # Treat moderation as disabled when no mod/jail/mute channels or roles are configured
            mod_enabled = bool(
                db_guild
                and (
                    db_guild.mod_log_channel_id
                    or db_guild.jail_channel_id
                    or db_guild.jail_role_id
                    or db_guild.muted_role_id
                )
            )
            if not mod_enabled:
                return

            # Deduplicate rapid duplicate actions (e.g., double timeout events within 10s)
            recent_cutoff = int(time.time()) - 10
            existing = (
                session.query(ModAction)
                .filter(
                    ModAction.guild_id == guild_id,
                    ModAction.action_type == action_type,
                    ModAction.target_id == (target.id if target else None),
                    ModAction.timestamp >= recent_cutoff,
                )
                .first()
            )
            if existing:
                return

            action = ModAction(
                guild_id=guild_id,
                mod_id=mod.id,
                mod_name=str(mod),
                action_type=action_type,
                target_id=target.id if target else None,
                target_name=str(target) if target else None,
                target_type="user",
                reason=reason,
                details=details,
                duration=duration,
            )
            session.add(action)

            log_channel_id = db_guild.mod_log_channel_id or db_guild.log_channel_id if db_guild else None

        # NOTE: Embed sending disabled - audit log cog handles all embeds to avoid duplicates
        # The audit log captures all moderation actions (role changes, timeouts, bans, etc.)
        # This cog only logs to the database for tracking purposes

        # # Send to mod log channel
        # if log_channel_id:
        #     guild = self.bot.get_guild(guild_id)
        #     if guild:
        #         log_channel = guild.get_channel(log_channel_id)
        #         if log_channel:
        #             embed = discord.Embed(
        #                 title=f"Mod Action: {action_type.upper()}",
        #                 color=self._get_action_color(action_type),
        #                 timestamp=datetime.now(timezone.utc)
        #             )
        #             embed.add_field(name="Moderator", value=f"{mod.mention}", inline=True)
        #             if target:
        #                 embed.add_field(name="Target", value=f"{target.mention} ({target.id})", inline=True)
        #             if reason:
        #                 embed.add_field(name="Reason", value=reason[:1024], inline=False)
        #             if duration:
        #                 embed.add_field(name="Duration", value=f"{duration} minutes", inline=True)
        #             if details:
        #                 embed.add_field(name="Details", value=details[:1024], inline=False)
        #
        #             try:
        #                 await log_channel.send(embed=embed)
        #             except discord.Forbidden:
        #                 pass

    def _get_action_color(self, action_type: str) -> discord.Color:
        """Get color for mod action embed."""
        colors = {
            "warn": discord.Color.yellow(),
            "timeout": discord.Color.orange(),
            "jail": discord.Color.dark_orange(),
            "mute": discord.Color.dark_grey(),
            "kick": discord.Color.red(),
            "ban": discord.Color.dark_red(),
            "unjail": discord.Color.green(),
            "unmute": discord.Color.green(),
            "pardon": discord.Color.green(),
        }
        return colors.get(action_type, discord.Color.blurple())

    async def dm_user(self, user: discord.Member | discord.User, embed: discord.Embed) -> bool:
        """Send a DM to a user. Returns True if successful."""
        try:
            await user.send(embed=embed)
            return True
        except discord.Forbidden:
            return False
        except Exception as e:
            logger.warning(f"Failed to DM {user}: {e}")
            return False

    async def issue_warning(self, guild: discord.Guild, user: discord.Member,
                             reason: str, mod: discord.Member | None = None,
                             warning_type: WarningType = WarningType.MANUAL,
                             triggered_content: str = None,
                             matched_pattern: str = None) -> tuple[Warning, str | None]:
        """
        Issue a warning to a user and handle escalation.
        Returns: (warning, action_taken)
        """
        action_taken = None

        with db_session_scope() as session:
            config = self.get_mod_config(session, guild.id)

            # Get current warning count
            warning_count = self.get_active_warning_count(
                session, guild.id, user.id, config.warning_decay_days
            )

            # Create warning
            warning = Warning(
                guild_id=guild.id,
                user_id=user.id,
                warning_type=warning_type,
                reason=reason,
                triggered_content=triggered_content[:500] if triggered_content else None,
                matched_pattern=matched_pattern,
                issued_by=mod.id if mod else None,
                issued_by_name=str(mod) if mod else "Auto-Mod",
            )

            new_count = warning_count + 1

            # Handle escalation (NEVER auto-ban)
            db_guild = session.get(Guild, guild.id)

            if new_count >= config.warnings_before_jail and db_guild.jail_role_id:
                # Jail the user
                action_taken = "jail"
                warning.action_taken = "jail"
            elif new_count >= config.warnings_before_timeout:
                # Timeout the user
                action_taken = "timeout"
                warning.action_taken = "timeout"
                warning.action_duration_minutes = config.timeout_duration_minutes

            session.add(warning)
            session.flush()
            warning_id = warning.id
            dm_on_warn = config.dm_on_warn
            timeout_duration = config.timeout_duration_minutes

        # Execute escalation action
        if action_taken == "jail":
            await self._jail_user(guild, user, f"Auto-escalation: {new_count} warnings")
        elif action_taken == "timeout":
            try:
                await user.timeout_for(
                    timedelta(minutes=timeout_duration),
                    reason=f"Auto-escalation: {new_count} warnings"
                )
            except discord.Forbidden:
                logger.warning(f"Cannot timeout {user} - missing permissions")

        # DM the user
        if dm_on_warn:
            embed = discord.Embed(
                title=f"Warning in {guild.name}",
                description=f"You have received a warning.",
                color=discord.Color.yellow()
            )
            embed.add_field(name="Reason", value=reason, inline=False)
            embed.add_field(name="Warning Count", value=f"{new_count} active warnings", inline=True)

            if action_taken:
                embed.add_field(
                    name="Action Taken",
                    value=f"You have been **{action_taken}ed** due to accumulated warnings.",
                    inline=False
                )

            embed.set_footer(text="Repeated violations may result in further action.")
            await self.dm_user(user, embed)

        # Log the action
        await self.log_mod_action(
            guild.id,
            mod or guild.me,
            "warn",
            user,
            reason,
            f"Warning #{new_count} | Type: {warning_type.value}" +
            (f" | Escalation: {action_taken}" if action_taken else "")
        )

        return (warning, action_taken)

    async def _jail_user(self, guild: discord.Guild, user: discord.Member, reason: str):
        """Apply jail role to user (removes all channel access)."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, guild.id)
            if not db_guild or not db_guild.jail_role_id:
                return False
            jail_role_id = db_guild.jail_role_id

        jail_role = guild.get_role(jail_role_id)
        if not jail_role:
            return False

        try:
            # Remove all other roles (except @everyone and managed roles)
            roles_to_remove = [r for r in user.roles if not r.is_default() and not r.managed]
            if roles_to_remove:
                await user.remove_roles(*roles_to_remove, reason=f"Jailed: {reason}")

            # Add jail role
            await user.add_roles(jail_role, reason=f"Jailed: {reason}")

            # Update database
            with db_session_scope() as session:
                db_member = session.get(GuildMember, (guild.id, user.id))
                if db_member:
                    db_member.is_quarantined = True
                    db_member.quarantined_at = int(time.time())
                    db_member.quarantine_reason = f"Jailed: {reason}"

            return True
        except discord.Forbidden:
            logger.warning(f"Cannot jail {user} in {guild.name} - missing permissions")
            return False

    async def _unjail_user(self, guild: discord.Guild, user: discord.Member, reason: str):
        """Remove jail role from user."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, guild.id)
            if not db_guild or not db_guild.jail_role_id:
                return False
            jail_role_id = db_guild.jail_role_id
            verified_role_id = db_guild.verified_role_id

        jail_role = guild.get_role(jail_role_id)
        if not jail_role or jail_role not in user.roles:
            return False

        try:
            await user.remove_roles(jail_role, reason=f"Unjailed: {reason}")

            # Add back verified role if exists
            if verified_role_id:
                verified_role = guild.get_role(verified_role_id)
                if verified_role:
                    await user.add_roles(verified_role, reason="Unjailed - restoring verified role")

            # Update database
            with db_session_scope() as session:
                db_member = session.get(GuildMember, (guild.id, user.id))
                if db_member:
                    db_member.is_quarantined = False

            # Log the action
            await self.log_mod_action(guild.id, guild.me, "unjail", user, reason)
            return True
        except discord.Forbidden:
            return False

    async def _unmute_user(self, guild: discord.Guild, user: discord.Member, reason: str):
        """Remove mute role from user."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, guild.id)
            if not db_guild or not db_guild.muted_role_id:
                return False
            muted_role_id = db_guild.muted_role_id

        muted_role = guild.get_role(muted_role_id)
        if not muted_role or muted_role not in user.roles:
            return False

        try:
            await user.remove_roles(muted_role, reason=f"Unmuted: {reason}")
            await self.log_mod_action(guild.id, guild.me, "unmute", user, reason)
            return True
        except discord.Forbidden:
            return False

    # Event listeners

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """Track timeout changes made outside of bot commands."""
        # Check if timeout status changed
        if before.timed_out == after.timed_out:
            return  # No change in timeout status

        # User was timed out
        if not before.timed_out and after.timed_out:
            # Fetch audit log to see who did it
            try:
                async for entry in after.guild.audit_logs(
                    limit=5,
                    action=discord.AuditLogAction.member_update
                ):
                    # Find the entry for this user's timeout
                    if entry.target.id == after.id and entry.created_at > (before.joined_at or before.created_at):
                        # Get timeout duration
                        duration_seconds = 0
                        if after.communication_disabled_until:
                            duration_seconds = int((after.communication_disabled_until - datetime.now(timezone.utc)).total_seconds())
                        duration_minutes = max(1, duration_seconds // 60)

                        # Log to database
                        await self.log_mod_action(
                            after.guild.id,
                            entry.user,  # The moderator who did it
                            "timeout",
                            after,
                            entry.reason or "No reason provided",
                            duration=duration_minutes
                        )
                        break
            except discord.Forbidden:
                # Can't access audit logs, log with unknown moderator
                await self.log_mod_action(
                    after.guild.id,
                    after.guild.me,  # Use bot as fallback
                    "timeout",
                    after,
                    "Manual timeout (audit log inaccessible)",
                    duration=0
                )

        # User timeout was removed
        elif before.timed_out and not after.timed_out:
            try:
                async for entry in after.guild.audit_logs(
                    limit=5,
                    action=discord.AuditLogAction.member_update
                ):
                    if entry.target.id == after.id and entry.created_at > (before.joined_at or before.created_at):
                        await self.log_mod_action(
                            after.guild.id,
                            entry.user,
                            "untimeout",
                            after,
                            entry.reason or "Timeout removed"
                        )
                        break
            except discord.Forbidden:
                await self.log_mod_action(
                    after.guild.id,
                    after.guild.me,
                    "untimeout",
                    after,
                    "Manual untimeout (audit log inaccessible)"
                )

    # Auto-mod message listener

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Auto-mod: Check messages for violations."""
        if message.author.bot or not message.guild:
            return

        # Skip if user has manage messages permission (mods exempt)
        if message.author.guild_permissions.manage_messages:
            return

        with db_session_scope() as session:
            config = self.get_mod_config(session, message.guild.id)
            if not config.automod_enabled:
                return

            filter_slurs = config.filter_slurs
            strict_mode = config.strict_slur_filter
            slur_action = config.slur_action

        # Check for slurs
        if filter_slurs:
            contains_slur, matched_pattern = check_for_slurs(message.content, strict_mode=strict_mode)
            if contains_slur:
                await self._handle_automod_violation(
                    message, "slur", slur_action,
                    matched_pattern=matched_pattern
                )

    async def _handle_automod_violation(self, message: discord.Message, violation_type: str,
                                         action: str, matched_pattern: str = None):
        """Handle an auto-mod violation."""
        guild = message.guild
        user = message.author

        # Delete message if action includes delete
        if "delete" in action:
            try:
                await message.delete()
            except discord.Forbidden:
                pass

        # Determine warning type
        warning_type_map = {
            "slur": WarningType.AUTO_SLUR,
            "spam": WarningType.AUTO_SPAM,
            "caps": WarningType.AUTO_CAPS,
            "links": WarningType.AUTO_LINKS,
            "mention": WarningType.AUTO_MENTION,
        }
        warning_type = warning_type_map.get(violation_type, WarningType.MANUAL)

        # Issue warning
        reason = f"Auto-mod: {violation_type.upper()} detected"
        await self.issue_warning(
            guild, user, reason,
            mod=None,
            warning_type=warning_type,
            triggered_content=message.content,
            matched_pattern=matched_pattern
        )

        # Log to mod channel
        await self.log_mod_action(
            guild.id, guild.me, "automod",
            user, reason,
            f"Message deleted: {'Yes' if 'delete' in action else 'No'} | Pattern: {matched_pattern or 'N/A'}"
        )

    # Slash commands

    @mod.command(name="warn", description="Issue a warning to a user")
    @commands.has_permissions(moderate_members=True)
    @discord.option("user", discord.Member, description="User to warn")
    @discord.option("reason", str, description="Reason for warning")
    async def mod_warn(self, ctx: discord.ApplicationContext, user: discord.Member, reason: str):
        """Issue a warning to a user."""
        if user.bot:
            await ctx.respond("Cannot warn bots.", ephemeral=True)
            return

        if user.top_role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
            await ctx.respond("You cannot warn someone with a higher or equal role.", ephemeral=True)
            return

        warning, action_taken = await self.issue_warning(
            ctx.guild, user, reason, ctx.author
        )

        response = f"Warned {user.mention} for: **{reason}**"
        if action_taken:
            response += f"\n**Escalation:** User has been {action_taken}ed due to accumulated warnings."

        await ctx.respond(response, ephemeral=True)

    @mod.command(name="kick", description="Kick a user from the server")
    @commands.has_permissions(kick_members=True)
    @discord.option("user", discord.Member, description="User to kick")
    @discord.option("reason", str, description="Reason for kick", required=False)
    async def mod_kick(self, ctx: discord.ApplicationContext, user: discord.Member, reason: str = None):
        """Kick a user from the server."""
        if user.bot:
            await ctx.respond("Cannot kick bots through this command.", ephemeral=True)
            return

        if user.top_role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
            await ctx.respond("You cannot kick someone with a higher or equal role.", ephemeral=True)
            return

        if user.top_role >= ctx.guild.me.top_role:
            await ctx.respond("I cannot kick this user - their role is too high.", ephemeral=True)
            return

        # DM the user before kicking
        embed = discord.Embed(
            title=f"Kicked from {ctx.guild.name}",
            description=f"You have been kicked from the server.",
            color=discord.Color.red()
        )
        if reason:
            embed.add_field(name="Reason", value=reason, inline=False)
        await self.dm_user(user, embed)

        try:
            await user.kick(reason=f"Kicked by {ctx.author}: {reason or 'No reason provided'}")
        except discord.Forbidden:
            await ctx.respond("I don't have permission to kick this user.", ephemeral=True)
            return

        await self.log_mod_action(ctx.guild.id, ctx.author, "kick", user, reason)
        await ctx.respond(f"Kicked {user.mention}" + (f" for: **{reason}**" if reason else ""), ephemeral=True)

    @mod.command(name="ban", description="Ban a user from the server")
    @commands.has_permissions(ban_members=True)
    @discord.option("user", discord.Member, description="User to ban")
    @discord.option("reason", str, description="Reason for ban")
    @discord.option("delete_days", int, description="Days of messages to delete (0-7)", default=0)
    async def mod_ban(self, ctx: discord.ApplicationContext, user: discord.Member, reason: str, delete_days: int = 0):
        """Ban a user from the server."""
        if user.bot:
            await ctx.respond("Cannot ban bots through this command.", ephemeral=True)
            return

        if user.top_role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
            await ctx.respond("You cannot ban someone with a higher or equal role.", ephemeral=True)
            return

        if user.top_role >= ctx.guild.me.top_role:
            await ctx.respond("I cannot ban this user - their role is too high.", ephemeral=True)
            return

        delete_days = max(0, min(7, delete_days))

        # DM the user before banning
        embed = discord.Embed(
            title=f"Banned from {ctx.guild.name}",
            description=f"You have been permanently banned from the server.",
            color=discord.Color.dark_red()
        )
        embed.add_field(name="Reason", value=reason, inline=False)
        await self.dm_user(user, embed)

        try:
            await user.ban(
                reason=f"Banned by {ctx.author}: {reason}",
                delete_message_seconds=delete_days * 86400  # Convert days to seconds
            )
        except discord.Forbidden:
            await ctx.respond("I don't have permission to ban this user.", ephemeral=True)
            return

        await self.log_mod_action(
            ctx.guild.id, ctx.author, "ban", user, reason,
            f"Message deletion: {delete_days} days"
        )
        await ctx.respond(f"Banned {user.mention} for: **{reason}**", ephemeral=True)

    @mod.command(name="timeout", description="Timeout a user (prevent them from interacting)")
    @commands.has_permissions(moderate_members=True)
    @discord.option("user", discord.Member, description="User to timeout")
    @discord.option("duration", int, description="Duration in minutes")
    @discord.option("reason", str, description="Reason for timeout", required=False)
    async def mod_timeout(self, ctx: discord.ApplicationContext, user: discord.Member,
                          duration: int, reason: str = None):
        """Timeout a user."""
        if user.bot:
            await ctx.respond("Cannot timeout bots.", ephemeral=True)
            return

        if user.top_role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
            await ctx.respond("You cannot timeout someone with a higher or equal role.", ephemeral=True)
            return

        if duration < 1 or duration > 40320:  # Max 28 days
            await ctx.respond("Duration must be between 1 minute and 28 days (40320 minutes).", ephemeral=True)
            return

        try:
            await user.timeout_for(
                timedelta(minutes=duration),
                reason=f"Timeout by {ctx.author}: {reason or 'No reason'}"
            )
        except discord.Forbidden:
            await ctx.respond("I don't have permission to timeout this user.", ephemeral=True)
            return

        # DM user
        embed = discord.Embed(
            title=f"Timed Out in {ctx.guild.name}",
            description=f"You have been timed out for **{duration} minutes**.",
            color=discord.Color.orange()
        )
        if reason:
            embed.add_field(name="Reason", value=reason, inline=False)
        await self.dm_user(user, embed)

        await self.log_mod_action(ctx.guild.id, ctx.author, "timeout", user, reason, duration=duration)
        await ctx.respond(
            f"Timed out {user.mention} for **{duration} minutes**" +
            (f": {reason}" if reason else ""),
            ephemeral=True
        )

    @mod.command(name="untimeout", description="Remove timeout from a user")
    @commands.has_permissions(moderate_members=True)
    @discord.option("user", discord.Member, description="User to untimeout")
    @discord.option("reason", str, description="Reason for removing timeout", required=False)
    async def mod_untimeout(self, ctx: discord.ApplicationContext, user: discord.Member, reason: str = None):
        """Remove timeout from a user."""
        # py-cord uses .timed_out property (boolean) or .communication_disabled_until (datetime)
        if not user.timed_out:
            await ctx.respond(f"{user.mention} is not timed out.", ephemeral=True)
            return

        try:
            await user.remove_timeout(reason=f"Timeout removed by {ctx.author}: {reason or 'No reason'}")
        except discord.Forbidden:
            await ctx.respond("I don't have permission to remove this timeout.", ephemeral=True)
            return

        await self.log_mod_action(ctx.guild.id, ctx.author, "untimeout", user, reason)
        await ctx.respond(f"Removed timeout from {user.mention}.", ephemeral=True)

    @mod.command(name="jail", description="Jail a user (remove all access, send to jail channel)")
    @commands.has_permissions(moderate_members=True)
    @discord.option("user", discord.Member, description="User to jail")
    @discord.option("reason", str, description="Reason for jailing")
    async def mod_jail(self, ctx: discord.ApplicationContext, user: discord.Member, reason: str):
        """Jail a user - removes all channel access except jail channel."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)
            if not db_guild or not db_guild.jail_role_id:
                await ctx.respond(
                    "Jail role not configured. Use `/mod setup-jail` first.",
                    ephemeral=True
                )
                return
            jail_channel_id = db_guild.jail_channel_id

        if user.bot:
            await ctx.respond("Cannot jail bots.", ephemeral=True)
            return

        if user.top_role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
            await ctx.respond("You cannot jail someone with a higher or equal role.", ephemeral=True)
            return

        success = await self._jail_user(ctx.guild, user, reason)
        if not success:
            await ctx.respond("Failed to jail user - check my permissions.", ephemeral=True)
            return

        # DM user
        embed = discord.Embed(
            title=f"Jailed in {ctx.guild.name}",
            description="You have been jailed. All channel access has been revoked.",
            color=discord.Color.dark_orange()
        )
        embed.add_field(name="Reason", value=reason, inline=False)
        if jail_channel_id:
            embed.add_field(
                name="What now?",
                value=f"Please go to <#{jail_channel_id}> to speak with moderators.",
                inline=False
            )
        await self.dm_user(user, embed)

        await self.log_mod_action(ctx.guild.id, ctx.author, "jail", user, reason)
        await ctx.respond(f"Jailed {user.mention}: **{reason}**", ephemeral=True)

    @mod.command(name="unjail", description="Release a user from jail")
    @commands.has_permissions(moderate_members=True)
    @discord.option("user", discord.Member, description="User to unjail")
    @discord.option("reason", str, description="Reason for unjailing", required=False)
    async def mod_unjail(self, ctx: discord.ApplicationContext, user: discord.Member, reason: str = None):
        """Release a user from jail."""
        success = await self._unjail_user(ctx.guild, user, reason or "Released by moderator")
        if not success:
            await ctx.respond("User is not jailed or I cannot unjail them.", ephemeral=True)
            return

        # DM user
        embed = discord.Embed(
            title=f"Released from Jail in {ctx.guild.name}",
            description="You have been released from jail. Your channel access has been restored.",
            color=discord.Color.green()
        )
        await self.dm_user(user, embed)

        await self.log_mod_action(ctx.guild.id, ctx.author, "unjail", user, reason)
        await ctx.respond(f"Unjailed {user.mention}.", ephemeral=True)

    @mod.command(name="mute", description="Mute a user (remove send message permissions)")
    @commands.has_permissions(moderate_members=True)
    @discord.option("user", discord.Member, description="User to mute")
    @discord.option("reason", str, description="Reason for muting", required=False)
    async def mod_mute(self, ctx: discord.ApplicationContext, user: discord.Member, reason: str = None):
        """Mute a user using the muted role."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)
            if not db_guild or not db_guild.muted_role_id:
                await ctx.respond(
                    "Muted role not configured. Use `/settings role muted @role` first.",
                    ephemeral=True
                )
                return
            muted_role_id = db_guild.muted_role_id

        muted_role = ctx.guild.get_role(muted_role_id)
        if not muted_role:
            await ctx.respond("Muted role not found.", ephemeral=True)
            return

        if muted_role in user.roles:
            await ctx.respond(f"{user.mention} is already muted.", ephemeral=True)
            return

        try:
            await user.add_roles(muted_role, reason=f"Muted by {ctx.author}: {reason or 'No reason'}")
        except discord.Forbidden:
            await ctx.respond("I don't have permission to mute this user.", ephemeral=True)
            return

        await self.log_mod_action(ctx.guild.id, ctx.author, "mute", user, reason)
        await ctx.respond(
            f"Muted {user.mention}" + (f": {reason}" if reason else ""),
            ephemeral=True
        )

    @mod.command(name="unmute", description="Unmute a user")
    @commands.has_permissions(moderate_members=True)
    @discord.option("user", discord.Member, description="User to unmute")
    async def mod_unmute(self, ctx: discord.ApplicationContext, user: discord.Member):
        """Unmute a user."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)
            if not db_guild or not db_guild.muted_role_id:
                await ctx.respond("Muted role not configured.", ephemeral=True)
                return
            muted_role_id = db_guild.muted_role_id

        muted_role = ctx.guild.get_role(muted_role_id)
        if not muted_role or muted_role not in user.roles:
            await ctx.respond(f"{user.mention} is not muted.", ephemeral=True)
            return

        try:
            await user.remove_roles(muted_role, reason=f"Unmuted by {ctx.author}")
        except discord.Forbidden:
            await ctx.respond("I don't have permission to unmute this user.", ephemeral=True)
            return

        await self.log_mod_action(ctx.guild.id, ctx.author, "unmute", user)
        await ctx.respond(f"Unmuted {user.mention}.", ephemeral=True)

    @mod.command(name="warnings", description="View warnings for a user")
    @commands.has_permissions(moderate_members=True)
    @discord.option("user", discord.Member, description="User to check")
    @discord.option("include_pardoned", bool, description="Include pardoned warnings", default=False)
    async def mod_warnings(self, ctx: discord.ApplicationContext, user: discord.Member,
                           include_pardoned: bool = False):
        """View a user's warnings."""
        with db_session_scope() as session:
            query = (
                session.query(Warning)
                .filter(Warning.guild_id == ctx.guild.id, Warning.user_id == user.id)
            )
            if not include_pardoned:
                query = query.filter(Warning.pardoned == False)

            warnings = query.order_by(Warning.issued_at.desc()).limit(10).all()

            if not warnings:
                await ctx.respond(f"{user.mention} has no warnings.", ephemeral=True)
                return

            config = self.get_mod_config(session, ctx.guild.id)
            active_count = self.get_active_warning_count(
                session, ctx.guild.id, user.id, config.warning_decay_days
            )

            embed = discord.Embed(
                title=f"Warnings for {user.display_name}",
                description=f"**Active warnings:** {active_count} (last {config.warning_decay_days} days)",
                color=discord.Color.yellow()
            )

            for w in warnings:
                status = ""
                if w.pardoned:
                    status = " [PARDONED]"
                elif not w.is_active:
                    status = " [EXPIRED]"

                time_str = f"<t:{w.issued_at}:R>"
                issuer = w.issued_by_name or "Auto-Mod"

                embed.add_field(
                    name=f"#{w.id} - {w.warning_type.value}{status}",
                    value=f"**Reason:** {w.reason[:100]}\n**By:** {issuer}\n**When:** {time_str}",
                    inline=False
                )

        await ctx.respond(embed=embed, ephemeral=True)

    @mod.command(name="pardon", description="Pardon (remove) a warning")
    @commands.has_permissions(moderate_members=True)
    @discord.option("warning_id", int, description="Warning ID to pardon")
    @discord.option("reason", str, description="Reason for pardon", required=False)
    async def mod_pardon(self, ctx: discord.ApplicationContext, warning_id: int, reason: str = None):
        """Pardon a warning."""
        with db_session_scope() as session:
            warning = (
                session.query(Warning)
                .filter(Warning.id == warning_id, Warning.guild_id == ctx.guild.id)
                .first()
            )

            if not warning:
                await ctx.respond("Warning not found.", ephemeral=True)
                return

            if warning.pardoned:
                await ctx.respond("Warning is already pardoned.", ephemeral=True)
                return

            warning.pardoned = True
            warning.pardoned_by = ctx.author.id
            warning.pardoned_at = int(time.time())
            warning.pardon_reason = reason
            warning.is_active = False

            user_id = warning.user_id

        user = ctx.guild.get_member(user_id)
        user_mention = user.mention if user else f"User {user_id}"

        await self.log_mod_action(
            ctx.guild.id, ctx.author, "pardon",
            user, reason, f"Warning #{warning_id} pardoned"
        )
        await ctx.respond(f"Pardoned warning #{warning_id} for {user_mention}.", ephemeral=True)

    @mod.command(name="setup-jail", description="Set up the jail system")
    @commands.has_permissions(administrator=True)
    @discord.option("jail_role", discord.Role, description="Role that denies all channel access")
    @discord.option("jail_channel", discord.TextChannel, description="Channel jailed users can see")
    async def mod_setup_jail(self, ctx: discord.ApplicationContext,
                              jail_role: discord.Role, jail_channel: discord.TextChannel):
        """Set up the jail system."""
        # Verify the jail role denies view permissions
        if jail_role >= ctx.guild.me.top_role:
            await ctx.respond("The jail role must be below my highest role.", ephemeral=True)
            return

        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)
            if not db_guild:
                await ctx.respond("Guild not found in database.", ephemeral=True)
                return

            db_guild.jail_role_id = jail_role.id
            db_guild.jail_channel_id = jail_channel.id

        # Set up permissions on the jail channel
        try:
            # Jail role can view and send in jail channel
            await jail_channel.set_permissions(
                jail_role,
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                reason="Jail system setup"
            )
            # @everyone cannot view jail channel
            await jail_channel.set_permissions(
                ctx.guild.default_role,
                view_channel=False,
                reason="Jail system setup"
            )
        except discord.Forbidden:
            await ctx.respond("I couldn't set up permissions on the jail channel.", ephemeral=True)
            return

        embed = discord.Embed(
            title="Jail System Configured",
            description="The jail system has been set up!",
            color=discord.Color.green()
        )
        embed.add_field(name="Jail Role", value=jail_role.mention, inline=True)
        embed.add_field(name="Jail Channel", value=jail_channel.mention, inline=True)
        embed.add_field(
            name="Important",
            value=(
                "Make sure the jail role:\n"
                "1. Denies `View Channel` on ALL other channels\n"
                "2. Is below the bot's role in the hierarchy\n"
                "3. Has no dangerous permissions"
            ),
            inline=False
        )

        await ctx.respond(embed=embed, ephemeral=True)

    # Auto-mod configuration

    @automod.command(name="config", description="Configure auto-moderation settings")
    @commands.has_permissions(administrator=True)
    @discord.option("enabled", bool, description="Enable/disable auto-mod", required=False)
    @discord.option("filter_slurs", bool, description="Filter slurs/isms", required=False)
    @discord.option("strict_mode", bool, description="Include optional patterns (may have false positives)", required=False)
    @discord.option("warnings_before_timeout", int, description="Warnings before auto-timeout (default 3)", required=False)
    @discord.option("timeout_duration", int, description="Auto-timeout duration in minutes", required=False)
    @discord.option("warnings_before_jail", int, description="Warnings before auto-jail (default 5)", required=False)
    async def automod_config(self, ctx: discord.ApplicationContext,
                              enabled: bool = None,
                              filter_slurs: bool = None,
                              strict_mode: bool = None,
                              warnings_before_timeout: int = None,
                              timeout_duration: int = None,
                              warnings_before_jail: int = None):
        """Configure auto-moderation settings."""
        with db_session_scope() as session:
            config = self.get_mod_config(session, ctx.guild.id)

            if enabled is not None:
                config.automod_enabled = enabled
            if filter_slurs is not None:
                config.filter_slurs = filter_slurs
            if strict_mode is not None:
                config.strict_slur_filter = strict_mode
            if warnings_before_timeout is not None:
                config.warnings_before_timeout = max(1, warnings_before_timeout)
            if timeout_duration is not None:
                config.timeout_duration_minutes = max(1, min(40320, timeout_duration))
            if warnings_before_jail is not None:
                config.warnings_before_jail = max(1, warnings_before_jail)

            # Return current config
            embed = discord.Embed(
                title="Auto-Mod Configuration",
                color=discord.Color.blurple()
            )
            embed.add_field(
                name="Status",
                value=f"{'Enabled' if config.automod_enabled else 'Disabled'}",
                inline=True
            )
            embed.add_field(
                name="Slur Filter",
                value=(
                    f"{'Enabled' if config.filter_slurs else 'Disabled'}\n"
                    f"Strict Mode: {'On' if config.strict_slur_filter else 'Off'}"
                ),
                inline=True
            )
            embed.add_field(
                name="Escalation",
                value=(
                    f"Timeout after: **{config.warnings_before_timeout}** warnings\n"
                    f"Timeout duration: **{config.timeout_duration_minutes}** min\n"
                    f"Jail after: **{config.warnings_before_jail}** warnings"
                ),
                inline=False
            )
            embed.add_field(
                name="Warning Decay",
                value=f"Warnings expire after **{config.warning_decay_days}** days",
                inline=True
            )
            embed.set_footer(text="Strict mode includes words like 'fag' (UK cigarette), 'cracker', etc.")

        await ctx.respond(embed=embed, ephemeral=True)

    @automod.command(name="status", description="View auto-mod status")
    @commands.has_permissions(manage_messages=True)
    async def automod_status(self, ctx: discord.ApplicationContext):
        """View current auto-mod status."""
        with db_session_scope() as session:
            config = self.get_mod_config(session, ctx.guild.id)

            embed = discord.Embed(
                title="Auto-Mod Status",
                color=discord.Color.green() if config.automod_enabled else discord.Color.red()
            )

            embed.add_field(
                name="Status",
                value=f"{'Enabled' if config.automod_enabled else 'Disabled'}",
                inline=True
            )

            filters = []
            if config.filter_slurs:
                filters.append("Slurs/Isms")
            if config.filter_spam:
                filters.append("Spam")
            if config.filter_caps:
                filters.append("Excessive Caps")
            if config.filter_links:
                filters.append("Links")
            if config.filter_mass_mentions:
                filters.append("Mass Mentions")

            embed.add_field(
                name="Active Filters",
                value=", ".join(filters) if filters else "None",
                inline=False
            )

            embed.add_field(
                name="Escalation",
                value=(
                    f"**{config.warnings_before_timeout}** warnings = Timeout ({config.timeout_duration_minutes} min)\n"
                    f"**{config.warnings_before_jail}** warnings = Jail\n"
                    f"Bans are **NEVER** automatic"
                ),
                inline=False
            )

        await ctx.respond(embed=embed, ephemeral=True)

    # Background tasks

    @tasks.loop(hours=24)
    async def warning_decay_task(self):
        """Mark old warnings as inactive (but keep in database)."""
        now = int(time.time())

        with db_session_scope() as session:
            # Get all guilds' configs
            configs = session.query(ModerationConfig).all()

            for config in configs:
                decay_cutoff = now - (config.warning_decay_days * 86400)

                # Mark old warnings as inactive
                old_warnings = (
                    session.query(Warning)
                    .filter(
                        Warning.guild_id == config.guild_id,
                        Warning.is_active == True,
                        Warning.issued_at < decay_cutoff
                    )
                    .all()
                )

                for warning in old_warnings:
                    warning.is_active = False

        logger.info("Warning decay task completed")

    @warning_decay_task.before_loop
    async def before_warning_decay(self):
        await self.bot.wait_until_ready()


def setup(bot: commands.Bot):
    bot.add_cog(ModerationCog(bot))
