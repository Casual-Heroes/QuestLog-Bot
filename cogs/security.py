# cogs/security.py - Anti-Raid Protection & Security
"""
Security cog for Warden bot.
Handles anti-raid protection, lockdown, and threat detection.

FREE FEATURES:
- Account age detection (flag <7 days)
- Mass join alerts
- Auto-quarantine new accounts
- Emergency lockdown
- Rate limiting

PREMIUM FEATURES:
- VPN/Proxy detection
- Similar name detection
- Honeypot channels
- Cross-server ban sharing
"""

import time
from collections import defaultdict
from datetime import datetime, timezone

import discord
from discord.ext import commands, tasks
from discord import SlashCommandGroup

from config import (
    db_session_scope,
    logger,
    DefaultRaidSettings,
    FeatureLimits,
    get_debug_guilds,
)
from models import (
    Guild,
    GuildMember,
    RaidConfig,
    RaidEvent,
    AuditLog,
    AuditAction,
    ModAction,
)


def get_guild_tier(session, guild_id: int) -> str:
    """Get the subscription tier for a guild."""
    db_guild = session.get(Guild, guild_id)
    if not db_guild:
        return "FREE"
    if db_guild.is_vip:
        return "PRO"
    return db_guild.subscription_tier.upper() if db_guild.subscription_tier else "FREE"

# Dangerous permissions that should trigger alerts
DANGEROUS_PERMS = [
    "administrator",
    "ban_members",
    "kick_members",
    "manage_guild",
    "manage_roles",
    "manage_channels",
    "manage_webhooks",
    "mention_everyone",
]


class SecurityCog(commands.Cog):
    """Anti-raid protection and security features."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

        # In-memory tracking for rate limiting (per guild)
        # {guild_id: [(user_id, join_timestamp), ...]}
        self._join_tracker: dict[int, list[tuple[int, float]]] = defaultdict(list)

        # Cleanup old entries periodically
        self.cleanup_join_tracker.start()

    def cog_unload(self):
        """Cleanup when cog is unloaded."""
        self.cleanup_join_tracker.cancel()

    # Slash command group
    raid = SlashCommandGroup(
        name="raid",
        description="Anti-raid protection commands",
        
    )

    # Member join event - core anti-raid logic
    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """
        Handle new member joins for anti-raid protection.

        Checks:
        1. Account age (flag/quarantine if too new)
        2. Mass join detection (trigger alert/lockdown)
        3. Rate limiting
        """
        if member.bot:
            return

        guild = member.guild
        now = time.time()

        # Track join for rate limiting
        self._join_tracker[guild.id].append((member.id, now))

        # Get guild's raid config
        with db_session_scope() as session:
            db_guild = session.get(Guild, guild.id)
            if not db_guild or not db_guild.anti_raid_enabled:
                return

            raid_config = session.get(RaidConfig, guild.id)
            if not raid_config:
                # Create default config
                raid_config = RaidConfig(
                    guild_id=guild.id,
                    min_account_age_days=DefaultRaidSettings.MIN_ACCOUNT_AGE_DAYS,
                    mass_join_threshold=DefaultRaidSettings.MASS_JOIN_THRESHOLD,
                    mass_join_window_seconds=DefaultRaidSettings.MASS_JOIN_WINDOW_SECONDS,
                )
                session.add(raid_config)
                session.flush()

            # Store config values (session will close)
            min_age_days = raid_config.min_account_age_days
            flag_new = raid_config.flag_new_accounts
            auto_quarantine = raid_config.auto_quarantine_new_accounts
            mass_threshold = raid_config.mass_join_threshold
            mass_window = raid_config.mass_join_window_seconds
            mass_action = raid_config.mass_join_action
            alert_channel_id = raid_config.alert_channel_id or db_guild.log_channel_id
            quarantine_role_id = db_guild.quarantine_role_id
            is_locked = raid_config.is_locked_down

        # If server is locked down, kick new members
        if is_locked:
            try:
                await member.send(
                    f"🔒 **{guild.name}** is currently in lockdown mode. "
                    "Please try joining again later."
                )
            except discord.Forbidden:
                pass

            try:
                await member.kick(reason="Server in lockdown mode")
                logger.info(f"Kicked {member} during lockdown in {guild.name}")
            except discord.Forbidden:
                logger.warning(f"Couldn't kick {member} during lockdown - missing permissions")
            return

        # Check 1: Account age
        account_age_days = (datetime.now(timezone.utc) - member.created_at).days
        is_new_account = account_age_days < min_age_days

        if is_new_account:
            await self._handle_new_account(
                member=member,
                account_age_days=account_age_days,
                min_age_days=min_age_days,
                flag_new=flag_new,
                auto_quarantine=auto_quarantine,
                quarantine_role_id=quarantine_role_id,
                alert_channel_id=alert_channel_id,
            )

        # Check 2: Mass join detection
        recent_joins = [
            (uid, ts) for uid, ts in self._join_tracker[guild.id]
            if now - ts <= mass_window
        ]
        self._join_tracker[guild.id] = recent_joins  # Clean up old entries

        if len(recent_joins) >= mass_threshold:
            await self._handle_mass_join(
                guild=guild,
                join_count=len(recent_joins),
                window_seconds=mass_window,
                action=mass_action,
                alert_channel_id=alert_channel_id,
            )

    async def _handle_new_account(
        self,
        member: discord.Member,
        account_age_days: int,
        min_age_days: int,
        flag_new: bool,
        auto_quarantine: bool,
        quarantine_role_id: int | None,
        alert_channel_id: int | None,
    ):
        """Handle a new account joining."""
        guild = member.guild

        # Log to audit
        with db_session_scope() as session:
            log = AuditLog(
                guild_id=guild.id,
                action=AuditAction.MEMBER_JOIN,
                action_category="security",
                target_id=member.id,
                target_name=str(member),
                target_type="user",
                details=f"New account: {account_age_days} days old (min: {min_age_days})",
            )
            session.add(log)

        # Auto-quarantine if enabled
        if auto_quarantine and quarantine_role_id:
            quarantine_role = guild.get_role(quarantine_role_id)
            if quarantine_role:
                try:
                    await member.add_roles(
                        quarantine_role,
                        reason=f"Auto-quarantine: Account only {account_age_days} days old"
                    )

                    # Update member in DB
                    with db_session_scope() as session:
                        db_member = session.get(GuildMember, (guild.id, member.id))
                        if db_member:
                            db_member.is_quarantined = True
                            db_member.quarantined_at = int(time.time())
                            db_member.quarantine_reason = f"New account ({account_age_days} days)"

                    logger.info(f"Quarantined new account {member} in {guild.name}")
                except discord.Forbidden:
                    logger.warning(f"Couldn't quarantine {member} - missing permissions")

        # Send alert if enabled
        if flag_new and alert_channel_id:
            alert_channel = guild.get_channel(alert_channel_id)
            if alert_channel:
                embed = discord.Embed(
                    title="⚠️ New Account Joined",
                    description=f"{member.mention} ({member})",
                    color=discord.Color.orange(),
                    timestamp=datetime.now(timezone.utc)
                )
                embed.add_field(
                    name="Account Age",
                    value=f"**{account_age_days} days** (minimum: {min_age_days})",
                    inline=True
                )
                embed.add_field(
                    name="Account Created",
                    value=f"<t:{int(member.created_at.timestamp())}:R>",
                    inline=True
                )
                embed.add_field(
                    name="Action Taken",
                    value="🔒 Quarantined" if auto_quarantine else "👁️ Flagged only",
                    inline=True
                )
                embed.set_thumbnail(url=member.display_avatar.url)

                try:
                    await alert_channel.send(embed=embed)
                except discord.Forbidden:
                    pass

    async def _handle_mass_join(
        self,
        guild: discord.Guild,
        join_count: int,
        window_seconds: int,
        action: str,
        alert_channel_id: int | None,
    ):
        """Handle a potential raid (mass join detected)."""
        logger.warning(
            f"RAID DETECTED in {guild.name}: {join_count} joins in {window_seconds}s"
        )

        # Log raid event to DB
        with db_session_scope() as session:
            raid_event = RaidEvent(
                guild_id=guild.id,
                join_count=join_count,
                window_seconds=window_seconds,
                action_taken=action,
            )
            session.add(raid_event)

            # Also add audit log
            log = AuditLog(
                guild_id=guild.id,
                action=AuditAction.RAID_DETECTED,
                action_category="security",
                details=f"Mass join: {join_count} joins in {window_seconds}s. Action: {action}",
            )
            session.add(log)

        # Send alert
        if alert_channel_id:
            alert_channel = guild.get_channel(alert_channel_id)
            if alert_channel:
                embed = discord.Embed(
                    title="🚨 RAID DETECTED",
                    description=(
                        f"**{join_count} accounts** joined in **{window_seconds} seconds**!\n\n"
                        f"Action taken: **{action.upper()}**"
                    ),
                    color=discord.Color.red(),
                    timestamp=datetime.now(timezone.utc)
                )

                if action == "lockdown":
                    embed.add_field(
                        name="🔒 Server Locked",
                        value="New members will be kicked until lockdown is lifted.",
                        inline=False
                    )
                elif action == "quarantine":
                    embed.add_field(
                        name="🔒 Members Quarantined",
                        value="Recent joins have been quarantined for review.",
                        inline=False
                    )

                embed.add_field(
                    name="Commands",
                    value=(
                        "`/raid lockdown` - Lock server\n"
                        "`/raid unlock` - Unlock server\n"
                        "`/raid quarantine-all` - Quarantine recent joins"
                    ),
                    inline=False
                )

                try:
                    # Try to ping admins
                    await alert_channel.send(
                        content="@here **Possible raid in progress!**",
                        embed=embed
                    )
                except discord.Forbidden:
                    pass

        # Execute action
        if action == "lockdown":
            await self._activate_lockdown(guild, reason="Automatic: Mass join detected")

    async def _activate_lockdown(
        self,
        guild: discord.Guild,
        reason: str,
        duration_minutes: int = 30
    ):
        """Activate server lockdown mode."""
        now = int(time.time())

        with db_session_scope() as session:
            raid_config = session.get(RaidConfig, guild.id)
            if raid_config:
                raid_config.is_locked_down = True
                raid_config.lockdown_started_at = now
                raid_config.lockdown_ends_at = now + (duration_minutes * 60)
                raid_config.lockdown_reason = reason

            # Audit log
            log = AuditLog(
                guild_id=guild.id,
                action=AuditAction.LOCKDOWN_ACTIVATED,
                action_category="security",
                reason=reason,
                details=f"Duration: {duration_minutes} minutes",
            )
            session.add(log)

        logger.info(f"Lockdown activated in {guild.name}: {reason}")

    # Slash commands
    @raid.command(name="status", description="Check current raid protection status")
    @commands.has_permissions(manage_guild=True)
    async def raid_status(self, ctx: discord.ApplicationContext):
        """Show raid protection status."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)
            raid_config = session.get(RaidConfig, ctx.guild.id)

            if not raid_config:
                await ctx.respond(
                    "⚠️ Raid protection not configured. Use `/warden setup` first.",
                    ephemeral=True
                )
                return

            # Get recent joins
            recent_joins = len(self._join_tracker.get(ctx.guild.id, []))

            embed = discord.Embed(
                title="🛡️ Raid Protection Status",
                color=discord.Color.red() if raid_config.is_locked_down else discord.Color.green()
            )

            # Status
            status = "🔒 **LOCKED DOWN**" if raid_config.is_locked_down else "✅ Normal"
            embed.add_field(name="Status", value=status, inline=True)
            embed.add_field(
                name="Anti-Raid",
                value="✅ Enabled" if db_guild.anti_raid_enabled else "❌ Disabled",
                inline=True
            )
            embed.add_field(
                name="Recent Joins (1min)",
                value=str(recent_joins),
                inline=True
            )

            # Settings
            embed.add_field(
                name="⚙️ Settings",
                value=(
                    f"Min Account Age: **{raid_config.min_account_age_days} days**\n"
                    f"Flag New Accounts: **{'Yes' if raid_config.flag_new_accounts else 'No'}**\n"
                    f"Auto-Quarantine: **{'Yes' if raid_config.auto_quarantine_new_accounts else 'No'}**\n"
                    f"Mass Join Threshold: **{raid_config.mass_join_threshold}** in **{raid_config.mass_join_window_seconds}s**\n"
                    f"Mass Join Action: **{raid_config.mass_join_action}**"
                ),
                inline=False
            )

            if raid_config.is_locked_down:
                embed.add_field(
                    name="🔒 Lockdown Info",
                    value=(
                        f"Reason: {raid_config.lockdown_reason or 'Unknown'}\n"
                        f"Started: <t:{raid_config.lockdown_started_at}:R>\n"
                        f"Ends: <t:{raid_config.lockdown_ends_at}:R>"
                    ),
                    inline=False
                )

        await ctx.respond(embed=embed, ephemeral=True)

    @raid.command(name="lockdown", description="Activate server lockdown (kicks new joins)")
    @commands.has_permissions(administrator=True)
    @discord.option("duration", int, description="Lockdown duration in minutes", default=30)
    @discord.option("reason", str, description="Reason for lockdown", default="Manual lockdown")
    async def raid_lockdown(
        self,
        ctx: discord.ApplicationContext,
        duration: int = 30,
        reason: str = "Manual lockdown"
    ):
        """Activate lockdown mode."""
        await self._activate_lockdown(
            guild=ctx.guild,
            reason=f"Manual: {reason} (by {ctx.author})",
            duration_minutes=duration
        )

        embed = discord.Embed(
            title="🔒 Server Locked Down",
            description=(
                f"New members will be kicked for **{duration} minutes**.\n\n"
                f"Reason: {reason}\n"
                f"To unlock: `/raid unlock`"
            ),
            color=discord.Color.red()
        )

        await ctx.respond(embed=embed)

    @raid.command(name="unlock", description="Deactivate server lockdown")
    @commands.has_permissions(administrator=True)
    async def raid_unlock(self, ctx: discord.ApplicationContext):
        """Deactivate lockdown mode."""
        with db_session_scope() as session:
            raid_config = session.get(RaidConfig, ctx.guild.id)

            if not raid_config or not raid_config.is_locked_down:
                await ctx.respond("✅ Server is not in lockdown.", ephemeral=True)
                return

            raid_config.is_locked_down = False
            raid_config.lockdown_started_at = None
            raid_config.lockdown_ends_at = None
            raid_config.lockdown_reason = None

            # Audit log
            log = AuditLog(
                guild_id=ctx.guild.id,
                action=AuditAction.LOCKDOWN_DEACTIVATED,
                action_category="security",
                actor_id=ctx.author.id,
                actor_name=str(ctx.author),
            )
            session.add(log)

        embed = discord.Embed(
            title="🔓 Lockdown Deactivated",
            description="Server is now accepting new members again.",
            color=discord.Color.green()
        )

        await ctx.respond(embed=embed)
        logger.info(f"Lockdown deactivated in {ctx.guild.name} by {ctx.author}")

    @raid.command(name="config", description="Configure raid protection settings")
    @commands.has_permissions(administrator=True)
    @discord.option("min_account_age", int, description="Minimum account age in days", required=False)
    @discord.option("auto_quarantine", bool, description="Auto-quarantine new accounts", required=False)
    @discord.option("mass_join_threshold", int, description="Number of joins to trigger alert", required=False)
    @discord.option("mass_join_window", int, description="Window in seconds for mass join detection", required=False)
    @discord.option("mass_join_action", str, description="Action: alert, lockdown, or quarantine", required=False)
    async def raid_config(
        self,
        ctx: discord.ApplicationContext,
        min_account_age: int = None,
        auto_quarantine: bool = None,
        mass_join_threshold: int = None,
        mass_join_window: int = None,
        mass_join_action: str = None,
    ):
        """Configure raid protection settings."""
        with db_session_scope() as session:
            raid_config = session.get(RaidConfig, ctx.guild.id)

            if not raid_config:
                raid_config = RaidConfig(guild_id=ctx.guild.id)
                session.add(raid_config)

            # Update provided settings
            if min_account_age is not None:
                raid_config.min_account_age_days = min_account_age
            if auto_quarantine is not None:
                raid_config.auto_quarantine_new_accounts = auto_quarantine
            if mass_join_threshold is not None:
                raid_config.mass_join_threshold = mass_join_threshold
            if mass_join_window is not None:
                raid_config.mass_join_window_seconds = mass_join_window
            if mass_join_action is not None:
                if mass_join_action in ["alert", "lockdown", "quarantine"]:
                    raid_config.mass_join_action = mass_join_action

        await ctx.respond(
            "✅ Raid protection settings updated! Use `/raid status` to view.",
            ephemeral=True
        )

    # Mod audit commands

    @raid.command(name="mod-log", description="View recent moderator actions")
    @commands.has_permissions(administrator=True)
    @discord.option("mod", discord.Member, description="Filter by moderator", required=False)
    @discord.option("action_type", str, description="Filter by action type", required=False)
    @discord.option("limit", int, description="Number of actions to show", default=10)
    async def mod_log(
        self,
        ctx: discord.ApplicationContext,
        mod: discord.Member = None,
        action_type: str = None,
        limit: int = 10,
    ):
        """View recent moderator actions."""
        with db_session_scope() as session:
            # Get tier-based log retention days
            tier = get_guild_tier(session, ctx.guild.id)
            log_days = FeatureLimits.get_limit(tier, "mod_log_days") or 7
            cutoff_time = int(time.time()) - (log_days * 86400)

            query = session.query(ModAction).filter(
                ModAction.guild_id == ctx.guild.id,
                ModAction.timestamp >= cutoff_time
            )

            if mod:
                query = query.filter(ModAction.mod_id == mod.id)
            if action_type:
                query = query.filter(ModAction.action_type == action_type)

            actions = query.order_by(ModAction.timestamp.desc()).limit(limit).all()

            if not actions:
                await ctx.respond(f"No mod actions found in the last {log_days} days.", ephemeral=True)
                return

            embed = discord.Embed(
                title=f"📋 Mod Action Log (Last {log_days} Days)",
                color=discord.Color.blue()
            )
            embed.set_footer(text=f"{tier} tier: {log_days}-day history | Upgrade for more")

            for action in actions:
                time_str = f"<t:{action.timestamp}:R>"
                embed.add_field(
                    name=f"{action.action_type} - {time_str}",
                    value=(
                        f"**Mod:** {action.mod_name}\n"
                        f"**Target:** {action.target_name or 'N/A'}\n"
                        f"**Details:** {(action.details or action.reason or 'No details')[:100]}"
                    ),
                    inline=False
                )

        await ctx.respond(embed=embed, ephemeral=True)

    @raid.command(name="audit-bots", description="Audit bot permissions in the server")
    @commands.has_permissions(administrator=True)
    async def audit_bots(self, ctx: discord.ApplicationContext):
        """Audit all bots and their permissions."""
        await ctx.defer(ephemeral=True)

        bots = [m for m in ctx.guild.members if m.bot]

        embed = discord.Embed(
            title="🤖 Bot Permission Audit",
            description=f"Found **{len(bots)}** bots in this server",
            color=discord.Color.blue()
        )

        dangerous_bots = []

        for bot in bots:
            # Get highest role permissions
            dangerous = []
            for perm in DANGEROUS_PERMS:
                if getattr(bot.guild_permissions, perm, False):
                    dangerous.append(perm)

            if dangerous:
                dangerous_bots.append((bot, dangerous))

        if dangerous_bots:
            for bot, perms in dangerous_bots[:10]:
                embed.add_field(
                    name=f"⚠️ {bot.name}",
                    value=f"Perms: {', '.join(perms[:5])}" + ("..." if len(perms) > 5 else ""),
                    inline=True
                )

            if len(dangerous_bots) > 10:
                embed.add_field(
                    name="...",
                    value=f"And {len(dangerous_bots) - 10} more bots with elevated permissions",
                    inline=False
                )
        else:
            embed.add_field(
                name="✅ All Clear",
                value="No bots with dangerous permissions found.",
                inline=False
            )

        await ctx.respond(embed=embed, ephemeral=True)

    @raid.command(name="audit-invites", description="Audit server invites")
    @commands.has_permissions(manage_guild=True)
    async def audit_invites(self, ctx: discord.ApplicationContext):
        """Audit all server invites."""
        await ctx.defer(ephemeral=True)

        try:
            invites = await ctx.guild.invites()
        except discord.Forbidden:
            await ctx.respond("I don't have permission to view invites.", ephemeral=True)
            return

        if not invites:
            await ctx.respond("No active invites found.", ephemeral=True)
            return

        embed = discord.Embed(
            title="🔗 Invite Audit",
            description=f"Found **{len(invites)}** active invites",
            color=discord.Color.blue()
        )

        # Sort by uses
        sorted_invites = sorted(invites, key=lambda i: i.uses or 0, reverse=True)

        for invite in sorted_invites[:15]:
            creator = invite.inviter.mention if invite.inviter else "Unknown"
            expires = f"<t:{int(invite.expires_at.timestamp())}:R>" if invite.expires_at else "Never"
            embed.add_field(
                name=f"`{invite.code}` - {invite.uses or 0} uses",
                value=f"By: {creator}\nExpires: {expires}",
                inline=True
            )

        if len(invites) > 15:
            embed.set_footer(text=f"Showing top 15 of {len(invites)} invites")

        await ctx.respond(embed=embed, ephemeral=True)

    @raid.command(name="purge-invites", description="Delete all invites from a user")
    @commands.has_permissions(manage_guild=True)
    @discord.option("user", discord.Member, description="User whose invites to delete")
    async def purge_invites(
        self,
        ctx: discord.ApplicationContext,
        user: discord.Member,
    ):
        """Delete all invites created by a user."""
        await ctx.defer(ephemeral=True)

        try:
            invites = await ctx.guild.invites()
        except discord.Forbidden:
            await ctx.respond("I don't have permission to manage invites.", ephemeral=True)
            return

        user_invites = [i for i in invites if i.inviter and i.inviter.id == user.id]

        if not user_invites:
            await ctx.respond(f"{user.mention} has no active invites.", ephemeral=True)
            return

        deleted = 0
        for invite in user_invites:
            try:
                await invite.delete(reason=f"Purged by {ctx.author}")
                deleted += 1
            except discord.Forbidden:
                pass

        # Log the action
        with db_session_scope() as session:
            action = ModAction(
                guild_id=ctx.guild.id,
                mod_id=ctx.author.id,
                mod_name=str(ctx.author),
                action_type="invite_purge",
                target_id=user.id,
                target_name=str(user),
                target_type="user",
                details=f"Deleted {deleted} invites",
            )
            session.add(action)

        await ctx.respond(
            f"✅ Deleted **{deleted}** invites from {user.mention}",
            ephemeral=True
        )

    # Permission change alerts

    @commands.Cog.listener()
    async def on_guild_role_update(self, before: discord.Role, after: discord.Role):
        """Alert when dangerous permissions are added to a role."""
        guild = after.guild

        with db_session_scope() as session:
            db_guild = session.get(Guild, guild.id)
            if not db_guild or not db_guild.anti_raid_enabled:
                return
            alert_channel_id = db_guild.log_channel_id

        if not alert_channel_id:
            return

        # Check for new dangerous permissions
        before_perms = dict(before.permissions)
        after_perms = dict(after.permissions)

        new_dangerous = []
        for perm in DANGEROUS_PERMS:
            if not before_perms.get(perm) and after_perms.get(perm):
                new_dangerous.append(perm)

        if not new_dangerous:
            return

        alert_channel = guild.get_channel(alert_channel_id)
        if not alert_channel:
            return

        embed = discord.Embed(
            title="⚠️ Dangerous Permission Added",
            description=f"Role **{after.name}** was updated with elevated permissions!",
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc)
        )

        embed.add_field(
            name="New Permissions",
            value=", ".join(new_dangerous),
            inline=False
        )

        embed.add_field(
            name="Members with this role",
            value=str(len(after.members)),
            inline=True
        )

        # Log to audit
        with db_session_scope() as session:
            log = AuditLog(
                guild_id=guild.id,
                action=AuditAction.PERMISSION_UPDATE,
                action_category="security",
                target_id=after.id,
                target_name=after.name,
                target_type="role",
                details=f"New dangerous perms: {', '.join(new_dangerous)}",
            )
            session.add(log)

        try:
            await alert_channel.send(embed=embed)
        except discord.Forbidden:
            pass

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """Alert when someone gains dangerous roles."""
        if before.roles == after.roles:
            return

        guild = after.guild

        with db_session_scope() as session:
            db_guild = session.get(Guild, guild.id)
            if not db_guild or not db_guild.anti_raid_enabled:
                return
            alert_channel_id = db_guild.log_channel_id

        if not alert_channel_id:
            return

        # Find new roles with dangerous permissions
        new_roles = set(after.roles) - set(before.roles)
        dangerous_new = []

        for role in new_roles:
            for perm in DANGEROUS_PERMS:
                if getattr(role.permissions, perm, False):
                    dangerous_new.append(role)
                    break

        if not dangerous_new:
            return

        alert_channel = guild.get_channel(alert_channel_id)
        if not alert_channel:
            return

        embed = discord.Embed(
            title="⚠️ Elevated Role Assigned",
            description=f"{after.mention} received role(s) with elevated permissions!",
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc)
        )

        embed.add_field(
            name="New Roles",
            value=", ".join(r.mention for r in dangerous_new),
            inline=False
        )

        embed.set_thumbnail(url=after.display_avatar.url)

        # Log to audit
        with db_session_scope() as session:
            log = AuditLog(
                guild_id=guild.id,
                action=AuditAction.ROLE_ADD,
                action_category="security",
                target_id=after.id,
                target_name=str(after),
                target_type="user",
                details=f"Dangerous roles: {', '.join(r.name for r in dangerous_new)}",
            )
            session.add(log)

        try:
            await alert_channel.send(embed=embed)
        except discord.Forbidden:
            pass

    # Background tasks
    @tasks.loop(minutes=5)
    async def cleanup_join_tracker(self):
        """Clean up old entries from join tracker."""
        now = time.time()
        cutoff = now - 300  # 5 minutes

        for guild_id in list(self._join_tracker.keys()):
            self._join_tracker[guild_id] = [
                (uid, ts) for uid, ts in self._join_tracker[guild_id]
                if ts > cutoff
            ]
            # Remove empty entries
            if not self._join_tracker[guild_id]:
                del self._join_tracker[guild_id]

    @cleanup_join_tracker.before_loop
    async def before_cleanup(self):
        """Wait for bot to be ready before starting task."""
        await self.bot.wait_until_ready()


def setup(bot: commands.Bot):
    """Load the cog."""
    bot.add_cog(SecurityCog(bot))
