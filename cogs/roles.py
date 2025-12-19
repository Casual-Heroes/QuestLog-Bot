# cogs/roles.py - Role & IAM Management
"""
Comprehensive role management for QuestLog.

Features:
- Mass assign/remove roles
- Role templates
- Temp roles with auto-expiry
- Role request system with approval workflow
- Access export/audit
- React-to-role
- Level-based roles
- Dangerous permission detection
"""

import time
import asyncio
import io
import csv
import discord
from discord.ext import commands, tasks

from config import db_session_scope, logger, get_debug_guilds, FeatureLimits
from models import (
    Guild, GuildMember, ReactRole, LevelRole,
    TempRole, RoleTemplate, RoleRequest, ModAction, ChannelTemplate
)


def get_guild_tier(session, guild_id: int) -> str:
    """Get the subscription tier for a guild."""
    db_guild = session.get(Guild, guild_id)
    if not db_guild:
        return "FREE"
    # VIP gets Pro features
    if db_guild.is_vip:
        return "PRO"
    return db_guild.subscription_tier.upper() if db_guild.subscription_tier else "FREE"


async def check_limit_and_respond(
    ctx: discord.ApplicationContext,
    session,
    feature: str,
    current_count: int = 0,
    action_count: int = 1
) -> tuple[bool, int | None]:
    """
    Check if action is within limits. Returns (is_allowed, limit).
    If not allowed, sends upgrade message and returns False.
    """
    tier = get_guild_tier(session, ctx.guild.id)
    limit = FeatureLimits.get_limit(tier, feature)

    # None = unlimited
    if limit is None:
        return (True, None)

    # Boolean feature (like featured_pool)
    if isinstance(limit, bool):
        if not limit:
            upgrade_msg = FeatureLimits.get_upgrade_message(feature, tier)
            await ctx.respond(f"⭐ {upgrade_msg}", ephemeral=True)
            return (False, limit)
        return (True, limit)

    # Numeric limit - check if action would exceed
    if current_count + action_count > limit:
        upgrade_msg = FeatureLimits.get_upgrade_message(feature, tier)
        await ctx.respond(
            f"⚠️ **Limit Reached!** You can only use **{limit}** {feature.replace('_', ' ')} on the {tier} tier.\n"
            f"Current: {current_count} | Requested: {action_count}\n\n"
            f"⭐ {upgrade_msg}",
            ephemeral=True
        )
        return (False, limit)

    return (True, limit)

# Dangerous permissions that should trigger alerts
DANGEROUS_PERMS = [
    "administrator",
    "ban_members",
    "kick_members",
    "manage_guild",
    "manage_roles",
    "manage_channels",
    "manage_webhooks",
    "manage_messages",
    "mention_everyone",
]


class TempRoleModal(discord.ui.Modal):
    """Modal for requesting a temporary role."""

    def __init__(self, role: discord.Role):
        super().__init__(title=f"Request: {role.name[:40]}")
        self.role = role

        self.event_name = discord.ui.InputText(
            label="Event Name",
            placeholder="e.g., Charity Stream 2024",
            max_length=255,
            required=True,
        )
        self.add_item(self.event_name)

        self.duration = discord.ui.InputText(
            label="Duration (hours)",
            placeholder="e.g., 24, 48, 72",
            max_length=5,
            required=True,
        )
        self.add_item(self.duration)

        self.reason = discord.ui.InputText(
            label="Reason for Request",
            style=discord.InputTextStyle.paragraph,
            placeholder="Why do you need this role?",
            max_length=500,
            required=True,
        )
        self.add_item(self.reason)

    async def callback(self, interaction: discord.Interaction):
        try:
            duration_hours = int(self.duration.value)
            if duration_hours < 1 or duration_hours > 720:
                await interaction.response.send_message(
                    "Duration must be between 1 and 720 hours (30 days).",
                    ephemeral=True
                )
                return
        except ValueError:
            await interaction.response.send_message(
                "Please enter a valid number for duration.",
                ephemeral=True
            )
            return

        # Create role request
        with db_session_scope() as session:
            db_guild = session.get(Guild, interaction.guild.id)
            if not db_guild:
                await interaction.response.send_message("Server not configured.", ephemeral=True)
                return

            request = RoleRequest(
                guild_id=interaction.guild.id,
                user_id=interaction.user.id,
                role_id=self.role.id,
                reason=self.reason.value,
                is_temp_request=True,
                requested_duration_hours=duration_hours,
                event_name=self.event_name.value,
            )
            session.add(request)
            session.flush()
            request_id = request.id
            log_channel_id = db_guild.log_channel_id

        # Send to mod channel for approval
        if log_channel_id:
            log_channel = interaction.guild.get_channel(log_channel_id)
            if log_channel:
                embed = discord.Embed(
                    title="🎫 Temp Role Request",
                    color=discord.Color.blue()
                )
                embed.add_field(name="User", value=interaction.user.mention, inline=True)
                embed.add_field(name="Role", value=self.role.mention, inline=True)
                embed.add_field(name="Duration", value=f"{duration_hours} hours", inline=True)
                embed.add_field(name="Event", value=self.event_name.value, inline=False)
                embed.add_field(name="Reason", value=self.reason.value, inline=False)
                embed.set_footer(text=f"Request ID: {request_id}")

                view = RoleRequestButtons(request_id)
                msg = await log_channel.send(embed=embed, view=view)

                with db_session_scope() as session:
                    req = session.get(RoleRequest, request_id)
                    if req:
                        req.message_id = msg.id
                        req.channel_id = log_channel.id

        await interaction.response.send_message(
            f"✅ Your request for **{self.role.name}** has been submitted for review!",
            ephemeral=True
        )


class RoleRequestButtons(discord.ui.View):
    """Approval/Denial buttons for role requests."""

    def __init__(self, request_id: int):
        super().__init__(timeout=None)
        self.request_id = request_id

    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success, emoji="✅")
    async def approve(self, button: discord.ui.Button, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.manage_roles:
            await interaction.response.send_message("You don't have permission.", ephemeral=True)
            return

        with db_session_scope() as session:
            request = session.get(RoleRequest, self.request_id)
            if not request or request.status != "pending":
                await interaction.response.send_message("Request already processed.", ephemeral=True)
                return

            request.status = "approved"
            request.reviewed_by = interaction.user.id
            request.reviewed_at = int(time.time())

            user_id = request.user_id
            role_id = request.role_id
            duration_hours = request.requested_duration_hours
            event_name = request.event_name
            is_temp = request.is_temp_request

        member = interaction.guild.get_member(user_id)
        role = interaction.guild.get_role(role_id)

        if not member or not role:
            await interaction.response.send_message("User or role not found.", ephemeral=True)
            return

        try:
            await member.add_roles(role, reason=f"Approved by {interaction.user}")

            if is_temp and duration_hours:
                expires_at = int(time.time()) + (duration_hours * 3600)
                with db_session_scope() as session:
                    temp = TempRole(
                        guild_id=interaction.guild.id,
                        user_id=user_id,
                        role_id=role_id,
                        assigned_by=interaction.user.id,
                        expires_at=expires_at,
                        reason="Approved request",
                        event_name=event_name,
                    )
                    session.add(temp)

            embed = interaction.message.embeds[0] if interaction.message.embeds else None
            if embed:
                embed.color = discord.Color.green()
                embed.add_field(name="✅ Approved", value=f"By {interaction.user.mention}", inline=False)
                await interaction.message.edit(embed=embed, view=None)

            await interaction.response.send_message(
                f"✅ Approved! {member.mention} now has {role.mention}" +
                (f" for {duration_hours}h" if is_temp else ""),
                ephemeral=True
            )

            try:
                await member.send(
                    f"✅ Your request for **{role.name}** in **{interaction.guild.name}** was approved!" +
                    (f"\nThis role will expire in {duration_hours} hours." if is_temp else "")
                )
            except discord.Forbidden:
                pass

        except discord.Forbidden:
            await interaction.response.send_message("I don't have permission to assign that role.", ephemeral=True)

    @discord.ui.button(label="Deny", style=discord.ButtonStyle.danger, emoji="❌")
    async def deny(self, button: discord.ui.Button, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.manage_roles:
            await interaction.response.send_message("You don't have permission.", ephemeral=True)
            return

        with db_session_scope() as session:
            request = session.get(RoleRequest, self.request_id)
            if not request or request.status != "pending":
                await interaction.response.send_message("Request already processed.", ephemeral=True)
                return

            request.status = "denied"
            request.reviewed_by = interaction.user.id
            request.reviewed_at = int(time.time())
            user_id = request.user_id
            role_id = request.role_id

        embed = interaction.message.embeds[0] if interaction.message.embeds else None
        if embed:
            embed.color = discord.Color.red()
            embed.add_field(name="❌ Denied", value=f"By {interaction.user.mention}", inline=False)
            await interaction.message.edit(embed=embed, view=None)

        await interaction.response.send_message("Request denied.", ephemeral=True)

        member = interaction.guild.get_member(user_id)
        role = interaction.guild.get_role(role_id)
        if member:
            try:
                await member.send(
                    f"❌ Your request for **{role.name if role else 'the role'}** "
                    f"in **{interaction.guild.name}** was denied."
                )
            except discord.Forbidden:
                pass


class RolesCog(commands.Cog):
    """Role & IAM management system."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.check_temp_roles.start()

    def cog_unload(self):
        self.check_temp_roles.cancel()

    roles = discord.SlashCommandGroup(
        name="roles",
        description="Role management commands",
        
    )

    iam = discord.SlashCommandGroup(
        name="iam",
        description="IAM & access management",
        
    )

    # Background task to expire temp roles

    @tasks.loop(minutes=5)
    async def check_temp_roles(self):
        """Check and expire temporary roles."""
        now = int(time.time())

        with db_session_scope() as session:
            expired = (
                session.query(TempRole)
                .filter(
                    TempRole.is_active == True,
                    TempRole.expires_at <= now
                )
                .all()
            )

            for temp in expired:
                guild = self.bot.get_guild(temp.guild_id)
                if not guild:
                    continue

                member = guild.get_member(temp.user_id)
                role = guild.get_role(temp.role_id)

                if member and role and role in member.roles:
                    try:
                        await member.remove_roles(role, reason="Temp role expired")
                        logger.info(f"Expired temp role {role.name} from {member} in {guild.name}")

                        try:
                            await member.send(
                                f"⏰ Your temporary role **{role.name}** in **{guild.name}** has expired."
                            )
                        except discord.Forbidden:
                            pass

                    except discord.Forbidden:
                        logger.warning(f"Cannot remove expired role {role.id} from {member.id}")

                temp.is_active = False
                temp.revoked_at = now

    @check_temp_roles.before_loop
    async def before_check_temp_roles(self):
        await self.bot.wait_until_ready()

    # Mod action logging helper

    async def log_mod_action(self, guild_id: int, mod: discord.Member, action_type: str,
                              target_id: int = None, target_name: str = None,
                              target_type: str = None, reason: str = None, details: str = None):
        """Log a moderator action."""
        with db_session_scope() as session:
            action = ModAction(
                guild_id=guild_id,
                mod_id=mod.id,
                mod_name=str(mod),
                action_type=action_type,
                target_id=target_id,
                target_name=target_name,
                target_type=target_type,
                reason=reason,
                details=details,
            )
            session.add(action)

    # Mass role operations

    @iam.command(name="mass-assign", description="Mass assign a role to multiple users")
    @commands.has_permissions(manage_roles=True)
    @discord.option("role", discord.Role, description="Role to assign")
    @discord.option("target", str, description="Target: 'all', 'humans', 'bots', or @role mention")
    async def mass_assign(self, ctx: discord.ApplicationContext, role: discord.Role, target: str):
        """Mass assign a role to users."""
        if role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
            await ctx.respond("❌ You cannot assign a role higher than your own.", ephemeral=True)
            return

        if role >= ctx.guild.me.top_role:
            await ctx.respond("❌ I cannot assign a role higher than my own.", ephemeral=True)
            return

        if target.lower() == "all":
            members = [m for m in ctx.guild.members if role not in m.roles]
        elif target.lower() == "humans":
            members = [m for m in ctx.guild.members if not m.bot and role not in m.roles]
        elif target.lower() == "bots":
            members = [m for m in ctx.guild.members if m.bot and role not in m.roles]
        elif target.startswith("<@&") and target.endswith(">"):
            role_id = int(target[3:-1])
            source_role = ctx.guild.get_role(role_id)
            if not source_role:
                await ctx.respond("❌ Could not find that role.", ephemeral=True)
                return
            members = [m for m in source_role.members if role not in m.roles]
        else:
            await ctx.respond("❌ Invalid target. Use 'all', 'humans', 'bots', or @role.", ephemeral=True)
            return

        if not members:
            await ctx.respond("No users to assign the role to.", ephemeral=True)
            return

        # Check bulk operation limit
        with db_session_scope() as session:
            allowed, limit = await check_limit_and_respond(
                ctx, session, "bulk_users_per_action", current_count=0, action_count=len(members)
            )
            if not allowed:
                return

        await ctx.defer(ephemeral=True)

        # If limit exists and members exceed it, only process up to limit
        if limit and len(members) > limit:
            members = members[:limit]
            await ctx.followup.send(
                f"📊 Processing first **{limit}** members (tier limit). Upgrade for more!",
                ephemeral=True
            )

        await ctx.followup.send(f"⏳ Assigning **{role.name}** to **{len(members)}** members...", ephemeral=True)

        success = 0
        failed = 0

        for member in members:
            try:
                await member.add_roles(role, reason=f"Mass assign by {ctx.author}")
                success += 1
                await asyncio.sleep(0.5)
            except discord.Forbidden:
                failed += 1

        await self.log_mod_action(
            ctx.guild.id, ctx.author, "mass_role_assign",
            target_id=role.id, target_name=role.name, target_type="role",
            details=f"Assigned to {success} members, {failed} failed"
        )

        await ctx.followup.send(
            f"✅ Assigned **{role.name}** to **{success}** members." +
            (f" ({failed} failed)" if failed else ""),
            ephemeral=True
        )

    @iam.command(name="mass-remove", description="Mass remove a role from multiple users")
    @commands.has_permissions(manage_roles=True)
    @discord.option("role", discord.Role, description="Role to remove")
    @discord.option("target", str, description="Target: 'all' or @role to filter", default="all")
    async def mass_remove(self, ctx: discord.ApplicationContext, role: discord.Role, target: str):
        """Mass remove a role from users."""
        if role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
            await ctx.respond("❌ You cannot remove a role higher than your own.", ephemeral=True)
            return

        members = list(role.members)

        if target.startswith("<@&") and target.endswith(">"):
            role_id = int(target[3:-1])
            filter_role = ctx.guild.get_role(role_id)
            if filter_role:
                members = [m for m in members if filter_role in m.roles]

        if not members:
            await ctx.respond("No users have that role.", ephemeral=True)
            return

        # Check bulk operation limit
        with db_session_scope() as session:
            allowed, limit = await check_limit_and_respond(
                ctx, session, "bulk_users_per_action", current_count=0, action_count=len(members)
            )
            if not allowed:
                return

        await ctx.defer(ephemeral=True)

        # If limit exists and members exceed it, only process up to limit
        if limit and len(members) > limit:
            members = members[:limit]
            await ctx.followup.send(
                f"📊 Processing first **{limit}** members (tier limit). Upgrade for more!",
                ephemeral=True
            )

        await ctx.followup.send(f"⏳ Removing **{role.name}** from **{len(members)}** members...", ephemeral=True)

        success = 0
        failed = 0

        for member in members:
            try:
                await member.remove_roles(role, reason=f"Mass remove by {ctx.author}")
                success += 1
                await asyncio.sleep(0.5)
            except discord.Forbidden:
                failed += 1

        await self.log_mod_action(
            ctx.guild.id, ctx.author, "mass_role_remove",
            target_id=role.id, target_name=role.name, target_type="role",
            details=f"Removed from {success} members, {failed} failed"
        )

        await ctx.followup.send(
            f"✅ Removed **{role.name}** from **{success}** members." +
            (f" ({failed} failed)" if failed else ""),
            ephemeral=True
        )

    # Role templates

    @iam.command(name="save-template", description="Save a role as a template")
    @commands.has_permissions(manage_roles=True)
    @discord.option("role", discord.Role, description="Role to save as template")
    @discord.option("template_name", str, description="Name for the template")
    @discord.option("description", str, description="Description", required=False)
    async def save_template(
        self, ctx: discord.ApplicationContext,
        role: discord.Role, template_name: str, description: str = None
    ):
        """Save a role configuration as a template."""
        with db_session_scope() as session:
            existing = (
                session.query(RoleTemplate)
                .filter(RoleTemplate.guild_id == ctx.guild.id, RoleTemplate.name == template_name)
                .first()
            )

            if existing:
                await ctx.respond(f"❌ Template '{template_name}' already exists.", ephemeral=True)
                return

            # Count total templates (role + channel combined)
            role_template_count = (
                session.query(RoleTemplate)
                .filter(RoleTemplate.guild_id == ctx.guild.id)
                .count()
            )
            channel_template_count = (
                session.query(ChannelTemplate)
                .filter(ChannelTemplate.guild_id == ctx.guild.id)
                .count()
            )
            total_templates = role_template_count + channel_template_count

            # Check limit
            allowed, limit = await check_limit_and_respond(
                ctx, session, "templates", current_count=total_templates, action_count=1
            )
            if not allowed:
                return

            template = RoleTemplate(
                guild_id=ctx.guild.id,
                name=template_name,
                description=description or f"Template from {role.name}",
                color=role.color.value,
                hoist=role.hoist,
                mentionable=role.mentionable,
                permissions_value=role.permissions.value,
                created_by=ctx.author.id,
            )
            session.add(template)

        await ctx.respond(f"✅ Saved **{role.name}** as template **{template_name}**", ephemeral=True)

    @iam.command(name="create-from-template", description="Create a role from a template")
    @commands.has_permissions(manage_roles=True)
    @discord.option("template_name", str, description="Template name")
    @discord.option("role_name", str, description="Name for the new role")
    async def create_from_template(
        self, ctx: discord.ApplicationContext,
        template_name: str, role_name: str
    ):
        """Create a new role from a template."""
        with db_session_scope() as session:
            template = (
                session.query(RoleTemplate)
                .filter(RoleTemplate.guild_id == ctx.guild.id, RoleTemplate.name == template_name)
                .first()
            )

            if not template:
                await ctx.respond(f"❌ Template '{template_name}' not found.", ephemeral=True)
                return

            color = template.color
            hoist = template.hoist
            mentionable = template.mentionable
            perms_value = template.permissions_value
            template.use_count += 1

        try:
            new_role = await ctx.guild.create_role(
                name=role_name,
                color=discord.Color(color),
                hoist=hoist,
                mentionable=mentionable,
                permissions=discord.Permissions(perms_value),
                reason=f"Created from template '{template_name}' by {ctx.author}"
            )

            await self.log_mod_action(
                ctx.guild.id, ctx.author, "role_create_from_template",
                target_id=new_role.id, target_name=role_name, target_type="role",
                details=f"From template: {template_name}"
            )

            await ctx.respond(
                f"✅ Created role {new_role.mention} from template **{template_name}**",
                ephemeral=True
            )

        except discord.Forbidden:
            await ctx.respond("❌ I don't have permission to create roles.", ephemeral=True)

    @iam.command(name="list-templates", description="List available role templates")
    @commands.has_permissions(manage_roles=True)
    async def list_templates(self, ctx: discord.ApplicationContext):
        """List all role templates."""
        with db_session_scope() as session:
            templates = (
                session.query(RoleTemplate)
                .filter(RoleTemplate.guild_id == ctx.guild.id)
                .order_by(RoleTemplate.name)
                .all()
            )

            if not templates:
                await ctx.respond("No role templates saved yet.", ephemeral=True)
                return

            embed = discord.Embed(title="📋 Role Templates", color=discord.Color.blue())

            for t in templates:
                embed.add_field(
                    name=t.name,
                    value=f"{t.description or 'No description'}\nUsed: {t.use_count} times",
                    inline=True
                )

        await ctx.respond(embed=embed, ephemeral=True)

    # Temp roles

    @iam.command(name="temp-role", description="Assign a temporary role")
    @commands.has_permissions(manage_roles=True)
    @discord.option("member", discord.Member, description="Member to assign")
    @discord.option("role", discord.Role, description="Role to assign")
    @discord.option("hours", int, description="Duration in hours (1-720)")
    @discord.option("reason", str, description="Reason", required=False)
    @discord.option("event_name", str, description="Event name (optional)", required=False)
    async def temp_role(
        self, ctx: discord.ApplicationContext,
        member: discord.Member, role: discord.Role, hours: int,
        reason: str = None, event_name: str = None
    ):
        """Assign a temporary role that auto-expires."""
        if hours < 1 or hours > 720:
            await ctx.respond("Duration must be between 1 and 720 hours.", ephemeral=True)
            return

        if role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
            await ctx.respond("❌ You cannot assign a role higher than your own.", ephemeral=True)
            return

        # Check active temp roles limit
        with db_session_scope() as session:
            active_count = (
                session.query(TempRole)
                .filter(TempRole.guild_id == ctx.guild.id, TempRole.is_active == True)
                .count()
            )

            allowed, limit = await check_limit_and_respond(
                ctx, session, "active_temp_roles", current_count=active_count, action_count=1
            )
            if not allowed:
                return

        try:
            await member.add_roles(role, reason=f"Temp role by {ctx.author}: {reason or 'No reason'}")
        except discord.Forbidden:
            await ctx.respond("❌ I don't have permission to assign that role.", ephemeral=True)
            return

        expires_at = int(time.time()) + (hours * 3600)

        with db_session_scope() as session:
            temp = TempRole(
                guild_id=ctx.guild.id,
                user_id=member.id,
                role_id=role.id,
                assigned_by=ctx.author.id,
                expires_at=expires_at,
                reason=reason,
                event_name=event_name,
            )
            session.add(temp)

        await self.log_mod_action(
            ctx.guild.id, ctx.author, "temp_role_assign",
            target_id=member.id, target_name=str(member), target_type="user",
            details=f"Role: {role.name}, Duration: {hours}h"
        )

        await ctx.respond(
            f"✅ Assigned **{role.name}** to {member.mention} for **{hours}** hours.\n"
            f"Expires: <t:{expires_at}:R>",
            ephemeral=True
        )

    @iam.command(name="request-role", description="Request a temporary role (with approval)")
    @discord.option("role", discord.Role, description="Role to request")
    async def request_role(self, ctx: discord.ApplicationContext, role: discord.Role):
        """Open a form to request a temporary role."""
        modal = TempRoleModal(role)
        await ctx.send_modal(modal)

    @iam.command(name="pending-requests", description="View pending role requests")
    @commands.has_permissions(manage_roles=True)
    async def pending_requests(self, ctx: discord.ApplicationContext):
        """View pending role requests."""
        with db_session_scope() as session:
            requests = (
                session.query(RoleRequest)
                .filter(RoleRequest.guild_id == ctx.guild.id, RoleRequest.status == "pending")
                .order_by(RoleRequest.requested_at.desc())
                .limit(10)
                .all()
            )

            if not requests:
                await ctx.respond("No pending role requests.", ephemeral=True)
                return

            embed = discord.Embed(title="🎫 Pending Role Requests", color=discord.Color.blue())

            for req in requests:
                member = ctx.guild.get_member(req.user_id)
                role = ctx.guild.get_role(req.role_id)
                embed.add_field(
                    name=f"ID: {req.id}",
                    value=(
                        f"User: {member.mention if member else req.user_id}\n"
                        f"Role: {role.mention if role else req.role_id}\n"
                        f"Duration: {req.requested_duration_hours}h\n"
                        f"Event: {req.event_name or 'N/A'}"
                    ),
                    inline=True
                )

        await ctx.respond(embed=embed, ephemeral=True)

    # Access audit

    @iam.command(name="export-role", description="Export all users with a role to CSV")
    @commands.has_permissions(manage_roles=True)
    @discord.option("role", discord.Role, description="Role to export")
    async def export_role(self, ctx: discord.ApplicationContext, role: discord.Role):
        """Export all members with a role to a CSV file."""
        members = list(role.members)
        if not members:
            await ctx.respond(f"No members have the {role.name} role.", ephemeral=True)
            return

        # Check export limit
        with db_session_scope() as session:
            tier = get_guild_tier(session, ctx.guild.id)
            limit = FeatureLimits.get_limit(tier, "export_members")

        # Limit export if needed
        export_limited = False
        if limit and len(members) > limit:
            members = members[:limit]
            export_limited = True

        await ctx.defer(ephemeral=True)

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["User ID", "Username", "Display Name", "Joined At", "Account Created"])

        for member in members:
            writer.writerow([
                member.id,
                str(member),
                member.display_name,
                member.joined_at.isoformat() if member.joined_at else "Unknown",
                member.created_at.isoformat(),
            ])

        output.seek(0)
        file = discord.File(
            io.BytesIO(output.getvalue().encode()),
            filename=f"{role.name}_members_{int(time.time())}.csv"
        )

        await self.log_mod_action(
            ctx.guild.id, ctx.author, "role_export",
            target_id=role.id, target_name=role.name, target_type="role",
            details=f"Exported {len(members)} members"
        )

        response = f"📊 Exported **{len(members)}** members with **{role.name}**:"
        if export_limited:
            response += f"\n⚠️ *Limited to {limit} members. Upgrade for more!*"

        await ctx.respond(response, file=file, ephemeral=True)

    @iam.command(name="audit-mods", description="Audit moderator role assignments")
    @commands.has_permissions(administrator=True)
    async def audit_mods(self, ctx: discord.ApplicationContext):
        """Audit all users with dangerous permissions."""
        await ctx.defer(ephemeral=True)

        dangerous_roles = []
        for role in ctx.guild.roles:
            for perm in DANGEROUS_PERMS:
                if getattr(role.permissions, perm, False):
                    dangerous_roles.append(role)
                    break

        if not dangerous_roles:
            await ctx.respond("No roles with dangerous permissions found.", ephemeral=True)
            return

        embed = discord.Embed(
            title="🔒 Mod/Admin Audit",
            description="Users with elevated permissions",
            color=discord.Color.orange()
        )

        for role in sorted(dangerous_roles, key=lambda r: r.position, reverse=True):
            perms = [p for p in DANGEROUS_PERMS if getattr(role.permissions, p, False)]
            members = [m.mention for m in role.members[:10]]
            member_text = ", ".join(members) if members else "No members"
            if len(role.members) > 10:
                member_text += f" (+{len(role.members) - 10} more)"

            embed.add_field(
                name=f"{role.name} ({len(role.members)} members)",
                value=f"**Perms:** {', '.join(perms)}\n**Members:** {member_text}",
                inline=False
            )

        await ctx.respond(embed=embed, ephemeral=True)

    @iam.command(name="permission-diff", description="Compare permissions between two roles")
    @commands.has_permissions(manage_roles=True)
    @discord.option("role1", discord.Role, description="First role")
    @discord.option("role2", discord.Role, description="Second role")
    async def permission_diff(
        self, ctx: discord.ApplicationContext,
        role1: discord.Role, role2: discord.Role
    ):
        """Compare permissions between two roles."""
        perms1 = dict(role1.permissions)
        perms2 = dict(role2.permissions)

        only_in_1 = [p for p, v in perms1.items() if v and not perms2.get(p)]
        only_in_2 = [p for p, v in perms2.items() if v and not perms1.get(p)]
        in_both = [p for p, v in perms1.items() if v and perms2.get(p)]

        embed = discord.Embed(title="🔍 Permission Comparison", color=discord.Color.blue())

        embed.add_field(
            name=f"Only in {role1.name}",
            value=", ".join(only_in_1) if only_in_1 else "None",
            inline=False
        )
        embed.add_field(
            name=f"Only in {role2.name}",
            value=", ".join(only_in_2) if only_in_2 else "None",
            inline=False
        )
        embed.add_field(
            name="In Both",
            value=", ".join(in_both[:20]) + ("..." if len(in_both) > 20 else "") if in_both else "None",
            inline=False
        )

        await ctx.respond(embed=embed, ephemeral=True)

    @iam.command(name="emergency-strip", description="Emergency strip all elevated roles from a user")
    @commands.has_permissions(administrator=True)
    @discord.option("member", discord.Member, description="Member to strip roles from")
    @discord.option("reason", str, description="Reason for emergency strip")
    async def emergency_strip(
        self, ctx: discord.ApplicationContext,
        member: discord.Member, reason: str
    ):
        """Emergency strip all elevated roles from a user."""
        dangerous_roles = []
        for role in member.roles:
            if role.is_default():
                continue
            for perm in DANGEROUS_PERMS:
                if getattr(role.permissions, perm, False):
                    dangerous_roles.append(role)
                    break

        if not dangerous_roles:
            await ctx.respond(f"{member.mention} has no elevated roles.", ephemeral=True)
            return

        await ctx.defer(ephemeral=True)

        removed = []
        for role in dangerous_roles:
            try:
                await member.remove_roles(role, reason=f"Emergency strip by {ctx.author}: {reason}")
                removed.append(role.name)
            except discord.Forbidden:
                pass

        await self.log_mod_action(
            ctx.guild.id, ctx.author, "emergency_role_strip",
            target_id=member.id, target_name=str(member), target_type="user",
            reason=reason, details=f"Removed: {', '.join(removed)}"
        )

        await ctx.respond(
            f"🚨 **Emergency Strip Complete**\n"
            f"Removed **{len(removed)}** elevated roles from {member.mention}:\n"
            f"{', '.join(removed)}",
            ephemeral=True
        )

    # React roles

    @roles.command(name="menu", description="View self-assignable roles")
    async def roles_menu(self, ctx: discord.ApplicationContext):
        """View available self-assignable roles."""
        with db_session_scope() as session:
            react_roles = (
                session.query(ReactRole)
                .filter(ReactRole.guild_id == ctx.guild.id)
                .all()
            )

            if not react_roles:
                await ctx.respond("No self-assignable roles configured.", ephemeral=True)
                return

            embed = discord.Embed(
                title="📋 Self-Assignable Roles",
                description="React to a message or use buttons to get roles!",
                color=discord.Color.blue()
            )

            for rr in react_roles:
                role = ctx.guild.get_role(rr.role_id)
                if role:
                    embed.add_field(
                        name=f"{rr.emoji} {role.name}",
                        value=f"React with {rr.emoji}",
                        inline=True
                    )

        await ctx.respond(embed=embed, ephemeral=True)

    @roles.command(name="add-react", description="Add a reaction role")
    @commands.has_permissions(manage_roles=True)
    @discord.option("message_id", str, description="Message ID")
    @discord.option("emoji", str, description="Emoji to react with")
    @discord.option("role", discord.Role, description="Role to assign")
    async def roles_add_react(
        self, ctx: discord.ApplicationContext,
        message_id: str, emoji: str, role: discord.Role
    ):
        """Add a reaction role to a message."""
        try:
            msg_id = int(message_id)
        except ValueError:
            await ctx.respond("Invalid message ID.", ephemeral=True)
            return

        message = None
        for channel in ctx.guild.text_channels:
            try:
                message = await channel.fetch_message(msg_id)
                break
            except (discord.NotFound, discord.Forbidden):
                continue

        if not message:
            await ctx.respond("Message not found.", ephemeral=True)
            return

        try:
            await message.add_reaction(emoji)
        except discord.HTTPException:
            await ctx.respond("Invalid emoji or cannot add reaction.", ephemeral=True)
            return

        with db_session_scope() as session:
            react_role = ReactRole(
                guild_id=ctx.guild.id,
                message_id=msg_id,
                channel_id=message.channel.id,
                emoji=emoji,
                role_id=role.id,
                role_name=role.name,
            )
            session.add(react_role)

        await ctx.respond(
            f"✅ React role configured! Users who react with {emoji} will get {role.mention}",
            ephemeral=True
        )

    # React role listeners

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        """Handle reaction role assignment."""
        if payload.member and payload.member.bot:
            return

        with db_session_scope() as session:
            react_role = (
                session.query(ReactRole)
                .filter(
                    ReactRole.guild_id == payload.guild_id,
                    ReactRole.message_id == payload.message_id,
                    ReactRole.emoji == str(payload.emoji)
                )
                .first()
            )

            if not react_role:
                return

            role_id = react_role.role_id

        guild = self.bot.get_guild(payload.guild_id)
        if not guild:
            return

        role = guild.get_role(role_id)
        member = payload.member or guild.get_member(payload.user_id)

        if role and member and role not in member.roles:
            try:
                await member.add_roles(role, reason="React role")
            except discord.Forbidden:
                pass

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent):
        """Handle reaction role removal."""
        with db_session_scope() as session:
            react_role = (
                session.query(ReactRole)
                .filter(
                    ReactRole.guild_id == payload.guild_id,
                    ReactRole.message_id == payload.message_id,
                    ReactRole.emoji == str(payload.emoji)
                )
                .first()
            )

            if not react_role or not react_role.remove_on_unreact:
                return

            role_id = react_role.role_id

        guild = self.bot.get_guild(payload.guild_id)
        if not guild:
            return

        role = guild.get_role(role_id)
        member = guild.get_member(payload.user_id)

        if role and member and role in member.roles:
            try:
                await member.remove_roles(role, reason="React role removed")
            except discord.Forbidden:
                pass


def setup(bot: commands.Bot):
    bot.add_cog(RolesCog(bot))
