# cogs/channels.py - Channel Management
"""
Channel management for Warden bot.

Features:
- Channel/Category templates
- Mass channel operations
- New channel announcements
- Channel permission audit
- Bulk slowmode
- Archive channels
"""

import time
import json
import asyncio
import discord
from discord.ext import commands

from config import db_session_scope, logger, get_debug_guilds, FeatureLimits
from models import Guild, ChannelTemplate, ModAction, RoleTemplate


def get_guild_tier(session, guild_id: int) -> str:
    """Get the subscription tier for a guild."""
    db_guild = session.get(Guild, guild_id)
    if not db_guild:
        return "FREE"
    if db_guild.is_vip:
        return "PRO"
    return db_guild.subscription_tier.upper() if db_guild.subscription_tier else "FREE"


async def check_template_limit(ctx, session) -> tuple[bool, int | None]:
    """Check if guild can create more templates."""
    tier = get_guild_tier(session, ctx.guild.id)
    limit = FeatureLimits.get_limit(tier, "templates")

    if limit is None:
        return (True, None)

    # Count total templates (role + channel combined)
    role_count = session.query(RoleTemplate).filter(RoleTemplate.guild_id == ctx.guild.id).count()
    channel_count = session.query(ChannelTemplate).filter(ChannelTemplate.guild_id == ctx.guild.id).count()
    total = role_count + channel_count

    if total >= limit:
        upgrade_msg = FeatureLimits.get_upgrade_message("templates", tier)
        await ctx.respond(
            f"⚠️ **Template Limit Reached!** You have **{total}/{limit}** templates on the {tier} tier.\n\n"
            f"⭐ {upgrade_msg}",
            ephemeral=True
        )
        return (False, limit)

    return (True, limit)


class ChannelsCog(commands.Cog):
    """Channel management system."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    channels = discord.SlashCommandGroup(
        name="channels",
        description="Channel management commands",
        
    )

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

    # Channel templates

    @channels.command(name="save-template", description="Save a channel as a template")
    @commands.has_permissions(manage_channels=True)
    @discord.option("channel", discord.abc.GuildChannel, description="Channel to save as template")
    @discord.option("template_name", str, description="Name for the template")
    @discord.option("description", str, description="Description", required=False)
    async def save_template(
        self, ctx: discord.ApplicationContext,
        channel: discord.abc.GuildChannel, template_name: str, description: str = None
    ):
        """Save a channel configuration as a template."""
        if isinstance(channel, discord.TextChannel):
            channel_type = "text"
            settings = {
                "topic": channel.topic,
                "nsfw": channel.nsfw,
                "slowmode_delay": channel.slowmode_delay,
                "default_auto_archive_duration": channel.default_auto_archive_duration,
            }
        elif isinstance(channel, discord.VoiceChannel):
            channel_type = "voice"
            settings = {
                "bitrate": channel.bitrate,
                "user_limit": channel.user_limit,
            }
        elif isinstance(channel, discord.CategoryChannel):
            channel_type = "category"
            settings = {}
        else:
            await ctx.respond("Unsupported channel type.", ephemeral=True)
            return

        permissions = {}
        for target, overwrite in channel.overwrites.items():
            target_type = "role" if isinstance(target, discord.Role) else "member"
            allow, deny = overwrite.pair()
            permissions[f"{target_type}:{target.id}"] = {
                "allow": allow.value,
                "deny": deny.value,
                "name": target.name if hasattr(target, 'name') else str(target),
            }

        with db_session_scope() as session:
            existing = (
                session.query(ChannelTemplate)
                .filter(ChannelTemplate.guild_id == ctx.guild.id, ChannelTemplate.name == template_name)
                .first()
            )

            if existing:
                await ctx.respond(f"Template '{template_name}' already exists.", ephemeral=True)
                return

            # Check template limit
            allowed, limit = await check_template_limit(ctx, session)
            if not allowed:
                return

            template = ChannelTemplate(
                guild_id=ctx.guild.id,
                name=template_name,
                description=description or f"Template from {channel.name}",
                template_type=channel_type,
                settings_json=json.dumps(settings),
                permissions_json=json.dumps(permissions),
                created_by=ctx.author.id,
            )
            session.add(template)

        await ctx.respond(
            f"✅ Saved **{channel.name}** as template **{template_name}** ({channel_type})",
            ephemeral=True
        )

    @channels.command(name="create-from-template", description="Create a channel from a template")
    @commands.has_permissions(manage_channels=True)
    @discord.option("template_name", str, description="Template name")
    @discord.option("channel_name", str, description="Name for the new channel")
    @discord.option("category", discord.CategoryChannel, description="Category to create in", required=False)
    async def create_from_template(
        self, ctx: discord.ApplicationContext,
        template_name: str, channel_name: str, category: discord.CategoryChannel = None
    ):
        """Create a new channel from a template."""
        with db_session_scope() as session:
            template = (
                session.query(ChannelTemplate)
                .filter(ChannelTemplate.guild_id == ctx.guild.id, ChannelTemplate.name == template_name)
                .first()
            )

            if not template:
                await ctx.respond(f"Template '{template_name}' not found.", ephemeral=True)
                return

            channel_type = template.template_type
            settings = json.loads(template.settings_json) if template.settings_json else {}
            permissions_data = json.loads(template.permissions_json) if template.permissions_json else {}
            template.use_count += 1

        overwrites = {}
        for key, perm_data in permissions_data.items():
            target_type, target_id = key.split(":")
            target_id = int(target_id)

            if target_type == "role":
                target = ctx.guild.get_role(target_id)
            else:
                target = ctx.guild.get_member(target_id)

            if target:
                overwrites[target] = discord.PermissionOverwrite.from_pair(
                    discord.Permissions(perm_data["allow"]),
                    discord.Permissions(perm_data["deny"])
                )

        try:
            if channel_type == "text":
                new_channel = await ctx.guild.create_text_channel(
                    name=channel_name,
                    category=category,
                    topic=settings.get("topic"),
                    nsfw=settings.get("nsfw", False),
                    slowmode_delay=settings.get("slowmode_delay", 0),
                    overwrites=overwrites,
                    reason=f"Created from template '{template_name}' by {ctx.author}"
                )
            elif channel_type == "voice":
                new_channel = await ctx.guild.create_voice_channel(
                    name=channel_name,
                    category=category,
                    bitrate=settings.get("bitrate", 64000),
                    user_limit=settings.get("user_limit", 0),
                    overwrites=overwrites,
                    reason=f"Created from template '{template_name}' by {ctx.author}"
                )
            elif channel_type == "category":
                new_channel = await ctx.guild.create_category(
                    name=channel_name,
                    overwrites=overwrites,
                    reason=f"Created from template '{template_name}' by {ctx.author}"
                )
            else:
                await ctx.respond("Unknown channel type.", ephemeral=True)
                return

            await self.log_mod_action(
                ctx.guild.id, ctx.author, "channel_create_from_template",
                target_id=new_channel.id, target_name=channel_name, target_type="channel",
                details=f"From template: {template_name}"
            )

            await ctx.respond(
                f"✅ Created {new_channel.mention} from template **{template_name}**",
                ephemeral=True
            )

        except discord.Forbidden:
            await ctx.respond("I don't have permission to create channels.", ephemeral=True)

    @channels.command(name="list-templates", description="List available channel templates")
    @commands.has_permissions(manage_channels=True)
    async def list_templates(self, ctx: discord.ApplicationContext):
        """List all channel templates."""
        with db_session_scope() as session:
            templates = (
                session.query(ChannelTemplate)
                .filter(ChannelTemplate.guild_id == ctx.guild.id)
                .order_by(ChannelTemplate.name)
                .all()
            )

            if not templates:
                await ctx.respond("No channel templates saved yet.", ephemeral=True)
                return

            embed = discord.Embed(title="📋 Channel Templates", color=discord.Color.blue())

            for t in templates:
                type_emoji = {"text": "💬", "voice": "🔊", "category": "📁"}.get(t.template_type, "❓")
                embed.add_field(
                    name=f"{type_emoji} {t.name}",
                    value=f"{t.description or 'No description'}\nType: {t.template_type}\nUsed: {t.use_count}x",
                    inline=True
                )

        await ctx.respond(embed=embed, ephemeral=True)

    # Mass channel operations

    @channels.command(name="mass-delete", description="Mass delete channels (DANGEROUS)")
    @commands.has_permissions(administrator=True)
    @discord.option("category", discord.CategoryChannel, description="Delete all channels in this category")
    @discord.option("confirm", str, description="Type 'CONFIRM' to proceed")
    async def mass_delete(
        self, ctx: discord.ApplicationContext,
        category: discord.CategoryChannel, confirm: str
    ):
        """Mass delete channels in a category."""
        if confirm != "CONFIRM":
            await ctx.respond(
                "⚠️ This will delete ALL channels in the category!\n"
                "To confirm, use `/channels mass-delete` with confirm: CONFIRM",
                ephemeral=True
            )
            return

        await ctx.defer(ephemeral=True)

        channels_to_delete = list(category.channels)
        deleted = []
        failed = []

        for channel in channels_to_delete:
            try:
                await channel.delete(reason=f"Mass delete by {ctx.author}")
                deleted.append(channel.name)
                await asyncio.sleep(0.5)
            except discord.Forbidden:
                failed.append(channel.name)

        try:
            await category.delete(reason=f"Mass delete by {ctx.author}")
            deleted.append(f"Category: {category.name}")
        except discord.Forbidden:
            failed.append(f"Category: {category.name}")

        await self.log_mod_action(
            ctx.guild.id, ctx.author, "mass_channel_delete",
            target_id=category.id, target_name=category.name, target_type="category",
            details=f"Deleted: {len(deleted)}, Failed: {len(failed)}"
        )

        await ctx.respond(
            f"🗑️ **Mass Delete Complete**\n"
            f"Deleted: {len(deleted)} channels\n"
            f"Failed: {len(failed)}" + (f"\nFailed: {', '.join(failed)}" if failed else ""),
            ephemeral=True
        )

    @channels.command(name="bulk-slowmode", description="Apply slowmode to multiple channels")
    @commands.has_permissions(manage_channels=True)
    @discord.option("category", discord.CategoryChannel, description="Apply to all text channels in category")
    @discord.option("seconds", int, description="Slowmode delay in seconds (0 to disable)")
    async def bulk_slowmode(
        self, ctx: discord.ApplicationContext,
        category: discord.CategoryChannel, seconds: int
    ):
        """Apply slowmode to all text channels in a category."""
        if seconds < 0 or seconds > 21600:
            await ctx.respond("Slowmode must be between 0 and 21600 seconds (6 hours).", ephemeral=True)
            return

        await ctx.defer(ephemeral=True)

        text_channels = [c for c in category.channels if isinstance(c, discord.TextChannel)]
        success = 0
        failed = 0

        for channel in text_channels:
            try:
                await channel.edit(slowmode_delay=seconds, reason=f"Bulk slowmode by {ctx.author}")
                success += 1
                await asyncio.sleep(0.3)
            except discord.Forbidden:
                failed += 1

        await self.log_mod_action(
            ctx.guild.id, ctx.author, "bulk_slowmode",
            target_id=category.id, target_name=category.name, target_type="category",
            details=f"Slowmode: {seconds}s, Channels: {success}"
        )

        await ctx.respond(
            f"✅ Set slowmode to **{seconds}s** on **{success}** channels" +
            (f" ({failed} failed)" if failed else ""),
            ephemeral=True
        )

    @channels.command(name="archive", description="Archive a channel (move to archive category)")
    @commands.has_permissions(manage_channels=True)
    @discord.option("channel", discord.TextChannel, description="Channel to archive")
    @discord.option("archive_category", discord.CategoryChannel, description="Archive category to move to")
    @discord.option("lock", bool, description="Lock the channel from members", default=True)
    async def archive_channel(
        self, ctx: discord.ApplicationContext,
        channel: discord.TextChannel, archive_category: discord.CategoryChannel, lock: bool = True
    ):
        """Archive a channel by moving it and optionally locking it."""
        try:
            await channel.edit(category=archive_category, reason=f"Archived by {ctx.author}")

            if lock:
                await channel.set_permissions(
                    ctx.guild.default_role,
                    send_messages=False,
                    add_reactions=False,
                    reason=f"Archived and locked by {ctx.author}"
                )

            embed = discord.Embed(
                title="📁 Channel Archived",
                description=f"This channel was archived by {ctx.author.mention}",
                color=discord.Color.greyple(),
                timestamp=discord.utils.utcnow()
            )
            await channel.send(embed=embed)

            await self.log_mod_action(
                ctx.guild.id, ctx.author, "channel_archive",
                target_id=channel.id, target_name=channel.name, target_type="channel",
                details=f"Moved to {archive_category.name}, Locked: {lock}"
            )

            await ctx.respond(
                f"✅ Archived {channel.mention} to **{archive_category.name}**" +
                (" (locked)" if lock else ""),
                ephemeral=True
            )

        except discord.Forbidden:
            await ctx.respond("I don't have permission to do that.", ephemeral=True)

    @channels.command(name="clone", description="Clone a channel with all permissions")
    @commands.has_permissions(manage_channels=True)
    @discord.option("channel", discord.abc.GuildChannel, description="Channel to clone")
    @discord.option("new_name", str, description="Name for the cloned channel", required=False)
    async def clone_channel(
        self, ctx: discord.ApplicationContext,
        channel: discord.abc.GuildChannel, new_name: str = None
    ):
        """Clone a channel with all its settings and permissions."""
        try:
            cloned = await channel.clone(
                name=new_name or f"{channel.name}-copy",
                reason=f"Cloned by {ctx.author}"
            )

            await self.log_mod_action(
                ctx.guild.id, ctx.author, "channel_clone",
                target_id=cloned.id, target_name=cloned.name, target_type="channel",
                details=f"Cloned from: {channel.name}"
            )

            await ctx.respond(f"✅ Cloned {channel.mention} → {cloned.mention}", ephemeral=True)

        except discord.Forbidden:
            await ctx.respond("I don't have permission to clone channels.", ephemeral=True)

    # Channel permission audit

    @channels.command(name="audit-access", description="Audit who can access a channel")
    @commands.has_permissions(manage_channels=True)
    @discord.option("channel", discord.abc.GuildChannel, description="Channel to audit")
    async def audit_access(self, ctx: discord.ApplicationContext, channel: discord.abc.GuildChannel):
        """Audit who has access to a channel."""
        embed = discord.Embed(
            title=f"🔍 Access Audit: #{channel.name}",
            color=discord.Color.blue()
        )

        role_overwrites = []
        member_overwrites = []

        for target, overwrite in channel.overwrites.items():
            allow, deny = overwrite.pair()

            if isinstance(target, discord.Role):
                perms = []
                if allow.view_channel:
                    perms.append("✅ view")
                if deny.view_channel:
                    perms.append("❌ view")
                if allow.send_messages:
                    perms.append("✅ send")
                if deny.send_messages:
                    perms.append("❌ send")
                if allow.manage_channels:
                    perms.append("✅ manage")

                if perms:
                    role_overwrites.append(f"**{target.name}**: {', '.join(perms)}")
            else:
                member_overwrites.append(f"{target.mention}: custom perms")

        if role_overwrites:
            embed.add_field(
                name="Role Overwrites",
                value="\n".join(role_overwrites[:15]) or "None",
                inline=False
            )

        if member_overwrites:
            embed.add_field(
                name="Member Overwrites",
                value="\n".join(member_overwrites[:10]) or "None",
                inline=False
            )

        can_view = 0
        cannot_view = 0
        for member in ctx.guild.members:
            perms = channel.permissions_for(member)
            if perms.view_channel:
                can_view += 1
            else:
                cannot_view += 1

        embed.add_field(
            name="Access Summary",
            value=f"✅ Can view: {can_view}\n❌ Cannot view: {cannot_view}",
            inline=False
        )

        await ctx.respond(embed=embed, ephemeral=True)

    # New channel announcement listener

    @commands.Cog.listener()
    async def on_guild_channel_create(self, channel: discord.abc.GuildChannel):
        """Announce new channel creation."""
        if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
            return

        guild_id = channel.guild.id

        with db_session_scope() as session:
            db_guild = session.get(Guild, guild_id)
            if not db_guild:
                return
            announce_channel_id = db_guild.welcome_channel_id

        if not announce_channel_id:
            return

        announce_channel = channel.guild.get_channel(announce_channel_id)
        if not announce_channel or not isinstance(announce_channel, discord.TextChannel):
            return

        channel_type = "text channel" if isinstance(channel, discord.TextChannel) else "voice channel"
        category_text = f" in **{channel.category.name}**" if channel.category else ""

        embed = discord.Embed(
            title="🆕 New Channel Created",
            description=f"A new {channel_type} has been created{category_text}!",
            color=discord.Color.green()
        )

        embed.add_field(name="Channel", value=channel.mention, inline=True)

        if isinstance(channel, discord.TextChannel) and channel.topic:
            embed.add_field(name="Topic", value=channel.topic[:100], inline=False)

        embed.set_footer(text="Check it out!")

        try:
            await announce_channel.send(embed=embed)
        except discord.Forbidden:
            pass

    # Lock/unlock commands

    @channels.command(name="lock", description="Lock a channel from members")
    @commands.has_permissions(manage_channels=True)
    @discord.option("channel", discord.TextChannel, description="Channel to lock", required=False)
    @discord.option("reason", str, description="Reason for locking", required=False)
    async def lock_channel(
        self, ctx: discord.ApplicationContext,
        channel: discord.TextChannel = None, reason: str = None
    ):
        """Lock a channel, preventing members from sending messages."""
        target = channel or ctx.channel

        try:
            await target.set_permissions(
                ctx.guild.default_role,
                send_messages=False,
                add_reactions=False,
                reason=f"Locked by {ctx.author}: {reason or 'No reason'}"
            )

            embed = discord.Embed(
                title="🔒 Channel Locked",
                description=reason or "This channel has been locked.",
                color=discord.Color.red()
            )
            embed.set_footer(text=f"Locked by {ctx.author}")

            await target.send(embed=embed)

            await self.log_mod_action(
                ctx.guild.id, ctx.author, "channel_lock",
                target_id=target.id, target_name=target.name, target_type="channel",
                reason=reason
            )

            await ctx.respond(f"🔒 Locked {target.mention}", ephemeral=True)

        except discord.Forbidden:
            await ctx.respond("I don't have permission to lock that channel.", ephemeral=True)

    @channels.command(name="unlock", description="Unlock a channel")
    @commands.has_permissions(manage_channels=True)
    @discord.option("channel", discord.TextChannel, description="Channel to unlock", required=False)
    async def unlock_channel(
        self, ctx: discord.ApplicationContext,
        channel: discord.TextChannel = None
    ):
        """Unlock a channel, allowing members to send messages again."""
        target = channel or ctx.channel

        try:
            await target.set_permissions(
                ctx.guild.default_role,
                send_messages=None,
                add_reactions=None,
                reason=f"Unlocked by {ctx.author}"
            )

            embed = discord.Embed(
                title="🔓 Channel Unlocked",
                description="This channel has been unlocked.",
                color=discord.Color.green()
            )
            embed.set_footer(text=f"Unlocked by {ctx.author}")

            await target.send(embed=embed)

            await self.log_mod_action(
                ctx.guild.id, ctx.author, "channel_unlock",
                target_id=target.id, target_name=target.name, target_type="channel"
            )

            await ctx.respond(f"🔓 Unlocked {target.mention}", ephemeral=True)

        except discord.Forbidden:
            await ctx.respond("I don't have permission to unlock that channel.", ephemeral=True)

    @channels.command(name="lock-category", description="Lock all channels in a category")
    @commands.has_permissions(manage_channels=True)
    @discord.option("category", discord.CategoryChannel, description="Category to lock")
    @discord.option("reason", str, description="Reason for locking", required=False)
    async def lock_category(
        self, ctx: discord.ApplicationContext,
        category: discord.CategoryChannel, reason: str = None
    ):
        """Lock all channels in a category."""
        await ctx.defer(ephemeral=True)

        success = 0
        failed = 0

        for channel in category.channels:
            if isinstance(channel, discord.TextChannel):
                try:
                    await channel.set_permissions(
                        ctx.guild.default_role,
                        send_messages=False,
                        add_reactions=False,
                        reason=f"Category locked by {ctx.author}"
                    )
                    success += 1
                    await asyncio.sleep(0.3)
                except discord.Forbidden:
                    failed += 1

        await self.log_mod_action(
            ctx.guild.id, ctx.author, "category_lock",
            target_id=category.id, target_name=category.name, target_type="category",
            reason=reason,
            details=f"Locked {success} channels"
        )

        await ctx.respond(
            f"🔒 Locked **{success}** channels in **{category.name}**" +
            (f" ({failed} failed)" if failed else ""),
            ephemeral=True
        )

    @channels.command(name="unlock-category", description="Unlock all channels in a category")
    @commands.has_permissions(manage_channels=True)
    @discord.option("category", discord.CategoryChannel, description="Category to unlock")
    async def unlock_category(
        self, ctx: discord.ApplicationContext,
        category: discord.CategoryChannel
    ):
        """Unlock all channels in a category."""
        await ctx.defer(ephemeral=True)

        success = 0
        failed = 0

        for channel in category.channels:
            if isinstance(channel, discord.TextChannel):
                try:
                    await channel.set_permissions(
                        ctx.guild.default_role,
                        send_messages=None,
                        add_reactions=None,
                        reason=f"Category unlocked by {ctx.author}"
                    )
                    success += 1
                    await asyncio.sleep(0.3)
                except discord.Forbidden:
                    failed += 1

        await self.log_mod_action(
            ctx.guild.id, ctx.author, "category_unlock",
            target_id=category.id, target_name=category.name, target_type="category",
            details=f"Unlocked {success} channels"
        )

        await ctx.respond(
            f"🔓 Unlocked **{success}** channels in **{category.name}**" +
            (f" ({failed} failed)" if failed else ""),
            ephemeral=True
        )


def setup(bot: commands.Bot):
    bot.add_cog(ChannelsCog(bot))
