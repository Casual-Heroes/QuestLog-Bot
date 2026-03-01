# cogs/channels.py - Channel Management
"""
Channel management for QuestLog.

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

from config import db_session_scope, logger, get_debug_guilds
from models import Guild, ChannelTemplate, ModAction, RoleTemplate


class ChannelsCog(commands.Cog):
    """Channel management system."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # Batching for mass channel creation (templates, server setup)
        # {guild_id: {'channels': [], 'task': asyncio.Task, 'first_time': float}}
        self._pending_channel_notifications: dict = {}

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
    @discord.default_permissions(administrator=True)
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

    # New channel announcement listener with batching support

    async def _send_channel_notification(self, guild_id: int):
        """Send batched channel creation notification after delay."""
        await asyncio.sleep(3)  # Wait 3 seconds for more channels

        pending = self._pending_channel_notifications.pop(guild_id, None)
        if not pending or not pending['channels']:
            return

        channels = pending['channels']
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return

        with db_session_scope() as session:
            db_guild = session.get(Guild, guild_id)
            if not db_guild:
                return

            # Get notification channel - prefer dedicated channel, fallback to log channel
            notify_channel_id = db_guild.channel_notify_channel_id or db_guild.log_channel_id
            if not notify_channel_id:
                return

            # Get temp voice categories to exclude
            temp_categories = set()
            if db_guild.temp_voice_category_ids:
                try:
                    # Convert to integers - website may store as strings
                    raw_ids = json.loads(db_guild.temp_voice_category_ids)
                    temp_categories = set(int(cat_id) for cat_id in raw_ids)
                except (json.JSONDecodeError, TypeError, ValueError):
                    pass

        notify_channel = guild.get_channel(notify_channel_id)
        if not notify_channel or not isinstance(notify_channel, discord.TextChannel):
            return

        # Filter out channels in temp voice categories
        filtered_channels = []
        for ch_data in channels:
            ch = guild.get_channel(ch_data['id'])
            if not ch:
                continue
            # Skip if in a temp voice category
            if ch.category and ch.category.id in temp_categories:
                continue
            filtered_channels.append(ch_data)

        if not filtered_channels:
            return

        # Build embed based on number of channels
        if len(filtered_channels) == 1:
            # Single channel - detailed embed
            ch_data = filtered_channels[0]
            ch = guild.get_channel(ch_data['id'])
            if not ch:
                return

            type_emoji = {
                'text': '💬',
                'voice': '🔊',
                'forum': '📋',
                'stage': '🎭',
                'category': '📁',
                'news': '📢',
            }.get(ch_data['type'], '❓')

            embed = discord.Embed(
                title=f"{type_emoji} New Channel Created",
                description=f"A new {ch_data['type']} channel has been created!",
                color=discord.Color.green(),
                timestamp=discord.utils.utcnow()
            )

            # Channel mention (categories can't be mentioned)
            if isinstance(ch, discord.CategoryChannel):
                embed.add_field(name="Category", value=f"📁 **{ch.name}**", inline=True)
            else:
                embed.add_field(name="Channel", value=ch.mention, inline=True)

            if ch.category:
                embed.add_field(name="In Category", value=ch.category.name, inline=True)

            if ch_data.get('creator'):
                embed.add_field(name="Created By", value=ch_data['creator'], inline=True)

            if isinstance(ch, discord.TextChannel) and ch.topic:
                embed.add_field(name="Topic", value=ch.topic[:100], inline=False)

            embed.set_footer(text="Check it out!")

        else:
            # Multiple channels - grouped by category
            embed = discord.Embed(
                title=f"🆕 {len(filtered_channels)} New Channels Created",
                description="Multiple channels were created at once!",
                color=discord.Color.green(),
                timestamp=discord.utils.utcnow()
            )

            # Group by category
            by_category: dict = {}  # {category_name: [channels]}
            for ch_data in filtered_channels:
                ch = guild.get_channel(ch_data['id'])
                if not ch:
                    continue

                cat_name = ch.category.name if ch.category else "No Category"
                if cat_name not in by_category:
                    by_category[cat_name] = []

                type_emoji = {
                    'text': '💬',
                    'voice': '🔊',
                    'forum': '📋',
                    'stage': '🎭',
                    'category': '📁',
                    'news': '📢',
                }.get(ch_data['type'], '❓')

                # Format channel line
                if isinstance(ch, discord.CategoryChannel):
                    by_category[cat_name].append(f"{type_emoji} **{ch.name}** (category)")
                else:
                    by_category[cat_name].append(f"{type_emoji} {ch.mention}")

            # Add fields for each category (max 25 fields)
            for cat_name, channel_list in list(by_category.items())[:10]:
                # Truncate if too many channels
                display_list = channel_list[:15]
                if len(channel_list) > 15:
                    display_list.append(f"*...and {len(channel_list) - 15} more*")

                embed.add_field(
                    name=f"📁 {cat_name}",
                    value="\n".join(display_list) or "None",
                    inline=False
                )

            # Show who created them if it was one person
            creators = set(ch_data.get('creator') for ch_data in filtered_channels if ch_data.get('creator'))
            if len(creators) == 1:
                embed.set_footer(text=f"Created by {list(creators)[0]}")
            else:
                embed.set_footer(text="Mass channel creation detected")

        try:
            await notify_channel.send(embed=embed)
        except discord.Forbidden:
            logger.warning(f"Cannot send channel notification to {notify_channel_id} in {guild.name}")

    @commands.Cog.listener()
    async def on_guild_channel_create(self, channel: discord.abc.GuildChannel):
        """Announce new channel creation with batching for mass creation."""
        guild = channel.guild
        guild_id = guild.id

        # Determine channel type
        if isinstance(channel, discord.TextChannel):
            if channel.is_news():
                ch_type = 'news'
            else:
                ch_type = 'text'
        elif isinstance(channel, discord.VoiceChannel):
            ch_type = 'voice'
        elif isinstance(channel, discord.ForumChannel):
            ch_type = 'forum'
        elif isinstance(channel, discord.StageChannel):
            ch_type = 'stage'
        elif isinstance(channel, discord.CategoryChannel):
            ch_type = 'category'
        else:
            return  # Unknown type

        # Try to get creator from audit log
        creator = None
        try:
            await asyncio.sleep(0.5)  # Small delay for audit log to populate
            async for entry in guild.audit_logs(limit=5, action=discord.AuditLogAction.channel_create):
                if entry.target and entry.target.id == channel.id:
                    creator = str(entry.user)
                    break
        except (discord.Forbidden, discord.HTTPException):
            pass

        ch_data = {
            'id': channel.id,
            'name': channel.name,
            'type': ch_type,
            'creator': creator,
            'category_id': channel.category.id if channel.category else None,
        }

        # Add to pending batch
        if guild_id not in self._pending_channel_notifications:
            self._pending_channel_notifications[guild_id] = {
                'channels': [],
                'task': None,
                'first_time': time.time(),
            }

        self._pending_channel_notifications[guild_id]['channels'].append(ch_data)

        # Cancel existing task and start new one (extends the wait window)
        existing_task = self._pending_channel_notifications[guild_id].get('task')
        if existing_task and not existing_task.done():
            existing_task.cancel()

        # Start new delayed task
        self._pending_channel_notifications[guild_id]['task'] = asyncio.create_task(
            self._send_channel_notification(guild_id)
        )

    # Channel notification configuration

    @channels.command(name="notify-config", description="Configure new channel notifications")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    @discord.option("notify_channel", discord.TextChannel, description="Channel to send notifications to", required=False)
    @discord.option("disable", bool, description="Disable channel notifications entirely", required=False)
    async def notify_config(
        self, ctx: discord.ApplicationContext,
        notify_channel: discord.TextChannel = None,
        disable: bool = False
    ):
        """Configure where new channel creation notifications are sent."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)
            if not db_guild:
                await ctx.respond("Guild not found in database.", ephemeral=True)
                return

            if disable:
                db_guild.channel_notify_channel_id = None
                session.commit()
                await ctx.respond("✅ Channel creation notifications disabled.", ephemeral=True)
                return

            if notify_channel:
                db_guild.channel_notify_channel_id = notify_channel.id
                session.commit()
                await ctx.respond(
                    f"✅ Channel creation notifications will be sent to {notify_channel.mention}\n\n"
                    f"*Note: This is separate from audit logs. Use `/channels notify-exclude` to ignore temp voice categories.*",
                    ephemeral=True
                )
                return

            # Show current config
            current_channel = ctx.guild.get_channel(db_guild.channel_notify_channel_id) if db_guild.channel_notify_channel_id else None
            fallback_channel = ctx.guild.get_channel(db_guild.log_channel_id) if db_guild.log_channel_id else None

            # Get excluded categories - convert to integers (website may store as strings)
            excluded_cats = []
            if db_guild.temp_voice_category_ids:
                try:
                    cat_ids = json.loads(db_guild.temp_voice_category_ids)
                    for cat_id in cat_ids:
                        cat = ctx.guild.get_channel(int(cat_id))
                        if cat:
                            excluded_cats.append(cat.name)
                except (json.JSONDecodeError, TypeError, ValueError):
                    pass

            embed = discord.Embed(
                title="🔔 Channel Notification Settings",
                color=discord.Color.blue()
            )

            if current_channel:
                embed.add_field(name="Notification Channel", value=current_channel.mention, inline=True)
            elif fallback_channel:
                embed.add_field(name="Notification Channel", value=f"{fallback_channel.mention} (using log channel)", inline=True)
            else:
                embed.add_field(name="Notification Channel", value="Not configured", inline=True)

            embed.add_field(
                name="Excluded Categories",
                value="\n".join(f"• {cat}" for cat in excluded_cats) if excluded_cats else "None (all channels notified)",
                inline=False
            )

            embed.set_footer(text="Use /channels notify-config #channel to set • /channels notify-exclude to ignore categories")

            await ctx.respond(embed=embed, ephemeral=True)

    @channels.command(name="notify-exclude", description="Exclude categories from notifications (e.g., temp voice)")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    @discord.option("category", discord.CategoryChannel, description="Category to exclude (e.g., Mee6 temp voice)", required=False)
    @discord.option("remove", discord.CategoryChannel, description="Remove a category from exclusion list", required=False)
    @discord.option("clear_all", bool, description="Clear all excluded categories", required=False)
    async def notify_exclude(
        self, ctx: discord.ApplicationContext,
        category: discord.CategoryChannel = None,
        remove: discord.CategoryChannel = None,
        clear_all: bool = False
    ):
        """Exclude categories from channel creation notifications (like Mee6 temp voice channels)."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)
            if not db_guild:
                await ctx.respond("Guild not found in database.", ephemeral=True)
                return

            # Load current exclusions - convert to integers (website may store as strings)
            excluded_ids = set()
            if db_guild.temp_voice_category_ids:
                try:
                    raw_ids = json.loads(db_guild.temp_voice_category_ids)
                    excluded_ids = set(int(cat_id) for cat_id in raw_ids)
                except (json.JSONDecodeError, TypeError, ValueError):
                    pass

            if clear_all:
                db_guild.temp_voice_category_ids = None
                session.commit()
                await ctx.respond("✅ Cleared all excluded categories. All channel creations will be notified.", ephemeral=True)
                return

            if remove:
                excluded_ids.discard(remove.id)
                db_guild.temp_voice_category_ids = json.dumps(list(excluded_ids)) if excluded_ids else None
                session.commit()
                await ctx.respond(f"✅ Removed **{remove.name}** from exclusion list.", ephemeral=True)
                return

            if category:
                excluded_ids.add(category.id)
                db_guild.temp_voice_category_ids = json.dumps(list(excluded_ids))
                session.commit()
                await ctx.respond(
                    f"✅ Added **{category.name}** to exclusion list.\n"
                    f"Channels created in this category will NOT trigger notifications.",
                    ephemeral=True
                )
                return

            # Show current exclusions
            excluded_cats = []
            for cat_id in excluded_ids:
                cat = ctx.guild.get_channel(cat_id)
                if cat:
                    excluded_cats.append(f"• {cat.name} ({len(cat.channels)} channels)")
                else:
                    excluded_cats.append(f"• Unknown category ({cat_id})")

            embed = discord.Embed(
                title="🔇 Excluded Categories",
                description="Channels created in these categories won't trigger notifications.\n"
                            "Useful for temp voice channels (Mee6, etc.)",
                color=discord.Color.orange()
            )

            embed.add_field(
                name="Currently Excluded",
                value="\n".join(excluded_cats) if excluded_cats else "None - all channels are notified",
                inline=False
            )

            embed.set_footer(text="Use /channels notify-exclude category:#category to add")

            await ctx.respond(embed=embed, ephemeral=True)

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
