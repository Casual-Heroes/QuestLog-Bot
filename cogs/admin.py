# cogs/admin.py - Admin Commands & Settings
"""
Admin cog for QuestLog.
Server settings, feature toggles, and administrative commands.
"""

import os
import time
import json
import asyncio
import re
import discord
from discord.ext import commands
from discord import SlashCommandGroup
from datetime import datetime, timedelta, timezone
from typing import Optional

from config import db_session_scope, logger, get_debug_guilds
from models import Guild, FeedbackConfig, Suggestion, SuggestionStatus, GuildMember, LFGGroup

# Bot owner ID - only this user can grant VIP status
# Set via environment variable or hardcode your Discord user ID
BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID", 0))


def replace_mentions(text: str, guild: discord.Guild) -> str:
    """Replace @role/@user/#channel tokens with proper mentions (case-insensitive)."""
    if not text:
        return text

    # Build quick lookup for members (username, display, global) lowercase
    member_map = {}
    for m in guild.members:
        for variant in {m.name, m.display_name, getattr(m, "global_name", None)}:
            if variant:
                member_map[variant.lower()] = m

    # Channels
    def ch_replace(match):
        name = match.group(1)
        ch = discord.utils.find(lambda c: c.name.lower() == name.lower(), guild.channels)
        return f"<#{ch.id}>" if ch else match.group(0)
    text = re.sub(r"#([A-Za-z0-9_\-\|]+)", ch_replace, text)

    # Roles/users
    def at_replace(match):
        name = match.group(1).strip()
        # Role
        role = discord.utils.find(lambda r: r.name.lower() == name.lower(), guild.roles)
        if role:
            return f"<@&{role.id}>"
        # Member by name/nick/global
        member = member_map.get(name.lower())
        if member:
            return f"<@{member.id}>"
        return match.group(0)

    text = re.sub(r"@([^\s]+)", at_replace, text)
    return text

# =============================================================================
# Embed helper modal
# =============================================================================

class SendEmbedModal(discord.ui.Modal):
    """Modal for composing and sending a custom embed."""
    def __init__(self, channel: Optional[discord.TextChannel], author: discord.Member):
        super().__init__(title="Send Custom Embed")
        self.target_channel = channel
        self.author = author

        self.add_item(discord.ui.InputText(
            label="Title",
            placeholder="Embed title",
            required=True,
            max_length=256
        ))
        self.add_item(discord.ui.InputText(
            label="Description",
            placeholder="Embed content (supports @role and #channel)",
            style=discord.InputTextStyle.long,
            required=True,
            max_length=4000
        ))
        self.add_item(discord.ui.InputText(
            label="Color (hex, optional)",
            placeholder="#5865F2",
            required=False,
            max_length=7
        ))
        self.add_item(discord.ui.InputText(
            label="Thumbnail URL (optional)",
            placeholder="https://example.com/thumb.png",
            required=False
        ))
        self.add_item(discord.ui.InputText(
            label="Footer (optional)",
            placeholder="Footer text",
            required=False,
            max_length=256
        ))

    async def callback(self, interaction: discord.Interaction):
        title = self.children[0].value
        description = self.children[1].value
        color_str = self.children[2].value
        thumb = self.children[3].value
        footer = self.children[4].value if len(self.children) > 4 else None

        # Replace simple mentions
        title = replace_mentions(title, interaction.guild)
        description = replace_mentions(description, interaction.guild)
        footer = replace_mentions(footer, interaction.guild) if footer else footer

        embed_color = discord.Color.blurple()
        if color_str:
            try:
                embed_color = discord.Color(int(color_str.replace("#", ""), 16))
            except Exception:
                await interaction.response.send_message("❌ Invalid hex color. Use #RRGGBB.", ephemeral=True)
                return

        embed = discord.Embed(
            title=title,
            description=description,
            color=embed_color,
            timestamp=datetime.now(timezone.utc)
        )
        if thumb:
            embed.set_thumbnail(url=thumb)
        if footer:
            embed.set_footer(text=footer)
        # Author field removed to stay within modal input limits

        channel = self.target_channel or interaction.channel
        try:
            msg = await channel.send(embed=embed)
        except discord.Forbidden:
            await interaction.response.send_message("❌ I don't have permission to send in that channel.", ephemeral=True)
            return

        await interaction.response.send_message(
            f"✅ Embed sent to {channel.mention}! Message ID: `{msg.id}`",
            ephemeral=True
        )

class SendMessageModal(discord.ui.Modal):
    """Modal for composing and sending a plain message."""
    def __init__(self, channel: Optional[discord.TextChannel], silent: bool = False):
        super().__init__(title="Send Message")
        self.target_channel = channel
        self.silent = silent

        self.add_item(discord.ui.InputText(
            label="Message",
            placeholder="Message body (supports @role, @user, #channel)",
            style=discord.InputTextStyle.long,
            required=True,
            max_length=2000
        ))

    async def callback(self, interaction: discord.Interaction):
        body = self.children[0].value
        body = replace_mentions(body, interaction.guild)

        channel = self.target_channel or interaction.channel
        # Safe default: allow user/role mentions but NOT @everyone/@here (prevent abuse)
        allowed = discord.AllowedMentions.none() if self.silent else discord.AllowedMentions(everyone=False, roles=True, users=True)
        try:
            msg = await channel.send(body, allowed_mentions=allowed)
        except discord.Forbidden:
            await interaction.response.send_message("❌ I don't have permission to send in that channel.", ephemeral=True)
            return
        await interaction.response.send_message(
            f"✅ Message sent to {channel.mention}! ID: `{msg.id}`",
            ephemeral=True
        )


class AdminCog(commands.Cog):
    """Admin and settings commands."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    message = SlashCommandGroup(
        name="message",
        description="Message System: send/edit messages and embeds",
    )

    settings = SlashCommandGroup(
        name="settings",
        description="Server settings (Admin)",
        
    )

    # Feature toggles
    @settings.command(name="toggle", description="Enable/disable QuestLog features")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    @discord.option(
        "feature", str, description="Feature to toggle",
        choices=["xp", "anti-raid", "verification", "audit-logging", "discovery"]
    )
    @discord.option("enabled", bool, description="Enable or disable the feature")
    async def settings_toggle(
        self,
        ctx: discord.ApplicationContext,
        feature: str,
        enabled: bool
    ):
        """Toggle features on/off."""
        with db_session_scope() as session:
            guild = session.get(Guild, ctx.guild.id)

            if not guild:
                await ctx.respond("❌ Guild not found in database.", ephemeral=True)
                return

            feature_map = {
                "xp": "xp_enabled",
                "anti-raid": "anti_raid_enabled",
                "verification": "verification_enabled",
                "audit-logging": "audit_logging_enabled",
                "discovery": "discovery_enabled",
            }

            attr = feature_map.get(feature)
            if attr:
                setattr(guild, attr, enabled)
                status = "✅ Enabled" if enabled else "❌ Disabled"
                await ctx.respond(
                    f"{status} **{feature}** for this server.",
                    ephemeral=True
                )
            else:
                await ctx.respond("❌ Unknown feature.", ephemeral=True)

    # Channel settings
    @settings.command(name="channel", description="Set a channel for QuestLog features")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    @discord.option(
        "channel_type", str, description="Type of channel to set",
        choices=["logs", "mod-log", "levelup", "verification", "self-promo", "welcome", "jail"]
    )
    @discord.option("channel", discord.TextChannel, description="The channel to use")
    async def settings_channel(
        self,
        ctx: discord.ApplicationContext,
        channel_type: str,
        channel: discord.TextChannel
    ):
        """Set feature channels."""
        with db_session_scope() as session:
            guild = session.get(Guild, ctx.guild.id)

            if not guild:
                await ctx.respond("❌ Guild not found.", ephemeral=True)
                return

            channel_map = {
                "logs": "log_channel_id",
                "mod-log": "mod_log_channel_id",
                "levelup": "level_up_channel_id",
                "verification": "verification_channel_id",
                "self-promo": "self_promo_channel_id",
                "welcome": "welcome_channel_id",
                "jail": "jail_channel_id",
            }

            attr = channel_map.get(channel_type)
            if attr:
                setattr(guild, attr, channel.id)
                await ctx.respond(
                    f"✅ Set **{channel_type}** channel to {channel.mention}.",
                    ephemeral=True
                )
            else:
                await ctx.respond("❌ Unknown channel type.", ephemeral=True)

    # Role settings
    @settings.command(name="role", description="Set a role for QuestLog features")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    @discord.option(
        "role_type", str, description="Type of role to set",
        choices=["verified", "quarantine", "muted", "jail"]
    )
    @discord.option("role", discord.Role, description="The role to use")
    async def settings_role(
        self,
        ctx: discord.ApplicationContext,
        role_type: str,
        role: discord.Role
    ):
        """Set feature roles."""
        with db_session_scope() as session:
            guild = session.get(Guild, ctx.guild.id)

            if not guild:
                await ctx.respond("❌ Guild not found.", ephemeral=True)
                return

            role_map = {
                "verified": "verified_role_id",
                "quarantine": "quarantine_role_id",
                "muted": "muted_role_id",
                "jail": "jail_role_id",
            }

            attr = role_map.get(role_type)
            if attr:
                setattr(guild, attr, role.id)
                await ctx.respond(
                    f"✅ Set **{role_type}** role to {role.mention}.",
                    ephemeral=True
                )
            else:
                await ctx.respond("❌ Unknown role type.", ephemeral=True)

    # View settings
    @settings.command(name="view", description="View current QuestLog settings")
    @discord.default_permissions(manage_guild=True)
    @commands.has_permissions(manage_guild=True)
    async def settings_view(self, ctx: discord.ApplicationContext):
        """View all settings."""
        with db_session_scope() as session:
            guild = session.get(Guild, ctx.guild.id)

            if not guild:
                await ctx.respond("❌ Guild not found.", ephemeral=True)
                return

            embed = discord.Embed(
                title=f"⚙️ {ctx.guild.name} Settings",
                color=discord.Color.blurple()
            )

            # Features
            features = (
                f"XP System: {'✅' if guild.xp_enabled else '❌'}\n"
                f"Anti-Raid: {'✅' if guild.anti_raid_enabled else '❌'}\n"
                f"Verification: {'✅' if guild.verification_enabled else '❌'}\n"
                f"Audit Logging: {'✅' if guild.audit_logging_enabled else '❌'}\n"
                f"Discovery: {'✅' if guild.discovery_enabled else '❌'} (Premium)"
            )
            embed.add_field(name="🔧 Features", value=features, inline=True)

            # Channels
            def get_channel_mention(cid):
                return f"<#{cid}>" if cid else "Not set"

            channels = (
                f"Logs: {get_channel_mention(guild.log_channel_id)}\n"
                f"Mod Log: {get_channel_mention(guild.mod_log_channel_id)}\n"
                f"Level Up: {get_channel_mention(guild.level_up_channel_id)}\n"
                f"Verification: {get_channel_mention(guild.verification_channel_id)}\n"
                f"Self-Promo: {get_channel_mention(guild.self_promo_channel_id)}\n"
                f"Welcome: {get_channel_mention(guild.welcome_channel_id)}\n"
                f"Jail: {get_channel_mention(guild.jail_channel_id)}"
            )
            embed.add_field(name="📺 Channels", value=channels, inline=True)

            # Roles
            def get_role_mention(rid):
                return f"<@&{rid}>" if rid else "Not set"

            roles = (
                f"Verified: {get_role_mention(guild.verified_role_id)}\n"
                f"Quarantine: {get_role_mention(guild.quarantine_role_id)}\n"
                f"Muted: {get_role_mention(guild.muted_role_id)}\n"
                f"Jail: {get_role_mention(guild.jail_role_id)}"
            )
            embed.add_field(name="👥 Roles", value=roles, inline=True)

            if guild.is_vip:
                embed.add_field(name="⭐ VIP", value="VIP Server", inline=False)

        await ctx.respond(embed=embed, ephemeral=True)

    # =============================================================================
    # EMBED COMMANDS
    # =============================================================================

    @discord.slash_command(name="send_embed", description="Send a custom embed message")
    @discord.default_permissions(manage_messages=True)
    @commands.has_permissions(manage_messages=True)
    @discord.option("channel", discord.TextChannel, description="Channel to send embed to", required=False)
    async def send_embed(
        self,
        ctx: discord.ApplicationContext,
        channel: Optional[discord.TextChannel] = None
    ):
        """Open a modal to compose and send an embed."""
        modal = SendEmbedModal(channel=channel, author=ctx.author)
        await ctx.send_modal(modal)

    # New Message System commands
    @message.command(name="send", description="Send a message via modal")
    @discord.default_permissions(manage_messages=True)
    @commands.has_permissions(manage_messages=True)
    @discord.option("channel", discord.TextChannel, description="Channel to send to", required=False)
    @discord.option("silent", bool, description="Suppress mentions?", required=False, default=False)
    async def message_send(self, ctx: discord.ApplicationContext, channel: Optional[discord.TextChannel] = None, silent: bool = False):
        modal = SendMessageModal(channel=channel, silent=silent)
        await ctx.send_modal(modal)

    @message.command(name="send_embed", description="Send an embed via modal")
    @discord.default_permissions(manage_messages=True)
    @commands.has_permissions(manage_messages=True)
    @discord.option("channel", discord.TextChannel, description="Channel to send to", required=False)
    async def message_send_embed(self, ctx: discord.ApplicationContext, channel: Optional[discord.TextChannel] = None):
        modal = SendEmbedModal(channel=channel, author=ctx.author)
        await ctx.send_modal(modal)


    @discord.slash_command(name="edit_embed", description="Edit an existing embed message")
    @discord.default_permissions(manage_messages=True)
    @commands.has_permissions(manage_messages=True)
    @discord.option("channel", discord.TextChannel, description="Channel containing the message")
    @discord.option("message_id", str, description="ID of the message to edit")
    @discord.option("title", str, description="New title (leave blank to keep)", required=False)
    @discord.option("description", str, description="New description (leave blank to keep)", required=False)
    @discord.option("color", str, description="New hex color (e.g., #FF5733)", required=False)
    @discord.option("footer", str, description="New footer text", required=False)
    @discord.option("clear_image", bool, description="Remove the image?", required=False)
    async def edit_embed(
        self,
        ctx: discord.ApplicationContext,
        channel: discord.TextChannel,
        message_id: str,
        title: str = None,
        description: str = None,
        color: str = None,
        footer: str = None,
        clear_image: bool = False
    ):
        """Edit an existing embed message sent by the bot."""
        try:
            msg_id = int(message_id)
            message = await channel.fetch_message(msg_id)

            if message.author.id != self.bot.user.id:
                await ctx.respond("❌ I can only edit my own messages!", ephemeral=True)
                return

            if not message.embeds:
                await ctx.respond("❌ That message doesn't have an embed!", ephemeral=True)
                return

            # Get the existing embed
            old_embed = message.embeds[0]

            # Build new embed, keeping old values if not provided
            # Replace mention tokens in provided fields
            if title:
                title = replace_mentions(title, ctx.guild)
            if description:
                description = replace_mentions(description, ctx.guild)
            if footer:
                footer = replace_mentions(footer, ctx.guild)

            new_embed = discord.Embed(
                title=title if title else old_embed.title,
                description=description if description else old_embed.description,
                color=old_embed.color,
                timestamp=datetime.now(timezone.utc)
            )

            # Update color if provided
            if color:
                color = color.strip('#')
                try:
                    new_embed.color = discord.Color(int(color, 16))
                except ValueError:
                    await ctx.respond("❌ Invalid hex color.", ephemeral=True)
                    return

            # Preserve or update other fields
            if old_embed.thumbnail and not clear_image:
                new_embed.set_thumbnail(url=old_embed.thumbnail.url)
            if old_embed.image and not clear_image:
                new_embed.set_image(url=old_embed.image.url)
            if footer:
                new_embed.set_footer(text=footer)
            elif old_embed.footer:
                new_embed.set_footer(text=old_embed.footer.text)
            if old_embed.author:
                new_embed.set_author(name=old_embed.author.name)

            new_embed.timestamp = datetime.now(timezone.utc) 
            
            await message.edit(embed=new_embed)
            await ctx.respond(f"✅ Embed updated in {channel.mention}!", ephemeral=True)

        except discord.NotFound:
            await ctx.respond("❌ Message not found!", ephemeral=True)
        except ValueError:
            await ctx.respond("❌ Invalid message ID!", ephemeral=True)
        except Exception as e:
            logger.error(f"Edit embed error: {e}")
            await ctx.respond(f"❌ Error: {str(e)}", ephemeral=True)

    # =============================================================================
    # PRUNE COMMAND
    # =============================================================================

    @discord.slash_command(name="prune", description="Remove inactive members from the server")
    @discord.default_permissions(kick_members=True)
    @commands.has_permissions(kick_members=True)
    @discord.option("days", int, description="Inactive for X days (7-30)", min_value=7, max_value=30)
    @discord.option("include_roles", bool, description="Include members with roles? (default: No)", required=False)
    @discord.option("dry_run", bool, description="Preview only, don't actually kick (default: Yes)", required=False)
    async def prune_members(
        self,
        ctx: discord.ApplicationContext,
        days: int,
        include_roles: bool = False,
        dry_run: bool = True
    ):
        """Prune inactive members who haven't been active."""
        await ctx.defer(ephemeral=True)

        try:
            # Use Discord's built-in prune estimate
            prune_count = await ctx.guild.estimate_pruned_members(
                days=days,
                roles=ctx.guild.roles if include_roles else []
            )

            if dry_run:
                embed = discord.Embed(
                    title="🔍 Prune Preview (Dry Run)",
                    description=f"**{prune_count}** members would be pruned.",
                    color=discord.Color.yellow()
                )
                embed.add_field(name="Inactive Days", value=str(days), inline=True)
                embed.add_field(name="Include Roles", value="Yes" if include_roles else "No", inline=True)
                embed.add_field(
                    name="⚠️ To Execute",
                    value="Run the command again with `dry_run: False`",
                    inline=False
                )
                await ctx.respond(embed=embed, ephemeral=True)
            else:
                if prune_count == 0:
                    await ctx.respond("✅ No members to prune!", ephemeral=True)
                    return

                # Confirm action
                confirm_embed = discord.Embed(
                    title="⚠️ Confirm Prune",
                    description=f"This will **kick {prune_count} members**. This action cannot be undone!",
                    color=discord.Color.red()
                )

                class ConfirmView(discord.ui.View):
                    def __init__(self):
                        super().__init__(timeout=30)
                        self.confirmed = False

                    @discord.ui.button(label="Confirm Prune", style=discord.ButtonStyle.danger)
                    async def confirm(self, button, interaction):
                        if interaction.user.id != ctx.author.id:
                            await interaction.response.send_message("Only the command user can confirm.", ephemeral=True)
                            return
                        self.confirmed = True
                        self.stop()

                    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
                    async def cancel(self, button, interaction):
                        if interaction.user.id != ctx.author.id:
                            await interaction.response.send_message("Only the command user can cancel.", ephemeral=True)
                            return
                        self.stop()

                view = ConfirmView()
                await ctx.respond(embed=confirm_embed, view=view, ephemeral=True)

                await view.wait()

                if view.confirmed:
                    pruned = await ctx.guild.prune_members(
                        days=days,
                        roles=ctx.guild.roles if include_roles else [],
                        reason=f"Prune by {ctx.author} - {days} days inactive"
                    )
                    await ctx.edit(
                        embed=discord.Embed(
                            title="✅ Prune Complete",
                            description=f"**{pruned}** members have been kicked.",
                            color=discord.Color.green()
                        ),
                        view=None
                    )
                    logger.info(f"Prune executed in {ctx.guild.name}: {pruned} members by {ctx.author}")
                else:
                    await ctx.edit(
                        embed=discord.Embed(
                            title="❌ Prune Cancelled",
                            color=discord.Color.grey()
                        ),
                        view=None
                    )

        except discord.Forbidden:
            await ctx.respond("❌ I don't have permission to kick members!", ephemeral=True)
        except Exception as e:
            logger.error(f"Prune error: {e}")
            await ctx.respond(f"❌ Error: {str(e)}", ephemeral=True)

    # =============================================================================
    # BROADCAST COMMAND
    # =============================================================================

    @discord.slash_command(name="broadcast", description="Send a message to multiple channels")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    @discord.option("message", str, description="Message to broadcast")
    @discord.option("channel_type", str, description="Which channels to send to", choices=[
        discord.OptionChoice(name="All Text Channels", value="all"),
        discord.OptionChoice(name="Announcement Channels Only", value="news"),
        discord.OptionChoice(name="Specific Category", value="category"),
    ])
    @discord.option("category", discord.CategoryChannel, description="Category (if using category type)", required=False)
    @discord.option("use_embed", bool, description="Send as embed?", required=False)
    @discord.option("embed_title", str, description="Embed title (if using embed)", required=False)
    @discord.option("ping_everyone", bool, description="Ping @everyone?", required=False)
    async def broadcast(
        self,
        ctx: discord.ApplicationContext,
        message: str,
        channel_type: str,
        category: discord.CategoryChannel = None,
        use_embed: bool = False,
        embed_title: str = None,
        ping_everyone: bool = False
    ):
        """Broadcast a message to multiple channels."""
        await ctx.defer(ephemeral=True)

        # Get target channels
        channels = []
        if channel_type == "all":
            channels = [c for c in ctx.guild.text_channels if c.permissions_for(ctx.guild.me).send_messages]
        elif channel_type == "news":
            channels = [c for c in ctx.guild.text_channels if c.is_news() and c.permissions_for(ctx.guild.me).send_messages]
        elif channel_type == "category":
            if not category:
                await ctx.respond("❌ Please select a category!", ephemeral=True)
                return
            channels = [c for c in category.text_channels if c.permissions_for(ctx.guild.me).send_messages]

        if not channels:
            await ctx.respond("❌ No channels found to broadcast to!", ephemeral=True)
            return

        # Confirm
        embed = discord.Embed(
            title="📢 Broadcast Preview",
            description=f"Will send to **{len(channels)}** channels",
            color=discord.Color.orange()
        )
        embed.add_field(name="Message", value=message[:500] + ("..." if len(message) > 500 else ""), inline=False)
        embed.add_field(name="Ping Everyone", value="Yes" if ping_everyone else "No", inline=True)
        embed.add_field(name="Use Embed", value="Yes" if use_embed else "No", inline=True)

        class BroadcastView(discord.ui.View):
            def __init__(self):
                super().__init__(timeout=60)
                self.confirmed = False

            @discord.ui.button(label="Send Broadcast", style=discord.ButtonStyle.danger)
            async def confirm(self, button, interaction):
                if interaction.user.id != ctx.author.id:
                    return
                self.confirmed = True
                self.stop()

            @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
            async def cancel(self, button, interaction):
                if interaction.user.id != ctx.author.id:
                    return
                self.stop()

        view = BroadcastView()
        await ctx.respond(embed=embed, view=view, ephemeral=True)
        await view.wait()

        if not view.confirmed:
            await ctx.edit(embed=discord.Embed(title="❌ Broadcast Cancelled", color=discord.Color.grey()), view=None)
            return

        # Send broadcast
        success = 0
        failed = 0
        content = f"@everyone\n{message}" if ping_everyone else message

        for channel in channels:
            try:
                if use_embed:
                    broadcast_embed = discord.Embed(
                        title=embed_title or "📢 Announcement",
                        description=message,
                        color=discord.Color.blurple()
                    )
                    broadcast_embed.set_footer(text=f"From {ctx.author.display_name}")
                    broadcast_embed.timestamp = datetime.utcnow()
                    await channel.send(content="@everyone" if ping_everyone else None, embed=broadcast_embed)
                else:
                    await channel.send(content)
                success += 1
                await asyncio.sleep(0.5)  # Rate limit protection
            except Exception as e:
                logger.warning(f"Broadcast failed to {channel.name}: {e}")
                failed += 1

        result_embed = discord.Embed(
            title="✅ Broadcast Complete",
            description=f"Sent to **{success}** channels\nFailed: **{failed}**",
            color=discord.Color.green()
        )
        await ctx.edit(embed=result_embed, view=None)
        logger.info(f"Broadcast by {ctx.author} in {ctx.guild.name}: {success} success, {failed} failed")

    # =============================================================================
    # SUGGESTION SYSTEM
    # =============================================================================

    suggestion = SlashCommandGroup(
        name="suggestion",
        description="Suggestion system commands",
        
    )

    @suggestion.command(name="submit", description="Submit a suggestion for the server")
    @discord.option("title", str, description="Brief title for your suggestion")
    @discord.option("description", str, description="Detailed description of your suggestion")
    @discord.option("category", str, description="Category", choices=[
        discord.OptionChoice(name="New Feature", value="feature"),
        discord.OptionChoice(name="Improvement", value="improvement"),
        discord.OptionChoice(name="Bug Report", value="bug"),
        discord.OptionChoice(name="Other", value="other"),
    ], required=False)
    async def suggestion_submit(
        self,
        ctx: discord.ApplicationContext,
        title: str,
        description: str,
        category: str = "other"
    ):
        """Submit a suggestion to the server."""
        try:
            with db_session_scope() as session:
                # Get feedback config
                config = session.query(FeedbackConfig).filter_by(guild_id=ctx.guild.id).first()

                if not config or not config.enabled:
                    await ctx.respond(
                        "❌ Suggestions are not enabled on this server.",
                        ephemeral=True
                    )
                    return

                if not config.feedback_channel_id:
                    await ctx.respond(
                        "❌ No suggestion channel has been set up. Ask an admin to configure it.",
                        ephemeral=True
                    )
                    return

                # Create suggestion
                suggestion = Suggestion(
                    guild_id=ctx.guild.id,
                    user_id=ctx.author.id,
                    title=title,
                    content=description,
                    category=category,
                    status=SuggestionStatus.PENDING
                )
                session.add(suggestion)
                session.flush()  # Get the ID

                # Build embed
                category_emoji = {
                    "feature": "✨",
                    "improvement": "📈",
                    "bug": "🐛",
                    "other": "💡"
                }

                embed = discord.Embed(
                    title=f"{category_emoji.get(category, '💡')} {title}",
                    description=description,
                    color=discord.Color.blurple()
                )
                embed.add_field(name="Category", value=category.title(), inline=True)
                embed.add_field(name="Status", value="🟡 Pending", inline=True)
                embed.add_field(name="Suggestion ID", value=f"#{suggestion.id}", inline=True)

                if not config.anonymous:
                    embed.set_author(
                        name=ctx.author.display_name,
                        icon_url=ctx.author.display_avatar.url
                    )
                else:
                    embed.set_author(name="Anonymous Suggestion")

                embed.timestamp = datetime.utcnow()
                embed.set_footer(text="React with 👍 or 👎 to vote!")

                # Send to suggestion channel
                channel = ctx.guild.get_channel(config.feedback_channel_id)
                if channel:
                    msg = await channel.send(embed=embed)
                    await msg.add_reaction("👍")
                    await msg.add_reaction("👎")

                    # Update suggestion with message ID
                    suggestion.message_id = msg.id
                    suggestion.channel_id = channel.id

                    await ctx.respond(
                        f"✅ Your suggestion has been submitted!\n"
                        f"View it in {channel.mention}",
                        ephemeral=True
                    )
                else:
                    await ctx.respond(
                        "✅ Suggestion recorded, but couldn't post to channel.",
                        ephemeral=True
                    )

        except Exception as e:
            logger.error(f"Suggestion submit error: {e}")
            await ctx.respond("❌ Error submitting suggestion!", ephemeral=True)

    @suggestion.command(name="review", description="Review and update a suggestion (Admin)")
    @discord.default_permissions(manage_guild=True)
    @commands.has_permissions(manage_guild=True)
    @discord.option("suggestion_id", int, description="Suggestion ID number")
    @discord.option("status", str, description="New status", choices=[
        discord.OptionChoice(name="Approved", value="approved"),
        discord.OptionChoice(name="Rejected", value="rejected"),
        discord.OptionChoice(name="Under Review", value="under_review"),
        discord.OptionChoice(name="Implemented", value="implemented"),
    ])
    @discord.option("note", str, description="Response/note to add", required=False)
    async def suggestion_review(
        self,
        ctx: discord.ApplicationContext,
        suggestion_id: int,
        status: str,
        note: str = None
    ):
        """Review and update the status of a suggestion."""
        try:
            with db_session_scope() as session:
                suggestion = session.query(Suggestion).filter_by(
                    id=suggestion_id,
                    guild_id=ctx.guild.id
                ).first()

                if not suggestion:
                    await ctx.respond("❌ Suggestion not found!", ephemeral=True)
                    return

                # Update suggestion
                old_status = suggestion.status
                suggestion.status = SuggestionStatus(status)
                suggestion.status_note = note
                suggestion.reviewed_by = ctx.author.id
                suggestion.reviewed_at = int(time.time())
                suggestion.updated_at = int(time.time())

                # Status colors and emojis
                status_info = {
                    "approved": ("✅", discord.Color.green(), "Approved"),
                    "rejected": ("❌", discord.Color.red(), "Rejected"),
                    "under_review": ("🔍", discord.Color.yellow(), "Under Review"),
                    "implemented": ("🎉", discord.Color.gold(), "Implemented"),
                }

                emoji, color, status_text = status_info.get(status, ("📋", discord.Color.grey(), status))

                # Try to update the original message
                if suggestion.message_id and suggestion.channel_id:
                    try:
                        channel = ctx.guild.get_channel(suggestion.channel_id)
                        if channel:
                            msg = await channel.fetch_message(suggestion.message_id)
                            if msg.embeds:
                                old_embed = msg.embeds[0]
                                new_embed = discord.Embed(
                                    title=old_embed.title,
                                    description=old_embed.description,
                                    color=color
                                )
                                new_embed.add_field(name="Category", value=suggestion.category.title() if suggestion.category else "Other", inline=True)
                                new_embed.add_field(name="Status", value=f"{emoji} {status_text}", inline=True)
                                new_embed.add_field(name="Suggestion ID", value=f"#{suggestion.id}", inline=True)

                                if note:
                                    new_embed.add_field(name="📝 Admin Response", value=note, inline=False)

                                if old_embed.author:
                                    new_embed.set_author(name=old_embed.author.name, icon_url=old_embed.author.icon_url)

                                new_embed.timestamp = datetime.utcnow()
                                new_embed.set_footer(text=f"Reviewed by {ctx.author.display_name}")

                                await msg.edit(embed=new_embed)
                    except Exception as e:
                        logger.warning(f"Could not update suggestion message: {e}")

                await ctx.respond(
                    f"✅ Suggestion #{suggestion_id} updated to **{status_text}**",
                    ephemeral=True
                )

        except Exception as e:
            logger.error(f"Suggestion review error: {e}")
            await ctx.respond("❌ Error updating suggestion!", ephemeral=True)

    @suggestion.command(name="list", description="List suggestions (Admin)")
    @discord.default_permissions(manage_guild=True)
    @commands.has_permissions(manage_guild=True)
    @discord.option("status", str, description="Filter by status", choices=[
        discord.OptionChoice(name="Pending", value="pending"),
        discord.OptionChoice(name="Approved", value="approved"),
        discord.OptionChoice(name="Rejected", value="rejected"),
        discord.OptionChoice(name="Under Review", value="under_review"),
        discord.OptionChoice(name="Implemented", value="implemented"),
        discord.OptionChoice(name="All", value="all"),
    ], required=False)
    async def suggestion_list(
        self,
        ctx: discord.ApplicationContext,
        status: str = "pending"
    ):
        """List suggestions for the server."""
        try:
            with db_session_scope() as session:
                query = session.query(Suggestion).filter(Suggestion.guild_id == ctx.guild.id)

                if status != "all":
                    query = query.filter(Suggestion.status == SuggestionStatus(status))

                suggestions = query.order_by(Suggestion.created_at.desc()).limit(10).all()

                if not suggestions:
                    await ctx.respond(f"No suggestions found with status: {status}", ephemeral=True)
                    return

                embed = discord.Embed(
                    title=f"📋 Suggestions ({status.replace('_', ' ').title()})",
                    color=discord.Color.blurple()
                )

                for s in suggestions:
                    status_emoji = {
                        SuggestionStatus.PENDING: "🟡",
                        SuggestionStatus.APPROVED: "✅",
                        SuggestionStatus.REJECTED: "❌",
                        SuggestionStatus.UNDER_REVIEW: "🔍",
                        SuggestionStatus.IMPLEMENTED: "🎉",
                    }
                    embed.add_field(
                        name=f"#{s.id} {status_emoji.get(s.status, '')} {s.title[:50]}",
                        value=f"{s.content[:100]}..." if len(s.content) > 100 else s.content,
                        inline=False
                    )

                embed.set_footer(text="Use /suggestion review <id> to update status")
                await ctx.respond(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Suggestion list error: {e}")
            await ctx.respond("❌ Error listing suggestions!", ephemeral=True)

    @suggestion.command(name="setup", description="Setup the suggestion system (Admin)")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    @discord.option("channel", discord.TextChannel, description="Channel for suggestions")
    @discord.option("anonymous", bool, description="Allow anonymous suggestions?", required=False)
    async def suggestion_setup(
        self,
        ctx: discord.ApplicationContext,
        channel: discord.TextChannel,
        anonymous: bool = True
    ):
        """Setup or update the suggestion system configuration."""
        try:
            with db_session_scope() as session:
                config = session.query(FeedbackConfig).filter_by(guild_id=ctx.guild.id).first()

                if not config:
                    config = FeedbackConfig(guild_id=ctx.guild.id)
                    session.add(config)

                config.enabled = True
                config.feedback_channel_id = channel.id
                config.anonymous = anonymous

            await ctx.respond(
                f"✅ Suggestion system configured!\n"
                f"Channel: {channel.mention}\n"
                f"Anonymous: {'Yes' if anonymous else 'No'}\n\n"
                f"Members can now use `/suggestion submit` to submit suggestions.",
                ephemeral=True
            )

        except Exception as e:
            logger.error(f"Suggestion setup error: {e}")
            await ctx.respond("❌ Error setting up suggestions!", ephemeral=True)

    # VIP Slash Commands (Bot Owner Only)
    vip = SlashCommandGroup(
        name="vip",
        description="VIP management (Bot Owner Only)",
        
    )

    def is_bot_owner():
        """Check if user is the bot owner."""
        async def predicate(ctx: discord.ApplicationContext):
            if ctx.author.id != BOT_OWNER_ID or BOT_OWNER_ID == 0:
                await ctx.respond("❌ This command is restricted to the bot owner.", ephemeral=True)
                return False
            return True
        return commands.check(predicate)

    @vip.command(name="grant", description="Grant VIP status to a server (Owner Only)")
    @is_bot_owner()
    @discord.option("guild_id", str, description="Guild ID to grant VIP to")
    @discord.option("note", str, description="Optional note", required=False)
    async def vip_grant_slash(
        self,
        ctx: discord.ApplicationContext,
        guild_id: str,
        note: str = None
    ):
        """Grant VIP status to a guild via slash command."""
        await ctx.defer(ephemeral=True)

        try:
            target_guild_id = int(guild_id)
        except ValueError:
            await ctx.followup.send("❌ Invalid guild ID.", ephemeral=True)
            return

        with db_session_scope() as session:
            guild = session.get(Guild, target_guild_id)

            if not guild:
                await ctx.followup.send(
                    f"❌ Guild `{target_guild_id}` not found in database.\n"
                    "The bot must be in the server first.",
                    ephemeral=True
                )
                return

            guild.is_vip = True
            guild.vip_granted_by = ctx.author.id
            guild.vip_granted_at = int(time.time())
            guild.vip_note = note

            guild_name = guild.guild_name or "Unknown"

        logger.info(f"VIP granted to {guild_name} ({target_guild_id}) by {ctx.author}")

        await ctx.followup.send(
            f"✅ **VIP Granted!**\n\n"
            f"Guild: **{guild_name}** (`{target_guild_id}`)\n"
            f"Note: {note or 'None'}\n\n"
            f"All premium features are now unlocked for free.",
            ephemeral=True
        )

    @vip.command(name="here", description="Grant VIP to current server (Owner Only)")
    @is_bot_owner()
    @discord.option("note", str, description="Optional note", required=False)
    async def vip_here_slash(
        self,
        ctx: discord.ApplicationContext,
        note: str = None
    ):
        """Grant VIP to the current server via slash command."""
        await ctx.defer(ephemeral=True)

        if not ctx.guild:
            await ctx.followup.send("❌ This command must be used in a server.", ephemeral=True)
            return

        with db_session_scope() as session:
            guild = session.get(Guild, ctx.guild.id)

            if not guild:
                await ctx.followup.send("❌ Guild not found in database.", ephemeral=True)
                return

            guild.is_vip = True
            guild.vip_granted_by = ctx.author.id
            guild.vip_granted_at = int(time.time())
            guild.vip_note = note or "Granted in-server"

        logger.info(f"VIP granted to {ctx.guild.name} ({ctx.guild.id}) by {ctx.author}")

        await ctx.followup.send(
            f"✅ **VIP Granted to this server!**\n\n"
            f"All premium features are now unlocked for **{ctx.guild.name}**.",
            ephemeral=True
        )

    @vip.command(name="revoke", description="Revoke VIP status from a server (Owner Only)")
    @is_bot_owner()
    @discord.option("guild_id", str, description="Guild ID to revoke VIP from")
    async def vip_revoke_slash(
        self,
        ctx: discord.ApplicationContext,
        guild_id: str
    ):
        """Revoke VIP status from a guild via slash command."""
        await ctx.defer(ephemeral=True)

        try:
            target_guild_id = int(guild_id)
        except ValueError:
            await ctx.followup.send("❌ Invalid guild ID.", ephemeral=True)
            return

        with db_session_scope() as session:
            guild = session.get(Guild, target_guild_id)

            if not guild:
                await ctx.followup.send(f"❌ Guild `{target_guild_id}` not found.", ephemeral=True)
                return

            if not guild.is_vip:
                await ctx.followup.send(f"⚠️ Guild is not VIP.", ephemeral=True)
                return

            guild.is_vip = False
            guild.vip_granted_by = None
            guild.vip_granted_at = None
            guild.vip_note = None

            guild_name = guild.guild_name or "Unknown"

        logger.info(f"VIP revoked from {guild_name} ({target_guild_id}) by {ctx.author}")

        await ctx.followup.send(
            f"✅ VIP status revoked from **{guild_name}** (`{target_guild_id}`).",
            ephemeral=True
        )

    @vip.command(name="list", description="List all VIP servers (Owner Only)")
    @is_bot_owner()
    async def vip_list_slash(
        self,
        ctx: discord.ApplicationContext
    ):
        """List all VIP guilds via slash command."""
        await ctx.defer(ephemeral=True)

        with db_session_scope() as session:
            vip_guilds = (
                session.query(Guild)
                .filter(Guild.is_vip == True)
                .all()
            )

            if not vip_guilds:
                await ctx.followup.send("No VIP guilds found.", ephemeral=True)
                return

            embed = discord.Embed(
                title="🌟 VIP Guilds",
                color=discord.Color.gold()
            )

            for guild in vip_guilds:
                granted_at = f"<t:{guild.vip_granted_at}:R>" if guild.vip_granted_at else "Unknown"
                embed.add_field(
                    name=f"{guild.guild_name or 'Unknown'}",
                    value=(
                        f"ID: `{guild.guild_id}`\n"
                        f"Note: {guild.vip_note or 'None'}\n"
                        f"Granted: {granted_at}"
                    ),
                    inline=True
                )

        await ctx.followup.send(embed=embed, ephemeral=True)

    # VIP/Friends commands (owner only, hidden message-based commands)
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Hidden VIP commands - only responds to bot owner."""
        # Ignore bots
        if message.author.bot:
            return

        # ONLY respond to the bot owner - everyone else is completely ignored
        if message.author.id != BOT_OWNER_ID or BOT_OWNER_ID == 0:
            return

        # Check for secret prefix
        if not message.content.startswith("w!vip "):
            return

        # Parse command
        content = message.content[6:].strip()  # Remove "w!vip "
        parts = content.split(maxsplit=2)

        if not parts:
            return

        cmd = parts[0].lower()

        # Delete the command message for secrecy (if in a guild)
        if message.guild:
            try:
                await message.delete()
            except discord.Forbidden:
                pass

        # Handle commands
        if cmd == "grant" and len(parts) >= 2:
            await self._vip_grant(message, parts[1], parts[2] if len(parts) > 2 else None)
        elif cmd == "revoke" and len(parts) >= 2:
            await self._vip_revoke(message, parts[1])
        elif cmd == "list":
            await self._vip_list(message)
        elif cmd == "here":
            await self._vip_here(message, parts[1] if len(parts) > 1 else None)
        elif cmd == "help":
            await self._vip_help(message)

    async def _vip_help(self, message: discord.Message):
        """Show VIP command help."""
        help_text = (
            "**VIP Commands (Owner Only)**\n\n"
            "`w!vip grant <guild_id> [note]` - Grant VIP to a server\n"
            "`w!vip revoke <guild_id>` - Revoke VIP from a server\n"
            "`w!vip here [note]` - Grant VIP to current server\n"
            "`w!vip list` - List all VIP servers\n"
            "`w!vip help` - Show this help"
        )
        await message.author.send(help_text)

    async def _vip_grant(self, message: discord.Message, guild_id_str: str, note: str = None):
        """Grant VIP status to a guild."""
        try:
            target_guild_id = int(guild_id_str)
        except ValueError:
            await message.author.send("❌ Invalid guild ID.")
            return

        with db_session_scope() as session:
            guild = session.get(Guild, target_guild_id)

            if not guild:
                await message.author.send(
                    f"❌ Guild `{target_guild_id}` not found in database.\n"
                    "The bot must be in the server first."
                )
                return

            guild.is_vip = True
            guild.vip_granted_by = message.author.id
            guild.vip_granted_at = int(time.time())
            guild.vip_note = note

            guild_name = guild.guild_name or "Unknown"

        logger.info(f"VIP granted to {guild_name} ({target_guild_id}) by {message.author}")

        await message.author.send(
            f"✅ **VIP Granted!**\n\n"
            f"Guild: **{guild_name}** (`{target_guild_id}`)\n"
            f"Note: {note or 'None'}\n\n"
            f"All premium features are now unlocked for free."
        )

    async def _vip_revoke(self, message: discord.Message, guild_id_str: str):
        """Revoke VIP status from a guild."""
        try:
            target_guild_id = int(guild_id_str)
        except ValueError:
            await message.author.send("❌ Invalid guild ID.")
            return

        with db_session_scope() as session:
            guild = session.get(Guild, target_guild_id)

            if not guild:
                await message.author.send(f"❌ Guild `{target_guild_id}` not found.")
                return

            if not guild.is_vip:
                await message.author.send(f"⚠️ Guild is not VIP.")
                return

            guild.is_vip = False
            guild.vip_granted_by = None
            guild.vip_granted_at = None
            guild.vip_note = None

            guild_name = guild.guild_name or "Unknown"

        logger.info(f"VIP revoked from {guild_name} ({target_guild_id}) by {message.author}")

        await message.author.send(
            f"✅ VIP status revoked from **{guild_name}** (`{target_guild_id}`)."
        )

    async def _vip_list(self, message: discord.Message):
        """List all VIP guilds."""
        with db_session_scope() as session:
            vip_guilds = (
                session.query(Guild)
                .filter(Guild.is_vip == True)
                .all()
            )

            if not vip_guilds:
                await message.author.send("No VIP guilds found.")
                return

            embed = discord.Embed(
                title="🌟 VIP Guilds",
                color=discord.Color.gold()
            )

            for guild in vip_guilds:
                granted_at = f"<t:{guild.vip_granted_at}:R>" if guild.vip_granted_at else "Unknown"
                embed.add_field(
                    name=f"{guild.guild_name or 'Unknown'}",
                    value=(
                        f"ID: `{guild.guild_id}`\n"
                        f"Note: {guild.vip_note or 'None'}\n"
                        f"Granted: {granted_at}"
                    ),
                    inline=True
                )

        await message.author.send(embed=embed)

    async def _vip_here(self, message: discord.Message, note: str = None):
        """Grant VIP to the current server."""
        if not message.guild:
            await message.author.send("❌ This command must be used in a server.")
            return

        with db_session_scope() as session:
            guild = session.get(Guild, message.guild.id)

            if not guild:
                await message.author.send("❌ Guild not found in database.")
                return

            guild.is_vip = True
            guild.vip_granted_by = message.author.id
            guild.vip_granted_at = int(time.time())
            guild.vip_note = note or f"Granted in-server"

        logger.info(f"VIP granted to {message.guild.name} ({message.guild.id}) by {message.author}")

        await message.author.send(
            f"✅ **VIP Granted to this server!**\n\n"
            f"All premium features are now unlocked for **{message.guild.name}**."
        )

    @discord.slash_command(
        name="purgelfgs",
        description="Purge all LFG threads older than 24 hours (Admin)"
    )
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    async def purge_lfgs(self, ctx: discord.ApplicationContext):
        """Purge all LFG threads that are past the 24-hour threshold."""
        await ctx.defer()

        if not ctx.guild:
            await ctx.respond("❌ This command must be used in a server.", ephemeral=True)
            return

        purged_count = 0
        failed_count = 0
        cutoff_time = int(time.time()) - (24 * 60 * 60)  # 24 hours ago

        with db_session_scope() as session:
            # Get all LFG groups for this guild
            lfg_groups = session.query(LFGGroup).filter_by(
                guild_id=ctx.guild.id,
                is_active=True
            ).all()

            for group in lfg_groups:
                # Check if group is older than 24 hours
                if group.created_at and group.created_at < cutoff_time:
                    try:
                        # Try to get and archive/delete the thread
                        if group.thread_id:
                            try:
                                thread = await ctx.guild.fetch_channel(group.thread_id)
                                if isinstance(thread, discord.Thread):
                                    await thread.delete()
                                    logger.info(f"Purged LFG thread {thread.name} (ID: {thread.id}) - older than 24h")
                                    purged_count += 1
                            except discord.NotFound:
                                # Thread already deleted
                                logger.debug(f"LFG thread {group.thread_id} already deleted")
                                purged_count += 1
                            except discord.Forbidden:
                                logger.warning(f"No permission to delete thread {group.thread_id}")
                                failed_count += 1
                                continue
                            except Exception as e:
                                logger.error(f"Error deleting thread {group.thread_id}: {e}")
                                failed_count += 1
                                continue

                        # Mark group as inactive in database
                        group.is_active = False

                    except Exception as e:
                        logger.error(f"Error processing LFG group {group.id}: {e}")
                        failed_count += 1

        # Send summary
        embed = discord.Embed(
            title="🧹 LFG Thread Purge Complete",
            color=discord.Color.green() if purged_count > 0 else discord.Color.orange(),
            timestamp=datetime.utcnow()
        )
        embed.add_field(name="Purged", value=f"{purged_count} threads", inline=True)
        embed.add_field(name="Failed", value=f"{failed_count} threads", inline=True)
        embed.add_field(
            name="Threshold",
            value="24 hours",
            inline=True
        )

        await ctx.respond(embed=embed)
        logger.info(f"LFG purge completed in {ctx.guild.name}: {purged_count} purged, {failed_count} failed")


def setup(bot: commands.Bot):
    bot.add_cog(AdminCog(bot))
