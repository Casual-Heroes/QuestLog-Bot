# admin_cog.py - Consolidated Admin Utilities for Warden
# Combines: broadcast, sendembed, editembed, sendmessage, feedback

import discord
from discord.ext import commands
from discord.commands import slash_command, option
from typing import Optional, List
import time
import logging
from datetime import datetime, timezone

import sys
sys.path.insert(0, '..')
from db import get_db_session
from models import FeedbackConfig

# Don't call basicConfig() - config.py already set up logging
logger = logging.getLogger("admin")

EMBED_COLOR = 0x5865F2  # Discord blurple


# =============================================================================
# MODALS
# =============================================================================

class SendEmbedModal(discord.ui.Modal):
    """Modal for creating and sending an embed."""
    def __init__(self, channel: discord.TextChannel = None):
        super().__init__(title="Create Embed Message")
        self.target_channel = channel

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
            label="Footer (optional)",
            placeholder="Footer text",
            required=False,
            max_length=256
        ))
        self.add_item(discord.ui.InputText(
            label="Color (hex, optional)",
            placeholder="e.g., #FF5500 or FF5500",
            required=False,
            max_length=7
        ))

    async def callback(self, interaction: discord.Interaction):
        title = self.children[0].value
        description = self.children[1].value
        footer = self.children[2].value
        color_str = self.children[3].value

        # Replace mentions
        for role in interaction.guild.roles:
            description = description.replace(f"@{role.name}", f"<@&{role.id}>")
        for channel in interaction.guild.channels:
            description = description.replace(f"#{channel.name}", f"<#{channel.id}>")

        # Parse color
        color = EMBED_COLOR
        if color_str:
            try:
                color = int(color_str.replace("#", ""), 16)
            except:
                pass

        embed = discord.Embed(title=title, description=description, color=color)
        if footer:
            embed.set_footer(text=footer)
        embed.timestamp = datetime.now(timezone.utc)

        target = self.target_channel or interaction.channel
        await target.send(embed=embed)
        await interaction.response.send_message(
            f"Embed sent to {target.mention}!",
            ephemeral=True
        )


class EditEmbedModal(discord.ui.Modal):
    """Modal for editing an existing embed."""
    def __init__(self, message: discord.Message):
        super().__init__(title="Edit Embed")
        self.target_message = message

        current = message.embeds[0] if message.embeds else None
        self.add_item(discord.ui.InputText(
            label="New Title",
            placeholder="Enter new title",
            default=current.title if current else "",
            required=True,
            max_length=256
        ))
        self.add_item(discord.ui.InputText(
            label="New Description",
            placeholder="Enter new description",
            style=discord.InputTextStyle.long,
            default=current.description if current else "",
            required=True,
            max_length=4000
        ))

    async def callback(self, interaction: discord.Interaction):
        new_title = self.children[0].value
        new_desc = self.children[1].value

        # Replace mentions
        for role in interaction.guild.roles:
            new_desc = new_desc.replace(f"@{role.name}", f"<@&{role.id}>")
        for channel in interaction.guild.channels:
            new_desc = new_desc.replace(f"#{channel.name}", f"<#{channel.id}>")

        embed = self.target_message.embeds[0] if self.target_message.embeds else discord.Embed()
        embed.title = new_title
        embed.description = new_desc

        await self.target_message.edit(embed=embed)
        await interaction.response.send_message("Embed updated!", ephemeral=True)


class BroadcastModal(discord.ui.Modal):
    """Modal for broadcast message content."""
    def __init__(self, channels: List[int]):
        super().__init__(title="Broadcast Message")
        self.channel_ids = channels

        self.add_item(discord.ui.InputText(
            label="Message",
            placeholder="Your message (supports @role and #channel)",
            style=discord.InputTextStyle.long,
            required=True,
            max_length=2000
        ))

    async def callback(self, interaction: discord.Interaction):
        message = self.children[0].value

        # Replace mentions
        for role in interaction.guild.roles:
            message = message.replace(f"@{role.name}", f"<@&{role.id}>")
        for channel in interaction.guild.channels:
            message = message.replace(f"#{channel.name}", f"<#{channel.id}>")

        success, failed = [], []
        for cid in self.channel_ids:
            channel = interaction.guild.get_channel(cid)
            if channel:
                try:
                    await channel.send(message)
                    success.append(channel.name)
                except:
                    failed.append(channel.name)

        result = f"**Sent to:** {', '.join(success) if success else 'None'}"
        if failed:
            result += f"\n**Failed:** {', '.join(failed)}"
        await interaction.response.send_message(result, ephemeral=True)


class FeedbackModal(discord.ui.Modal):
    """Modal for anonymous feedback submission."""
    def __init__(self, channel_id: int, anonymous: bool = True):
        super().__init__(title="Submit Feedback")
        self.channel_id = channel_id
        self.anonymous = anonymous

        self.add_item(discord.ui.InputText(
            label="Subject",
            placeholder="Brief summary of your feedback",
            required=True,
            max_length=100
        ))
        self.add_item(discord.ui.InputText(
            label="Details",
            placeholder="Explain your feedback or suggestion",
            style=discord.InputTextStyle.long,
            required=True,
            max_length=2000
        ))

    async def callback(self, interaction: discord.Interaction):
        subject = self.children[0].value
        details = self.children[1].value

        embed = discord.Embed(
            title=f"Feedback: {subject}",
            description=details,
            color=EMBED_COLOR,
            timestamp=datetime.now(timezone.utc)
        )

        if not self.anonymous:
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.display_avatar.url
            )
        else:
            embed.set_footer(text="Submitted anonymously")

        channel = interaction.guild.get_channel(self.channel_id)
        if channel:
            await channel.send(embed=embed)
            await interaction.response.send_message(
                "Thank you! Your feedback has been submitted.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "Feedback channel not found!",
                ephemeral=True
            )


# =============================================================================
# VIEWS
# =============================================================================

class ChannelSelectView(discord.ui.View):
    """View for selecting channels to broadcast to."""
    def __init__(self, channels: List[discord.TextChannel]):
        super().__init__(timeout=60)
        options = [
            discord.SelectOption(label=ch.name, value=str(ch.id))
            for ch in channels[:25]
        ]
        self.select = discord.ui.Select(
            placeholder="Select channels",
            options=options,
            min_values=1,
            max_values=len(options)
        )
        self.select.callback = self.on_select
        self.add_item(self.select)

    async def on_select(self, interaction: discord.Interaction):
        selected = [int(cid) for cid in self.select.values]
        await interaction.response.send_modal(BroadcastModal(selected))


# =============================================================================
# MAIN COG
# =============================================================================

class AdminCog(commands.Cog):
    """Admin utilities for server management."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # ─── EMBED COMMANDS ───────────────────────────────────────────────────────

    @slash_command(name="sendembed", description="Send an embed to a channel")
    @commands.has_permissions(manage_messages=True)
    @discord.default_permissions(manage_messages=True)
    @option("channel", description="Target channel (default: current)", required=False)
    async def sendembed(
        self,
        ctx: discord.ApplicationContext,
        channel: discord.TextChannel = None
    ):
        """Open a modal to create and send an embed."""
        modal = SendEmbedModal(channel)
        await ctx.send_modal(modal)

    @slash_command(name="editembed", description="Edit an existing embed by message ID")
    @commands.has_permissions(manage_messages=True)
    @discord.default_permissions(manage_messages=True)
    @option("message_id", description="The message ID to edit")
    async def editembed(self, ctx: discord.ApplicationContext, message_id: str):
        """Edit an existing embed."""
        try:
            msg_id = int(message_id)
            message = await ctx.channel.fetch_message(msg_id)
        except (ValueError, discord.NotFound):
            await ctx.respond("Message not found!", ephemeral=True)
            return

        if not message.embeds:
            await ctx.respond("That message has no embed!", ephemeral=True)
            return

        modal = EditEmbedModal(message)
        await ctx.send_modal(modal)

    # ─── BROADCAST COMMANDS ───────────────────────────────────────────────────

    @slash_command(name="broadcast", description="Send a message to multiple channels")
    @commands.has_permissions(manage_messages=True)
    @discord.default_permissions(manage_messages=True)
    @option("message", description="Message to send")
    @option("channel1", description="First channel")
    @option("channel2", description="Second channel", required=False)
    @option("channel3", description="Third channel", required=False)
    @option("channel4", description="Fourth channel", required=False)
    @option("channel5", description="Fifth channel", required=False)
    async def broadcast(
        self,
        ctx: discord.ApplicationContext,
        message: str,
        channel1: discord.TextChannel,
        channel2: discord.TextChannel = None,
        channel3: discord.TextChannel = None,
        channel4: discord.TextChannel = None,
        channel5: discord.TextChannel = None
    ):
        """Send a message to up to 5 channels."""
        await ctx.defer(ephemeral=True)

        channels = [ch for ch in [channel1, channel2, channel3, channel4, channel5] if ch]

        # Replace mentions
        for role in ctx.guild.roles:
            message = message.replace(f"@{role.name}", f"<@&{role.id}>")
        for channel in ctx.guild.channels:
            message = message.replace(f"#{channel.name}", f"<#{channel.id}>")

        success, failed = [], []
        for ch in channels:
            try:
                await ch.send(message)
                success.append(ch.name)
            except:
                failed.append(ch.name)

        result = f"**Sent to:** {', '.join(success)}"
        if failed:
            result += f"\n**Failed:** {', '.join(failed)}"
        await ctx.respond(result, ephemeral=True)

    @slash_command(name="broadcast_select", description="Select channels from a list to broadcast to")
    @commands.has_permissions(manage_messages=True)
    @discord.default_permissions(manage_messages=True)
    async def broadcast_select(self, ctx: discord.ApplicationContext):
        """Show a channel selector for broadcasting."""
        channels = [
            ch for ch in ctx.guild.text_channels
            if ch.permissions_for(ctx.author).send_messages
        ]
        view = ChannelSelectView(channels)
        await ctx.respond("Select channels:", view=view, ephemeral=True)

    # ─── FEEDBACK COMMANDS ────────────────────────────────────────────────────

    @slash_command(name="feedback", description="Submit feedback or suggestions")
    async def feedback(self, ctx: discord.ApplicationContext):
        """Submit feedback to the configured channel."""
        try:
            with get_db_session() as session:
                config = session.query(FeedbackConfig).filter_by(
                    guild_id=ctx.guild.id
                ).first()

                if not config or not config.feedback_channel_id:
                    await ctx.respond(
                        "Feedback not configured! Ask an admin to set it up.",
                        ephemeral=True
                    )
                    return

                modal = FeedbackModal(config.feedback_channel_id, config.anonymous)
                await ctx.send_modal(modal)

        except Exception as e:
            logger.error(f"Feedback error: {e}")
            await ctx.respond("Error loading feedback config!", ephemeral=True)

    @slash_command(name="feedback_setup", description="Configure the feedback system (Admin)")
    @commands.has_permissions(administrator=True)
    @discord.default_permissions(administrator=True)
    @option("channel", description="Channel for feedback submissions")
    @option("anonymous", description="Hide submitter info?", required=False, default=True)
    async def feedback_setup(
        self,
        ctx: discord.ApplicationContext,
        channel: discord.TextChannel,
        anonymous: bool = True
    ):
        """Configure the feedback system."""
        try:
            with get_db_session() as session:
                config = session.query(FeedbackConfig).filter_by(
                    guild_id=ctx.guild.id
                ).first()

                if config:
                    config.feedback_channel_id = channel.id
                    config.anonymous = anonymous
                else:
                    config = FeedbackConfig(
                        guild_id=ctx.guild.id,
                        feedback_channel_id=channel.id,
                        anonymous=anonymous
                    )
                    session.add(config)

                await ctx.respond(
                    f"Feedback configured! Submissions go to {channel.mention}.\n"
                    f"Anonymous: {'Yes' if anonymous else 'No'}",
                    ephemeral=True
                )

        except Exception as e:
            logger.error(f"Feedback setup error: {e}")
            await ctx.respond("Error configuring feedback!", ephemeral=True)

    # ─── MESSAGE COMMANDS ─────────────────────────────────────────────────────

    @slash_command(name="say", description="Make the bot say something")
    @commands.has_permissions(manage_messages=True)
    @discord.default_permissions(manage_messages=True)
    @option("message", description="What to say")
    @option("channel", description="Target channel", required=False)
    async def say(
        self,
        ctx: discord.ApplicationContext,
        message: str,
        channel: discord.TextChannel = None
    ):
        """Make the bot send a message."""
        target = channel or ctx.channel

        # Replace mentions
        for role in ctx.guild.roles:
            message = message.replace(f"@{role.name}", f"<@&{role.id}>")
        for ch in ctx.guild.channels:
            message = message.replace(f"#{ch.name}", f"<#{ch.id}>")

        await target.send(message)
        await ctx.respond(f"Message sent to {target.mention}!", ephemeral=True)


def setup(bot: commands.Bot):
    bot.add_cog(AdminCog(bot))
