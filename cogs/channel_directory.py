# cogs/channel_directory.py - Automatic public channel directory
"""Maintain a public channel directory grouped by Discord category."""

import asyncio
import re

import discord
from discord.ext import commands

from config import logger


DIRECTORY_CHANNEL_NAME = "channel-directory"
DIRECTORY_TITLE = "📚 Channel Directory"
DIRECTORY_FOOTER = "QuestLog Channel Directory | Updates automatically"
DIRECTORY_DEBOUNCE_SECONDS = 3
MAX_FIELD_LENGTH = 1024
MAX_EMBED_LENGTH = 5600


class ChannelDirectoryCog(commands.Cog):
    """Keep a category-based directory of channels visible to everyone."""

    directory = discord.SlashCommandGroup(
        name="directory",
        description="Channel directory commands",
    )

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._directory_message_ids = {}
        self._refresh_tasks = {}
        self._startup_task = None

    def cog_unload(self):
        if self._startup_task:
            self._startup_task.cancel()
        for task in self._refresh_tasks.values():
            task.cancel()

    @staticmethod
    def _normalized_channel_name(name: str) -> str:
        return name.strip().lower().replace("_", "-").replace(" ", "-")

    def _find_directory_channel(self, guild: discord.Guild):
        for channel in guild.text_channels:
            if self._normalized_channel_name(channel.name) == DIRECTORY_CHANNEL_NAME:
                return channel
        return None

    @staticmethod
    def _is_supported_channel(channel) -> bool:
        return isinstance(channel, (
            discord.TextChannel,
            discord.VoiceChannel,
            discord.ForumChannel,
            discord.StageChannel,
        ))

    @staticmethod
    def _is_public_channel(channel, guild: discord.Guild) -> bool:
        """Return whether @everyone can see the channel."""
        return channel.permissions_for(guild.default_role).view_channel

    @staticmethod
    def _channel_description(channel) -> str:
        topic = getattr(channel, "topic", None)
        if topic:
            # Topics can contain newlines and very long link previews. Keep the
            # directory compact while preserving the useful text.
            description = re.sub(r"\s+", " ", topic).strip()
            if len(description) > 240:
                return description[:237].rstrip() + "..."
            return description
        if isinstance(channel, (discord.VoiceChannel, discord.StageChannel)):
            return "Voice channel"
        return "No description set"

    @classmethod
    def _channel_line(cls, channel) -> str:
        return f"{channel.mention} - {cls._channel_description(channel)}"

    @staticmethod
    def _split_lines(lines, limit=MAX_FIELD_LENGTH):
        """Split lines into chunks that fit in one Discord embed field."""
        chunks = []
        current = []
        current_length = 0

        for line in lines:
            line = line[:limit]
            added_length = len(line) + (1 if current else 0)
            if current and current_length + added_length > limit:
                chunks.append("\n".join(current))
                current = [line]
                current_length = len(line)
            else:
                current.append(line)
                current_length += added_length

        if current:
            chunks.append("\n".join(current))
        return chunks

    def _directory_fields(self, guild: discord.Guild, directory_channel):
        """Build ordered embed fields from public guild channels."""
        fields = []

        uncategorized = [
            channel
            for channel in guild.channels
            if channel.category is None
            and not isinstance(channel, discord.CategoryChannel)
            and channel.id != directory_channel.id
            and self._is_supported_channel(channel)
            and self._is_public_channel(channel, guild)
        ]
        uncategorized.sort(key=lambda channel: (channel.position, channel.id))
        if uncategorized:
            for index, value in enumerate(self._split_lines([
                self._channel_line(channel) for channel in uncategorized
            ])):
                name = "Other Channels" if index == 0 else "Other Channels (continued)"
                fields.append((name, value))

        for category in sorted(guild.categories, key=lambda item: (item.position, item.id)):
            public_channels = [
                channel
                for channel in category.channels
                if channel.id != directory_channel.id
                and self._is_supported_channel(channel)
                and self._is_public_channel(channel, guild)
            ]
            if not public_channels:
                continue

            public_channels.sort(key=lambda channel: (channel.position, channel.id))
            chunks = self._split_lines([
                self._channel_line(channel) for channel in public_channels
            ])
            for index, value in enumerate(chunks):
                name = category.name if index == 0 else f"{category.name} (continued)"
                fields.append((name, value))

        return fields

    @staticmethod
    def _pack_fields(fields):
        """Pack fields into groups that stay within Discord embed limits."""
        pages = []
        current = []
        current_length = 0

        for name, value in fields:
            field_length = len(name) + len(value)
            if current and (
                len(current) >= 25
                or current_length + field_length > MAX_EMBED_LENGTH
            ):
                pages.append(current)
                current = []
                current_length = 0
            current.append((name, value))
            current_length += field_length

        if current:
            pages.append(current)
        return pages

    def _build_embeds(self, guild: discord.Guild, directory_channel):
        fields = self._directory_fields(guild, directory_channel)
        pages = self._pack_fields(fields) or [[]]
        embeds = []

        for page_number, page in enumerate(pages[:10]):
            title = DIRECTORY_TITLE
            if page_number:
                title += f" (continued {page_number + 1})"
            embed = discord.Embed(
                title=title,
                description=(
                    "Browse the public channels below. Channel descriptions "
                    "come from each channel's topic."
                    if page_number == 0 else None
                ),
                color=discord.Color.blurple(),
                timestamp=discord.utils.utcnow(),
            )
            for name, value in page:
                embed.add_field(name=name, value=value, inline=False)
            if not page and page_number == 0:
                embed.add_field(
                    name="No public channels found",
                    value="Public channels will appear here automatically.",
                    inline=False,
                )
            embed.set_footer(text=DIRECTORY_FOOTER)
            embeds.append(embed)

        return embeds

    async def _find_directory_message(self, channel: discord.TextChannel):
        message_id = self._directory_message_ids.get(channel.guild.id)
        if message_id:
            try:
                return await channel.fetch_message(message_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                self._directory_message_ids.pop(channel.guild.id, None)

        try:
            async for message in channel.history(limit=50):
                if message.author.id != self.bot.user.id:
                    continue
                if any(
                    embed.footer
                    and embed.footer.text == DIRECTORY_FOOTER
                    for embed in message.embeds
                ):
                    self._directory_message_ids[channel.guild.id] = message.id
                    return message
        except (discord.Forbidden, discord.HTTPException):
            pass
        return None

    async def refresh_directory(self, guild: discord.Guild) -> bool:
        """Create or update a guild's directory message."""
        channel = self._find_directory_channel(guild)
        if not channel:
            return False

        embeds = self._build_embeds(guild, channel)
        try:
            message = await self._find_directory_message(channel)
            if message:
                await message.edit(content=None, embeds=embeds)
            else:
                message = await channel.send(embeds=embeds)
                self._directory_message_ids[guild.id] = message.id
            logger.info(f"Updated channel directory for {guild.name} ({guild.id})")
            return True
        except discord.Forbidden:
            logger.warning(
                f"Cannot update #{channel.name} in {guild.name}; "
                "check View Channel, Send Messages, and Embed Links permissions"
            )
        except discord.HTTPException as exc:
            logger.error(f"Could not update channel directory for {guild.id}: {exc}")
        return False

    def _schedule_refresh(self, guild: discord.Guild):
        existing = self._refresh_tasks.get(guild.id)
        if existing and not existing.done():
            existing.cancel()
        self._refresh_tasks[guild.id] = asyncio.create_task(
            self._delayed_refresh(guild.id)
        )

    async def _delayed_refresh(self, guild_id: int):
        try:
            await asyncio.sleep(DIRECTORY_DEBOUNCE_SECONDS)
            guild = self.bot.get_guild(guild_id)
            if guild:
                await self.refresh_directory(guild)
        except asyncio.CancelledError:
            return

    async def _refresh_all_directories(self):
        try:
            for guild in self.bot.guilds:
                if self._find_directory_channel(guild):
                    await self.refresh_directory(guild)
                    await asyncio.sleep(1)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(f"Could not initialize channel directories: {exc}")

    @commands.Cog.listener()
    async def on_ready(self):
        if not self._startup_task or self._startup_task.done():
            self._startup_task = asyncio.create_task(self._refresh_all_directories())

    @commands.Cog.listener()
    async def on_guild_channel_create(self, channel: discord.abc.GuildChannel):
        self._schedule_refresh(channel.guild)

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel):
        self._schedule_refresh(channel.guild)

    @commands.Cog.listener()
    async def on_guild_channel_update(self, before, after):
        self._schedule_refresh(after.guild)

    @commands.Cog.listener()
    async def on_guild_role_update(self, before: discord.Role, after: discord.Role):
        if after.is_default():
            self._schedule_refresh(after.guild)

    @directory.command(name="refresh", description="Refresh the public channel directory")
    @discord.default_permissions(manage_channels=True)
    @commands.has_permissions(manage_channels=True)
    async def directory_refresh(self, ctx: discord.ApplicationContext):
        await ctx.defer(ephemeral=True)
        if not self._find_directory_channel(ctx.guild):
            await ctx.respond(
                "Create a text channel named `channel-directory` first.",
                ephemeral=True,
            )
            return

        updated = await self.refresh_directory(ctx.guild)
        if updated:
            await ctx.respond("Channel directory refreshed.", ephemeral=True)
        else:
            await ctx.respond(
                "I could not update the directory. Check my channel permissions.",
                ephemeral=True,
            )


def setup(bot: commands.Bot):
    bot.add_cog(ChannelDirectoryCog(bot))
