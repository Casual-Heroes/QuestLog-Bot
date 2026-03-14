# warden/cogs/activity_tracker.py
"""
Channel Activity Tracker - Updates channel topics with live member/game stats

Features:
- Track role member counts in channel topics
- Show "currently playing" counts for specific games
- Configurable via slash commands or web dashboard
- Per-guild configuration stored in database
"""

import discord
from discord.ext import commands, tasks
from discord import option
import logging
import time
from typing import Optional
from sqlalchemy import select
from sqlalchemy.orm import Session

from models import ChannelStatTracker, Guild

logger = logging.getLogger(__name__)


class ActivityTrackerCog(commands.Cog):
    """Track role members and game activity in channel topics."""

    def __init__(self, bot):
        self.bot = bot
        self.update_loop.start()

    def cog_unload(self):
        self.update_loop.cancel()

    @tasks.loop(seconds=60)
    async def update_loop(self):
        """Main loop to update all channel topics."""
        try:
            await self._update_all_trackers()
        except Exception as e:
            logger.error(f"[ActivityTracker] Error in update loop: {e}")

    @update_loop.before_loop
    async def before_update_loop(self):
        """Wait for bot to be ready before starting loop."""
        await self.bot.wait_until_ready()
        logger.info("[ActivityTracker] Bot ready, starting update loop")

    async def _update_all_trackers(self):
        """Update all enabled trackers across all guilds."""
        with Session(self.bot.db_engine) as session:
            # Get all enabled trackers
            stmt = select(ChannelStatTracker).where(ChannelStatTracker.enabled == True)
            trackers = session.scalars(stmt).all()

            for tracker in trackers:
                try:
                    await self._update_tracker(tracker, session)
                except Exception as e:
                    logger.error(f"[ActivityTracker] Error updating tracker {tracker.id}: {e}")

            # Update LFG game player counts
            try:
                await self._update_lfg_player_counts(session)
            except Exception as e:
                logger.error(f"[ActivityTracker] Error updating LFG player counts: {e}")

            session.commit()

    async def _update_tracker(self, tracker: ChannelStatTracker, session: Session):
        """Update a single tracker's channel topic."""
        guild = self.bot.get_guild(tracker.guild_id)
        if not guild:
            logger.debug(f"[ActivityTracker] Guild {tracker.guild_id} not found, skipping")
            return

        role = guild.get_role(tracker.role_id)
        channel = guild.get_channel(tracker.channel_id)

        if not role:
            logger.warning(f"[ActivityTracker] [guild_id:{guild.id}] Role {tracker.role_id} not found in guild {guild.name}")
            return

        if not channel:
            logger.warning(f"[ActivityTracker] [guild_id:{guild.id}] Channel {tracker.channel_id} not found in guild {guild.name}")
            return

        # Count role members
        members = role.members
        total_count = len(members)

        # Build topic string
        emoji_str = f"{tracker.emoji} " if tracker.emoji else ""

        if tracker.game_name and tracker.show_playing_count:
            # Count members currently playing the game
            playing_count = 0
            for member in members:
                for activity in member.activities:
                    activity_name = getattr(activity, 'name', None)
                    if activity_name and tracker.game_name.lower() in activity_name.lower():
                        playing_count += 1
                        break

            topic = f"{emoji_str}{tracker.label}: {total_count} members - {playing_count} currently playing"
        else:
            topic = f"{emoji_str}{tracker.label}: {total_count} members"

        # Only update if topic changed (avoid rate limits)
        if topic != tracker.last_topic:
            try:
                await channel.edit(topic=topic)
                tracker.last_topic = topic
                tracker.last_updated = int(time.time())
                logger.info(f"[ActivityTracker] [guild_id:{guild.id}] Updated {channel.name}: {topic}")
            except discord.Forbidden:
                logger.error(f"[ActivityTracker] [guild_id:{guild.id}] No permission to edit channel {channel.name}")
            except discord.HTTPException as e:
                logger.error(f"[ActivityTracker] [guild_id:{guild.id}] HTTP error editing channel: {e}")

    async def _update_lfg_player_counts(self, session: Session):
        """Update player counts for all LFG games (privacy-focused - just counts, no names)."""
        from models import LFGGame

        # Get all LFG games
        stmt = select(LFGGame).where(LFGGame.enabled == True)
        lfg_games = session.scalars(stmt).all()

        logger.debug(f"[ActivityTracker] Checking player counts for {len(lfg_games)} LFG games")

        for game in lfg_games:
            try:
                guild = self.bot.get_guild(game.guild_id)
                if not guild:
                    logger.debug(f"[ActivityTracker] Guild {game.guild_id} not found for game '{game.game_name}'")
                    continue

                # Count members currently playing this game
                playing_count = 0
                total_checked = 0
                matched_activities = []

                for member in guild.members:
                    # Skip bots
                    if member.bot:
                        continue

                    total_checked += 1

                    # Check member's activities
                    for activity in member.activities:
                        activity_name = getattr(activity, 'name', None)
                        if not activity_name:
                            continue

                        # Match game name (case-insensitive)
                        if game.game_name.lower() in activity_name.lower():
                            playing_count += 1
                            matched_activities.append(f"{member.name}: {activity_name}")
                            break

                # Log details for debugging
                logger.debug(
                    f"[ActivityTracker] [guild_id:{guild.id}] Game '{game.game_name}': checked {total_checked} members, found {playing_count} playing"
                )
                count_changed = game.current_player_count != playing_count or game.player_count_updated_at is None
                if matched_activities:
                    if count_changed:
                        logger.info(
                            f"[ActivityTracker] [guild_id:{guild.id}] Matched activities for '{game.game_name}': {matched_activities}"
                        )
                    else:
                        logger.debug(
                            f"[ActivityTracker] [guild_id:{guild.id}] Matched activities for '{game.game_name}': {matched_activities}"
                        )

                # Update database (always update to set player_count_updated_at even if count is 0)
                if game.current_player_count != playing_count or game.player_count_updated_at is None:
                    old_count = game.current_player_count
                    game.current_player_count = playing_count
                    game.player_count_updated_at = int(time.time())
                    logger.info(f"[ActivityTracker] [guild_id:{guild.id}] Updated LFG game '{game.game_name}' in guild {guild.name}: {old_count} -> {playing_count} players")

            except Exception as e:
                logger.error(f"[ActivityTracker] [guild_id:{game.guild_id}] Error updating LFG game {game.id}: {e}", exc_info=True)

    # =========================================================================
    # Slash Commands
    # =========================================================================

    tracker_group = discord.SlashCommandGroup(
        name="tracker",
        description="Configure channel topic stat trackers",
        default_member_permissions=discord.Permissions(manage_channels=True)
    )

    @tracker_group.command(name="add", description="Add a new channel stat tracker")
    @option("channel", discord.TextChannel, description="Channel to update topic", required=True)
    @option("role", discord.Role, description="Role to track member count", required=True)
    @option("label", str, description="Display label (e.g., 'Pantheon Heroes')", required=True)
    @option("emoji", str, description="Emoji to show before label", required=False)
    @option("game_name", str, description="Game name to track (for 'currently playing')", required=False)
    async def tracker_add(
        self,
        ctx: discord.ApplicationContext,
        channel: discord.TextChannel,
        role: discord.Role,
        label: str,
        emoji: Optional[str] = None,
        game_name: Optional[str] = None
    ):
        """Add a new channel stat tracker."""
        await ctx.defer()

        with Session(self.bot.db_engine) as session:
            # Check if tracker already exists for this channel
            existing = session.scalar(
                select(ChannelStatTracker).where(
                    ChannelStatTracker.guild_id == ctx.guild.id,
                    ChannelStatTracker.channel_id == channel.id
                )
            )

            if existing:
                await ctx.respond(
                    f"A tracker already exists for {channel.mention}. Use `/tracker edit` to modify it.",
                    ephemeral=True
                )
                return

            # Create new tracker
            tracker = ChannelStatTracker(
                guild_id=ctx.guild.id,
                channel_id=channel.id,
                role_id=role.id,
                label=label,
                emoji=emoji,
                game_name=game_name,
                show_playing_count=bool(game_name),
                enabled=True,
                created_by=ctx.author.id
            )

            session.add(tracker)
            session.commit()

            # Build preview
            preview = f"{emoji} " if emoji else ""
            if game_name:
                preview += f"{label}: X members — Y currently playing"
            else:
                preview += f"{label}: X members"

            embed = discord.Embed(
                title="Tracker Created",
                color=discord.Color.green(),
                description=f"Channel topic stats tracker has been set up."
            )
            embed.add_field(name="Channel", value=channel.mention, inline=True)
            embed.add_field(name="Tracking Role", value=role.mention, inline=True)
            embed.add_field(name="Game Filter", value=game_name or "None", inline=True)
            embed.add_field(name="Preview", value=f"`{preview}`", inline=False)
            embed.set_footer(text="Topic will update within 60 seconds")

            await ctx.respond(embed=embed)

            # Trigger immediate update
            await self._update_tracker(tracker, session)

    @tracker_group.command(name="remove", description="Remove a channel stat tracker")
    @option("channel", discord.TextChannel, description="Channel with tracker to remove", required=True)
    async def tracker_remove(
        self,
        ctx: discord.ApplicationContext,
        channel: discord.TextChannel
    ):
        """Remove a channel stat tracker."""
        with Session(self.bot.db_engine) as session:
            tracker = session.scalar(
                select(ChannelStatTracker).where(
                    ChannelStatTracker.guild_id == ctx.guild.id,
                    ChannelStatTracker.channel_id == channel.id
                )
            )

            if not tracker:
                await ctx.respond(
                    f"No tracker found for {channel.mention}.",
                    ephemeral=True
                )
                return

            session.delete(tracker)
            session.commit()

            await ctx.respond(
                f"Removed stat tracker from {channel.mention}.",
                ephemeral=True
            )

    @tracker_group.command(name="list", description="List all channel stat trackers")
    async def tracker_list(self, ctx: discord.ApplicationContext):
        """List all trackers for this guild."""
        with Session(self.bot.db_engine) as session:
            trackers = session.scalars(
                select(ChannelStatTracker).where(
                    ChannelStatTracker.guild_id == ctx.guild.id
                )
            ).all()

            if not trackers:
                await ctx.respond(
                    "No stat trackers configured. Use `/tracker add` to create one.",
                    ephemeral=True
                )
                return

            embed = discord.Embed(
                title="Channel Stat Trackers",
                color=discord.Color.blurple(),
                description=f"**{len(trackers)}** tracker(s) configured"
            )

            for tracker in trackers:
                channel = ctx.guild.get_channel(tracker.channel_id)
                role = ctx.guild.get_role(tracker.role_id)

                channel_name = channel.mention if channel else f"Unknown ({tracker.channel_id})"
                role_name = role.mention if role else f"Unknown ({tracker.role_id})"

                status = "Enabled" if tracker.enabled else "Disabled"
                game_info = f" | Game: `{tracker.game_name}`" if tracker.game_name else ""

                embed.add_field(
                    name=f"{tracker.emoji or ''} {tracker.label}",
                    value=f"Channel: {channel_name}\nRole: {role_name}\nStatus: {status}{game_info}",
                    inline=False
                )

            await ctx.respond(embed=embed)

    @tracker_group.command(name="toggle", description="Enable or disable a tracker")
    @option("channel", discord.TextChannel, description="Channel with tracker", required=True)
    @option("enabled", bool, description="Enable or disable", required=True)
    async def tracker_toggle(
        self,
        ctx: discord.ApplicationContext,
        channel: discord.TextChannel,
        enabled: bool
    ):
        """Toggle a tracker on or off."""
        with Session(self.bot.db_engine) as session:
            tracker = session.scalar(
                select(ChannelStatTracker).where(
                    ChannelStatTracker.guild_id == ctx.guild.id,
                    ChannelStatTracker.channel_id == channel.id
                )
            )

            if not tracker:
                await ctx.respond(
                    f"No tracker found for {channel.mention}.",
                    ephemeral=True
                )
                return

            tracker.enabled = enabled
            session.commit()

            status = "enabled" if enabled else "disabled"
            await ctx.respond(
                f"Tracker for {channel.mention} has been **{status}**.",
                ephemeral=True
            )

    @tracker_group.command(name="edit", description="Edit a tracker's settings")
    @option("channel", discord.TextChannel, description="Channel with tracker to edit", required=True)
    @option("label", str, description="New display label", required=False)
    @option("emoji", str, description="New emoji", required=False)
    @option("role", discord.Role, description="New role to track", required=False)
    @option("game_name", str, description="Game name to track (or 'none' to disable)", required=False)
    async def tracker_edit(
        self,
        ctx: discord.ApplicationContext,
        channel: discord.TextChannel,
        label: Optional[str] = None,
        emoji: Optional[str] = None,
        role: Optional[discord.Role] = None,
        game_name: Optional[str] = None
    ):
        """Edit an existing tracker."""
        with Session(self.bot.db_engine) as session:
            tracker = session.scalar(
                select(ChannelStatTracker).where(
                    ChannelStatTracker.guild_id == ctx.guild.id,
                    ChannelStatTracker.channel_id == channel.id
                )
            )

            if not tracker:
                await ctx.respond(
                    f"No tracker found for {channel.mention}.",
                    ephemeral=True
                )
                return

            changes = []

            if label:
                tracker.label = label
                changes.append(f"Label → `{label}`")

            if emoji:
                tracker.emoji = emoji
                changes.append(f"Emoji → {emoji}")

            if role:
                tracker.role_id = role.id
                changes.append(f"Role → {role.mention}")

            if game_name:
                if game_name.lower() == "none":
                    tracker.game_name = None
                    tracker.show_playing_count = False
                    changes.append("Game tracking → Disabled")
                else:
                    tracker.game_name = game_name
                    tracker.show_playing_count = True
                    changes.append(f"Game → `{game_name}`")

            if not changes:
                await ctx.respond(
                    "No changes specified. Use options to modify the tracker.",
                    ephemeral=True
                )
                return

            # Reset last_topic to force update
            tracker.last_topic = None
            session.commit()

            await ctx.respond(
                f"Updated tracker for {channel.mention}:\n" + "\n".join(f"• {c}" for c in changes),
                ephemeral=True
            )

    @tracker_group.command(name="refresh", description="Force refresh all trackers")
    async def tracker_refresh(self, ctx: discord.ApplicationContext):
        """Force an immediate refresh of all trackers."""
        await ctx.defer(ephemeral=True)

        try:
            await self._update_all_trackers()
            await ctx.respond(
                "All trackers have been refreshed.",
                ephemeral=True
            )
        except Exception as e:
            await ctx.respond(
                f"Error refreshing trackers: {e}",
                ephemeral=True
            )


def setup(bot):
    bot.add_cog(ActivityTrackerCog(bot))
