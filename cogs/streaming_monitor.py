# cogs/streaming_monitor.py - YouTube/Twitch Live Stream Monitor
"""
Monitors approved streamers for live status and sends Discord notifications.

This cog periodically checks if approved streamers have gone live on YouTube
or Twitch and sends notifications to configured channels.

ARCHITECTURE:
- Runs every 3 minutes (180 seconds) to check live status
- Only checks creators who are approved streamers in guilds
- Respects minimum level requirements for notifications
- Updates creator_profiles with live stream data
- Sends rich embed notifications to configured channels

FEATURES:
- Auto token refresh when expired
- Rate limit handling
- Per-guild notification configuration
- Minimum level requirements
- Optional role pings
- Live status tracking
"""

import sys
import os
import asyncio
import time as time_lib
from datetime import datetime
from typing import Optional, Dict, Any

import discord
from discord.ext import commands, tasks

# Import bot's config and models first
from config import db_session_scope, logger
from models import (
    CreatorProfile,
    StreamingNotificationsConfig,
    ApprovedStreamer,
    GuildMember
)


class StreamingMonitorCog(commands.Cog):
    """Monitors approved streamers and sends live notifications."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

        # Setup Django and import YouTube service on initialization (not module import)
        if '/srv/ch-webserver' not in sys.path:
            sys.path.insert(0, '/srv/ch-webserver')
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'casualsite.settings')

        # Import and setup Django
        import django
        try:
            django.setup()
        except RuntimeError:
            # Django already setup, ignore
            pass

        # Now we can import Django app services
        from app.services.youtube_service import YouTubeService
        from app.services.twitch_service import TwitchService
        from app.utils.encryption import decrypt_token as dt, encrypt_token as et

        # Store as instance variables
        self.YouTubeService = YouTubeService
        self.TwitchService = TwitchService
        self.decrypt_token = dt
        self.encrypt_token = et

        self.youtube_service = YouTubeService()
        self.twitch_service = TwitchService()
        self.check_interval = 180  # 3 minutes

        # Track last notification time per creator to avoid spam
        self.last_notification = {}  # {creator_profile_id: timestamp}

        # Start the monitoring loop
        self.stream_monitor_loop.start()
        logger.info("StreamingMonitor: Started stream monitoring (3 minute interval)")

    def cog_unload(self):
        """Stop the loop when cog unloads."""
        self.stream_monitor_loop.cancel()
        logger.info("StreamingMonitor: Stopped stream monitoring")

    @tasks.loop(seconds=180)  # Check every 3 minutes
    async def stream_monitor_loop(self):
        """Check all approved streamers for live status."""
        try:
            await self._check_all_streams()
        except Exception as e:
            logger.error(f"StreamingMonitor: Error in monitor loop: {e}", exc_info=True)

    @stream_monitor_loop.before_loop
    async def before_stream_monitor(self):
        """Wait until bot is ready before starting loop."""
        await self.bot.wait_until_ready()
        logger.info("StreamingMonitor: Bot ready, starting stream checks")

    async def _check_all_streams(self):
        """Check all approved streamers across all guilds."""
        with db_session_scope() as db:
            # Get all approved streamers with their creator profiles
            approved = db.query(ApprovedStreamer).filter(
                ApprovedStreamer.revoked == False
            ).all()

            if not approved:
                return  # No approved streamers

            logger.debug(f"StreamingMonitor: Checking {len(approved)} approved streamers")

            # Group by creator to avoid duplicate API calls
            creators_to_check = {}
            for approval in approved:
                creator_id = approval.creator_profile_id
                if creator_id not in creators_to_check:
                    creators_to_check[creator_id] = []
                creators_to_check[creator_id].append(approval)

            # Check each unique creator
            for creator_id, approvals in creators_to_check.items():
                creator = db.query(CreatorProfile).filter(
                    CreatorProfile.id == creator_id
                ).first()

                if not creator:
                    continue

                # Check YouTube status
                if creator.youtube_channel_id and creator.youtube_refresh_token:
                    await self._check_youtube_status(db, creator, approvals)

                # Check Twitch status
                if creator.twitch_user_id and creator.twitch_refresh_token:
                    await self._check_twitch_status(db, creator, approvals)

            db.commit()

    async def _check_youtube_status(
        self,
        db,
        creator: CreatorProfile,
        approvals: list
    ):
        """
        Check YouTube live status for a creator and send notifications.

        Args:
            db: Database session
            creator: CreatorProfile instance
            approvals: List of ApprovedStreamer instances for this creator
        """
        try:
            # Decrypt tokens for API use
            try:
                access_token = self.decrypt_token(creator.youtube_access_token)
                refresh_token = self.decrypt_token(creator.youtube_refresh_token) if creator.youtube_refresh_token else None
            except Exception as e:
                logger.error(f"StreamingMonitor: Failed to decrypt tokens for creator {creator.id}: {e}")
                return

            # Check if token needs refresh
            current_time = int(time_lib.time())
            if creator.youtube_token_expires and creator.youtube_token_expires < current_time + 300:
                # Token expires in less than 5 minutes, refresh it
                try:
                    tokens = self.youtube_service.refresh_access_token(refresh_token)
                    # Store new encrypted token
                    creator.youtube_access_token = self.encrypt_token(tokens.get('access_token'))
                    creator.youtube_token_expires = current_time + tokens.get('expires_in', 3600)
                    db.commit()
                    # Update local variable with new decrypted token
                    access_token = tokens.get('access_token')
                    logger.info(f"StreamingMonitor: Refreshed YouTube token for creator {creator.id}")
                except Exception as e:
                    logger.error(f"StreamingMonitor: Failed to refresh token for creator {creator.id}: {e}")
                    return

            # Check if currently live
            live_info = self.youtube_service.get_live_broadcasts(access_token)

            was_live = creator.is_live_youtube
            is_live = live_info is not None

            # Update creator status
            if is_live:
                # Parse ISO 8601 timestamp to Unix timestamp
                started_at_iso = live_info.get('started_at')
                if started_at_iso:
                    started_dt = datetime.fromisoformat(started_at_iso.replace('Z', '+00:00'))
                    started_at = int(started_dt.timestamp())
                else:
                    started_at = current_time

                creator.is_live_youtube = True
                creator.current_stream_title = live_info.get('title')
                creator.current_stream_game = live_info.get('game_name')
                creator.current_stream_started_at = started_at
                creator.current_stream_thumbnail = live_info.get('thumbnail_url')
                creator.current_stream_viewer_count = live_info.get('viewer_count', 0)
            else:
                # Clear live status
                creator.is_live_youtube = False
                creator.current_stream_title = None
                creator.current_stream_game = None
                creator.current_stream_started_at = None
                creator.current_stream_thumbnail = None
                creator.current_stream_viewer_count = None

            db.commit()

            # Send notifications if just went live (not already live)
            if is_live and not was_live:
                # Check cooldown (don't spam if we just sent notification)
                last_notif = self.last_notification.get(creator.id, 0)
                if current_time - last_notif < 600:  # 10 minute cooldown
                    logger.debug(f"StreamingMonitor: Skipping notification for creator {creator.id} (cooldown)")
                    return

                self.last_notification[creator.id] = current_time

                # Send notification to each guild this creator is approved in
                for approval in approvals:
                    await self._send_live_notification(
                        db,
                        creator,
                        approval.guild_id,
                        live_info
                    )

        except YouTubeAPIError as e:
            logger.error(f"StreamingMonitor: YouTube API error for creator {creator.id}: {e}")
        except Exception as e:
            logger.error(f"StreamingMonitor: Error checking YouTube status for creator {creator.id}: {e}", exc_info=True)

    async def _check_twitch_status(
        self,
        db,
        creator: CreatorProfile,
        approvals: list
    ):
        """
        Check Twitch live status for a creator and send notifications.

        Args:
            db: Database session
            creator: CreatorProfile instance
            approvals: List of ApprovedStreamer instances for this creator
        """
        try:
            # Import TwitchAPIError from the service
            from app.services.twitch_service import TwitchAPIError

            # Decrypt tokens for API use
            try:
                access_token = self.decrypt_token(creator.twitch_access_token)
                refresh_token = self.decrypt_token(creator.twitch_refresh_token) if creator.twitch_refresh_token else None
            except Exception as e:
                logger.error(f"StreamingMonitor: Failed to decrypt Twitch tokens for creator {creator.id}: {e}")
                return

            # Check if token needs refresh
            current_time = int(time_lib.time())
            if creator.twitch_token_expires and creator.twitch_token_expires < current_time + 300:
                # Token expires in less than 5 minutes, refresh it
                try:
                    tokens = self.twitch_service.refresh_access_token(refresh_token)
                    # Store new encrypted token
                    creator.twitch_access_token = self.encrypt_token(tokens.get('access_token'))
                    creator.twitch_token_expires = current_time + tokens.get('expires_in', 3600)
                    db.commit()
                    # Update local variable with new decrypted token
                    access_token = tokens.get('access_token')
                    logger.info(f"StreamingMonitor: Refreshed Twitch token for creator {creator.id}")
                except Exception as e:
                    logger.error(f"StreamingMonitor: Failed to refresh Twitch token for creator {creator.id}: {e}")
                    return

            # Check if currently live
            live_info = self.twitch_service.get_live_streams(access_token, creator.twitch_user_id)

            was_live = creator.is_live_twitch
            is_live = live_info is not None

            # Update creator status
            if is_live:
                creator.is_live_twitch = True
                creator.current_stream_title = live_info.get('title')
                creator.current_stream_game = live_info.get('game_name')
                creator.current_stream_started_at = live_info.get('started_at')
                creator.current_stream_thumbnail = live_info.get('thumbnail_url')
                creator.current_stream_viewer_count = live_info.get('viewer_count', 0)
            else:
                # Clear live status
                creator.is_live_twitch = False
                creator.current_stream_title = None
                creator.current_stream_game = None
                creator.current_stream_started_at = None
                creator.current_stream_thumbnail = None
                creator.current_stream_viewer_count = None

            db.commit()

            # Send notifications if just went live (not already live)
            if is_live and not was_live:
                # Check cooldown (don't spam if we just sent notification)
                # Use a separate key for Twitch to allow both platforms to notify independently
                twitch_key = f"twitch_{creator.id}"
                last_notif = self.last_notification.get(twitch_key, 0)
                if current_time - last_notif < 600:  # 10 minute cooldown
                    logger.debug(f"StreamingMonitor: Skipping Twitch notification for creator {creator.id} (cooldown)")
                    return

                self.last_notification[twitch_key] = current_time

                # Send notification to each guild this creator is approved in
                for approval in approvals:
                    await self._send_twitch_live_notification(
                        db,
                        creator,
                        approval.guild_id,
                        live_info
                    )

        except TwitchAPIError as e:
            logger.error(f"StreamingMonitor: Twitch API error for creator {creator.id}: {e}")
        except Exception as e:
            logger.error(f"StreamingMonitor: Error checking Twitch status for creator {creator.id}: {e}", exc_info=True)

    async def _send_live_notification(
        self,
        db,
        creator: CreatorProfile,
        guild_id: int,
        live_info: Dict[str, Any]
    ):
        """
        Send live notification to a guild's configured channel.

        Args:
            db: Database session
            creator: CreatorProfile instance
            guild_id: Guild ID to send notification to
            live_info: Live stream information dict
        """
        try:
            # Get notification config for this guild
            config = db.query(StreamingNotificationsConfig).filter(
                StreamingNotificationsConfig.guild_id == guild_id
            ).first()

            if not config or not config.enabled:
                return  # Notifications disabled for this guild

            if not config.notification_channel_id:
                logger.warning(f"StreamingMonitor: No notification channel set for guild {guild_id}")
                return

            # Get guild and channel
            guild = self.bot.get_guild(guild_id)
            if not guild:
                logger.warning(f"StreamingMonitor: Guild {guild_id} not found")
                return

            channel = guild.get_channel(config.notification_channel_id)
            if not channel:
                logger.warning(f"StreamingMonitor: Channel {config.notification_channel_id} not found in guild {guild_id}")
                return

            # Check if creator meets minimum level requirement
            if config.minimum_level_required > 0:
                member = db.query(GuildMember).filter(
                    GuildMember.guild_id == guild_id,
                    GuildMember.user_id == creator.discord_id
                ).first()

                if not member or member.level < config.minimum_level_required:
                    logger.debug(
                        f"StreamingMonitor: Creator {creator.id} doesn't meet level requirement "
                        f"({member.level if member else 0} < {config.minimum_level_required}) in guild {guild_id}"
                    )
                    return

            # Get custom notification settings
            notification_title = config.notification_title or '🔴 {creator} is now LIVE!'
            notification_message = config.notification_message or 'Check out the stream!'
            embed_color_hex = config.embed_color or '#FF0000'

            # Replace {creator} placeholder
            notification_title = notification_title.replace('{creator}', creator.display_name)
            notification_message = notification_message.replace('{creator}', creator.display_name)

            # Convert hex color to discord.Color
            embed_color_int = int(embed_color_hex.lstrip('#'), 16)

            # Build notification embed
            embed = discord.Embed(
                title=notification_title,
                description=notification_message,
                color=discord.Color(embed_color_int),
                url=f"https://www.youtube.com/channel/{creator.youtube_channel_id}/live"
            )

            # Add stream title as a field
            embed.add_field(
                name="Stream Title",
                value=live_info.get('title', 'Untitled Stream'),
                inline=False
            )

            # Add game/category if available
            if live_info.get('game_name'):
                embed.add_field(
                    name="Playing",
                    value=live_info['game_name'],
                    inline=True
                )

            # Add viewer count
            viewer_count = live_info.get('viewer_count', 0)
            embed.add_field(
                name="Viewers",
                value=f"{viewer_count:,}",
                inline=True
            )

            # Add thumbnail
            if live_info.get('thumbnail_url'):
                embed.set_image(url=live_info['thumbnail_url'])

            # Add creator avatar
            if creator.avatar_url:
                embed.set_thumbnail(url=creator.avatar_url)

            embed.set_footer(text="YouTube", icon_url="https://www.youtube.com/s/desktop/f506bd45/img/favicon_32.png")
            embed.timestamp = datetime.utcnow()

            # Build message content with optional role ping
            content = None
            if config.ping_role_id:
                role = guild.get_role(config.ping_role_id)
                if role:
                    content = f"{role.mention} - {creator.display_name} is live!"

            # Send notification
            await channel.send(content=content, embed=embed)

            logger.info(
                f"StreamingMonitor: Sent live notification for creator {creator.id} "
                f"({creator.display_name}) to guild {guild_id}"
            )

        except discord.Forbidden:
            logger.error(f"StreamingMonitor: No permission to send notification in guild {guild_id}")
        except Exception as e:
            logger.error(f"StreamingMonitor: Error sending notification to guild {guild_id}: {e}", exc_info=True)

    async def _send_twitch_live_notification(
        self,
        db,
        creator: CreatorProfile,
        guild_id: int,
        live_info: Dict[str, Any]
    ):
        """
        Send Twitch live notification to a guild's configured channel.

        Args:
            db: Database session
            creator: CreatorProfile instance
            guild_id: Guild ID to send notification to
            live_info: Live stream information dict
        """
        try:
            # Get notification config for this guild
            config = db.query(StreamingNotificationsConfig).filter(
                StreamingNotificationsConfig.guild_id == guild_id
            ).first()

            if not config or not config.enabled:
                return  # Notifications disabled for this guild

            if not config.notification_channel_id:
                logger.warning(f"StreamingMonitor: No notification channel set for guild {guild_id}")
                return

            # Get guild and channel
            guild = self.bot.get_guild(guild_id)
            if not guild:
                logger.warning(f"StreamingMonitor: Guild {guild_id} not found")
                return

            channel = guild.get_channel(config.notification_channel_id)
            if not channel:
                logger.warning(f"StreamingMonitor: Channel {config.notification_channel_id} not found in guild {guild_id}")
                return

            # Check if creator meets minimum level requirement
            if config.minimum_level_required > 0:
                member = db.query(GuildMember).filter(
                    GuildMember.guild_id == guild_id,
                    GuildMember.user_id == creator.discord_id
                ).first()

                if not member or member.level < config.minimum_level_required:
                    logger.debug(
                        f"StreamingMonitor: Creator {creator.id} doesn't meet level requirement "
                        f"({member.level if member else 0} < {config.minimum_level_required}) in guild {guild_id}"
                    )
                    return

            # Get custom notification settings
            notification_title = config.notification_title or '🔴 {creator} is now live on Twitch!'
            notification_message = config.notification_message or 'Come watch the stream!'
            embed_color_hex = config.embed_color or '#9147ff'

            # Replace {creator} placeholder
            notification_title = notification_title.replace('{creator}', creator.display_name)
            notification_message = notification_message.replace('{creator}', creator.display_name)

            # Convert hex color to discord.Color
            embed_color_int = int(embed_color_hex.lstrip('#'), 16)

            # Build notification embed with Twitch URL
            twitch_url = f"https://twitch.tv/{creator.twitch_handle}" if creator.twitch_handle else f"https://twitch.tv"
            embed = discord.Embed(
                title=notification_title,
                description=notification_message,
                color=discord.Color(embed_color_int),
                url=twitch_url
            )

            # Add stream title as a field
            embed.add_field(
                name="Stream Title",
                value=live_info.get('title', 'Untitled Stream'),
                inline=False
            )

            # Add game/category if available
            if live_info.get('game_name'):
                embed.add_field(
                    name="Playing",
                    value=live_info['game_name'],
                    inline=True
                )

            # Add viewer count
            viewer_count = live_info.get('viewer_count', 0)
            embed.add_field(
                name="Viewers",
                value=f"{viewer_count:,}",
                inline=True
            )

            # Add thumbnail
            if live_info.get('thumbnail_url'):
                embed.set_image(url=live_info['thumbnail_url'])

            # Add creator avatar
            if creator.avatar_url:
                embed.set_thumbnail(url=creator.avatar_url)

            embed.set_footer(
                text="Twitch",
                icon_url="https://static-cdn.jtvnw.net/jtv_user_pictures/8a6381c7-d0c0-4576-b179-38bd5ce1d6af-profile_image-70x70.png"
            )
            embed.timestamp = datetime.utcnow()

            # Build message content with optional role ping
            content = None
            if config.ping_role_id:
                role = guild.get_role(config.ping_role_id)
                if role:
                    content = f"{role.mention} - {creator.display_name} is live on Twitch!"

            # Send notification
            await channel.send(content=content, embed=embed)

            logger.info(
                f"StreamingMonitor: Sent Twitch live notification for creator {creator.id} "
                f"({creator.display_name}) to guild {guild_id}"
            )

        except discord.Forbidden:
            logger.error(f"StreamingMonitor: No permission to send Twitch notification in guild {guild_id}")
        except Exception as e:
            logger.error(f"StreamingMonitor: Error sending Twitch notification to guild {guild_id}: {e}", exc_info=True)


def setup(bot: commands.Bot):
    """Load the cog."""
    bot.add_cog(StreamingMonitorCog(bot))
