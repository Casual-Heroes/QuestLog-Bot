# cogs/streaming_monitor.py - YouTube/Twitch Live Stream Monitor
"""
Monitors approved streamers for live status and sends Discord notifications.

This cog periodically checks if approved streamers have gone live on YouTube
or Twitch and sends notifications to configured channels.

ARCHITECTURE:
- Runs every 3 minutes (180 seconds) to check live status
- Only checks creators who are approved streamers in guilds with Discovery module
- Respects minimum level requirements for notifications
- Updates creator_profiles with live stream data
- Sends rich embed notifications to configured channels
- Uses database-backed notification history to survive restarts

SECURITY FEATURES:
- Module access checks (Discovery module or Complete tier required)
- Encrypted token storage and handling
- Exponential backoff for API failures
- Rate limiting per creator per platform
- Database-backed deduplication to prevent spam
- Graceful error handling with detailed logging

FEATURES:
- Auto token refresh when expired (with 5-minute buffer)
- Per-guild notification configuration
- Minimum level requirements
- Optional role pings
- Live status tracking in database
- Historical notification tracking (survives restarts)
"""

import sys
import os
import asyncio
import time as time_lib
from datetime import datetime
from typing import Optional, Dict, Any, Set, Tuple
from collections import defaultdict

import discord
from discord.ext import commands, tasks

# Import bot's config and models first
from config import db_session_scope, logger
from models import (
    CreatorProfile,
    StreamingNotificationsConfig,
    ApprovedStreamer,
    GuildMember,
    Guild,
    GuildModule,
    StreamNotificationHistory,
)


class StreamingMonitorCog(commands.Cog):
    """Monitors approved streamers and sends live notifications."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

        # Setup Django and import services on initialization (not module import)
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
        from app.services.youtube_service import YouTubeService, YouTubeAPIError
        from app.services.twitch_service import TwitchService, TwitchAPIError
        from app.utils.encryption import decrypt_token as dt, encrypt_token as et

        # Store as instance variables
        self.YouTubeService = YouTubeService
        self.TwitchService = TwitchService
        self.YouTubeAPIError = YouTubeAPIError
        self.TwitchAPIError = TwitchAPIError
        self.decrypt_token = dt
        self.encrypt_token = et

        self.youtube_service = YouTubeService()
        self.twitch_service = TwitchService()
        self.check_interval = 180  # 3 minutes

        # Track last notification time per creator to avoid spam (in-memory cooldown)
        # Key format: "youtube_{creator_id}" or "twitch_{creator_id}"
        self.last_notification: Dict[str, int] = {}

        # Track API failure counts for exponential backoff
        # Key: creator_id, Value: (failure_count, last_failure_time)
        self.api_failures: Dict[int, Tuple[int, int]] = {}

        # Maximum backoff: 30 minutes (1800 seconds)
        self.max_backoff_seconds = 1800
        # Base backoff: 3 minutes (180 seconds - same as check interval)
        self.base_backoff_seconds = 180

        # Notification cooldown (10 minutes)
        self.notification_cooldown_seconds = 600

        # Start the monitoring loop
        self.stream_monitor_loop.start()
        logger.info("StreamingMonitor: Started stream monitoring (3 minute interval)")

    def cog_unload(self):
        """Stop the loop when cog unloads."""
        self.stream_monitor_loop.cancel()
        logger.info("StreamingMonitor: Stopped stream monitoring")

    def _has_discovery_access(self, db, guild_id: int) -> bool:
        """
        Check if guild has access to Discovery features (streaming notifications).

        Args:
            db: Database session
            guild_id: Guild ID to check

        Returns:
            True if guild has Discovery module, Complete tier, or VIP status
        """
        guild = db.query(Guild).filter(Guild.guild_id == guild_id).first()
        if not guild:
            return False

        # VIP or Complete tier always has access
        if guild.is_vip or guild.subscription_tier == 'complete':
            return True

        # Check for Discovery module
        has_discovery_module = db.query(GuildModule).filter_by(
            guild_id=guild_id,
            module_name='discovery',
            enabled=True
        ).first() is not None

        return has_discovery_module

    def _should_skip_creator(self, creator_id: int) -> bool:
        """
        Check if we should skip this creator due to API failures (exponential backoff).

        Args:
            creator_id: Creator profile ID

        Returns:
            True if creator should be skipped due to recent failures
        """
        if creator_id not in self.api_failures:
            return False

        failure_count, last_failure_time = self.api_failures[creator_id]
        current_time = int(time_lib.time())

        # Calculate backoff time: base * 2^(failures-1), capped at max
        backoff_seconds = min(
            self.base_backoff_seconds * (2 ** (failure_count - 1)),
            self.max_backoff_seconds
        )

        if current_time - last_failure_time < backoff_seconds:
            logger.debug(
                f"StreamingMonitor: Skipping creator {creator_id} due to backoff "
                f"({failure_count} failures, {backoff_seconds}s backoff)"
            )
            return True

        return False

    def _record_api_failure(self, creator_id: int):
        """Record an API failure for exponential backoff."""
        current_time = int(time_lib.time())
        if creator_id in self.api_failures:
            failure_count, _ = self.api_failures[creator_id]
            self.api_failures[creator_id] = (failure_count + 1, current_time)
        else:
            self.api_failures[creator_id] = (1, current_time)

    def _clear_api_failure(self, creator_id: int):
        """Clear API failure record on successful call."""
        if creator_id in self.api_failures:
            del self.api_failures[creator_id]

    def _was_notification_sent(
        self,
        db,
        guild_id: int,
        creator_id: int,
        platform: str,
        stream_started_at: int
    ) -> bool:
        """
        Check if we already sent a notification for this stream (survives restarts).

        Args:
            db: Database session
            guild_id: Guild to check
            creator_id: Creator profile ID
            platform: 'youtube' or 'twitch'
            stream_started_at: Unix timestamp when stream started

        Returns:
            True if notification was already sent
        """
        existing = db.query(StreamNotificationHistory).filter(
            StreamNotificationHistory.guild_id == guild_id,
            StreamNotificationHistory.creator_profile_id == creator_id,
            StreamNotificationHistory.platform == platform,
            StreamNotificationHistory.stream_started_at == stream_started_at
        ).first()

        return existing is not None

    def _record_notification_sent(
        self,
        db,
        guild_id: int,
        creator_id: int,
        platform: str,
        stream_started_at: int,
        stream_title: Optional[str] = None
    ):
        """
        Record that we sent a notification for this stream.

        Args:
            db: Database session
            guild_id: Guild ID
            creator_id: Creator profile ID
            platform: 'youtube' or 'twitch'
            stream_started_at: Unix timestamp when stream started
            stream_title: Optional stream title for logging
        """
        try:
            notification = StreamNotificationHistory(
                guild_id=guild_id,
                creator_profile_id=creator_id,
                platform=platform,
                stream_started_at=stream_started_at,
                notified_at=int(time_lib.time()),
                stream_title=stream_title[:500] if stream_title else None
            )
            db.add(notification)
            db.commit()
        except Exception as e:
            # Unique constraint violation means we already recorded it
            db.rollback()
            logger.debug(f"StreamingMonitor: Notification already recorded: {e}")

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
        # Wait a bit more for guilds to populate
        await asyncio.sleep(10)
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
            # Also track which guilds each creator is approved in
            creators_to_check: Dict[int, list] = {}
            for approval in approved:
                # Check if this guild has Discovery access
                if not self._has_discovery_access(db, approval.guild_id):
                    continue

                creator_id = approval.creator_profile_id
                if creator_id not in creators_to_check:
                    creators_to_check[creator_id] = []
                creators_to_check[creator_id].append(approval)

            # Check each unique creator
            for creator_id, approvals in creators_to_check.items():
                # Check exponential backoff
                if self._should_skip_creator(creator_id):
                    continue

                creator = db.query(CreatorProfile).filter(
                    CreatorProfile.id == creator_id
                ).first()

                if not creator:
                    continue

                # Check YouTube status
                if creator.youtube_channel_id and creator.youtube_refresh_token:
                    await self._check_youtube_status(db, creator, approvals)
                    # Small delay between API calls to avoid rate limits
                    await asyncio.sleep(0.5)

                # Check Twitch status
                if creator.twitch_user_id and creator.twitch_refresh_token:
                    await self._check_twitch_status(db, creator, approvals)
                    await asyncio.sleep(0.5)

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

                if not access_token or not refresh_token:
                    logger.warning(f"StreamingMonitor: Missing YouTube tokens for creator {creator.id}")
                    return
            except Exception as e:
                logger.error(f"StreamingMonitor: Failed to decrypt YouTube tokens for creator {creator.id}: {e}")
                return

            # Check if token needs refresh (5 minute buffer)
            current_time = int(time_lib.time())
            if creator.youtube_token_expires and creator.youtube_token_expires < current_time + 300:
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
                    logger.error(f"StreamingMonitor: Failed to refresh YouTube token for creator {creator.id}: {e}")
                    self._record_api_failure(creator.id)
                    return

            # Check if currently live
            try:
                live_info = self.youtube_service.get_live_broadcasts(
                    access_token,
                    channel_id=creator.youtube_channel_id
                )
                # Clear failure record on success
                self._clear_api_failure(creator.id)
            except self.YouTubeAPIError as e:
                logger.error(f"StreamingMonitor: YouTube API error for creator {creator.id}: {e}")
                self._record_api_failure(creator.id)
                return

            was_live = creator.is_live_youtube
            is_live = live_info is not None

            # Parse stream start time
            stream_started_at = None
            if is_live:
                started_at_iso = live_info.get('started_at')
                if started_at_iso:
                    try:
                        started_dt = datetime.fromisoformat(started_at_iso.replace('Z', '+00:00'))
                        stream_started_at = int(started_dt.timestamp())
                    except (ValueError, TypeError):
                        stream_started_at = current_time
                else:
                    stream_started_at = current_time

            # Update creator status in database
            if is_live:
                creator.is_live_youtube = True
                creator.current_stream_title = live_info.get('title')
                creator.current_stream_game = live_info.get('game_name')
                creator.current_stream_started_at = stream_started_at
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

            # Send notifications if just went live
            if is_live and not was_live and stream_started_at:
                # Check in-memory cooldown
                cooldown_key = f"youtube_{creator.id}"
                last_notif = self.last_notification.get(cooldown_key, 0)
                if current_time - last_notif < self.notification_cooldown_seconds:
                    logger.debug(f"StreamingMonitor: Skipping YouTube notification for creator {creator.id} (cooldown)")
                    return

                self.last_notification[cooldown_key] = current_time

                # Send notification to each guild this creator is approved in
                for approval in approvals:
                    # Check database-backed deduplication (survives restarts)
                    if self._was_notification_sent(
                        db, approval.guild_id, creator.id, 'youtube', stream_started_at
                    ):
                        logger.debug(
                            f"StreamingMonitor: Skipping YouTube notification for creator {creator.id} "
                            f"in guild {approval.guild_id} (already sent)"
                        )
                        continue

                    await self._send_youtube_notification(
                        db,
                        creator,
                        approval.guild_id,
                        live_info,
                        stream_started_at
                    )

        except Exception as e:
            logger.error(f"StreamingMonitor: Error checking YouTube status for creator {creator.id}: {e}", exc_info=True)
            self._record_api_failure(creator.id)

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
            # Decrypt tokens for API use
            try:
                access_token = self.decrypt_token(creator.twitch_access_token)
                refresh_token = self.decrypt_token(creator.twitch_refresh_token) if creator.twitch_refresh_token else None

                if not access_token or not refresh_token:
                    logger.warning(f"StreamingMonitor: Missing Twitch tokens for creator {creator.id}")
                    return
            except Exception as e:
                logger.error(f"StreamingMonitor: Failed to decrypt Twitch tokens for creator {creator.id}: {e}")
                return

            # Check if token needs refresh (5 minute buffer)
            current_time = int(time_lib.time())
            if creator.twitch_token_expires and creator.twitch_token_expires < current_time + 300:
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
                    self._record_api_failure(creator.id)
                    return

            # Check if currently live
            try:
                live_info = self.twitch_service.get_live_streams(access_token, creator.twitch_user_id)
                # Clear failure record on success
                self._clear_api_failure(creator.id)
            except self.TwitchAPIError as e:
                logger.error(f"StreamingMonitor: Twitch API error for creator {creator.id}: {e}")
                self._record_api_failure(creator.id)
                return

            was_live = creator.is_live_twitch
            is_live = live_info is not None

            # Get stream start time
            stream_started_at = None
            if is_live:
                stream_started_at = live_info.get('started_at')
                if not stream_started_at:
                    stream_started_at = current_time

            # Update creator status in database
            if is_live:
                creator.is_live_twitch = True
                creator.current_stream_title = live_info.get('title')
                creator.current_stream_game = live_info.get('game_name')
                creator.current_stream_started_at = stream_started_at
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

            # Send notifications if just went live
            if is_live and not was_live and stream_started_at:
                # Check in-memory cooldown (separate key for Twitch)
                cooldown_key = f"twitch_{creator.id}"
                last_notif = self.last_notification.get(cooldown_key, 0)
                if current_time - last_notif < self.notification_cooldown_seconds:
                    logger.debug(f"StreamingMonitor: Skipping Twitch notification for creator {creator.id} (cooldown)")
                    return

                self.last_notification[cooldown_key] = current_time

                # Send notification to each guild this creator is approved in
                for approval in approvals:
                    # Check database-backed deduplication (survives restarts)
                    if self._was_notification_sent(
                        db, approval.guild_id, creator.id, 'twitch', stream_started_at
                    ):
                        logger.debug(
                            f"StreamingMonitor: Skipping Twitch notification for creator {creator.id} "
                            f"in guild {approval.guild_id} (already sent)"
                        )
                        continue

                    await self._send_twitch_notification(
                        db,
                        creator,
                        approval.guild_id,
                        live_info,
                        stream_started_at
                    )

        except Exception as e:
            logger.error(f"StreamingMonitor: Error checking Twitch status for creator {creator.id}: {e}", exc_info=True)
            self._record_api_failure(creator.id)

    async def _send_youtube_notification(
        self,
        db,
        creator: CreatorProfile,
        guild_id: int,
        live_info: Dict[str, Any],
        stream_started_at: int
    ):
        """
        Send YouTube live notification to a guild's configured channel.

        Args:
            db: Database session
            creator: CreatorProfile instance
            guild_id: Guild ID to send notification to
            live_info: Live stream information dict
            stream_started_at: Unix timestamp when stream started
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
            try:
                embed_color_int = int(embed_color_hex.lstrip('#'), 16)
            except ValueError:
                embed_color_int = 0xFF0000  # Default red

            # Build notification embed
            embed = discord.Embed(
                title=notification_title,
                description=notification_message,
                color=discord.Color(embed_color_int),
                url=f"https://www.youtube.com/channel/{creator.youtube_channel_id}/live"
            )

            # Add stream title as a field
            stream_title = live_info.get('title', 'Untitled Stream')
            embed.add_field(
                name="Stream Title",
                value=stream_title[:1024],  # Discord field value limit
                inline=False
            )

            # Add game/category if available
            if live_info.get('game_name'):
                embed.add_field(
                    name="Playing",
                    value=live_info['game_name'][:1024],
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
                text="YouTube",
                icon_url="https://www.youtube.com/s/desktop/f506bd45/img/favicon_32.png"
            )
            embed.timestamp = datetime.utcnow()

            # Build message content with optional role ping
            content = None
            if config.ping_role_id:
                role = guild.get_role(config.ping_role_id)
                if role:
                    content = f"{role.mention} - {creator.display_name} is live!"

            # Send notification
            await channel.send(content=content, embed=embed)

            # Record notification in database (for restart survival)
            self._record_notification_sent(
                db, guild_id, creator.id, 'youtube', stream_started_at, stream_title
            )

            logger.info(
                f"StreamingMonitor: Sent YouTube notification for creator {creator.id} "
                f"({creator.display_name}) to guild {guild_id}"
            )

        except discord.Forbidden:
            logger.error(f"StreamingMonitor: No permission to send YouTube notification in guild {guild_id}")
        except Exception as e:
            logger.error(f"StreamingMonitor: Error sending YouTube notification to guild {guild_id}: {e}", exc_info=True)

    async def _send_twitch_notification(
        self,
        db,
        creator: CreatorProfile,
        guild_id: int,
        live_info: Dict[str, Any],
        stream_started_at: int
    ):
        """
        Send Twitch live notification to a guild's configured channel.

        Args:
            db: Database session
            creator: CreatorProfile instance
            guild_id: Guild ID to send notification to
            live_info: Live stream information dict
            stream_started_at: Unix timestamp when stream started
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
            try:
                embed_color_int = int(embed_color_hex.lstrip('#'), 16)
            except ValueError:
                embed_color_int = 0x9147ff  # Default Twitch purple

            # Build notification embed with Twitch URL
            twitch_url = f"https://twitch.tv/{creator.twitch_handle}" if creator.twitch_handle else "https://twitch.tv"
            embed = discord.Embed(
                title=notification_title,
                description=notification_message,
                color=discord.Color(embed_color_int),
                url=twitch_url
            )

            # Add stream title as a field
            stream_title = live_info.get('title', 'Untitled Stream')
            embed.add_field(
                name="Stream Title",
                value=stream_title[:1024],  # Discord field value limit
                inline=False
            )

            # Add game/category if available
            if live_info.get('game_name'):
                embed.add_field(
                    name="Playing",
                    value=live_info['game_name'][:1024],
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

            # Record notification in database (for restart survival)
            self._record_notification_sent(
                db, guild_id, creator.id, 'twitch', stream_started_at, stream_title
            )

            logger.info(
                f"StreamingMonitor: Sent Twitch notification for creator {creator.id} "
                f"({creator.display_name}) to guild {guild_id}"
            )

        except discord.Forbidden:
            logger.error(f"StreamingMonitor: No permission to send Twitch notification in guild {guild_id}")
        except Exception as e:
            logger.error(f"StreamingMonitor: Error sending Twitch notification to guild {guild_id}: {e}", exc_info=True)


def setup(bot: commands.Bot):
    """Load the cog."""
    bot.add_cog(StreamingMonitorCog(bot))
