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

        # Start the monitoring loops
        self.stream_monitor_loop.start()
        self.embed_time_updater_loop.start()
        logger.info("StreamingMonitor: Started stream monitoring (3 minute interval)")
        logger.info("StreamingMonitor: Started embed time updater (30 minute interval)")

    def cog_unload(self):
        """Stop the loops when cog unloads."""
        self.stream_monitor_loop.cancel()
        self.embed_time_updater_loop.cancel()
        logger.info("StreamingMonitor: Stopped stream monitoring")

    def _format_elapsed_time(self, seconds: int) -> str:
        """Format elapsed seconds into human-readable string."""
        if seconds < 1800:  # Less than 30 minutes
            return "Streaming Now"

        hours = seconds // 3600
        minutes = (seconds % 3600) // 60

        if hours == 0:
            return f"Went live {minutes} minutes ago"
        elif hours == 1:
            if minutes >= 30:
                return "Went live 1 hour 30 minutes ago"
            else:
                return "Went live 1 hour ago"
        else:
            if minutes >= 30:
                return f"Went live {hours} hours 30 minutes ago"
            else:
                return f"Went live {hours} hours ago"

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
        stream_title: Optional[str] = None,
        message_id: Optional[int] = None,
        channel_id: Optional[int] = None
    ):
        """
        Record that we sent a notification for this stream.

        Args:
            db: Database session
            guild_id: Guild ID
            creator_id: Creator profile ID
            platform: 'youtube', 'twitch', or 'combined'
            stream_started_at: Unix timestamp when stream started
            stream_title: Optional stream title for logging
            message_id: Discord message ID for future edits
            channel_id: Discord channel ID where message was sent
        """
        try:
            notification = StreamNotificationHistory(
                guild_id=guild_id,
                creator_profile_id=creator_id,
                platform=platform,
                stream_started_at=stream_started_at,
                notified_at=int(time_lib.time()),
                stream_title=stream_title[:500] if stream_title else None,
                message_id=message_id,
                channel_id=channel_id
            )
            db.add(notification)
            db.commit()
        except Exception as e:
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

    @tasks.loop(seconds=1800)  # Update every 30 minutes
    async def embed_time_updater_loop(self):
        """Update stream notification embeds with elapsed time."""
        try:
            await self._update_embed_times()
        except Exception as e:
            logger.error(f"StreamingMonitor: Error in embed time updater: {e}", exc_info=True)

    @embed_time_updater_loop.before_loop
    async def before_embed_time_updater(self):
        """Wait until bot is ready before starting loop."""
        await self.bot.wait_until_ready()
        # Wait longer than stream monitor to let initial notifications go out
        await asyncio.sleep(60)

    async def _update_embed_times(self):
        """
        Update stream notification embeds with elapsed time.
        Also handles when one platform ends - updates embed to show only remaining platform.
        """
        current_time = int(time_lib.time())

        with db_session_scope() as db:
            # Find notifications from last 12 hours that have message_id
            cutoff = current_time - 43200  # 12 hours
            recent_notifs = db.query(StreamNotificationHistory).filter(
                StreamNotificationHistory.notified_at > cutoff,
                StreamNotificationHistory.message_id.isnot(None)
            ).all()

            if not recent_notifs:
                return

            logger.debug(f"StreamingMonitor: Checking {len(recent_notifs)} notifications for updates")

            for notif in recent_notifs:
                # Get creator
                creator = db.query(CreatorProfile).filter(
                    CreatorProfile.id == notif.creator_profile_id
                ).first()

                if not creator:
                    continue

                # Skip if creator is no longer live on ANY platform
                if not creator.is_live_youtube and not creator.is_live_twitch:
                    continue

                # Get elapsed time since notification was sent
                elapsed = current_time - notif.notified_at

                # Only update if 30+ minutes have passed (for time updates)
                # But always update if platform status changed (detected by comparing notif.platform)
                was_combined = notif.platform == 'combined'
                is_now_combined = creator.is_live_youtube and creator.is_live_twitch
                platform_changed = was_combined and not is_now_combined

                if elapsed < 1800 and not platform_changed:
                    continue

                try:
                    guild = self.bot.get_guild(notif.guild_id)
                    if not guild:
                        continue

                    channel = guild.get_channel(notif.channel_id)
                    if not channel:
                        continue

                    message = await channel.fetch_message(notif.message_id)
                    if not message or not message.embeds:
                        continue

                    # Get notification config for styling
                    config = db.query(StreamingNotificationsConfig).filter(
                        StreamingNotificationsConfig.guild_id == notif.guild_id
                    ).first()

                    if not config:
                        continue

                    # Determine current platform status
                    youtube_is_live = creator.is_live_youtube
                    twitch_is_live = creator.is_live_twitch

                    if youtube_is_live and twitch_is_live:
                        platform_text = "Twitch & YouTube"
                    elif twitch_is_live:
                        platform_text = "Twitch"
                    else:
                        platform_text = "YouTube"

                    # Format elapsed time for footer
                    time_text = self._format_elapsed_time(elapsed)
                    footer_text = f"{time_text} on {platform_text}"

                    # Determine footer icon
                    if twitch_is_live:
                        footer_icon = "https://static-cdn.jtvnw.net/jtv_user_pictures/8a6381c7-d0c0-4576-b179-38bd5ce1d6af-profile_image-70x70.png"
                    else:
                        footer_icon = "https://www.youtube.com/s/desktop/c01ea7e3/img/favicon_144x144.png"

                    # Get custom notification settings
                    notification_title = config.notification_title or '🔴 {creator} is live on {platform}!'
                    notification_message = config.notification_message or 'One of our community members just went live! Come hang out and show some support 🎮'
                    embed_color_hex = config.embed_color or '#9147ff'

                    # Replace placeholders
                    notification_title = notification_title.replace('{creator}', creator.display_name)
                    notification_title = notification_title.replace('{platform}', platform_text)
                    # Handle custom titles that mention specific platforms
                    if 'Twitch & YouTube' in notification_title and not is_now_combined:
                        notification_title = notification_title.replace('Twitch & YouTube', platform_text)
                    elif 'Twitch' in notification_title and not twitch_is_live:
                        notification_title = notification_title.replace('Twitch', 'YouTube')
                    elif 'YouTube' in notification_title and not youtube_is_live:
                        notification_title = notification_title.replace('YouTube', 'Twitch')
                    notification_message = notification_message.replace('{creator}', creator.display_name)

                    # Convert color
                    try:
                        embed_color_int = int(embed_color_hex.lstrip('#'), 16)
                    except ValueError:
                        embed_color_int = 0x9147ff

                    # Build watch links for currently live platforms
                    watch_links = []
                    if twitch_is_live and creator.twitch_handle:
                        watch_links.append(f"[Twitch](https://twitch.tv/{creator.twitch_handle})")
                    if youtube_is_live and creator.youtube_channel_id:
                        watch_links.append(f"[YouTube](https://www.youtube.com/channel/{creator.youtube_channel_id}/live)")

                    # Build new embed
                    new_embed = discord.Embed(
                        title=notification_title,
                        description=notification_message,
                        color=discord.Color(embed_color_int)
                    )

                    # Stream title from creator's current stream data
                    stream_title = creator.current_stream_title or 'Untitled Stream'
                    new_embed.add_field(
                        name="Stream Title",
                        value=stream_title[:1024],
                        inline=False
                    )

                    # Game/category
                    if creator.current_stream_game:
                        new_embed.add_field(
                            name="Playing",
                            value=creator.current_stream_game,
                            inline=True
                        )

                    # Viewer counts - show separate fields only if both live
                    if youtube_is_live and twitch_is_live:
                        # For combined, we need to fetch current viewer counts
                        # Use stored values from creator profile
                        new_embed.add_field(
                            name="Twitch Viewers",
                            value=f"{creator.current_stream_viewer_count or 0:,}",
                            inline=True
                        )
                        # YouTube viewers would need separate tracking - for now show combined
                        new_embed.add_field(
                            name="YouTube Viewers",
                            value="Live",
                            inline=True
                        )
                    else:
                        new_embed.add_field(
                            name="Viewers",
                            value=f"{creator.current_stream_viewer_count or 0:,}",
                            inline=True
                        )

                    # Watch on links
                    if watch_links:
                        new_embed.add_field(
                            name="📺 Watch on",
                            value=" • ".join(watch_links),
                            inline=False
                        )

                    # Thumbnail - use current stream thumbnail
                    if creator.current_stream_thumbnail:
                        new_embed.set_image(url=creator.current_stream_thumbnail)

                    # Creator avatar
                    if creator.avatar_url:
                        new_embed.set_thumbnail(url=creator.avatar_url)

                    # Footer with elapsed time
                    new_embed.set_footer(text=footer_text, icon_url=footer_icon)

                    await message.edit(embed=new_embed)

                    # Update notification record if platform changed
                    if platform_changed:
                        notif.platform = 'twitch' if twitch_is_live else 'youtube'
                        db.commit()
                        logger.info(f"StreamingMonitor: Updated embed for {creator.display_name} - now only live on {platform_text}")
                    else:
                        logger.debug(f"StreamingMonitor: Updated embed time for {creator.display_name} in guild {notif.guild_id}")

                except discord.NotFound:
                    continue
                except discord.Forbidden:
                    continue
                except Exception as e:
                    logger.debug(f"StreamingMonitor: Failed to update embed: {e}")
                    continue

    async def _check_all_streams(self):
        """Check all approved streamers across all guilds."""
        with db_session_scope() as db:
            # Get all approved streamers with their creator profiles
            approved = db.query(ApprovedStreamer).filter(
                ApprovedStreamer.revoked == False
            ).all()

            if not approved:
                return  # No approved streamers

            logger.info(f"StreamingMonitor: Checking {len(approved)} approved streamers")

            # Group by creator to avoid duplicate API calls
            # Also track which guilds each creator is approved in
            creators_to_check: Dict[int, list] = {}
            for approval in approved:
                # Check if this guild has Discovery access
                if not self._has_discovery_access(db, approval.guild_id):
                    logger.info(f"StreamingMonitor: Guild {approval.guild_id} lacks Discovery access, skipping")
                    continue

                creator_id = approval.creator_profile_id
                if creator_id not in creators_to_check:
                    creators_to_check[creator_id] = []
                creators_to_check[creator_id].append(approval)

            logger.info(f"StreamingMonitor: {len(creators_to_check)} creators to check after filtering")

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

                # Check both platforms and collect results
                youtube_result = None
                twitch_result = None

                # Check YouTube status
                if creator.youtube_channel_id and creator.youtube_refresh_token:
                    youtube_result = await self._check_youtube_status(db, creator)
                    await asyncio.sleep(0.5)

                # Check Twitch status
                if creator.twitch_user_id and creator.twitch_refresh_token:
                    twitch_result = await self._check_twitch_status(db, creator)
                    await asyncio.sleep(0.5)

                # Send combined notification if either platform just went live
                youtube_just_live = youtube_result and youtube_result.get('just_went_live')
                twitch_just_live = twitch_result and twitch_result.get('just_went_live')

                if youtube_just_live or twitch_just_live:
                    # Pass full results so we know ALL platforms currently live (not just newly live)
                    await self._send_combined_notification(
                        db, creator, approvals,
                        youtube_result,  # Pass full result to check is_live
                        twitch_result    # Pass full result to check is_live
                    )

            db.commit()

    async def _check_youtube_status(
        self,
        db,
        creator: CreatorProfile
    ) -> Optional[Dict[str, Any]]:
        """
        Check YouTube live status for a creator and return results.

        Args:
            db: Database session
            creator: CreatorProfile instance

        Returns:
            Dict with live_info, stream_started_at, just_went_live, or None on error
        """
        try:
            # Decrypt tokens for API use
            try:
                access_token = self.decrypt_token(creator.youtube_access_token)
                refresh_token = self.decrypt_token(creator.youtube_refresh_token) if creator.youtube_refresh_token else None

                if not access_token or not refresh_token:
                    logger.warning(f"StreamingMonitor: Missing YouTube tokens for creator {creator.id}")
                    return None
            except Exception as e:
                logger.error(f"StreamingMonitor: Failed to decrypt YouTube tokens for creator {creator.id}: {e}")
                return None

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
                    return None

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
                return None

            was_live = creator.is_live_youtube
            is_live = live_info is not None

            logger.info(f"StreamingMonitor: YouTube check for {creator.display_name} (id={creator.id}): was_live={was_live}, is_live={is_live}")

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

            db.commit()

            # Determine if just went live
            just_went_live = is_live and not was_live and stream_started_at is not None

            if just_went_live:
                logger.info(f"StreamingMonitor: {creator.display_name} just went LIVE on YouTube!")
            elif not is_live:
                logger.info(f"StreamingMonitor: {creator.display_name} is not live on YouTube")
            elif was_live:
                logger.info(f"StreamingMonitor: {creator.display_name} was already marked live on YouTube (no transition)")

            return {
                'platform': 'youtube',
                'live_info': live_info,
                'stream_started_at': stream_started_at,
                'just_went_live': just_went_live,
                'is_live': is_live
            }

        except Exception as e:
            logger.error(f"StreamingMonitor: Error checking YouTube status for creator {creator.id}: {e}", exc_info=True)
            self._record_api_failure(creator.id)
            return None

    async def _check_twitch_status(
        self,
        db,
        creator: CreatorProfile
    ) -> Optional[Dict[str, Any]]:
        """
        Check Twitch live status for a creator and return results.

        Args:
            db: Database session
            creator: CreatorProfile instance

        Returns:
            Dict with live_info, stream_started_at, just_went_live, or None on error
        """
        try:
            # Decrypt tokens for API use
            try:
                access_token = self.decrypt_token(creator.twitch_access_token)
                refresh_token = self.decrypt_token(creator.twitch_refresh_token) if creator.twitch_refresh_token else None

                if not access_token or not refresh_token:
                    logger.warning(f"StreamingMonitor: Missing Twitch tokens for creator {creator.id}")
                    return None
            except Exception as e:
                logger.error(f"StreamingMonitor: Failed to decrypt Twitch tokens for creator {creator.id}: {e}")
                return None

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
                    return None

            # Check if currently live
            try:
                live_info = self.twitch_service.get_live_streams(access_token, creator.twitch_user_id)
                # Clear failure record on success
                self._clear_api_failure(creator.id)
            except self.TwitchAPIError as e:
                logger.error(f"StreamingMonitor: Twitch API error for creator {creator.id}: {e}")
                self._record_api_failure(creator.id)
                return None

            was_live = creator.is_live_twitch
            is_live = live_info is not None

            logger.info(f"StreamingMonitor: Twitch check for {creator.display_name} (id={creator.id}): was_live={was_live}, is_live={is_live}")

            # Get stream start time
            stream_started_at = None
            if is_live:
                stream_started_at = live_info.get('started_at')
                if not stream_started_at:
                    stream_started_at = current_time
                logger.info(f"StreamingMonitor: Stream info - title='{live_info.get('title')}', started_at={stream_started_at}")

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

            db.commit()

            # Determine if just went live
            just_went_live = is_live and not was_live and stream_started_at is not None

            if just_went_live:
                logger.info(f"StreamingMonitor: {creator.display_name} just went LIVE on Twitch!")
            elif not is_live:
                logger.info(f"StreamingMonitor: {creator.display_name} is not live on Twitch")
            elif was_live:
                logger.info(f"StreamingMonitor: {creator.display_name} was already marked live on Twitch (no transition)")

            return {
                'platform': 'twitch',
                'live_info': live_info,
                'stream_started_at': stream_started_at,
                'just_went_live': just_went_live,
                'is_live': is_live
            }

        except Exception as e:
            logger.error(f"StreamingMonitor: Error checking Twitch status for creator {creator.id}: {e}", exc_info=True)
            self._record_api_failure(creator.id)
            return None

    async def _send_combined_notification(
        self,
        db,
        creator: CreatorProfile,
        approvals: list,
        youtube_result: Optional[Dict[str, Any]],
        twitch_result: Optional[Dict[str, Any]]
    ):
        """
        Send or update a combined notification for platforms going live.

        If creator was already live on one platform and just went live on another,
        we UPDATE the existing embed instead of sending a new one.

        Args:
            db: Database session
            creator: CreatorProfile instance
            approvals: List of ApprovedStreamer instances
            youtube_result: YouTube check result (or None)
            twitch_result: Twitch check result (or None)
        """
        current_time = int(time_lib.time())

        # Determine which platforms JUST went live
        youtube_just_live = youtube_result and youtube_result.get('just_went_live')
        twitch_just_live = twitch_result and twitch_result.get('just_went_live')

        # Determine which platforms ARE live (for embed content)
        youtube_is_live = youtube_result and youtube_result.get('is_live')
        twitch_is_live = twitch_result and twitch_result.get('is_live')

        platforms_just_live = []
        if youtube_just_live:
            platforms_just_live.append('youtube')
        if twitch_just_live:
            platforms_just_live.append('twitch')

        if not platforms_just_live:
            return

        logger.info(f"StreamingMonitor: {creator.display_name} just went LIVE on {', '.join(platforms_just_live)}! Preparing notification...")

        # Get stream_started_at for the platform that just went live
        stream_started_at = None
        if youtube_just_live and youtube_result.get('stream_started_at'):
            stream_started_at = youtube_result['stream_started_at']
        if twitch_just_live and twitch_result.get('stream_started_at'):
            if stream_started_at is None or twitch_result['stream_started_at'] < stream_started_at:
                stream_started_at = twitch_result['stream_started_at']

        # Process each guild
        logger.info(f"StreamingMonitor: Will process {len(approvals)} guild(s) for {creator.display_name}")
        for approval in approvals:
            # Check if there's a recent notification we should UPDATE instead of create new
            # Only update if:
            # 1. Notification was sent in last 30 minutes
            # 2. Creator is STILL live on the OTHER platform (not a new stream session)
            # This prevents updating old embeds when someone ends and restarts a stream
            recent_cutoff = current_time - 1800  # 30 minutes
            existing_notif = db.query(StreamNotificationHistory).filter(
                StreamNotificationHistory.guild_id == approval.guild_id,
                StreamNotificationHistory.creator_profile_id == creator.id,
                StreamNotificationHistory.notified_at > recent_cutoff,
                StreamNotificationHistory.message_id.isnot(None)
            ).order_by(StreamNotificationHistory.notified_at.desc()).first()

            # Only update if creator was already live on one platform and just added another
            # If both platforms JUST went live, or only one is live total, send new notification
            should_update = False
            if existing_notif and existing_notif.message_id:
                # Check: one platform was already live (not just_went_live) and other just went live
                youtube_was_already_live = youtube_is_live and not youtube_just_live
                twitch_was_already_live = twitch_is_live and not twitch_just_live

                if (youtube_just_live and twitch_was_already_live) or (twitch_just_live and youtube_was_already_live):
                    should_update = True
                    logger.info(f"StreamingMonitor: Will update existing notification - creator added second platform")

            if should_update:
                # UPDATE existing embed - creator added a second platform
                logger.info(f"StreamingMonitor: Updating existing notification (msg_id={existing_notif.message_id}) for {creator.display_name} in guild {approval.guild_id}")
                await self._update_combined_embed(
                    db,
                    creator,
                    approval.guild_id,
                    existing_notif,
                    youtube_result,
                    twitch_result
                )
            else:
                # Send NEW notification
                logger.info(f"StreamingMonitor: Sending new notification to guild {approval.guild_id}...")
                await self._send_combined_embed(
                    db,
                    creator,
                    approval.guild_id,
                    youtube_result,
                    twitch_result,
                    stream_started_at
                )

    async def _send_combined_embed(
        self,
        db,
        creator: CreatorProfile,
        guild_id: int,
        youtube_result: Optional[Dict[str, Any]],
        twitch_result: Optional[Dict[str, Any]],
        stream_started_at: int
    ):
        """
        Build and send a combined embed for multi-platform streams.

        Args:
            db: Database session
            creator: CreatorProfile instance
            guild_id: Guild to send to
            youtube_result: YouTube check result (or None)
            twitch_result: Twitch check result (or None)
            stream_started_at: Earliest stream start timestamp
        """
        try:
            # Get notification config
            config = db.query(StreamingNotificationsConfig).filter(
                StreamingNotificationsConfig.guild_id == guild_id
            ).first()

            if not config or not config.enabled:
                logger.info(f"StreamingMonitor: Notifications disabled for guild {guild_id}")
                return

            if not config.notification_channel_id:
                logger.warning(f"StreamingMonitor: No notification channel set for guild {guild_id}")
                return

            # Get guild and channel
            guild = self.bot.get_guild(guild_id)
            if not guild:
                logger.warning(f"StreamingMonitor: Guild {guild_id} not found (bot not in guild?)")
                return

            channel = guild.get_channel(config.notification_channel_id)
            if not channel:
                logger.warning(f"StreamingMonitor: Channel {config.notification_channel_id} not found in guild {guild_id}")
                return

            # Check minimum level requirement
            if config.minimum_level_required > 0:
                member = db.query(GuildMember).filter(
                    GuildMember.guild_id == guild_id,
                    GuildMember.user_id == creator.discord_id
                ).first()

                if not member or member.level < config.minimum_level_required:
                    logger.info(
                        f"StreamingMonitor: {creator.display_name} doesn't meet level requirement "
                        f"(level {member.level if member else 0} < required {config.minimum_level_required}) in guild {guild_id}"
                    )
                    return

            logger.info(f"StreamingMonitor: Building combined embed for {creator.display_name} in guild {guild_id}")

            # Determine which platforms are CURRENTLY live (not just went live)
            # This ensures we show links for all platforms the creator is streaming on
            youtube_is_live = youtube_result and youtube_result.get('is_live')
            twitch_is_live = twitch_result and twitch_result.get('is_live')

            # Get stream info - prefer Twitch for game/title, YouTube for thumbnail if both live
            twitch_info = twitch_result.get('live_info') if twitch_result else None
            youtube_info = youtube_result.get('live_info') if youtube_result else None

            # Use Twitch info for title/game (more accurate), but get YouTube thumbnail if available
            if twitch_is_live and twitch_info:
                stream_title = twitch_info.get('title', 'Untitled Stream')
                game_name = twitch_info.get('game_name')
                viewer_count = twitch_info.get('viewer_count', 0)
            elif youtube_is_live and youtube_info:
                stream_title = youtube_info.get('title', 'Untitled Stream')
                game_name = youtube_info.get('game_name')
                viewer_count = youtube_info.get('viewer_count', 0)
            else:
                stream_title = 'Untitled Stream'
                game_name = None
                viewer_count = 0

            # Prefer YouTube thumbnail if live on YouTube (higher quality), otherwise Twitch
            if youtube_is_live and youtube_info and youtube_info.get('thumbnail_url'):
                thumbnail_url = youtube_info['thumbnail_url']
            elif twitch_is_live and twitch_info and twitch_info.get('thumbnail_url'):
                thumbnail_url = twitch_info['thumbnail_url']
            else:
                thumbnail_url = None

            # Build platform text for title
            if youtube_is_live and twitch_is_live:
                platform_text = "Twitch & YouTube"
            elif twitch_is_live:
                platform_text = "Twitch"
            else:
                platform_text = "YouTube"

            # Custom notification settings - replace default with platform-aware title
            notification_title = config.notification_title or '🔴 {creator} is live on {platform}!'
            notification_message = config.notification_message or 'One of our community members just went live! Come hang out and show some support 🎮'
            embed_color_hex = config.embed_color or '#9147ff'  # Default to Twitch purple

            # Replace placeholders
            notification_title = notification_title.replace('{creator}', creator.display_name)
            notification_title = notification_title.replace('{platform}', platform_text)
            # If user's custom title mentions only "Twitch" but they're on both, append YouTube
            if youtube_is_live and twitch_is_live:
                if 'Twitch' in notification_title and 'YouTube' not in notification_title:
                    notification_title = notification_title.replace('Twitch', 'Twitch & YouTube')
                elif 'YouTube' in notification_title and 'Twitch' not in notification_title:
                    notification_title = notification_title.replace('YouTube', 'Twitch & YouTube')
            notification_message = notification_message.replace('{creator}', creator.display_name)

            # Convert color
            try:
                embed_color_int = int(embed_color_hex.lstrip('#'), 16)
            except ValueError:
                embed_color_int = 0x9147ff

            # Build watch links for ALL platforms currently live
            watch_links = []
            if twitch_is_live and creator.twitch_handle:
                watch_links.append(f"[Twitch](https://twitch.tv/{creator.twitch_handle})")
            if youtube_is_live and creator.youtube_channel_id:
                watch_links.append(f"[YouTube](https://www.youtube.com/channel/{creator.youtube_channel_id}/live)")

            # Build embed
            embed = discord.Embed(
                title=notification_title,
                description=notification_message,
                color=discord.Color(embed_color_int)
            )

            # Stream title field
            embed.add_field(
                name="Stream Title",
                value=stream_title[:1024],
                inline=False
            )

            # Game/category if available
            if game_name:
                embed.add_field(
                    name="Playing",
                    value=game_name,
                    inline=True
                )

            # Viewer counts - separate fields if multistreaming, single field if single platform
            if youtube_is_live and twitch_is_live:
                # Both platforms - show separate viewer counts
                twitch_viewers = twitch_info.get('viewer_count', 0) if twitch_info else 0
                youtube_viewers = youtube_info.get('viewer_count', 0) if youtube_info else 0
                embed.add_field(
                    name="Twitch Viewers",
                    value=f"{twitch_viewers:,}",
                    inline=True
                )
                embed.add_field(
                    name="YouTube Viewers",
                    value=f"{youtube_viewers:,}",
                    inline=True
                )
            elif twitch_is_live:
                # Twitch only
                twitch_viewers = twitch_info.get('viewer_count', 0) if twitch_info else 0
                embed.add_field(
                    name="Viewers",
                    value=f"{twitch_viewers:,}",
                    inline=True
                )
            else:
                # YouTube only
                youtube_viewers = youtube_info.get('viewer_count', 0) if youtube_info else 0
                embed.add_field(
                    name="Viewers",
                    value=f"{youtube_viewers:,}",
                    inline=True
                )

            # Watch on links
            if watch_links:
                embed.add_field(
                    name="📺 Watch on",
                    value=" • ".join(watch_links),
                    inline=False
                )

            # Thumbnail
            if thumbnail_url:
                embed.set_image(url=thumbnail_url)

            # Creator avatar
            if creator.avatar_url:
                embed.set_thumbnail(url=creator.avatar_url)

            # Footer with "Streaming Now on X" and timestamp from stream start
            if youtube_is_live and twitch_is_live:
                footer_text = "Streaming Now on Twitch & YouTube"
                footer_icon = "https://static-cdn.jtvnw.net/jtv_user_pictures/8a6381c7-d0c0-4576-b179-38bd5ce1d6af-profile_image-70x70.png"
            elif twitch_is_live:
                footer_text = "Streaming Now on Twitch"
                footer_icon = "https://static-cdn.jtvnw.net/jtv_user_pictures/8a6381c7-d0c0-4576-b179-38bd5ce1d6af-profile_image-70x70.png"
            else:
                footer_text = "Streaming Now on YouTube"
                footer_icon = "https://www.youtube.com/s/desktop/c01ea7e3/img/favicon_144x144.png"

            embed.set_footer(text=footer_text, icon_url=footer_icon)
            # No timestamp needed - "Streaming Now" in footer is clear enough

            # Build content with optional role ping
            content = None
            if config.ping_role_id:
                role = guild.get_role(config.ping_role_id)
                if role:
                    content = f"{role.mention} - {creator.display_name} just went live on {platform_text}! Come show some support!"

            # Send notification and capture message for future edits
            message = await channel.send(content=content, embed=embed)

            # Record notification in database with message_id for future updates
            self._record_notification_sent(
                db, guild_id, creator.id, 'combined', stream_started_at, stream_title,
                message_id=message.id, channel_id=channel.id
            )

            logger.info(
                f"StreamingMonitor: Sent combined notification for creator {creator.id} "
                f"({creator.display_name}) to guild {guild_id} (msg_id={message.id})"
            )

        except discord.Forbidden:
            logger.error(f"StreamingMonitor: No permission to send notification in guild {guild_id}")
        except Exception as e:
            logger.error(f"StreamingMonitor: Error sending combined notification to guild {guild_id}: {e}", exc_info=True)

    async def _update_combined_embed(
        self,
        db,
        creator: CreatorProfile,
        guild_id: int,
        existing_notif: StreamNotificationHistory,
        youtube_result: Optional[Dict[str, Any]],
        twitch_result: Optional[Dict[str, Any]]
    ):
        """
        Update an existing notification embed when a second platform goes live.

        Args:
            db: Database session
            creator: CreatorProfile instance
            guild_id: Guild to update notification in
            existing_notif: The existing StreamNotificationHistory record
            youtube_result: YouTube check result
            twitch_result: Twitch check result
        """
        try:
            # Get guild and channel
            guild = self.bot.get_guild(guild_id)
            if not guild:
                logger.warning(f"StreamingMonitor: Guild {guild_id} not found for update")
                return

            channel = guild.get_channel(existing_notif.channel_id)
            if not channel:
                logger.warning(f"StreamingMonitor: Channel {existing_notif.channel_id} not found for update")
                return

            # Get the original message
            try:
                message = await channel.fetch_message(existing_notif.message_id)
            except discord.NotFound:
                logger.warning(f"StreamingMonitor: Message {existing_notif.message_id} not found for update")
                return
            except discord.Forbidden:
                logger.warning(f"StreamingMonitor: No permission to fetch message {existing_notif.message_id}")
                return

            # Get notification config
            config = db.query(StreamingNotificationsConfig).filter(
                StreamingNotificationsConfig.guild_id == guild_id
            ).first()

            if not config:
                return

            # Determine which platforms are CURRENTLY live
            youtube_is_live = youtube_result and youtube_result.get('is_live')
            twitch_is_live = twitch_result and twitch_result.get('is_live')

            # Get stream info from both platforms
            twitch_info = twitch_result.get('live_info') if twitch_result else None
            youtube_info = youtube_result.get('live_info') if youtube_result else None

            # Use Twitch info for title/game (more accurate)
            if twitch_is_live and twitch_info:
                stream_title = twitch_info.get('title', 'Untitled Stream')
                game_name = twitch_info.get('game_name')
            elif youtube_is_live and youtube_info:
                stream_title = youtube_info.get('title', 'Untitled Stream')
                game_name = youtube_info.get('game_name')
            else:
                stream_title = 'Untitled Stream'
                game_name = None

            # Prefer YouTube thumbnail if available
            if youtube_is_live and youtube_info and youtube_info.get('thumbnail_url'):
                thumbnail_url = youtube_info['thumbnail_url']
            elif twitch_is_live and twitch_info and twitch_info.get('thumbnail_url'):
                thumbnail_url = twitch_info['thumbnail_url']
            else:
                thumbnail_url = None

            # Build platform text
            if youtube_is_live and twitch_is_live:
                platform_text = "Twitch & YouTube"
            elif twitch_is_live:
                platform_text = "Twitch"
            else:
                platform_text = "YouTube"

            # Custom notification settings
            notification_title = config.notification_title or '🔴 {creator} is live on {platform}!'
            notification_message = config.notification_message or 'One of our community members just went live! Come hang out and show some support 🎮'
            embed_color_hex = config.embed_color or '#9147ff'

            # Replace placeholders
            notification_title = notification_title.replace('{creator}', creator.display_name)
            notification_title = notification_title.replace('{platform}', platform_text)
            if youtube_is_live and twitch_is_live:
                if 'Twitch' in notification_title and 'YouTube' not in notification_title:
                    notification_title = notification_title.replace('Twitch', 'Twitch & YouTube')
                elif 'YouTube' in notification_title and 'Twitch' not in notification_title:
                    notification_title = notification_title.replace('YouTube', 'Twitch & YouTube')
            notification_message = notification_message.replace('{creator}', creator.display_name)

            # Convert color
            try:
                embed_color_int = int(embed_color_hex.lstrip('#'), 16)
            except ValueError:
                embed_color_int = 0x9147ff

            # Build watch links
            watch_links = []
            if twitch_is_live and creator.twitch_handle:
                watch_links.append(f"[Twitch](https://twitch.tv/{creator.twitch_handle})")
            if youtube_is_live and creator.youtube_channel_id:
                watch_links.append(f"[YouTube](https://www.youtube.com/channel/{creator.youtube_channel_id}/live)")

            # Build updated embed
            embed = discord.Embed(
                title=notification_title,
                description=notification_message,
                color=discord.Color(embed_color_int)
            )

            embed.add_field(
                name="Stream Title",
                value=stream_title[:1024],
                inline=False
            )

            if game_name:
                embed.add_field(
                    name="Playing",
                    value=game_name,
                    inline=True
                )

            # Viewer counts
            if youtube_is_live and twitch_is_live:
                twitch_viewers = twitch_info.get('viewer_count', 0) if twitch_info else 0
                youtube_viewers = youtube_info.get('viewer_count', 0) if youtube_info else 0
                embed.add_field(name="Twitch Viewers", value=f"{twitch_viewers:,}", inline=True)
                embed.add_field(name="YouTube Viewers", value=f"{youtube_viewers:,}", inline=True)
            elif twitch_is_live:
                twitch_viewers = twitch_info.get('viewer_count', 0) if twitch_info else 0
                embed.add_field(name="Viewers", value=f"{twitch_viewers:,}", inline=True)
            else:
                youtube_viewers = youtube_info.get('viewer_count', 0) if youtube_info else 0
                embed.add_field(name="Viewers", value=f"{youtube_viewers:,}", inline=True)

            if watch_links:
                embed.add_field(
                    name="📺 Watch on",
                    value=" • ".join(watch_links),
                    inline=False
                )

            if thumbnail_url:
                embed.set_image(url=thumbnail_url)

            if creator.avatar_url:
                embed.set_thumbnail(url=creator.avatar_url)

            # Footer
            if youtube_is_live and twitch_is_live:
                footer_text = "Streaming Now on Twitch & YouTube"
                footer_icon = "https://static-cdn.jtvnw.net/jtv_user_pictures/8a6381c7-d0c0-4576-b179-38bd5ce1d6af-profile_image-70x70.png"
            elif twitch_is_live:
                footer_text = "Streaming Now on Twitch"
                footer_icon = "https://static-cdn.jtvnw.net/jtv_user_pictures/8a6381c7-d0c0-4576-b179-38bd5ce1d6af-profile_image-70x70.png"
            else:
                footer_text = "Streaming Now on YouTube"
                footer_icon = "https://www.youtube.com/s/desktop/c01ea7e3/img/favicon_144x144.png"

            embed.set_footer(text=footer_text, icon_url=footer_icon)
            # No timestamp needed - "Streaming Now" in footer is clear enough

            # Update the message (keep original content/ping)
            await message.edit(embed=embed)

            # Update the database record
            existing_notif.platform = 'combined'
            existing_notif.notified_at = int(time_lib.time())
            db.commit()

            logger.info(
                f"StreamingMonitor: Updated notification for creator {creator.id} "
                f"({creator.display_name}) in guild {guild_id} - now live on {platform_text}"
            )

        except Exception as e:
            logger.error(f"StreamingMonitor: Error updating notification in guild {guild_id}: {e}", exc_info=True)


def setup(bot: commands.Bot):
    """Load the cog."""
    bot.add_cog(StreamingMonitorCog(bot))
