# cogs/discovery.py - Discovery & Self-Promo System
"""
Full discovery system for QuestLog.

FREE FEATURES:
- Self-promo: Post in #self-promo channel (0 hero_tokens, daily limit based on tier)

PREMIUM FEATURES:
- Featured Pool: 10 hero_tokens to enter for 3-day feature chance
- Higher daily self-promo limit

PRO FEATURES:
- Cross-server discovery network
- Server listing in discovery directory
- Unlimited self-promo posts
"""

import time
import random
import re
import json
import asyncio
from datetime import datetime
from urllib.parse import urlparse
import discord
from discord.ext import commands, tasks
from discord import SlashCommandGroup

from config import (
    db_session_scope, logger, get_debug_guilds, DefaultXPSettings, FeatureLimits
)
from models import (
    Guild, GuildMember, GuildModule, PromoPost, FeaturedPool, FeaturedCreator, DiscoveryNetwork,
    ServerListing, PromoTier, DiscoveryConfig, XPConfig, AnnouncedGame,
    GameSearchConfig, CreatorOfTheMonth, CreatorOfTheWeek
)
from utils import igdb

DASHBOARD_BASE_URL = "https://dashboard.casual-heroes.com"


def get_guild_tier(session, guild_id: int) -> str:
    """Get the effective tier for a guild (FREE, PREMIUM, or PRO)."""
    db_guild = session.get(Guild, guild_id)
    if not db_guild:
        return "FREE"
    if db_guild.is_vip:
        return "PRO"
    return db_guild.subscription_tier.upper() if db_guild.subscription_tier else "FREE"


def get_today_start_timestamp() -> int:
    """Get Unix timestamp for start of today (UTC)."""
    now = int(time.time())
    return now - (now % 86400)


def extract_links(text: str) -> list:
    """Extract all URLs from text."""
    url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+/]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    return url_pattern.findall(text)


def detect_platform(url: str) -> str:
    """Detect the platform from a URL."""
    if not url:
        return 'other'

    url_lower = url.lower()
    domain = urlparse(url).netloc.lower()

    if 'youtube.com' in domain or 'youtu.be' in domain:
        return 'youtube'
    elif 'twitch.tv' in domain:
        return 'twitch'
    elif 'twitter.com' in domain or 'x.com' in domain:
        return 'twitter'
    elif 'tiktok.com' in domain:
        return 'tiktok'
    elif 'instagram.com' in domain:
        return 'instagram'
    elif 'bsky.app' in domain:
        return 'bsky'
    elif 'discord.gg' in domain or 'discord.com/invite' in url_lower:
        return 'discord'
    elif 'github.com' in domain:
        return 'github'
    elif 'reddit.com' in domain:
        return 'reddit'
    else:
        return 'other'


def get_or_create_global_creator(session, user_id: int, guild_id: int):
    """
    Get or create a global FeaturedCreator entry for a user.

    Returns:
        tuple: (creator, is_new) where is_new=True if created, False if existed
    """
    creator = session.get(FeaturedCreator, user_id)

    if creator:
        # Add guild if not already in list
        guilds = json.loads(creator.guilds) if creator.guilds else []
        if guild_id not in guilds:
            guilds.append(guild_id)
            creator.guilds = json.dumps(guilds)

            # Auto-update primary to most recent if enabled
            if creator.auto_select_primary:
                creator.primary_guild_id = guild_id

        # Reactivate if they were inactive
        if not creator.is_active:
            creator.is_active = True
            creator.inactive_since = None

        return creator, False  # exists
    else:
        # Create new global creator
        now = int(time.time())
        creator = FeaturedCreator(
            user_id=user_id,
            guilds=json.dumps([guild_id]),
            primary_guild_id=guild_id,
            auto_select_primary=True,
            is_active=True,
            inactive_since=None,
            times_featured_total=0,
            created_at=now,
            updated_at=now
        )
        session.add(creator)
        return creator, True  # new


def remove_guild_from_creator(session, user_id: int, guild_id: int):
    """
    Remove a guild from a creator's guilds list.
    If no guilds remain, mark creator as inactive.

    Returns:
        bool: True if creator marked inactive, False otherwise
    """
    creator = session.get(FeaturedCreator, user_id)

    if not creator:
        return False

    # Remove guild from list
    guilds = json.loads(creator.guilds) if creator.guilds else []
    if guild_id in guilds:
        guilds.remove(guild_id)
        creator.guilds = json.dumps(guilds)

        # If no guilds remain, mark as inactive
        if len(guilds) == 0:
            creator.is_active = False
            creator.inactive_since = int(time.time())
            logger.info(f"Marked creator {user_id} as inactive (no guilds remaining)")
            return True
        else:
            # If they had auto-select and lost their primary guild, pick a new one
            if creator.auto_select_primary and creator.primary_guild_id == guild_id:
                creator.primary_guild_id = guilds[0]  # Pick first remaining guild
                logger.info(f"Updated primary guild for creator {user_id} to {guilds[0]}")

    return False


class DiscoveryCog(commands.Cog):
    """Discovery and self-promotion system."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.featured_selection_task.start()
        self.game_discovery_task.start()
        # DISABLED: forum_scanner_task - redundant with on_thread_create event listener (line 519)
        # self.forum_scanner_task.start()
        self.creator_of_week_task.start()
        self.creator_of_month_task.start()
        self.cleanup_inactive_creators_task.start()
        self.featured_reminder_task.start()
        self.cotw_cotm_auto_rotation_task.start()

    def cog_unload(self):
        self.featured_selection_task.cancel()
        self.game_discovery_task.cancel()
        # forum_scanner_task disabled - not started, no need to cancel
        # self.forum_scanner_task.cancel()
        self.creator_of_week_task.cancel()
        self.cotw_cotm_auto_rotation_task.cancel()
        self.creator_of_month_task.cancel()
        self.cleanup_inactive_creators_task.cancel()
        self.featured_reminder_task.cancel()

    # ========== EVENT LISTENERS ==========

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """
        Auto-detect posts in selfpromo channel and add to featured pool.
        - Requires configured selfpromo channel
        - Costs hero_tokens (default 10, configurable)
        - 24h cooldown per user (configurable)
        - Free posts allowed, just not entered in pool
        """
        # Ignore bots, DMs, and command messages
        if message.author.bot or not message.guild or message.content.startswith(('/','!')):
            return

        try:
            with db_session_scope() as session:
                # Check if Discovery is configured for this guild
                config = session.query(DiscoveryConfig).filter_by(
                    guild_id=message.guild.id
                ).first()

                if not config or not config.enabled:
                    return

                # Check if this is the selfpromo channel
                if not config.selfpromo_channel_id or message.channel.id != config.selfpromo_channel_id:
                    return

                # Get user's XP/token data
                member = session.query(GuildMember).filter_by(
                    guild_id=message.guild.id,
                    user_id=message.author.id
                ).first()

                if not member:
                    # Create member record if doesn't exist
                    logger.warning(
                        f"[DUPLICATE TRACKER] discovery.on_message CREATING GuildMember: "
                        f"guild_id={message.guild.id}, user_id={message.author.id}, user_id_type={type(message.author.id)}, "
                        f"display_name={message.author.display_name}, source=Discord.Message.author"
                    )
                    member = GuildMember(
                        guild_id=message.guild.id,
                        user_id=message.author.id,
                        xp=0,
                        level=1,
                        hero_tokens=0
                    )
                    session.add(member)
                    session.flush()

                token_cost = config.token_cost if config.token_cost else 10  # Default 10 hero_tokens
                now = int(time.time())

                # Check if user has enough hero_tokens
                if member.hero_tokens < token_cost:
                    # Not enough hero_tokens - just react with thumbs up (no spam message)
                    await message.add_reaction("👍")
                    logger.info(f"User {message.author.id} doesn't have enough tokens ({member.hero_tokens}/{token_cost})")
                    return

                # Check cooldown (default 24 hours)
                cooldown_hours = getattr(config, 'entry_cooldown_hours', 24)  # Default 24h
                cooldown_seconds = cooldown_hours * 3600

                # Find user's last pool entry
                last_entry = session.query(FeaturedPool).filter_by(
                    guild_id=message.guild.id,
                    user_id=message.author.id
                ).order_by(FeaturedPool.entered_at.desc()).first()

                if last_entry and (now - last_entry.entered_at) < cooldown_seconds:
                    # Still on cooldown
                    time_left = cooldown_seconds - (now - last_entry.entered_at)
                    hours_left = int(time_left // 3600)
                    minutes_left = int((time_left % 3600) // 60)

                    # Use customizable cooldown message
                    cooldown_msg = config.cooldown_message if config.cooldown_message else (
                        "⏰ You're on cooldown! Can enter the featured pool again in {time_left}.\n"
                        "💰 Your {token_cost} hero_tokens were saved."
                    )

                    # Format the message with time_left and token_cost
                    formatted_msg = cooldown_msg.format(
                        time_left=f"**{hours_left}h {minutes_left}m**",
                        token_cost=token_cost
                    )

                    await message.add_reaction("⏰")  # Clock emoji
                    await self._send_discovery_message(
                        config=config,
                        guild=message.guild,
                        user=message.author,
                        message_text=formatted_msg,
                        original_channel=message.channel
                    )
                    return

                # User has hero_tokens and not on cooldown - deduct hero_tokens and add to pool!
                member.hero_tokens -= token_cost

                # Parse content and links
                content = message.content[:1000]  # Limit to 1000 chars
                links = extract_links(message.content)
                link_url = links[0] if links else None
                platform = detect_platform(link_url) if link_url else 'other'

                # Add to featured pool
                pool_entry = FeaturedPool(
                    guild_id=message.guild.id,
                    user_id=message.author.id,
                    content=content,
                    link_url=link_url,
                    platform=platform,
                    entered_at=now,
                    expires_at=now + (config.pool_entry_duration_hours * 3600),
                    original_message_id=message.id,
                    original_channel_id=message.channel.id,
                    was_selected=False
                )
                session.add(pool_entry)
                session.commit()

                # Success reactions
                await message.add_reaction("✅")  # Check mark
                await message.add_reaction("🎟️")  # Ticket for hero_tokens

                # Confirmation message
                next_cooldown = now + cooldown_seconds
                success_message = (
                    f"✅ **Added to featured pool!** 🎉\n"
                    f"🎟️ Cost: {token_cost} hero_tokens ({member.hero_tokens} remaining)\n"
                    f"⏰ Next entry: <t:{next_cooldown}:R>\n"
                    f"🎲 Good luck getting featured!"
                )
                await self._send_discovery_message(
                    config=config,
                    guild=message.guild,
                    user=message.author,
                    message_text=success_message,
                    original_channel=message.channel
                )

                # Removed deprecated quick feature - now only adds to pool
                # Features will only be shown when selected from the pool

                logger.info(f"Added {message.author} to featured pool in guild {message.guild.id} (cost: {token_cost} hero_tokens)")

        except Exception as e:
            logger.error(f"Error processing selfpromo message: {e}", exc_info=True)

    # ========== BACKGROUND TASKS ==========

    def _parse_social_links(self, link_url: str) -> dict:
        """Extract social media platform from URL."""
        if not link_url:
            return {}

        links = {}
        link_lower = link_url.lower()

        if 'twitch.tv' in link_lower:
            links['twitch'] = link_url
        elif 'youtube.com' in link_lower or 'youtu.be' in link_lower:
            links['youtube'] = link_url
        elif 'twitter.com' in link_lower or 'x.com' in link_lower:
            links['twitter'] = link_url
        elif 'tiktok.com' in link_lower:
            links['tiktok'] = link_url
        elif 'instagram.com' in link_lower:
            links['instagram'] = link_url
        elif 'bsky.app' in link_lower:
            links['bsky'] = link_url
        else:
            links['other'] = link_url

        return links

    async def _add_to_featured_creators_hall(self, guild_id: int, member: discord.Member, winner: FeaturedPool):
        """Add or update creator in permanent featured creators list (GLOBAL model)."""
        with db_session_scope() as session:
            now = int(time.time())

            # Get or create global creator entry
            creator, is_new = get_or_create_global_creator(session, member.id, guild_id)

            # Extract social links from winner.link_url
            social_links = self._parse_social_links(winner.link_url)

            # Update creator data
            creator.last_featured_at = now
            creator.times_featured_total += 1  # GLOBAL count
            creator.avatar_url = member.display_avatar.url
            creator.display_name = member.display_name
            creator.username = member.name
            creator.bio = winner.content or creator.bio
            creator.updated_at = now

            # Set forum_thread_id if this was a forum post
            if hasattr(winner, 'forum_thread_id') and winner.forum_thread_id:
                creator.forum_thread_id = winner.forum_thread_id
                creator.source = 'forum'

            # Set first_featured_at if new
            if is_new:
                creator.first_featured_at = now

            # Update social links (only if new ones provided)
            if social_links.get('twitch'):
                creator.twitch_url = social_links['twitch']
            if social_links.get('youtube'):
                creator.youtube_url = social_links['youtube']
            if social_links.get('twitter'):
                creator.twitter_url = social_links['twitter']
            if social_links.get('tiktok'):
                creator.tiktok_url = social_links['tiktok']
            if social_links.get('instagram'):
                creator.instagram_url = social_links['instagram']
            if social_links.get('bsky'):
                creator.bsky_url = social_links['bsky']
            if social_links.get('other'):
                creator.other_links = social_links['other']

            session.commit()

            action = "Added" if is_new else "Updated"
            logger.info(f"[Discovery] [guild_id:{guild_id}] {action} {member.display_name} in GLOBAL featured creators hall (total: {creator.times_featured_total})")

    # async def edit_featured_creators_hall(self, guild_id: int, member: discord.Member, winner: FeaturedPool):
    #     """Add or update creator in permanent featured creators list."""
    #     with db_session_scope() as session:
    #         now = int(time.time())

    #         # Check if creator already exists
    #         creator = session.query(FeaturedCreator).filter_by(
    #             guild_id=guild_id,
    #             user_id=member.id
    #         ).first()

    #         # Extract social links from winner.link_url
    #         social_links = self._parse_social_links(winner.link_url)

    #         if creator:
    #             # Update existing creator
    #             creator.last_featured_at = now
    #             creator.times_featured += 1
          

    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        """
        Update creator info when they edit their forum intro post (GLOBAL model).
        """
        # Ignore bots and non-forum threads
        if after.author.bot or not after.guild or not isinstance(after.channel, discord.Thread):
            return

        try:
            # Check if this is a forum thread
            if not hasattr(after.channel, 'parent') or not isinstance(after.channel.parent, discord.ForumChannel):
                return

            with db_session_scope() as session:
                # Get global creator (check if this thread is their primary)
                creator = session.get(FeaturedCreator, after.author.id)

                if not creator:
                    return  # Not a tracked creator

                # Only update if this is their primary guild's forum thread
                if creator.forum_thread_id != after.channel.id:
                    return

                # Update bio and social links with edited content
                social_links = self._parse_social_links(after.content)

                creator.bio = after.content
                creator.updated_at = int(time.time())

                # Update social links
                if social_links.get('twitch'):
                    creator.twitch_url = social_links['twitch']
                if social_links.get('youtube'):
                    creator.youtube_url = social_links['youtube']
                if social_links.get('twitter'):
                    creator.twitter_url = social_links['twitter']
                if social_links.get('tiktok'):
                    creator.tiktok_url = social_links['tiktok']
                if social_links.get('instagram'):
                    creator.instagram_url = social_links['instagram']
                if social_links.get('bsky'):
                    creator.bsky_url = social_links['bsky']
                if social_links.get('other'):
                    creator.other_links = social_links['other']

                session.commit()
                logger.info(f"[Forum Edit] Updated GLOBAL creator {after.author.display_name} (primary guild: {creator.primary_guild_id})")

        except Exception as e:
            logger.error(f"[Forum Edit] Error updating creator: {e}", exc_info=True)

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """
        Handle when a member leaves a guild - remove guild from their creator entry.
        If they leave all guilds, mark as inactive (14-day grace period before deletion).
        """
        try:
            with db_session_scope() as session:
                marked_inactive = remove_guild_from_creator(session, member.id, member.guild.id)
                session.commit()

                if marked_inactive:
                    logger.info(f"[Member Remove] User {member.id} left all guilds, marked inactive")
                else:
                    logger.info(f"[Member Remove] User {member.id} left guild {member.guild.id}")

        except Exception as e:
            logger.error(f"[Member Remove] Error handling member removal: {e}", exc_info=True)

    @commands.Cog.listener()
    async def on_thread_create(self, thread: discord.Thread):
        """
        Handle new forum threads - add to featured pool if it's a Self-Promo Intro.
        This is the primary way to catch new forum posts (more reliable than periodic scanning).
        """
        try:
            # Only process forum threads
            if not hasattr(thread, 'parent') or not isinstance(thread.parent, discord.ForumChannel):
                return

            # Check if this is a configured intro forum
            with db_session_scope() as session:
                config = session.query(DiscoveryConfig).filter_by(
                    guild_id=thread.guild.id,
                    intro_forum_channel_id=thread.parent.id
                ).first()

                if not config or not config.forum_enabled:
                    return  # Not a configured forum or forum discovery disabled

                # Check if thread has "Self-Promo Intro" tag
                has_intro_tag = any(
                    tag.name.lower() == "self-promo intro" for tag in thread.applied_tags
                )

                if not has_intro_tag:
                    logger.debug(f"[Thread Create] Thread {thread.id} doesn't have Self-Promo Intro tag")
                    return

                # Get the starter message (pycord forum threads)
                # Try with retry logic since message might not be immediately available
                starter_message = None
                max_retries = 3

                for attempt in range(max_retries):
                    if hasattr(thread, 'starter_message') and thread.starter_message:
                        starter_message = thread.starter_message
                        break
                    else:
                        # Fetch the first message from thread history
                        async for message in thread.history(limit=1, oldest_first=True):
                            starter_message = message
                            break

                    if starter_message:
                        break

                    # If we didn't get it and have retries left, wait and try again
                    if attempt < max_retries - 1:
                        logger.debug(f"[Thread Create] Attempt {attempt + 1}/{max_retries}: Could not get starter message for thread {thread.id}, retrying...")
                        await asyncio.sleep(1.5)  # Wait 1.5 seconds before retry

                if not starter_message:
                    logger.warning(f"[Thread Create] Could not get starter message for thread {thread.id} after {max_retries} attempts")
                    return

                # Token checking for forum posts
                author = starter_message.author

                # Get or create member record
                member = session.query(GuildMember).filter_by(
                    guild_id=thread.guild.id,
                    user_id=author.id
                ).first()

                if not member:
                    logger.info(
                        f"[Thread Create] Creating new GuildMember record for {author.id} in guild {thread.guild.id}, "
                        f"display_name={author.display_name}, source=Discord.Thread.author"
                    )
                    member = GuildMember(
                        guild_id=thread.guild.id,
                        user_id=author.id,
                        xp=0,
                        level=1,
                        hero_tokens=0
                    )
                    session.add(member)
                    session.flush()

                # Check if tokens are required for forum posts
                require_tokens = getattr(config, 'require_tokens_forum', False)
                token_cost = config.token_cost_forum if hasattr(config, 'token_cost_forum') else 10
                now = int(time.time())

                if require_tokens and member.hero_tokens < token_cost:
                    # Not enough tokens - just log (no spam message)
                    logger.info(f"[Thread Create] User {author.id} doesn't have enough tokens ({member.hero_tokens}/{token_cost})")
                    return

                # Check cooldown
                cooldown_hours = getattr(config, 'entry_cooldown_hours', 24)
                cooldown_seconds = cooldown_hours * 3600

                last_entry = session.query(FeaturedPool).filter_by(
                    guild_id=thread.guild.id,
                    user_id=author.id
                ).order_by(FeaturedPool.entered_at.desc()).first()

                if last_entry and (now - last_entry.entered_at) < cooldown_seconds:
                    # Still on cooldown
                    time_left = cooldown_seconds - (now - last_entry.entered_at)
                    hours_left = int(time_left // 3600)
                    minutes_left = int((time_left % 3600) // 60)

                    # Use customizable cooldown message
                    cooldown_msg = config.cooldown_message if config.cooldown_message else (
                        "⏰ You're on cooldown! Can enter the featured pool again in {time_left}.\n"
                        "💰 Your {token_cost} hero_tokens were saved."
                    )

                    # Format the message with time_left and token_cost
                    formatted_msg = cooldown_msg.format(
                        time_left=f"**{hours_left}h {minutes_left}m**",
                        token_cost=token_cost
                    )

                    await self._send_discovery_message(
                        config=config,
                        guild=thread.guild,
                        user=author,
                        message_text=formatted_msg,
                        original_channel=thread
                    )
                    logger.info(f"[Thread Create] User {author.id} is on cooldown ({hours_left}h {minutes_left}m remaining)")
                    return

                # Deduct tokens if required
                if require_tokens:
                    member.hero_tokens -= token_cost
                    logger.info(f"[Thread Create] Deducted {token_cost} tokens from user {author.id} ({member.hero_tokens} remaining)")

                # Add to featured pool
                await self._add_forum_post_to_pool(
                    guild_id=thread.guild.id,
                    thread=thread,
                    message=starter_message,
                    config=config
                )

                logger.info(f"[Thread Create] Added new forum post from {starter_message.author.display_name} to pool (guild {thread.guild.id})")

        except Exception as e:
            logger.error(f"[Thread Create] Error handling new thread: {e}", exc_info=True)

    @commands.Cog.listener()
    async def on_thread_delete(self, thread: discord.Thread):
        """
        Handle when a forum thread is deleted - just clear the thread ID, don't mark inactive.
        Deleting a thread doesn't mean they left the guild!
        """
        try:
            # Only process forum threads
            if not hasattr(thread, 'parent') or not isinstance(thread.parent, discord.ForumChannel):
                return

            with db_session_scope() as session:
                # Find creator with this forum thread
                creator = session.query(FeaturedCreator).filter_by(
                    forum_thread_id=thread.id
                ).first()

                if not creator:
                    return  # Not a tracked thread

                # Just clear the forum thread ID - they might repost a new intro
                # Don't mark them inactive or remove the guild - they're still in the server!
                creator.forum_thread_id = None
                creator.updated_at = int(time.time())
                session.commit()

                logger.info(f"[Thread Delete] Cleared forum thread for user {creator.user_id} in guild {thread.guild.id if thread.guild else 'unknown'}")

        except Exception as e:
            logger.error(f"[Thread Delete] Error handling thread deletion: {e}", exc_info=True)

    async def select_and_feature_winner(self, guild_id: int) -> dict:
        """
        Select a random winner from the pool and feature them.
        Returns dict with success status and message.
        """
        with db_session_scope() as session:
            now = int(time.time())

            # Get discovery config
            config = session.query(DiscoveryConfig).filter_by(guild_id=guild_id).first()
            if not config or not config.enabled or not config.feature_channel_id:
                return {'success': False, 'error': 'Discovery not configured or disabled'}

            # Get active pool entries
            entries = (
                session.query(FeaturedPool)
                .filter(
                    FeaturedPool.guild_id == guild_id,
                    FeaturedPool.was_selected == False,
                    FeaturedPool.expires_at > now
                )
                .all()
            )

            if not entries:
                return {'success': False, 'error': 'No entries in pool'}

            # Check cooldown - don't feature same user twice within cooldown period
            cooldown_seconds = config.feature_cooldown_hours * 3600 if config.feature_cooldown_hours else 0
            if cooldown_seconds > 0 and config.last_featured_user_id:
                eligible_entries = []
                for entry in entries:
                    if entry.user_id != config.last_featured_user_id:
                        eligible_entries.append(entry)
                    else:
                        # Check if they've been in pool entry cooldown
                        time_since_last = now - (config.last_feature_at or 0)
                        if time_since_last >= cooldown_seconds:
                            eligible_entries.append(entry)

                if eligible_entries:
                    entries = eligible_entries

            # Select random winner
            winner = random.choice(entries)
            winner.was_selected = True
            winner.selected_at = now

            # Update config
            config.last_feature_at = now
            config.last_featured_user_id = winner.user_id

            session.commit()

            # Post feature announcement
            discord_guild = self.bot.get_guild(guild_id)
            if not discord_guild:
                return {'success': False, 'error': 'Guild not found'}

            feature_channel = discord_guild.get_channel(config.feature_channel_id)
            if not feature_channel:
                return {'success': False, 'error': 'Feature channel not found'}

            # Delete previous featured message to prevent channel clutter
            if config.last_featured_message_id:
                try:
                    old_message = await feature_channel.fetch_message(config.last_featured_message_id)
                    await old_message.delete()
                    logger.info(f"[Discovery] [guild_id:{guild_id}] Deleted previous featured message {config.last_featured_message_id}")
                except discord.NotFound:
                    logger.debug(f"[Discovery] [guild_id:{guild_id}] Previous featured message not found (already deleted or invalid ID)")
                except discord.Forbidden:
                    logger.warning(f"[Discovery] [guild_id:{guild_id}] No permission to delete previous featured message")
                except Exception as e:
                    logger.error(f"[Discovery] [guild_id:{guild_id}] Error deleting previous featured message: {e}")

            member = discord_guild.get_member(winner.user_id)
            if not member:
                return {'success': False, 'error': 'Winner not found in guild'}

            # Create feature message/embed
            if config.use_embed:
                # Get member's XP data for level
                db_member = session.query(GuildMember).filter_by(
                    guild_id=guild_id,
                    user_id=winner.user_id
                ).first()

                # Count how many times they've been featured
                times_featured = session.query(FeaturedPool).filter_by(
                    guild_id=guild_id,
                    user_id=winner.user_id,
                    was_selected=True
                ).count()

                # Get their highest role (main role)
                main_role = None
                if member.roles and len(member.roles) > 1:  # Skip @everyone
                    sorted_roles = sorted(member.roles, key=lambda r: r.position, reverse=True)
                    main_role = sorted_roles[0] if sorted_roles[0].name != "@everyone" else (sorted_roles[1] if len(sorted_roles) > 1 else None)

                # Get their current activity (game they're playing)
                current_game = None
                if member.activities:
                    for activity in member.activities:
                        if activity.type == discord.ActivityType.playing:
                            current_game = activity.name
                            break

                # Create enhanced embed
                embed = discord.Embed(
                    title="✨ Featured Creator ✨",
                    description=winner.content if winner.content else "Check out their content!",
                    color=0xFFD700,  # Gold
                    timestamp=discord.utils.utcnow()
                )

                # Set author with name (without icon for cleaner look)
                embed.set_author(name=member.display_name)

                # Link field
                if winner.link_url:
                    embed.add_field(name="🔗 Link", value=winner.link_url, inline=False)

                # Platform field (detect from URL if not set correctly)
                platform_display = winner.platform
                if winner.link_url:
                    link_lower = winner.link_url.lower()
                    if 'twitch.tv' in link_lower:
                        platform_display = 'twitch'
                    elif 'youtube.com' in link_lower or 'youtu.be' in link_lower:
                        platform_display = 'youtube'
                    elif 'twitter.com' in link_lower or 'x.com' in link_lower:
                        platform_display = 'twitter'
                    elif 'tiktok.com' in link_lower:
                        platform_display = 'tiktok'

                if platform_display:
                    platform_emoji = {
                        'youtube': '📺',
                        'twitch': '🎮',
                        'twitter': '🐦',
                        'tiktok': '🎵',
                        'kick': '⚽',
                        'discord': '💬'
                    }.get(platform_display.lower(), '🌐')
                    embed.add_field(
                        name="Platform",
                        value=f"{platform_emoji} {platform_display.title()}",
                        inline=True
                    )

                # Level field
                if db_member:
                    embed.add_field(
                        name="🏆 Level",
                        value=str(db_member.level),
                        inline=True
                    )

                # Role Flair field
                if main_role:
                    embed.add_field(
                        name="👑 Role Flair",
                        value=main_role.mention,
                        inline=True
                    )

                # Current game field
                if current_game:
                    embed.add_field(
                        name="🎮 Currently Playing",
                        value=current_game,
                        inline=False
                    )

                # Times featured field
                embed.add_field(
                    name="⭐ Times Featured",
                    value=f"{times_featured}x",
                    inline=True
                )

                # Featured timestamp
                embed.add_field(
                    name="📅 Featured",
                    value=f"<t:{winner.selected_at}:R>",
                    inline=True
                )

                # Set profile picture at bottom for bigger, cleaner look
                embed.set_image(url=member.display_avatar.url)

                embed.set_footer(text=f"🎉 Congratulations!")

                try:
                    msg = await feature_channel.send(
                        content=config.feature_message.replace('{user}', member.mention) if config.feature_message else f"🎉 Shoutout to {member.mention}!",
                        embed=embed
                    )
                except Exception as e:
                    logger.error(f"Failed to send feature embed: {e}")
                    return {'success': False, 'error': str(e)}
            else:
                # Plain text message
                message = config.feature_message.replace('{user}', member.mention) if config.feature_message else f"Shoutout to {member.mention}!"
                if winner.content:
                    message += f"\n\n{winner.content}"
                if winner.link_url:
                    message += f"\n🔗 {winner.link_url}"

                try:
                    msg = await feature_channel.send(message)
                except Exception as e:
                    logger.error(f"Failed to send feature message: {e}")
                    return {'success': False, 'error': str(e)}

            # Save message ID to both FeaturedPool entry and DiscoveryConfig
            with db_session_scope() as session2:
                winner_refresh = session2.query(FeaturedPool).filter_by(id=winner.id).first()
                if winner_refresh:
                    winner_refresh.featured_message_id = msg.id

                # Save to config for deletion next time
                config_refresh = session2.query(DiscoveryConfig).filter_by(guild_id=guild_id).first()
                if config_refresh:
                    config_refresh.last_featured_message_id = msg.id

                session2.commit()

            # Add creator to hall of fame (permanent record for website)
            await self._add_to_featured_creators_hall(guild_id, member, winner)

            logger.info(f"[Discovery] [guild_id:{guild_id}] Featured {member.display_name} (message ID: {msg.id})")
            return {'success': True, 'winner': member.display_name}

    @tasks.loop(hours=6)
    async def featured_selection_task(self):
        """
        Select random winners from the featured pool every 6 hours.
        """
        logger.info("Running featured pool selection task...")

        with db_session_scope() as session:
            now = int(time.time())

            # Get all guilds with Discovery enabled and feature interval reached
            configs = session.query(DiscoveryConfig).filter(
                DiscoveryConfig.enabled == True,
                DiscoveryConfig.feature_channel_id != None
            ).all()

            for config in configs:
                try:
                    # Check if it's time to feature (based on interval)
                    if config.last_feature_at:
                        time_since_last = now - config.last_feature_at
                        interval_seconds = (config.feature_interval_hours or 3) * 3600
                        if time_since_last < interval_seconds:
                            continue  # Too soon

                    # Feature a winner
                    result = await self.select_and_feature_winner(config.guild_id)
                    if result['success']:
                        logger.info(f"Auto-featured winner in guild {config.guild_id}: {result.get('winner')}")
                    else:
                        logger.debug(f"Could not feature in guild {config.guild_id}: {result.get('error')}")

                except Exception as e:
                    logger.error(f"Error processing featured pool for guild {config.guild_id}: {e}", exc_info=True)

        logger.info("Featured pool selection task completed")

    @featured_selection_task.before_loop
    async def before_featured_task(self):
        await self.bot.wait_until_ready()

        # On bot reboot, check if we need to wait before running the first feature
        try:
            with db_session_scope() as session:
                now = int(time.time())

                # Get all configs with feature enabled
                configs = session.query(DiscoveryConfig).filter(
                    DiscoveryConfig.enabled == True,
                    DiscoveryConfig.feature_channel_id != None
                ).all()

                max_wait_time = 0

                for config in configs:
                    if config.last_feature_at:
                        time_since_last = now - config.last_feature_at
                        interval_seconds = (config.feature_interval_hours or 3) * 3600

                        # If interval hasn't passed yet, calculate how long to wait
                        if time_since_last < interval_seconds:
                            wait_time = interval_seconds - time_since_last
                            max_wait_time = max(max_wait_time, wait_time)

                # Wait until it's time to feature (if needed)
                if max_wait_time > 0:
                    logger.info(f"[Discovery] Waiting {max_wait_time}s before first feature selection (respecting interval)")
                    await asyncio.sleep(max_wait_time)
                else:
                    logger.info(f"[Discovery] Feature interval has passed, will run feature selection immediately")

        except Exception as e:
            logger.error(f"[Discovery] Error calculating initial wait time: {e}")
            # If there's an error, just proceed normally
            pass

    # ========== GAME DISCOVERY TASK ==========

    def create_game_announcement_embed(self, game: igdb.IGDBGame) -> discord.Embed:
        """Create a beautiful embed for a new game announcement with rich data."""
        # Get description
        description = game.summary[:500] + "..." if game.summary and len(game.summary) > 500 else game.summary or "No description available."

        # Add hype and rating to description if available
        stats_line = []
        if hasattr(game, 'hypes') and game.hypes:
            stats_line.append(f"🔥 **Hype:** {game.hypes:,} follows")
        if hasattr(game, 'rating') and game.rating:
            stats_line.append(f"⭐ **Rating:** {game.rating:.1f}")
        if stats_line:
            description = " | ".join(stats_line) + "\n\n" + description

        embed = discord.Embed(
            title=f"🎮 New Game Alert: {game.name}",
            description=description,
            color=discord.Color.blue(),
            url=game.igdb_url
        )

        # Add cover image (or first screenshot if no cover)
        if game.cover_url:
            embed.set_image(url=game.cover_url)
        elif hasattr(game, 'screenshots') and game.screenshots:
            embed.set_image(url=game.screenshots[0])

        # Release date
        if game.release_date:
            release_dt = datetime.fromtimestamp(game.release_date)
            embed.add_field(
                name="📅 Release Date",
                value=release_dt.strftime("%B %d, %Y"),
                inline=True
            )

        # Genres
        if hasattr(game, 'genres') and game.genres:
            embed.add_field(
                name="🎯 Genres",
                value=", ".join(game.genres[:3]),  # Limit to 3 genres
                inline=True
            )

        # Platforms
        if game.platforms:
            platforms_str = ", ".join(game.platforms[:5])  # Limit to 5 platforms
            if len(game.platforms) > 5:
                platforms_str += f" +{len(game.platforms) - 5} more"
            embed.add_field(
                name="🎮 Platforms",
                value=platforms_str,
                inline=True
            )

        # Game Modes
        if hasattr(game, 'game_modes') and game.game_modes:
            embed.add_field(
                name="👥 Game Modes",
                value=", ".join(game.game_modes),
                inline=True
            )

        # Add store links if available
        if hasattr(game, 'websites') and game.websites:
            store_links = []
            for website in game.websites:
                category = website.get("category")
                url = website.get("url")
                # Category mappings: 1=official, 13=steam, 16=epicgames, 17=gog
                if category == 13:  # Steam
                    store_links.append(f"[Steam]({url})")
                elif category == 16:  # Epic Games
                    store_links.append(f"[Epic]({url})")
                elif category == 17:  # GOG
                    store_links.append(f"[GOG]({url})")
                elif category == 1:  # Official website
                    store_links.append(f"[Official Site]({url})")

            if store_links:
                embed.add_field(
                    name="🔗 Links",
                    value=" | ".join(store_links[:4]),  # Limit to 4 links
                    inline=False
                )

        # Add video links if available
        if hasattr(game, 'videos') and game.videos:
            video_links = []
            for video in game.videos[:2]:  # Limit to 2 videos
                video_id = video.get("video_id")
                name = video.get("name", "Trailer")
                if video_id:
                    video_links.append(f"[{name}](https://www.youtube.com/watch?v={video_id})")

            if video_links:
                embed.add_field(
                    name="🎬 Videos",
                    value=" | ".join(video_links),
                    inline=False
                )

        # Add thumbnail from screenshots if available (and we used cover for main image)
        if game.cover_url and hasattr(game, 'screenshots') and game.screenshots:
            embed.set_thumbnail(url=game.screenshots[0])

        # Footer
        embed.set_footer(text="Powered by IGDB | Game Discovery")

        return embed

    def _build_game_summary_embed(self, guild_id: int, total_found: int, search_count: int, announced_count: int, target_channel: discord.abc.Messageable = None):
        dash_url = f"{DASHBOARD_BASE_URL}/questlog/guild/{guild_id}/found-games/"
        channel_text = target_channel.mention if target_channel else "this channel"
        embed = discord.Embed(
            title="✅ Game Discovery Check Complete",
            description=f"Found {total_found} games across {search_count} search configurations.",
            color=discord.Color.blue()
        )
        embed.add_field(name="📢 Announced", value=f"{announced_count} games to {channel_text}", inline=True)
        embed.add_field(name="💾 Saved", value=f"{total_found} games total", inline=True)
        embed.add_field(name="🔗 View Public Games", value=f"[Click here to view all {total_found} games on the dashboard]({dash_url})", inline=False)
        embed.set_footer(text="Public games → Dashboard • Private games → Discord threads")
        return embed

    @tasks.loop(minutes=15)
    async def game_discovery_task(self):
        """
        Check for new game releases based on guild search configurations.
        Runs every 15 minutes (guilds control their own interval via config).
        Uses IGDB for comprehensive game discovery with multiple saved searches.
        """
        logger.info("Running game discovery task...")

        # Skip the first loop after a bot restart to avoid immediate re-announcements;
        # stamp last_game_check_at to now so intervals resume from this boot time.
        if not hasattr(self, "_game_discovery_boot_skipped"):
            with db_session_scope() as session:
                now_stamp = int(time.time())
                session.query(DiscoveryConfig).filter(
                    DiscoveryConfig.game_discovery_enabled == True
                ).update({'last_game_check_at': now_stamp})
                session.commit()
            self._game_discovery_boot_skipped = True
            logger.info("First discovery loop after boot skipped; intervals reset to now.")
            return

        with db_session_scope() as session:
            now = int(time.time())

            # Get all guilds with game discovery enabled and at least one channel configured
            configs = session.query(DiscoveryConfig).filter(
                DiscoveryConfig.game_discovery_enabled == True
            ).all()

            # Filter to only configs with at least one channel configured
            configs = [c for c in configs if c.public_game_channel_id or c.private_game_channel_id]

            logger.info(f"Found {len(configs)} guilds with game discovery enabled")

            for config in configs:
                try:
                    # On first boot, avoid immediate re-run if we've checked recently
                    if not config.last_game_check_at:
                        config.last_game_check_at = now
                        session.commit()
                        continue

                    # Check if it's time to check (based on interval)
                    if config.last_game_check_at:
                        time_since_last = now - config.last_game_check_at
                        interval_seconds = (config.game_check_interval_hours or 24) * 3600
                        hours_since = time_since_last / 3600
                        interval_hours = interval_seconds / 3600

                        if time_since_last < interval_seconds:
                            logger.info(f"Skipping guild {config.guild_id}: checked {hours_since:.2f}h ago (interval: {interval_hours:.0f}h)")
                            continue
                        else:
                            logger.info(f"Proceeding with guild {config.guild_id}: {hours_since:.2f}h since last check (interval: {interval_hours:.0f}h)")

                    logger.info(f"Checking games for guild {config.guild_id}")

                    # Get the guild
                    guild = self.bot.get_guild(config.guild_id)
                    if not guild:
                        logger.warning(f"Could not find guild {config.guild_id}")
                        continue

                    # Get the ONE discovery channel for all announcements
                    discovery_channel = guild.get_channel(config.public_game_channel_id) if config.public_game_channel_id else None

                    if not discovery_channel:
                        logger.warning(f"Could not find game discovery channel in guild {config.guild_id}")
                        continue

                    # Check if IGDB is configured
                    if not igdb.is_configured():
                        logger.warning(f"IGDB not configured, skipping guild {config.guild_id}")
                        continue

                    # Get all enabled search configurations for this guild
                    search_configs = session.query(GameSearchConfig).filter(
                        GameSearchConfig.guild_id == config.guild_id,
                        GameSearchConfig.enabled == True
                    ).all()

                    if not search_configs:
                        logger.info(f"No enabled search configs for guild {config.guild_id}, skipping")
                        continue

                    logger.info(f"Found {len(search_configs)} enabled search configs for guild {config.guild_id}")

                    # Track announced games to avoid duplicates
                    announced_count = 0
                    all_games_to_announce = {}  # Key: IGDB ID, Value: game object

                    # Run each search configuration
                    for search_config in search_configs:
                        try:
                            logger.info(f"Running search '{search_config.name}' for guild {config.guild_id}")

                            # Parse filters
                            genres = json.loads(search_config.genres) if search_config.genres else None
                            themes = json.loads(search_config.themes) if search_config.themes else None
                            modes = json.loads(search_config.game_modes) if search_config.game_modes else None
                            platforms = json.loads(search_config.platforms) if search_config.platforms else None
                            announcement_window = search_config.days_ahead or 30
                            min_hype = search_config.min_hype
                            min_rating = search_config.min_rating

                            logger.info(f"Search '{search_config.name}' filters - Genres: {genres}, Themes: {themes}, Modes: {modes}, Platforms: {platforms}")

                            # Fetch games from IGDB
                            games = await igdb.search_upcoming_games(
                                days_ahead=365,  # Search wide window
                                days_behind=0,
                                genres=genres,
                                themes=themes,
                                game_modes=modes,
                                platforms=platforms,
                                min_hype=min_hype,
                                min_rating=min_rating,
                                limit=100
                            )

                            logger.info(f"Search '{search_config.name}' found {len(games)} games from IGDB")

                            # Filter to announcement window
                            announcement_cutoff = now + (announcement_window * 24 * 60 * 60)
                            games = [g for g in games if g.release_date and g.release_date <= announcement_cutoff]
                            logger.info(f"Filtered to {len(games)} games within {announcement_window} day window")

                            # Add to master list (avoid duplicates across searches), track privacy
                            for game in games:
                                if game.id not in all_games_to_announce:
                                    all_games_to_announce[game.id] = {
                                        "game": game,
                                        "is_public": bool(search_config.show_on_website),
                                    }

                        except Exception as e:
                            logger.error(f"Error running search '{search_config.name}': {e}")
                            continue

                    logger.info(f"Total unique games found across all searches: {len(all_games_to_announce)}")

                    # Filter to ONLY games with "Share on Discovery Network" enabled
                    games_to_announce = []
                    announced_count = 0

                    for game_id, meta in all_games_to_announce.items():
                        game = meta["game"]
                        is_public = meta["is_public"]

                        # ONLY announce games with "Share on Discovery Network" enabled
                        if not is_public:
                            logger.info(f"Skipping server-only game '{game.name}' (IGDB:{game.id}) - not shared on Discovery Network")
                            continue

                        # Check if already announced
                        already_announced = session.query(AnnouncedGame).filter(
                            AnnouncedGame.guild_id == config.guild_id,
                            AnnouncedGame.igdb_id == game.id
                        ).first()

                        if already_announced:
                            continue

                        games_to_announce.append(game)

                        # Record announcement in database (without posting individual messages)
                        try:
                            announced = AnnouncedGame(
                                guild_id=config.guild_id,
                                igdb_id=game.id,
                                igdb_slug=game.slug if hasattr(game, 'slug') else None,
                                steam_id=None,
                                game_name=game.name,
                                release_date=game.release_date,
                                genres=json.dumps(game.genres) if hasattr(game, 'genres') else None,
                                platforms=json.dumps(game.platforms),
                                cover_url=game.cover_url,
                                announced_at=now,
                                announcement_message_id=None  # No individual message
                            )
                            session.add(announced)
                            announced_count += 1
                            logger.info(f"Recorded game '{game.name}' (IGDB:{game.id}) in guild {config.guild_id}")
                        except Exception as e:
                            logger.error(f"Failed to record game '{game.name}' in guild {config.guild_id}: {e}")
                            continue

                    # Send ONE summary embed for all games shared on Discovery Network
                    if games_to_announce and discovery_channel:
                        try:
                            dash_url = f"{DASHBOARD_BASE_URL}/questlog/guild/{config.guild_id}/found-games/"
                            summary_embed = discord.Embed(
                                title="🎮 New Games Discovered!",
                                description=f"Found **{announced_count}** new game{'s' if announced_count != 1 else ''} matching your searches!",
                                color=discord.Color.green()
                            )
                            summary_embed.add_field(
                                name="📊 View All Games",
                                value=f"[Click here to view all {announced_count} games on the dashboard]({dash_url})",
                                inline=False
                            )
                            summary_embed.set_footer(text=f"Based on {len(search_configs)} active search configuration{'s' if len(search_configs) != 1 else ''}")

                            # Add role ping if configured
                            ping_content = None
                            if config.public_game_ping_role_id:
                                ping_content = f"<@&{config.public_game_ping_role_id}>"

                            await discovery_channel.send(content=ping_content, embed=summary_embed)
                            logger.info(f"Sent game summary for {announced_count} games in guild {config.guild_id}")
                        except Exception as e:
                            logger.warning(f"Failed to send game summary in guild {config.guild_id}: {e}")

                    logger.info(f"Game discovery for guild {config.guild_id} complete. Announced {announced_count} new games.")

                    # Update last check time ONLY after successful processing
                    # (not in finally block, which runs even when skipping via continue)
                    try:
                        old_timestamp = config.last_game_check_at
                        config.last_game_check_at = now
                        session.flush()
                        session.commit()
                        session.refresh(config)
                        logger.info(f"Updated last_game_check_at for guild {config.guild_id}: {old_timestamp} → {now}")
                    except Exception as update_error:
                        logger.error(f"Failed to update last_game_check_at for guild {config.guild_id}: {update_error}")

                except Exception as e:
                    logger.error(f"Error processing game discovery for guild {config.guild_id}: {e}", exc_info=True)
                    # On error, still update timestamp to prevent rapid retries
                    try:
                        config.last_game_check_at = now
                        session.commit()
                    except:
                        pass

        logger.info("Game discovery task completed")

    @game_discovery_task.before_loop
    async def before_game_discovery_task(self):
        await self.bot.wait_until_ready()

    # ========== FORUM-BASED CREATOR DISCOVERY ==========

    @tasks.loop(minutes=5)
    async def forum_scanner_task(self):
        """
        Scan intro forum channels for creator posts (checks every 5 minutes, scans per-guild based on configured interval).

        Rate Limit Strategy:
        - Runs every 5 minutes
        - Each guild has configurable scan interval (default 1 hour)
        - Processes guilds in batches of 10
        - 6-second delay between batches
        - Can handle 1000+ guilds in ~10 minutes
        """
        with db_session_scope() as session:
            # Get all guilds with intro forum configured
            configs = session.query(DiscoveryConfig).filter(
                DiscoveryConfig.intro_forum_channel_id != None
            ).all()

            if not configs:
                return

            # Filter configs that are ready for scanning based on their interval
            now = int(time.time())
            ready_configs = []

            for config in configs:
                interval_seconds = (config.intro_scan_interval_hours or 1) * 3600
                last_scan = config.last_intro_scan_at or 0

                if now - last_scan >= interval_seconds:
                    ready_configs.append(config)

            if not ready_configs:
                return

            logger.info(f"[Forum Scanner] Found {len(ready_configs)} guilds ready for forum scan")

            # Process in batches to avoid rate limits
            batch_size = 10
            processed_count = 0
            error_count = 0

            for i in range(0, len(ready_configs), batch_size):
                batch = ready_configs[i:i+batch_size]

                for config in batch:
                    try:
                        await self._scan_guild_intro_forum(config.guild_id)
                        processed_count += 1
                    except Exception as e:
                        logger.error(f"[Forum Scanner] Error for guild {config.guild_id}: {e}", exc_info=True)
                        error_count += 1

                # Delay between batches (6 seconds)
                if i + batch_size < len(ready_configs):
                    await asyncio.sleep(6)

            logger.info(f"[Forum Scanner] Completed scan: {processed_count} successful, {error_count} errors")

    @forum_scanner_task.before_loop
    async def before_forum_scanner_task(self):
        await self.bot.wait_until_ready()

    @tasks.loop(hours=168)  # Weekly (7 days)
    async def creator_of_week_task(self):
        """Select and announce Creator of the Week (runs weekly, PRO feature)."""
        logger.info("[COTW] Starting weekly Creator of the Week selection...")

        with db_session_scope() as session:
            # Get all guilds with COTW enabled (Pro tier+)
            configs = session.query(DiscoveryConfig).filter(
                DiscoveryConfig.cotw_enabled == True,
                DiscoveryConfig.cotw_channel_id != None
            ).all()

            if not configs:
                logger.info("[COTW] No guilds with COTW enabled")
                return

            for config in configs:
                guild_id = config.guild_id  # Capture guild_id before it might become detached
                try:
                    # Check if guild has Discovery module or Complete tier
                    guild_record = session.query(Guild).filter_by(guild_id=guild_id).first()
                    if not guild_record:
                        logger.warning(f"[COTW] Guild {guild_id} not found in database")
                        continue

                    # Check for Discovery module subscription or Complete tier
                    has_discovery_module = session.query(GuildModule).filter_by(
                        guild_id=guild_id,
                        module_name='discovery',
                        enabled=True
                    ).first() is not None

                    has_access = guild_record.subscription_tier == 'complete' or has_discovery_module

                    if not has_access:
                        logger.warning(f"[COTW] Guild {guild_id} has COTW enabled but no Discovery module or Complete tier")
                        continue

                    await self._select_and_announce_cotw(guild_id)
                    await asyncio.sleep(2)  # Small delay between guilds

                except Exception as e:
                    logger.error(f"[COTW] Error for guild {guild_id}: {e}", exc_info=True)

        logger.info("[COTW] Completed weekly COTW selection")

    @creator_of_week_task.before_loop
    async def before_creator_of_week_task(self):
        await self.bot.wait_until_ready()

    @tasks.loop(hours=720)  # Monthly (30 days)
    async def creator_of_month_task(self):
        """Select and announce Creator of the Month (runs monthly, PREMIUM feature)."""
        logger.info("[COTM] Starting monthly Creator of the Month selection...")

        with db_session_scope() as session:
            # Get all guilds with COTM enabled (Premium tier)
            configs = session.query(DiscoveryConfig).filter(
                DiscoveryConfig.cotm_enabled == True,
                DiscoveryConfig.cotm_channel_id != None
            ).all()

            if not configs:
                logger.info("[COTM] No guilds with COTM enabled")
                return

            for config in configs:
                guild_id = config.guild_id  # Capture guild_id before it might become detached
                try:
                    # Check if guild has Discovery module or Complete tier
                    guild_record = session.query(Guild).filter_by(guild_id=guild_id).first()
                    if not guild_record:
                        logger.warning(f"[COTM] Guild {guild_id} not found in database")
                        continue

                    # Check for Discovery module subscription or Complete tier
                    has_discovery_module = session.query(GuildModule).filter_by(
                        guild_id=guild_id,
                        module_name='discovery',
                        enabled=True
                    ).first() is not None

                    has_access = guild_record.subscription_tier == 'complete' or has_discovery_module

                    if not has_access:
                        logger.warning(f"[COTM] Guild {guild_id} has COTM enabled but no Discovery module or Complete tier")
                        continue

                    await self._select_and_announce_cotm(guild_id)
                    await asyncio.sleep(2)  # Small delay between guilds

                except Exception as e:
                    logger.error(f"[COTM] Error for guild {guild_id}: {e}", exc_info=True)

        logger.info("[COTM] Completed monthly COTM selection")

    @creator_of_month_task.before_loop
    async def before_creator_of_month_task(self):
        await self.bot.wait_until_ready()

    @tasks.loop(hours=24)  # Run daily
    async def cleanup_inactive_creators_task(self):
        """
        Cleanup task: Delete creators who have been inactive for 14+ days.
        A creator is marked inactive when they leave all guilds.
        """
        logger.info("[Cleanup] Starting inactive creators cleanup...")

        GRACE_PERIOD_SECONDS = 14 * 24 * 3600  # 14 days
        now = int(time.time())

        with db_session_scope() as session:
            # Find creators who have been inactive for 14+ days
            expired_creators = session.query(FeaturedCreator).filter(
                FeaturedCreator.is_active == False,
                FeaturedCreator.inactive_since != None,
                FeaturedCreator.inactive_since < (now - GRACE_PERIOD_SECONDS)
            ).all()

            if not expired_creators:
                logger.info("[Cleanup] No expired creators to delete")
                return

            # Delete expired creators
            for creator in expired_creators:
                logger.info(f"[Cleanup] Deleting creator {creator.user_id} (inactive since {creator.inactive_since})")
                session.delete(creator)

            session.commit()
            logger.info(f"[Cleanup] Deleted {len(expired_creators)} expired creators")

    @cleanup_inactive_creators_task.before_loop
    async def before_cleanup_inactive_creators_task(self):
        await self.bot.wait_until_ready()

    async def _scan_guild_intro_forum(self, guild_id: int):
        """Scan a guild's intro forum and add/update creators."""
        with db_session_scope() as session:
            config = session.query(DiscoveryConfig).filter_by(guild_id=guild_id).first()

            if not config or not config.intro_forum_channel_id:
                return

            guild = self.bot.get_guild(guild_id)
            if not guild:
                return

            forum = guild.get_channel(config.intro_forum_channel_id)
            if not forum or not isinstance(forum, discord.ForumChannel):
                logger.warning(f"[Forum Scanner] Invalid forum channel for guild {guild_id}")
                return

            # Scan forum threads (pycord only has cached threads in forum.threads)
            # For better coverage, also listen to on_thread_create event
            processed = 0
            try:
                all_threads = list(forum.threads)
                logger.info(f"[Forum Scanner] Found {len(all_threads)} cached threads in forum {forum.name} ({forum.id})")

                for thread in all_threads:
                    # Debug: log thread info
                    tag_names = [tag.name for tag in thread.applied_tags]
                    logger.debug(f"[Forum Scanner] Thread: {thread.name} ({thread.id}), Tags: {tag_names}, Archived: {thread.archived}")

                    # Check if thread has "Self-Promo Intro" tag (case-insensitive)
                    has_intro_tag = any(
                        tag.name.lower() == "self-promo intro" for tag in thread.applied_tags
                    )

                    if not has_intro_tag:
                        continue

                    # Skip archived threads
                    if thread.archived:
                        continue

                    try:
                        # Check if already in pool FIRST to avoid unnecessary API calls
                        existing = session.query(FeaturedPool).filter_by(
                            guild_id=guild_id,
                            forum_thread_id=thread.id
                        ).first()

                        if existing:
                            logger.debug(f"[Forum Scanner] Thread {thread.id} already in pool, skipping")
                            continue

                        # Get starter message (pycord forum threads)
                        # Try with retry logic since message might not be immediately available
                        starter_message = None
                        max_retries = 2  # Fewer retries for background scanner

                        for attempt in range(max_retries):
                            if hasattr(thread, 'starter_message') and thread.starter_message:
                                starter_message = thread.starter_message
                                break
                            else:
                                # Fetch the first message from thread history
                                async for message in thread.history(limit=1, oldest_first=True):
                                    starter_message = message
                                    break

                            if starter_message:
                                break

                            # If we didn't get it and have retries left, wait and try again
                            if attempt < max_retries - 1:
                                logger.debug(f"[Forum Scanner] Attempt {attempt + 1}/{max_retries}: Could not get starter message for thread {thread.id}, retrying...")
                                await asyncio.sleep(1.0)

                        if not starter_message:
                            logger.debug(f"[Forum Scanner] Could not get starter message for thread {thread.id} after {max_retries} attempts")
                            continue

                        # Get member record to check tokens
                        author = starter_message.author
                        member = session.query(GuildMember).filter_by(
                            guild_id=guild_id,
                            user_id=author.id
                        ).first()

                        if not member:
                            logger.debug(f"[Forum Scanner] No member record for user {author.id}, skipping")
                            continue

                        # Check if tokens are required
                        require_tokens = getattr(config, 'require_tokens_forum', False)
                        token_cost = config.token_cost_forum if hasattr(config, 'token_cost_forum') else 10

                        if require_tokens and member.hero_tokens < token_cost:
                            logger.debug(f"[Forum Scanner] User {author.id} doesn't have enough tokens ({member.hero_tokens}/{token_cost}), skipping")
                            continue

                        # Check cooldown
                        now = int(time.time())
                        cooldown_hours = getattr(config, 'entry_cooldown_hours', 24)
                        cooldown_seconds = cooldown_hours * 3600

                        last_entry = session.query(FeaturedPool).filter_by(
                            guild_id=guild_id,
                            user_id=author.id
                        ).order_by(FeaturedPool.entered_at.desc()).first()

                        if last_entry and (now - last_entry.entered_at) < cooldown_seconds:
                            logger.debug(f"[Forum Scanner] User {author.id} is on cooldown, skipping")
                            continue

                        # Deduct tokens if required
                        if require_tokens:
                            member.hero_tokens -= token_cost
                            logger.info(f"[Forum Scanner] Deducted {token_cost} tokens from user {author.id} ({member.hero_tokens} remaining)")

                        # Add to featured pool
                        await self._add_forum_post_to_pool(
                            guild_id=guild_id,
                            thread=thread,
                            message=starter_message,
                            config=config
                        )
                        processed += 1
                        logger.info(f"[Forum Scanner] Added user {author.id} from thread {thread.id} to pool")

                    except Exception as e:
                        logger.error(f"[Forum Scanner] Error processing thread {thread.id}: {e}", exc_info=True)

                if processed > 0:
                    logger.info(f"[Forum Scanner] Processed {processed} intro threads for guild {guild_id}")
                else:
                    logger.info(f"[Forum Scanner] No unprocessed intro threads found for guild {guild_id}")

            except Exception as e:
                logger.error(f"[Forum Scanner] Error fetching threads for guild {guild_id}: {e}", exc_info=True)

            # Update last scan time
            config.last_intro_scan_at = int(time.time())
            session.commit()

    async def _send_discovery_message(self, config, guild, user, message_text, original_channel=None, delete_after=300):
        """
        Send a discovery message either to the response channel or as a reply.

        Args:
            config: DiscoveryConfig object
            guild: discord.Guild
            user: discord.User or discord.Member
            message_text: The formatted message to send
            original_channel: The channel/thread where the action happened (for replies)
            delete_after: Seconds until message auto-deletes (default 300 = 5 minutes)

        Returns:
            The sent message object or None
        """
        try:
            # Check if message_response_channel_id is set
            if config.message_response_channel_id:
                # Send to response channel with username (not mention)
                response_channel = guild.get_channel(config.message_response_channel_id)
                if response_channel:
                    # Include username in message
                    full_message = f"**{user.display_name}** {message_text}"
                    msg = await response_channel.send(full_message)
                    # Delete after specified seconds
                    await asyncio.sleep(delete_after)
                    try:
                        await msg.delete()
                    except:
                        pass  # Message might already be deleted
                    return msg
                else:
                    logger.warning(f"Message response channel {config.message_response_channel_id} not found in guild {guild.id}")

            # Fallback: reply in original channel with mention (if original_channel provided)
            if original_channel:
                # Check if it's a TextChannel/Thread (has send method)
                if hasattr(original_channel, 'send'):
                    msg = await original_channel.send(f"{user.mention} {message_text}")
                    await asyncio.sleep(delete_after)
                    try:
                        await msg.delete()
                    except:
                        pass
                    return msg

            return None
        except Exception as e:
            logger.error(f"Error sending discovery message: {e}", exc_info=True)
            return None

    async def _add_forum_post_to_pool(self, guild_id: int, thread: discord.Thread, message: discord.Message, config=None):
        """
        Add forum intro post to the featured pool for random selection.
        This replaces direct Hall of Fame addition.
        """
        author = message.author
        content = message.content[:1000]  # Limit to 1000 chars

        # Extract links
        links = extract_links(content)
        link_url = links[0] if links else None
        platform = detect_platform(link_url) if link_url else 'forum'

        with db_session_scope() as session:
            now = int(time.time())

            # Check if this thread is already in the pool
            existing = session.query(FeaturedPool).filter_by(
                guild_id=guild_id,
                user_id=author.id,
                forum_thread_id=thread.id
            ).first()

            if existing:
                # Update existing pool entry
                existing.content = content
                existing.link_url = link_url
                existing.platform = platform
                existing.updated_at = now
                logger.debug(f"[Forum Pool] Updated pool entry for {author.display_name} in guild {guild_id}")
            else:
                # Add to pool with long duration (forum posts don't expire)
                pool_duration = config.pool_entry_duration_hours if config else 720  # Default 30 days for forum
                pool_entry = FeaturedPool(
                    guild_id=guild_id,
                    user_id=author.id,
                    content=content,
                    link_url=link_url,
                    platform=platform,
                    entered_at=now,
                    expires_at=now + (pool_duration * 3600),
                    forum_thread_id=thread.id,
                    was_selected=False
                )
                session.add(pool_entry)
                logger.info(f"[Forum Pool] Added {author.display_name} to featured pool from forum (guild {guild_id})")

                # Send post response message to the thread ONLY if it's recent (within last hour)
                # This prevents spam on bot restart when scanning old threads
                if config and config.post_response:
                    message_age = now - int(message.created_at.timestamp())
                    if message_age < 3600:  # Only send if message is less than 1 hour old
                        try:
                            await self._send_discovery_message(
                                config=config,
                                guild=thread.guild,
                                user=author,
                                message_text=config.post_response,
                                original_channel=thread
                            )
                            logger.debug(f"[Forum Pool] Sent post response to thread {thread.id}")
                        except Exception as e:
                            logger.error(f"[Forum Pool] Failed to send post response: {e}")
                    else:
                        logger.debug(f"[Forum Pool] Skipped post response for old thread {thread.id} (age: {message_age}s)")

            session.commit()

    async def _process_forum_creator(self, guild_id: int, thread: discord.Thread, message: discord.Message, config=None):
        """
        DEPRECATED: This function is no longer used for forum scanning.
        Forum posts now go to FeaturedPool first, then to Hall of Fame when selected.

        Kept for potential manual creator addition in the future.
        """
        author = message.author
        content = message.content

        # Parse social links from content
        social_links = self._parse_social_links(content)

        # Get tag names
        tag_name = None
        if thread.applied_tags:
            tag_name = ", ".join([tag.name for tag in thread.applied_tags])

        with db_session_scope() as session:
            now = int(time.time())

            # Get or create global creator entry
            creator, is_new_creator = get_or_create_global_creator(session, author.id, guild_id)

            # Update creator data from forum post
            creator.avatar_url = author.display_avatar.url
            creator.display_name = author.display_name
            creator.username = author.name
            creator.bio = content
            creator.source = 'forum'
            creator.updated_at = now

            # Set first_featured_at if new
            if is_new_creator:
                creator.first_featured_at = now

            # If this is their primary guild OR auto_select enabled, update forum data
            if creator.primary_guild_id == guild_id or creator.auto_select_primary:
                creator.forum_thread_id = thread.id
                creator.forum_tag_name = tag_name
                creator.last_featured_at = now

            # Update social links (only if new ones provided)
            if social_links.get('twitch'):
                creator.twitch_url = social_links['twitch']
            if social_links.get('youtube'):
                creator.youtube_url = social_links['youtube']
            if social_links.get('twitter'):
                creator.twitter_url = social_links['twitter']
            if social_links.get('tiktok'):
                creator.tiktok_url = social_links['tiktok']
            if social_links.get('instagram'):
                creator.instagram_url = social_links['instagram']
            if social_links.get('bsky'):
                creator.bsky_url = social_links['bsky']
            if social_links.get('other'):
                creator.other_links = social_links['other']

            session.commit()

            action = "Added new" if is_new_creator else "Updated"
            logger.info(f"[Forum Scanner] {action} GLOBAL creator {author.display_name} (guild {guild_id}, primary: {creator.primary_guild_id})")
            return is_new_creator

    async def _post_quick_feature_embed(
        self,
        guild: discord.Guild,
        channel: discord.TextChannel,
        author: discord.Member,
        content: str,
        link_url: str = None,
        platform: str = 'other'
    ):
        """Post a quick celebratory embed for self-promo posts (Discord-only feature)."""
        try:
            # Platform emoji mapping
            platform_emojis = {
                'twitch': '🎮',
                'youtube': '📺',
                'twitter': '🐦',
                'tiktok': '🎵',
                'instagram': '📸',
                'bsy': '🦋',
                'other': '🔗'
            }

            emoji = platform_emojis.get(platform, '🔗')

            embed = discord.Embed(
                title=f"{emoji} Featured Creator Spotlight!",
                description=f"**{author.display_name}** just shared their content!\n\n{content[:500]}",
                color=0xFF6B35,  # Orange color
                timestamp=discord.utils.utcnow()
            )

            embed.set_thumbnail(url=author.display_avatar.url)

            if link_url:
                embed.add_field(
                    name="🔗 Check it out!",
                    value=f"[Click here to view]({link_url})",
                    inline=False
                )

            embed.set_footer(text=f"{guild.name} • Discord-Only Feature")

            await channel.send(embed=embed)
            logger.info(f"[Quick Feature] Posted embed for {author.display_name} in guild {guild.id}")

        except Exception as e:
            logger.error(f"[Quick Feature] Error posting embed: {e}", exc_info=True)

    async def _post_forum_creator_embed(
        self,
        guild: discord.Guild,
        channel: discord.TextChannel,
        author: discord.Member,
        content: str,
        thread_url: str
    ):
        """Post a detailed embed for forum-based creators (website + Discord feature)."""
        try:
            # Parse social links from content
            social_links = self._parse_social_links(content)

            # Create rich embed with creator info
            embed = discord.Embed(
                title="⭐ New Featured Creator!",
                description=f"**{author.display_name}** has been added to our Featured Creators Hall of Fame!\n\n{content[:800]}{'...' if len(content) > 800 else ''}",
                color=0xFFD700,  # Gold color for premium feature
                timestamp=discord.utils.utcnow()
            )

            embed.set_author(
                name=author.display_name,
                icon_url=author.display_avatar.url
            )
            embed.set_thumbnail(url=author.display_avatar.url)

            # Add social links as fields
            social_field_value = []
            if social_links.get('twitch'):
                social_field_value.append(f"🎮 [Twitch]({social_links['twitch']})")
            if social_links.get('youtube'):
                social_field_value.append(f"📺 [YouTube]({social_links['youtube']})")
            if social_links.get('twitter'):
                social_field_value.append(f"🐦 [Twitter]({social_links['twitter']})")
            if social_links.get('tiktok'):
                social_field_value.append(f"🎵 [TikTok]({social_links['tiktok']})")
            if social_links.get('instagram'):
                social_field_value.append(f"📸 [Instagram]({social_links['instagram']})")
            if social_links.get('bsky'):
                social_field_value.append(f"🦋 [bsky]({social_links['bsky']})")


            if social_field_value:
                embed.add_field(
                    name="🔗 Where to Find Them",
                    value=" • ".join(social_field_value),
                    inline=False
                )

            # Add forum thread link
            embed.add_field(
                name="💬 Full Introduction",
                value=f"[View their full creator intro post]({thread_url})",
                inline=False
            )

            # Add website link
            website_url = f"https://casual-heroes.com/questlog/guild/{guild.id}/featured-creators"
            embed.add_field(
                name="🌐 Featured Creators Hall of Fame",
                value=f"[View all featured creators on our website]({website_url})",
                inline=False
            )

            embed.set_footer(text=f"{guild.name} • Featured on Website & Discord")

            await channel.send(embed=embed)
            logger.info(f"[Forum Creator] Posted embed for {author.display_name} in guild {guild.id}")

        except Exception as e:
            logger.error(f"[Forum Creator] Error posting embed: {e}", exc_info=True)

    async def _select_and_announce_cotw(self, guild_id: int):
        """Select a random creator and announce as Creator of the Week."""
        import datetime
        import random

        with db_session_scope() as session:
            config = session.query(DiscoveryConfig).filter_by(guild_id=guild_id).first()
            if not config or not config.cotw_enabled or not config.cotw_channel_id:
                return

            # Get all active featured creators
            all_creators = session.query(FeaturedCreator).filter_by(
                is_active=True
            ).all()

            # Filter to only creators in this guild (check guilds JSON array)
            import json as json_lib
            creators = []
            for creator in all_creators:
                try:
                    guild_ids = json_lib.loads(creator.guilds) if creator.guilds else []
                    if guild_id in guild_ids:
                        creators.append(creator)
                except:
                    continue

            if not creators:
                logger.warning(f"[COTW] No featured creators found for guild {guild_id}")
                return

            # Avoid featuring the same creator twice in a row
            if config.cotw_last_featured_user_id:
                creators = [c for c in creators if c.user_id != config.cotw_last_featured_user_id]

            if not creators:
                logger.warning(f"[COTW] All creators already featured for guild {guild_id}")
                return

            # Select creator with highest times_featured_total count (featured globally)
            # This rewards creators who have been randomly featured the most
            selected = max(creators, key=lambda c: c.times_featured_total)
            logger.info(f"[COTW] Selected {selected.display_name} with {selected.times_featured_total} total features")

            # Get current week and year
            now = datetime.datetime.utcnow()
            year = now.year
            week = now.isocalendar()[1]

            # Delete old COTW message if exists
            guild = self.bot.get_guild(guild_id)
            if guild and config.cotw_last_message_id:
                try:
                    channel = guild.get_channel(config.cotw_channel_id)
                    if channel:
                        old_msg = await channel.fetch_message(config.cotw_last_message_id)
                        await old_msg.delete()
                except discord.NotFound:
                    pass
                except Exception as e:
                    logger.error(f"[COTW] Error deleting old message: {e}")

            # Post announcement
            channel = guild.get_channel(config.cotw_channel_id)
            if not channel:
                logger.error(f"[COTW] Channel not found for guild {guild_id}")
                return

            embed = discord.Embed(
                title="⭐ Creator of the Week! ⭐",
                description=f"Meet this week's featured creator: **{selected.display_name}**!",
                color=0x00D9FF,  # Bright blue
                timestamp=discord.utils.utcnow()
            )

            embed.set_thumbnail(url=selected.avatar_url)

            if selected.bio:
                embed.add_field(name="About", value=selected.bio[:500], inline=False)

            # Add social links
            links = []
            if selected.twitch_url:
                links.append(f"[Twitch]({selected.twitch_url})")
            if selected.youtube_url:
                links.append(f"[YouTube]({selected.youtube_url})")
            if selected.twitter_url:
                links.append(f"[Twitter/X]({selected.twitter_url})")
            if selected.tiktok_url:
                links.append(f"[TikTok]({selected.tiktok_url})")
            if selected.instagram_url:
                links.append(f"[Instagram]({selected.instagram_url})")
            if selected.bsky_url:
                links.append(f"[bsky]({selected.bsky})")

            if links:
                embed.add_field(name="🔗 Links", value=" • ".join(links), inline=False)

            # Show times_featured_total count (global)
            embed.add_field(
                name="⭐ Total Features",
                value=f"Featured **{selected.times_featured_total}x** across all guilds!",
                inline=True
            )

            embed.set_footer(text=f"Week {week}, {year} • Sponsored by Casual Heroes")

            message = await channel.send(embed=embed)

            # Update config
            config.cotw_last_message_id = message.id
            config.cotw_last_posted_at = int(datetime.datetime.utcnow().timestamp())
            config.cotw_last_featured_user_id = selected.user_id

            # Add to history
            cotw_record = CreatorOfTheWeek(
                guild_id=guild_id,
                user_id=selected.user_id,
                username=selected.username,
                display_name=selected.display_name,
                avatar_url=selected.avatar_url,
                bio=selected.bio,
                week=week,
                year=year,
                message_id=message.id,
                channel_id=channel.id,
                featured_at=int(datetime.datetime.utcnow().timestamp()),
                created_at=int(datetime.datetime.utcnow().timestamp())
            )
            session.add(cotw_record)
            session.commit()

            logger.info(f"[COTW] Featured {selected.display_name} as Creator of the Week for guild {guild_id}")

    async def _select_and_announce_cotm(self, guild_id: int):
        """Select a random creator and announce as Creator of the Month."""
        import datetime
        import random

        with db_session_scope() as session:
            config = session.query(DiscoveryConfig).filter_by(guild_id=guild_id).first()
            if not config or not config.cotm_enabled or not config.cotm_channel_id:
                return

            # Get all active featured creators
            all_creators = session.query(FeaturedCreator).filter_by(
                is_active=True
            ).all()

            # Filter to only creators in this guild (check guilds JSON array)
            import json as json_lib
            creators = []
            for creator in all_creators:
                try:
                    guild_ids = json_lib.loads(creator.guilds) if creator.guilds else []
                    if guild_id in guild_ids:
                        creators.append(creator)
                except:
                    continue

            if not creators:
                logger.warning(f"[COTM] No featured creators found for guild {guild_id}")
                return

            # Avoid featuring the same creator twice in a row
            if config.cotm_last_featured_user_id:
                creators = [c for c in creators if c.user_id != config.cotm_last_featured_user_id]

            if not creators:
                logger.warning(f"[COTM] All creators already featured for guild {guild_id}")
                return

            # Select creator with highest times_featured_total count (featured globally)
            # This rewards creators who have been randomly featured the most
            selected = max(creators, key=lambda c: c.times_featured_total)
            logger.info(f"[COTM] Selected {selected.display_name} with {selected.times_featured_total} total features")

            # Get current month and year
            now = datetime.datetime.utcnow()
            year = now.year
            month = now.month

            # Delete old COTM message if exists
            guild = self.bot.get_guild(guild_id)
            if guild and config.cotm_last_message_id:
                try:
                    channel = guild.get_channel(config.cotm_channel_id)
                    if channel:
                        old_msg = await channel.fetch_message(config.cotm_last_message_id)
                        await old_msg.delete()
                except discord.NotFound:
                    pass
                except Exception as e:
                    logger.error(f"[COTM] Error deleting old message: {e}")

            # Post announcement
            channel = guild.get_channel(config.cotm_channel_id)
            if not channel:
                logger.error(f"[COTM] Channel not found for guild {guild_id}")
                return

            month_name = datetime.datetime(year, month, 1).strftime("%B")

            embed = discord.Embed(
                title=f"👑 Creator of the Month - {month_name}! 👑",
                description=f"This month's spotlight creator: **{selected.display_name}**!",
                color=0xFFD700,  # Gold
                timestamp=discord.utils.utcnow()
            )

            embed.set_thumbnail(url=selected.avatar_url)

            if selected.bio:
                embed.add_field(name="About", value=selected.bio[:500], inline=False)

            # Add social links
            links = []
            if selected.twitch_url:
                links.append(f"[Twitch]({selected.twitch_url})")
            if selected.youtube_url:
                links.append(f"[YouTube]({selected.youtube_url})")
            if selected.twitter_url:
                links.append(f"[Twitter/X]({selected.twitter_url})")
            if selected.tiktok_url:
                links.append(f"[TikTok]({selected.tiktok_url})")
            if selected.instagram_url:
                links.append(f"[Instagram]({selected.instagram_url})")
            if selected.bsky_url:
                links.append(f"[bsky]({selected.bsky})")

            if links:
                embed.add_field(name="🔗 Links", value=" • ".join(links), inline=False)

            # Show times_featured_total count (global)
            embed.add_field(
                name="⭐ Total Features",
                value=f"Featured **{selected.times_featured_total}x** across all guilds!",
                inline=True
            )

            embed.set_footer(text=f"{month_name} {year} • Sponsored by Casual Heroes")

            message = await channel.send(embed=embed)

            # Update config
            config.cotm_last_message_id = message.id
            config.cotm_last_posted_at = int(datetime.datetime.utcnow().timestamp())
            config.cotm_last_featured_user_id = selected.user_id

            # Add to history
            cotm_record = CreatorOfTheMonth(
                guild_id=guild_id,
                user_id=selected.user_id,
                username=selected.username,
                display_name=selected.display_name,
                avatar_url=selected.avatar_url,
                bio=selected.bio,
                month=month,
                year=year,
                message_id=message.id,
                channel_id=channel.id,
                featured_at=int(datetime.datetime.utcnow().timestamp()),
                created_at=int(datetime.datetime.utcnow().timestamp())
            )
            session.add(cotm_record)
            session.commit()

            logger.info(f"[COTM] Featured {selected.display_name} as Creator of the Month for guild {guild_id}")

    # ========== SLASH COMMAND GROUPS ==========

    promo = SlashCommandGroup(
        name="promo",
        description="Self-promotion commands",

    )

    discovery = SlashCommandGroup(
        name="discovery",
        description="Discovery network commands",
        
    )

    listing = SlashCommandGroup(
        name="listing",
        description="Server listing commands (PRO)",
        
    )

    # ========== SELF-PROMO COMMANDS ==========

    @promo.command(name="post", description="Post self-promotion (FREE, daily limits apply)")
    @discord.option(
        name="content",
        description="Your promo content (max 1000 characters)",
        required=True
    )
    @discord.option(
        name="link",
        description="Optional link to your content",
        required=False
    )
    async def promo_post(
        self,
        ctx: discord.ApplicationContext,
        content: str,
        link: str = None
    ):
        """
        Post self-promotion - FREE for all members.
        Daily limit based on tier: FREE=2, PREMIUM=10, PRO=Unlimited
        """
        if len(content) > 1000:
            await ctx.respond("Content must be 1000 characters or less.", ephemeral=True)
            return

        with db_session_scope() as session:
            guild = session.get(Guild, ctx.guild.id)

            if not guild:
                await ctx.respond("Guild not configured. Ask an admin to run `/questlog setup`.", ephemeral=True)
                return

            # Check if self-promo channel is set
            if not guild.self_promo_channel_id:
                await ctx.respond(
                    "Self-promo channel not configured.\n"
                    "Ask an admin to set it with `/settings channel self-promo #channel`.",
                    ephemeral=True
                )
                return

            promo_channel = ctx.guild.get_channel(guild.self_promo_channel_id)
            if not promo_channel:
                await ctx.respond("Self-promo channel not found.", ephemeral=True)
                return

            # Check daily limit
            tier = get_guild_tier(session, ctx.guild.id)
            daily_limit = FeatureLimits.get_limit(tier, "self_promo_per_day")

            if daily_limit is not None:  # None means unlimited
                today_start = get_today_start_timestamp()
                today_posts = (
                    session.query(PromoPost)
                    .filter(
                        PromoPost.guild_id == ctx.guild.id,
                        PromoPost.user_id == ctx.author.id,
                        PromoPost.created_at >= today_start
                    )
                    .count()
                )

                if today_posts >= daily_limit:
                    upgrade_msg = FeatureLimits.get_upgrade_message("self_promo_per_day", tier)
                    await ctx.respond(
                        f"**Daily limit reached!** You've posted {today_posts}/{daily_limit} times today.\n\n"
                        f"{upgrade_msg}\n\n"
                        f"Your limit resets at midnight UTC.",
                        ephemeral=True
                    )
                    return

            # Get or create member
            db_member = session.get(GuildMember, (ctx.guild.id, ctx.author.id))
            if not db_member:
                logger.warning(
                    f"[DUPLICATE TRACKER] discovery.promo_post CREATING GuildMember: "
                    f"guild_id={ctx.guild.id}, user_id={ctx.author.id}, user_id_type={type(ctx.author.id)}, "
                    f"display_name={ctx.author.display_name}, source=Discord.ApplicationContext.author"
                )
                db_member = GuildMember(
                    guild_id=ctx.guild.id,
                    user_id=ctx.author.id,
                    display_name=ctx.author.display_name,
                )
                session.add(db_member)

            # Create promo record (0 hero_tokens - FREE)
            promo = PromoPost(
                guild_id=ctx.guild.id,
                user_id=ctx.author.id,
                content=content,
                link_url=link,
                promo_tier=PromoTier.BASIC,
                hero_tokens_spent=0,
            )
            session.add(promo)

            # Calculate remaining posts for today
            remaining = "Unlimited" if daily_limit is None else f"{daily_limit - (today_posts + 1 if daily_limit else 0)}"

        # Post to self-promo channel
        embed = discord.Embed(
            description=content,
            color=discord.Color.blue(),
            timestamp=discord.utils.utcnow()
        )
        embed.set_author(
            name=ctx.author.display_name,
            icon_url=ctx.author.display_avatar.url
        )
        if link:
            embed.add_field(name="Link", value=link, inline=False)
        embed.set_footer(text="Self-Promotion")

        try:
            await promo_channel.send(embed=embed)
            response_msg = f"Your promo has been posted in {promo_channel.mention}!"
            if daily_limit is not None:
                response_msg += f"\n\n**Daily posts remaining:** {remaining}"
            response_msg += "\n\n**Tip:** Want to be featured? Use `/promo featured` (Premium servers)."

            await ctx.respond(response_msg, ephemeral=True)
        except discord.Forbidden:
            await ctx.respond("I don't have permission to post in the self-promo channel.", ephemeral=True)

    @promo.command(name="featured", description="Enter featured pool for 15 hero_tokens (Premium)")
    @discord.option(
        name="content",
        description="Your promo content to be featured",
        required=True
    )
    @discord.option(
        name="link",
        description="Optional link to your content",
        required=False
    )
    async def promo_featured(
        self,
        ctx: discord.ApplicationContext,
        content: str,
        link: str = None
    ):
        """
        Enter the featured pool - PREMIUM servers only.
        Costs 15 Hero hero_tokens. Random selection for 3-day feature.
        """
        if len(content) > 1000:
            await ctx.respond("Content must be 1000 characters or less.", ephemeral=True)
            return

        with db_session_scope() as session:
            guild = session.get(Guild, ctx.guild.id)
            tier = get_guild_tier(session, ctx.guild.id)

            # Check premium status using FeatureLimits
            has_featured_pool = FeatureLimits.get_limit(tier, "featured_pool")
            if not has_featured_pool:
                await ctx.respond(
                    "**Featured Pool requires QuestLog Premium!**\n\n"
                    "Ask a server admin to upgrade with `/questlog premium`.\n\n"
                    "Free alternative: Use `/promo post` to share your content!",
                    ephemeral=True
                )
                return

            db_member = session.get(GuildMember, (ctx.guild.id, ctx.author.id))

            # Check if already in pool
            now = int(time.time())
            existing_entry = (
                session.query(FeaturedPool)
                .filter(
                    FeaturedPool.guild_id == ctx.guild.id,
                    FeaturedPool.user_id == ctx.author.id,
                    FeaturedPool.was_selected == False,
                    FeaturedPool.expires_at > now
                )
                .first()
            )

            if existing_entry:
                await ctx.respond(
                    "You already have an active entry in the featured pool!\n\n"
                    f"Your entry expires <t:{existing_entry.expires_at}:R>.\n"
                    "Wait for the selection or let it expire before entering again.",
                    ephemeral=True
                )
                return

            # Check token balance
            token_cost = DefaultXPSettings.FEATURED_POOL_COST
            if not db_member or db_member.hero_hero_tokens < token_cost:
                hero_tokens = db_member.hero_hero_tokens if db_member else 0
                await ctx.respond(
                    f"You need **{token_cost} Hero hero_tokens** for featured pool. You have **{hero_tokens}**.\n\n"
                    f"Earn hero_tokens by gaining XP (100 XP = 15 hero_tokens).\n"
                    f"Use `/promo post` for FREE self-promotion!",
                    ephemeral=True
                )
                return

            # Deduct hero_tokens
            db_member.hero_hero_tokens -= token_cost

            # Create promo post
            promo = PromoPost(
                guild_id=ctx.guild.id,
                user_id=ctx.author.id,
                content=content,
                link_url=link,
                promo_tier=PromoTier.FEATURED,
                hero_tokens_spent=token_cost,
            )
            session.add(promo)
            session.flush()

            # Add to featured pool
            pool_entry = FeaturedPool(
                guild_id=ctx.guild.id,
                user_id=ctx.author.id,
                promo_post_id=promo.id,
                entered_at=now,
                expires_at=now + (7 * 86400),  # 7 days
            )
            session.add(pool_entry)

            final_hero_tokens = db_member.hero_hero_tokens

        await ctx.respond(
            f"**You've entered the Featured Pool!** (-{token_cost} hero_tokens)\n\n"
            f"You now have **{final_hero_tokens}** Hero hero_tokens remaining.\n\n"
            f"**What happens next:**\n"
            f"- Random selection from the pool every 6 hours\n"
            f"- Winners get pinned in featured channel for **3 days**\n"
            f"- Shoutout announcement to the server\n\n"
            f"Your entry expires in 7 days if not selected. Good luck!",
            ephemeral=True
        )

    @promo.command(name="status", description="Check your promo status")
    async def promo_status(self, ctx: discord.ApplicationContext):
        """Check if you're in the featured pool and your promo history."""
        with db_session_scope() as session:
            db_member = session.get(GuildMember, (ctx.guild.id, ctx.author.id))
            tier = get_guild_tier(session, ctx.guild.id)

            if not db_member:
                await ctx.respond("You haven't earned any hero_tokens yet! Start chatting to earn XP.", ephemeral=True)
                return

            now = int(time.time())
            today_start = get_today_start_timestamp()

            # Check pool status
            pool_entry = (
                session.query(FeaturedPool)
                .filter(
                    FeaturedPool.guild_id == ctx.guild.id,
                    FeaturedPool.user_id == ctx.author.id,
                    FeaturedPool.was_selected == False,
                    FeaturedPool.expires_at > now
                )
                .first()
            )

            # Count today's promos
            today_posts = (
                session.query(PromoPost)
                .filter(
                    PromoPost.guild_id == ctx.guild.id,
                    PromoPost.user_id == ctx.author.id,
                    PromoPost.created_at >= today_start
                )
                .count()
            )

            # Total promos
            total_promos = (
                session.query(PromoPost)
                .filter(
                    PromoPost.guild_id == ctx.guild.id,
                    PromoPost.user_id == ctx.author.id
                )
                .count()
            )

            # Times featured
            times_featured = (
                session.query(PromoPost)
                .filter(
                    PromoPost.guild_id == ctx.guild.id,
                    PromoPost.user_id == ctx.author.id,
                    PromoPost.is_featured == True
                )
                .count()
            )

            daily_limit = FeatureLimits.get_limit(tier, "self_promo_per_day")
            daily_display = f"{today_posts}/{daily_limit}" if daily_limit else f"{today_posts} (Unlimited)"

            embed = discord.Embed(
                title="Your Promo Status",
                color=discord.Color.gold()
            )

            embed.add_field(
                name="Hero hero_tokens",
                value=f"**{db_member.hero_hero_tokens}**",
                inline=True
            )
            embed.add_field(
                name="Today's Posts",
                value=f"**{daily_display}**",
                inline=True
            )
            embed.add_field(
                name="Total Promos",
                value=f"**{total_promos}**",
                inline=True
            )
            embed.add_field(
                name="Times Featured",
                value=f"**{times_featured}**",
                inline=True
            )
            embed.add_field(
                name="Featured Pool",
                value="Active entry!" if pool_entry else "Not in pool",
                inline=True
            )
            embed.add_field(
                name="Server Tier",
                value=f"**{tier}**",
                inline=True
            )

            if pool_entry:
                embed.add_field(
                    name="Pool Entry Expires",
                    value=f"<t:{pool_entry.expires_at}:R>",
                    inline=False
                )

        await ctx.respond(embed=embed, ephemeral=True)

    # ========== DISCOVERY NETWORK COMMANDS ==========

    @discovery.command(name="browse", description="Browse featured creators")
    async def discovery_browse(self, ctx: discord.ApplicationContext):
        """Browse featured creators in this server."""
        with db_session_scope() as session:
            now = int(time.time())

            # Get current featured posts
            featured = (
                session.query(PromoPost)
                .filter(
                    PromoPost.guild_id == ctx.guild.id,
                    PromoPost.is_featured == True,
                    PromoPost.featured_until > now
                )
                .order_by(PromoPost.featured_at.desc())
                .limit(10)
                .all()
            )

            if not featured:
                await ctx.respond(
                    "No featured creators right now. Check back soon!\n\n"
                    "**Want to be featured?** Use `/promo featured` to enter the pool!",
                    ephemeral=True
                )
                return

            embed = discord.Embed(
                title="Featured Creators",
                description="These creators are currently featured!",
                color=discord.Color.gold()
            )

            for post in featured:
                member = ctx.guild.get_member(post.user_id)
                name = member.display_name if member else f"User {post.user_id}"
                value = post.content[:200] + "..." if len(post.content) > 200 else post.content
                if post.link_url:
                    value += f"\n[Link]({post.link_url})"
                value += f"\nFeatured until: <t:{post.featured_until}:R>"
                embed.add_field(name=name, value=value, inline=False)

        await ctx.respond(embed=embed, ephemeral=True)

    # ========================================================================
    # PHASE 2/3 FEATURES - DISABLED (Require web integration & documentation)
    # ========================================================================
    # The following commands are COMMENTED OUT until web dashboard and
    # documentation are ready:
    # - Cross-server Discovery Network (/discovery servers, join, settings, leave)
    # - Server Listings (/listing create, edit, publish, stats, delete)
    # - Primary Guild Selection (/discovery set-primary-guild, set-primary, auto-select)
    #
    # These features require additional web development and user documentation
    # before they can be launched. Uncomment when ready for Phase 2/3.
    # ========================================================================

# PHASE2:     @discovery.command(name="set-primary-guild", description="Set which guild's intro to display globally")
# PHASE2:     async def set_primary_guild(self, ctx: discord.ApplicationContext):
# PHASE2:         """
# PHASE2:         Set which guild's intro should be displayed on your global Hall of Fame profile.
# PHASE2:         By default, your most recent intro is used (auto-select).
# PHASE2:         """
# PHASE2:         with db_session_scope() as session:
# PHASE2:             # Get creator
# PHASE2:             creator = session.get(FeaturedCreator, ctx.author.id)
# PHASE2: 
# PHASE2:             if not creator:
# PHASE2:                 await ctx.respond(
# PHASE2:                     "You don't have a Hall of Fame entry yet.\n\n"
# PHASE2:                     "Post an intro in a forum or use `/promo featured` to get featured!",
# PHASE2:                     ephemeral=True
# PHASE2:                 )
# PHASE2:                 return
# PHASE2: 
# PHASE2:             # Get list of guilds they're in
# PHASE2:             guilds_list = json.loads(creator.guilds) if creator.guilds else []
# PHASE2: 
# PHASE2:             if not guilds_list:
# PHASE2:                 await ctx.respond(
# PHASE2:                     "You're not in any tracked guilds.",
# PHASE2:                     ephemeral=True
# PHASE2:                 )
# PHASE2:                 return
# PHASE2: 
# PHASE2:             # Build guild selection embed
# PHASE2:             embed = discord.Embed(
# PHASE2:                 title="🏆 Set Your Primary Guild",
# PHASE2:                 description=(
# PHASE2:                     f"**Current Primary:** {creator.primary_guild_id}\n"
# PHASE2:                     f"**Auto-Select:** {'Enabled ✅' if creator.auto_select_primary else 'Disabled ❌'}\n\n"
# PHASE2:                     "Choose which guild's intro should be displayed on your Hall of Fame profile.\n\n"
# PHASE2:                     "**Your Guilds:**"
# PHASE2:                 ),
# PHASE2:                 color=discord.Color.gold()
# PHASE2:             )
# PHASE2: 
# PHASE2:             for guild_id in guilds_list:
# PHASE2:                 guild = self.bot.get_guild(guild_id)
# PHASE2:                 guild_name = guild.name if guild else f"Guild {guild_id}"
# PHASE2:                 is_primary = "⭐ **PRIMARY**" if guild_id == creator.primary_guild_id else ""
# PHASE2:                 embed.add_field(
# PHASE2:                     name=f"{guild_name} {is_primary}",
# PHASE2:                     value=f"Guild ID: `{guild_id}`",
# PHASE2:                     inline=False
# PHASE2:                 )
# PHASE2: 
# PHASE2:             embed.add_field(
# PHASE2:                 name="💡 How to Change",
# PHASE2:                 value=(
# PHASE2:                     "To change your primary guild, use:\n"
# PHASE2:                     "`/discovery set-primary <guild_id>`\n\n"
# PHASE2:                     "To enable auto-select (always use most recent):\n"
# PHASE2:                     "`/discovery auto-select true`"
# PHASE2:                 ),
# PHASE2:                 inline=False
# PHASE2:             )
# PHASE2: 
# PHASE2:             await ctx.respond(embed=embed, ephemeral=True)
# PHASE2: 
# PHASE2:     @discovery.command(name="set-primary", description="Set your primary guild by ID")
# PHASE2:     @discord.option(
# PHASE2:         name="guild_id",
# PHASE2:         description="The guild ID to set as primary",
# PHASE2:         required=True
# PHASE2:     )
# PHASE2:     async def set_primary(
# PHASE2:         self,
# PHASE2:         ctx: discord.ApplicationContext,
# PHASE2:         guild_id: str
# PHASE2:     ):
# PHASE2:         """Set which guild's intro should be displayed globally."""
# PHASE2:         try:
# PHASE2:             guild_id_int = int(guild_id)
# PHASE2:         except ValueError:
# PHASE2:             await ctx.respond("Invalid guild ID. Please provide a valid number.", ephemeral=True)
# PHASE2:             return
# PHASE2: 
# PHASE2:         with db_session_scope() as session:
# PHASE2:             # Get creator
# PHASE2:             creator = session.get(FeaturedCreator, ctx.author.id)
# PHASE2: 
# PHASE2:             if not creator:
# PHASE2:                 await ctx.respond(
# PHASE2:                     "You don't have a Hall of Fame entry yet.",
# PHASE2:                     ephemeral=True
# PHASE2:                 )
# PHASE2:                 return
# PHASE2: 
# PHASE2:             # Check if they're in this guild
# PHASE2:             guilds_list = json.loads(creator.guilds) if creator.guilds else []
# PHASE2: 
# PHASE2:             if guild_id_int not in guilds_list:
# PHASE2:                 await ctx.respond(
# PHASE2:                     f"You're not in guild {guild_id_int}.\n\n"
# PHASE2:                     "Use `/discovery set-primary-guild` to see your guilds.",
# PHASE2:                     ephemeral=True
# PHASE2:                 )
# PHASE2:                 return
# PHASE2: 
# PHASE2:             # Update primary guild
# PHASE2:             creator.primary_guild_id = guild_id_int
# PHASE2:             creator.auto_select_primary = False  # Disable auto-select when manually set
# PHASE2:             creator.updated_at = int(time.time())
# PHASE2:             session.commit()
# PHASE2: 
# PHASE2:             guild = self.bot.get_guild(guild_id_int)
# PHASE2:             guild_name = guild.name if guild else f"Guild {guild_id_int}"
# PHASE2: 
# PHASE2:             await ctx.respond(
# PHASE2:                 f"✅ **Primary guild set!**\n\n"
# PHASE2:                 f"Your Hall of Fame profile will now display your intro from **{guild_name}**.\n"
# PHASE2:                 f"Auto-select has been disabled.\n\n"
# PHASE2:                 f"To re-enable auto-select, use `/discovery auto-select true`",
# PHASE2:                 ephemeral=True
# PHASE2:             )
# PHASE2: 
# PHASE2:     @discovery.command(name="auto-select", description="Enable/disable auto-select for primary guild")
# PHASE2:     @discord.option(
# PHASE2:         name="enabled",
# PHASE2:         description="Enable or disable auto-select",
# PHASE2:         required=True
# PHASE2:     )
# PHASE2:     async def auto_select(
# PHASE2:         self,
# PHASE2:         ctx: discord.ApplicationContext,
# PHASE2:         enabled: bool
# PHASE2:     ):
# PHASE2:         """Enable or disable automatic primary guild selection."""
# PHASE2:         with db_session_scope() as session:
# PHASE2:             # Get creator
# PHASE2:             creator = session.get(FeaturedCreator, ctx.author.id)
# PHASE2: 
# PHASE2:             if not creator:
# PHASE2:                 await ctx.respond(
# PHASE2:                     "You don't have a Hall of Fame entry yet.",
# PHASE2:                     ephemeral=True
# PHASE2:                 )
# PHASE2:                 return
# PHASE2: 
# PHASE2:             # Update auto-select
# PHASE2:             creator.auto_select_primary = enabled
# PHASE2:             creator.updated_at = int(time.time())
# PHASE2:             session.commit()
# PHASE2: 
# PHASE2:             if enabled:
# PHASE2:                 await ctx.respond(
# PHASE2:                     "✅ **Auto-select enabled!**\n\n"
# PHASE2:                     "Your Hall of Fame profile will now automatically display your most recent intro.",
# PHASE2:                     ephemeral=True
# PHASE2:                 )
# PHASE2:             else:
# PHASE2:                 await ctx.respond(
# PHASE2:                     "✅ **Auto-select disabled!**\n\n"
# PHASE2:                     f"Your Hall of Fame profile will continue displaying your intro from guild {creator.primary_guild_id}.\n"
# PHASE2:                     "Use `/discovery set-primary <guild_id>` to change it manually.",
# PHASE2:                     ephemeral=True
# PHASE2:                 )

# PHASE2:     @discovery.command(name="servers", description="Browse servers in discovery network (PRO)")
# PHASE2:     @discord.option(
# PHASE2:         name="category",
# PHASE2:         description="Filter by category",
# PHASE2:         required=False,
# PHASE2:         choices=["gaming", "streaming", "content", "esports", "casual", "competitive"]
# PHASE2:     )
# PHASE2:     async def discovery_servers(
# PHASE2:         self,
# PHASE2:         ctx: discord.ApplicationContext,
# PHASE2:         category: str = None
# PHASE2:     ):
# PHASE2:         """Browse servers in the discovery network."""
# PHASE2:         with db_session_scope() as session:
# PHASE2:             tier = get_guild_tier(session, ctx.guild.id)
# PHASE2:             has_discovery = FeatureLimits.get_limit(tier, "discovery_network")
# PHASE2: 
# PHASE2:             if not has_discovery:
# PHASE2:                 await ctx.respond(
# PHASE2:                     "**Discovery Network requires QuestLog PRO!**\n\n"
# PHASE2:                     "Upgrade with `/questlog upgrade` to:\n"
# PHASE2:                     "- Browse and join partner servers\n"
# PHASE2:                     "- List your server in the directory\n"
# PHASE2:                     "- Cross-promote with other communities",
# PHASE2:                     ephemeral=True
# PHASE2:                 )
# PHASE2:                 return
# PHASE2: 
# PHASE2:             # Build query
# PHASE2:             query = (
# PHASE2:                 session.query(ServerListing)
# PHASE2:                 .filter(
# PHASE2:                     ServerListing.is_published == True,
# PHASE2:                     ServerListing.guild_id != ctx.guild.id  # Don't show own server
# PHASE2:                 )
# PHASE2:             )
# PHASE2: 
# PHASE2:             if category:
# PHASE2:                 query = query.filter(ServerListing.categories.contains(category))
# PHASE2: 
# PHASE2:             listings = query.order_by(ServerListing.member_count.desc()).limit(15).all()
# PHASE2: 
# PHASE2:             if not listings:
# PHASE2:                 await ctx.respond(
# PHASE2:                     "No servers found in the discovery network yet.\n\n"
# PHASE2:                     "Be the first! Use `/listing create` to add your server.",
# PHASE2:                     ephemeral=True
# PHASE2:                 )
# PHASE2:                 return
# PHASE2: 
# PHASE2:             embed = discord.Embed(
# PHASE2:                 title="Discovery Network",
# PHASE2:                 description=f"Servers matching: **{category or 'All Categories'}**",
# PHASE2:                 color=discord.Color.purple()
# PHASE2:             )
# PHASE2: 
# PHASE2:             for listing in listings:
# PHASE2:                 # Increment view count
# PHASE2:                 listing.views += 1
# PHASE2: 
# PHASE2:                 tags = f"\n*{listing.tags}*" if listing.tags else ""
# PHASE2:                 invite_text = f"\n[Join Server](https://discord.gg/{listing.invite_code})" if listing.invite_code else ""
# PHASE2: 
# PHASE2:                 embed.add_field(
# PHASE2:                     name=f"{listing.title} ({listing.member_count:,} members)",
# PHASE2:                     value=f"{listing.description[:150] if listing.description else 'No description'}{tags}{invite_text}",
# PHASE2:                     inline=False
# PHASE2:                 )
# PHASE2: 
# PHASE2:             embed.set_footer(text=f"Showing {len(listings)} servers | /listing create to add yours")
# PHASE2: 
# PHASE2:         await ctx.respond(embed=embed, ephemeral=True)
# PHASE2: 
# PHASE2:     @discovery.command(name="join", description="Join the discovery network (Admin, PRO)")
# PHASE2:     @discord.default_permissions(administrator=True)
# PHASE2:     @commands.has_permissions(administrator=True)
# PHASE2:     async def discovery_join(self, ctx: discord.ApplicationContext):
# PHASE2:         """Join the discovery network."""
# PHASE2:         with db_session_scope() as session:
# PHASE2:             tier = get_guild_tier(session, ctx.guild.id)
# PHASE2:             has_discovery = FeatureLimits.get_limit(tier, "discovery_network")
# PHASE2: 
# PHASE2:             if not has_discovery:
# PHASE2:                 await ctx.respond(
# PHASE2:                     "**Discovery Network requires QuestLog PRO!**\n\n"
# PHASE2:                     "Upgrade with `/questlog upgrade` to access cross-server promotion.",
# PHASE2:                     ephemeral=True
# PHASE2:                 )
# PHASE2:                 return
# PHASE2: 
# PHASE2:             # Check if already joined
# PHASE2:             existing = session.get(DiscoveryNetwork, ctx.guild.id)
# PHASE2:             if existing and existing.is_active:
# PHASE2:                 await ctx.respond(
# PHASE2:                     "Your server is already in the discovery network!\n\n"
# PHASE2:                     "Use `/discovery settings` to configure your preferences.",
# PHASE2:                     ephemeral=True
# PHASE2:                 )
# PHASE2:                 return
# PHASE2: 
# PHASE2:             # Join network
# PHASE2:             if existing:
# PHASE2:                 existing.is_active = True
# PHASE2:             else:
# PHASE2:                 network = DiscoveryNetwork(
# PHASE2:                     guild_id=ctx.guild.id,
# PHASE2:                     is_active=True,
# PHASE2:                     allow_incoming=True,
# PHASE2:                     allow_outgoing=True,
# PHASE2:                     categories="gaming",
# PHASE2:                 )
# PHASE2:                 session.add(network)
# PHASE2: 
# PHASE2:             # Enable discovery on guild
# PHASE2:             guild = session.get(Guild, ctx.guild.id)
# PHASE2:             if guild:
# PHASE2:                 guild.discovery_enabled = True
# PHASE2: 
# PHASE2:         await ctx.respond(
# PHASE2:             "**Welcome to the Discovery Network!**\n\n"
# PHASE2:             "Your server is now part of the cross-server promotion network.\n\n"
# PHASE2:             "**Next steps:**\n"
# PHASE2:             "1. `/listing create` - Create your server listing\n"
# PHASE2:             "2. `/discovery settings` - Configure your preferences\n"
# PHASE2:             "3. `/discovery servers` - Browse other servers",
# PHASE2:             ephemeral=True
# PHASE2:         )
# PHASE2: 
# PHASE2:     @discovery.command(name="settings", description="Configure discovery settings (Admin, PRO)")
# PHASE2:     @discord.default_permissions(administrator=True)
# PHASE2:     @commands.has_permissions(administrator=True)
# PHASE2:     @discord.option(
# PHASE2:         name="incoming",
# PHASE2:         description="Allow incoming promo posts from other servers",
# PHASE2:         required=False
# PHASE2:     )
# PHASE2:     @discord.option(
# PHASE2:         name="outgoing",
# PHASE2:         description="Share your promos with other servers",
# PHASE2:         required=False
# PHASE2:     )
# PHASE2:     @discord.option(
# PHASE2:         name="channel",
# PHASE2:         description="Channel for cross-server promos",
# PHASE2:         required=False
# PHASE2:     )
# PHASE2:     @discord.option(
# PHASE2:         name="categories",
# PHASE2:         description="Categories (comma-separated: gaming,streaming,esports)",
# PHASE2:         required=False
# PHASE2:     )
# PHASE2:     async def discovery_settings(
# PHASE2:         self,
# PHASE2:         ctx: discord.ApplicationContext,
# PHASE2:         incoming: bool = None,
# PHASE2:         outgoing: bool = None,
# PHASE2:         channel: discord.TextChannel = None,
# PHASE2:         categories: str = None
# PHASE2:     ):
# PHASE2:         """Configure discovery network settings."""
# PHASE2:         with db_session_scope() as session:
# PHASE2:             tier = get_guild_tier(session, ctx.guild.id)
# PHASE2:             has_discovery = FeatureLimits.get_limit(tier, "discovery_network")
# PHASE2: 
# PHASE2:             if not has_discovery:
# PHASE2:                 await ctx.respond("Discovery Network requires QuestLog PRO!", ephemeral=True)
# PHASE2:                 return
# PHASE2: 
# PHASE2:             network = session.get(DiscoveryNetwork, ctx.guild.id)
# PHASE2:             if not network:
# PHASE2:                 await ctx.respond("Join the network first with `/discovery join`.", ephemeral=True)
# PHASE2:                 return
# PHASE2: 
# PHASE2:             # Update settings
# PHASE2:             changes = []
# PHASE2:             if incoming is not None:
# PHASE2:                 network.allow_incoming = incoming
# PHASE2:                 changes.append(f"Incoming promos: **{'Enabled' if incoming else 'Disabled'}**")
# PHASE2:             if outgoing is not None:
# PHASE2:                 network.allow_outgoing = outgoing
# PHASE2:                 changes.append(f"Outgoing promos: **{'Enabled' if outgoing else 'Disabled'}**")
# PHASE2:             if channel is not None:
# PHASE2:                 network.network_channel_id = channel.id
# PHASE2:                 changes.append(f"Network channel: {channel.mention}")
# PHASE2:             if categories is not None:
# PHASE2:                 network.categories = categories.lower().replace(" ", "")
# PHASE2:                 changes.append(f"Categories: **{network.categories}**")
# PHASE2: 
# PHASE2:             if not changes:
# PHASE2:                 # Show current settings
# PHASE2:                 embed = discord.Embed(
# PHASE2:                     title="Discovery Network Settings",
# PHASE2:                     color=discord.Color.purple()
# PHASE2:                 )
# PHASE2:                 embed.add_field(
# PHASE2:                     name="Incoming Promos",
# PHASE2:                     value="Enabled" if network.allow_incoming else "Disabled",
# PHASE2:                     inline=True
# PHASE2:                 )
# PHASE2:                 embed.add_field(
# PHASE2:                     name="Outgoing Promos",
# PHASE2:                     value="Enabled" if network.allow_outgoing else "Disabled",
# PHASE2:                     inline=True
# PHASE2:                 )
# PHASE2:                 ch = ctx.guild.get_channel(network.network_channel_id) if network.network_channel_id else None
# PHASE2:                 embed.add_field(
# PHASE2:                     name="Network Channel",
# PHASE2:                     value=ch.mention if ch else "Not set",
# PHASE2:                     inline=True
# PHASE2:                 )
# PHASE2:                 embed.add_field(
# PHASE2:                     name="Categories",
# PHASE2:                     value=network.categories or "gaming",
# PHASE2:                     inline=True
# PHASE2:                 )
# PHASE2:                 await ctx.respond(embed=embed, ephemeral=True)
# PHASE2:             else:
# PHASE2:                 await ctx.respond(
# PHASE2:                     "**Settings updated:**\n" + "\n".join(changes),
# PHASE2:                     ephemeral=True
# PHASE2:                 )
# PHASE2: 
# PHASE2:     @discovery.command(name="leave", description="Leave the discovery network (Admin)")
# PHASE2:     @discord.default_permissions(administrator=True)
# PHASE2:     @commands.has_permissions(administrator=True)
# PHASE2:     async def discovery_leave(self, ctx: discord.ApplicationContext):
# PHASE2:         """Leave the discovery network."""
# PHASE2:         with db_session_scope() as session:
# PHASE2:             network = session.get(DiscoveryNetwork, ctx.guild.id)
# PHASE2:             if not network or not network.is_active:
# PHASE2:                 await ctx.respond("Your server is not in the discovery network.", ephemeral=True)
# PHASE2:                 return
# PHASE2: 
# PHASE2:             network.is_active = False
# PHASE2: 
# PHASE2:             # Unpublish listing
# PHASE2:             listing = session.get(ServerListing, ctx.guild.id)
# PHASE2:             if listing:
# PHASE2:                 listing.is_published = False
# PHASE2: 
# PHASE2:             guild = session.get(Guild, ctx.guild.id)
# PHASE2:             if guild:
# PHASE2:                 guild.discovery_enabled = False
# PHASE2: 
# PHASE2:         await ctx.respond(
# PHASE2:             "You've left the discovery network.\n"
# PHASE2:             "Your server listing has been unpublished.\n\n"
# PHASE2:             "Rejoin anytime with `/discovery join`.",
# PHASE2:             ephemeral=True
# PHASE2:         )
# PHASE2: 
# PHASE2:     # ========== SERVER LISTING COMMANDS ==========
# PHASE2: 
# PHASE2:     @listing.command(name="create", description="Create your server listing (PRO)")
# PHASE2:     @discord.default_permissions(administrator=True)
# PHASE2:     @commands.has_permissions(administrator=True)
# PHASE2:     @discord.option(name="title", description="Server title (max 100 chars)", required=True)
# PHASE2:     @discord.option(name="description", description="Server description", required=True)
# PHASE2:     @discord.option(name="invite_code", description="Invite code (without discord.gg/)", required=False)
# PHASE2:     @discord.option(
# PHASE2:         name="category",
# PHASE2:         description="Primary category",
# PHASE2:         required=True,
# PHASE2:         choices=["gaming", "streaming", "content", "esports", "casual", "competitive"]
# PHASE2:     )
# PHASE2:     @discord.option(name="tags", description="Tags (comma-separated)", required=False)
# PHASE2:     async def listing_create(
# PHASE2:         self,
# PHASE2:         ctx: discord.ApplicationContext,
# PHASE2:         title: str,
# PHASE2:         description: str,
# PHASE2:         category: str,
# PHASE2:         invite_code: str = None,
# PHASE2:         tags: str = None
# PHASE2:     ):
# PHASE2:         """Create or update your server listing."""
# PHASE2:         with db_session_scope() as session:
# PHASE2:             tier = get_guild_tier(session, ctx.guild.id)
# PHASE2:             has_discovery = FeatureLimits.get_limit(tier, "discovery_network")
# PHASE2: 
# PHASE2:             if not has_discovery:
# PHASE2:                 await ctx.respond("Server listings require QuestLog PRO!", ephemeral=True)
# PHASE2:                 return
# PHASE2: 
# PHASE2:             if len(title) > 100:
# PHASE2:                 await ctx.respond("Title must be 100 characters or less.", ephemeral=True)
# PHASE2:                 return
# PHASE2: 
# PHASE2:             # Check if in network
# PHASE2:             network = session.get(DiscoveryNetwork, ctx.guild.id)
# PHASE2:             if not network or not network.is_active:
# PHASE2:                 await ctx.respond(
# PHASE2:                     "Join the discovery network first with `/discovery join`.",
# PHASE2:                     ephemeral=True
# PHASE2:                 )
# PHASE2:                 return
# PHASE2: 
# PHASE2:             # Create or update listing
# PHASE2:             listing = session.get(ServerListing, ctx.guild.id)
# PHASE2:             if listing:
# PHASE2:                 listing.title = title
# PHASE2:                 listing.description = description
# PHASE2:                 listing.invite_code = invite_code
# PHASE2:                 listing.categories = category
# PHASE2:                 listing.tags = tags
# PHASE2:                 listing.member_count = ctx.guild.member_count
# PHASE2:                 listing.updated_at = int(time.time())
# PHASE2:                 action = "updated"
# PHASE2:             else:
# PHASE2:                 listing = ServerListing(
# PHASE2:                     guild_id=ctx.guild.id,
# PHASE2:                     title=title,
# PHASE2:                     description=description,
# PHASE2:                     invite_code=invite_code,
# PHASE2:                     categories=category,
# PHASE2:                     tags=tags,
# PHASE2:                     member_count=ctx.guild.member_count,
# PHASE2:                     is_published=True,
# PHASE2:                 )
# PHASE2:                 session.add(listing)
# PHASE2:                 action = "created"
# PHASE2: 
# PHASE2:         invite_url = f"https://discord.gg/{invite_code}" if invite_code else "Not set"
# PHASE2:         await ctx.respond(
# PHASE2:             f"**Server listing {action}!**\n\n"
# PHASE2:             f"**Title:** {title}\n"
# PHASE2:             f"**Category:** {category}\n"
# PHASE2:             f"**Invite:** {invite_url}\n\n"
# PHASE2:             f"Your server is now visible in `/discovery servers`!",
# PHASE2:             ephemeral=True
# PHASE2:         )
# PHASE2: 
# PHASE2:     @listing.command(name="edit", description="Edit your server listing (PRO)")
# PHASE2:     @discord.default_permissions(administrator=True)
# PHASE2:     @commands.has_permissions(administrator=True)
# PHASE2:     @discord.option(name="title", description="New title", required=False)
# PHASE2:     @discord.option(name="description", description="New description", required=False)
# PHASE2:     @discord.option(name="invite_code", description="New invite code", required=False)
# PHASE2:     @discord.option(
# PHASE2:         name="category",
# PHASE2:         description="New category",
# PHASE2:         required=False,
# PHASE2:         choices=["gaming", "streaming", "content", "esports", "casual", "competitive"]
# PHASE2:     )
# PHASE2:     @discord.option(name="tags", description="New tags", required=False)
# PHASE2:     async def listing_edit(
# PHASE2:         self,
# PHASE2:         ctx: discord.ApplicationContext,
# PHASE2:         title: str = None,
# PHASE2:         description: str = None,
# PHASE2:         invite_code: str = None,
# PHASE2:         category: str = None,
# PHASE2:         tags: str = None
# PHASE2:     ):
# PHASE2:         """Edit your server listing."""
# PHASE2:         with db_session_scope() as session:
# PHASE2:             listing = session.get(ServerListing, ctx.guild.id)
# PHASE2:             if not listing:
# PHASE2:                 await ctx.respond(
# PHASE2:                     "No listing found. Create one with `/listing create`.",
# PHASE2:                     ephemeral=True
# PHASE2:                 )
# PHASE2:                 return
# PHASE2: 
# PHASE2:             changes = []
# PHASE2:             if title:
# PHASE2:                 listing.title = title
# PHASE2:                 changes.append(f"Title: **{title}**")
# PHASE2:             if description:
# PHASE2:                 listing.description = description
# PHASE2:                 changes.append("Description: Updated")
# PHASE2:             if invite_code:
# PHASE2:                 listing.invite_code = invite_code
# PHASE2:                 changes.append(f"Invite: **{invite_code}**")
# PHASE2:             if category:
# PHASE2:                 listing.categories = category
# PHASE2:                 changes.append(f"Category: **{category}**")
# PHASE2:             if tags:
# PHASE2:                 listing.tags = tags
# PHASE2:                 changes.append(f"Tags: **{tags}**")
# PHASE2: 
# PHASE2:             listing.updated_at = int(time.time())
# PHASE2:             listing.member_count = ctx.guild.member_count
# PHASE2: 
# PHASE2:         if changes:
# PHASE2:             await ctx.respond("**Listing updated:**\n" + "\n".join(changes), ephemeral=True)
# PHASE2:         else:
# PHASE2:             await ctx.respond("No changes provided. Use options to update fields.", ephemeral=True)
# PHASE2: 
# PHASE2:     @listing.command(name="publish", description="Publish/unpublish your listing (PRO)")
# PHASE2:     @discord.default_permissions(administrator=True)
# PHASE2:     @commands.has_permissions(administrator=True)
# PHASE2:     @discord.option(name="published", description="Publish listing?", required=True)
# PHASE2:     async def listing_publish(
# PHASE2:         self,
# PHASE2:         ctx: discord.ApplicationContext,
# PHASE2:         published: bool
# PHASE2:     ):
# PHASE2:         """Publish or unpublish your server listing."""
# PHASE2:         with db_session_scope() as session:
# PHASE2:             listing = session.get(ServerListing, ctx.guild.id)
# PHASE2:             if not listing:
# PHASE2:                 await ctx.respond("No listing found. Create one with `/listing create`.", ephemeral=True)
# PHASE2:                 return
# PHASE2: 
# PHASE2:             listing.is_published = published
# PHASE2:             listing.updated_at = int(time.time())
# PHASE2: 
# PHASE2:         status = "published" if published else "unpublished"
# PHASE2:         await ctx.respond(f"Your server listing is now **{status}**.", ephemeral=True)
# PHASE2: 
# PHASE2:     @listing.command(name="stats", description="View your listing stats (PRO)")
# PHASE2:     async def listing_stats(self, ctx: discord.ApplicationContext):
# PHASE2:         """View your server listing stats."""
# PHASE2:         with db_session_scope() as session:
# PHASE2:             listing = session.get(ServerListing, ctx.guild.id)
# PHASE2:             if not listing:
# PHASE2:                 await ctx.respond("No listing found. Create one with `/listing create`.", ephemeral=True)
# PHASE2:                 return
# PHASE2: 
# PHASE2:             embed = discord.Embed(
# PHASE2:                 title=f"Listing Stats: {listing.title}",
# PHASE2:                 color=discord.Color.purple()
# PHASE2:             )
# PHASE2:             embed.add_field(name="Views", value=f"**{listing.views:,}**", inline=True)
# PHASE2:             embed.add_field(name="Clicks", value=f"**{listing.clicks:,}**", inline=True)
# PHASE2:             embed.add_field(name="Joins", value=f"**{listing.joins_from_discovery:,}**", inline=True)
# PHASE2:             embed.add_field(name="Status", value="Published" if listing.is_published else "Unpublished", inline=True)
# PHASE2:             embed.add_field(name="Category", value=listing.categories, inline=True)
# PHASE2:             embed.add_field(name="Member Count", value=f"{listing.member_count:,}", inline=True)
# PHASE2: 
# PHASE2:             if listing.views > 0:
# PHASE2:                 ctr = (listing.clicks / listing.views) * 100
# PHASE2:                 embed.add_field(name="Click Rate", value=f"**{ctr:.1f}%**", inline=True)
# PHASE2:             if listing.clicks > 0:
# PHASE2:                 join_rate = (listing.joins_from_discovery / listing.clicks) * 100
# PHASE2:                 embed.add_field(name="Join Rate", value=f"**{join_rate:.1f}%**", inline=True)
# PHASE2: 
# PHASE2:         await ctx.respond(embed=embed, ephemeral=True)
# PHASE2:
# PHASE2:     @listing.command(name="delete", description="Delete your server listing (Admin)")
# PHASE2:     @discord.default_permissions(administrator=True)
# PHASE2:     @commands.has_permissions(administrator=True)
# PHASE2:     async def listing_delete(self, ctx: discord.ApplicationContext):
# PHASE2:         """Delete your server listing."""
# PHASE2:         with db_session_scope() as session:
# PHASE2:             listing = session.get(ServerListing, ctx.guild.id)
# PHASE2:             if not listing:
# PHASE2:                 await ctx.respond("No listing found.", ephemeral=True)
# PHASE2:                 return
# PHASE2: 
# PHASE2:             session.delete(listing)
# PHASE2:
# PHASE2:         await ctx.respond("Your server listing has been deleted.", ephemeral=True)

    @promo.command(name="clearfeatured", description="Clear current featured person (Admin)")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    async def promo_clearfeatured(self, ctx: discord.ApplicationContext):
        """Clear the current featured person from the featured pool."""
        await ctx.defer()

        with db_session_scope() as session:
            config = session.get(DiscoveryConfig, ctx.guild.id)
            if not config or not config.selfpromo_channel_id:
                await ctx.respond("❌ Discovery is not configured for this server.", ephemeral=True)
                return

            # Find currently featured entries
            featured_entries = (
                session.query(FeaturedPool)
                .filter_by(guild_id=ctx.guild.id, was_selected=True)
                .all()
            )

            if not featured_entries:
                await ctx.respond("ℹ️ No one is currently featured.", ephemeral=True)
                return

            cleared_count = 0
            deleted_messages = 0

            for entry in featured_entries:
                # Try to delete the featured message
                if entry.featured_message_id:
                    try:
                        channel = self.bot.get_channel(config.selfpromo_channel_id)
                        if channel:
                            msg = await channel.fetch_message(entry.featured_message_id)
                            await msg.delete()
                            deleted_messages += 1
                    except discord.NotFound:
                        pass  # Message already deleted
                    except Exception as e:
                        logger.error(f"Error deleting featured message {entry.featured_message_id}: {e}")

                # Remove from featured pool
                session.delete(entry)
                cleared_count += 1

            # Clear last featured user tracking
            config.last_featured_user_id = None

        embed = discord.Embed(
            title="🧹 Featured Cleared",
            description=f"Cleared {cleared_count} featured entry/entries",
            color=discord.Color.green()
        )
        embed.add_field(name="Messages Deleted", value=str(deleted_messages), inline=True)

        await ctx.respond(embed=embed)
        logger.info(f"Cleared {cleared_count} featured entries in guild {ctx.guild.id} by {ctx.author}")

    # ========== GAME DISCOVERY COMMANDS ==========

    @discovery.command(name="game-settings", description="Configure game discovery settings (Admin)")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    @discord.option(
        name="enabled",
        description="Enable or disable game discovery",
        choices=["Enable", "Disable"],
        required=False
    )
    @discord.option(
        name="channel",
        description="Channel for game announcements",
        type=discord.TextChannel,
        required=False
    )
    @discord.option(
        name="interval_hours",
        description="Hours between discovery checks (default: 24)",
        min_value=1,
        max_value=168,
        required=False
    )
    async def game_settings(
        self,
        ctx: discord.ApplicationContext,
        enabled: str = None,
        channel: discord.TextChannel = None,
        interval_hours: int = None
    ):
        """Configure game discovery settings."""
        with db_session_scope() as session:
            config = session.get(DiscoveryConfig, ctx.guild.id)
            if not config:
                await ctx.respond("❌ Discovery is not configured. Ask an admin to run `/questlog setup`.", ephemeral=True)
                return

            changes = []

            if enabled:
                new_value = enabled == "Enable"
                config.game_discovery_enabled = new_value
                changes.append(f"Enabled: **{new_value}**")

            if channel:
                config.public_game_channel_id = channel.id
                changes.append(f"Public Channel: {channel.mention}")

            if interval_hours:
                config.game_check_interval_hours = interval_hours
                changes.append(f"Check Interval: **{interval_hours} hours**")

            if changes:
                embed = discord.Embed(
                    title="✅ Game Discovery Settings Updated",
                    description="\n".join(changes),
                    color=discord.Color.green()
                )
            else:
                # Show current settings
                embed = discord.Embed(
                    title="🎮 Game Discovery Settings",
                    color=discord.Color.blue()
                )
                embed.add_field(
                    name="Enabled",
                    value="✅ Yes" if config.game_discovery_enabled else "❌ No",
                    inline=True
                )
                if config.public_game_channel_id:
                    channel_obj = ctx.guild.get_channel(config.public_game_channel_id)
                    embed.add_field(
                        name="Public Channel",
                        value=channel_obj.mention if channel_obj else "Not set",
                        inline=True
                    )
                if config.private_game_channel_id:
                    channel_obj = ctx.guild.get_channel(config.private_game_channel_id)
                    embed.add_field(
                        name="Private Channel",
                        value=channel_obj.mention if channel_obj else "Not set",
                        inline=True
                    )
                embed.add_field(
                    name="Check Interval",
                    value=f"{config.game_check_interval_hours or 24} hours",
                    inline=True
                )

                last_check = "Never"
                if config.last_game_check_at:
                    hours_ago = (int(time.time()) - config.last_game_check_at) // 3600
                    last_check = f"{hours_ago} hours ago"
                embed.add_field(name="Last Check", value=last_check, inline=True)

        await ctx.respond(embed=embed, ephemeral=True)

    @discovery.command(name="game-filters", description="Configure genre/mode/platform filters (Admin)")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    @discord.option(
        name="genres",
        description="Comma-separated genres (e.g., RPG,ARPG,MMO)",
        required=False
    )
    @discord.option(
        name="modes",
        description="Comma-separated modes (e.g., Single-player,Co-op,Multiplayer)",
        required=False
    )
    @discord.option(
        name="platforms",
        description="Comma-separated platforms (e.g., PC,PlayStation 5,Xbox Series X|S)",
        required=False
    )
    async def game_filters(
        self,
        ctx: discord.ApplicationContext,
        genres: str = None,
        modes: str = None,
        platforms: str = None
    ):
        """Configure game discovery filters."""
        with db_session_scope() as session:
            config = session.get(DiscoveryConfig, ctx.guild.id)
            if not config:
                await ctx.respond("❌ Discovery is not configured. Ask an admin to run `/questlog setup`.", ephemeral=True)
                return

            changes = []

            if genres:
                genre_list = [g.strip() for g in genres.split(",")]
                config.game_genres = json.dumps(genre_list)
                changes.append(f"**Genres:** {', '.join(genre_list)}")

            if modes:
                mode_list = [m.strip() for m in modes.split(",")]
                config.game_modes = json.dumps(mode_list)
                changes.append(f"**Modes:** {', '.join(mode_list)}")

            if platforms:
                platform_list = [p.strip() for p in platforms.split(",")]
                config.game_platforms = json.dumps(platform_list)
                changes.append(f"**Platforms:** {', '.join(platform_list)}")

            if changes:
                embed = discord.Embed(
                    title="✅ Game Discovery Filters Updated",
                    description="\n".join(changes),
                    color=discord.Color.green()
                )
                embed.set_footer(text="Leave a filter empty to announce ALL games in that category")
            else:
                # Show current filters
                embed = discord.Embed(
                    title="🎮 Current Game Discovery Filters",
                    description="Empty filters mean ALL games in that category will be announced.",
                    color=discord.Color.blue()
                )

                # Genres
                if config.game_genres:
                    genre_list = json.loads(config.game_genres)
                    embed.add_field(
                        name="Genres",
                        value=", ".join(genre_list) if genre_list else "All",
                        inline=False
                    )
                else:
                    embed.add_field(name="Genres", value="All", inline=False)

                # Modes
                if config.game_modes:
                    mode_list = json.loads(config.game_modes)
                    embed.add_field(
                        name="Game Modes",
                        value=", ".join(mode_list) if mode_list else "All",
                        inline=False
                    )
                else:
                    embed.add_field(name="Game Modes", value="All", inline=False)

                # Platforms
                if config.game_platforms:
                    platform_list = json.loads(config.game_platforms)
                    embed.add_field(
                        name="Platforms",
                        value=", ".join(platform_list) if platform_list else "All",
                        inline=False
                    )
                else:
                    embed.add_field(name="Platforms", value="All", inline=False)

                embed.set_footer(text="Available: RPG, ARPG, FPS, MMO, Strategy, Action, Adventure, etc.")

        await ctx.respond(embed=embed, ephemeral=True)

    @discovery.command(name="check-games", description="Manually check for new games now (Admin)")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    async def check_games(self, ctx: discord.ApplicationContext):
        """Manually trigger game discovery check."""
        await ctx.defer()

        with db_session_scope() as session:
            config = session.get(DiscoveryConfig, ctx.guild.id)
            if not config:
                await ctx.respond("❌ Discovery is not configured. Ask an admin to run `/questlog setup`.", ephemeral=True)
                return

            if not config.game_discovery_enabled:
                await ctx.respond("❌ Game discovery is not enabled. Enable it with `/discovery game-settings`.", ephemeral=True)
                return

            if not config.public_game_channel_id and not config.private_game_channel_id:
                await ctx.respond("❌ No game discovery channels set. Set one with `/discovery game-settings` or the web dashboard.", ephemeral=True)
                return

            # Check if IGDB is configured
            if not igdb.is_configured():
                await ctx.respond("❌ IGDB is not configured. Add TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET to your .env file.", ephemeral=True)
                return

            # Get discovery channel (ONE channel for all announcements)
            discovery_channel = ctx.guild.get_channel(config.public_game_channel_id) if config.public_game_channel_id else None

            if not discovery_channel:
                await ctx.respond("❌ No game discovery channel configured.", ephemeral=True)
                return

            # Get all enabled search configurations
            search_configs = session.query(GameSearchConfig).filter(
                GameSearchConfig.guild_id == ctx.guild.id,
                GameSearchConfig.enabled == True
            ).all()

            if not search_configs:
                await ctx.respond("❌ No enabled search configurations found. Create searches on the web dashboard.", ephemeral=True)
                return

            now = int(time.time())
            all_games_to_announce = {}  # Key: IGDB ID, Value: {game, is_public}

            # Run each search configuration
            for search_config in search_configs:
                try:
                    # Parse filters
                    genres = json.loads(search_config.genres) if search_config.genres else None
                    themes = json.loads(search_config.themes) if search_config.themes else None
                    modes = json.loads(search_config.game_modes) if search_config.game_modes else None
                    platforms = json.loads(search_config.platforms) if search_config.platforms else None
                    announcement_window = search_config.days_ahead or 30
                    min_hype = search_config.min_hype
                    min_rating = search_config.min_rating

                    # Fetch games from IGDB
                    games = await igdb.search_upcoming_games(
                        days_ahead=365,
                        days_behind=0,
                        genres=genres,
                        themes=themes,
                        game_modes=modes,
                        platforms=platforms,
                        min_hype=min_hype,
                        min_rating=min_rating,
                        limit=100
                    )

                    # Filter to announcement window
                    announcement_cutoff = now + (announcement_window * 24 * 60 * 60)
                    games = [g for g in games if g.release_date and g.release_date <= announcement_cutoff]

                    # Add to master list, track privacy
                    for game in games:
                        if game.id not in all_games_to_announce:
                            all_games_to_announce[game.id] = {
                                "game": game,
                                "is_public": bool(search_config.show_on_website),
                            }

                except Exception as e:
                    logger.error(f"Error running search '{search_config.name}': {e}")
                    continue

            if not all_games_to_announce:
                await ctx.respond("ℹ️ No upcoming games found matching your search filters.", ephemeral=True)
                return

            # Filter to ONLY games with "Share on Discovery Network" enabled
            games_to_announce = []
            announced_count = 0

            for game_id, meta in all_games_to_announce.items():
                game = meta["game"]
                is_public = meta["is_public"]

                # ONLY announce games with "Share on Discovery Network" enabled
                if not is_public:
                    continue

                # Check if already announced
                already_announced = session.query(AnnouncedGame).filter(
                    AnnouncedGame.guild_id == ctx.guild.id,
                    AnnouncedGame.igdb_id == game.id
                ).first()

                if already_announced:
                    continue

                games_to_announce.append(game)

                # Record announcement in database
                try:
                    announced = AnnouncedGame(
                        guild_id=ctx.guild.id,
                        igdb_id=game.id,
                        igdb_slug=game.slug if hasattr(game, 'slug') else None,
                        steam_id=None,
                        game_name=game.name,
                        release_date=game.release_date,
                        genres=json.dumps(game.genres) if hasattr(game, 'genres') else None,
                        platforms=json.dumps(game.platforms),
                        cover_url=game.cover_url,
                        announced_at=now,
                        announcement_message_id=None
                    )
                    session.add(announced)
                    announced_count += 1
                except Exception as e:
                    logger.error(f"Failed to record game '{game.name}': {e}")
                    continue

            if announced_count == 0:
                await ctx.respond(f"ℹ️ Found {len(all_games_to_announce)} games, but all have already been announced.", ephemeral=True)
                return

            # Update last check time
            config.last_game_check_at = now

            # Send ONE summary embed for games with "Share on Discovery Network" enabled
            dash_url = f"{DASHBOARD_BASE_URL}/questlog/guild/{ctx.guild.id}/found-games/"
            summary_embed = discord.Embed(
                title="🎮 New Games Discovered!",
                description=f"Found **{announced_count}** new game{'s' if announced_count != 1 else ''} matching your searches!",
                color=discord.Color.green()
            )
            summary_embed.add_field(
                name="📊 View All Games",
                value=f"[Click here to view all {announced_count} games on the dashboard]({dash_url})",
                inline=False
            )
            public_search_count = len([s for s in search_configs if s.show_on_website])
            summary_embed.set_footer(text=f"Based on {public_search_count} active search configuration{'s' if public_search_count != 1 else ''}")

            ping_content = None
            if config.public_game_ping_role_id:
                ping_content = f"<@&{config.public_game_ping_role_id}>"

            await discovery_channel.send(content=ping_content, embed=summary_embed)

        # Respond to command
        response_embed = discord.Embed(
            title="✅ Game Discovery Complete",
            description=f"Announced **{announced_count}** new game{'s' if announced_count != 1 else ''}!",
            color=discord.Color.green()
        )
        response_embed.add_field(name="Dashboard", value=f"[View Games]({dash_url})", inline=False)
        await ctx.respond(embed=response_embed)

    @tasks.loop(hours=1)
    async def featured_reminder_task(self):
        """
        Send scheduled "How to Get Featured" reminders based on admin configuration.
        Runs every hour and checks if reminders need to be sent.
        """
        logger.debug("Running featured reminder task...")

        now = int(time.time())
        schedule_intervals = {
            "hourly": 3600,
            "every_6_hours": 21600,
            "daily": 86400,
            "weekly": 604800,
            "monthly": 2592000,
        }
        reminders = []

        # Avoid holding a DB session across awaits to prevent cross-task session conflicts.
        with db_session_scope() as session:
            # Get all guilds with Discovery enabled and reminders scheduled
            configs = session.query(DiscoveryConfig).filter(
                DiscoveryConfig.enabled == True,
                DiscoveryConfig.reminder_schedule != 'disabled',
                DiscoveryConfig.reminder_schedule.isnot(None)
            ).all()

            for config in configs:
                # Determine if reminder should be sent based on schedule
                schedule = (config.reminder_schedule or "").lower()
                last_sent = config.last_reminder_sent_at or 0

                # If never sent before, initialize timestamp without sending (prevents spam on bot restart)
                if last_sent == 0:
                    config.last_reminder_sent_at = now
                    logger.info(f"Initialized reminder timestamp for guild {config.guild_id} without sending")
                    continue

                interval = schedule_intervals.get(schedule)
                if not interval or (now - last_sent) < interval:
                    continue

                # Get guild settings for token_name
                guild_record = session.query(Guild).filter_by(guild_id=config.guild_id).first()
                token_name = guild_record.token_name if guild_record and guild_record.token_name else "Hero Tokens"

                reminders.append({
                    "guild_id": config.guild_id,
                    "schedule": schedule,
                    "selfpromo_channel_id": config.selfpromo_channel_id,
                    "feature_channel_id": config.feature_channel_id,
                    "how_to_enter_response": config.how_to_enter_response,
                    "token_cost": config.token_cost,
                    "entry_cooldown_hours": config.entry_cooldown_hours,
                    "token_name": token_name,
                })

        for reminder in reminders:
            try:
                # Get guild
                guild = self.bot.get_guild(reminder["guild_id"])
                if not guild:
                    logger.warning(f"Guild {reminder['guild_id']} not found for reminder task")
                    continue

                # Determine target channel (selfpromo or feature channel)
                target_channel = None
                if reminder["selfpromo_channel_id"]:
                    target_channel = guild.get_channel(reminder["selfpromo_channel_id"])
                elif reminder["feature_channel_id"]:
                    target_channel = guild.get_channel(reminder["feature_channel_id"])

                if not target_channel:
                    logger.warning(f"No target channel found for reminder in guild {reminder['guild_id']}")
                    continue

                # Build the embed - fully customizable by admin through "How to get Featured" field
                description = reminder["how_to_enter_response"] or (
                    "Want to be featured in our Creator Discovery system? Here's how!"
                )

                # Replace placeholders with actual values
                description = description.replace("{token_cost}", str(reminder["token_cost"]))
                description = description.replace("{token_name}", reminder["token_name"])
                description = description.replace(
                    "{entry_cooldown}",
                    f"{reminder['entry_cooldown_hours']} hours"
                )
                # Replace {hero_tokens} with ??? since we can't show individual balances in broadcast messages
                description = description.replace("{hero_tokens}", "???")

                embed = discord.Embed(
                    title="🌟 How to Get Featured",
                    description=description,
                    color=discord.Color.gold()
                )
                embed.timestamp = discord.utils.utcnow()

                # Send the embed
                await target_channel.send(embed=embed)
                logger.info(
                    f"Sent scheduled featured reminder to guild {reminder['guild_id']} (schedule: {reminder['schedule']})"
                )

                # Update last sent timestamp in a fresh session
                with db_session_scope() as session:
                    config = session.query(DiscoveryConfig).filter_by(guild_id=reminder["guild_id"]).first()
                    if config:
                        config.last_reminder_sent_at = int(time.time())

            except Exception as e:
                logger.error(
                    f"Error sending scheduled reminder for guild {reminder['guild_id']}: {e}",
                    exc_info=True
                )

    @featured_reminder_task.before_loop
    async def before_featured_reminder_task(self):
        await self.bot.wait_until_ready()

    @discord.slash_command(name="send-featured-reminder", description="Send a reminder about how to get featured")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    async def send_featured_reminder(
        self,
        ctx: discord.ApplicationContext,
        channel: discord.Option(discord.TextChannel, description="Channel to send the reminder to (defaults to self-promo channel)", required=False) = None
    ):
        """Send an embed explaining how to get featured in the discovery system."""
        await ctx.defer(ephemeral=True)

        with db_session_scope() as session:
            config = session.query(DiscoveryConfig).filter_by(guild_id=ctx.guild.id).first()

            if not config or not config.enabled:
                await ctx.respond("❌ Creator Discovery is not enabled in this server!", ephemeral=True)
                return

            # Determine target channel
            target_channel = channel
            if not target_channel:
                # Use selfpromo channel if available
                if config.selfpromo_channel_id:
                    target_channel = ctx.guild.get_channel(config.selfpromo_channel_id)
                elif config.feature_channel_id:
                    target_channel = ctx.guild.get_channel(config.feature_channel_id)

            if not target_channel:
                await ctx.respond("❌ Please specify a channel or configure the self-promo channel first!", ephemeral=True)
                return

            # Get guild settings for token_name
            guild_record = session.query(Guild).filter_by(guild_id=ctx.guild.id).first()
            token_name = guild_record.token_name if guild_record and guild_record.token_name else "Hero Tokens"

            # Build the embed - fully customizable by admin through "How to get Featured" field
            description = config.how_to_enter_response if config.how_to_enter_response else (
                "Want to be featured in our Creator Discovery system? Here's how!"
            )

            # Replace placeholders with actual values
            description = description.replace("{token_cost}", str(config.token_cost))
            description = description.replace("{token_name}", token_name)
            description = description.replace("{entry_cooldown}", f"{config.entry_cooldown_hours} hours" if hasattr(config, 'entry_cooldown_hours') else "24 hours")
            # Replace {hero_tokens} with ??? since we can't show individual balances in broadcast messages
            description = description.replace("{hero_tokens}", "???")

            embed = discord.Embed(
                title="🌟 How to Get Featured",
                description=description,
                color=discord.Color.gold()
            )
            embed.timestamp = discord.utils.utcnow()

            try:
                await target_channel.send(embed=embed)
                await ctx.respond(f"✅ Sent featured reminder to {target_channel.mention}!", ephemeral=True)
            except Exception as e:
                logger.error(f"Failed to send featured reminder: {e}")
                await ctx.respond(f"❌ Failed to send reminder: {str(e)}", ephemeral=True)

    # ====== COTW/COTM Auto-Rotation Task ======

    @tasks.loop(hours=24)  # Run daily at midnight UTC
    async def cotw_cotm_auto_rotation_task(self):
        """
        Automatic rotation task for Creator of the Week and Creator of the Month.
        Checks each guild's auto-rotation settings and rotates on the configured day.
        """
        from datetime import datetime, timezone
        import calendar

        now = datetime.now(timezone.utc)
        current_weekday = now.weekday()  # 0=Monday, 6=Sunday
        current_day_of_month = now.day   # 1-31

        # Get the last day of the current month
        last_day_of_month = calendar.monthrange(now.year, now.month)[1]

        logger.info(f"[COTW/COTM Auto-Rotation] Running daily check (Weekday: {current_weekday}, Day of Month: {current_day_of_month}/{last_day_of_month})")

        with db_session_scope() as session:
            # Get all guilds with auto-rotation enabled
            configs = session.query(DiscoveryConfig).filter(
                (DiscoveryConfig.cotw_auto_rotate == True) | (DiscoveryConfig.cotm_auto_rotate == True)
            ).all()

            logger.info(f"[COTW/COTM Auto-Rotation] Found {len(configs)} guilds with auto-rotation enabled")

            for config in configs:
                try:
                    guild = self.bot.get_guild(config.guild_id)
                    if not guild:
                        continue

                    # Check COTW rotation
                    if config.cotw_enabled and config.cotw_auto_rotate and config.cotw_rotation_day == current_weekday:
                        await self._rotate_creator_of_week(session, guild, config)

                    # Check COTM rotation
                    # If configured day is beyond this month's length, rotate on the last day instead
                    if config.cotm_enabled and config.cotm_auto_rotate:
                        rotation_day = min(config.cotm_rotation_day, last_day_of_month)
                        if current_day_of_month == rotation_day:
                            await self._rotate_creator_of_month(session, guild, config)

                except Exception as e:
                    logger.error(f"[COTW/COTM Auto-Rotation] Error processing guild {config.guild_id}: {e}", exc_info=True)

            # Network-wide COTW/COTM rotation (runs every Sunday for COTW, 1st of month for COTM)
            try:
                # Rotate Network COTW every Sunday (6=Sunday)
                if current_weekday == 6:
                    await self._rotate_network_creator_of_week(session)

                # Rotate Network COTM on the 1st of each month
                if current_day_of_month == 1:
                    await self._rotate_network_creator_of_month(session)

            except Exception as e:
                logger.error(f"[Network COTW/COTM Auto-Rotation] Error during network rotation: {e}", exc_info=True)

    @cotw_cotm_auto_rotation_task.before_loop
    async def before_cotw_cotm_auto_rotation_task(self):
        await self.bot.wait_until_ready()

    async def _rotate_creator_of_week(self, session, guild: discord.Guild, config: DiscoveryConfig):
        """Rotate Creator of the Week for a guild."""
        from models import CreatorProfile
        from sqlalchemy import or_

        logger.info(f"[COTW Rotation] Rotating Creator of the Week for guild {guild.id} ({guild.name})")

        # Calculate cooldown timestamp (2 weeks = 14 days)
        cooldown_seconds = 14 * 24 * 60 * 60  # 14 days in seconds
        cooldown_timestamp = int(time.time()) - cooldown_seconds

        # Get all eligible creators for this guild
        # Exclude: current COTW, current COTM, and anyone who was COTW in the last 2 weeks
        eligible_creators = session.query(CreatorProfile).filter(
            CreatorProfile.guild_id == guild.id,
            CreatorProfile.is_current_cotw == False,
            CreatorProfile.is_current_cotm == False,  # Don't pick current COTM
            or_(
                CreatorProfile.cotw_last_featured == None,  # Never been COTW before
                CreatorProfile.cotw_last_featured < cooldown_timestamp  # Last COTW was 2+ weeks ago
            )
        ).all()

        if not eligible_creators:
            logger.warning(f"[COTW Rotation] No eligible creators for guild {guild.id}")
            return

        # Clear previous COTW
        session.query(CreatorProfile).filter(
            CreatorProfile.guild_id == guild.id,
            CreatorProfile.is_current_cotw == True
        ).update({'is_current_cotw': False})

        # Randomly select new COTW
        new_cotw = random.choice(eligible_creators)
        new_cotw.is_current_cotw = True
        new_cotw.cotw_last_featured = int(time.time())
        session.commit()

        logger.info(f"[COTW Rotation] Selected {new_cotw.display_name} ({new_cotw.discord_id}) as new COTW for guild {guild.id}")

        # Post announcement to Discord if channel is configured
        if config.cotw_channel_id:
            channel = guild.get_channel(config.cotw_channel_id)
            if channel:
                try:
                    member = guild.get_member(new_cotw.discord_id)
                    if member:
                        embed = discord.Embed(
                            title="🏆 Creator of the Week",
                            description=f"Congratulations to **{new_cotw.display_name}** ({member.mention}) for being selected as this week's featured creator!",
                            color=discord.Color.gold()
                        )

                        if new_cotw.bio:
                            embed.add_field(name="About", value=new_cotw.bio[:1024], inline=False)

                        if member.avatar:
                            embed.set_thumbnail(url=member.avatar.url)

                        embed.add_field(
                            name="View Profile",
                            value=f"[See full creator profile](https://dashboard.casual-heroes.com/questlog/guild/{guild.id}/featured-creators/)",
                            inline=False
                        )

                        embed.set_footer(text="Auto-rotated weekly • Manually set COTW on Featured Creators page to override")
                        embed.timestamp = discord.utils.utcnow()

                        await channel.send(embed=embed)
                        logger.info(f"[COTW Rotation] Posted announcement to channel {channel.id}")
                except Exception as e:
                    logger.error(f"[COTW Rotation] Failed to post announcement: {e}", exc_info=True)

    async def _rotate_creator_of_month(self, session, guild: discord.Guild, config: DiscoveryConfig):
        """Rotate Creator of the Month for a guild."""
        from models import CreatorProfile
        from sqlalchemy import or_

        logger.info(f"[COTM Rotation] Rotating Creator of the Month for guild {guild.id} ({guild.name})")

        # Calculate cooldown timestamp (2 months = 60 days)
        cooldown_seconds = 60 * 24 * 60 * 60  # 60 days in seconds
        cooldown_timestamp = int(time.time()) - cooldown_seconds

        # Get all eligible creators for this guild
        # Exclude: current COTM, current COTW, and anyone who was COTM in the last 2 months
        eligible_creators = session.query(CreatorProfile).filter(
            CreatorProfile.guild_id == guild.id,
            CreatorProfile.is_current_cotm == False,
            CreatorProfile.is_current_cotw == False,  # Don't pick current COTW
            or_(
                CreatorProfile.cotm_last_featured == None,  # Never been COTM before
                CreatorProfile.cotm_last_featured < cooldown_timestamp  # Last COTM was 2+ months ago
            )
        ).all()

        if not eligible_creators:
            logger.warning(f"[COTM Rotation] No eligible creators for guild {guild.id}")
            return

        # Clear previous COTM
        session.query(CreatorProfile).filter(
            CreatorProfile.guild_id == guild.id,
            CreatorProfile.is_current_cotm == True
        ).update({'is_current_cotm': False})

        # Randomly select new COTM
        new_cotm = random.choice(eligible_creators)
        new_cotm.is_current_cotm = True
        new_cotm.cotm_last_featured = int(time.time())
        session.commit()

        logger.info(f"[COTM Rotation] Selected {new_cotm.display_name} ({new_cotm.discord_id}) as new COTM for guild {guild.id}")

        # Post announcement to Discord if channel is configured
        if config.cotm_channel_id:
            channel = guild.get_channel(config.cotm_channel_id)
            if channel:
                try:
                    member = guild.get_member(new_cotm.discord_id)
                    if member:
                        embed = discord.Embed(
                            title="👑 Creator of the Month",
                            description=f"Congratulations to **{new_cotm.display_name}** ({member.mention}) for being selected as this month's featured creator!",
                            color=discord.Color.purple()
                        )

                        if new_cotm.bio:
                            embed.add_field(name="About", value=new_cotm.bio[:1024], inline=False)

                        if member.avatar:
                            embed.set_thumbnail(url=member.avatar.url)

                        embed.add_field(
                            name="View Profile",
                            value=f"[See full creator profile](https://dashboard.casual-heroes.com/questlog/guild/{guild.id}/featured-creators/)",
                            inline=False
                        )

                        embed.set_footer(text="Auto-rotated monthly • Manually set COTM on Featured Creators page to override")
                        embed.timestamp = discord.utils.utcnow()

                        await channel.send(embed=embed)
                        logger.info(f"[COTM Rotation] Posted announcement to channel {channel.id}")
                except Exception as e:
                    logger.error(f"[COTM Rotation] Failed to post announcement: {e}", exc_info=True)

    async def _rotate_network_creator_of_week(self, session):
        """Rotate Network Creator of the Week (cross-server discovery)."""
        from models import CreatorProfile, DiscoveryConfig
        from sqlalchemy import or_

        logger.info(f"[Network COTW Rotation] Starting network-wide Creator of the Week rotation")

        # Calculate cooldown timestamp (2 weeks = 14 days)
        cooldown_seconds = 14 * 24 * 60 * 60  # 14 days in seconds
        cooldown_timestamp = int(time.time()) - cooldown_seconds

        # Get all eligible creators from the network (share_to_network=True)
        # Exclude: current Network COTW, current Network COTM, and anyone who was Network COTW in the last 2 weeks
        eligible_creators = session.query(CreatorProfile).filter(
            CreatorProfile.share_to_network == True,
            CreatorProfile.is_current_network_cotw == False,
            CreatorProfile.is_current_network_cotm == False,  # Don't pick current Network COTM
            or_(
                CreatorProfile.network_cotw_last_featured == None,  # Never been Network COTW before
                CreatorProfile.network_cotw_last_featured < cooldown_timestamp  # Last Network COTW was 2+ weeks ago
            )
        ).all()

        if not eligible_creators:
            logger.warning(f"[Network COTW Rotation] No eligible creators for network rotation")
            return

        # Clear previous Network COTW
        session.query(CreatorProfile).filter(
            CreatorProfile.is_current_network_cotw == True
        ).update({'is_current_network_cotw': False})

        # Randomly select new Network COTW
        new_cotw = random.choice(eligible_creators)
        new_cotw.is_current_network_cotw = True
        new_cotw.network_cotw_last_featured = int(time.time())
        session.commit()

        logger.info(f"[Network COTW Rotation] Selected {new_cotw.display_name} ({new_cotw.discord_id}) from guild {new_cotw.guild_id} as new Network COTW")

        # Post announcement to all opted-in guilds
        await self._announce_network_creator(session, new_cotw, "COTW")

    async def _rotate_network_creator_of_month(self, session):
        """Rotate Network Creator of the Month (cross-server discovery)."""
        from models import CreatorProfile, DiscoveryConfig
        from sqlalchemy import or_

        logger.info(f"[Network COTM Rotation] Starting network-wide Creator of the Month rotation")

        # Calculate cooldown timestamp (2 months = 60 days)
        cooldown_seconds = 60 * 24 * 60 * 60  # 60 days in seconds
        cooldown_timestamp = int(time.time()) - cooldown_seconds

        # Get all eligible creators from the network (share_to_network=True)
        # Exclude: current Network COTM, current Network COTW, and anyone who was Network COTM in the last 2 months
        eligible_creators = session.query(CreatorProfile).filter(
            CreatorProfile.share_to_network == True,
            CreatorProfile.is_current_network_cotm == False,
            CreatorProfile.is_current_network_cotw == False,  # Don't pick current Network COTW
            or_(
                CreatorProfile.network_cotm_last_featured == None,  # Never been Network COTM before
                CreatorProfile.network_cotm_last_featured < cooldown_timestamp  # Last Network COTM was 2+ months ago
            )
        ).all()

        if not eligible_creators:
            logger.warning(f"[Network COTM Rotation] No eligible creators for network rotation")
            return

        # Clear previous Network COTM
        session.query(CreatorProfile).filter(
            CreatorProfile.is_current_network_cotm == True
        ).update({'is_current_network_cotm': False})

        # Randomly select new Network COTM
        new_cotm = random.choice(eligible_creators)
        new_cotm.is_current_network_cotm = True
        new_cotm.network_cotm_last_featured = int(time.time())
        session.commit()

        logger.info(f"[Network COTM Rotation] Selected {new_cotm.display_name} ({new_cotm.discord_id}) from guild {new_cotm.guild_id} as new Network COTM")

        # Post announcement to all opted-in guilds
        await self._announce_network_creator(session, new_cotm, "COTM")

    async def _announce_network_creator(self, session, creator_profile, award_type: str):
        """
        Announce Network COTW/COTM to all opted-in guilds.

        Args:
            creator_profile: The CreatorProfile that was selected
            award_type: "COTW" or "COTM"
        """
        from models import DiscoveryConfig

        # Get all guilds that opted in to network announcements
        configs = session.query(DiscoveryConfig).filter(
            DiscoveryConfig.network_announcements_enabled == True,
            DiscoveryConfig.network_announcement_channel_id != None
        ).all()

        logger.info(f"[Network {award_type} Announcement] Posting to {len(configs)} opted-in guilds")

        # Prepare embed details
        if award_type == "COTW":
            title = "🏆 Network Creator of the Week"
            description = f"Congratulations to **{creator_profile.display_name}** for being selected as this week's featured creator across the QuestLog network!"
            color = discord.Color.gold()
            footer_text = "Auto-rotated weekly • View all creators on Discovery Network"
        else:  # COTM
            title = "👑 Network Creator of the Month"
            description = f"Congratulations to **{creator_profile.display_name}** for being selected as this month's featured creator across the QuestLog network!"
            color = discord.Color.purple()
            footer_text = "Auto-rotated monthly • View all creators on Discovery Network"

        # Get the creator's home guild for avatar
        home_guild = self.bot.get_guild(creator_profile.guild_id)
        home_member = home_guild.get_member(creator_profile.discord_id) if home_guild else None

        # Post to each opted-in guild
        for config in configs:
            try:
                guild = self.bot.get_guild(config.guild_id)
                if not guild:
                    continue

                channel = guild.get_channel(config.network_announcement_channel_id)
                if not channel:
                    logger.warning(f"[Network {award_type} Announcement] Channel {config.network_announcement_channel_id} not found in guild {guild.id}")
                    continue

                embed = discord.Embed(
                    title=title,
                    description=description,
                    color=color
                )

                if creator_profile.bio:
                    embed.add_field(name="About", value=creator_profile.bio[:1024], inline=False)

                # Add home server info
                if home_guild:
                    embed.add_field(name="Home Server", value=home_guild.name, inline=True)

                # Add social links if available
                social_links = []
                if creator_profile.twitch_handle:
                    social_links.append(f"[Twitch](https://twitch.tv/{creator_profile.twitch_handle})")
                if creator_profile.youtube_handle:
                    social_links.append(f"[YouTube](https://youtube.com/@{creator_profile.youtube_handle})")
                if creator_profile.twitter_handle:
                    social_links.append(f"[Twitter](https://twitter.com/{creator_profile.twitter_handle})")
                if creator_profile.tiktok_handle:
                    social_links.append(f"[TikTok](https://tiktok.com/@{creator_profile.tiktok_handle})")
                if creator_profile.instagram_handle:
                    social_links.append(f"[Instagram](https://instagram.com/{creator_profile.instagram_handle})")
                if creator_profile.bluesky_handle:
                    social_links.append(f"[Bluesky](https://bsky.app/profile/{creator_profile.bluesky_handle})")

                if social_links:
                    embed.add_field(name="Follow", value=" • ".join(social_links), inline=False)

                # Add avatar from home server
                if home_member and home_member.avatar:
                    embed.set_thumbnail(url=home_member.avatar.url)

                embed.add_field(
                    name="View Profile",
                    value=f"[See full creator profile](https://dashboard.casual-heroes.com/discovery-network/featured-creators/)",
                    inline=False
                )

                embed.set_footer(text=footer_text)
                embed.timestamp = discord.utils.utcnow()

                await channel.send(embed=embed)
                logger.info(f"[Network {award_type} Announcement] Posted to guild {guild.id} ({guild.name}) in channel {channel.id}")

            except Exception as e:
                logger.error(f"[Network {award_type} Announcement] Failed to post to guild {config.guild_id}: {e}", exc_info=True)


def setup(bot: commands.Bot):
    bot.add_cog(DiscoveryCog(bot))
