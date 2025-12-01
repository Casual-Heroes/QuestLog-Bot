# cogs/discovery.py - Discovery & Self-Promo System
"""
Full discovery system for Warden bot.

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
    Guild, GuildMember, PromoPost, FeaturedPool, FeaturedCreator, DiscoveryNetwork,
    ServerListing, PromoTier, DiscoveryConfig, XPConfig, AnnouncedGame,
    GameSearchConfig, CreatorOfTheMonth, CreatorOfTheWeek
)
from utils import igdb


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
    url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
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
    elif 'discord.gg' in domain or 'discord.com/invite' in url_lower:
        return 'discord'
    elif 'github.com' in domain:
        return 'github'
    elif 'reddit.com' in domain:
        return 'reddit'
    else:
        return 'other'


class DiscoveryCog(commands.Cog):
    """Discovery and self-promotion system."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.featured_selection_task.start()
        self.game_discovery_task.start()
        self.forum_scanner_task.start()
        self.creator_of_week_task.start()
        self.creator_of_month_task.start()

    def cog_unload(self):
        self.featured_selection_task.cancel()
        self.game_discovery_task.cancel()
        self.forum_scanner_task.cancel()
        self.creator_of_week_task.cancel()
        self.creator_of_month_task.cancel()

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
                    # Not enough hero_tokens - just acknowledge post
                    await message.add_reaction("👍")
                    if token_cost > 0:
                        reply = await message.reply(
                            f"💬 Thanks for sharing your content! Remember to be added to the featured pool you Need **{token_cost}.\n"
                            f"You have **{member.hero_tokens} Hero Tokens**. The more active you are the more Hero Tokens you earn! ",
                            delete_after=10
                        )
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

                    await message.add_reaction("⏰")  # Clock emoji
                    await message.reply(
                        f"⏰ You're on cooldown! Can enter the featured pool again in **{hours_left}h {minutes_left}m**.\n"
                        f"💰 Your {token_cost} hero_tokens were saved.",
                        delete_after=20
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
                reply = await message.reply(
                    f"✅ **Added to featured pool!** 🎉\n"
                    f"🎟️ Cost: {token_cost} hero_tokens ({member.hero_tokens} remaining)\n"
                    f"⏰ Next entry: <t:{next_cooldown}:R>\n"
                    f"🎲 Good luck getting featured!",
                    delete_after=20
                )

                # Quick Discord embed feature (Discord-only, doesn't add to website)
                if config.selfpromo_quick_feature:
                    await self._post_quick_feature_embed(
                        guild=message.guild,
                        channel=message.channel,
                        author=message.author,
                        content=content,
                        link_url=link_url,
                        platform=platform
                    )

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
        else:
            links['other'] = link_url

        return links

    async def _add_to_featured_creators_hall(self, guild_id: int, member: discord.Member, winner: FeaturedPool):
        """Add or update creator in permanent featured creators list."""
        with db_session_scope() as session:
            now = int(time.time())

            # Check if creator already exists
            creator = session.query(FeaturedCreator).filter_by(
                guild_id=guild_id,
                user_id=member.id
            ).first()

            # Extract social links from winner.link_url
            social_links = self._parse_social_links(winner.link_url)

            if creator:
                # Update existing creator
                creator.last_featured_at = now
                creator.times_featured += 1
                creator.avatar_url = member.display_avatar.url
                creator.display_name = member.display_name
                creator.username = member.name
                creator.bio = winner.content or creator.bio
                creator.updated_at = now

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
                if social_links.get('other'):
                    creator.other_links = social_links['other']

                logger.info(f"[Discovery] [guild_id:{guild_id}] Updated {member.display_name} in featured creators hall (times: {creator.times_featured})")
            else:
                # Create new creator entry
                creator = FeaturedCreator(
                    guild_id=guild_id,
                    user_id=member.id,
                    username=member.name,
                    display_name=member.display_name,
                    avatar_url=member.display_avatar.url,
                    first_featured_at=now,
                    last_featured_at=now,
                    times_featured=1,
                    twitch_url=social_links.get('twitch'),
                    youtube_url=social_links.get('youtube'),
                    twitter_url=social_links.get('twitter'),
                    tiktok_url=social_links.get('tiktok'),
                    instagram_url=social_links.get('instagram'),
                    other_links=social_links.get('other'),
                    bio=winner.content,
                    discord_connections=None,  # TODO: Implement OAuth flow
                    created_at=now,
                    updated_at=now
                )
                session.add(creator)
                logger.info(f"[Discovery] [guild_id:{guild_id}] Added {member.display_name} to featured creators hall (first time)")

            session.commit()

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

                # Set profile picture as thumbnail
                embed.set_thumbnail(url=member.display_avatar.url)

                # Set author with name
                embed.set_author(
                    name=member.display_name,
                    icon_url=member.display_avatar.url
                )

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

    @tasks.loop(hours=12)
    async def game_discovery_task(self):
        """
        Check for new game releases based on guild search configurations.
        Runs every 12 hours (guilds can have custom intervals).
        Uses IGDB for comprehensive game discovery with multiple saved searches.
        """
        logger.info("Running game discovery task...")

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
                    # Check if it's time to check (based on interval)
                    if config.last_game_check_at:
                        time_since_last = now - config.last_game_check_at
                        interval_seconds = (config.game_check_interval_hours or 24) * 3600
                        if time_since_last < interval_seconds:
                            logger.debug(f"Skipping guild {config.guild_id}, checked {time_since_last//3600}h ago")
                            continue

                    logger.info(f"Checking games for guild {config.guild_id}")

                    # Get the guild
                    guild = self.bot.get_guild(config.guild_id)
                    if not guild:
                        logger.warning(f"Could not find guild {config.guild_id}")
                        continue

                    # Verify at least one channel is accessible
                    public_channel = guild.get_channel(config.public_game_channel_id) if config.public_game_channel_id else None
                    private_channel = guild.get_channel(config.private_game_channel_id) if config.private_game_channel_id else None

                    if not public_channel and not private_channel:
                        logger.warning(f"Could not find any game discovery channels in guild {config.guild_id}")
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

                            # Add to master list (avoid duplicates across searches)
                            for game in games:
                                if game.id not in all_games_to_announce:
                                    all_games_to_announce[game.id] = game

                        except Exception as e:
                            logger.error(f"Error running search '{search_config.name}': {e}")
                            continue

                    logger.info(f"Total unique games found across all searches: {len(all_games_to_announce)}")

                    # Announce all unique games
                    for game_id, game in all_games_to_announce.items():
                        # Check if already announced
                        already_announced = session.query(AnnouncedGame).filter(
                            AnnouncedGame.guild_id == config.guild_id,
                            AnnouncedGame.igdb_id == game.id
                        ).first()

                        if already_announced:
                            continue

                        # Create and post announcement
                        try:
                            embed = self.create_game_announcement_embed(game)
                            message = await channel.send(embed=embed)

                            # Record announcement
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
                                announcement_message_id=message.id
                            )
                            session.add(announced)
                            announced_count += 1

                            logger.info(f"Announced game '{game.name}' (IGDB:{game.id}) in guild {config.guild_id}")

                        except Exception as e:
                            logger.error(f"Failed to announce game '{game.name}' in guild {config.guild_id}: {e}")
                            continue

                    # Update last check time
                    config.last_game_check_at = now
                    session.commit()

                    logger.info(f"Game discovery for guild {config.guild_id} complete. Announced {announced_count} new games.")

                except Exception as e:
                    logger.error(f"Error processing game discovery for guild {config.guild_id}: {e}", exc_info=True)

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
        import time

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
                try:
                    # Check subscription tier (Pro or Premium)
                    guild_record = session.query(Guild).filter_by(guild_id=config.guild_id).first()
                    if not guild_record or guild_record.subscription_tier not in ['pro', 'premium']:
                        logger.warning(f"[COTW] Guild {config.guild_id} has COTW enabled but not Pro/Premium tier")
                        continue

                    await self._select_and_announce_cotw(config.guild_id)
                    await asyncio.sleep(2)  # Small delay between guilds

                except Exception as e:
                    logger.error(f"[COTW] Error for guild {config.guild_id}: {e}", exc_info=True)

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
                try:
                    # Check subscription tier (Premium only)
                    guild_record = session.query(Guild).filter_by(guild_id=config.guild_id).first()
                    if not guild_record or guild_record.subscription_tier != 'premium':
                        logger.warning(f"[COTM] Guild {config.guild_id} has COTM enabled but not Premium tier")
                        continue

                    await self._select_and_announce_cotm(config.guild_id)
                    await asyncio.sleep(2)  # Small delay between guilds

                except Exception as e:
                    logger.error(f"[COTM] Error for guild {config.guild_id}: {e}", exc_info=True)

        logger.info("[COTM] Completed monthly COTM selection")

    @creator_of_month_task.before_loop
    async def before_creator_of_month_task(self):
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

            # Iterate through active threads
            processed = 0
            for thread in forum.threads:
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
                    # Get starter message (first post in thread)
                    starter_message = await thread.fetch_message(thread.id)

                    # Process creator (returns True if newly added)
                    is_new = await self._process_forum_creator(
                        guild_id=guild_id,
                        thread=thread,
                        message=starter_message,
                        config=config
                    )
                    processed += 1

                    # Post Discord embed if this is a new creator and announcement channel is configured
                    if is_new and config.intro_announcement_channel_id:
                        try:
                            announcement_channel = guild.get_channel(config.intro_announcement_channel_id)
                            if announcement_channel:
                                # Generate Discord thread URL
                                thread_url = f"https://discord.com/channels/{guild_id}/{thread.id}"

                                # Post embed to Discord
                                await self._post_forum_creator_embed(
                                    guild=guild,
                                    channel=announcement_channel,
                                    author=starter_message.author,
                                    content=starter_message.content,
                                    thread_url=thread_url
                                )
                        except Exception as embed_error:
                            logger.error(f"[Forum Scanner] Failed to post embed for thread {thread.id}: {embed_error}")

                except discord.NotFound:
                    logger.debug(f"[Forum Scanner] Thread {thread.id} starter message not found")
                except Exception as e:
                    logger.error(f"[Forum Scanner] Error processing thread {thread.id}: {e}", exc_info=True)

            if processed > 0:
                logger.info(f"[Forum Scanner] Processed {processed} intro threads for guild {guild_id}")

            # Update last scan time
            import time
            config.last_intro_scan_at = int(time.time())
            session.commit()

    async def _process_forum_creator(self, guild_id: int, thread: discord.Thread, message: discord.Message, config=None):
        """
        Add or update a creator from forum intro post.

        Returns:
            bool: True if creator was newly added, False if updated
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

            # Check if creator already exists
            creator = session.query(FeaturedCreator).filter_by(
                guild_id=guild_id,
                user_id=author.id
            ).first()

            is_new_creator = creator is None

            if creator:
                # Update existing creator
                creator.last_featured_at = now
                creator.times_featured += 1
                creator.avatar_url = author.display_avatar.url
                creator.display_name = author.display_name
                creator.username = author.name
                creator.bio = content
                creator.forum_thread_id = thread.id
                creator.forum_tag_name = tag_name
                creator.source = 'forum'
                creator.updated_at = now

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
                if social_links.get('other'):
                    creator.other_links = social_links['other']

                logger.debug(f"[Forum Scanner] Updated creator {author.display_name} in guild {guild_id}")
            else:
                # Create new creator
                creator = FeaturedCreator(
                    guild_id=guild_id,
                    user_id=author.id,
                    username=author.name,
                    display_name=author.display_name,
                    avatar_url=author.display_avatar.url,
                    first_featured_at=now,
                    last_featured_at=now,
                    times_featured=1,
                    twitch_url=social_links.get('twitch'),
                    youtube_url=social_links.get('youtube'),
                    twitter_url=social_links.get('twitter'),
                    tiktok_url=social_links.get('tiktok'),
                    instagram_url=social_links.get('instagram'),
                    other_links=social_links.get('other'),
                    bio=content,
                    source='forum',
                    forum_thread_id=thread.id,
                    forum_tag_name=tag_name,
                    discord_connections=None,
                    created_at=now,
                    updated_at=now
                )
                session.add(creator)
                logger.info(f"[Forum Scanner] Added new creator {author.display_name} to guild {guild_id}")

            session.commit()
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
            website_url = f"https://casual-heroes.com/warden/guild/{guild.id}/featured-creators"
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

            # Get all featured creators (forum-based only)
            creators = session.query(FeaturedCreator).filter_by(
                guild_id=guild_id,
                source='forum'
            ).all()

            if not creators:
                logger.warning(f"[COTW] No featured creators found for guild {guild_id}")
                return

            # Avoid featuring the same creator twice in a row
            if config.cotw_last_featured_user_id:
                creators = [c for c in creators if c.user_id != config.cotw_last_featured_user_id]

            if not creators:
                logger.warning(f"[COTW] All creators already featured for guild {guild_id}")
                return

            # Select random creator
            selected = random.choice(creators)

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

            if links:
                embed.add_field(name="🔗 Links", value=" • ".join(links), inline=False)

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

            # Get all featured creators (forum-based only)
            creators = session.query(FeaturedCreator).filter_by(
                guild_id=guild_id,
                source='forum'
            ).all()

            if not creators:
                logger.warning(f"[COTM] No featured creators found for guild {guild_id}")
                return

            # Avoid featuring the same creator twice in a row
            if config.cotm_last_featured_user_id:
                creators = [c for c in creators if c.user_id != config.cotm_last_featured_user_id]

            if not creators:
                logger.warning(f"[COTM] All creators already featured for guild {guild_id}")
                return

            # Select random creator
            selected = random.choice(creators)

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

            if links:
                embed.add_field(name="🔗 Links", value=" • ".join(links), inline=False)

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
                await ctx.respond("Guild not configured. Ask an admin to run `/warden setup`.", ephemeral=True)
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
                    "**Featured Pool requires Warden Premium!**\n\n"
                    "Ask a server admin to upgrade with `/warden premium`.\n\n"
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

    @discovery.command(name="servers", description="Browse servers in discovery network (PRO)")
    @discord.option(
        name="category",
        description="Filter by category",
        required=False,
        choices=["gaming", "streaming", "content", "esports", "casual", "competitive"]
    )
    async def discovery_servers(
        self,
        ctx: discord.ApplicationContext,
        category: str = None
    ):
        """Browse servers in the discovery network."""
        with db_session_scope() as session:
            tier = get_guild_tier(session, ctx.guild.id)
            has_discovery = FeatureLimits.get_limit(tier, "discovery_network")

            if not has_discovery:
                await ctx.respond(
                    "**Discovery Network requires Warden PRO!**\n\n"
                    "Upgrade with `/warden upgrade` to:\n"
                    "- Browse and join partner servers\n"
                    "- List your server in the directory\n"
                    "- Cross-promote with other communities",
                    ephemeral=True
                )
                return

            # Build query
            query = (
                session.query(ServerListing)
                .filter(
                    ServerListing.is_published == True,
                    ServerListing.guild_id != ctx.guild.id  # Don't show own server
                )
            )

            if category:
                query = query.filter(ServerListing.categories.contains(category))

            listings = query.order_by(ServerListing.member_count.desc()).limit(15).all()

            if not listings:
                await ctx.respond(
                    "No servers found in the discovery network yet.\n\n"
                    "Be the first! Use `/listing create` to add your server.",
                    ephemeral=True
                )
                return

            embed = discord.Embed(
                title="Discovery Network",
                description=f"Servers matching: **{category or 'All Categories'}**",
                color=discord.Color.purple()
            )

            for listing in listings:
                # Increment view count
                listing.views += 1

                tags = f"\n*{listing.tags}*" if listing.tags else ""
                invite_text = f"\n[Join Server](https://discord.gg/{listing.invite_code})" if listing.invite_code else ""

                embed.add_field(
                    name=f"{listing.title} ({listing.member_count:,} members)",
                    value=f"{listing.description[:150] if listing.description else 'No description'}{tags}{invite_text}",
                    inline=False
                )

            embed.set_footer(text=f"Showing {len(listings)} servers | /listing create to add yours")

        await ctx.respond(embed=embed, ephemeral=True)

    @discovery.command(name="join", description="Join the discovery network (Admin, PRO)")
    @commands.has_permissions(administrator=True)
    async def discovery_join(self, ctx: discord.ApplicationContext):
        """Join the discovery network."""
        with db_session_scope() as session:
            tier = get_guild_tier(session, ctx.guild.id)
            has_discovery = FeatureLimits.get_limit(tier, "discovery_network")

            if not has_discovery:
                await ctx.respond(
                    "**Discovery Network requires Warden PRO!**\n\n"
                    "Upgrade with `/warden upgrade` to access cross-server promotion.",
                    ephemeral=True
                )
                return

            # Check if already joined
            existing = session.get(DiscoveryNetwork, ctx.guild.id)
            if existing and existing.is_active:
                await ctx.respond(
                    "Your server is already in the discovery network!\n\n"
                    "Use `/discovery settings` to configure your preferences.",
                    ephemeral=True
                )
                return

            # Join network
            if existing:
                existing.is_active = True
            else:
                network = DiscoveryNetwork(
                    guild_id=ctx.guild.id,
                    is_active=True,
                    allow_incoming=True,
                    allow_outgoing=True,
                    categories="gaming",
                )
                session.add(network)

            # Enable discovery on guild
            guild = session.get(Guild, ctx.guild.id)
            if guild:
                guild.discovery_enabled = True

        await ctx.respond(
            "**Welcome to the Discovery Network!**\n\n"
            "Your server is now part of the cross-server promotion network.\n\n"
            "**Next steps:**\n"
            "1. `/listing create` - Create your server listing\n"
            "2. `/discovery settings` - Configure your preferences\n"
            "3. `/discovery servers` - Browse other servers",
            ephemeral=True
        )

    @discovery.command(name="settings", description="Configure discovery settings (Admin, PRO)")
    @commands.has_permissions(administrator=True)
    @discord.option(
        name="incoming",
        description="Allow incoming promo posts from other servers",
        required=False
    )
    @discord.option(
        name="outgoing",
        description="Share your promos with other servers",
        required=False
    )
    @discord.option(
        name="channel",
        description="Channel for cross-server promos",
        required=False
    )
    @discord.option(
        name="categories",
        description="Categories (comma-separated: gaming,streaming,esports)",
        required=False
    )
    async def discovery_settings(
        self,
        ctx: discord.ApplicationContext,
        incoming: bool = None,
        outgoing: bool = None,
        channel: discord.TextChannel = None,
        categories: str = None
    ):
        """Configure discovery network settings."""
        with db_session_scope() as session:
            tier = get_guild_tier(session, ctx.guild.id)
            has_discovery = FeatureLimits.get_limit(tier, "discovery_network")

            if not has_discovery:
                await ctx.respond("Discovery Network requires Warden PRO!", ephemeral=True)
                return

            network = session.get(DiscoveryNetwork, ctx.guild.id)
            if not network:
                await ctx.respond("Join the network first with `/discovery join`.", ephemeral=True)
                return

            # Update settings
            changes = []
            if incoming is not None:
                network.allow_incoming = incoming
                changes.append(f"Incoming promos: **{'Enabled' if incoming else 'Disabled'}**")
            if outgoing is not None:
                network.allow_outgoing = outgoing
                changes.append(f"Outgoing promos: **{'Enabled' if outgoing else 'Disabled'}**")
            if channel is not None:
                network.network_channel_id = channel.id
                changes.append(f"Network channel: {channel.mention}")
            if categories is not None:
                network.categories = categories.lower().replace(" ", "")
                changes.append(f"Categories: **{network.categories}**")

            if not changes:
                # Show current settings
                embed = discord.Embed(
                    title="Discovery Network Settings",
                    color=discord.Color.purple()
                )
                embed.add_field(
                    name="Incoming Promos",
                    value="Enabled" if network.allow_incoming else "Disabled",
                    inline=True
                )
                embed.add_field(
                    name="Outgoing Promos",
                    value="Enabled" if network.allow_outgoing else "Disabled",
                    inline=True
                )
                ch = ctx.guild.get_channel(network.network_channel_id) if network.network_channel_id else None
                embed.add_field(
                    name="Network Channel",
                    value=ch.mention if ch else "Not set",
                    inline=True
                )
                embed.add_field(
                    name="Categories",
                    value=network.categories or "gaming",
                    inline=True
                )
                await ctx.respond(embed=embed, ephemeral=True)
            else:
                await ctx.respond(
                    "**Settings updated:**\n" + "\n".join(changes),
                    ephemeral=True
                )

    @discovery.command(name="leave", description="Leave the discovery network (Admin)")
    @commands.has_permissions(administrator=True)
    async def discovery_leave(self, ctx: discord.ApplicationContext):
        """Leave the discovery network."""
        with db_session_scope() as session:
            network = session.get(DiscoveryNetwork, ctx.guild.id)
            if not network or not network.is_active:
                await ctx.respond("Your server is not in the discovery network.", ephemeral=True)
                return

            network.is_active = False

            # Unpublish listing
            listing = session.get(ServerListing, ctx.guild.id)
            if listing:
                listing.is_published = False

            guild = session.get(Guild, ctx.guild.id)
            if guild:
                guild.discovery_enabled = False

        await ctx.respond(
            "You've left the discovery network.\n"
            "Your server listing has been unpublished.\n\n"
            "Rejoin anytime with `/discovery join`.",
            ephemeral=True
        )

    # ========== SERVER LISTING COMMANDS ==========

    @listing.command(name="create", description="Create your server listing (PRO)")
    @commands.has_permissions(administrator=True)
    @discord.option(name="title", description="Server title (max 100 chars)", required=True)
    @discord.option(name="description", description="Server description", required=True)
    @discord.option(name="invite_code", description="Invite code (without discord.gg/)", required=False)
    @discord.option(
        name="category",
        description="Primary category",
        required=True,
        choices=["gaming", "streaming", "content", "esports", "casual", "competitive"]
    )
    @discord.option(name="tags", description="Tags (comma-separated)", required=False)
    async def listing_create(
        self,
        ctx: discord.ApplicationContext,
        title: str,
        description: str,
        category: str,
        invite_code: str = None,
        tags: str = None
    ):
        """Create or update your server listing."""
        with db_session_scope() as session:
            tier = get_guild_tier(session, ctx.guild.id)
            has_discovery = FeatureLimits.get_limit(tier, "discovery_network")

            if not has_discovery:
                await ctx.respond("Server listings require Warden PRO!", ephemeral=True)
                return

            if len(title) > 100:
                await ctx.respond("Title must be 100 characters or less.", ephemeral=True)
                return

            # Check if in network
            network = session.get(DiscoveryNetwork, ctx.guild.id)
            if not network or not network.is_active:
                await ctx.respond(
                    "Join the discovery network first with `/discovery join`.",
                    ephemeral=True
                )
                return

            # Create or update listing
            listing = session.get(ServerListing, ctx.guild.id)
            if listing:
                listing.title = title
                listing.description = description
                listing.invite_code = invite_code
                listing.categories = category
                listing.tags = tags
                listing.member_count = ctx.guild.member_count
                listing.updated_at = int(time.time())
                action = "updated"
            else:
                listing = ServerListing(
                    guild_id=ctx.guild.id,
                    title=title,
                    description=description,
                    invite_code=invite_code,
                    categories=category,
                    tags=tags,
                    member_count=ctx.guild.member_count,
                    is_published=True,
                )
                session.add(listing)
                action = "created"

        invite_url = f"https://discord.gg/{invite_code}" if invite_code else "Not set"
        await ctx.respond(
            f"**Server listing {action}!**\n\n"
            f"**Title:** {title}\n"
            f"**Category:** {category}\n"
            f"**Invite:** {invite_url}\n\n"
            f"Your server is now visible in `/discovery servers`!",
            ephemeral=True
        )

    @listing.command(name="edit", description="Edit your server listing (PRO)")
    @commands.has_permissions(administrator=True)
    @discord.option(name="title", description="New title", required=False)
    @discord.option(name="description", description="New description", required=False)
    @discord.option(name="invite_code", description="New invite code", required=False)
    @discord.option(
        name="category",
        description="New category",
        required=False,
        choices=["gaming", "streaming", "content", "esports", "casual", "competitive"]
    )
    @discord.option(name="tags", description="New tags", required=False)
    async def listing_edit(
        self,
        ctx: discord.ApplicationContext,
        title: str = None,
        description: str = None,
        invite_code: str = None,
        category: str = None,
        tags: str = None
    ):
        """Edit your server listing."""
        with db_session_scope() as session:
            listing = session.get(ServerListing, ctx.guild.id)
            if not listing:
                await ctx.respond(
                    "No listing found. Create one with `/listing create`.",
                    ephemeral=True
                )
                return

            changes = []
            if title:
                listing.title = title
                changes.append(f"Title: **{title}**")
            if description:
                listing.description = description
                changes.append("Description: Updated")
            if invite_code:
                listing.invite_code = invite_code
                changes.append(f"Invite: **{invite_code}**")
            if category:
                listing.categories = category
                changes.append(f"Category: **{category}**")
            if tags:
                listing.tags = tags
                changes.append(f"Tags: **{tags}**")

            listing.updated_at = int(time.time())
            listing.member_count = ctx.guild.member_count

        if changes:
            await ctx.respond("**Listing updated:**\n" + "\n".join(changes), ephemeral=True)
        else:
            await ctx.respond("No changes provided. Use options to update fields.", ephemeral=True)

    @listing.command(name="publish", description="Publish/unpublish your listing (PRO)")
    @commands.has_permissions(administrator=True)
    @discord.option(name="published", description="Publish listing?", required=True)
    async def listing_publish(
        self,
        ctx: discord.ApplicationContext,
        published: bool
    ):
        """Publish or unpublish your server listing."""
        with db_session_scope() as session:
            listing = session.get(ServerListing, ctx.guild.id)
            if not listing:
                await ctx.respond("No listing found. Create one with `/listing create`.", ephemeral=True)
                return

            listing.is_published = published
            listing.updated_at = int(time.time())

        status = "published" if published else "unpublished"
        await ctx.respond(f"Your server listing is now **{status}**.", ephemeral=True)

    @listing.command(name="stats", description="View your listing stats (PRO)")
    async def listing_stats(self, ctx: discord.ApplicationContext):
        """View your server listing stats."""
        with db_session_scope() as session:
            listing = session.get(ServerListing, ctx.guild.id)
            if not listing:
                await ctx.respond("No listing found. Create one with `/listing create`.", ephemeral=True)
                return

            embed = discord.Embed(
                title=f"Listing Stats: {listing.title}",
                color=discord.Color.purple()
            )
            embed.add_field(name="Views", value=f"**{listing.views:,}**", inline=True)
            embed.add_field(name="Clicks", value=f"**{listing.clicks:,}**", inline=True)
            embed.add_field(name="Joins", value=f"**{listing.joins_from_discovery:,}**", inline=True)
            embed.add_field(name="Status", value="Published" if listing.is_published else "Unpublished", inline=True)
            embed.add_field(name="Category", value=listing.categories, inline=True)
            embed.add_field(name="Member Count", value=f"{listing.member_count:,}", inline=True)

            if listing.views > 0:
                ctr = (listing.clicks / listing.views) * 100
                embed.add_field(name="Click Rate", value=f"**{ctr:.1f}%**", inline=True)
            if listing.clicks > 0:
                join_rate = (listing.joins_from_discovery / listing.clicks) * 100
                embed.add_field(name="Join Rate", value=f"**{join_rate:.1f}%**", inline=True)

        await ctx.respond(embed=embed, ephemeral=True)

    @listing.command(name="delete", description="Delete your server listing (Admin)")
    @commands.has_permissions(administrator=True)
    async def listing_delete(self, ctx: discord.ApplicationContext):
        """Delete your server listing."""
        with db_session_scope() as session:
            listing = session.get(ServerListing, ctx.guild.id)
            if not listing:
                await ctx.respond("No listing found.", ephemeral=True)
                return

            session.delete(listing)

        await ctx.respond("Your server listing has been deleted.", ephemeral=True)

    @promo.command(name="clearfeatured", description="Clear current featured person (Admin)")
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
                await ctx.respond("❌ Discovery is not configured. Ask an admin to run `/warden setup`.", ephemeral=True)
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
                await ctx.respond("❌ Discovery is not configured. Ask an admin to run `/warden setup`.", ephemeral=True)
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
    @commands.has_permissions(administrator=True)
    async def check_games(self, ctx: discord.ApplicationContext):
        """Manually trigger game discovery check."""
        await ctx.defer()

        with db_session_scope() as session:
            config = session.get(DiscoveryConfig, ctx.guild.id)
            if not config:
                await ctx.respond("❌ Discovery is not configured. Ask an admin to run `/warden setup`.", ephemeral=True)
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

            # Get at least one channel
            public_channel = ctx.guild.get_channel(config.public_game_channel_id) if config.public_game_channel_id else None
            private_channel = ctx.guild.get_channel(config.private_game_channel_id) if config.private_game_channel_id else None

            if not public_channel and not private_channel:
                await ctx.respond("❌ Game discovery channels not found.", ephemeral=True)
                return

            # Parse filters
            genres = json.loads(config.game_genres) if config.game_genres else None
            themes = json.loads(config.game_themes) if config.game_themes else None
            modes = json.loads(config.game_modes) if config.game_modes else None
            platforms = json.loads(config.game_platforms) if config.game_platforms else None
            announcement_window = config.game_days_ahead or 30  # How far ahead to announce
            min_hype = config.game_min_hype
            min_rating = config.game_min_rating

            # Fetch games from IGDB - search wide window (1 year) with filters
            # Then we'll filter down to only announce games within the announcement window
            games = []
            try:
                now = int(time.time())
                games = await igdb.search_upcoming_games(
                    days_ahead=365,  # Search next year
                    days_behind=0,   # Only upcoming games
                    genres=genres,
                    themes=themes,
                    game_modes=modes,
                    platforms=platforms,
                    min_hype=min_hype,
                    min_rating=min_rating,
                    limit=100  # Increased limit since we're filtering wider
                )
                logger.info(f"Found {len(games)} total games from IGDB")

                # Filter to only games within announcement window
                announcement_cutoff = now + (announcement_window * 24 * 60 * 60)
                games = [g for g in games if g.release_date and g.release_date <= announcement_cutoff]
                logger.info(f"Filtered to {len(games)} games within {announcement_window} day announcement window")
            except Exception as e:
                logger.error(f"Error fetching from IGDB: {e}")
                await ctx.respond(f"❌ Error fetching games from IGDB: {str(e)}", ephemeral=True)
                return

            if not games:
                await ctx.respond("ℹ️ No upcoming games found matching your filters.", ephemeral=True)
                return

            # Check which are new
            new_games = []
            for game in games:
                already_announced = session.query(AnnouncedGame).filter(
                    AnnouncedGame.guild_id == ctx.guild.id,
                    AnnouncedGame.igdb_id == game.id
                ).first()

                if not already_announced:
                    new_games.append(game)

            if not new_games:
                await ctx.respond(f"ℹ️ Found {len(games)} upcoming games, but all have already been announced.", ephemeral=True)
                return

            # Announce new games (use public channel if available, else private)
            announcement_channel = public_channel or private_channel
            announced_count = 0

            for game in new_games[:10]:  # Limit to 10 games per manual check
                try:
                    embed = self.create_game_announcement_embed(game)
                    message = await announcement_channel.send(embed=embed)

                    # Record announcement
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
                        announcement_message_id=message.id
                    )
                    session.add(announced)
                    announced_count += 1

                except Exception as e:
                    logger.error(f"Failed to announce game '{game.name}': {e}")
                    continue

            # Update last check time
            config.last_game_check_at = now

            result_embed = discord.Embed(
                title="✅ Game Discovery Check Complete",
                description=f"Announced {announced_count} new game(s) in {channel.mention}",
                color=discord.Color.green()
            )
            result_embed.add_field(name="Source", value="IGDB", inline=True)
            result_embed.add_field(name="Total Found", value=str(len(games)), inline=True)
            result_embed.add_field(name="New Games", value=str(len(new_games)), inline=True)
            result_embed.add_field(name="Announced", value=str(announced_count), inline=True)

            if announced_count < len(new_games):
                result_embed.set_footer(text=f"{len(new_games) - announced_count} more will be announced in the next check")

        await ctx.respond(embed=result_embed)


def setup(bot: commands.Bot):
    bot.add_cog(DiscoveryCog(bot))
