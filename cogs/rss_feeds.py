# cogs/rss_feeds.py - RSS Feed Monitor
"""
Monitors RSS feeds and posts new entries to configured Discord channels.

This cog periodically checks RSS feeds for new entries and sends
Discord embed notifications to configured channels.

ARCHITECTURE:
- Runs every 1 minute to check feeds due for polling
- Respects per-feed poll intervals (5, 10, 15, 30, 60 min)
- Uses database-backed entry tracking to prevent duplicates
- Implements exponential backoff for failed feeds (max 60 min)
- Cleans up old entries older than 30 days

SECURITY FEATURES:
- Only polls feeds for guilds with bot present
- HTML sanitization in embed content
- Respects Discord embed limits (256 char title, 4096 char description)
- Rate limiting between feeds (1 second delay)
- Rate limiting between entries (2 second delay)

BILLING:
- Free tier: 3 feeds max (enforced at creation, not polling)
- Discovery Module / Complete / Lifetime: Unlimited
"""

import asyncio
import html
import json
import re
import time as time_lib
from typing import Optional, Dict, Any, List, Tuple

import discord
from discord.ext import commands, tasks

try:
    import feedparser
except ImportError:
    feedparser = None

from config import db_session_scope, logger
from models import (
    RSSFeed,
    RSSFeedEntry,
    Guild,
    GuildModule,
)

# Import requests for secure fetching (optional - falls back to feedparser if not available)
try:
    import requests
except ImportError:
    requests = None

# Security constants for RSS fetching
RSS_FETCH_TIMEOUT = 30  # seconds
RSS_MAX_SIZE = 5 * 1024 * 1024  # 5MB max feed size
RSS_MAX_REDIRECTS = 5


def _validate_rss_url_for_fetch(url: str) -> Tuple[bool, Optional[str]]:
    """
    Validate RSS feed URL for SSRF protections.
    Checks all resolved addresses (IPv4 and IPv6) for private/internal ranges.

    Returns:
        Tuple of (is_valid, error_message)
    """
    from urllib.parse import urlparse
    import ipaddress
    import socket

    if not url:
        return False, 'URL is required'

    url = url.strip()

    # Check scheme
    try:
        parsed = urlparse(url)
    except Exception:
        return False, 'Invalid URL format'

    if parsed.scheme not in ('http', 'https'):
        return False, 'URL must use HTTP or HTTPS'

    hostname = parsed.hostname
    if not hostname:
        return False, 'Invalid URL - no hostname'

    hostname_lower = hostname.lower()

    # Block obvious localhost patterns
    blocked_hosts = {
        'localhost', '127.0.0.1', '::1', '0.0.0.0',
        '[::1]', '[::ffff:127.0.0.1]', '[0:0:0:0:0:0:0:1]',
        '0', '0.0', '0.0.0', '127.1', '127.0.1'
    }
    if hostname_lower in blocked_hosts:
        return False, 'Localhost URLs are not allowed'

    # Block internal domain patterns
    blocked_suffixes = ['.local', '.internal', '.private', '.corp', '.lan', '.intranet', '.localdomain']
    for suffix in blocked_suffixes:
        if hostname_lower.endswith(suffix):
            return False, f'Internal domains ({suffix}) are not allowed'

    # Block cloud metadata endpoints
    metadata_hosts = ['169.254.169.254', 'metadata.google.internal', 'metadata.goog']
    if hostname_lower in metadata_hosts:
        return False, 'Cloud metadata endpoints are not allowed'

    # Resolve ALL addresses and check each one
    try:
        addr_info = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM)

        for family, sock_type, proto, canonname, sockaddr in addr_info:
            ip_str = sockaddr[0]
            try:
                ip_obj = ipaddress.ip_address(ip_str)

                if ip_obj.is_private:
                    return False, f'Private IP address not allowed: {ip_str}'
                if ip_obj.is_loopback:
                    return False, f'Loopback address not allowed: {ip_str}'
                if ip_obj.is_link_local:
                    return False, f'Link-local address not allowed: {ip_str}'
                if ip_obj.is_reserved:
                    return False, f'Reserved address not allowed: {ip_str}'
                if ip_obj.is_multicast:
                    return False, f'Multicast address not allowed: {ip_str}'

                # Check IPv4-mapped IPv6 addresses
                if isinstance(ip_obj, ipaddress.IPv6Address) and ip_obj.ipv4_mapped:
                    mapped_v4 = ip_obj.ipv4_mapped
                    if mapped_v4.is_private or mapped_v4.is_loopback or mapped_v4.is_link_local:
                        return False, f'IPv4-mapped address not allowed: {ip_str}'

            except ValueError:
                continue

    except socket.gaierror:
        pass  # DNS resolution failed - let the actual fetch handle it
    except Exception:
        pass

    return True, None


def _secure_fetch_rss_sync(url: str, timeout: int = RSS_FETCH_TIMEOUT, max_size: int = RSS_MAX_SIZE):
    """
    Synchronous secure RSS fetch with SSRF protections.
    Called from executor in async context.

    Returns:
        Tuple of (parsed_feed or None, error_message or None)
    """
    # Validate URL first
    is_valid, error = _validate_rss_url_for_fetch(url)
    if not is_valid:
        logger.warning(f"RSSFeeds: URL validation failed for {url}: {error}")
        return None, error

    # If requests is not available, fall back to feedparser (less secure but functional)
    if requests is None:
        logger.debug(f"RSSFeeds: Using feedparser fallback (requests not available) for {url}")
        try:
            parsed = feedparser.parse(url)
            if parsed.bozo and not parsed.entries:
                return None, str(parsed.get('bozo_exception', 'Parse error'))
            return parsed, None
        except Exception as e:
            return None, str(e)

    try:
        current_url = url
        redirect_count = 0

        while redirect_count <= RSS_MAX_REDIRECTS:
            response = requests.get(
                current_url,
                timeout=timeout,
                stream=True,
                allow_redirects=False,
                headers={
                    'User-Agent': 'QuestLog RSS Bot/1.0 (+https://questlog.gg)',
                    'Accept': 'application/rss+xml, application/xml, application/atom+xml, text/xml, */*'
                }
            )

            # Handle redirects manually - validate each hop
            if response.is_redirect or response.status_code in (301, 302, 303, 307, 308):
                redirect_url = response.headers.get('Location')
                if not redirect_url:
                    return None, 'Redirect with no Location header'

                # Handle relative redirects
                if redirect_url.startswith('/'):
                    from urllib.parse import urlparse, urlunparse
                    parsed = urlparse(current_url)
                    redirect_url = urlunparse((parsed.scheme, parsed.netloc, redirect_url, '', '', ''))

                # Validate redirect target
                is_valid, error = _validate_rss_url_for_fetch(redirect_url)
                if not is_valid:
                    return None, f'Blocked redirect: {error}'

                current_url = redirect_url
                redirect_count += 1
                response.close()
                continue

            break
        else:
            return None, f'Too many redirects (max {RSS_MAX_REDIRECTS})'

        if response.status_code != 200:
            response.close()
            return None, f'HTTP error: {response.status_code}'

        # Check content length
        content_length = response.headers.get('Content-Length')
        if content_length and int(content_length) > max_size:
            response.close()
            return None, f'Feed too large (max {max_size // 1024 // 1024}MB)'

        # Read with size limit
        content = b''
        for chunk in response.iter_content(chunk_size=8192):
            content += chunk
            if len(content) > max_size:
                response.close()
                return None, f'Feed too large (max {max_size // 1024 // 1024}MB)'

        response.close()

        # Parse the fetched content
        parsed = feedparser.parse(content)

        if parsed.bozo and not parsed.entries:
            return None, str(parsed.get('bozo_exception', 'Parse error'))

        return parsed, None

    except requests.Timeout:
        return None, f'Request timed out after {timeout} seconds'
    except requests.ConnectionError as e:
        return None, f'Connection error: {str(e)}'
    except requests.RequestException as e:
        return None, f'Request failed: {str(e)}'
    except Exception as e:
        return None, f'Unexpected error: {str(e)}'


class RSSFeedsCog(commands.Cog):
    """Monitors RSS feeds and posts new entries to Discord channels."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

        if feedparser is None:
            logger.error("RSSFeeds: feedparser library not installed! RSS monitoring disabled.")
            return

        # Maximum backoff: 60 minutes (3600 seconds)
        self.max_backoff_seconds = 3600
        # Base backoff: 5 minutes (300 seconds)
        self.base_backoff_seconds = 300

        # Start the monitoring loop
        self.rss_monitor_loop.start()
        self.rss_cleanup_loop.start()
        logger.info("RSSFeeds: Started RSS feed monitoring (1 minute interval)")
        logger.info("RSSFeeds: Started entry cleanup (24 hour interval)")

    def cog_unload(self):
        """Stop the loops when cog unloads."""
        self.rss_monitor_loop.cancel()
        self.rss_cleanup_loop.cancel()
        logger.info("RSSFeeds: Stopped RSS feed monitoring")

    def _strip_html(self, text: str) -> str:
        """Strip HTML tags and decode entities."""
        if not text:
            return ""
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        # Decode HTML entities
        text = html.unescape(text)
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _truncate(self, text: str, max_length: int, suffix: str = "...") -> str:
        """Truncate text to max_length, adding suffix if truncated."""
        if not text:
            return ""
        if len(text) <= max_length:
            return text
        return text[:max_length - len(suffix)] + suffix

    def _calculate_backoff(self, failure_count: int) -> int:
        """Calculate backoff time based on failure count."""
        if failure_count <= 0:
            return 0
        # Exponential backoff: base * 2^(failures-1), capped at max
        return min(
            self.base_backoff_seconds * (2 ** (failure_count - 1)),
            self.max_backoff_seconds
        )

    def _should_poll_feed(self, feed: RSSFeed, current_time: int) -> bool:
        """
        Check if a feed should be polled now.

        Args:
            feed: RSSFeed object
            current_time: Current Unix timestamp

        Returns:
            True if feed should be polled
        """
        if not feed.enabled:
            return False

        # Check if enough time has passed since last poll
        if feed.last_polled_at:
            interval_seconds = feed.poll_interval_minutes * 60
            time_since_last_poll = current_time - feed.last_polled_at
            if time_since_last_poll < interval_seconds:
                return False

        # Check exponential backoff for failed feeds
        if feed.consecutive_failures > 0 and feed.last_error_at:
            backoff_seconds = self._calculate_backoff(feed.consecutive_failures)
            time_since_error = current_time - feed.last_error_at
            if time_since_error < backoff_seconds:
                logger.debug(
                    f"RSSFeeds: Skipping feed {feed.id} ({feed.name}) due to backoff "
                    f"({feed.consecutive_failures} failures, {backoff_seconds}s backoff)"
                )
                return False

        return True

    def _extract_thumbnail(self, entry: Any, embed_config: Dict) -> Optional[str]:
        """Extract thumbnail URL from RSS entry based on config."""
        thumbnail_mode = embed_config.get('thumbnail_mode', 'rss')

        if thumbnail_mode == 'none':
            return None
        elif thumbnail_mode == 'custom':
            return embed_config.get('custom_thumbnail_url', '')

        # Mode is 'rss' - try to extract from feed
        # Check media:thumbnail
        if hasattr(entry, 'media_thumbnail') and entry.media_thumbnail:
            return entry.media_thumbnail[0].get('url')

        # Check media:content (common in many RSS feeds)
        if hasattr(entry, 'media_content') and entry.media_content:
            for media in entry.media_content:
                # Some feeds don't specify type, so also check URL extension
                media_type = media.get('type', '')
                media_url = media.get('url', '')
                if media_type.startswith('image/') or any(ext in media_url.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
                    return media_url

        # Check enclosures
        if hasattr(entry, 'enclosures') and entry.enclosures:
            for enc in entry.enclosures:
                enc_type = enc.get('type', '')
                enc_url = enc.get('href', enc.get('url', ''))
                if enc_type.startswith('image/') or any(ext in enc_url.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
                    return enc_url

        # Check for 'image' field (some feeds use this directly)
        if hasattr(entry, 'image') and entry.image:
            if isinstance(entry.image, dict):
                return entry.image.get('href', entry.image.get('url'))
            elif isinstance(entry.image, str):
                return entry.image

        # Try to extract from content HTML (check multiple content fields)
        content_fields = ['content', 'summary', 'description']
        for field in content_fields:
            content = entry.get(field, '')
            # Handle feedparser's content list format
            if isinstance(content, list) and content:
                content = content[0].get('value', '') if isinstance(content[0], dict) else str(content[0])

            if content:
                # Look for img tags with src attribute
                img_match = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', content, re.IGNORECASE)
                if img_match:
                    img_url = img_match.group(1)
                    # Skip tiny tracking pixels (usually 1x1)
                    if 'width="1"' not in content or 'height="1"' not in content:
                        return img_url

        # Check links array for image type
        if hasattr(entry, 'links') and entry.links:
            for link in entry.links:
                link_type = link.get('type', '')
                link_href = link.get('href', '')
                if link_type.startswith('image/') or link.get('rel') == 'enclosure' and any(ext in link_href.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
                    return link_href

        return None

    def _build_embed(self, entry: Any, feed: RSSFeed, embed_config: Dict) -> discord.Embed:
        """Build a Discord embed from an RSS entry."""
        # Extract title
        title = entry.get('title', 'No Title')
        title_prefix = embed_config.get('title_prefix', '')
        title_suffix = embed_config.get('title_suffix', '')
        custom_emoji = embed_config.get('custom_emoji_prefix', '')

        # Build title with proper spacing around prefix/suffix
        if title_prefix:
            title = f"{title_prefix} {title}"
        if title_suffix:
            title = f"{title} {title_suffix}"
        if custom_emoji:
            title = f"{custom_emoji} {title}"

        # Truncate title for Discord (256 char limit)
        title = self._truncate(title, 256)

        # Extract and clean description
        description = entry.get('summary', entry.get('description', ''))
        description = self._strip_html(description)

        # Truncate description based on config
        show_full = embed_config.get('show_full_content', False)
        max_desc = 4000 if show_full else 500
        description = self._truncate(description, max_desc)

        # Prepend custom description if configured
        custom_desc = embed_config.get('custom_description', '')
        if custom_desc:
            description = f"{custom_desc}\n\n{description}"
            # Re-truncate to ensure we don't exceed Discord limits
            description = self._truncate(description, 4096)

        # Get link
        link = entry.get('link', '')

        # Get color
        color_hex = embed_config.get('color', '#5865F2')
        try:
            if color_hex.startswith('#'):
                color = int(color_hex[1:], 16)
            else:
                color = 0x5865F2
        except:
            color = 0x5865F2

        # Create embed
        embed = discord.Embed(
            title=title,
            description=description,
            url=link,
            color=color
        )

        # Add author if enabled
        if embed_config.get('show_author', True):
            author = entry.get('author')
            if not author and hasattr(entry, 'author_detail'):
                author = entry.author_detail.get('name')
            if author:
                embed.set_author(name=self._truncate(author, 256))

        # Add image (use set_image for larger display, like Found Games)
        thumbnail_url = self._extract_thumbnail(entry, embed_config)
        if thumbnail_url:
            try:
                embed.set_image(url=thumbnail_url)
            except:
                pass  # Invalid URL, skip

        # Add categories/tags if enabled
        if embed_config.get('show_categories', False):
            tags = entry.get('tags', [])
            if tags:
                categories = [tag.get('term', tag.get('label', '')) for tag in tags[:5]]
                categories = [c for c in categories if c]
                if categories:
                    embed.add_field(
                        name="Tags",
                        value=", ".join(categories),
                        inline=True
                    )

        # Add publish date if enabled
        if embed_config.get('show_publish_date', True):
            published = entry.get('published', entry.get('updated'))
            if published:
                embed.add_field(
                    name="Published",
                    value=self._truncate(published, 100),
                    inline=True
                )

        # Add footer
        footer_text = embed_config.get('footer_text', '')
        if footer_text:
            embed.set_footer(text=self._truncate(footer_text, 2048))

        return embed

    def _get_entry_guid(self, entry: Any) -> str:
        """Get unique identifier for an RSS entry."""
        # Try guid first
        guid = entry.get('id', entry.get('guid'))
        if guid:
            return str(guid)[:500]

        # Fall back to link
        link = entry.get('link', '')
        if link:
            return link[:500]

        # Fall back to title + published
        title = entry.get('title', '')
        published = entry.get('published', '')
        return f"{title}:{published}"[:500]

    def _parse_published_time(self, entry: Any) -> Optional[int]:
        """Parse published time from RSS entry to Unix timestamp."""
        # Try time struct
        if hasattr(entry, 'published_parsed') and entry.published_parsed:
            try:
                import calendar
                return calendar.timegm(entry.published_parsed)
            except:
                pass

        if hasattr(entry, 'updated_parsed') and entry.updated_parsed:
            try:
                import calendar
                return calendar.timegm(entry.updated_parsed)
            except:
                pass

        return None

    async def _poll_feed(self, feed: RSSFeed) -> Tuple[bool, Optional[str], List[Any]]:
        """
        Poll an RSS feed and return new entries.

        Uses secure fetch with SSRF protections, timeouts, and size limits.

        Args:
            feed: RSSFeed object

        Returns:
            Tuple of (success, error_message, new_entries)
        """
        try:
            logger.debug(f"RSSFeeds: Polling feed {feed.id} ({feed.name}): {feed.feed_url}")

            # Fetch and parse in executor with SSRF protections
            loop = asyncio.get_event_loop()
            parsed, error = await loop.run_in_executor(
                None,
                _secure_fetch_rss_sync,
                feed.feed_url
            )

            if error:
                logger.warning(f"RSSFeeds: Secure fetch error for feed {feed.id}: {error}")
                return False, self._truncate(error, 500), []

            if not parsed or not parsed.entries:
                logger.debug(f"RSSFeeds: Feed {feed.id} is empty or has no entries")
                return False, 'Feed is empty', []

            logger.debug(f"RSSFeeds: Feed {feed.id} fetched successfully with {len(parsed.entries)} entries")
            return True, None, parsed.entries

        except Exception as e:
            logger.error(f"RSSFeeds: Error fetching feed {feed.id}: {e}", exc_info=True)
            return False, self._truncate(str(e), 500), []

    def _extract_entry_data(self, entry: Any, embed_config: Dict) -> Dict:
        """Extract all relevant data from an RSS entry for storage."""
        # Get summary/description and clean it
        summary = entry.get('summary', entry.get('description', ''))
        summary = self._strip_html(summary)
        summary = self._truncate(summary, 1000)

        # Get author
        author = entry.get('author')
        if not author and hasattr(entry, 'author_detail'):
            author = entry.author_detail.get('name')
        author = self._truncate(author, 256) if author else None

        # Get thumbnail
        thumbnail = self._extract_thumbnail(entry, embed_config)
        thumbnail = self._truncate(thumbnail, 500) if thumbnail else None

        # Get categories/tags
        categories = []
        tags = entry.get('tags', [])
        if tags:
            categories = [tag.get('term', tag.get('label', '')) for tag in tags[:10]]
            categories = [c for c in categories if c]

        return {
            'summary': summary,
            'author': author,
            'thumbnail': thumbnail,
            'categories': json.dumps(categories) if categories else None
        }

    async def _process_feed(self, feed: RSSFeed, db) -> int:
        """
        Process a single feed - poll and post new entries.

        - Saves ALL new entries to the database (for RSS Articles dashboard)
        - Posts to Discord based on max_individual_posts setting:
          - 0 = always summary (never individual posts)
          - N = if more than N new entries, use summary; otherwise individual posts
        - Summary embed uses admin's styling (color, emoji, footer, thumbnail)

        Args:
            feed: RSSFeed object
            db: Database session

        Returns:
            Number of new entries saved (not just posted)
        """
        current_time = int(time_lib.time())

        # ALWAYS update last_polled_at first to prevent rapid re-polling
        feed.last_polled_at = current_time

        # Get the channel
        channel = self.bot.get_channel(feed.channel_id)
        if not channel:
            logger.debug(f"RSSFeeds: Channel {feed.channel_id} not found for feed {feed.id}")
            return 0

        # Poll the feed
        success, error_msg, entries = await self._poll_feed(feed)

        if not success:
            feed.consecutive_failures += 1
            feed.last_error = error_msg
            feed.last_error_at = current_time
            logger.warning(f"RSSFeeds: Feed {feed.id} ({feed.name}) failed: {error_msg}")
            return 0

        # Clear failure tracking on success
        if feed.consecutive_failures > 0:
            feed.consecutive_failures = 0
            feed.last_error = None
            feed.last_error_at = None

        # Get embed config
        embed_config = {}
        if feed.embed_config:
            try:
                embed_config = json.loads(feed.embed_config)
            except:
                pass

        # Get existing entry GUIDs for this feed
        existing_entries = db.query(RSSFeedEntry.entry_guid).filter(
            RSSFeedEntry.feed_id == feed.id
        ).all()
        existing_guids = {e.entry_guid for e in existing_entries}

        # Find new entries (check last 50 to capture more for the dashboard)
        new_entries = []
        for entry in entries[:50]:
            guid = self._get_entry_guid(entry)
            if guid not in existing_guids:
                new_entries.append((entry, guid))

        if not new_entries:
            return 0

        # Apply category filtering if configured
        filter_mode = getattr(feed, 'category_filter_mode', 'none') or 'none'
        if filter_mode != 'none':
            category_filters = []
            if feed.category_filters:
                try:
                    category_filters = json.loads(feed.category_filters)
                except:
                    category_filters = []

            if category_filters:
                original_count = len(new_entries)
                filtered_entries = []
                # Convert filter categories to lowercase for case-insensitive matching
                filter_set_lower = {f.lower() for f in category_filters}

                for entry, guid in new_entries:
                    # Get entry's categories (lowercase for comparison)
                    entry_categories_lower = set()
                    tags = entry.get('tags', [])
                    for tag in tags:
                        cat = tag.get('term', tag.get('label', ''))
                        if cat:
                            entry_categories_lower.add(cat.lower())

                    # Check if entry matches filter (case-insensitive)
                    has_match = bool(entry_categories_lower & filter_set_lower)  # Any intersection

                    if filter_mode == 'include' and has_match:
                        filtered_entries.append((entry, guid))
                    elif filter_mode == 'exclude' and not has_match:
                        filtered_entries.append((entry, guid))

                new_entries = filtered_entries
                logger.debug(f"RSSFeeds: Category filter ({filter_mode}) reduced entries from {original_count} to {len(new_entries)} for feed {feed.id}")

                if not new_entries:
                    logger.debug(f"RSSFeeds: All entries filtered out by category filter for feed {feed.id}")
                    return 0

        # Process entries oldest-first (reverse the list)
        new_entries.reverse()
        total_new = len(new_entries)
        saved_count = 0
        posted_count = 0

        # First, save ALL entries to the database for the dashboard
        for entry, guid in new_entries:
            try:
                published_at = self._parse_published_time(entry)
                entry_data = self._extract_entry_data(entry, embed_config)

                entry_record = RSSFeedEntry(
                    feed_id=feed.id,
                    entry_guid=guid,
                    entry_link=self._truncate(entry.get('link', ''), 500),
                    entry_title=self._truncate(entry.get('title', ''), 500),
                    entry_summary=entry_data['summary'],
                    entry_author=entry_data['author'],
                    entry_thumbnail=entry_data['thumbnail'],
                    entry_categories=entry_data['categories'],
                    published_at=published_at,
                    posted_at=current_time,
                    message_id=None  # Will be updated if we post this entry
                )
                db.add(entry_record)
                saved_count += 1

                # Update feed tracking with the latest entry
                feed.last_entry_id = guid
                if published_at:
                    feed.last_entry_published = published_at

            except Exception as e:
                logger.error(f"RSSFeeds: Error saving entry to database: {e}")
                continue

        # Commit saved entries before posting - use commit() not flush()
        # flush() only sends SQL but doesn't commit the transaction
        db.commit()
        logger.info(f"RSSFeeds: Committed {saved_count} entries to database for feed {feed.id}")

        # Build the dashboard URL for this guild
        dashboard_url = f"https://dashboard.casual-heroes.com/questlog/guild/{feed.guild_id}/rss-articles/"

        # Build role ping content if configured
        role_ping_content = None
        if feed.ping_role_id:
            role_ping_content = f"<@&{feed.ping_role_id}>"

        # Check if admin wants to always use summary mode
        always_summary = embed_config.get('always_use_summary', False)

        # Get configurable max individual posts (default 5, 0 = always summary)
        max_individual = embed_config.get('max_individual_posts', 5)

        # Determine if we should use summary mode
        # Use summary if: admin enabled always_summary, OR max_individual is 0, OR more than max new entries
        use_summary = always_summary or (max_individual == 0) or (total_new > max_individual)

        logger.info(f"RSSFeeds: Feed {feed.id} - total_new={total_new}, always_summary={always_summary}, use_summary={use_summary}")

        if use_summary:
            # Send summary embed with admin's styling applied
            try:
                # Get admin's color setting (default Discord blurple)
                embed_color = 0x5865F2
                color_str = embed_config.get('color', '#5865F2')
                if color_str:
                    try:
                        embed_color = int(color_str.lstrip('#'), 16)
                    except:
                        pass

                # Build title with admin's emoji prefix
                emoji_prefix = embed_config.get('custom_emoji_prefix', '')
                title_prefix = embed_config.get('title_prefix', '')
                title_suffix = embed_config.get('title_suffix', '')

                title = f"{total_new} New Articles from {feed.name}"
                if title_prefix:
                    title = f"{title_prefix} {title}"
                if title_suffix:
                    title = f"{title} {title_suffix}"
                if emoji_prefix:
                    title = f"{emoji_prefix} {title}"
                else:
                    title = f"📰 {title}"  # Default emoji if none set

                # Build summary description with optional custom message
                custom_desc = embed_config.get('custom_description', '')
                summary_desc = f"{custom_desc}\n\n" if custom_desc else ""
                summary_desc += f"**[View All Articles on Dashboard]({dashboard_url})**"

                summary_embed = discord.Embed(
                    title=self._truncate(title, 256),
                    description=self._truncate(summary_desc, 4096),
                    color=embed_color
                )

                # Use admin's footer or default
                footer_text = embed_config.get('footer_text', '')
                if footer_text:
                    summary_embed.set_footer(text=self._truncate(footer_text, 2048))
                else:
                    summary_embed.set_footer(text="QuestLog RSS Feeds • View all on dashboard!")

                # Add custom thumbnail if configured
                thumbnail_mode = embed_config.get('thumbnail_mode', 'rss')
                if thumbnail_mode == 'custom':
                    custom_thumb = embed_config.get('custom_thumbnail_url', '')
                    if custom_thumb:
                        try:
                            summary_embed.set_thumbnail(url=custom_thumb)
                        except:
                            pass

                await channel.send(content=role_ping_content, embed=summary_embed)
                logger.info(f"RSSFeeds: Sent summary for {total_new} entries from feed {feed.id} (max_individual={max_individual})")
            except Exception as e:
                logger.error(f"RSSFeeds: Failed to send summary embed: {e}")
        else:
            # 5 or fewer entries - post individual embeds
            first_post = True
            for entry, guid in new_entries:
                try:
                    # Build embed
                    embed = self._build_embed(entry, feed, embed_config)

                    # Send to channel (only ping role on first post to avoid spam)
                    ping_content = role_ping_content if first_post else None
                    message = await channel.send(content=ping_content, embed=embed)
                    first_post = False

                    # Update the entry record with the message ID
                    entry_record = db.query(RSSFeedEntry).filter(
                        RSSFeedEntry.feed_id == feed.id,
                        RSSFeedEntry.entry_guid == guid
                    ).first()
                    if entry_record:
                        entry_record.message_id = message.id

                    posted_count += 1
                    logger.info(f"RSSFeeds: Posted entry from feed {feed.id} ({feed.name}) to channel {channel.name}")

                    # Rate limit between entries
                    await asyncio.sleep(2)

                except discord.Forbidden:
                    logger.warning(f"RSSFeeds: No permission to post in channel {channel.id} for feed {feed.id}")
                    break
                except discord.HTTPException as e:
                    logger.error(f"RSSFeeds: Failed to post entry: {e}")
                    continue
                except Exception as e:
                    logger.error(f"RSSFeeds: Error posting entry: {e}")
                    continue

            # After individual posts, also send the summary embed with dashboard link
            if posted_count > 0:
                try:
                    await asyncio.sleep(1)  # Brief pause before summary

                    # Build summary embed with admin's styling (same as summary-only mode)
                    embed_color = 0x5865F2
                    color_str = embed_config.get('color', '#5865F2')
                    if color_str:
                        try:
                            embed_color = int(color_str.lstrip('#'), 16)
                        except:
                            pass

                    # Build title with admin's emoji prefix
                    emoji_prefix = embed_config.get('custom_emoji_prefix', '')
                    title_prefix = embed_config.get('title_prefix', '')
                    title_suffix = embed_config.get('title_suffix', '')

                    title = f"{posted_count} New Articles from {feed.name}"
                    if title_prefix:
                        title = f"{title_prefix} {title}"
                    if title_suffix:
                        title = f"{title} {title_suffix}"
                    if emoji_prefix:
                        title = f"{emoji_prefix} {title}"
                    else:
                        title = f"📰 {title}"

                    # Build description with optional custom message
                    custom_desc = embed_config.get('custom_description', '')
                    summary_desc = f"{custom_desc}\n\n" if custom_desc else ""
                    summary_desc += f"**[View All Articles on Dashboard]({dashboard_url})**"

                    summary_embed = discord.Embed(
                        title=self._truncate(title, 256),
                        description=self._truncate(summary_desc, 4096),
                        color=embed_color
                    )

                    # Use admin's footer or default
                    footer_text = embed_config.get('footer_text', '')
                    if footer_text:
                        summary_embed.set_footer(text=self._truncate(footer_text, 2048))
                    else:
                        summary_embed.set_footer(text="QuestLog RSS Feeds • View all on dashboard!")

                    # Add custom thumbnail if configured
                    thumbnail_mode = embed_config.get('thumbnail_mode', 'rss')
                    if thumbnail_mode == 'custom':
                        custom_thumb = embed_config.get('custom_thumbnail_url', '')
                        if custom_thumb:
                            try:
                                summary_embed.set_thumbnail(url=custom_thumb)
                            except:
                                pass

                    # Send without role ping (already pinged on first article)
                    await channel.send(embed=summary_embed)
                    logger.info(f"RSSFeeds: Sent summary after {posted_count} individual posts for feed {feed.id}")
                except Exception as e:
                    logger.error(f"RSSFeeds: Failed to send summary embed: {e}")

        return saved_count

    @tasks.loop(minutes=1)
    async def rss_monitor_loop(self):
        """Main monitoring loop - runs every 1 minute."""
        if feedparser is None:
            return

        try:
            current_time = int(time_lib.time())
            feeds_checked = 0
            entries_posted = 0

            with db_session_scope() as db:
                # Get all enabled feeds that are due for polling
                feeds = db.query(RSSFeed).filter(
                    RSSFeed.enabled == True
                ).all()

                for feed in feeds:
                    # Check if feed should be polled
                    if not self._should_poll_feed(feed, current_time):
                        continue

                    # Check if bot is in the guild
                    guild = self.bot.get_guild(feed.guild_id)
                    if not guild:
                        logger.debug(f"RSSFeeds: Bot not in guild {feed.guild_id} for feed {feed.id}")
                        continue

                    feeds_checked += 1

                    # Process the feed
                    posted = await self._process_feed(feed, db)
                    entries_posted += posted

                    # Commit after each feed to save progress
                    db.commit()

                    # Rate limit between feeds
                    await asyncio.sleep(1)

            if feeds_checked > 0 or entries_posted > 0:
                logger.debug(f"RSSFeeds: Checked {feeds_checked} feeds, posted {entries_posted} entries")

        except Exception as e:
            logger.error(f"RSSFeeds: Error in monitor loop: {e}", exc_info=True)

    @rss_monitor_loop.before_loop
    async def before_rss_monitor(self):
        """Wait for bot to be ready before starting the loop."""
        await self.bot.wait_until_ready()
        logger.info("RSSFeeds: Bot ready, starting RSS monitoring")

    @tasks.loop(hours=24)
    async def rss_cleanup_loop(self):
        """Clean up old entry records (older than 30 days)."""
        try:
            with db_session_scope() as db:
                # Delete entries older than 30 days
                cutoff_time = int(time_lib.time()) - (30 * 24 * 60 * 60)

                deleted = db.query(RSSFeedEntry).filter(
                    RSSFeedEntry.posted_at < cutoff_time
                ).delete()

                if deleted > 0:
                    logger.info(f"RSSFeeds: Cleaned up {deleted} old entry records")

        except Exception as e:
            logger.error(f"RSSFeeds: Error in cleanup loop: {e}", exc_info=True)

    @rss_cleanup_loop.before_loop
    async def before_cleanup_loop(self):
        """Wait for bot to be ready before starting the cleanup loop."""
        await self.bot.wait_until_ready()


def setup(bot: commands.Bot):
    """Standard setup function for loading the cog."""
    bot.add_cog(RSSFeedsCog(bot))
