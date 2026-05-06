# cogs/bridge_cog.py - Discord <-> Fluxer message bridge (WardenBot side)
#
# Mirrors bridge.py in questlogfluxer but for Discord.
# Relays Discord messages to the Fluxer side via QuestLog internal hub.
#
# Flow (Discord -> Fluxer):
#   1. User sends message in a bridged Discord channel
#   2. on_message fires -> POST to /ql/internal/bridge/relay/ with source=discord
#   3. Hub queues it for the Fluxer side
#   4. questlogfluxer bridge cog picks it up and posts to Fluxer channel
#
# Flow (Fluxer -> Discord):
#   1. questlogfluxer relays to hub (source=fluxer)
#   2. This cog polls /ql/internal/bridge/pending/discord/ every 3s
#   3. Posts formatted message to the Discord target channel
#   4. Records sent message ID in /ql/internal/bridge/message-map/
#
# Reactions:
#   - on_raw_reaction_add fires for unicode emojis only
#   - POSTs to /ql/internal/bridge/reaction/ with platform + message_id + emoji
#   - Hub looks up cross-platform message map and queues the reaction
#   - Polls /ql/internal/bridge/pending-reactions/discord/ every 6s
#
# Replies:
#   - message.reference.resolved or fetch_message provides the quoted message
#   - Sent as "> quote\n**[F] Author:** reply"
#
# Anti-loop:
#   - Never relay messages from bots (message.author.bot)
#   - Never relay messages starting with "**[D]" or "**[F]"

import asyncio
import logging
import re
import urllib.parse

import aiohttp
import discord
from discord.ext import commands

from config import logger, QUESTLOG_INTERNAL_API_URL, QUESTLOG_BOT_SECRET, MATRIX_ACCESS_TOKEN, MATRIX_HOMESERVER, FLUXER_API_URL, FLUXER_BOT_TOKEN

_BASE = QUESTLOG_INTERNAL_API_URL.rstrip('/')
_RELAY_URL              = _BASE + '/api/internal/bridge/relay/'
_PENDING_URL            = _BASE + '/api/internal/bridge/pending/discord/'
_MSG_MAP_URL            = _BASE + '/api/internal/bridge/message-map/'
_THREAD_MAP_URL         = _BASE + '/api/internal/bridge/thread-map/'
_REACTION_URL           = _BASE + '/api/internal/bridge/reaction/'
_PENDING_REACTIONS_URL  = _BASE + '/api/internal/bridge/pending-reactions/discord/'
_DELETE_URL             = _BASE + '/api/internal/bridge/delete/'
_PENDING_DELETIONS_URL  = _BASE + '/api/internal/bridge/pending-deletions/discord/'
_EDIT_URL               = _BASE + '/api/internal/bridge/edit/'
_PENDING_EDITS_URL      = _BASE + '/api/internal/bridge/pending-edits/discord/'

_HEADERS = {'X-Bot-Secret': QUESTLOG_BOT_SECRET, 'Content-Type': 'application/json'}
_TYPING_URL = _BASE + '/api/internal/bridge/typing/'
_MATRIX_TYPING_TIMEOUT_MS = 8000  # Matrix typing indicator expires after 8 seconds

_RELAY_PREFIXES = ('**[D]', '**[F]', '**[M]', '[D]', '[F]', '[M]')

_CUSTOM_EMOJI_RE = re.compile(r'<a?:(\w+):\d+>')

# Matches a string that is purely one or more URLs (possibly separated by whitespace)
_URL_ONLY_RE = re.compile(r'^(https?://\S+\s*)+$')


def _format_bridged(tag: str, author: str, content: str, reply_quote: str = '') -> str:
    """
    Format a bridged message for delivery.
    If content is a bare URL (or URLs), put them on a new line so the platform
    can unfurl the link preview. Mixed text+URL stays on one line.
    """
    header = f"**[{tag}] {author}:**"
    body = content.rstrip() if content else ''
    if reply_quote:
        prefix = f"> {reply_quote}\n"
    else:
        prefix = ''
    if body and _URL_ONLY_RE.match(body):
        return f"{prefix}{header}\n{body}"
    return f"{prefix}{header} {body}".rstrip()


def _resolve_discord_content(message: discord.Message) -> tuple[str, list]:
    """
    Resolve Discord mention markup and custom emoji for relay.
    Returns (content, mentions) where:
    - content keeps raw <@userid> tokens for user mentions (hub will resolve cross-platform)
    - mentions is a list of {id, display_name} for each mentioned user
    - role mentions and custom emoji are resolved to readable text
    """
    content = message.content or ''

    # Build mention lookup maps from objects already attached to the message
    user_map = {str(m.id): (m.display_name or m.name) for m in message.mentions}
    role_map = {str(r.id): r.name for r in message.role_mentions}

    # Build mentions list for hub resolution - keep <@userid> tokens as-is in content
    mentions = [
        {'id': uid, 'display_name': name}
        for uid, name in user_map.items()
    ]

    # Normalise <@!userid> -> <@userid> so hub regex matches consistently
    content = re.sub(r'<@!(\d+)>', r'<@\1>', content)

    # <@&roleid> -> @RoleName (roles don't cross platforms)
    content = re.sub(
        r'<@&(\d+)>',
        lambda m: f'@{role_map.get(m.group(1), "role")}',
        content,
    )
    # <:name:id> and <a:name:id> (custom / animated emoji) -> :name:
    content = _CUSTOM_EMOJI_RE.sub(r':\1:', content)

    return content.strip(), mentions


def _format_reply_quote(content: str, max_len: int = 120) -> str:
    """Strip relay prefix formatting and truncate quoted reply content."""
    text = (content or '').strip()
    for marker in ('**[D] ', '**[F] ', '**[M] '):
        if text.startswith(marker):
            idx = text.find(':** ')
            if idx != -1:
                text = text[idx + 4:]
            break
    if len(text) > max_len:
        text = text[:max_len] + '...'
    return text


class BridgeCog(commands.Cog):
    """Bidirectional Discord <-> Fluxer message bridge."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._session: aiohttp.ClientSession | None = None
        self._poll_task: asyncio.Task | None = None

    @commands.Cog.listener()
    async def on_ready(self):
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession()
        if self._poll_task is None or self._poll_task.done():
            self._poll_task = asyncio.create_task(self._poll_loop())
        logger.info("BridgeCog (Discord): relay polling started")

    def cog_unload(self):
        if self._poll_task:
            self._poll_task.cancel()
        if self._session and not self._session.closed:
            asyncio.create_task(self._session.close())

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Relay Discord messages to Fluxer via hub queue."""
        if message.author.bot:
            return
        if message.content and message.content.startswith(_RELAY_PREFIXES):
            return
        # Ignore bot commands - keep them native to this platform
        if message.content and message.content.lstrip().startswith(('!', '/')):
            return

        # Resolve mentions and custom emoji - keeps raw <@id> tokens for hub cross-platform resolution
        content, mentions = _resolve_discord_content(message)

        # Convert @everyone/@here to @room for Matrix
        content = content.replace('@everyone', '@room').replace('@here', '@room')

        # If no text, check for stickers and relay them as image URLs
        if not content and message.stickers:
            content = '  '.join(f'[Sticker: {s.name}] {s.url}' for s in message.stickers)

        # Extract attachments (images, GIFs, videos, files)
        attachments = []
        for att in (message.attachments or []):
            url = str(att.url or '').strip()
            if url.startswith('https://'):
                attachments.append({
                    'url': url,
                    'filename': str(att.filename or ''),
                    'content_type': str(att.content_type or '') if att.content_type else '',
                })

        # Extract image/GIF URLs from embeds (Tenor, Giphy, direct image embeds).
        # Skip 'rich' and 'link' embed types - these are auto-generated Discord link previews
        # from URLs already present in message.content; extracting their thumbnails causes
        # the link to be double-posted (once as content, once as the preview thumbnail).
        for emb in (message.embeds or []):
            img_url = None
            if emb.type == 'gifv':
                # Tenor/Giphy gifv embed - generated from a URL the user pasted.
                # If that source URL is already in message content, skip the embed entirely:
                # the receiving platform will render the link itself. Only extract the
                # CDN thumbnail when the GIF has no text representation in the message.
                emb_source_url = str(emb.url or '')
                if emb_source_url and emb_source_url in (content or ''):
                    continue
                if emb.video and emb.video.url:
                    img_url = str(emb.thumbnail.proxy_url or emb.thumbnail.url) if emb.thumbnail else str(emb.video.url)
            elif emb.type == 'image' and emb.url:
                img_url = str(emb.url)
            elif emb.image and emb.image.url and emb.type not in ('rich', 'link'):
                img_url = str(emb.image.url)
            # Deliberately skip emb.type in ('rich', 'link') thumbnails - those are link previews,
            # not user-uploaded images. The URL is already in message.content.
            if img_url and img_url.startswith('https://'):
                lower = img_url.split('?')[0].lower()
                if lower.endswith('.gif') or 'tenor.com' in img_url or 'giphy.com' in img_url:
                    fname, ctype = 'image.gif', 'image/gif'
                elif lower.endswith('.png'):
                    fname, ctype = 'image.png', 'image/png'
                elif lower.endswith(('.jpg', '.jpeg')):
                    fname, ctype = 'image.jpg', 'image/jpeg'
                elif lower.endswith('.webp'):
                    fname, ctype = 'image.webp', 'image/webp'
                else:
                    fname, ctype = 'image.gif', 'image/gif'
                attachments.append({'url': img_url, 'filename': fname, 'content_type': ctype})

        # Reply vs forward detection.
        # Discord "forward" (message_snapshot) = message.flags.is_forwarded or message.message_snapshots
        # Older forward pattern = message.reference pointing to a DIFFERENT channel.
        reply_quote = None
        reply_to_message_id = None
        is_forward = False
        forward_from_author = None
        forward_content = None

        # Check for Discord's native forward (message_snapshots, available since 2024)
        # IMPORTANT: this check must happen before the empty-content bail below,
        # because native forwards have empty message.content.
        if hasattr(message, 'message_snapshots') and message.message_snapshots:
            is_forward = True
            snap = message.message_snapshots[0]
            snap_msg = getattr(snap, 'message', snap)
            forward_content = (getattr(snap_msg, 'content', '') or '').strip()
            # Forwarded messages don't always carry original author - use generic label
            forward_from_author = 'forwarded message'
        elif message.reference and message.reference.message_id:
            ref_channel_id = message.reference.channel_id
            if ref_channel_id and ref_channel_id != message.channel.id:
                # Reference to a different channel = forward
                is_forward = True
                ref_msg = message.reference.resolved
                if ref_msg is None:
                    try:
                        channel = self.bot.get_channel(ref_channel_id) or await self.bot.fetch_channel(ref_channel_id)
                        ref_msg = await channel.fetch_message(message.reference.message_id)
                    except Exception:
                        pass
                if ref_msg:
                    forward_content = (ref_msg.content or '').strip()
                    forward_from_author = ref_msg.author.display_name if ref_msg.author else 'Unknown'
            else:
                # Same channel = reply
                ref_msg = message.reference.resolved
                if ref_msg is None:
                    try:
                        ref_msg = await message.channel.fetch_message(message.reference.message_id)
                    except Exception:
                        pass
                if ref_msg:
                    if ref_msg.content:
                        reply_quote = _format_reply_quote(ref_msg.content)
                    reply_to_message_id = str(message.reference.message_id)

        # If this is a forward, prepend the forwarded content
        if is_forward and forward_content:
            content = f"[forwarded from {forward_from_author}]\n> {forward_content}" + (f"\n{content}" if content else '')

        # Drop if nothing to relay (check after forward detection - native forwards have empty content)
        if not content and not attachments:
            return

        # Thread detection: if message is in a thread, use the thread's parent channel for bridge lookup
        # and pass the thread_id so the hub can map it to the Matrix thread
        thread_id = None
        bridge_channel_id = str(message.channel.id)
        if isinstance(message.channel, discord.Thread):
            thread_id = str(message.channel.id)
            bridge_channel_id = str(message.channel.parent_id)

        avatar_url = str(message.author.display_avatar.url) if message.author.display_avatar else None
        payload = {
            'source_platform': 'discord',
            'discord_channel_id': bridge_channel_id,
            'source_message_id': str(message.id),
            'author_name': message.author.display_name or str(message.author),
            'author_avatar': avatar_url,
            'content': content,
            'reply_quote': reply_quote,
            'reply_to_message_id': reply_to_message_id,
            'attachments': attachments,
            'mentions': mentions,
            'thread_id': thread_id,
        }

        try:
            if self._session and not self._session.closed:
                async with self._session.post(
                    _RELAY_URL, json=payload, headers=_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get('queued', 0) == 0:
                            return
                    elif resp.status not in (200, 201):
                        logger.debug(f"BridgeCog (Discord): relay non-200: {resp.status}")
        except Exception as e:
            logger.debug(f"BridgeCog (Discord): relay POST error: {e}")

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent):
        """Relay message deletions to Fluxer via hub."""
        try:
            if self._session and not self._session.closed:
                async with self._session.post(
                    _DELETE_URL,
                    json={
                        'platform': 'discord',
                        'message_id': str(payload.message_id),
                        'channel_id': str(payload.channel_id),
                    },
                    headers=_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    pass  # best-effort
        except Exception as e:
            logger.debug(f"BridgeCog (Discord): delete relay error: {e}")

    @commands.Cog.listener()
    async def on_raw_message_edit(self, payload: discord.RawMessageUpdateEvent):
        """Relay Discord message edits to Fluxer via hub."""
        new_content = (payload.data.get('content') or '').strip()
        if not new_content:
            return
        author = payload.data.get('author') or {}
        if author.get('bot'):
            return
        if new_content.startswith(_RELAY_PREFIXES):
            return
        try:
            if self._session and not self._session.closed:
                async with self._session.post(
                    _EDIT_URL,
                    json={
                        'platform': 'discord',
                        'message_id': str(payload.message_id),
                        'channel_id': str(payload.channel_id),
                        'new_content': new_content,
                    },
                    headers=_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    pass
        except Exception as e:
            logger.debug(f"BridgeCog (Discord): edit relay error: {e}")

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        """Relay unicode emoji reactions to Fluxer via hub."""
        # Ignore bot reactions
        if payload.member and payload.member.bot:
            return
        # Only relay unicode emojis - skip custom emojis (have an integer id)
        if payload.emoji.id is not None:
            return
        emoji_str = str(payload.emoji)
        if not emoji_str:
            return

        try:
            if self._session and not self._session.closed:
                async with self._session.post(
                    _REACTION_URL,
                    json={
                        'platform': 'discord',
                        'message_id': str(payload.message_id),
                        'channel_id': str(payload.channel_id),
                        'emoji': emoji_str,
                    },
                    headers=_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    pass  # best-effort
        except Exception as e:
            logger.debug(f"BridgeCog (Discord): reaction relay error: {e}")

    @commands.Cog.listener()
    async def on_typing(self, channel, user, when):
        """Relay Discord typing indicators to Fluxer via Fluxer API."""
        if user.bot:
            return
        if not self._session or self._session.closed:
            return
        try:
            async with self._session.post(
                _TYPING_URL,
                json={'platform': 'discord', 'channel_id': str(channel.id)},
                headers=_HEADERS,
                timeout=aiohttp.ClientTimeout(total=3)
            ) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
        except Exception:
            return

        for target in data.get('targets', []):
            if target.get('platform') != 'fluxer':
                continue
            fluxer_channel_id = str(target.get('channel_id', ''))
            if not fluxer_channel_id or not FLUXER_BOT_TOKEN:
                continue
            try:
                async with self._session.post(
                    f'{FLUXER_API_URL.rstrip("/")}/channels/{fluxer_channel_id}/typing',
                    headers={'Authorization': f'Bot {FLUXER_BOT_TOKEN}', 'Content-Type': 'application/json'},
                    timeout=aiohttp.ClientTimeout(total=3)
                ) as _:
                    pass  # best-effort, 204 No Content
            except Exception as e:
                logger.debug(f"BridgeCog (Discord): Fluxer typing POST error: {e}")

    async def _poll_loop(self):
        """Poll hub every 3s for messages; every 6s for reactions/edits/deletions."""
        await asyncio.sleep(8)  # Wait for bot ready and session init
        tick = 0
        while True:
            try:
                await self._deliver_pending()
            except Exception as e:
                logger.warning(f"BridgeCog (Discord): poll loop error: {e}")

            if tick % 2 == 0:
                try:
                    await self._deliver_pending_reactions()
                except Exception as e:
                    logger.warning(f"BridgeCog (Discord): reaction poll error: {e}")
                try:
                    await self._deliver_pending_deletions()
                except Exception as e:
                    logger.warning(f"BridgeCog (Discord): deletion poll error: {e}")
                try:
                    await self._deliver_pending_edits()
                except Exception as e:
                    logger.warning(f"BridgeCog (Discord): edit poll error: {e}")

            tick += 1
            await asyncio.sleep(3)

    async def _deliver_pending(self):
        """Fetch pending Fluxer->Discord messages and post to Discord channels."""
        if not self._session or self._session.closed:
            return
        try:
            async with self._session.get(
                _PENDING_URL, headers=_HEADERS,
                timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"BridgeCog (Discord): pending fetch returned {resp.status} from {_PENDING_URL}")
                    return
                data = await resp.json()
        except Exception as e:
            logger.warning(f"BridgeCog (Discord): pending fetch error: {e}")
            return

        messages = data.get('messages', [])
        for msg in messages:
            channel_id_str = str(msg.get('target_channel_id', ''))
            content = msg.get('content', '')
            author = msg.get('author_name', 'Unknown')
            reply_quote = msg.get('reply_quote', '')
            reply_to_event_id = msg.get('reply_to_event_id')        # Discord message ID to reply to
            thread_root_event_id = msg.get('thread_root_event_id')  # Discord message ID of thread root
            attachments = msg.get('attachments', []) or []
            relay_id = msg.get('id')
            source = msg.get('source_platform', 'fluxer')
            source_thread_id = msg.get('source_thread_id')    # Matrix root event ID if in a thread
            target_thread_id = msg.get('target_thread_id')    # Discord thread channel ID if already mapped
            bridge_id = msg.get('bridge_id')
            _TAG_MAP = {'discord': 'D', 'fluxer': 'F', 'matrix': 'M'}
            tag = _TAG_MAP.get(source, 'F')

            if not channel_id_str or (not content and not attachments):
                continue

            # Only include the blockquote if we can't do a native reply
            rq = reply_quote if (reply_quote and not reply_to_event_id) else ''
            formatted = _format_bridged(tag, author, content, rq)

            # Download Matrix media and re-upload as Discord files so GIFs render inline
            discord_files = []
            plain_urls = []
            for att in attachments:
                # Prefer discord_url (direct Matrix URL) over the public proxy url
                url = att.get('discord_url') or att.get('url', '')
                filename = att.get('filename') or 'file'
                content_type = att.get('content_type', '')
                if not url:
                    continue
                is_image = (
                    content_type.startswith('image/') or
                    filename.lower().endswith(('.gif', '.png', '.jpg', '.jpeg', '.webp'))
                )
                if is_image:
                    try:
                        async with self._session.get(
                            url,
                            timeout=aiohttp.ClientTimeout(total=15)
                        ) as resp:
                            if resp.status == 200:
                                data_bytes = await resp.read()
                                # Discord limit: 25MB for non-boosted servers
                                if len(data_bytes) > 25 * 1024 * 1024:
                                    logger.warning(f"BridgeCog: skipping {filename} ({len(data_bytes)//1024}KB) - exceeds Discord 25MB limit")
                                    plain_urls.append(att.get('url', url))
                                else:
                                    import io
                                    discord_files.append(discord.File(io.BytesIO(data_bytes), filename=filename))
                            else:
                                plain_urls.append(att.get('url', url))
                    except Exception as e:
                        logger.warning(f"BridgeCog: media download failed: {e}")
                        plain_urls.append(att.get('url', url))
                else:
                    plain_urls.append(att.get('url', url))

            if plain_urls:
                formatted = (formatted + '\n' + '\n'.join(plain_urls)).strip()

            try:
                channel = self.bot.get_channel(int(channel_id_str))
                if channel is None:
                    channel = await self.bot.fetch_channel(int(channel_id_str))

                # Thread handling: route into existing thread or create new one
                send_channel = channel
                if source_thread_id:
                    if target_thread_id:
                        # Already mapped - send into existing Discord thread
                        try:
                            send_channel = await self.bot.fetch_channel(int(target_thread_id))
                        except Exception:
                            send_channel = channel  # Thread gone, fall back to main channel
                    else:
                        # First message in this thread - create a Discord thread on the root message
                        # thread_root_event_id is the Discord message ID of the Matrix thread root
                        thread_parent_msg = None
                        root_id = thread_root_event_id or reply_to_event_id
                        if root_id:
                            try:
                                thread_parent_msg = await channel.fetch_message(int(root_id))
                            except Exception:
                                pass
                        if thread_parent_msg:
                            try:
                                thread_name = f"[{tag}] {author}"[:100]
                                send_channel = await thread_parent_msg.create_thread(name=thread_name)
                                # Register the mapping (include parent message ID for reply routing)
                                if bridge_id:
                                    await self._store_thread_map(bridge_id, str(send_channel.id), source_thread_id, str(thread_parent_msg.id))
                            except Exception as e:
                                logger.warning(f"BridgeCog: create thread failed: {e}")
                                send_channel = channel

                # Use Discord native reply if we have the target message ID
                reference = None
                if reply_to_event_id and send_channel == channel:
                    try:
                        ref_msg = await channel.fetch_message(int(reply_to_event_id))
                        reference = ref_msg.to_reference(fail_if_not_exists=False)
                    except Exception:
                        pass  # Reply target gone - send without reference

                sent = await send_channel.send(formatted, reference=reference, files=discord_files or None)
                # Record the sent message ID for reaction mapping
                if relay_id and sent:
                    await self._store_message_map(relay_id, str(sent.id), str(send_channel.id))
            except discord.Forbidden:
                logger.warning(f"BridgeCog (Discord): no permission to send to channel {channel_id_str}")
            except Exception as e:
                logger.warning(f"BridgeCog (Discord): send failed to {channel_id_str}: {e}")

    async def _store_thread_map(self, bridge_id, discord_thread_id: str, matrix_thread_event_id: str, discord_parent_message_id: str = None):
        """Best-effort: record Discord thread <-> Matrix thread root event mapping."""
        try:
            if self._session and not self._session.closed:
                async with self._session.post(
                    _THREAD_MAP_URL,
                    json={
                        'bridge_id': bridge_id,
                        'discord_thread_id': discord_thread_id,
                        'discord_parent_message_id': discord_parent_message_id,
                        'matrix_thread_event_id': matrix_thread_event_id,
                    },
                    headers=_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    pass
        except Exception as e:
            logger.debug(f"BridgeCog: thread-map store error: {e}")

    async def _store_message_map(self, relay_id: int, message_id: str, channel_id: str):
        """Best-effort: record sent message ID so reactions can be mapped cross-platform."""
        try:
            if self._session and not self._session.closed:
                async with self._session.post(
                    _MSG_MAP_URL,
                    json={
                        'relay_queue_id': relay_id,
                        'platform': 'discord',
                        'message_id': message_id,
                        'channel_id': channel_id,
                    },
                    headers=_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    pass
        except Exception as e:
            logger.debug(f"BridgeCog (Discord): message-map store error: {e}")

    async def _deliver_pending_reactions(self):
        """Fetch pending reactions from hub and add to Discord messages."""
        if not self._session or self._session.closed:
            return
        try:
            async with self._session.get(
                _PENDING_REACTIONS_URL, headers=_HEADERS,
                timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
        except Exception as e:
            logger.debug(f"BridgeCog (Discord): pending reactions fetch error: {e}")
            return

        for item in data.get('reactions', []):
            message_id = str(item.get('target_message_id', ''))
            channel_id = str(item.get('target_channel_id', ''))
            emoji = item.get('emoji', '')
            if not message_id or not channel_id or not emoji:
                continue
            try:
                channel = self.bot.get_channel(int(channel_id))
                if channel is None:
                    channel = await self.bot.fetch_channel(int(channel_id))
                message = await channel.fetch_message(int(message_id))
                await message.add_reaction(emoji)
            except discord.Forbidden:
                logger.warning(f"BridgeCog (Discord): no permission to react in {channel_id}")
            except Exception as e:
                logger.debug(f"BridgeCog (Discord): add reaction to {message_id} failed: {e}")


    async def _deliver_pending_deletions(self):
        """Fetch pending deletions from hub and delete messages from Discord channels."""
        if not self._session or self._session.closed:
            return
        try:
            async with self._session.get(
                _PENDING_DELETIONS_URL, headers=_HEADERS,
                timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
        except Exception as e:
            logger.debug(f"BridgeCog (Discord): pending deletions fetch error: {e}")
            return

        for item in data.get('deletions', []):
            message_id = str(item.get('target_message_id', ''))
            channel_id = str(item.get('target_channel_id', ''))
            if not message_id or not channel_id:
                continue
            try:
                channel = self.bot.get_channel(int(channel_id))
                if channel is None:
                    channel = await self.bot.fetch_channel(int(channel_id))
                message = await channel.fetch_message(int(message_id))
                await message.delete()
            except discord.NotFound:
                pass  # already deleted, ignore
            except discord.Forbidden:
                logger.warning(f"BridgeCog (Discord): no permission to delete in {channel_id}")
            except Exception as e:
                logger.debug(f"BridgeCog (Discord): delete message {message_id} failed: {e}")


    async def _deliver_pending_edits(self):
        """Fetch pending edits from hub and apply them to Discord messages."""
        if not self._session or self._session.closed:
            return
        try:
            async with self._session.get(
                _PENDING_EDITS_URL, headers=_HEADERS,
                timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
        except Exception as e:
            logger.debug(f"BridgeCog (Discord): pending edits fetch error: {e}")
            return

        for item in data.get('edits', []):
            message_id = str(item.get('target_message_id', ''))
            channel_id = str(item.get('target_channel_id', ''))
            new_content = item.get('new_content', '')
            if not message_id or not channel_id or not new_content:
                continue
            try:
                channel = self.bot.get_channel(int(channel_id))
                if channel is None:
                    channel = await self.bot.fetch_channel(int(channel_id))
                message = await channel.fetch_message(int(message_id))
                await message.edit(content=new_content)
            except discord.NotFound:
                pass  # message already deleted, ignore
            except discord.Forbidden:
                logger.warning(f"BridgeCog (Discord): no permission to edit in {channel_id}")
            except Exception as e:
                logger.debug(f"BridgeCog (Discord): edit message {message_id} failed: {e}")


def setup(bot):
    bot.add_cog(BridgeCog(bot))
