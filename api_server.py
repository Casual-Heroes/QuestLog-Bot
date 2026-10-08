"""
Simple API server for bot control endpoints.
Allows the web app to trigger actions like forcing a guild sync.
"""
from aiohttp import web
import json
import secrets
import logging
import os
import discord

from utils.signed_request_auth import (
    AuthenticationError,
    ReplayCache,
    load_trusted_signers,
    validate_required_scopes,
    verify_signed_request,
)

logger = logging.getLogger("api_server")

# Store bot reference (set by bot.py on startup)
bot_instance = None

# Signed authentication is rolled out in dual mode so the website and bot can
# be deployed independently. Set WARDEN_API_AUTH_MODE=signed after the website
# is signing every request, then remove the legacy token from this service.
API_AUTH_MODE = os.getenv("WARDEN_API_AUTH_MODE", "dual").strip().lower()
if API_AUTH_MODE not in {"legacy", "dual", "signed"}:
    raise RuntimeError("WARDEN_API_AUTH_MODE must be legacy, dual, or signed")

API_TOKEN = os.getenv("DISCORD_BOT_API_TOKEN")
if API_TOKEN and len(API_TOKEN) < 32:
    logger.critical("DISCORD_BOT_API_TOKEN must be at least 32 characters.")
    raise RuntimeError("DISCORD_BOT_API_TOKEN must be at least 32 characters.")

LOCAL_SYNC_TOKEN = os.getenv("WARDEN_API_LOCAL_SYNC_TOKEN")
if LOCAL_SYNC_TOKEN and len(LOCAL_SYNC_TOKEN) < 32:
    raise RuntimeError("WARDEN_API_LOCAL_SYNC_TOKEN must be at least 32 characters")

TRUSTED_SIGNERS = load_trusted_signers(os.getenv("WARDEN_API_TRUSTED_SIGNERS"))
REPLAY_CACHE = ReplayCache()
REQUIRED_SIGNED_SCOPES = {
    "guilds.read",
    "guilds.sync",
    "moderation.write",
    "creator.write",
    "creator.network.write",
}
if API_AUTH_MODE == "legacy" and not API_TOKEN:
    raise RuntimeError("DISCORD_BOT_API_TOKEN is required in legacy authentication mode")
if API_AUTH_MODE == "dual" and not API_TOKEN and not TRUSTED_SIGNERS:
    raise RuntimeError(
        "Dual authentication mode requires DISCORD_BOT_API_TOKEN or WARDEN_API_TRUSTED_SIGNERS"
    )
if API_AUTH_MODE == "signed":
    validate_required_scopes(TRUSTED_SIGNERS, REQUIRED_SIGNED_SCOPES)
    if not LOCAL_SYNC_TOKEN:
        raise RuntimeError(
            "WARDEN_API_LOCAL_SYNC_TOKEN is required in signed mode for internal sync jobs"
        )


ROUTE_AUTH_POLICIES = {
    "guilds-list": ("guilds.read", False),
    "guild-sync-all": ("guilds.sync", True),
    "guild-sync-one": ("guilds.sync", True),
    "mod-untimeout": ("moderation.write", True),
    "mod-kick": ("moderation.write", True),
    "mod-ban": ("moderation.write", True),
    "mod-unban": ("moderation.write", True),
    "mod-unmute": ("moderation.write", True),
    "mod-unjail": ("moderation.write", True),
    "creator-announce-cotw": ("creator.write", True),
    "creator-announce-cotm": ("creator.write", True),
    "creator-delete-message": ("creator.write", True),
    "creator-network-cotw": ("creator.network.write", True),
    "creator-network-cotm": ("creator.network.write", True),
}


def _legacy_token_is_valid(request):
    auth_header = request.headers.get('Authorization', '')
    scheme, separator, token = auth_header.partition(' ')
    return bool(
        API_TOKEN
        and separator == ' '
        and scheme.lower() == 'bearer'
        and token
        and ' ' not in token
        and secrets.compare_digest(token, API_TOKEN)
    )


def _local_sync_token_is_valid(request, policy):
    if not LOCAL_SYNC_TOKEN or not policy or policy[0] != "guilds.sync":
        return False
    if request.remote not in {"127.0.0.1", "::1"}:
        return False
    auth_header = request.headers.get('Authorization', '')
    scheme, separator, token = auth_header.partition(' ')
    return bool(
        separator == ' '
        and scheme.lower() == 'bearer'
        and token
        and ' ' not in token
        and secrets.compare_digest(token, LOCAL_SYNC_TOKEN)
    )


def _signed_actor_is_authorized(request, scope, actor_id, body):
    """Apply defense-in-depth actor checks after signature verification."""
    if scope == "moderation.write":
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return False, "Signed moderation request must contain valid JSON"
        if str(payload.get("requester_id", "")) != actor_id:
            return False, "Signed actor does not match requester_id"

    if scope in {"guilds.sync", "creator.write"}:
        guild_id = request.match_info.get("guild_id")
        if guild_id is None:
            try:
                payload = json.loads(body)
                guild_id = payload.get("guild_id")
            except (UnicodeDecodeError, json.JSONDecodeError):
                return False, "Signed request must contain a guild ID"
        try:
            guild_id = int(guild_id)
            actor_snowflake = int(actor_id)
        except (TypeError, ValueError):
            return False, "Signed request has an invalid actor or guild ID"

        guild = bot_instance.get_guild(guild_id) if bot_instance else None
        actor = guild.get_member(actor_snowflake) if guild else None
        if not actor:
            return False, "Signed actor is not a current member of the guild"
        if scope == "creator.write" and not actor.guild_permissions.administrator:
            return False, "Signed actor is not a current guild administrator"

    return True, ""


@web.middleware
async def auth_middleware(request, handler):
    """Authenticate signed requests, with an explicit migration fallback."""
    # Skip auth for health check
    if request.path == '/health':
        return await handler(request)

    route_name = request.match_info.route.name
    policy = ROUTE_AUTH_POLICIES.get(route_name)
    signed_attempt = bool(
        request.headers.get("X-Warden-Auth-Version")
        or request.headers.get("X-Warden-Signature")
    )

    if API_AUTH_MODE != "legacy" and signed_attempt and policy:
        scope, actor_required = policy
        body = await request.read()
        try:
            context = verify_signed_request(
                headers=request.headers,
                method=request.method,
                path=request.path,
                body=body,
                expected_scope=scope,
                actor_required=actor_required,
                signers=TRUSTED_SIGNERS,
                replay_cache=REPLAY_CACHE,
            )
            authorized, reason = _signed_actor_is_authorized(
                request, context.scope, context.actor_id, body
            )
            if not authorized:
                logger.warning("Signed API authorization rejected: %s", reason)
                return web.json_response({'error': 'Forbidden'}, status=403)
            request["auth_method"] = "signed"
            request["auth_actor_id"] = context.actor_id
            request["auth_key_id"] = context.key_id
            return await handler(request)
        except AuthenticationError as exc:
            logger.warning("Signed API authentication failed from %s: %s", request.remote, exc)
            return web.json_response({'error': 'Unauthorized'}, status=401)

    if _local_sync_token_is_valid(request, policy):
        request["auth_method"] = "local-sync"
        request["auth_actor_id"] = None
        return await handler(request)

    if API_AUTH_MODE != "signed" and _legacy_token_is_valid(request):
        request["auth_method"] = "legacy"
        request["auth_actor_id"] = None
        logger.warning("Legacy bearer authentication used for %s", request.path)
        return await handler(request)

    logger.warning("Unauthorized API request from %s", request.remote)
    return web.json_response(
        {'error': 'Unauthorized'},
        status=401,
        headers={'WWW-Authenticate': 'WardenSignature realm="warden-api"'},
    )


async def force_guild_sync(request):
    """Force an immediate sync of guild stats from Discord."""
    try:
        from db import get_db_session
        from models import Guild
        import json as json_lib

        guild_id = request.match_info.get('guild_id')

        if not bot_instance:
            return web.json_response({'error': 'Bot not ready'}, status=503)

        # If guild_id provided, sync just that guild
        if guild_id:
            try:
                guild_id = int(guild_id)
                if guild_id <= 0 or guild_id > 9223372036854775807:
                    return web.json_response({'error': 'Invalid guild ID'}, status=400)
                guild = bot_instance.get_guild(guild_id)
                if not guild:
                    return web.json_response({'error': 'Guild not found'}, status=404)

                # Sync guild data directly to database
                with get_db_session() as session:
                    db_guild = session.query(Guild).filter_by(guild_id=guild_id).first()
                    if not db_guild:
                        db_guild = Guild(guild_id=guild_id, name=guild.name)
                        session.add(db_guild)

                    # Count members excluding bots (same as guild_sync_cog)
                    member_count = sum(1 for m in guild.members if not m.bot)
                    online_count = sum(
                        1 for m in guild.members
                        if not m.bot and m.status != discord.Status.offline
                    )

                    # Update guild info
                    db_guild.name = guild.name
                    db_guild.icon_url = guild.icon.url if guild.icon else None
                    db_guild.member_count = member_count  # Exclude bots
                    db_guild.online_count = online_count  # Exclude bots
                    db_guild.guild_icon_hash = guild.icon.key if guild.icon else None  # For CDN URLs

                    # Cache channels
                    channels_data = []
                    for channel in guild.channels:
                        channel_data = {
                            'id': str(channel.id),
                            'name': channel.name,
                            'type': channel.type.value,  # Use numeric type ID (0=text, 2=voice, 15=forum, etc.)
                            'category_name': channel.category.name if hasattr(channel, 'category') and channel.category else None
                        }
                        channels_data.append(channel_data)
                    db_guild.cached_channels = json_lib.dumps(channels_data)

                    # Cache roles
                    roles_data = []
                    for role in guild.roles:
                        if role.name != '@everyone':
                            role_data = {
                                'id': str(role.id),
                                'name': role.name,
                                'color': role.color.value,
                                'position': role.position
                            }
                            roles_data.append(role_data)
                    db_guild.cached_roles = json_lib.dumps(roles_data)

                    # Cache emojis
                    emojis_data = []
                    for emoji in guild.emojis:
                        emoji_data = {
                            'id': str(emoji.id),
                            'name': emoji.name,
                            'animated': emoji.animated
                        }
                        emojis_data.append(emoji_data)
                    db_guild.cached_emojis = json_lib.dumps(emojis_data)

                    # Cache members (industry standard: cache from Gateway)
                    members_data = []
                    for member in guild.members:
                        if not member.bot:  # Exclude bots from cache
                            member_data = {
                                'id': str(member.id),
                                'username': member.name,
                                'discriminator': member.discriminator,
                                'display_name': member.display_name,
                                'avatar': member.avatar.url if member.avatar else None,
                                'roles': [str(role.id) for role in member.roles if role.name != "@everyone"],
                                'joined_at': member.joined_at.isoformat() if member.joined_at else None
                            }
                            members_data.append(member_data)
                    db_guild.cached_members = json_lib.dumps(members_data)

                    session.commit()

                # Invalidate Django cache so changes appear immediately
                try:
                    import aiohttp
                    django_url = os.getenv('DJANGO_URL', 'https://casual-heroes.com')
                    cache_url = f"{django_url}/questlog/api/guild/{guild_id}/invalidate-cache/"
                    async with aiohttp.ClientSession() as cache_session:
                        await cache_session.post(cache_url, timeout=aiohttp.ClientTimeout(total=2))
                except Exception as cache_error:
                    logger.warning(f"Failed to invalidate Django cache for guild {guild_id}: {cache_error}")

                logger.info(f"✅ Forced sync for guild {guild_id} - {len(channels_data)} channels, {len(roles_data)} roles, {len(members_data)} members")
                return web.json_response({
                    'success': True,
                    'guild_id': guild_id,
                    'channels': len(channels_data),
                    'roles': len(roles_data)
                })
            except ValueError:
                return web.json_response({'error': 'Invalid guild ID'}, status=400)
        else:
            return web.json_response({'error': 'Guild ID required'}, status=400)

    except Exception as e:
        logger.error(f"Error in force_guild_sync: {e}", exc_info=True)
        return web.json_response({'error': 'An internal error occurred. Please try again later.'}, status=500)


async def health_check(request):
    """Simple health check endpoint."""
    return web.json_response({'status': 'ok', 'bot_ready': bot_instance is not None})


async def get_guild_ids(request):
    """Return list of guild IDs where the bot is currently installed."""
    if not bot_instance:
        return web.json_response({'error': 'Bot not ready'}, status=503)

    try:
        # Get all guild IDs the bot is currently in
        guild_ids = [str(guild.id) for guild in bot_instance.guilds]
        return web.json_response({'guild_ids': guild_ids})
    except Exception as e:
        logger.error(f"Error getting guild IDs: {e}", exc_info=True)
        return web.json_response({'error': 'An internal error occurred. Please try again later.'}, status=500)


async def mod_untimeout(request):
    """Remove timeout from a user."""
    try:
        data = await request.json()

        # SECURITY: Validate all required fields before type conversion
        guild_id_raw = data.get('guild_id')
        user_id_raw = data.get('user_id')
        requester_id = data.get('requester_id')
        reason = data.get('reason', 'Timeout removed via web dashboard')

        if not guild_id_raw or not user_id_raw or not requester_id:
            return web.json_response({'error': 'guild_id, user_id, and requester_id are required'}, status=400)

        # Convert to integers with bounds checking (Discord snowflakes are max 19 digits)
        _MAX_SNOWFLAKE = 9223372036854775807
        try:
            guild_id = int(guild_id_raw)
            user_id = int(user_id_raw)
            requester_id = int(requester_id)
            if not all(0 < x <= _MAX_SNOWFLAKE for x in (guild_id, user_id, requester_id)):
                return web.json_response({'error': 'Invalid ID value'}, status=400)
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid ID format in mod_untimeout: {e}")
            return web.json_response({'error': 'Invalid ID format - must be integers'}, status=400)

        if not bot_instance:
            return web.json_response({'error': 'Bot not ready'}, status=503)

        guild = bot_instance.get_guild(guild_id)
        if not guild:
            return web.json_response({'error': 'Guild not found'}, status=404)

        # SECURITY: Verify requester has admin permissions in this guild
        requester = guild.get_member(requester_id)
        if not requester or not requester.guild_permissions.administrator:
            logger.warning(f"User {requester_id} attempted to untimeout in guild {guild_id} without admin permissions")
            return web.json_response({'error': 'No admin permission in this guild'}, status=403)

        # SECURITY: Check role hierarchy - cannot moderate users with equal or higher roles
        member = guild.get_member(user_id)
        if not member:
            return web.json_response({'error': 'Member not found'}, status=404)

        if member.top_role >= requester.top_role:
            logger.warning(f"User {requester_id} attempted to untimeout {user_id} who has equal or higher role")
            return web.json_response({'error': 'Cannot moderate users with equal or higher roles'}, status=403)

        # SECURITY: Never allow moderating the server owner
        if member.id == guild.owner_id:
            logger.warning(f"User {requester_id} attempted to untimeout the server owner {user_id}")
            return web.json_response({'error': 'Cannot moderate the server owner'}, status=403)

        if not member.timed_out:
            return web.json_response({'error': 'User is not timed out'}, status=400)

        # Remove timeout
        await member.remove_timeout(reason=reason)

        logger.info(f"Removed timeout from {member} in {guild.name} via API (requester: {requester})")
        return web.json_response({'success': True, 'message': f'Timeout removed from {member}'})

    except ValueError as e:
        logger.error(f"Invalid input in mod_untimeout: {e}")
        return web.json_response({'error': 'Invalid guild_id, user_id, or requester_id'}, status=400)
    except Exception as e:
        logger.error(f"Error in mod_untimeout: {e}", exc_info=True)
        return web.json_response({'error': 'An internal error occurred. Please try again later.'}, status=500)


async def mod_kick(request):
    """Kick a user from the server."""
    try:
        data = await request.json()

        # SECURITY: Validate all required fields before type conversion
        guild_id_raw = data.get('guild_id')
        user_id_raw = data.get('user_id')
        requester_id = data.get('requester_id')
        reason = data.get('reason', 'Kicked via web dashboard')

        if not guild_id_raw or not user_id_raw or not requester_id:
            return web.json_response({'error': 'guild_id, user_id, and requester_id are required'}, status=400)

        # Convert to integers with error handling
        try:
            guild_id = int(guild_id_raw)
            user_id = int(user_id_raw)
            requester_id = int(requester_id)
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid ID format in mod_kick: {e}")
            return web.json_response({'error': 'Invalid ID format - must be integers'}, status=400)

        if not bot_instance:
            return web.json_response({'error': 'Bot not ready'}, status=503)

        guild = bot_instance.get_guild(guild_id)
        if not guild:
            return web.json_response({'error': 'Guild not found'}, status=404)

        # SECURITY: Verify requester has admin permissions in this guild
        requester = guild.get_member(requester_id)
        if not requester or not requester.guild_permissions.administrator:
            logger.warning(f"User {requester_id} attempted to kick in guild {guild_id} without admin permissions")
            return web.json_response({'error': 'No admin permission in this guild'}, status=403)

        # SECURITY: Check role hierarchy - cannot moderate users with equal or higher roles
        member = guild.get_member(user_id)
        if not member:
            return web.json_response({'error': 'Member not found'}, status=404)

        if member.top_role >= requester.top_role:
            logger.warning(f"User {requester_id} attempted to kick {user_id} who has equal or higher role")
            return web.json_response({'error': 'Cannot moderate users with equal or higher roles'}, status=403)

        # SECURITY: Never allow moderating the server owner
        if member.id == guild.owner_id:
            logger.warning(f"User {requester_id} attempted to kick the server owner {user_id}")
            return web.json_response({'error': 'Cannot moderate the server owner'}, status=403)

        # Kick the user
        await member.kick(reason=reason)

        # Log the action via moderation cog
        mod_cog = bot_instance.get_cog('ModerationCog')
        if mod_cog:
            await mod_cog.log_mod_action(
                guild_id,
                guild.me,  # Bot is the moderator
                'kick',
                member,
                reason
            )

        logger.info(f"Kicked {member} from {guild.name} via API (requester: {requester})")
        return web.json_response({'success': True, 'message': f'Kicked {member}'})

    except ValueError as e:
        logger.error(f"Invalid input in mod_kick: {e}")
        return web.json_response({'error': 'Invalid guild_id, user_id, or requester_id'}, status=400)
    except Exception as e:
        logger.error(f"Error in mod_kick: {e}", exc_info=True)
        return web.json_response({'error': 'An internal error occurred. Please try again later.'}, status=500)


async def mod_ban(request):
    """Ban a user from the server."""
    try:
        data = await request.json()

        # SECURITY: Validate all required fields before type conversion
        guild_id_raw = data.get('guild_id')
        user_id_raw = data.get('user_id')
        requester_id = data.get('requester_id')
        reason = data.get('reason', 'Banned via web dashboard')

        if not guild_id_raw or not user_id_raw or not requester_id:
            return web.json_response({'error': 'guild_id, user_id, and requester_id are required'}, status=400)

        # Convert to integers with error handling
        try:
            guild_id = int(guild_id_raw)
            user_id = int(user_id_raw)
            requester_id = int(requester_id)
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid ID format in mod_ban: {e}")
            return web.json_response({'error': 'Invalid ID format - must be integers'}, status=400)

        if not bot_instance:
            return web.json_response({'error': 'Bot not ready'}, status=503)

        guild = bot_instance.get_guild(guild_id)
        if not guild:
            return web.json_response({'error': 'Guild not found'}, status=404)

        # SECURITY: Verify requester has admin permissions in this guild
        requester = guild.get_member(requester_id)
        if not requester or not requester.guild_permissions.administrator:
            logger.warning(f"User {requester_id} attempted to ban in guild {guild_id} without admin permissions")
            return web.json_response({'error': 'No admin permission in this guild'}, status=403)

        member = guild.get_member(user_id)
        if not member:
            # User might not be in the guild, try to ban by user object
            try:
                user = await bot_instance.fetch_user(user_id)
                await guild.ban(user, reason=reason)

                # Log the action via moderation cog
                mod_cog = bot_instance.get_cog('ModerationCog')
                if mod_cog:
                    await mod_cog.log_mod_action(
                        guild_id,
                        guild.me,  # Bot is the moderator
                        'ban',
                        user,
                        reason
                    )

                logger.info(f"Banned user {user_id} from {guild.name} via API (requester: {requester})")
                return web.json_response({'success': True, 'message': f'Banned user {user_id}'})
            except Exception as e:
                logger.error(f"Error banning user {user_id}: {e}", exc_info=True)
                return web.json_response({'error': 'User not found'}, status=404)

        # SECURITY: Check role hierarchy - cannot moderate users with equal or higher roles
        if member.top_role >= requester.top_role:
            logger.warning(f"User {requester_id} attempted to ban {user_id} who has equal or higher role")
            return web.json_response({'error': 'Cannot moderate users with equal or higher roles'}, status=403)

        # SECURITY: Never allow moderating the server owner
        if member.id == guild.owner_id:
            logger.warning(f"User {requester_id} attempted to ban the server owner {user_id}")
            return web.json_response({'error': 'Cannot moderate the server owner'}, status=403)

        # Ban the member
        await member.ban(reason=reason)

        # Log the action via moderation cog
        mod_cog = bot_instance.get_cog('ModerationCog')
        if mod_cog:
            await mod_cog.log_mod_action(
                guild_id,
                guild.me,  # Bot is the moderator
                'ban',
                member,
                reason
            )

        logger.info(f"Banned {member} from {guild.name} via API (requester: {requester})")
        return web.json_response({'success': True, 'message': f'Banned {member}'})

    except ValueError as e:
        logger.error(f"Invalid input in mod_ban: {e}")
        return web.json_response({'error': 'Invalid guild_id, user_id, or requester_id'}, status=400)
    except Exception as e:
        logger.error(f"Error in mod_ban: {e}", exc_info=True)
        return web.json_response({'error': 'An internal error occurred. Please try again later.'}, status=500)


async def mod_unban(request):
    """Unban a user from the server."""
    try:
        data = await request.json()

        # SECURITY: Validate all required fields before type conversion
        guild_id_raw = data.get('guild_id')
        user_id_raw = data.get('user_id')
        requester_id = data.get('requester_id')
        reason = data.get('reason', 'Unbanned via web dashboard')

        if not guild_id_raw or not user_id_raw or not requester_id:
            return web.json_response({'error': 'guild_id, user_id, and requester_id are required'}, status=400)

        # Convert to integers with error handling
        try:
            guild_id = int(guild_id_raw)
            user_id = int(user_id_raw)
            requester_id = int(requester_id)
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid ID format in mod_unban: {e}")
            return web.json_response({'error': 'Invalid ID format - must be integers'}, status=400)

        if not bot_instance:
            return web.json_response({'error': 'Bot not ready'}, status=503)

        guild = bot_instance.get_guild(guild_id)
        if not guild:
            return web.json_response({'error': 'Guild not found'}, status=404)

        # SECURITY: Verify requester has admin permissions in this guild
        requester = guild.get_member(requester_id)
        if not requester or not requester.guild_permissions.administrator:
            logger.warning(f"User {requester_id} attempted to unban in guild {guild_id} without admin permissions")
            return web.json_response({'error': 'No admin permission in this guild'}, status=403)

        # Get user object
        try:
            user = await bot_instance.fetch_user(user_id)
        except Exception as e:
            logger.error(f"Error fetching user {user_id}: {e}", exc_info=True)
            return web.json_response({'error': 'User not found'}, status=404)

        # Unban the user
        await guild.unban(user, reason=reason)

        # Log the action via moderation cog
        mod_cog = bot_instance.get_cog('ModerationCog')
        if mod_cog:
            await mod_cog.log_mod_action(
                guild_id,
                guild.me,  # Bot is the moderator
                'unban',
                user,
                reason
            )

        logger.info(f"Unbanned {user} from {guild.name} via API (requester: {requester})")
        return web.json_response({'success': True, 'message': f'Unbanned {user}'})

    except ValueError as e:
        logger.error(f"Invalid input in mod_unban: {e}")
        return web.json_response({'error': 'Invalid guild_id, user_id, or requester_id'}, status=400)
    except Exception as e:
        logger.error(f"Error in mod_unban: {e}", exc_info=True)
        return web.json_response({'error': 'An internal error occurred. Please try again later.'}, status=500)


async def mod_unmute(request):
    """Unmute a user."""
    try:
        data = await request.json()

        # SECURITY: Validate all required fields before type conversion
        guild_id_raw = data.get('guild_id')
        user_id_raw = data.get('user_id')
        requester_id = data.get('requester_id')
        reason = data.get('reason', 'Unmuted via web dashboard')

        if not guild_id_raw or not user_id_raw or not requester_id:
            return web.json_response({'error': 'guild_id, user_id, and requester_id are required'}, status=400)

        # Convert to integers with error handling
        try:
            guild_id = int(guild_id_raw)
            user_id = int(user_id_raw)
            requester_id = int(requester_id)
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid ID format in mod_unmute: {e}")
            return web.json_response({'error': 'Invalid ID format - must be integers'}, status=400)

        if not bot_instance:
            return web.json_response({'error': 'Bot not ready'}, status=503)

        guild = bot_instance.get_guild(guild_id)
        if not guild:
            return web.json_response({'error': 'Guild not found'}, status=404)

        # SECURITY: Verify requester has admin permissions in this guild
        requester = guild.get_member(requester_id)
        if not requester or not requester.guild_permissions.administrator:
            logger.warning(f"User {requester_id} attempted to unmute in guild {guild_id} without admin permissions")
            return web.json_response({'error': 'No admin permission in this guild'}, status=403)

        # SECURITY: Check role hierarchy - cannot moderate users with equal or higher roles
        member = guild.get_member(user_id)
        if not member:
            return web.json_response({'error': 'Member not found'}, status=404)

        if member.top_role >= requester.top_role:
            logger.warning(f"User {requester_id} attempted to unmute {user_id} who has equal or higher role")
            return web.json_response({'error': 'Cannot moderate users with equal or higher roles'}, status=403)

        # SECURITY: Never allow moderating the server owner
        if member.id == guild.owner_id:
            logger.warning(f"User {requester_id} attempted to unmute the server owner {user_id}")
            return web.json_response({'error': 'Cannot moderate the server owner'}, status=403)

        # Get moderation cog
        mod_cog = bot_instance.get_cog('ModerationCog')
        if not mod_cog:
            return web.json_response({'error': 'Moderation cog not loaded'}, status=500)

        # Unmute the user using the cog's method
        success = await mod_cog._unmute_user(guild, member, reason)

        if success:
            logger.info(f"Unmuted {member} in {guild.name} via API (requester: {requester})")
            return web.json_response({'success': True, 'message': f'Unmuted {member}'})
        else:
            return web.json_response({'error': 'Failed to unmute user'}, status=500)

    except ValueError as e:
        logger.error(f"Invalid input in mod_unmute: {e}")
        return web.json_response({'error': 'Invalid guild_id, user_id, or requester_id'}, status=400)
    except Exception as e:
        logger.error(f"Error in mod_unmute: {e}", exc_info=True)
        return web.json_response({'error': 'An internal error occurred. Please try again later.'}, status=500)


async def mod_unjail(request):
    """Unjail a user."""
    try:
        data = await request.json()

        # SECURITY: Validate all required fields before type conversion
        guild_id_raw = data.get('guild_id')
        user_id_raw = data.get('user_id')
        requester_id = data.get('requester_id')
        reason = data.get('reason', 'Unjailed via web dashboard')

        if not guild_id_raw or not user_id_raw or not requester_id:
            return web.json_response({'error': 'guild_id, user_id, and requester_id are required'}, status=400)

        # Convert to integers with error handling
        try:
            guild_id = int(guild_id_raw)
            user_id = int(user_id_raw)
            requester_id = int(requester_id)
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid ID format in mod_unjail: {e}")
            return web.json_response({'error': 'Invalid ID format - must be integers'}, status=400)

        if not bot_instance:
            return web.json_response({'error': 'Bot not ready'}, status=503)

        guild = bot_instance.get_guild(guild_id)
        if not guild:
            return web.json_response({'error': 'Guild not found'}, status=404)

        # SECURITY: Verify requester has admin permissions in this guild
        requester = guild.get_member(requester_id)
        if not requester or not requester.guild_permissions.administrator:
            logger.warning(f"User {requester_id} attempted to unjail in guild {guild_id} without admin permissions")
            return web.json_response({'error': 'No admin permission in this guild'}, status=403)

        # SECURITY: Check role hierarchy - cannot moderate users with equal or higher roles
        member = guild.get_member(user_id)
        if not member:
            return web.json_response({'error': 'Member not found'}, status=404)

        if member.top_role >= requester.top_role:
            logger.warning(f"User {requester_id} attempted to unjail {user_id} who has equal or higher role")
            return web.json_response({'error': 'Cannot moderate users with equal or higher roles'}, status=403)

        # SECURITY: Never allow moderating the server owner
        if member.id == guild.owner_id:
            logger.warning(f"User {requester_id} attempted to unjail the server owner {user_id}")
            return web.json_response({'error': 'Cannot moderate the server owner'}, status=403)

        # Get moderation cog
        mod_cog = bot_instance.get_cog('ModerationCog')
        if not mod_cog:
            return web.json_response({'error': 'Moderation cog not loaded'}, status=500)

        # Unjail the user using the cog's method
        success = await mod_cog._unjail_user(guild, member, reason)

        if success:
            logger.info(f"Unjailed {member} in {guild.name} via API (requester: {requester})")
            return web.json_response({'success': True, 'message': f'Unjailed {member}'})
        else:
            return web.json_response({'error': 'Failed to unjail user'}, status=500)

    except ValueError as e:
        logger.error(f"Invalid input in mod_unjail: {e}")
        return web.json_response({'error': 'Invalid guild_id, user_id, or requester_id'}, status=400)
    except Exception as e:
        logger.error(f"Error in mod_unjail: {e}", exc_info=True)
        return web.json_response({'error': 'An internal error occurred. Please try again later.'}, status=500)


async def announce_cotw(request):
    """Announce Creator of the Week - delete old message and post new announcement."""
    try:
        data = await request.json()

        guild_id = int(data.get('guild_id'))
        user_id = int(data.get('user_id'))
        channel_id = int(data.get('channel_id'))
        old_message_id = data.get('old_message_id')

        if not bot_instance:
            return web.json_response({'error': 'Bot not ready'}, status=503)

        guild = bot_instance.get_guild(guild_id)
        if not guild:
            return web.json_response({'error': 'Guild not found'}, status=404)

        channel = guild.get_channel(channel_id)
        if not channel:
            return web.json_response({'error': 'Channel not found'}, status=404)

        # Delete old announcement message if it exists
        if old_message_id:
            try:
                old_message = await channel.fetch_message(int(old_message_id))
                await old_message.delete()
                logger.info(f"Deleted old COTW announcement message {old_message_id} in guild {guild_id}")
            except Exception as e:
                logger.warning(f"Failed to delete old COTW message {old_message_id}: {e}")

        # Get creator profile from database
        from db import get_db_session
        from models import CreatorProfile, DiscoveryConfig

        with get_db_session() as db:
            profile = db.query(CreatorProfile).filter_by(
                guild_id=guild_id,
                discord_id=user_id
            ).first()

            if not profile:
                return web.json_response({'error': 'Creator profile not found'}, status=404)

            # Create announcement embed
            member = guild.get_member(user_id)
            if not member:
                return web.json_response({'error': 'Member not found in guild'}, status=404)

            embed = discord.Embed(
                title="🏆 Creator of the Week",
                description=f"Congratulations to {member.mention}!",
                color=discord.Color.gold()
            )
            embed.set_thumbnail(url=member.display_avatar.url)

            if profile.bio:
                embed.add_field(name="About", value=profile.bio[:1024], inline=False)

            # Add social links
            social_links = []
            if profile.twitch_handle:
                social_links.append(f"[Twitch](https://twitch.tv/{profile.twitch_handle})")
            if profile.youtube_handle:
                social_links.append(f"[YouTube](https://youtube.com/@{profile.youtube_handle})")
            elif profile.youtube_url:
                social_links.append(f"[YouTube]({profile.youtube_url})")
            if profile.twitter_handle:
                social_links.append(f"[Twitter](https://twitter.com/{profile.twitter_handle})")
            if profile.tiktok_handle:
                social_links.append(f"[TikTok](https://tiktok.com/@{profile.tiktok_handle})")
            if profile.instagram_handle:
                social_links.append(f"[Instagram](https://instagram.com/{profile.instagram_handle})")
            if profile.bluesky_handle:
                social_links.append(f"[Bluesky](https://bsky.app/profile/{profile.bluesky_handle})")

            if social_links:
                embed.add_field(name="Links", value=" • ".join(social_links), inline=False)

            embed.set_footer(text="Check out their content!")

            # Post new announcement
            new_message = await channel.send(embed=embed)

            # Update database with new message ID
            config = db.query(DiscoveryConfig).filter_by(guild_id=guild_id).first()
            if config:
                config.cotw_last_message_id = new_message.id
                import time
                config.cotw_last_posted_at = int(time.time())
                config.cotw_last_featured_user_id = user_id
                db.commit()

            logger.info(f"Posted new COTW announcement for user {user_id} in guild {guild_id}, message {new_message.id}")
            return web.json_response({'success': True, 'message_id': new_message.id})

    except Exception as e:
        logger.error(f"Error announcing COTW: {e}", exc_info=True)
        return web.json_response({'error': 'Internal server error'}, status=500)


async def announce_cotm(request):
    """Announce Creator of the Month - delete old message and post new announcement."""
    try:
        data = await request.json()

        guild_id = int(data.get('guild_id'))
        user_id = int(data.get('user_id'))
        channel_id = int(data.get('channel_id'))
        old_message_id = data.get('old_message_id')

        if not bot_instance:
            return web.json_response({'error': 'Bot not ready'}, status=503)

        guild = bot_instance.get_guild(guild_id)
        if not guild:
            return web.json_response({'error': 'Guild not found'}, status=404)

        channel = guild.get_channel(channel_id)
        if not channel:
            return web.json_response({'error': 'Channel not found'}, status=404)

        # Delete old announcement message if it exists
        if old_message_id:
            try:
                old_message = await channel.fetch_message(int(old_message_id))
                await old_message.delete()
                logger.info(f"Deleted old COTM announcement message {old_message_id} in guild {guild_id}")
            except Exception as e:
                logger.warning(f"Failed to delete old COTM message {old_message_id}: {e}")

        # Get creator profile from database
        from db import get_db_session
        from models import CreatorProfile, DiscoveryConfig

        with get_db_session() as db:
            profile = db.query(CreatorProfile).filter_by(
                guild_id=guild_id,
                discord_id=user_id
            ).first()

            if not profile:
                return web.json_response({'error': 'Creator profile not found'}, status=404)

            # Create announcement embed
            member = guild.get_member(user_id)
            if not member:
                return web.json_response({'error': 'Member not found in guild'}, status=404)

            embed = discord.Embed(
                title="👑 Creator of the Month",
                description=f"Congratulations to {member.mention}!",
                color=discord.Color.purple()
            )
            embed.set_thumbnail(url=member.display_avatar.url)

            if profile.bio:
                embed.add_field(name="About", value=profile.bio[:1024], inline=False)

            # Add social links
            social_links = []
            if profile.twitch_handle:
                social_links.append(f"[Twitch](https://twitch.tv/{profile.twitch_handle})")
            if profile.youtube_handle:
                social_links.append(f"[YouTube](https://youtube.com/@{profile.youtube_handle})")
            elif profile.youtube_url:
                social_links.append(f"[YouTube]({profile.youtube_url})")
            if profile.twitter_handle:
                social_links.append(f"[Twitter](https://twitter.com/{profile.twitter_handle})")
            if profile.tiktok_handle:
                social_links.append(f"[TikTok](https://tiktok.com/@{profile.tiktok_handle})")
            if profile.instagram_handle:
                social_links.append(f"[Instagram](https://instagram.com/{profile.instagram_handle})")
            if profile.bluesky_handle:
                social_links.append(f"[Bluesky](https://bsky.app/profile/{profile.bluesky_handle})")

            if social_links:
                embed.add_field(name="Links", value=" • ".join(social_links), inline=False)

            embed.set_footer(text="Check out their content!")

            # Post new announcement
            new_message = await channel.send(embed=embed)

            # Update database with new message ID
            config = db.query(DiscoveryConfig).filter_by(guild_id=guild_id).first()
            if config:
                config.cotm_last_message_id = new_message.id
                import time
                config.cotm_last_posted_at = int(time.time())
                config.cotm_last_featured_user_id = user_id
                db.commit()

            logger.info(f"Posted new COTM announcement for user {user_id} in guild {guild_id}, message {new_message.id}")
            return web.json_response({'success': True, 'message_id': new_message.id})

    except Exception as e:
        logger.error(f"Error announcing COTM: {e}", exc_info=True)
        return web.json_response({'error': 'Internal server error'}, status=500)


async def delete_message(request):
    """Delete a specific message from a channel."""
    try:
        data = await request.json()

        guild_id = int(data.get('guild_id'))
        channel_id = int(data.get('channel_id'))
        message_id = int(data.get('message_id'))

        if not bot_instance:
            return web.json_response({'error': 'Bot not ready'}, status=503)

        guild = bot_instance.get_guild(guild_id)
        if not guild:
            return web.json_response({'error': 'Guild not found'}, status=404)

        # guild.get_channel() only returns channels that belong to this guild,
        # preventing cross-guild channel access even if a valid channel_id from
        # another guild is supplied.
        channel = guild.get_channel(channel_id)
        if not channel:
            return web.json_response({'error': 'Channel not found'}, status=404)

        # Delete the message
        try:
            message = await channel.fetch_message(message_id)
            await message.delete()
            logger.info(f"Deleted message {message_id} from channel {channel_id} in guild {guild_id}")
            return web.json_response({'success': True})
        except Exception as e:
            logger.warning(f"Failed to delete message {message_id}: {e}")
            return web.json_response({'error': 'Failed to delete message'}, status=500)

    except Exception as e:
        logger.error(f"Error deleting message: {e}", exc_info=True)
        return web.json_response({'error': 'Internal server error'}, status=500)


async def announce_network_cotw(request):
    """Announce Network Creator of the Week to all opted-in guilds."""
    try:
        data = await request.json()
        profile_id = int(data.get('profile_id'))

        if not bot_instance:
            return web.json_response({'error': 'Bot not ready'}, status=503)

        # Get creator profile from database
        from db import get_db_session
        from models import CreatorProfile, DiscoveryConfig

        with get_db_session() as db:
            profile = db.query(CreatorProfile).filter_by(id=profile_id).first()

            if not profile:
                return web.json_response({'error': 'Creator profile not found'}, status=404)

            # Get all guilds that opted in to network announcements
            configs = db.query(DiscoveryConfig).filter(
                DiscoveryConfig.network_announcements_enabled == True,
                DiscoveryConfig.network_announcement_channel_id != None
            ).all()

            if not configs:
                return web.json_response({'success': True, 'message': 'No guilds opted in for network announcements'})

            # Get the creator's home guild for member object
            home_guild = bot_instance.get_guild(profile.guild_id)
            home_member = home_guild.get_member(profile.discord_id) if home_guild else None

            posted_count = 0
            failed_count = 0

            # Post to each opted-in guild
            for config in configs:
                try:
                    guild = bot_instance.get_guild(config.guild_id)
                    if not guild:
                        continue

                    channel = guild.get_channel(config.network_announcement_channel_id)
                    if not channel:
                        logger.warning(f"[Network COTW] Channel {config.network_announcement_channel_id} not found in guild {guild.id}")
                        failed_count += 1
                        continue

                    embed = discord.Embed(
                        title="🏆 Network Creator of the Week",
                        description=f"Congratulations to **{profile.display_name}** for being selected as this week's featured creator across the QuestLog network!",
                        color=discord.Color.gold()
                    )

                    if home_member:
                        embed.set_thumbnail(url=home_member.display_avatar.url)

                    if profile.bio:
                        embed.add_field(name="About", value=profile.bio[:1024], inline=False)

                    # Add home server info
                    if home_guild:
                        embed.add_field(name="Home Server", value=home_guild.name, inline=True)

                    # Add social links
                    social_links = []
                    if profile.twitch_handle:
                        social_links.append(f"[Twitch](https://twitch.tv/{profile.twitch_handle})")
                    if profile.youtube_handle:
                        social_links.append(f"[YouTube](https://youtube.com/@{profile.youtube_handle})")
                    elif profile.youtube_url:
                        social_links.append(f"[YouTube]({profile.youtube_url})")
                    if profile.twitter_handle:
                        social_links.append(f"[Twitter](https://twitter.com/{profile.twitter_handle})")
                    if profile.tiktok_handle:
                        social_links.append(f"[TikTok](https://tiktok.com/@{profile.tiktok_handle})")
                    if profile.instagram_handle:
                        social_links.append(f"[Instagram](https://instagram.com/{profile.instagram_handle})")
                    if profile.bluesky_handle:
                        social_links.append(f"[Bluesky](https://bsky.app/profile/{profile.bluesky_handle})")

                    if social_links:
                        embed.add_field(name="Links", value=" • ".join(social_links), inline=False)

                    embed.set_footer(text="Manually set by Discovery Approver • View all creators on Discovery Network")

                    await channel.send(embed=embed)
                    posted_count += 1
                    logger.info(f"[Network COTW] Posted announcement in guild {guild.id}")

                except Exception as e:
                    logger.error(f"[Network COTW] Failed to post in guild {config.guild_id}: {e}")
                    failed_count += 1

            return web.json_response({
                'success': True,
                'posted_count': posted_count,
                'failed_count': failed_count
            })

    except Exception as e:
        logger.error(f"Error announcing Network COTW: {e}", exc_info=True)
        return web.json_response({'error': 'Internal server error'}, status=500)


async def announce_network_cotm(request):
    """Announce Network Creator of the Month to all opted-in guilds."""
    try:
        data = await request.json()
        profile_id = int(data.get('profile_id'))

        if not bot_instance:
            return web.json_response({'error': 'Bot not ready'}, status=503)

        # Get creator profile from database
        from db import get_db_session
        from models import CreatorProfile, DiscoveryConfig

        with get_db_session() as db:
            profile = db.query(CreatorProfile).filter_by(id=profile_id).first()

            if not profile:
                return web.json_response({'error': 'Creator profile not found'}, status=404)

            # Get all guilds that opted in to network announcements
            configs = db.query(DiscoveryConfig).filter(
                DiscoveryConfig.network_announcements_enabled == True,
                DiscoveryConfig.network_announcement_channel_id != None
            ).all()

            if not configs:
                return web.json_response({'success': True, 'message': 'No guilds opted in for network announcements'})

            # Get the creator's home guild for member object
            home_guild = bot_instance.get_guild(profile.guild_id)
            home_member = home_guild.get_member(profile.discord_id) if home_guild else None

            posted_count = 0
            failed_count = 0

            # Post to each opted-in guild
            for config in configs:
                try:
                    guild = bot_instance.get_guild(config.guild_id)
                    if not guild:
                        continue

                    channel = guild.get_channel(config.network_announcement_channel_id)
                    if not channel:
                        logger.warning(f"[Network COTM] Channel {config.network_announcement_channel_id} not found in guild {guild.id}")
                        failed_count += 1
                        continue

                    embed = discord.Embed(
                        title="👑 Network Creator of the Month",
                        description=f"Congratulations to **{profile.display_name}** for being selected as this month's featured creator across the QuestLog network!",
                        color=discord.Color.purple()
                    )

                    if home_member:
                        embed.set_thumbnail(url=home_member.display_avatar.url)

                    if profile.bio:
                        embed.add_field(name="About", value=profile.bio[:1024], inline=False)

                    # Add home server info
                    if home_guild:
                        embed.add_field(name="Home Server", value=home_guild.name, inline=True)

                    # Add social links
                    social_links = []
                    if profile.twitch_handle:
                        social_links.append(f"[Twitch](https://twitch.tv/{profile.twitch_handle})")
                    if profile.youtube_handle:
                        social_links.append(f"[YouTube](https://youtube.com/@{profile.youtube_handle})")
                    elif profile.youtube_url:
                        social_links.append(f"[YouTube]({profile.youtube_url})")
                    if profile.twitter_handle:
                        social_links.append(f"[Twitter](https://twitter.com/{profile.twitter_handle})")
                    if profile.tiktok_handle:
                        social_links.append(f"[TikTok](https://tiktok.com/@{profile.tiktok_handle})")
                    if profile.instagram_handle:
                        social_links.append(f"[Instagram](https://instagram.com/{profile.instagram_handle})")
                    if profile.bluesky_handle:
                        social_links.append(f"[Bluesky](https://bsky.app/profile/{profile.bluesky_handle})")

                    if social_links:
                        embed.add_field(name="Links", value=" • ".join(social_links), inline=False)

                    embed.set_footer(text="Manually set by Discovery Approver • View all creators on Discovery Network")

                    await channel.send(embed=embed)
                    posted_count += 1
                    logger.info(f"[Network COTM] Posted announcement in guild {guild.id}")

                except Exception as e:
                    logger.error(f"[Network COTM] Failed to post in guild {config.guild_id}: {e}")
                    failed_count += 1

            return web.json_response({
                'success': True,
                'posted_count': posted_count,
                'failed_count': failed_count
            })

    except Exception as e:
        logger.error(f"Error announcing Network COTM: {e}", exc_info=True)
        return web.json_response({'error': 'Internal server error'}, status=500)


def create_app():
    """Create the aiohttp web application."""
    # SECURITY: Add authentication middleware
    # Dashboard requests are small JSON control messages. A tight body limit
    # reduces memory-exhaustion risk if the localhost boundary is bypassed.
    app = web.Application(
        middlewares=[auth_middleware],
        client_max_size=64 * 1024,
    )

    # Routes
    app.router.add_get('/health', health_check, name='health')
    app.router.add_get('/api/guilds', get_guild_ids, name='guilds-list')
    app.router.add_post('/api/sync', force_guild_sync, name='guild-sync-all')
    app.router.add_post('/api/sync/{guild_id}', force_guild_sync, name='guild-sync-one')

    # Moderation endpoints (now protected by auth_middleware)
    app.router.add_post('/mod/untimeout', mod_untimeout, name='mod-untimeout')
    app.router.add_post('/mod/kick', mod_kick, name='mod-kick')
    app.router.add_post('/mod/ban', mod_ban, name='mod-ban')
    app.router.add_post('/mod/unban', mod_unban, name='mod-unban')
    app.router.add_post('/mod/unmute', mod_unmute, name='mod-unmute')
    app.router.add_post('/mod/unjail', mod_unjail, name='mod-unjail')

    # Creator Discovery endpoints
    app.router.add_post(
        '/api/announce-cotw', announce_cotw, name='creator-announce-cotw'
    )
    app.router.add_post(
        '/api/announce-cotm', announce_cotm, name='creator-announce-cotm'
    )
    app.router.add_post(
        '/api/delete-message', delete_message, name='creator-delete-message'
    )

    # Network Creator Discovery endpoints (DISCOVERY_APPROVERS only)
    app.router.add_post(
        '/api/announce-network-cotw',
        announce_network_cotw,
        name='creator-network-cotw',
    )
    app.router.add_post(
        '/api/announce-network-cotm',
        announce_network_cotm,
        name='creator-network-cotm',
    )

    logger.info("API server created with %s authentication mode", API_AUTH_MODE)
    return app


async def start_api_server(bot):
    """Start the API server."""
    global bot_instance
    bot_instance = bot

    app = create_app()
    runner = web.AppRunner(app)
    await runner.setup()

    # Get port from env or use default
    port = int(os.getenv('BOT_API_PORT', 8001))
    site = web.TCPSite(runner, 'localhost', port)
    await site.start()

    logger.info(f"✅ Bot API server started on http://localhost:{port}")
    return runner
