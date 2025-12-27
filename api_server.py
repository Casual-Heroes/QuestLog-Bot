"""
Simple API server for bot control endpoints.
Allows the web app to trigger actions like forcing a guild sync.
"""
from aiohttp import web
import logging
import os
import discord

logger = logging.getLogger("api_server")

# Store bot reference (set by bot.py on startup)
bot_instance = None

# SECURITY: Load API token from environment
API_TOKEN = os.getenv("DISCORD_BOT_API_TOKEN")
if not API_TOKEN:
    logger.warning("DISCORD_BOT_API_TOKEN not set - API will be unauthenticated!")


@web.middleware
async def auth_middleware(request, handler):
    """Require Bearer token authentication for all non-health endpoints."""
    # Skip auth for health check
    if request.path == '/health':
        return await handler(request)

    # Require authentication
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        logger.warning(f"Unauthorized API request from {request.remote}")
        return web.json_response({'error': 'Unauthorized - Missing Bearer token'}, status=401)

    token = auth_header.split('Bearer ', 1)[1]
    if token != API_TOKEN:
        logger.warning(f"Invalid API token from {request.remote}")
        return web.json_response({'error': 'Unauthorized - Invalid token'}, status=401)

    # Token is valid, proceed
    return await handler(request)


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

        # Convert to integers with error handling
        try:
            guild_id = int(guild_id_raw)
            user_id = int(user_id_raw)
            requester_id = int(requester_id)
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


def create_app():
    """Create the aiohttp web application."""
    # SECURITY: Add authentication middleware
    app = web.Application(middlewares=[auth_middleware])

    # Routes
    app.router.add_get('/health', health_check)
    app.router.add_get('/api/guilds', get_guild_ids)
    app.router.add_post('/api/sync', force_guild_sync)
    app.router.add_post('/api/sync/{guild_id}', force_guild_sync)

    # Moderation endpoints (now protected by auth_middleware)
    app.router.add_post('/mod/untimeout', mod_untimeout)
    app.router.add_post('/mod/kick', mod_kick)
    app.router.add_post('/mod/ban', mod_ban)
    app.router.add_post('/mod/unban', mod_unban)
    app.router.add_post('/mod/unmute', mod_unmute)
    app.router.add_post('/mod/unjail', mod_unjail)

    logger.info("API server created with authentication middleware")
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
