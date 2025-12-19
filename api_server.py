"""
Simple API server for bot control endpoints.
Allows the web app to trigger actions like forcing a guild sync.
"""
from aiohttp import web
import logging
import os

logger = logging.getLogger("api_server")

# Store bot reference (set by bot.py on startup)
bot_instance = None


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

                    # Update guild info
                    db_guild.name = guild.name
                    db_guild.icon_url = guild.icon.url if guild.icon else None
                    db_guild.member_count = guild.member_count

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

                logger.info(f"✅ Forced sync for guild {guild_id} - {len(channels_data)} channels, {len(roles_data)} roles")
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
        return web.json_response({'error': str(e)}, status=500)


async def mod_untimeout(request):
    """Remove timeout from a user."""
    try:
        data = await request.json()
        guild_id = int(data.get('guild_id'))
        user_id = int(data.get('user_id'))
        reason = data.get('reason', 'Timeout removed via web dashboard')

        if not bot_instance:
            return web.json_response({'error': 'Bot not ready'}, status=503)

        guild = bot_instance.get_guild(guild_id)
        if not guild:
            return web.json_response({'error': 'Guild not found'}, status=404)

        member = guild.get_member(user_id)
        if not member:
            return web.json_response({'error': 'Member not found'}, status=404)

        if not member.timed_out:
            return web.json_response({'error': 'User is not timed out'}, status=400)

        # Remove timeout
        await member.remove_timeout(reason=reason)

        logger.info(f"Removed timeout from {member} in {guild.name} via API")
        return web.json_response({'success': True, 'message': f'Timeout removed from {member}'})

    except Exception as e:
        logger.error(f"Error in mod_untimeout: {e}", exc_info=True)
        return web.json_response({'error': str(e)}, status=500)


async def mod_kick(request):
    """Kick a user from the server."""
    try:
        data = await request.json()
        guild_id = int(data.get('guild_id'))
        user_id = int(data.get('user_id'))
        reason = data.get('reason', 'Kicked via web dashboard')

        if not bot_instance:
            return web.json_response({'error': 'Bot not ready'}, status=503)

        guild = bot_instance.get_guild(guild_id)
        if not guild:
            return web.json_response({'error': 'Guild not found'}, status=404)

        member = guild.get_member(user_id)
        if not member:
            return web.json_response({'error': 'Member not found'}, status=404)

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

        logger.info(f"Kicked {member} from {guild.name} via API")
        return web.json_response({'success': True, 'message': f'Kicked {member}'})

    except Exception as e:
        logger.error(f"Error in mod_kick: {e}", exc_info=True)
        return web.json_response({'error': str(e)}, status=500)


async def mod_ban(request):
    """Ban a user from the server."""
    try:
        data = await request.json()
        guild_id = int(data.get('guild_id'))
        user_id = int(data.get('user_id'))
        reason = data.get('reason', 'Banned via web dashboard')

        if not bot_instance:
            return web.json_response({'error': 'Bot not ready'}, status=503)

        guild = bot_instance.get_guild(guild_id)
        if not guild:
            return web.json_response({'error': 'Guild not found'}, status=404)

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

                logger.info(f"Banned user {user_id} from {guild.name} via API")
                return web.json_response({'success': True, 'message': f'Banned user {user_id}'})
            except Exception as e:
                return web.json_response({'error': f'User not found: {str(e)}'}, status=404)

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

        logger.info(f"Banned {member} from {guild.name} via API")
        return web.json_response({'success': True, 'message': f'Banned {member}'})

    except Exception as e:
        logger.error(f"Error in mod_ban: {e}", exc_info=True)
        return web.json_response({'error': str(e)}, status=500)


async def mod_unban(request):
    """Unban a user from the server."""
    try:
        data = await request.json()
        guild_id = int(data.get('guild_id'))
        user_id = int(data.get('user_id'))
        reason = data.get('reason', 'Unbanned via web dashboard')

        if not bot_instance:
            return web.json_response({'error': 'Bot not ready'}, status=503)

        guild = bot_instance.get_guild(guild_id)
        if not guild:
            return web.json_response({'error': 'Guild not found'}, status=404)

        # Get user object
        try:
            user = await bot_instance.fetch_user(user_id)
        except Exception as e:
            return web.json_response({'error': f'User not found: {str(e)}'}, status=404)

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

        logger.info(f"Unbanned {user} from {guild.name} via API")
        return web.json_response({'success': True, 'message': f'Unbanned {user}'})

    except Exception as e:
        logger.error(f"Error in mod_unban: {e}", exc_info=True)
        return web.json_response({'error': str(e)}, status=500)


async def mod_unmute(request):
    """Unmute a user."""
    try:
        data = await request.json()
        guild_id = int(data.get('guild_id'))
        user_id = int(data.get('user_id'))
        reason = data.get('reason', 'Unmuted via web dashboard')

        if not bot_instance:
            return web.json_response({'error': 'Bot not ready'}, status=503)

        guild = bot_instance.get_guild(guild_id)
        if not guild:
            return web.json_response({'error': 'Guild not found'}, status=404)

        member = guild.get_member(user_id)
        if not member:
            return web.json_response({'error': 'Member not found'}, status=404)

        # Get moderation cog
        mod_cog = bot_instance.get_cog('ModerationCog')
        if not mod_cog:
            return web.json_response({'error': 'Moderation cog not loaded'}, status=500)

        # Unmute the user using the cog's method
        success = await mod_cog._unmute_user(guild, member, reason)

        if success:
            logger.info(f"Unmuted {member} in {guild.name} via API")
            return web.json_response({'success': True, 'message': f'Unmuted {member}'})
        else:
            return web.json_response({'error': 'Failed to unmute user'}, status=500)

    except Exception as e:
        logger.error(f"Error in mod_unmute: {e}", exc_info=True)
        return web.json_response({'error': str(e)}, status=500)


async def mod_unjail(request):
    """Unjail a user."""
    try:
        data = await request.json()
        guild_id = int(data.get('guild_id'))
        user_id = int(data.get('user_id'))
        reason = data.get('reason', 'Unjailed via web dashboard')

        if not bot_instance:
            return web.json_response({'error': 'Bot not ready'}, status=503)

        guild = bot_instance.get_guild(guild_id)
        if not guild:
            return web.json_response({'error': 'Guild not found'}, status=404)

        member = guild.get_member(user_id)
        if not member:
            return web.json_response({'error': 'Member not found'}, status=404)

        # Get moderation cog
        mod_cog = bot_instance.get_cog('ModerationCog')
        if not mod_cog:
            return web.json_response({'error': 'Moderation cog not loaded'}, status=500)

        # Unjail the user using the cog's method
        success = await mod_cog._unjail_user(guild, member, reason)

        if success:
            logger.info(f"Unjailed {member} in {guild.name} via API")
            return web.json_response({'success': True, 'message': f'Unjailed {member}'})
        else:
            return web.json_response({'error': 'Failed to unjail user'}, status=500)

    except Exception as e:
        logger.error(f"Error in mod_unjail: {e}", exc_info=True)
        return web.json_response({'error': str(e)}, status=500)


def create_app():
    """Create the aiohttp web application."""
    app = web.Application()

    # Routes
    app.router.add_get('/health', health_check)
    app.router.add_get('/api/guilds', get_guild_ids)
    app.router.add_post('/api/sync', force_guild_sync)
    app.router.add_post('/api/sync/{guild_id}', force_guild_sync)

    # Moderation endpoints
    app.router.add_post('/mod/untimeout', mod_untimeout)
    app.router.add_post('/mod/kick', mod_kick)
    app.router.add_post('/mod/ban', mod_ban)
    app.router.add_post('/mod/unban', mod_unban)
    app.router.add_post('/mod/unmute', mod_unmute)
    app.router.add_post('/mod/unjail', mod_unjail)

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
