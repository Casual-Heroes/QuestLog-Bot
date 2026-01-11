# bot.py - Main Bot Entry Point
"""
QuestLog - Discord Security & Engagement Platform

Run with: python -m bot
"""

import os
import sys
import asyncio
from pathlib import Path
import discord
from discord.ext import commands, tasks

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (
    bot,
    get_bot_token,
    init_database,
    db_session_scope,
    get_engine,
    intents,
    logger,
    IS_PRODUCTION,
    get_debug_guilds,
)
from models import Guild, SubscriptionTier


# ====== Rotating Presence ======

# Presence messages to rotate through
PRESENCE_MESSAGES = [
    ("watching", "{server_count} servers"),
    ("playing", "/questlog help"),
    ("playing", "/xp profile"),
    ("playing", "/flair store"),
    ("playing", "/leaderboard"),
    ("watching", "your server grow"),
    ("playing", "/questlog dashboard"),
    ("listening", "your commands"),
]

current_presence_index = 0

@tasks.loop(hours=2)  # Rotate every 2 hours
async def rotate_presence():
    """Rotate bot presence every 2 hours."""
    global current_presence_index

    if not bot.guilds:
        return

    presence_type, message = PRESENCE_MESSAGES[current_presence_index]
    server_count = len(bot.guilds)

    # Replace {server_count} placeholder
    message = message.format(server_count=server_count)

    # Set activity type
    if presence_type == "playing":
        activity = discord.Activity(type=discord.ActivityType.playing, name=message)
    elif presence_type == "watching":
        activity = discord.Activity(type=discord.ActivityType.watching, name=message)
    elif presence_type == "listening":
        activity = discord.Activity(type=discord.ActivityType.listening, name=message)
    else:
        activity = discord.Activity(type=discord.ActivityType.watching, name=message)

    await bot.change_presence(activity=activity, status=discord.Status.online)
    logger.debug(f"Rotated presence to: {presence_type} {message}")

    # Move to next presence
    current_presence_index = (current_presence_index + 1) % len(PRESENCE_MESSAGES)

@rotate_presence.before_loop
async def before_rotate_presence():
    """Wait for bot to be ready before starting rotation."""
    await bot.wait_until_ready()


# ====== Event Handlers ======

@bot.event
async def on_ready():
    """Called when bot is ready and connected."""
    import time

    if bot.start_time is None:
        bot.start_time = time.time()

    logger.info(f"{'=' * 50}")
    logger.info(f"QuestLog is ready!")
    logger.info(f"Logged in as: {bot.user.name} ({bot.user.id})")
    logger.info(f"Connected to {len(bot.guilds)} guilds")
    logger.info(f"Pycord version: {discord.__version__}")
    logger.info(f"Production mode: {IS_PRODUCTION}")
    logger.info(f"{'=' * 50}")

    # Start rotating presence (only on first ready)
    if not bot._cogs_loaded and not rotate_presence.is_running():
        rotate_presence.start()
        logger.info("✅ Started rotating presence")

    # Set initial presence manually for immediate effect
    activity = discord.Activity(
        type=discord.ActivityType.watching,
        name=f"{len(bot.guilds)} servers"
    )
    await bot.change_presence(activity=activity, status=discord.Status.online)

    # Start API server (only on first ready)
    if not bot._cogs_loaded:
        from api_server import start_api_server
        try:
            await start_api_server(bot)
        except Exception as e:
            logger.error(f"Failed to start API server: {e}")

    # Force sync commands to ensure permissions are up-to-date (only on first ready)
    if not bot._cogs_loaded:
        try:
            await bot.sync_commands()
            logger.info("✅ Commands synced successfully")
        except Exception as e:
            logger.error(f"Failed to sync commands: {e}")

    # Sync guilds to database (only on first ready)
    if not bot._cogs_loaded:
        logger.info("Syncing guilds to database...")
        await sync_all_guilds()
        bot._cogs_loaded = True
        logger.info("✅ Bot ready - commands should sync automatically")
    else:
        logger.info("Bot reconnected - skipping guild sync")


async def sync_all_guilds():
    """Ensure all connected guilds are in the database and marked as active."""
    import json
    synced = 0
    reactivated = 0
    with db_session_scope() as session:
        for guild in bot.guilds:
            existing = session.get(Guild, guild.id)

            # Cache guild resources (channels, roles, emojis)
            channels_data = []
            for channel in guild.channels:
                channels_data.append({
                    'id': str(channel.id),
                    'name': channel.name,
                    'type': channel.type.value,  # Numeric value (0=text, 2=voice, 4=category, etc.)
                    'category_name': channel.category.name if channel.category else None
                })

            roles_data = []
            for role in guild.roles:
                if role.name != "@everyone":  # Skip @everyone role
                    roles_data.append({
                        'id': str(role.id),
                        'name': role.name,
                        'color': role.color.value,
                        'position': role.position
                    })

            emojis_data = []
            for emoji in guild.emojis:
                emojis_data.append({
                    'id': str(emoji.id),
                    'name': emoji.name,
                    'animated': emoji.animated
                })

            # Cache guild members (industry standard: cache from Gateway events)
            members_data = []
            for member in guild.members:
                if not member.bot:  # Exclude bots from cache
                    members_data.append({
                        'id': str(member.id),
                        'username': member.name,
                        'discriminator': member.discriminator,
                        'display_name': member.display_name,
                        'avatar': member.avatar.url if member.avatar else None,
                        'roles': [str(role.id) for role in member.roles if role.name != "@everyone"],
                        'joined_at': member.joined_at.isoformat() if member.joined_at else None
                    })

            if not existing:
                new_guild = Guild(
                    guild_id=guild.id,
                    guild_name=guild.name,
                    owner_id=guild.owner_id,
                    subscription_tier='free',
                    bot_present=True,
                    left_at=None,
                    cached_channels=json.dumps(channels_data),
                    cached_roles=json.dumps(roles_data),
                    cached_emojis=json.dumps(emojis_data),
                    cached_members=json.dumps(members_data),
                )
                session.add(new_guild)
                synced += 1
            else:
                if not existing.bot_present:
                    existing.bot_present = True
                    existing.left_at = None
                    reactivated += 1
                if existing.guild_name != guild.name:
                    existing.guild_name = guild.name
                # Update cached resources
                existing.cached_channels = json.dumps(channels_data)
                existing.cached_roles = json.dumps(roles_data)
                existing.cached_emojis = json.dumps(emojis_data)
                existing.cached_members = json.dumps(members_data)

    logger.info(f"✅ Synced {synced} new guilds, reactivated {reactivated} guilds")


@bot.event
async def on_guild_join(guild: discord.Guild):
    """Called when bot joins a new guild."""
    import json
    logger.info(f"Joined guild: {guild.name} ({guild.id}) - {guild.member_count} members")

    # Cache guild resources
    channels_data = []
    for channel in guild.channels:
        channels_data.append({
            'id': str(channel.id),
            'name': channel.name,
            'type': channel.type.value,  # Numeric value (0=text, 2=voice, 4=category, etc.)
            'category_name': channel.category.name if channel.category else None
        })

    roles_data = []
    for role in guild.roles:
        if role.name != "@everyone":
            roles_data.append({
                'id': str(role.id),
                'name': role.name,
                'color': role.color.value,
                'position': role.position
            })

    emojis_data = []
    for emoji in guild.emojis:
        emojis_data.append({
            'id': str(emoji.id),
            'name': emoji.name,
            'animated': emoji.animated
        })

    # Cache guild members
    members_data = []
    for member in guild.members:
        if not member.bot:  # Exclude bots from cache
            members_data.append({
                'id': str(member.id),
                'username': member.name,
                'discriminator': member.discriminator,
                'display_name': member.display_name,
                'avatar': member.avatar.url if member.avatar else None,
                'roles': [str(role.id) for role in member.roles if role.name != "@everyone"],
                'joined_at': member.joined_at.isoformat() if member.joined_at else None
            })

    with db_session_scope() as session:
        existing = session.get(Guild, guild.id)
        if not existing:
            new_guild = Guild(
                guild_id=guild.id,
                guild_name=guild.name,
                owner_id=guild.owner_id,
                subscription_tier='free',
                bot_present=True,
                left_at=None,
                cached_channels=json.dumps(channels_data),
                cached_roles=json.dumps(roles_data),
                cached_emojis=json.dumps(emojis_data),
                cached_members=json.dumps(members_data),
            )
            session.add(new_guild)
            logger.info(f"✅ Added new guild {guild.name} to database")
        else:
            existing.bot_present = True
            existing.left_at = None
            existing.guild_name = guild.name
            existing.owner_id = guild.owner_id
            existing.cached_channels = json.dumps(channels_data)
            existing.cached_roles = json.dumps(roles_data)
            existing.cached_emojis = json.dumps(emojis_data)
            existing.cached_members = json.dumps(members_data)
            logger.info(f"✅ Reactivated guild {guild.name} - all data preserved!")

    activity = discord.Activity(
        type=discord.ActivityType.watching,
        name=f"{len(bot.guilds)} servers | /questlog help"
    )
    await bot.change_presence(activity=activity)

    if guild.system_channel:
        try:
            embed = discord.Embed(
                title="👋 Thanks for adding QuestLog!",
                description=(
                    "QuestLog is your all-in-one security and engagement bot.\n\n"
                    "**Get started:**\n"
                    "• `/questlog setup` - Quick setup wizard\n"
                    "• `/questlog help` - See all commands\n"
                    "• `/questlog dashboard` - Web dashboard\n\n"
                    "**Free features:** XP, leveling, anti-raid, verification\n"
                    "**Premium:** Discovery network, game server sync, analytics"
                ),
                color=discord.Color.brand_green()
            )
            embed.set_footer(text="Need help? Join our support server: discord.gg/questlog")
            await guild.system_channel.send(embed=embed)
        except discord.Forbidden:
            logger.warning(f"Couldn't send welcome message to {guild.name}")


@bot.event
async def on_guild_remove(guild: discord.Guild):
    """Called when bot is removed from a guild."""
    import time
    logger.info(f"Removed from guild: {guild.name} ({guild.id})")

    with db_session_scope() as session:
        existing = session.get(Guild, guild.id)
        if existing:
            existing.bot_present = False
            existing.left_at = int(time.time())
            logger.info(f"✅ Marked guild {guild.name} as inactive - data preserved for rejoin")

    activity = discord.Activity(
        type=discord.ActivityType.watching,
        name=f"{len(bot.guilds)} servers | /questlog help"
    )
    await bot.change_presence(activity=activity)


@bot.event
async def on_application_command_error(
    ctx: discord.ApplicationContext,
    error: discord.DiscordException
):
    """Global error handler for slash commands."""
    if isinstance(error, commands.CommandOnCooldown):
        await ctx.respond(
            f"⏳ Command on cooldown. Try again in {error.retry_after:.1f}s",
            ephemeral=True
        )
    elif isinstance(error, commands.MissingPermissions):
        await ctx.respond(
            "❌ You don't have permission to use this command.",
            ephemeral=True
        )
    elif isinstance(error, commands.BotMissingPermissions):
        await ctx.respond(
            f"❌ I'm missing permissions: {', '.join(error.missing_permissions)}",
            ephemeral=True
        )
    else:
        logger.error(f"Command error in {ctx.guild}: {error}", exc_info=error)
        await ctx.respond(
            "❌ An error occurred. Please try again later.",
            ephemeral=True
        )


# Monkey-patch pycord's HTTP client to add better rate limit logging
original_request = discord.http.HTTPClient.request

async def patched_request(self, *args, **kwargs):
    """Wrapper around HTTPClient.request to log rate limit details."""
    try:
        return await original_request(self, *args, **kwargs)
    except discord.HTTPException as e:
        if e.status == 429:
            # Extract route/endpoint info
            route = args[0] if args else "unknown"
            logger.error(
                f"Discord API Rate Limited (429):\n"
                f"  Route: {route}\n"
                f"  Status: {e.status}\n"
                f"  Code: {e.code}\n"
                f"  Response: {e.response}\n"
                f"  Text: {e.text}"
            )
        raise

discord.http.HTTPClient.request = patched_request



def main():
    """Entry point for running the bot."""
    logger.info("Starting QuestLog...")

    # Initialize database (was in QuestLogBot.setup_hook)
    try:
        init_database()
        logger.info("✅ Database initialized")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        sys.exit(1)

    try:
        token = get_bot_token()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    # Use imported bot from config (simple pattern like Q7)
    # Add bot attributes that QuestLogBot had
    bot.db_engine = get_engine()
    bot.start_time = None
    bot.commands_processed = 0
    bot.events_processed = 0
    bot._cogs_loaded = False

    # Load all cogs before connecting (like Q7 bot pattern)
    logger.info("Loading cogs...")
    cogs_to_load = [
        "cogs.core",
        "cogs.security",
        "cogs.verification",
        "cogs.audit",
        "cogs.xp",
        "cogs.roles",
        "cogs.welcome",
        "cogs.moderation",
        "cogs.channels",
        "cogs.lfg_cog",
        "cogs.discovery",
        "cogs.admin",
        "cogs.action_processor",
        "cogs.activity_tracker",
        "cogs.billing",
        "cogs.guild_sync_cog",  # Syncs member counts from Discord every 5 min
        "cogs.guild_sync",  # Auto-syncs roles/channels when they change (60s cooldown)
        "cogs.flair_cog",  # Flair store - let members customize their profile
        "cogs.raffles",  # Raffles integration
        "cogs.scheduled_messages",  # Scheduled message processor
        "cogs.streaming_monitor",  # YouTube/Twitch live stream monitor & notifications
        "cogs.site_activity_tracker",  # Site activity tracker - database-driven Discord game tracking
    ]

    loaded_count = 0
    for cog in cogs_to_load:
        try:
            bot.load_extension(cog)
            loaded_count += 1
            logger.info(f"  ✅ Loaded: {cog}")
        except Exception as e:
            logger.warning(f"  ⚠️ Failed to load {cog}: {e}")

    logger.info(f"✅ Loaded {loaded_count}/{len(cogs_to_load)} cogs")

    try:
        bot.run(token)
    except discord.LoginFailure:
        logger.error("Invalid bot token! Check WARDEN_BOT_TOKEN environment variable.")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot crashed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

