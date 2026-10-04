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
    ENABLE_BRIDGE,
    ENABLE_BRIDGE_IMPLICIT,
    ENABLE_LEGACY_STREAMING_MONITOR,
    ENABLE_LEGACY_DISCORD_FLAIR_STORE,
    ENABLE_EMERGENCY_SERVICE_CONTROL,
    ENABLE_LEGACY_SITE_ACTIVITY_EXPORT,
)
from models import Guild


# ====== Rotating Presence ======

# Presence messages to rotate through
PRESENCE_MESSAGES = [
    ("watching", "{server_count} communities | /questlog help"),
    ("playing", "/xp profile | {server_count} communities"),
    ("playing", "/leaderboard | {server_count} communities"),
    ("watching", "{server_count} communities organize play"),
    ("playing", "/questlog dashboard | {server_count} communities"),
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
        name=f"{len(bot.guilds)} communities | /questlog help"
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
    """Reconcile stored guild presence with Discord's complete READY guild list."""
    import json
    import time
    from utils.guild_presence import mark_departed_guilds

    synced = 0
    reactivated = 0
    departed = []
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

        active_records = session.query(Guild).filter(Guild.bot_present.is_(True)).all()
        departed_records = mark_departed_guilds(
            active_records,
            (guild.id for guild in bot.guilds),
            left_at=int(time.time()),
        )
        departed = [
            (record.guild_name, record.guild_id)
            for record in departed_records
        ]

    for guild_name, guild_id in departed:
        logger.info(
            "Marked guild %s (%s) inactive during startup reconciliation",
            guild_name,
            guild_id,
        )

    logger.info(
        "✅ Guild presence reconciled: %s new, %s reactivated, %s departed",
        synced,
        reactivated,
        len(departed),
    )


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
        name=f"{len(bot.guilds)} communities | /questlog help"
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
                    "**All features are free:** XP, leveling, anti-raid, verification, discovery, and more!"
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
        name=f"{len(bot.guilds)} communities | /questlog help"
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
        "cogs.progression_api",        # Scoped QuestLog XP evidence adapter
        "cogs.xp",
        "cogs.roles",
        "cogs.rss_feeds",
        "cogs.welcome",
        "cogs.moderation",
        "cogs.channels",
        "cogs.channel_directory",      # Auto-updating public channel directory
        "cogs.lfg_api",                # Shared canonical QuestLog LFG API client
        "cogs.lfg_cog",
        "cogs.discovery",
        "cogs.admin",
        "cogs.action_processor",
        "cogs.activity_tracker",
        "cogs.guild_sync_cog",  # Syncs member counts from Discord every 5 min
        "cogs.guild_sync",  # Auto-syncs roles/channels when they change (60s cooldown)
        "cogs.raffles",  # Raffles integration
        "cogs.scheduled_messages",  # Scheduled message processor
        "cogs.live_alerts",        # Per-guild streamer subscriptions (web dashboard managed)
        "cogs.network_broadcasts",     # QuestLog Network - receive LFG broadcasts from site
        "cogs.invite",                 # /invite slash command - Discord early access codes
        "cogs.flair_sync",             # QuestLog flair -> Discord role sync (opt-in per guild)
        "cogs.gameserver",             # Game server status embeds (Quest Control dashboard)
        "cogs.legacy",                 # Legacy points: star reactions + clean record milestones
        "cogs.nominations",            # Monthly community spotlight nominations + voting
        "cogs.creators",               # Creator of Week/Month spotlight commands
        "cogs.ffxiv_timers",           # FFXIV gathering/ocean/reset timer alerts
        # "cogs.soulmask",              # Soulmask cluster management via AMP RCON - DISABLED, not an active game (2026-07-12)
    ]

    if ENABLE_BRIDGE:
        bridge_secret = os.getenv("QUESTLOG_BOT_SECRET", "").strip()
        if len(bridge_secret) < 32:
            logger.critical(
                "ENABLE_BRIDGE is true but QUESTLOG_BOT_SECRET is shorter than 32 characters; refusing to load the bridge"
            )
        else:
            cogs_to_load.append("cogs.bridge_cog")
            if ENABLE_BRIDGE_IMPLICIT:
                logger.warning(
                    "Bridge enabled by legacy secret detection; set ENABLE_BRIDGE=true explicitly"
                )
            logger.warning(
                "Cross-platform bridge enabled; remote media and channel targets are restricted"
            )

    # This legacy cog embeds the website's Django process inside Warden and
    # duplicates the dashboard-managed live alerts adapter. It is off by
    # default and exists only as a controlled migration escape hatch.
    if ENABLE_LEGACY_STREAMING_MONITOR:
        cogs_to_load.append("cogs.streaming_monitor")
        logger.warning("Legacy streaming monitor enabled; migrate to cogs.live_alerts")

    if ENABLE_LEGACY_DISCORD_FLAIR_STORE:
        cogs_to_load.append("cogs.flair_cog")
        logger.warning("Legacy Discord flair store enabled; migrate selection to QuestLog")

    if ENABLE_EMERGENCY_SERVICE_CONTROL:
        cogs_to_load.append("cogs.emergency")
        logger.critical(
            "Discord-triggered host service control is ENABLED; treat the bot owner account as root-equivalent"
        )

    if ENABLE_LEGACY_SITE_ACTIVITY_EXPORT:
        cogs_to_load.append("cogs.site_activity_tracker")
        logger.warning(
            "Legacy site activity JSON export enabled; this directly couples Warden to the website filesystem"
        )

    # If one of these fails to import, running the bot would silently remove a
    # security or moderation control while still appearing healthy.
    critical_cogs = {
        "cogs.core",
        "cogs.security",
        "cogs.verification",
        "cogs.audit",
        "cogs.moderation",
        "cogs.action_processor",
    }

    loaded_count = 0
    failed_critical_cogs = []
    for cog in cogs_to_load:
        try:
            bot.load_extension(cog)
            loaded_count += 1
            logger.info(f"  ✅ Loaded: {cog}")
        except Exception as e:
            if cog in critical_cogs:
                failed_critical_cogs.append(cog)
                logger.critical(f"  ❌ Critical cog failed to load {cog}: {e}", exc_info=True)
            else:
                logger.warning(f"  ⚠️ Failed to load {cog}: {e}", exc_info=True)

    logger.info(f"✅ Loaded {loaded_count}/{len(cogs_to_load)} cogs")

    if failed_critical_cogs:
        logger.critical(
            "Refusing to start without critical cogs: %s",
            ", ".join(failed_critical_cogs),
        )
        sys.exit(1)

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
