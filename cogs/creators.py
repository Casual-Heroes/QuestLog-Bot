# cogs/creators.py - Creator spotlight commands for WardenBot
#
# Ported from QuestLogFluxer/cogs/creators.py (June 2026)
# Translated from Fluxer Cog pattern to discord.py slash commands.
#
# Slash commands:
#   /creator cotw   - Current Creator of the Week
#   /creator cotm   - Current Creator of the Month
#   /creator raffle - Link to this server's raffles dashboard

from datetime import datetime, timedelta, timezone

import discord
from discord import SlashCommandGroup
from discord.ext import commands
from sqlalchemy import text as sa_text

from config import db_session_scope, logger, get_debug_guilds

PROFILE_BASE   = "https://questlog.casual-heroes.com/u"
CREATORS_URL   = "https://questlog.casual-heroes.com/creators/"
DASHBOARD_BASE = "https://dashboard.casual-heroes.com/ql/dashboard/discord"

GOLD_COLOR   = 0xFEE75C
PURPLE_COLOR = 0xA855F7


def _next_monday_ts() -> int:
    now = datetime.now(timezone.utc)
    days = (7 - now.weekday()) % 7 or 7
    next_monday = (now + timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(next_monday.timestamp())


def _next_first_ts() -> int:
    now = datetime.now(timezone.utc)
    year, month = (now.year + 1, 1) if now.month == 12 else (now.year, now.month + 1)
    return int(datetime(year, month, 1, tzinfo=timezone.utc).timestamp())


_CREATOR_SELECT_PREFIX = (
        "SELECT cp.display_name, cp.bio, cp.avatar_url, cp.twitch_url, "
        "cp.youtube_url, cp.kick_url, cp.twitter_url, "
        "cp.cotw_last_featured, cp.cotm_last_featured, wu.username "
        "FROM web_creator_profiles cp "
        "JOIN web_users wu ON wu.id = cp.user_id "
)
_CURRENT_COTW_QUERY = sa_text(  # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
    # Static SQL only: no request or user-controlled text is interpolated.
    _CREATOR_SELECT_PREFIX + "WHERE cp.is_current_cotw = 1 LIMIT 1"
)
_CURRENT_COTM_QUERY = sa_text(  # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
    # Static SQL only: no request or user-controlled text is interpolated.
    _CREATOR_SELECT_PREFIX + "WHERE cp.is_current_cotm = 1 LIMIT 1"
)


def _get_creator(is_cotw: bool):
    """Fetch current cotw or cotm from DB. Returns row or None."""
    query = _CURRENT_COTW_QUERY if is_cotw else _CURRENT_COTM_QUERY
    try:
        with db_session_scope() as db:
            return db.execute(query).fetchone()
    except Exception as e:
        logger.error(f"CreatorsCog: DB error: {e}")
        return None


def _build_creator_embed(row, is_cotw: bool) -> discord.Embed:
    color = GOLD_COLOR if is_cotw else PURPLE_COLOR
    icon  = '⭐' if is_cotw else '👑'
    label = 'Creator of the Week' if is_cotw else 'Creator of the Month'
    next_ts = _next_monday_ts() if is_cotw else _next_first_ts()
    featured_ts = row[7] if is_cotw else row[8]  # cotw_last_featured or cotm_last_featured

    name = row[0] or row[9]  # display_name or username
    profile_url = f"{PROFILE_BASE}/{row[9]}/"

    links = []
    if row[3]: links.append(f"[Twitch]({row[3]})")
    if row[4]: links.append(f"[YouTube]({row[4]})")
    if row[5]: links.append(f"[Kick]({row[5]})")
    if row[6]: links.append(f"[Twitter/X]({row[6]})")

    desc = []
    if row[1]:
        bio = row[1][:200] + ('...' if len(row[1]) > 200 else '')
        desc.append(bio)
    if links:
        desc.append('**Platforms:** ' + ' | '.join(links))
    desc.append(f'\n[View Full Profile]({profile_url})')

    embed = discord.Embed(
        title=f'{icon} {label} - {name}',
        description='\n'.join(desc),
        color=color,
        url=profile_url,
    )
    if row[2]:
        embed.set_thumbnail(url=row[2])
    if featured_ts:
        embed.add_field(name='Featured Since', value=f'<t:{featured_ts}:D>', inline=True)
    embed.add_field(name='Next Rotation', value=f'<t:{next_ts}:R>', inline=True)
    embed.set_footer(text=f'QuestLog Creators - {CREATORS_URL}')
    return embed


class CreatorsCog(commands.Cog):
    """Creator spotlight commands."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    creator = SlashCommandGroup(
        'creator',
        'Creator spotlight commands',
        guild_ids=get_debug_guilds(),
    )

    @creator.command(name='cotw', description='Show the current Creator of the Week')
    async def slash_cotw(self, ctx: discord.ApplicationContext):
        row = _get_creator(is_cotw=True)
        if not row:
            embed = discord.Embed(
                title='⭐ Creator of the Week',
                description=f'No Creator of the Week selected yet. Check back soon!\n\n[Browse Creators]({CREATORS_URL})',
                color=GOLD_COLOR,
            )
            embed.add_field(name='Next Rotation', value=f'<t:{_next_monday_ts()}:R>', inline=False)
        else:
            embed = _build_creator_embed(row, is_cotw=True)
        await ctx.respond(embed=embed)

    @creator.command(name='cotm', description='Show the current Creator of the Month')
    async def slash_cotm(self, ctx: discord.ApplicationContext):
        row = _get_creator(is_cotw=False)
        if not row:
            embed = discord.Embed(
                title='👑 Creator of the Month',
                description=f'No Creator of the Month selected yet. Check back soon!\n\n[Browse Creators]({CREATORS_URL})',
                color=PURPLE_COLOR,
            )
            embed.add_field(name='Next Rotation', value=f'<t:{_next_first_ts()}:R>', inline=False)
        else:
            embed = _build_creator_embed(row, is_cotw=False)
        await ctx.respond(embed=embed)

    @creator.command(name='raffle', description="Link to this server's raffles dashboard")
    async def slash_raffle(self, ctx: discord.ApplicationContext):
        url = f"{DASHBOARD_BASE}/{ctx.guild.id}/raffles/"
        embed = discord.Embed(
            title='🎟️ Raffles',
            description=(
                f"Browse active raffles, enter to win, and manage past draws for **{ctx.guild.name}**.\n\n"
                f"[**Open Raffles Dashboard**]({url})"
            ),
            color=GOLD_COLOR,
        )
        embed.set_footer(text='QuestLog - questlog.casual-heroes.com')
        await ctx.respond(embed=embed)


def setup(bot: commands.Bot):
    bot.add_cog(CreatorsCog(bot))
