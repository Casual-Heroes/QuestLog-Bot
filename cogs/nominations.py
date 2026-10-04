# cogs/nominations.py - Community Spotlight: Monthly nominations
#
# Ported from QuestLogFluxer/cogs/nominations.py (June 2026)
# Translated from Fluxer Cog pattern to discord.py slash commands.
# Key difference: uses discord_id (not fluxer_id) for user lookups,
# uses Guild.spotlight_channel_id from guilds table,
# slash commands instead of ! prefix.
#
# Monthly schedule (UTC):
#   1st  - nominations open announcement
#   20th - reminder with current top nominees
#   26th - close nominations, post voting embed
#   Last day - tally votes, call web API, announce winners
#
# Slash commands:
#   /nominate @user reason  - nominate for Most Helpful (community category)
#   /nominations            - show current nominees

import asyncio
import calendar
import datetime
import time

import discord
import requests
from discord import SlashCommandGroup
from discord.ext import commands
from sqlalchemy import text as sa_text

from config import (
    db_session_scope,
    logger,
    QUESTLOG_INTERNAL_API_URL,
    QUESTLOG_BOT_SECRET,
    get_debug_guilds,
)

CHECK_INTERVAL = 3600  # check once per hour

CATEGORIES = [
    {'key': 'community', 'label': 'Most Helpful',        'points': 15},
    {'key': 'lfg_host',  'label': 'Best LFG Host',       'points': 12},
    {'key': 'build',     'label': 'Most Creative Build',  'points': 12},
    {'key': '7dtd',      'label': '7DTD MVP',             'points': 10},
    {'key': 'valheim',   'label': 'Valheim Wanderer',     'points': 10},
    {'key': 'minecraft', 'label': 'Minecraft Builder',    'points': 10},
    {'key': 'dayz',      'label': 'DayZ Survivor',        'points': 10},
    {'key': 'palworld',  'label': 'Palworld Tamer',       'points': 10},
]

NOMINATION_COLOR  = 0xEAB308
REMINDER_COLOR    = 0xF97316
VOTING_COLOR      = 0x8B5CF6


def _last_day_of_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def _get_guilds_with_spotlight() -> list[dict]:
    """Return guilds that have a spotlight_channel_id configured."""
    try:
        with db_session_scope() as db:
            rows = db.execute(
                sa_text(
                    "SELECT guild_id, spotlight_channel_id FROM guilds "
                    "WHERE spotlight_channel_id IS NOT NULL AND bot_present = 1"
                )
            ).fetchall()
            return [{'guild_id': r[0], 'channel_id': r[1]} for r in rows]
    except Exception as e:
        logger.error(f"NominationsCog: failed to load spotlight guilds: {e}")
        return []


def _top_nominees(month_year: str, category: str, limit: int = 5) -> list[dict]:
    """Return top nominees for a category this month sorted by nomination count."""
    try:
        with db_session_scope() as db:
            rows = db.execute(
                sa_text(
                    "SELECT n.nominated_user_id, u.username, COUNT(*) as cnt "
                    "FROM web_legacy_nominations n "
                    "JOIN web_users u ON u.id = n.nominated_user_id "
                    "WHERE n.month_year = :my AND n.category = :cat "
                    "GROUP BY n.nominated_user_id, u.username "
                    "ORDER BY cnt DESC LIMIT :lim"
                ),
                {'my': month_year, 'cat': category, 'lim': limit}
            ).fetchall()
            return [{'user_id': r[0], 'username': r[1], 'count': r[2]} for r in rows]
    except Exception as e:
        logger.error(f"NominationsCog: top_nominees failed: {e}")
        return []


def _resolve_web_user_id(discord_user_id: int) -> int | None:
    """Map Discord user ID to web_users.id via discord_id column."""
    try:
        with db_session_scope() as db:
            row = db.execute(
                sa_text("SELECT id FROM web_users WHERE discord_id = :did AND is_banned = 0 LIMIT 1"),
                {'did': str(discord_user_id)}
            ).fetchone()
            return row[0] if row else None
    except Exception:
        return None


def _save_nomination(month_year: str, category: str,
                     nominated_user_id: int, nominated_by_discord_id: int,
                     guild_id: int, reason: str) -> bool:
    """Upsert a nomination. Returns True on success."""
    now = int(time.time())
    try:
        with db_session_scope() as db:
            existing = db.execute(
                sa_text(
                    "SELECT id FROM web_legacy_nominations "
                    "WHERE month_year = :my AND category = :cat "
                    "AND nominated_by_discord_id = :did LIMIT 1"
                ),
                {'my': month_year, 'cat': category, 'did': str(nominated_by_discord_id)}
            ).fetchone()

            if existing:
                db.execute(
                    sa_text(
                        "UPDATE web_legacy_nominations SET nominated_user_id = :uid, "
                        "reason = :reason, updated_at = :now WHERE id = :id"
                    ),
                    {'uid': nominated_user_id, 'reason': reason[:500], 'now': now, 'id': existing[0]}
                )
            else:
                db.execute(
                    sa_text(
                        "INSERT INTO web_legacy_nominations "
                        "(month_year, category, nominated_user_id, nominated_by_discord_id, "
                        "guild_id, platform, reason, created_at, updated_at) "
                        "VALUES (:my, :cat, :uid, :did, :gid, 'discord', :reason, :now, :now)"
                    ),
                    {
                        'my': month_year, 'cat': category, 'uid': nominated_user_id,
                        'did': str(nominated_by_discord_id), 'gid': str(guild_id),
                        'reason': reason[:500], 'now': now,
                    }
                )
            db.commit()
        return True
    except Exception as e:
        logger.error(f"NominationsCog: save_nomination failed: {e}")
        return False


def _call_close_nominations(month_year: str) -> dict:
    """POST to QuestLog web API to tally votes and award winners."""
    try:
        url = f"{QUESTLOG_INTERNAL_API_URL}/api/internal/close-nominations/"
        resp = requests.post(
            url,
            json={'month_year': month_year},
            headers={'X-Bot-Secret': QUESTLOG_BOT_SECRET, 'Content-Type': 'application/json'},
            timeout=15,
        )
        return resp.json() if resp.ok else {'error': resp.text[:200]}
    except Exception as e:
        logger.error(f"NominationsCog: close_nominations API failed: {e}")
        return {'error': str(e)}


class NominationsCog(commands.Cog):
    """Monthly community spotlight nominations and awards."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._task: asyncio.Task | None = None
        self._last_fired: dict[str, int] = {}

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info("NominationsCog ready - starting monthly loop")
        if self._task is None or self._task.done():
            self._task = asyncio.ensure_future(self._monthly_loop())

    # ------------------------------------------------------------------
    # Background loop
    # ------------------------------------------------------------------

    async def _monthly_loop(self):
        await asyncio.sleep(30)
        while True:
            try:
                await self._check_monthly_events()
            except Exception as e:
                logger.error(f"NominationsCog: loop error: {e}")
            await asyncio.sleep(CHECK_INTERVAL)

    async def _check_monthly_events(self):
        now = datetime.datetime.utcnow()
        day = now.day
        month_year = now.strftime('%Y-%m')
        last_day = _last_day_of_month(now.year, now.month)

        guilds = await asyncio.to_thread(_get_guilds_with_spotlight)
        if not guilds:
            return

        for g in guilds:
            channel_id = g['channel_id']
            guild_id = g['guild_id']
            fire_key = f"{guild_id}:{month_year}:{day}"

            if self._last_fired.get(fire_key):
                continue

            channel = self.bot.get_channel(int(channel_id))
            if not channel:
                continue

            if day == 1:
                await self._post_nominations_open(channel, month_year)
                self._last_fired[fire_key] = 1
            elif day == 20:
                await self._post_nominations_reminder(channel, month_year)
                self._last_fired[fire_key] = 1
            elif day == 26:
                await self._post_voting_poll(channel, month_year)
                self._last_fired[fire_key] = 1
            elif day == last_day:
                await self._close_and_announce(channel, guild_id, month_year)
                self._last_fired[fire_key] = 1

        if len(self._last_fired) > 500:
            self._last_fired = {}

    # ------------------------------------------------------------------
    # Monthly event posts
    # ------------------------------------------------------------------

    async def _post_nominations_open(self, channel: discord.TextChannel, month_year: str):
        embed = discord.Embed(
            title='Community Spotlight - Nominations Open!',
            description=(
                "It's the start of a new month - time to recognize your community heroes!\n\n"
                "**Nominate someone** who helped you, built something awesome, or made the server better.\n\n"
                "Use `/nominations nominate @user reason` to nominate someone.\n\n"
                "Nominations close on the **25th**. Voting begins on the **26th**."
            ),
            color=NOMINATION_COLOR,
        )
        embed.set_footer(text=f"Month: {month_year}")
        try:
            await channel.send(embed=embed)
        except Exception as e:
            logger.error(f"NominationsCog: post_nominations_open failed: {e}")

    async def _post_nominations_reminder(self, channel: discord.TextChannel, month_year: str):
        lines = []
        for cat in CATEGORIES:
            nominees = await asyncio.to_thread(_top_nominees, month_year, cat['key'], 3)
            if nominees:
                names = ', '.join(n['username'] for n in nominees)
                lines.append(f"**{cat['label']}:** {names}")
            else:
                lines.append(f"**{cat['label']}:** No nominations yet")

        embed = discord.Embed(
            title='Nominations Reminder - 5 Days Left!',
            description=(
                "Nominations close in 5 days. Current standings:\n\n"
                + '\n'.join(lines)
                + "\n\nNominate via `/nominations nominate @user reason`"
            ),
            color=REMINDER_COLOR,
        )
        embed.set_footer(text=f"Month: {month_year}")
        try:
            await channel.send(embed=embed)
        except Exception as e:
            logger.error(f"NominationsCog: post_reminder failed: {e}")

    async def _post_voting_poll(self, channel: discord.TextChannel, month_year: str):
        emoji_nums = ['1⃣', '2⃣', '3⃣', '4⃣', '5⃣']
        lines = ['Nominations are closed! React to vote for your favorites:\n']

        for cat in CATEGORIES:
            nominees = await asyncio.to_thread(_top_nominees, month_year, cat['key'], 5)
            if not nominees:
                continue
            lines.append(f"**{cat['label']}**")
            for i, n in enumerate(nominees):
                lines.append(f"{emoji_nums[i]} {n['username']} ({n['count']} nominations)")
            lines.append('')

        if len(lines) == 1:
            lines.append('No nominations received this month.')

        embed = discord.Embed(
            title='Community Spotlight - Voting Open!',
            description='\n'.join(lines),
            color=VOTING_COLOR,
        )
        embed.set_footer(text=f"Voting closes end of month | {month_year}")
        try:
            await channel.send(embed=embed)
        except Exception as e:
            logger.error(f"NominationsCog: post_voting_poll failed: {e}")

    async def _close_and_announce(self, channel: discord.TextChannel, guild_id: int, month_year: str):
        result = await asyncio.to_thread(_call_close_nominations, month_year)
        if 'error' in result:
            logger.error(f"NominationsCog: close_nominations error for {guild_id}: {result['error']}")
            return

        winners = [r for r in result.get('results', []) if r.get('winner_id')]
        if not winners:
            embed = discord.Embed(
                title=f'Community Spotlight - {month_year}',
                description='No nominations were received this month. Nominate someone next month!',
                color=0x6B7280,
            )
        else:
            lines = []
            for w in winners:
                cat_label = next((c['label'] for c in CATEGORIES if c['key'] == w['category']), w['category'])
                username = w.get('username') or f"User #{w['winner_id']}"
                lines.append(f"**{cat_label}:** {username}")

            embed = discord.Embed(
                title=f'Community Spotlight Winners - {month_year}',
                description=(
                    "Congratulations to this month's Community Spotlight winners! "
                    "Legacy points have been awarded.\n\n"
                    + '\n'.join(lines)
                    + "\n\nSee their profiles at https://questlog.casual-heroes.com/"
                ),
                color=NOMINATION_COLOR,
            )
            embed.set_footer(text="Nominations for next month open on the 1st")

        try:
            await channel.send(embed=embed)
        except Exception as e:
            logger.error(f"NominationsCog: close_and_announce failed: {e}")

    # ------------------------------------------------------------------
    # Slash commands
    # ------------------------------------------------------------------

    nominations_group = SlashCommandGroup(
        "nominations",
        "Community spotlight nominations",
        guild_ids=get_debug_guilds(),
    )

    @nominations_group.command(name="nominate", description="Nominate a member for Community Most Helpful")
    async def slash_nominate(
        self,
        ctx: discord.ApplicationContext,
        member: discord.Option(discord.Member, "Member to nominate"),
        reason: discord.Option(str, "Why are you nominating them?", max_length=500),
    ):
        now = datetime.datetime.utcnow()
        if now.day > 25:
            await ctx.respond("Nominations are closed for this month. Voting is now open!", ephemeral=True)
            return

        if member.id == ctx.author.id:
            await ctx.respond("You can't nominate yourself!", ephemeral=True)
            return

        if member.bot:
            await ctx.respond("You can't nominate a bot.", ephemeral=True)
            return

        nominated_web_id = await asyncio.to_thread(_resolve_web_user_id, member.id)
        if not nominated_web_id:
            await ctx.respond(
                f"{member.display_name} doesn't have a linked QuestLog account. "
                "They need to connect their Discord at questlog.casual-heroes.com/settings/",
                ephemeral=True
            )
            return

        month_year = now.strftime('%Y-%m')
        ok = await asyncio.to_thread(
            _save_nomination,
            month_year,
            'community',
            nominated_web_id,
            ctx.author.id,
            ctx.guild.id,
            reason,
        )

        if ok:
            await ctx.respond(
                f"Nomination submitted for **{member.display_name}**! "
                "You can update it anytime before the 25th.",
                ephemeral=True
            )
        else:
            await ctx.respond("Failed to save nomination. Please try again.", ephemeral=True)

    @nominations_group.command(name="list", description="Show current month's top nominees")
    async def slash_nominations(self, ctx: discord.ApplicationContext):
        month_year = datetime.datetime.utcnow().strftime('%Y-%m')
        lines = []
        for cat in CATEGORIES:
            nominees = await asyncio.to_thread(_top_nominees, month_year, cat['key'], 3)
            if nominees:
                names = ', '.join(f"{n['username']} ({n['count']})" for n in nominees)
                lines.append(f"**{cat['label']}:** {names}")
            else:
                lines.append(f"**{cat['label']}:** No nominations yet")

        embed = discord.Embed(
            title=f'Current Nominees - {month_year}',
            description='\n'.join(lines) + "\n\nNominate: `/nominations nominate @user reason`",
            color=NOMINATION_COLOR,
        )
        await ctx.respond(embed=embed)


def setup(bot: commands.Bot):
    bot.add_cog(NominationsCog(bot))
