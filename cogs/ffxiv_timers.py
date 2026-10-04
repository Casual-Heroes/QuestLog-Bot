# cogs/ffxiv_timers.py - FFXIV Timer Alerts for WardenBot
#
# Ported from QuestLogFluxer/cogs/ffxiv_timers.py (June 2026)
# Translated from Fluxer Cog to discord.py. Key differences:
# - Config read from guilds table (ffxiv_*_channel_id / ffxiv_*_enabled columns)
# - Uses channel.send() with discord.Embed instead of bot._http.send_message with dict embeds
# - No discord_webhook_url fallback needed (this IS the Discord bot)
#
# Three alert types per guild:
#   1. Gathering nodes  - warning before unspoiled/legendary nodes spawn (Eorzea time)
#   2. Ocean Fishing    - registration-open alert 45 min before each 2-hour window
#   3. Daily/Weekly resets - 15:00 UTC daily, Tuesday 08:00 UTC weekly

import asyncio
import json
import os
import time
from datetime import datetime, timezone, timedelta

import discord
from discord.ext import commands
from sqlalchemy import text as sa_text

from config import db_session_scope, logger

# ---------------------------------------------------------------------------
# Eorzea time constants
# ---------------------------------------------------------------------------
EORZEA_DAY_SECONDS = 4200  # 70 real minutes = 1 Eorzea day

def _eorzea_time(unix_ts=None):
    if unix_ts is None:
        unix_ts = time.time()
    et_minutes_total = (unix_ts / EORZEA_DAY_SECONDS) * 1440
    et_minutes_today = et_minutes_total % 1440
    return int(et_minutes_today // 60), int(et_minutes_today % 60)

def _et_minutes_since_midnight(unix_ts=None):
    if unix_ts is None:
        unix_ts = time.time()
    return ((unix_ts / EORZEA_DAY_SECONDS) * 1440) % 1440

# ---------------------------------------------------------------------------
# Ocean Fishing constants
# ---------------------------------------------------------------------------
_OF_EPOCH_ANCHOR    = 1593302400
_OF_WINDOW_DURATION = 7200
_OF_REG_OFFSET      = 2700  # 45 min before departure

_OF_ROUTES = [
    {'name': 'Indigo Route', 'color': 0x5865F2, 'stops': ['Bloodbrine Sea', 'Rothlyt Sound', 'Northern Strait of Merlthor'], 'times': ['Day', 'Sunset', 'Night'],     'blue_fish': ['Hafgufa', 'Seafaring Toad']},
    {'name': 'Ruby Route',   'color': 0xED4245, 'stops': ['Rhotano Sea',    'Cieldalaes Margin', 'Galadion Bay'],            'times': ['Night', 'Day', 'Evening'],    'blue_fish': ['Stonescale']},
    {'name': 'Indigo Route', 'color': 0x5865F2, 'stops': ['Bloodbrine Sea', 'Rothlyt Sound', 'Northern Strait of Merlthor'], 'times': ['Night', 'Day', 'Sunset'],     'blue_fish': ['Hafgufa', 'Seafaring Toad']},
    {'name': 'Ruby Route',   'color': 0xED4245, 'stops': ['Rhotano Sea',    'Cieldalaes Margin', 'Galadion Bay'],            'times': ['Evening', 'Night', 'Day'],    'blue_fish': ['Stonescale']},
    {'name': 'Indigo Route', 'color': 0x5865F2, 'stops': ['Bloodbrine Sea', 'Rothlyt Sound', 'Northern Strait of Merlthor'], 'times': ['Sunset', 'Night', 'Day'],     'blue_fish': ['Hafgufa', 'Seafaring Toad']},
    {'name': 'Ruby Route',   'color': 0xED4245, 'stops': ['Rhotano Sea',    'Cieldalaes Margin', 'Galadion Bay'],            'times': ['Day', 'Evening', 'Night'],    'blue_fish': ['Stonescale']},
    {'name': 'Indigo Route', 'color': 0x5865F2, 'stops': ['Bloodbrine Sea', 'Rothlyt Sound', 'Northern Strait of Merlthor'], 'times': ['Day', 'Night', 'Sunset'],     'blue_fish': ['Hafgufa', 'Seafaring Toad']},
    {'name': 'Ruby Route',   'color': 0xED4245, 'stops': ['Rhotano Sea',    'Cieldalaes Margin', 'Galadion Bay'],            'times': ['Sunset', 'Day', 'Evening'],   'blue_fish': ['Stonescale']},
]

def _current_of_window_num(ts=None):
    return int(((ts or time.time()) - _OF_EPOCH_ANCHOR) // _OF_WINDOW_DURATION)

def _of_window_start(n):
    return _OF_EPOCH_ANCHOR + n * _OF_WINDOW_DURATION

# ---------------------------------------------------------------------------
# Gathering nodes
# ---------------------------------------------------------------------------
_NODES_PATH = '/srv/ch-webserver/app/static/data/ffxiv_gathering_nodes.json'
TIMED_TYPES = {'Unspoiled', 'Legendary', 'Ephemeral'}

JOB_COLORS = {'BTN': 0x57C75E, 'MIN': 0xA8A8A8, 'FSH': 0x5B9BD5}
JOB_ICONS  = {'BTN': '🌿', 'MIN': '⛏️', 'FSH': '🎣'}
COLOR_RESETS = 0xAB8BFF

def _load_nodes():
    try:
        with open(_NODES_PATH) as f:
            return [n for n in json.load(f) if n.get('limitType') in TIMED_TYPES and n.get('times')]
    except Exception:
        logger.warning("FFXIVTimers: could not load ffxiv_gathering_nodes.json")
        return []

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_configured_guilds(feature: str) -> list[dict]:
    """Return guilds with a given FFXIV feature enabled."""
    queries = {
        'gathering': (
            "SELECT guild_id, ffxiv_gathering_channel_id FROM guilds "
            "WHERE ffxiv_gathering_enabled = 1 "
            "AND ffxiv_gathering_channel_id IS NOT NULL AND bot_present = 1"
        ),
        'ocean': (
            "SELECT guild_id, ffxiv_ocean_channel_id FROM guilds "
            "WHERE ffxiv_ocean_enabled = 1 "
            "AND ffxiv_ocean_channel_id IS NOT NULL AND bot_present = 1"
        ),
        'resets': (
            "SELECT guild_id, ffxiv_resets_channel_id FROM guilds "
            "WHERE ffxiv_resets_enabled = 1 "
            "AND ffxiv_resets_channel_id IS NOT NULL AND bot_present = 1"
        ),
    }
    query = queries.get(feature)
    if query is None:
        logger.warning("FFXIVTimers: rejected unknown feature %r", feature)
        return []
    try:
        with db_session_scope() as db:
            rows = db.execute(sa_text(query)).fetchall()
            return [{'guild_id': r[0], 'channel_id': r[1]} for r in rows]
    except Exception as e:
        logger.error(f"FFXIVTimers: DB error for {feature}: {e}")
        return []

# ---------------------------------------------------------------------------
# Cog
# ---------------------------------------------------------------------------

class FFXIVTimersCog(commands.Cog):
    """FFXIV gathering, ocean fishing, and reset alerts."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._t_gathering = None
        self._t_ocean     = None
        self._t_resets    = None

    @commands.Cog.listener()
    async def on_ready(self):
        if self._t_gathering is None or self._t_gathering.done():
            self._t_gathering = asyncio.ensure_future(self._loop_gathering())
        if self._t_ocean is None or self._t_ocean.done():
            self._t_ocean = asyncio.ensure_future(self._loop_ocean())
        if self._t_resets is None or self._t_resets.done():
            self._t_resets = asyncio.ensure_future(self._loop_resets())
        logger.info("FFXIVTimers: all loops started")

    async def _send(self, channel_id: int, embed: discord.Embed):
        channel = self.bot.get_channel(channel_id)
        if channel:
            try:
                await channel.send(embed=embed)
            except Exception as e:
                logger.warning(f"FFXIVTimers: send to {channel_id} failed: {e}")

    # ------------------------------------------------------------------
    # Gathering loop
    # ------------------------------------------------------------------

    async def _loop_gathering(self):
        await asyncio.sleep(10)
        nodes = _load_nodes()
        logger.info(f"FFXIVTimers: gathering loop - {len(nodes)} timed nodes")
        while True:
            try:
                await self._check_gathering(nodes)
            except Exception as e:
                logger.error(f"FFXIVTimers: gathering error: {e}")
            await asyncio.sleep(30)

    async def _check_gathering(self, nodes):
        guilds = _get_configured_guilds('gathering')
        if not guilds:
            return

        now = time.time()
        et_min = _et_minutes_since_midnight(now)
        warn_min = 5

        for node in nodes:
            for spawn_et_hour in node['times']:
                spawn_et = spawn_et_hour * 60
                uptime = node.get('uptime', 180)
                warn_et = (spawn_et - warn_min) % 1440

                if warn_et <= et_min < warn_et + 1:
                    embed = self._gathering_warn_embed(node, spawn_et_hour, warn_min)
                    for g in guilds:
                        await self._send(g['channel_id'], embed)

                if int(et_min) == spawn_et:
                    embed = self._gathering_active_embed(node, spawn_et_hour, uptime)
                    for g in guilds:
                        await self._send(g['channel_id'], embed)

    def _gathering_warn_embed(self, node, spawn_et_hour, warn_min) -> discord.Embed:
        job = node.get('job', '')
        color = JOB_COLORS.get(job, 0x57C75E)
        icon = JOB_ICONS.get(job, '⏰')
        items = '\n'.join(f'- {i}' for i in node.get('items', [])) or 'Unknown'
        coords = node.get('coords', [])
        coords_str = f"({coords[0]}, {coords[1]})" if len(coords) == 2 else 'N/A'
        e = discord.Embed(
            title=f'{icon} Node Spawning Soon - {node["name"]}',
            description=f'**{warn_min} minutes** until this node spawns.\nSpawns at **ET {spawn_et_hour:02d}:00**',
            color=color,
            url='https://questlog.casual-heroes.com/ffxiv/tools/gathering/',
        )
        e.add_field(name='Zone', value=node.get('zone', 'Unknown'), inline=True)
        e.add_field(name='Job / Level', value=f'{job} Lv{node.get("lvl", "?")}', inline=True)
        e.add_field(name='Coordinates', value=coords_str, inline=True)
        e.add_field(name='Items', value=items, inline=False)
        e.set_footer(text=f'{node.get("limitType", "")} Node')
        return e

    def _gathering_active_embed(self, node, spawn_et_hour, uptime_min) -> discord.Embed:
        job = node.get('job', '')
        color = JOB_COLORS.get(job, 0x57C75E)
        icon = JOB_ICONS.get(job, '🌿')
        items = '\n'.join(f'- {i}' for i in node.get('items', [])) or 'Unknown'
        coords = node.get('coords', [])
        coords_str = f"({coords[0]}, {coords[1]})" if len(coords) == 2 else 'N/A'
        real_min = int(uptime_min * EORZEA_DAY_SECONDS / 1440 / 60)
        e = discord.Embed(
            title=f'{icon} Node Now Active - {node["name"]}',
            description=f'This node is **active now!** You have ~{real_min} real minutes.',
            color=color,
            url='https://questlog.casual-heroes.com/ffxiv/tools/gathering/',
        )
        e.add_field(name='Zone', value=node.get('zone', 'Unknown'), inline=True)
        e.add_field(name='Job / Level', value=f'{job} Lv{node.get("lvl", "?")}', inline=True)
        e.add_field(name='Coordinates', value=coords_str, inline=True)
        e.add_field(name='Items', value=items, inline=False)
        e.set_footer(text=f'{node.get("limitType", "")} Node - ET {spawn_et_hour:02d}:00 for {uptime_min} ET min')
        return e

    # ------------------------------------------------------------------
    # Ocean Fishing loop
    # ------------------------------------------------------------------

    async def _loop_ocean(self):
        await asyncio.sleep(20)
        logger.info("FFXIVTimers: ocean fishing loop started")
        while True:
            try:
                await self._check_ocean()
            except Exception as e:
                logger.error(f"FFXIVTimers: ocean error: {e}")
            await asyncio.sleep(60)

    async def _check_ocean(self):
        guilds = _get_configured_guilds('ocean')
        if not guilds:
            return

        now = time.time()
        next_wnum = _current_of_window_num(now) + 1
        next_start = _of_window_start(next_wnum)
        reg_opens = next_start - _OF_REG_OFFSET

        if reg_opens <= now < reg_opens + 60:
            route = _OF_ROUTES[next_wnum % 8]
            embed = self._ocean_embed(route, next_start)
            for g in guilds:
                await self._send(g['channel_id'], embed)

    def _ocean_embed(self, route, departure_ts) -> discord.Embed:
        dep_dt = datetime.fromtimestamp(departure_ts, tz=timezone.utc)
        stops = '\n'.join(f'{s} ({t})' for s, t in zip(route['stops'], route['times']))
        blue = ', '.join(route.get('blue_fish', [])) or 'None this run'
        e = discord.Embed(
            title='🎣 Ocean Fishing - Registration Now Open!',
            description=(
                f"**{route['name']}** departing **{dep_dt.strftime('%b %d at %H:%M UTC')}**\n"
                "Registration closes at departure. Board at Limsa Lominsa Lower Decks."
            ),
            color=route['color'],
            url='https://questlog.casual-heroes.com/ffxiv/tools/ocean-fishing/',
        )
        e.add_field(name='Stops', value=stops, inline=False)
        e.add_field(name='Blue / Achievement Fish', value=blue, inline=False)
        e.set_footer(text='questlog.casual-heroes.com/ffxiv/tools/ocean-fishing/')
        return e

    # ------------------------------------------------------------------
    # Resets loop
    # ------------------------------------------------------------------

    async def _loop_resets(self):
        await asyncio.sleep(30)
        logger.info("FFXIVTimers: resets loop started")
        while True:
            try:
                await self._check_resets()
            except Exception as e:
                logger.error(f"FFXIVTimers: resets error: {e}")
            await asyncio.sleep(60)

    async def _check_resets(self):
        guilds = _get_configured_guilds('resets')
        if not guilds:
            return

        now = datetime.now(tz=timezone.utc)
        is_daily  = (now.hour == 15 and now.minute == 0)
        is_weekly = (now.weekday() == 1 and now.hour == 8 and now.minute == 0)

        if is_daily:
            embed = self._daily_reset_embed(now)
            for g in guilds:
                await self._send(g['channel_id'], embed)

        if is_weekly:
            embed = self._weekly_reset_embed(now)
            for g in guilds:
                await self._send(g['channel_id'], embed)

    def _daily_reset_embed(self, dt) -> discord.Embed:
        e = discord.Embed(
            title='🔄 Daily Reset',
            description=f"**{dt.strftime('%A, %B %d')}** - Daily tasks have reset.",
            color=COLOR_RESETS,
            url='https://questlog.casual-heroes.com/ffxiv/tools/resets/',
        )
        e.add_field(name='Resets Now', value=(
            '- Duty Roulettes\n'
            '- Beast Tribe Quests\n'
            '- Wondrous Tails stamps\n'
            '- Custom Deliveries\n'
            '- Cosmic Exploration (09:00 UTC)\n'
            '- GC Daily Supply & Provisioning (20:00 UTC)'
        ), inline=False)
        e.set_footer(text='questlog.casual-heroes.com/ffxiv/tools/resets/')
        return e

    def _weekly_reset_embed(self, dt) -> discord.Embed:
        e = discord.Embed(
            title='📅 Weekly Reset',
            description=f"**{dt.strftime('%A, %B %d')}** - Weekly tasks have reset.",
            color=COLOR_RESETS,
            url='https://questlog.casual-heroes.com/ffxiv/tools/resets/',
        )
        e.add_field(name='Resets Now', value=(
            '- Normal Raids (4 turns)\n'
            '- Alliance Raid\n'
            '- Trials (Extreme)\n'
            '- Savage & Ultimate Raids\n'
            '- Weekly Challenge Log\n'
            '- Hunt Elite Mark Bills\n'
            '- Wondrous Tails book exchange\n'
            '- Tomestone weekly cap reset'
        ), inline=False)
        e.set_footer(text='questlog.casual-heroes.com/ffxiv/tools/resets/')
        return e


def setup(bot: commands.Bot):
    bot.add_cog(FFXIVTimersCog(bot))
