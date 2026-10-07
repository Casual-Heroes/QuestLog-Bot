# cogs/soulmask.py - Soulmask cluster server management for WardenBot
#
# Ported from QuestLogFluxer/cogs/soulmask.py (June 2026)
# Translated from Fluxer Cog to discord.py slash commands.
# Key differences:
# - Uses channel.send() / ctx.respond() instead of bot._http.send_message
# - Slash commands instead of ! prefix
# - Announce channel from guilds.soulmask_announce_channel_id
#
# Manages Soulmask instances via AMP RCON:
#   SoulQuest-SunkenThrone01 (Server B) - end-game zone with rotating modes
#   SoulQuest01 (Server C)              - Verdant Wilds login server
#
# Config: data/soulmask/schedule.json (same file as Fluxer uses)

import asyncio
import datetime
import json
import logging
import os
from pathlib import Path

import discord
from discord import SlashCommandGroup
from discord.ext import commands
from sqlalchemy import text as sa_text

from config import db_session_scope, logger, get_debug_guilds

# ---- AMP credentials ----
AMP_URL      = os.getenv('AMP_URL', '')
AMP_USER     = os.getenv('AMP_USER', '')
AMP_PASSWORD = os.getenv('AMP_PASSWORD', '')

# ---- Instance names ----
INSTANCE_SUNKEN  = os.getenv('SOULMASK_INSTANCE_B', 'SoulQuest-SunkenThrone01')
INSTANCE_VERDANT = os.getenv('SOULMASK_INSTANCE_C', 'SoulQuest01')

# ---- Config path (shared with Fluxer) ----
_DATA_DIR     = Path(os.getenv('SOULMASK_DATA_DIR', '/mnt/gamestoreage2/DiscordBots/questlogfluxer/data/soulmask'))
SCHEDULE_FILE = _DATA_DIR / 'schedule.json'

SOULMASK_COLOR = 0xB8860B

# Suppress noisy AMP logs
logging.getLogger('ampapi').setLevel(logging.CRITICAL)

_SHORT_MAP = {
    'sunken':  INSTANCE_SUNKEN,
    'verdant': INSTANCE_VERDANT,
    'b':       INSTANCE_SUNKEN,
    'c':       INSTANCE_VERDANT,
}


# ---------------------------------------------------------------------------
# Schedule helpers (identical logic to Fluxer)
# ---------------------------------------------------------------------------

def _load_schedule() -> dict:
    if not SCHEDULE_FILE.exists():
        return {}
    try:
        return json.loads(SCHEDULE_FILE.read_text())
    except Exception as e:
        logger.error(f'SoulmaskCog: failed to load schedule.json: {e}')
        return {}


def _active_mode_for_day(instance_cfg: dict, day_name: str) -> str:
    for entry in instance_cfg.get('schedule', []):
        if day_name in entry.get('days', []):
            return entry['mode']
    return instance_cfg.get('default_mode', 'pve')


def _next_mode_change(instance_cfg: dict, now: datetime.datetime):
    candidates = []
    for entry in instance_cfg.get('schedule', []):
        mode = entry['mode']
        start_hour = entry.get('start_hour', 0)
        for i in range(7):
            candidate_day = now + datetime.timedelta(days=i)
            if candidate_day.strftime('%A') in entry.get('days', []):
                candidate_dt = candidate_day.replace(hour=start_hour, minute=0, second=0, microsecond=0)
                if candidate_dt > now:
                    candidates.append((mode, candidate_dt))
    if not candidates:
        return None, None
    candidates.sort(key=lambda x: x[1])
    return candidates[0]


# ---------------------------------------------------------------------------
# AMP / RCON helpers (identical logic to Fluxer)
# ---------------------------------------------------------------------------

async def _get_amp_instance(instance_name: str):
    try:
        from ampapi.dataclass import APIParams
        from ampapi.bridge import Bridge
        from ampapi.controller import AMPControllerInstance as _AMPCtrl
        Bridge(api_params=APIParams(url=AMP_URL, user=AMP_USER, password=AMP_PASSWORD))
        ctrl = _AMPCtrl()
        await ctrl.get_instances()
        return next((i for i in ctrl.instances if i.instance_name == instance_name), None)
    except Exception as e:
        logger.warning(f'SoulmaskCog: _get_amp_instance({instance_name}) failed: {e}')
        return None


async def _set_coefficient(instance_name: str, key: str, value) -> bool:
    try:
        instance = await _get_amp_instance(instance_name)
        if instance:
            await instance.send_console_message(f'Set_Coefficient {key} {value}')
            return True
        return False
    except Exception as e:
        logger.warning(f'SoulmaskCog: RCON failed [{instance_name}] Set_Coefficient {key}: {e}')
        return False


async def _apply_mode(instance_name: str, mode: dict) -> tuple[int, int]:
    ok = fail = 0
    for key, value in mode.get('coefficients', {}).items():
        if await _set_coefficient(instance_name, key, value):
            ok += 1
        else:
            fail += 1
        await asyncio.sleep(0.25)
    return ok, fail


async def _reset_to_baseline(instance_name: str, instance_cfg: dict) -> tuple[int, int]:
    all_keys = set()
    for mode in instance_cfg.get('modes', {}).values():
        all_keys.update(mode.get('coefficients', {}).keys())
    ok = fail = 0
    for key in all_keys:
        if await _set_coefficient(instance_name, key, 1):
            ok += 1
        else:
            fail += 1
        await asyncio.sleep(0.25)
    return ok, fail


# ---------------------------------------------------------------------------
# Embed helpers
# ---------------------------------------------------------------------------

def _mode_embed(instance_name: str, mode_name: str, mode: dict, action: str = 'Active') -> discord.Embed:
    label = mode.get('label', mode_name.title())
    coefficients = mode.get('coefficients', {})
    coeff_lines = '\n'.join(f'`{k}` = {v}' for k, v in coefficients.items()) or 'No overrides (baseline)'
    e = discord.Embed(
        title=f'SoulQuest — {action}: {label}',
        description=f'**{instance_name}**\n{mode.get("description", "")}',
        color=SOULMASK_COLOR,
    )
    e.add_field(name='Coefficients Applied', value=coeff_lines, inline=False)
    return e


def _status_embed(instance_name: str, mode_name: str, mode: dict,
                  next_mode, next_dt) -> discord.Embed:
    label = mode.get('label', mode_name.title())
    next_info = 'No scheduled changes'
    if next_mode and next_dt:
        next_info = f'{next_mode.title()} at {next_dt.strftime("%A %H:%M UTC")}'
    return discord.Embed(
        title=f'SoulQuest — {instance_name}',
        description=f'**Current Mode:** {label}\n**Next Change:** {next_info}',
        color=SOULMASK_COLOR,
    )


# ---------------------------------------------------------------------------
# DB helper - get announce channel for a guild
# ---------------------------------------------------------------------------

def _get_announce_channels() -> list[int]:
    """Return all configured soulmask announce channel IDs."""
    try:
        with db_session_scope() as db:
            rows = db.execute(sa_text(
                "SELECT soulmask_announce_channel_id FROM guilds "
                "WHERE soulmask_announce_channel_id IS NOT NULL AND bot_present = 1"
            )).fetchall()
            return [r[0] for r in rows]
    except Exception as e:
        logger.error(f'SoulmaskCog: DB error: {e}')
        return []


# ---------------------------------------------------------------------------
# Cog
# ---------------------------------------------------------------------------

class SoulmaskCog(commands.Cog):
    """Soulmask cluster management — scheduled mode rotations via RCON."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._scheduler_task: asyncio.Task | None = None
        self._active_modes: dict[str, str] = {}

    @commands.Cog.listener()
    async def on_ready(self):
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        if self._scheduler_task is None or self._scheduler_task.done():
            self._scheduler_task = asyncio.ensure_future(self._scheduler_loop())
            logger.info('SoulmaskCog: scheduler started')

    # ---- Scheduler ----

    async def _scheduler_loop(self):
        await asyncio.sleep(30)
        _last_applied: dict[str, str] = {}

        while True:
            try:
                schedule = _load_schedule()
                now = datetime.datetime.utcnow()
                day_name = now.strftime('%A')

                for instance_name, instance_cfg in schedule.get('instances', {}).items():
                    mode_name = _active_mode_for_day(instance_cfg, day_name)
                    run_key = f'{instance_name}:{day_name}:{mode_name}'
                    if _last_applied.get(instance_name) == run_key:
                        continue

                    start_hour = 0
                    for entry in instance_cfg.get('schedule', []):
                        if day_name in entry.get('days', []) and entry['mode'] == mode_name:
                            start_hour = entry.get('start_hour', 0)
                            break
                    if now.hour < start_hour:
                        continue

                    mode = instance_cfg.get('modes', {}).get(mode_name, {})
                    logger.info(f'SoulmaskCog: applying [{mode_name}] to {instance_name}')

                    # Announce upcoming change
                    for ch_id in _get_announce_channels():
                        channel = self.bot.get_channel(ch_id)
                        if channel:
                            try:
                                await channel.send(embed=_mode_embed(instance_name, mode_name, mode, 'Activating'))
                            except Exception as e:
                                logger.warning(f'SoulmaskCog: announce failed: {e}')

                    ok, fail = await _apply_mode(instance_name, mode)
                    _last_applied[instance_name] = run_key
                    self._active_modes[instance_name] = mode_name
                    logger.info(f'SoulmaskCog: [{mode_name}] applied to {instance_name} — {ok} ok, {fail} fail')

                    # Confirm
                    label = mode.get('label', mode_name.title())
                    result = f'{ok} coefficients applied' + (f', {fail} failed' if fail else '')
                    for ch_id in _get_announce_channels():
                        channel = self.bot.get_channel(ch_id)
                        if channel:
                            try:
                                await channel.send(f'**{label}** mode is now active on `{instance_name}`. {result}.')
                            except Exception:
                                pass

            except Exception as e:
                logger.error(f'SoulmaskCog: scheduler error: {e}', exc_info=True)

            await asyncio.sleep(60)

    # ---- Slash commands ----

    sm = SlashCommandGroup(
        'sm',
        'Soulmask server management',
        guild_ids=get_debug_guilds(),
        default_member_permissions=discord.Permissions(administrator=True),
    )

    @sm.command(name='status', description='Show current mode and next scheduled change')
    @commands.has_permissions(administrator=True)
    async def slash_status(self, ctx: discord.ApplicationContext):
        schedule = _load_schedule()
        instances = schedule.get('instances', {})
        if not instances:
            await ctx.respond('No Soulmask instances configured in schedule.json.', ephemeral=True)
            return

        now = datetime.datetime.utcnow()
        day_name = now.strftime('%A')
        embeds = []
        for instance_name, instance_cfg in instances.items():
            mode_name = self._active_modes.get(instance_name) or _active_mode_for_day(instance_cfg, day_name)
            mode = instance_cfg.get('modes', {}).get(mode_name, {})
            next_mode, next_dt = _next_mode_change(instance_cfg, now)
            embeds.append(_status_embed(instance_name, mode_name, mode, next_mode, next_dt))

        await ctx.respond(embeds=embeds[:3])  # Discord max 3 embeds per message

    @sm.command(name='mode', description='Manually apply a mode to an instance')
    @commands.has_permissions(administrator=True)
    async def slash_mode(
        self, ctx: discord.ApplicationContext,
        instance: discord.Option(str, 'Instance: sunken or verdant', choices=['sunken', 'verdant']),
        mode_name: discord.Option(str, 'Mode name (e.g. pve, pvec, loot_frenzy, build_day)'),
    ):
        await ctx.defer()
        instance_name = _SHORT_MAP.get(instance.lower(), instance)
        schedule = _load_schedule()
        instance_cfg = schedule.get('instances', {}).get(instance_name)
        if not instance_cfg:
            await ctx.respond(f'Unknown instance `{instance}`.', ephemeral=True)
            return

        mode = instance_cfg.get('modes', {}).get(mode_name.lower())
        if not mode:
            available = ', '.join(instance_cfg.get('modes', {}).keys())
            await ctx.respond(f'Unknown mode `{mode_name}`. Available: {available}', ephemeral=True)
            return

        ok, fail = await _apply_mode(instance_name, mode)
        self._active_modes[instance_name] = mode_name.lower()
        result = f'{ok} coefficients applied' + (f', {fail} failed' if fail else '')
        await ctx.respond(embed=_mode_embed(instance_name, mode_name, mode, 'Applied'))
        await ctx.send_followup(result)

    @sm.command(name='reset', description='Reset all coefficients to baseline')
    @commands.has_permissions(administrator=True)
    async def slash_reset(
        self, ctx: discord.ApplicationContext,
        instance: discord.Option(str, 'Instance: sunken or verdant', choices=['sunken', 'verdant']),
    ):
        await ctx.defer()
        instance_name = _SHORT_MAP.get(instance.lower(), instance)
        schedule = _load_schedule()
        instance_cfg = schedule.get('instances', {}).get(instance_name)
        if not instance_cfg:
            await ctx.respond(f'Unknown instance `{instance}`.', ephemeral=True)
            return

        ok, fail = await _reset_to_baseline(instance_name, instance_cfg)
        self._active_modes[instance_name] = instance_cfg.get('default_mode', 'pve')
        result = f'{ok} coefficients reset' + (f', {fail} failed' if fail else '')
        await ctx.respond(f'Baseline restored on `{instance_name}`. {result}.')

    @sm.command(name='coefficients', description='Show active coefficients for an instance')
    @commands.has_permissions(administrator=True)
    async def slash_coefficients(
        self, ctx: discord.ApplicationContext,
        instance: discord.Option(str, 'Instance: sunken or verdant', choices=['sunken', 'verdant']),
    ):
        instance_name = _SHORT_MAP.get(instance.lower(), instance)
        schedule = _load_schedule()
        instance_cfg = schedule.get('instances', {}).get(instance_name)
        if not instance_cfg:
            await ctx.respond(f'Unknown instance `{instance}`.', ephemeral=True)
            return

        mode_name = self._active_modes.get(instance_name, instance_cfg.get('default_mode', 'pve'))
        mode = instance_cfg.get('modes', {}).get(mode_name, {})
        coefficients = mode.get('coefficients', {})

        if not coefficients:
            await ctx.respond(f'`{instance_name}` is in **{mode_name}** mode - no overrides (baseline).', ephemeral=True)
            return

        lines = '\n'.join(f'`{k}` = {v}' for k, v in coefficients.items())
        await ctx.respond(f'**{instance_name}** — {mode_name} mode:\n{lines}', ephemeral=True)

    @sm.command(name='reload', description='Reload schedule.json without restarting')
    @commands.has_permissions(administrator=True)
    async def slash_reload(self, ctx: discord.ApplicationContext):
        schedule = _load_schedule()
        count = len(schedule.get('instances', {}))
        await ctx.respond(f'Schedule reloaded. {count} instance(s) configured.', ephemeral=True)

    @sm.command(name='schedule', description='Show the full weekly schedule')
    @commands.has_permissions(administrator=True)
    async def slash_schedule(self, ctx: discord.ApplicationContext):
        schedule = _load_schedule()
        instances = schedule.get('instances', {})
        if not instances:
            await ctx.respond('No instances configured.', ephemeral=True)
            return

        lines = []
        for instance_name, instance_cfg in instances.items():
            lines.append(f'**{instance_name}**')
            lines.append(f'  Default: {instance_cfg.get("default_mode", "pve")}')
            for entry in instance_cfg.get('schedule', []):
                days = ', '.join(entry.get('days', []))
                lines.append(f'  {days} @ {entry.get("start_hour", 0):02d}:00 UTC → {entry["mode"]}')

        await ctx.respond('\n'.join(lines), ephemeral=True)


def setup(bot: commands.Bot):
    bot.add_cog(SoulmaskCog(bot))
