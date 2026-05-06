# cogs/gameserver.py - Game Server Status Monitor for WardenBot (Discord)
#
# Polls gamebot_configs every 30s, builds a server info embed, and keeps it
# edited in place so it stays pinned rather than spamming new messages.
#
# Reads from:  gamebot_configs.discord_stats_channel_id (set via Discord Quest Control dashboard)
#              gamebot_configs.discord_stats_message_id (stored message ID for in-place edit)
# Writes to:   gamebot_configs.discord_stats_message_id  (updated after first post)
#
# AMP credentials: AMP_URL / AMP_USER / AMP_PASSWORD from warden.env
# Same embed logic as questlogfluxer/cogs/gameserver.py, translated to discord.py.

import asyncio
import datetime
import os

import discord
from discord.ext import commands, tasks
from sqlalchemy import text

from config import logger, db_session_scope

# Suppress noisy AMP library logs
import logging
logging.getLogger('ampapi').setLevel(logging.CRITICAL)

# ---- AMP credentials ----
AMP_URL      = os.getenv('AMP_URL', '')
AMP_USER     = os.getenv('AMP_USER', '')
AMP_PASSWORD = os.getenv('AMP_PASSWORD', '')

# ---- AMP instance paths (same as Fluxer bot) ----
from pathlib import Path
AMP_INSTANCES_BASE = Path(os.getenv('AMP_INSTANCES_BASE', '/mnt/gamestoreage2/ampinstances'))
_extra_raw = os.getenv('AMP_INSTANCES_EXTRA', '')
AMP_INSTANCES_PATHS: list[Path] = [AMP_INSTANCES_BASE] + [
    Path(p.strip()) for p in _extra_raw.split(':') if p.strip()
]

# ---- Game color / emoji maps (1:1 with Fluxer) ----
GAME_COLORS = {
    'V Rising':              0x8B0000,
    'Seven Days To Die':     0xE8890C,
    'Enshrouded':            0x7B4EA0,
    'Valheim':               0x3A7BD5,
    'Icarus':                0x2E8B57,
    'Palworld':              0xF4A261,
    'Palworld (Modded)':     0xF4A261,
}
DEFAULT_COLOR = 0x008080

GAME_EMOJIS = {
    'V Rising':              '\U0001fa78',   # 🩸
    'Seven Days To Die':     '\U0001f9df',   # 🧟
    'Enshrouded':            '\U0001f32b\ufe0f',  # 🌫️
    'Valheim':               '\u2694\ufe0f', # ⚔️
    'Icarus':                '\U0001fa90',   # 🪐
    'Palworld':              '\U0001f43e',   # 🐾
    'Palworld (Modded)':     '\U0001f43e',   # 🐾
}
DEFAULT_EMOJI = '\U0001f3ae'  # 🎮


# ---------------------------------------------------------------------------
# AMP helpers (same logic as Fluxer bot)
# ---------------------------------------------------------------------------

async def _get_amp_instance(instance_name: str):
    if not AMP_URL or not AMP_USER or not AMP_PASSWORD:
        return None
    try:
        from ampapi.dataclass import APIParams
        from ampapi.bridge import Bridge
        from ampapi.controller import AMPControllerInstance as _AMPCtrl

        params = APIParams(url=AMP_URL, user=AMP_USER, password=AMP_PASSWORD)
        Bridge(api_params=params)
        ctrl = _AMPCtrl()
        await ctrl.get_instances()
        for inst in ctrl.instances:
            if getattr(inst, 'instance_name', '') == instance_name:
                return inst
        return None
    except Exception as e:
        logger.debug(f'[gameserver] _get_amp_instance {instance_name}: {e}')
        return None


async def _get_server_status(instance_name: str, public_ip: str | None = None) -> dict:
    result = {
        'state': 'Unknown', 'is_running': False, 'uptime': None,
        'cpu': None, 'ram_mb': None, 'ram_max_mb': None,
        'player_count': 0, 'player_max': 0, 'ip': None, 'port': None,
    }
    instance = await _get_amp_instance(instance_name)
    if not instance:
        return result
    try:
        status = await instance.get_status(format_data=False)
        metrics = status.get('metrics', {})
        result['uptime'] = status.get('uptime')
        result['state']  = status.get('state', 'Unknown')
        state_str = str(result['state']).strip()
        result['is_running'] = state_str in ('Running', '5', '20') or 'running' in state_str.lower()
        cpu_m = metrics.get('cpu_usage', {})
        result['cpu'] = round(cpu_m.get('percent', 0), 1) if cpu_m else None
        ram_m = metrics.get('memory_usage', {})
        if ram_m:
            result['ram_mb']     = ram_m.get('raw_value', 0)
            result['ram_max_mb'] = ram_m.get('max_value', 0)
        users_m = metrics.get('active_users', {})
        if users_m:
            result['player_count'] = int(users_m.get('raw_value', 0))
            result['player_max']   = int(users_m.get('max_value', 0))
    except Exception:
        pass
    try:
        import requests as _requests
        ports = await instance.get_port_summaries(format_data=False)
        # Exclude management/infra ports that are never the game connect port
        _excluded = ('sftp', 'control panel', 'telnet', 'allocs', 'webserver', 'metrics')
        valid_ports = [
            p for p in (ports or [])
            if not p.get('internalonly', False)
            and p.get('port') is not None
            and not any(ex in p.get('name', '').lower() for ex in _excluded)
        ]
        preferred_names = [
            'server and steam port', 'game and mods port', 'game port',
            'server port', 'query port',
        ]
        game_port = next(
            (p for name in preferred_names for p in valid_ports
             if name in p.get('name', '').lower()),
            None
        )
        if not game_port and valid_ports:
            game_port = valid_ports[0]
        if game_port:
            raw_ip = (
                game_port.get('ip') or game_port.get('hostname')
                or game_port.get('address') or game_port.get('Address')
            )
            if not raw_ip or raw_ip in ('0.0.0.0', '::'):
                if public_ip:
                    raw_ip = public_ip
                else:
                    try:
                        raw_ip = _requests.get('https://ifconfig.me', timeout=5).text.strip()
                    except Exception:
                        raw_ip = None
            result['ip']   = raw_ip
            result['port'] = str(game_port.get('port', ''))
    except Exception:
        pass
    return result


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _load_all_configs() -> list[dict]:
    try:
        with db_session_scope() as db:
            rows = db.execute(text(
                "SELECT * FROM gamebot_configs WHERE configured = 1 AND guild_id IS NOT NULL"
            )).fetchall()
            return [dict(r._mapping) for r in rows]
    except Exception as e:
        logger.error(f'[gameserver] _load_all_configs: {e}')
        return []


def _get_online_players(instance_name: str) -> list[str]:
    try:
        with db_session_scope() as db:
            rows = db.execute(text(
                "SELECT username FROM gamebot_players WHERE instance_name=:inst ORDER BY joined_at"
            ), {'inst': instance_name}).fetchall()
            return [r.username for r in rows]
    except Exception:
        return []


def _update_discord_message_id(instance_name: str, msg_id: str | None):
    """Store the Discord stats message ID so we can edit it in-place next cycle."""
    try:
        with db_session_scope() as db:
            db.execute(text(
                "UPDATE gamebot_configs SET discord_stats_message_id=:mid WHERE instance_name=:n"
            ), {'mid': msg_id, 'n': instance_name})
    except Exception as e:
        logger.error(f'[gameserver] _update_discord_message_id: {e}')


# ---------------------------------------------------------------------------
# In-game server name reader (1:1 port from Fluxer)
# ---------------------------------------------------------------------------

def read_ingame_server_name(instance_name: str, game_type: str) -> str | None:
    """Read the in-game server name from the game's config file on disk."""
    import xml.etree.ElementTree as ET
    import configparser
    import json as _json

    instance_dir = next(
        (p / instance_name for p in AMP_INSTANCES_PATHS if (p / instance_name).exists()),
        None
    )
    if not instance_dir:
        return None

    config_globs = [
        '**/serverconfig.xml',
        '**/server_config.xml',
        '**/GameUserSettings.ini',
        '**/serverconfig.ini',
        '**/server.cfg',
        '**/server.properties',
        '**/settings.json',
        '**/config.json',
    ]
    name_keys = ['ServerName', 'server_name', 'Name', 'hostname', 'ServerHostName', 'DisplayName']

    for glob_pattern in config_globs:
        matches = sorted(instance_dir.glob(glob_pattern))
        if not matches:
            continue
        config_file = matches[0]
        ext = config_file.suffix.lower()
        try:
            if ext == '.xml':
                tree = ET.parse(config_file)
                root = tree.getroot()
                for key in name_keys:
                    for elem in root.iter('property'):
                        if elem.get('name', '').lower() == key.lower():
                            val = elem.get('value', '').strip()
                            if val:
                                return val
                for key in name_keys:
                    elem = root.find(f'.//{key}')
                    if elem is not None and elem.text and elem.text.strip():
                        return elem.text.strip()

            elif ext in ('.ini', '.cfg', '.properties'):
                content = config_file.read_text(errors='replace')
                parser = configparser.RawConfigParser()
                try:
                    parser.read_string('[root]\n' + content)
                    for key in name_keys:
                        try:
                            val = parser.get('root', key).strip().strip('"\'')
                            if val:
                                return val
                        except configparser.NoOptionError:
                            pass
                except Exception:
                    pass
                for line in content.splitlines():
                    for key in name_keys:
                        if line.strip().lower().startswith(key.lower() + '='):
                            val = line.split('=', 1)[1].strip().strip('"\'')
                            if val:
                                return val

            elif ext == '.json':
                data = _json.loads(config_file.read_text(errors='replace'))
                if isinstance(data, dict):
                    for key in name_keys:
                        val = data.get(key, '')
                        if val and isinstance(val, str):
                            return val.strip()

        except Exception:
            continue

    return None


# ---------------------------------------------------------------------------
# Embed builder (1:1 port of Fluxer build_serverinfo_embed)
# ---------------------------------------------------------------------------

async def build_serverinfo_embed(cfg: dict) -> discord.Embed:
    instance_name = cfg['instance_name']
    game_type     = cfg.get('game_type', 'Game Server')
    display_name  = cfg.get('server_display_name') or game_type
    color         = GAME_COLORS.get(game_type, DEFAULT_COLOR)
    game_emoji    = GAME_EMOJIS.get(game_type, DEFAULT_EMOJI)

    status  = await _get_server_status(instance_name, public_ip=cfg.get('public_ip') or None)
    players = _get_online_players(instance_name)

    is_running  = status['is_running']
    state_emoji = '\U0001f7e2' if is_running else '\U0001f534'  # 🟢 / 🔴
    state_label = 'Online' if is_running else 'Offline'

    # timestamp set after construction (same pattern as Fluxer) so Discord shows
    # the actual last-updated time, not the message post time
    embed = discord.Embed(
        title=f'{game_emoji} {display_name} Server Info',
        color=color,
    )

    # Row 1: Status + Players
    embed.add_field(name='Server Status', value=f'{state_emoji} {state_label}', inline=True)
    if cfg.get('show_player_count', True):
        player_count = status['player_count'] or len(players)
        player_max   = status['player_max'] or 0
        pc_str = f"{player_count}/{player_max}" if player_max else str(player_count)
        embed.add_field(name='Players', value=pc_str, inline=True)

    # Row 2: In-game server name from config file, fallback to display name
    ingame_name = read_ingame_server_name(instance_name, game_type)
    embed.add_field(name='Server Name', value=f"```{ingame_name or display_name}```", inline=False)

    # Connect info
    if cfg.get('show_ip_port', True) and status.get('ip'):
        connect = f"{status['ip']}:{status['port']}" if status.get('port') else status['ip']
        embed.add_field(name='IP Address', value=f"```{connect}```", inline=False)

    # Password
    if cfg.get('show_password') and cfg.get('server_password'):
        embed.add_field(name='Server Password', value=f"```{cfg['server_password']}```", inline=False)

    # Stats
    if status['cpu'] is not None:
        embed.add_field(name='CPU Usage', value=f"{status['cpu']}%", inline=True)
    if status['ram_mb'] is not None:
        embed.add_field(name='Memory Usage', value=f"{int(status['ram_mb'])} MB", inline=True)
    if status['uptime']:
        embed.add_field(name='Uptime', value=str(status['uptime']), inline=True)

    # Online players / top players by playtime
    if cfg.get('show_top_5_players', True):
        if players:
            lines_out = []
            char_count = 0
            for i, name in enumerate(players, 1):
                line = f"{i}. {name}"
                if char_count + len(line) + 1 > 1000:
                    lines_out.append(f'... and {len(players) - i + 1} more')
                    break
                lines_out.append(line)
                char_count += len(line) + 1
            embed.add_field(name=f'Currently Online ({len(players)})', value='\n'.join(lines_out), inline=False)
        else:
            # No one online - show top players by playtime from AMP analytics
            top_players = []
            try:
                instance = await _get_amp_instance(instance_name)
                if instance:
                    summary = await instance.get_analytics_summary(period_days=30)
                    top_players = getattr(summary, 'top_players', [])
            except Exception:
                pass
            if top_players:
                tp_lines = '\n'.join(
                    f"{i}. {p.username}  {p.display_session_time}"
                    for i, p in enumerate(top_players[:10], 1)
                    if getattr(p, 'username', '').strip()
                )
                if tp_lines:
                    embed.add_field(name='Top Players by Playtime (30d)', value=tp_lines, inline=False)
            elif is_running:
                embed.add_field(name='Currently Online', value='No players online.', inline=False)

    embed.set_footer(text='Powered by QuestLog - Casual Heroes')
    # Set timestamp after construction so it reflects last-updated time on each edit
    embed.timestamp = datetime.datetime.now(datetime.timezone.utc)
    return embed


# ---------------------------------------------------------------------------
# Cog
# ---------------------------------------------------------------------------

class GameServerCog(commands.Cog):
    """Keeps a server-info embed edited in-place in the configured Discord channel."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.refresh_embeds.start()

    def cog_unload(self):
        self.refresh_embeds.cancel()

    @tasks.loop(seconds=30)
    async def refresh_embeds(self):
        try:
            configs = _load_all_configs()
            for cfg in configs:
                if not cfg.get('discord_stats_channel_id'):
                    continue
                try:
                    await self._refresh_one(cfg)
                except Exception as e:
                    logger.error(f"[gameserver] refresh {cfg['instance_name']}: {e}")
        except Exception as e:
            logger.error(f'[gameserver] refresh_embeds loop error: {e}')

    @refresh_embeds.before_loop
    async def before_refresh(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(20)
        logger.info('[gameserver] status monitor started (30s interval)')

    async def _refresh_one(self, cfg: dict):
        instance_name = cfg['instance_name']
        channel_id    = cfg.get('discord_stats_channel_id')
        if not channel_id:
            return

        # Resolve channel
        channel = self.bot.get_channel(int(channel_id))
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(int(channel_id))
            except Exception as e:
                logger.warning(f'[gameserver] channel {channel_id} not found for {instance_name}: {e}')
                return

        embed = await build_serverinfo_embed(cfg)

        old_msg_id = cfg.get('discord_stats_message_id')
        if old_msg_id:
            try:
                msg = await channel.fetch_message(int(old_msg_id))
                await msg.edit(embed=embed)
                logger.debug(f'[gameserver] edited embed for {instance_name} msg={old_msg_id}')
                return
            except discord.NotFound:
                logger.warning(f'[gameserver] message {old_msg_id} not found for {instance_name} - posting fresh')
                _update_discord_message_id(instance_name, None)
                old_msg_id = None
            except discord.Forbidden as e:
                logger.warning(f'[gameserver] no permission to edit msg {old_msg_id} for {instance_name}: {e}')
                return
            except Exception as e:
                logger.warning(f'[gameserver] edit failed for {instance_name} msg={old_msg_id}: {e!r}')
                # Non-404 error (rate limit, server error) - skip this cycle, do NOT post new
                return

        # No existing message (first run or 404 cleared it) - post a new one
        try:
            msg = await channel.send(embed=embed)
            _update_discord_message_id(instance_name, str(msg.id))
            logger.info(f'[gameserver] posted new embed for {instance_name} msg={msg.id} channel={channel_id}')
        except discord.Forbidden as e:
            logger.error(f'[gameserver] no permission to send to channel {channel_id} for {instance_name}: {e}')
        except Exception as e:
            logger.error(f'[gameserver] send failed for {instance_name}: {e}')


def setup(bot: commands.Bot):
    bot.add_cog(GameServerCog(bot))
