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
import json
import os
import time
from glob import glob

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
AMP_SERVER_PASSWORD_NODE = 'Meta.GenericModule.ServerPassword'  # pragma: allowlist secret

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


def _extract_amp_server_password(setting) -> tuple[bool, str]:
    """Extract AMP's authoritative password without confusing missing with empty."""
    missing = object()
    if isinstance(setting, dict):
        node = setting.get('node', setting.get('Node'))
        input_type = setting.get('input_type', setting.get('InputType'))
        value = setting.get(
            'current_value',
            setting.get('CurrentValue', missing),
        )
    else:
        node = getattr(setting, 'node', None)
        input_type = getattr(setting, 'input_type', None)
        value = getattr(setting, 'current_value', missing)

    if node != AMP_SERVER_PASSWORD_NODE:
        return False, ''
    if input_type and str(input_type).lower() != 'password':
        return False, ''
    if value is missing:
        return False, ''
    return True, '' if value is None else str(value)


async def _get_amp_server_password(instance_name: str) -> str | None:
    """Read the player password directly from AMP.

    None means AMP did not make the setting available. An empty string means
    AMP explicitly has no password configured.
    """
    instance = await _get_amp_instance(instance_name)
    if not instance:
        return None
    try:
        setting = await asyncio.wait_for(
            instance.get_config(
                AMP_SERVER_PASSWORD_NODE,
                format_data=False,
            ),
            timeout=5,
        )
        found, password = _extract_amp_server_password(setting)
        return password if found else None
    except Exception as e:
        logger.warning(
            '[gameserver] AMP protected-setting lookup failed for %s: %s',
            instance_name,
            type(e).__name__,
        )
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
            # Admin-configured public_ip always wins when set - AMP reports the
            # NATed/internal address regardless of what's actually reachable from
            # outside, so there's no reliable way to auto-detect the right IP for
            # a server behind port-forwarding. Only fall back to AMP's own value
            # (then ifconfig.me) when the admin hasn't set an override.
            if public_ip:
                raw_ip = public_ip
            else:
                raw_ip = (
                    game_port.get('ip') or game_port.get('hostname')
                    or game_port.get('address') or game_port.get('Address')
                )
                # Comparison with wildcard addresses, not a listening socket.
                if not raw_ip or raw_ip in ('0.0.0.0', '::'):  # nosec B104
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
                "SELECT * FROM gamebot_configs WHERE configured = 1 AND discord_guild_id IS NOT NULL"
            )).fetchall()
            configs = [dict(r._mapping) for r in rows]
            # The legacy schema still has a plaintext password column used by
            # another service. Warden deliberately drops it at the boundary and
            # reads the current value from AMP only when a private channel needs it.
            for config in configs:
                config.pop('server_password', None)
            return configs
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


# ---------------------------------------------------------------------------
# Live log forwarding - ported from questlogfluxer/cogs/gameserver.py so a
# Discord-linked instance can forward its AMP console log the same way a
# Fluxer-linked one already does. Discord-only: this cog has no join/leave
# regex processing (that's a separate, not-yet-built feature) - purely tails
# the log and forwards filtered lines to live_log_discord_channel_id.
# ---------------------------------------------------------------------------

class LogWatcher:
    """Tails the latest AMPLOG_*.log file in the given directory."""

    def __init__(self, log_dir: str):
        self.log_dir = log_dir
        self.glob_pattern = os.path.join(log_dir, 'AMPLOG_*.log')
        self.current_file: str | None = None
        self.file_handle = None
        self.file_position: int = 0
        self._update_latest()

    def _update_latest(self):
        files = sorted(glob(self.glob_pattern))
        if not files:
            return
        latest = files[-1]
        if latest != self.current_file:
            if self.file_handle:
                self.file_handle.close()
            self.current_file = latest
            self.file_handle = open(latest, 'rb')
            # Seek to end on first open so we only read NEW lines
            self.file_handle.seek(0, 2)
            self.file_position = self.file_handle.tell()

    def read_new_lines(self) -> list[str]:
        self._update_latest()
        if not self.file_handle:
            return []
        self.file_handle.seek(self.file_position)
        raw = self.file_handle.readlines()
        self.file_position = self.file_handle.tell()
        lines = []
        for l in raw:
            try:
                lines.append(l.decode('utf-8', errors='replace').strip())
            except Exception:
                pass
        return [l for l in lines if l]

    def close(self):
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None


# Lines to suppress from live log - AMP internal noise generated by our own polling
_LIVE_LOG_BLOCKLIST = [
    'Authentication attempt for user SVC-AMP-SITEOPS',
    'Authentication success',
    'Authentication attempt for user SVC-AMP',
]


def _filter_live_log_lines(lines: list[str]) -> list[str]:
    """Filter out AMP internal noise, return only meaningful game server output."""
    out = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if any(blocked in line for blocked in _LIVE_LOG_BLOCKLIST):
            continue
        out.append(line)
    return out


def _chunk_log_lines(lines: list[str], max_chars: int = 1900) -> list[str]:
    """Batch filtered log lines into chunks under Discord's message length limit."""
    chunks = []
    chunk = ''
    for line in lines:
        if len(chunk) + len(line) + 1 > max_chars:
            chunks.append(chunk)
            chunk = ''
        chunk += line + '\n'
    if chunk:
        chunks.append(chunk)
    return chunks


def _parse_stats_channel_ids(raw) -> list[str]:
    """discord_stats_channel_id is a JSON array of channel IDs, e.g. '["123","456"]'.
    Falls back to treating a bare numeric string as a single-item list, in case any
    row was written before the multi-channel migration."""
    if not raw:
        return []
    raw = raw.strip()
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [str(c) for c in parsed if c]
    except (json.JSONDecodeError, TypeError):
        pass
    return [raw] if raw else []


def _parse_stats_message_map(raw) -> dict:
    """discord_stats_message_id is a JSON object mapping channel_id -> message_id."""
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return {str(k): str(v) for k, v in parsed.items() if v}
    except (json.JSONDecodeError, TypeError):
        pass
    return {}


def _update_discord_message_id(instance_name: str, channel_id: str, msg_id: str | None):
    """Update just this channel's entry in the per-channel message-id map, so
    other channels' tracked messages are left untouched."""
    try:
        with db_session_scope() as db:
            row = db.execute(text(
                "SELECT discord_stats_message_id FROM gamebot_configs WHERE instance_name=:n"
            ), {'n': instance_name}).fetchone()
            msg_map = _parse_stats_message_map(row[0] if row else None)
            if msg_id:
                msg_map[channel_id] = msg_id
            else:
                msg_map.pop(channel_id, None)
            db.execute(text(
                "UPDATE gamebot_configs SET discord_stats_message_id=:mid WHERE instance_name=:n"
            ), {'mid': json.dumps(msg_map), 'n': instance_name})
    except Exception as e:
        logger.error(f'[gameserver] _update_discord_message_id: {e}')


async def _resolve_server_password(cfg: dict) -> str | None:
    """Read the password from AMP without copying it into the shared database."""
    if not cfg.get('show_password'):
        return None
    return await _get_amp_server_password(cfg['instance_name'])


# ---------------------------------------------------------------------------
# In-game server name reader (1:1 port from Fluxer)
# ---------------------------------------------------------------------------

def read_ingame_server_name(instance_name: str, game_type: str) -> str | None:
    """Read the in-game server name from the game's config file on disk."""
    from defusedxml import ElementTree as ET
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
        try:
            # Recursive glob walks the whole instance tree, including game-managed
            # save/temp dirs (e.g. Palworld's world_save_temp) that can be created
            # and removed mid-save - a dir vanishing between listdir and stat during
            # that walk raises FileNotFoundError here, not inside the parse below.
            matches = sorted(instance_dir.glob(glob_pattern))
        except (FileNotFoundError, OSError) as e:
            logger.debug(f'read_ingame_server_name glob {glob_pattern} for {instance_name}: {e}')
            continue
        if not matches:
            continue
        config_file = matches[0]
        ext = config_file.suffix.lower()
        try:
            if ext == '.xml':
                if config_file.stat().st_size > 2 * 1024 * 1024:
                    logger.warning(
                        'Skipping oversized XML server config: %s', config_file
                    )
                    continue
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

async def build_serverinfo_embed(cfg: dict, *, include_password: bool = True) -> discord.Embed:
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

    # Passwords are allowed only in channels hidden from @everyone. The caller
    # determines channel visibility before requesting this field.
    server_password = await _resolve_server_password(cfg) if include_password else None
    if server_password:
        embed.add_field(name='Server Password', value=f"```{server_password}```", inline=False)

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

def _embed_fingerprint(embed: discord.Embed) -> tuple:
    """Content-only signature for change detection - excludes embed.timestamp,
    which is set to "now" on every build and would otherwise always differ."""
    data = embed.to_dict()
    data.pop('timestamp', None)
    return json.dumps(data, sort_keys=True)


class GameServerCog(commands.Cog):
    """Keeps a server-info embed edited in-place in the configured Discord channel."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._last_fingerprint = {}  # (instance_name, channel_id) -> content fingerprint
        self._log_watchers: dict[str, LogWatcher] = {}
        self.refresh_embeds.start()
        self.watch_logs.start()

    def cog_unload(self):
        self.refresh_embeds.cancel()
        self.watch_logs.cancel()
        for watcher in self._log_watchers.values():
            watcher.close()

    @tasks.loop(seconds=35)
    async def refresh_embeds(self):
        try:
            configs = _load_all_configs()
            for i, cfg in enumerate(configs):
                if not cfg.get('discord_stats_channel_id'):
                    continue
                try:
                    await self._refresh_one(cfg)
                except Exception as e:
                    logger.error(f"[gameserver] refresh {cfg['instance_name']}: {e}")
                # Multiple configs can share the same Discord channel (e.g. a shared
                # status board) - space out edits so a burst of configs doesn't blow
                # through Discord's per-channel edit rate limit in one instant. 5s
                # gives more headroom than 3s did (still occasionally hit the limit)
                # - a 6-config shared channel now spreads over ~25s, still comfortably
                # under the 35s loop interval.
                if i < len(configs) - 1:
                    await asyncio.sleep(5)
        except Exception as e:
            logger.error(f'[gameserver] refresh_embeds loop error: {e}')

    @refresh_embeds.before_loop
    async def before_refresh(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(20)
        logger.info('[gameserver] status monitor started (35s interval)')

    @tasks.loop(seconds=3)
    async def watch_logs(self):
        """Tails each Discord-linked instance's AMP log and forwards filtered lines
        to live_log_discord_channel_id, when alert_live_logs is on. Mirrors
        questlogfluxer's log watcher - same glob pattern, same filter/chunk logic -
        but Discord-only and forwarding-only (no join/leave regex processing here)."""
        try:
            configs = _load_all_configs()
            for cfg in configs:
                if not cfg.get('alert_live_logs') or not cfg.get('live_log_discord_channel_id'):
                    continue
                instance_name = cfg['instance_name']
                log_dir = cfg.get('amp_log_dir', '')
                try:
                    log_dir_exists = bool(log_dir) and Path(log_dir).exists()
                except PermissionError:
                    logger.warning(f'[gameserver] watch_logs: permission denied on {log_dir} - fix with: sudo chmod o+rx {log_dir}')
                    continue
                if not log_dir_exists:
                    continue
                if instance_name not in self._log_watchers:
                    try:
                        self._log_watchers[instance_name] = LogWatcher(log_dir)
                        logger.info(f'[gameserver] LogWatcher started for {instance_name}')
                    except Exception as e:
                        logger.error(f'[gameserver] LogWatcher init {instance_name}: {e}')
                        continue
                watcher = self._log_watchers[instance_name]
                try:
                    lines = watcher.read_new_lines()
                except Exception as e:
                    logger.error(f'[gameserver] watch_logs read {instance_name}: {e}')
                    continue
                if not lines:
                    continue
                filtered = _filter_live_log_lines(lines)
                if not filtered:
                    continue
                channel_id = cfg['live_log_discord_channel_id']
                try:
                    channel = self.bot.get_channel(int(channel_id))
                    if channel is None:
                        channel = await self.bot.fetch_channel(int(channel_id))
                except Exception as e:
                    logger.warning(f'[gameserver] watch_logs channel {channel_id} not found for {instance_name}: {e}')
                    continue
                for chunk in _chunk_log_lines(filtered):
                    try:
                        await channel.send(f'```\n{chunk}\n```')
                    except Exception as e:
                        logger.error(f'[gameserver] watch_logs send {instance_name}: {e}')
        except Exception as e:
            logger.error(f'[gameserver] watch_logs loop error: {e}')

    @watch_logs.before_loop
    async def before_watch_logs(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(20)
        logger.info('[gameserver] live log watcher started (3s poll interval)')

    async def _refresh_one(self, cfg: dict):
        instance_name = cfg['instance_name']
        channel_ids   = _parse_stats_channel_ids(cfg.get('discord_stats_channel_id'))
        if not channel_ids:
            return

        msg_map = _parse_stats_message_map(cfg.get('discord_stats_message_id'))
        embeds = {}

        # Each selected channel gets its own live-edited message, tracked independently -
        # one channel's permission/404 failure must not stop the others from updating.
        for channel_id in channel_ids:
            channel = self.bot.get_channel(int(channel_id))
            if channel is None:
                try:
                    channel = await self.bot.fetch_channel(int(channel_id))
                except Exception as e:
                    logger.warning(
                        '[gameserver] channel %s not found for %s: %s',
                        channel_id,
                        instance_name,
                        type(e).__name__,
                    )
                    continue

            include_password = False
            if isinstance(channel, discord.abc.GuildChannel):
                everyone = channel.guild.default_role
                include_password = not channel.permissions_for(everyone).view_channel
            if cfg.get('show_password') and not include_password:
                logger.warning(
                    '[gameserver] sensitive field hidden in public channel %s',
                    channel_id,
                )

            if include_password not in embeds:
                embed = await build_serverinfo_embed(
                    cfg,
                    include_password=include_password,
                )
                embeds[include_password] = (embed, _embed_fingerprint(embed))
            embed, fingerprint = embeds[include_password]

            cache_key = (instance_name, channel_id)
            if self._last_fingerprint.get(cache_key) == fingerprint:
                continue  # content unchanged since last edit - skip the API call entirely
            ok = await self._refresh_one_channel(
                instance_name,
                channel_id,
                msg_map.get(channel_id),
                embed,
                channel=channel,
            )
            if ok:
                self._last_fingerprint[cache_key] = fingerprint
            # else: leave the cached fingerprint as-is, so a real content change is
            # retried next cycle instead of being silently swallowed by the cache.

    async def _refresh_one_channel(
        self,
        instance_name: str,
        channel_id: str,
        old_msg_id: str | None,
        embed,
        *,
        channel=None,
    ) -> bool:
        channel = channel or self.bot.get_channel(int(channel_id))
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(int(channel_id))
            except Exception as e:
                logger.warning(f'[gameserver] channel {channel_id} not found for {instance_name}: {e}')
                return False

        if old_msg_id:
            try:
                msg = await channel.fetch_message(int(old_msg_id))
                await msg.edit(embed=embed)
                logger.debug(f'[gameserver] edited embed for {instance_name} channel={channel_id} msg={old_msg_id}')
                return True
            except discord.NotFound:
                logger.warning(f'[gameserver] message {old_msg_id} not found for {instance_name} channel={channel_id} - posting fresh')
                _update_discord_message_id(instance_name, channel_id, None)
                old_msg_id = None
            except discord.Forbidden as e:
                logger.warning(f'[gameserver] no permission to edit msg {old_msg_id} for {instance_name} channel={channel_id}: {e}')
                return False
            except Exception as e:
                logger.warning(f'[gameserver] edit failed for {instance_name} channel={channel_id} msg={old_msg_id}: {e!r}')
                # Non-404 error (rate limit, server error) - skip this cycle, do NOT post new
                return False

        # No existing message (first run or 404 cleared it) - post a new one
        try:
            msg = await channel.send(embed=embed)
            _update_discord_message_id(instance_name, channel_id, str(msg.id))
            logger.info(f'[gameserver] posted new embed for {instance_name} msg={msg.id} channel={channel_id}')
            return True
        except discord.Forbidden as e:
            logger.error(f'[gameserver] no permission to send to channel {channel_id} for {instance_name}: {e}')
            return False
        except Exception as e:
            logger.error(f'[gameserver] send failed for {instance_name}: {e}')
            return False


def setup(bot: commands.Bot):
    bot.add_cog(GameServerCog(bot))
