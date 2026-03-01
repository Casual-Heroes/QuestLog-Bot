# cogs/emergency.py — Emergency Kill Switch
"""
Owner-only emergency controls for incident response.

Levels:
  /emergency status            — Show current service state
  /emergency maintenance on    — Enable maintenance mode (site shows 503)
  /emergency maintenance off   — Lift maintenance mode
  /emergency stop web          — Stop gunicorn (full site down)
  /emergency stop all          — Stop web + Matrix
  /emergency start web         — Start web server
  /emergency start all         — Start web + Matrix

Security hardening:
  - Hardcoded to BOT_OWNER_ID env var — Discord permissions do NOT apply
  - Destructive actions require a confirmation modal (type CONFIRM)
  - All responses are ephemeral (never visible in channels)
  - Logs every action to console + optional audit channel
  - Uses allowlisted subprocess calls only — no arbitrary shell execution

Sudo setup required (run once as root):
  echo "wardenbot ALL=(ALL) NOPASSWD: /bin/systemctl stop casualheroes" > /etc/sudoers.d/wardenbot-emergency
  echo "wardenbot ALL=(ALL) NOPASSWD: /bin/systemctl start casualheroes" >> /etc/sudoers.d/wardenbot-emergency
  echo "wardenbot ALL=(ALL) NOPASSWD: /bin/systemctl stop matrix-synapse" >> /etc/sudoers.d/wardenbot-emergency
  echo "wardenbot ALL=(ALL) NOPASSWD: /bin/systemctl start matrix-synapse" >> /etc/sudoers.d/wardenbot-emergency
  chmod 440 /etc/sudoers.d/wardenbot-emergency

  Replace 'wardenbot' with the actual user the bot process runs as.
  Replace 'matrix-synapse' with your actual Matrix service name if different.
"""

import logging
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import discord
from discord import SlashCommandGroup
from discord.ext import commands

logger = logging.getLogger("warden.emergency")

# ─────────────────────────────────────────────────────────────────────────────
# Config — all from env, nothing hardcoded
# ─────────────────────────────────────────────────────────────────────────────
BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID", "0"))
MAINTENANCE_FLAG = Path(os.getenv("MAINTENANCE_FLAG_PATH", "/srv/ch-webserver/.maintenance"))
WEB_SERVICE = os.getenv("EMERGENCY_WEB_SERVICE", "casualheroes")
MATRIX_SERVICE = os.getenv("EMERGENCY_MATRIX_SERVICE", "matrix-synapse")

# Optional: channel ID to post audit notifications to (leave 0 to disable)
EMERGENCY_AUDIT_CHANNEL_ID = int(os.getenv("EMERGENCY_AUDIT_CHANNEL_ID", "0"))

if BOT_OWNER_ID == 0:
    logger.warning(
        "BOT_OWNER_ID is not set — all /emergency commands are DISABLED. "
        "Set BOT_OWNER_ID in your environment to enable emergency controls."
    )

# Allowlist — the ONLY systemctl commands this cog will ever run
_ALLOWED_SYSTEMCTL = {
    ("stop",  WEB_SERVICE),
    ("start", WEB_SERVICE),
    ("stop",  MATRIX_SERVICE),
    ("start", MATRIX_SERVICE),
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _is_owner(user_id: int) -> bool:
    """Triple-check: env var set, non-zero, and exact match."""
    return BOT_OWNER_ID != 0 and user_id == BOT_OWNER_ID


def _run_systemctl(action: str, service: str) -> tuple[bool, str]:
    """
    Run a single allowlisted systemctl command via sudo.
    Returns (success, output_message).
    """
    if (action, service) not in _ALLOWED_SYSTEMCTL:
        return False, f"Command not in allowlist: systemctl {action} {service}"

    try:
        result = subprocess.run(
            ["sudo", "systemctl", action, service],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            return True, f"systemctl {action} {service}: OK"
        else:
            err = (result.stderr or result.stdout or "no output").strip()
            return False, f"systemctl {action} {service} failed (rc={result.returncode}): {err}"
    except subprocess.TimeoutExpired:
        return False, f"systemctl {action} {service} timed out after 30s"
    except FileNotFoundError:
        return False, "sudo/systemctl not found — check PATH"
    except Exception as e:
        return False, f"Unexpected error: {e}"


def _service_status(service: str) -> str:
    """Return 'active', 'inactive', 'failed', or 'unknown'."""
    try:
        result = subprocess.run(
            ["systemctl", "is-active", service],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def _maintenance_active() -> bool:
    return MAINTENANCE_FLAG.exists()


# ─────────────────────────────────────────────────────────────────────────────
# Confirmation Modal
# ─────────────────────────────────────────────────────────────────────────────

class ConfirmModal(discord.ui.Modal):
    """Forces the owner to type CONFIRM before any destructive action fires."""

    def __init__(self, action_label: str, callback_fn):
        super().__init__(title=f"Confirm: {action_label}")
        self._callback_fn = callback_fn

        self.add_item(discord.ui.InputText(
            label='Type CONFIRM to proceed',
            placeholder="CONFIRM",
            min_length=7,
            max_length=7,
        ))

    async def callback(self, interaction: discord.Interaction):
        typed = self.children[0].value.strip().upper()
        if typed != "CONFIRM":
            await interaction.response.send_message(
                "❌ Confirmation failed — you must type exactly `CONFIRM`.",
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True)
        await self._callback_fn(interaction)


# ─────────────────────────────────────────────────────────────────────────────
# Cog
# ─────────────────────────────────────────────────────────────────────────────

class EmergencyCog(commands.Cog):
    """Owner-only emergency server kill switch."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # ── Guard ──────────────────────────────────────────────────────────────

    async def _owner_check(self, ctx: discord.ApplicationContext) -> bool:
        """Reject anyone who isn't the bot owner. Silent ephemeral error."""
        if not _is_owner(ctx.author.id):
            logger.warning(
                "EMERGENCY: Unauthorized attempt by %s (%d) — command: %s",
                ctx.author, ctx.author.id, ctx.command.qualified_name,
            )
            await ctx.respond(
                "❌ This command is restricted to the bot owner.",
                ephemeral=True,
            )
            return False
        return True

    # ── Audit log ─────────────────────────────────────────────────────────

    async def _audit(self, description: str, color: discord.Color = discord.Color.orange()):
        """Post an audit embed to the configured channel (if set) and always log to console."""
        logger.warning("EMERGENCY ACTION: %s", description)

        if not EMERGENCY_AUDIT_CHANNEL_ID:
            return
        try:
            channel = self.bot.get_channel(EMERGENCY_AUDIT_CHANNEL_ID)
            if channel:
                embed = discord.Embed(
                    title="🚨 Emergency Action",
                    description=description,
                    color=color,
                    timestamp=datetime.now(timezone.utc),
                )
                await channel.send(embed=embed)
        except Exception as e:
            logger.error("Failed to post emergency audit: %s", e)

    # ── Command group ──────────────────────────────────────────────────────

    emergency = SlashCommandGroup(
        name="emergency",
        description="[OWNER ONLY] Emergency server controls",
    )

    # ── /emergency status ──────────────────────────────────────────────────

    @emergency.command(name="status", description="[OWNER ONLY] Show current service status")
    async def emergency_status(self, ctx: discord.ApplicationContext):
        if not await self._owner_check(ctx):
            return

        await ctx.defer(ephemeral=True)

        web_state     = _service_status(WEB_SERVICE)
        matrix_state  = _service_status(MATRIX_SERVICE)
        maint_active  = _maintenance_active()

        def state_emoji(s: str) -> str:
            return {"active": "✅", "inactive": "⏹️", "failed": "❌"}.get(s, "❓")

        embed = discord.Embed(
            title="🚨 Emergency Status",
            color=discord.Color.red() if web_state != "active" else discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name=f"Web ({WEB_SERVICE})",       value=f"{state_emoji(web_state)} {web_state}",    inline=True)
        embed.add_field(name=f"Matrix ({MATRIX_SERVICE})", value=f"{state_emoji(matrix_state)} {matrix_state}", inline=True)
        embed.add_field(name="Maintenance Mode",           value="🔴 ON" if maint_active else "🟢 OFF",       inline=True)
        embed.set_footer(text=f"Flag: {MAINTENANCE_FLAG}")

        await ctx.respond(embed=embed, ephemeral=True)

    # ── /emergency maintenance ─────────────────────────────────────────────

    maintenance_group = emergency.create_subgroup(
        "maintenance", "[OWNER ONLY] Toggle maintenance mode"
    )

    @maintenance_group.command(name="on", description="[OWNER ONLY] Enable maintenance mode (site shows 503)")
    async def maintenance_on(self, ctx: discord.ApplicationContext):
        if not await self._owner_check(ctx):
            return

        await ctx.defer(ephemeral=True)

        if _maintenance_active():
            await ctx.respond("ℹ️ Maintenance mode is already **ON**.", ephemeral=True)
            return

        try:
            MAINTENANCE_FLAG.parent.mkdir(parents=True, exist_ok=True)
            MAINTENANCE_FLAG.touch()
            await self._audit(
                f"**Maintenance mode ENABLED** by <@{ctx.author.id}> at <t:{int(time.time())}:F>",
                discord.Color.orange(),
            )
            await ctx.respond(
                "🔴 **Maintenance mode is now ON.**\nAll site requests will return 503. Use `/emergency maintenance off` to restore.",
                ephemeral=True,
            )
        except Exception as e:
            logger.error("Failed to create maintenance flag: %s", e)
            await ctx.respond(f"❌ Failed to create maintenance flag: `{e}`", ephemeral=True)

    @maintenance_group.command(name="off", description="[OWNER ONLY] Lift maintenance mode")
    async def maintenance_off(self, ctx: discord.ApplicationContext):
        if not await self._owner_check(ctx):
            return

        await ctx.defer(ephemeral=True)

        if not _maintenance_active():
            await ctx.respond("ℹ️ Maintenance mode is already **OFF**.", ephemeral=True)
            return

        try:
            MAINTENANCE_FLAG.unlink()
            await self._audit(
                f"**Maintenance mode DISABLED** by <@{ctx.author.id}> at <t:{int(time.time())}:F>",
                discord.Color.green(),
            )
            await ctx.respond("🟢 **Maintenance mode lifted.** Site is live again.", ephemeral=True)
        except Exception as e:
            logger.error("Failed to remove maintenance flag: %s", e)
            await ctx.respond(f"❌ Failed to remove flag: `{e}`", ephemeral=True)

    # ── /emergency stop ────────────────────────────────────────────────────

    stop_group = emergency.create_subgroup(
        "stop", "[OWNER ONLY] Stop services (requires CONFIRM)"
    )

    @stop_group.command(name="web", description="[OWNER ONLY] Stop the web server (requires CONFIRM)")
    async def stop_web(self, ctx: discord.ApplicationContext):
        if not await self._owner_check(ctx):
            return

        async def _do_stop(interaction: discord.Interaction):
            # Enable maintenance first so nginx shows a clean page while gunicorn is down
            if not _maintenance_active():
                try:
                    MAINTENANCE_FLAG.touch()
                except Exception:
                    pass

            ok, msg = _run_systemctl("stop", WEB_SERVICE)
            await self._audit(
                f"**STOP WEB** by <@{ctx.author.id}> at <t:{int(time.time())}:F>\nResult: `{msg}`",
                discord.Color.red() if not ok else discord.Color.orange(),
            )
            icon = "✅" if ok else "❌"
            await interaction.followup.send(
                f"{icon} `{msg}`\n\nMaintenance mode is also **ON**. Use `/emergency start web` to restore.",
                ephemeral=True,
            )

        await ctx.send_modal(ConfirmModal("Stop Web Server", _do_stop))

    @stop_group.command(name="all", description="[OWNER ONLY] Stop web + Matrix (requires CONFIRM)")
    async def stop_all(self, ctx: discord.ApplicationContext):
        if not await self._owner_check(ctx):
            return

        async def _do_stop_all(interaction: discord.Interaction):
            if not _maintenance_active():
                try:
                    MAINTENANCE_FLAG.touch()
                except Exception:
                    pass

            results = []
            for service in [WEB_SERVICE, MATRIX_SERVICE]:
                ok, msg = _run_systemctl("stop", service)
                results.append(f"{'✅' if ok else '❌'} `{msg}`")

            summary = "\n".join(results)
            await self._audit(
                f"**STOP ALL** by <@{ctx.author.id}> at <t:{int(time.time())}:F>\n{summary}",
                discord.Color.red(),
            )
            await interaction.followup.send(
                f"🚨 **Full stop executed:**\n{summary}\n\nUse `/emergency start all` to restore.",
                ephemeral=True,
            )

        await ctx.send_modal(ConfirmModal("Stop ALL Services", _do_stop_all))

    # ── /emergency start ───────────────────────────────────────────────────

    start_group = emergency.create_subgroup(
        "start", "[OWNER ONLY] Start/restore services"
    )

    @start_group.command(name="web", description="[OWNER ONLY] Start the web server and lift maintenance")
    async def start_web(self, ctx: discord.ApplicationContext):
        if not await self._owner_check(ctx):
            return

        await ctx.defer(ephemeral=True)

        ok, msg = _run_systemctl("start", WEB_SERVICE)

        # Lift maintenance if start succeeded
        if ok and _maintenance_active():
            try:
                MAINTENANCE_FLAG.unlink()
                msg += " | Maintenance flag removed."
            except Exception as e:
                msg += f" | WARNING: could not remove maintenance flag: {e}"

        await self._audit(
            f"**START WEB** by <@{ctx.author.id}> at <t:{int(time.time())}:F>\nResult: `{msg}`",
            discord.Color.green() if ok else discord.Color.red(),
        )
        icon = "✅" if ok else "❌"
        await ctx.respond(f"{icon} `{msg}`", ephemeral=True)

    @start_group.command(name="all", description="[OWNER ONLY] Start web + Matrix and lift maintenance")
    async def start_all(self, ctx: discord.ApplicationContext):
        if not await self._owner_check(ctx):
            return

        await ctx.defer(ephemeral=True)

        results = []
        all_ok = True
        for service in [WEB_SERVICE, MATRIX_SERVICE]:
            ok, msg = _run_systemctl("start", service)
            results.append(f"{'✅' if ok else '❌'} `{msg}`")
            if not ok:
                all_ok = False

        if all_ok and _maintenance_active():
            try:
                MAINTENANCE_FLAG.unlink()
                results.append("🟢 Maintenance flag removed.")
            except Exception as e:
                results.append(f"⚠️ Could not remove maintenance flag: {e}")

        summary = "\n".join(results)
        await self._audit(
            f"**START ALL** by <@{ctx.author.id}> at <t:{int(time.time())}:F>\n{summary}",
            discord.Color.green() if all_ok else discord.Color.orange(),
        )
        await ctx.respond(f"**Restore complete:**\n{summary}", ephemeral=True)


def setup(bot: commands.Bot):
    bot.add_cog(EmergencyCog(bot))
