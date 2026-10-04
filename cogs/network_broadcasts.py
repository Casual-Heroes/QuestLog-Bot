# cogs/network_broadcasts.py - QuestLog Network broadcast receiver for Discord
"""
Polls discord_pending_broadcasts (written by the QuestLog site) and posts
embed messages to the configured channels in each Discord guild.

Also provides /questlog-network setup to let server admins subscribe their
Discord guild to QuestLog Network LFG broadcasts.

Flow:
  1. User posts LFG on casual-heroes.com/ql/lfg/
  2. They click "Broadcast to Network"
  3. Site writes a row to discord_pending_broadcasts for every Discord guild
     that has subscribed via web_community_bot_configs (platform='discord')
  4. This cog polls every 10 seconds, claims jobs, posts embeds, persists the
     result, acknowledges QuestLog, and only then deletes acknowledged rows

Setup command:
  /questlog-network setup channel:#lfg-channel
  /questlog-network status
  /questlog-network disable
"""

import json
import time
import asyncio
import hashlib
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlsplit

import discord
from discord.ext import commands, tasks
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from config import db_session_scope, logger, LFG_LEGACY_WRITES_ENABLED
from models import LFGDeliveryReceipt
from utils.lfg_api import LFGAPIError


BRAND_COLOR = 0xFEE75C   # gold - matches the LFG embed color on the site
POLL_INTERVAL = 10        # seconds between DB polls
MAX_STALE_SECONDS = 300   # drop rows older than 5 minutes (bot was offline)
STAFF_BOARD_CONFIG_KEY = "community_staff_board_v1"
STAFF_BOARD_MESSAGE_KEY = "community_staff_board"
TERMINAL_DELIVERY_STATUSES = {
    "delivered", "permission_missing", "destination_missing", "failed"
}


@dataclass(frozen=True)
class DeliveryOutcome:
    status: str
    message_id: Optional[int] = None
    thread_id: Optional[int] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None


def _canonical_payload(result) -> dict:
    """Unwrap QuestLog's API envelope while tolerating older adapters."""
    payload = getattr(result, "data", result) or {}
    if not isinstance(payload, dict):
        return {}
    nested = payload.get("data") or payload.get("group")
    return nested if isinstance(nested, dict) else payload


def _discord_http_url(value: Any) -> Optional[str]:
    """Return a Discord-safe absolute HTTP(S) URL, or ``None``.

    Site payloads are external input.  Pycord accepts malformed media URLs
    while building an embed, but Discord rejects the entire message later.
    Keeping validation here lets a card post even when its optional artwork is
    missing or the producer accidentally supplies a relative media path.
    """
    if not isinstance(value, str):
        return None

    candidate = value.strip()
    # Some image providers emit protocol-relative CDN URLs.  Discord requires
    # an explicit scheme, and HTTPS is the safe equivalent for those values.
    if candidate.startswith("//"):
        candidate = f"https:{candidate}"
    if not candidate or len(candidate) > 2048:
        return None
    if any(character.isspace() or ord(character) < 32 for character in candidate):
        return None

    try:
        parsed = urlsplit(candidate)
        # Accessing ``port`` also rejects invalid values such as ``:abc``.
        parsed.port
    except (TypeError, ValueError):
        return None

    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        return None
    return candidate


def _canonical_actor_id(discord_user_id: int) -> Optional[int]:
    with db_session_scope() as session:
        row = session.execute(
            text(
                "SELECT id FROM web_users WHERE discord_id=:discord_id "
                "AND is_banned=0 LIMIT 1"
            ),
            {"discord_id": str(discord_user_id)},
        ).fetchone()
    return int(row[0]) if row else None


def _canonical_error(error: Exception) -> str:
    if isinstance(error, LFGAPIError):
        messages = {
            "lfg_full": "This group is full.",
            "already_joined": "You're already in this group.",
            "not_a_member": "You are not in this group.",
            "creator_cannot_leave": (
                "You created this group. Use Delete if you want to close it."
            ),
            "not_group_creator": "Only the group creator can delete it.",
            "lfg_not_found": "This group no longer exists.",
            "transport_error": (
                "QuestLog is temporarily unavailable. Please try again."
            ),
        }
        return messages.get(error.code, error.message or "The LFG action failed.")
    return "The QuestLog LFG connection is unavailable."


async def _canonical_reply(interaction, message: str):
    if interaction.response.is_done():
        await interaction.followup.send(message, ephemeral=True)
    else:
        await interaction.response.send_message(message, ephemeral=True)


def _member_fields(group: dict) -> list:
    config = group.get("configuration") or {}
    schema = config.get("schema") or {}
    return list(schema.get("member_fields") or [])


def _roster_slots(group: dict) -> list:
    config = group.get("configuration") or {}
    schema = config.get("schema") or {}
    return list(schema.get("roster_slots") or [])


async def _save_setup(bot, group_id, actor_id, interaction, selections, role):
    api_cog = bot.get_cog("LFGAPICog")
    client = api_cog.client if api_cog else None
    await client.update_member(
        group_id,
        {"role": role, "selections": selections},
        actor_user_id=actor_id,
        idempotency_key=f"discord:{interaction.id}:lfg:member-update",
    )


class CanonicalSetupValueModal(discord.ui.Modal):
    def __init__(self, dashboard, field):
        super().__init__(title=f"Update {field.get('label', 'setup')}"[:45])
        self.dashboard = dashboard
        self.field = field
        current = str(dashboard.selections.get(field.get("key"), ""))[:300]
        self.value_input = discord.ui.InputText(
            label=str(field.get("label") or "Value")[:45],
            placeholder=str(field.get("placeholder") or "Enter a value")[:100],
            value=current or None,
            required=bool(field.get("required", False)),
            max_length=300,
        )
        self.add_item(self.value_input)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        self.dashboard.selections[self.field["key"]] = str(self.value_input.value).strip()
        try:
            await _save_setup(
                self.dashboard.bot, self.dashboard.group_id,
                self.dashboard.actor_id, interaction,
                self.dashboard.selections, self.dashboard.role,
            )
            await interaction.followup.send(
                f"{self.field['label']} updated.", ephemeral=True
            )
        except (AttributeError, LFGAPIError) as error:
            await interaction.followup.send(_canonical_error(error), ephemeral=True)


class CanonicalSetupSelect(discord.ui.Select):
    def __init__(self, dashboard, field, choices):
        self.dashboard = dashboard
        self.field = field
        current = dashboard.role if field["key"] == "__role__" else dashboard.selections.get(field["key"])
        current_values = set(current if isinstance(current, list) else [current])
        options = []
        for choice in choices[:25]:
            item = choice if isinstance(choice, dict) else {"value": str(choice), "label": str(choice)}
            options.append(discord.SelectOption(
                label=str(item.get("label") or item.get("value"))[:100],
                value=str(item.get("value"))[:100],
                default=str(item.get("value")) in current_values,
            ))
        multiple = field.get("input") == "multi_select"
        super().__init__(
            placeholder=f"Choose {field.get('label', 'a value')}"[:150],
            options=options,
            min_values=1 if field.get("required") else 0,
            max_values=min(len(options), int(field.get("max_selections") or (len(options) if multiple else 1))),
        )

    async def callback(self, interaction: discord.Interaction):
        values = list(self.values)
        selected = values if self.field.get("input") == "multi_select" else (values[0] if values else "")
        if self.field["key"] == "__role__":
            self.dashboard.role = selected or None
        else:
            self.dashboard.selections[self.field["key"]] = selected
            choices = self.field.get("choices") or []
            if not isinstance(choices, dict):
                match = next((choice for choice in choices if isinstance(choice, dict) and str(choice.get("value")) == str(selected)), None)
                if match and match.get("role"):
                    self.dashboard.role = match["role"]
        await interaction.response.defer(ephemeral=True)
        try:
            await _save_setup(
                self.dashboard.bot, self.dashboard.group_id,
                self.dashboard.actor_id, interaction,
                self.dashboard.selections, self.dashboard.role,
            )
            await interaction.edit_original_response(
                content=self.dashboard.summary("Setup updated."),
                view=self.dashboard,
            )
        except (AttributeError, LFGAPIError) as error:
            await interaction.followup.send(_canonical_error(error), ephemeral=True)


class CanonicalSetupChoiceView(discord.ui.View):
    def __init__(self, dashboard, field, choices):
        super().__init__(timeout=300)
        self.dashboard = dashboard
        self.add_item(CanonicalSetupSelect(dashboard, field, choices))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, emoji="↩️")
    async def back(self, _button, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content=self.dashboard.summary(), view=self.dashboard
        )


class CanonicalSetupFieldButton(discord.ui.Button):
    def __init__(self, dashboard, field, row):
        self.dashboard = dashboard
        self.field = field
        current = dashboard.role if field["key"] == "__role__" else dashboard.selections.get(field["key"])
        display = ", ".join(current) if isinstance(current, list) else str(current or "Not set")
        super().__init__(
            label=f"{field.get('label')}: {display}"[:80],
            style=discord.ButtonStyle.secondary,
            row=row,
        )

    async def callback(self, interaction: discord.Interaction):
        choices = self.field.get("choices") or []
        if isinstance(choices, dict):
            parent = self.dashboard.selections.get(self.field.get("depends_on"))
            if not parent:
                await interaction.response.send_message(
                    "Choose the earlier option first so QuestLog can show the right choices.",
                    ephemeral=True,
                )
                return
            choices = choices.get(str(parent), [])
        if choices:
            await interaction.response.edit_message(
                content=f"Choose **{self.field.get('label')}**.",
                view=CanonicalSetupChoiceView(self.dashboard, self.field, choices),
            )
            return
        await interaction.response.send_modal(
            CanonicalSetupValueModal(self.dashboard, self.field)
        )


class CanonicalSetupDashboard(discord.ui.View):
    def __init__(self, bot, group_id, actor_id, group, member):
        super().__init__(timeout=300)
        self.bot = bot
        self.group_id = int(group_id)
        self.actor_id = int(actor_id)
        self.group = group
        self.selections = dict(member.get("selections") or {})
        self.role = member.get("role")
        fields = _member_fields(group)
        for index, field in enumerate(fields[:20]):
            self.add_item(CanonicalSetupFieldButton(self, field, index // 5))
        slots = _roster_slots(group)
        if slots and len(fields) < 20:
            role_field = {
                "key": "__role__", "label": "Roster role", "input": "select",
                "choices": [{"value": slot.get("slot"), "label": slot.get("label")} for slot in slots],
            }
            self.add_item(CanonicalSetupFieldButton(self, role_field, len(fields) // 5))

    def summary(self, prefix=None):
        lines = [prefix] if prefix else []
        lines.append("Choose what you want to update. These options come from this group's saved QuestLog setup.")
        config = self.group.get("configuration") or {}
        if config.get("frozen"):
            lines.append(f"Setup version {config.get('version')} · saved with this group")
        return "\n".join(lines)


class CanonicalDeleteConfirmView(discord.ui.View):
    def __init__(self, controls, owner_id: int):
        super().__init__(timeout=60)
        self.controls = controls
        self.owner_id = int(owner_id)

    @discord.ui.button(
        label="Yes, delete group",
        style=discord.ButtonStyle.danger,
        emoji="🗑️",
    )
    async def confirm(self, _button, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Only the person who opened this confirmation can use it.",
                ephemeral=True,
            )
            return
        await self.controls.delete_group(interaction)
        self.stop()

    @discord.ui.button(label="Keep it", style=discord.ButtonStyle.secondary)
    async def cancel(self, _button, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Only the person who opened this confirmation can use it.",
                ephemeral=True,
            )
            return
        await interaction.response.edit_message(
            content="Delete cancelled.",
            view=None,
        )
        self.stop()


class CanonicalLFGButton(discord.ui.Button):
    def __init__(self, controls, action, **kwargs):
        super().__init__(
            custom_id=f"questlog_lfg_{action}_{controls.group_id}",
            **kwargs,
        )
        self.controls = controls
        self.action = action

    async def callback(self, interaction: discord.Interaction):
        await self.controls.dispatch(self.action, interaction)


class CanonicalLFGControls(discord.ui.View):
    """Persistent controls for a QuestLog-owned LFG card."""

    def __init__(self, bot, group_id: int, role_schema=None, use_roles=False):
        super().__init__(timeout=None)
        self.bot = bot
        self.group_id = int(group_id)
        self.role_schema = role_schema or []
        self.use_roles = bool(use_roles)
        self.add_item(CanonicalLFGButton(
            self,
            "join",
            label="Join",
            emoji="✅",
            style=discord.ButtonStyle.success,
        ))
        self.add_item(CanonicalLFGButton(
            self,
            "leave",
            label="Leave",
            emoji="↩️",
            style=discord.ButtonStyle.secondary,
        ))
        self.add_item(CanonicalLFGButton(
            self,
            "setup",
            label="Update My Setup",
            emoji="🛠️",
            style=discord.ButtonStyle.primary,
        ))
        self.add_item(CanonicalLFGButton(
            self,
            "delete",
            label="Delete",
            emoji="🗑️",
            style=discord.ButtonStyle.danger,
        ))

    def _client(self):
        api_cog = self.bot.get_cog("LFGAPICog")
        return api_cog.client if api_cog else None

    async def _actor(self, interaction) -> Optional[int]:
        actor_id = _canonical_actor_id(interaction.user.id)
        if not actor_id:
            await _canonical_reply(
                interaction,
                "Link your Discord account to QuestLog before using this group.",
            )
        return actor_id

    async def dispatch(self, action: str, interaction: discord.Interaction):
        if action == "setup":
            await interaction.response.defer(ephemeral=True)
            actor_id = await self._actor(interaction)
            if not actor_id:
                return
            client = self._client()
            try:
                detail = await client.detail(self.group_id, actor_user_id=actor_id)
                group = _canonical_payload(detail)
                member = next((
                    item for item in group.get("members", [])
                    if int(item.get("user_id") or 0) == actor_id
                ), None)
                if not member:
                    await interaction.followup.send(
                        "Join the group before updating your setup.", ephemeral=True
                    )
                    return
                dashboard = CanonicalSetupDashboard(
                    self.bot, self.group_id, actor_id, group, member
                )
                if not dashboard.children:
                    await interaction.followup.send(
                        "This group does not ask players for any extra setup.",
                        ephemeral=True,
                    )
                    return
                await interaction.followup.send(
                    dashboard.summary(), view=dashboard, ephemeral=True
                )
            except (AttributeError, LFGAPIError) as error:
                await interaction.followup.send(_canonical_error(error), ephemeral=True)
            return
        if action == "delete":
            await interaction.response.send_message(
                "Delete this group from QuestLog and remove all of its "
                "Discord/Fluxer posts?",
                view=CanonicalDeleteConfirmView(self, interaction.user.id),
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)
        actor_id = await self._actor(interaction)
        if not actor_id:
            return
        client = self._client()
        try:
            if action == "join":
                await client.join_group(
                    self.group_id,
                    {"selections": {}},
                    actor_user_id=actor_id,
                    idempotency_key=f"discord:{interaction.id}:lfg:join",
                )
                message = "You joined the group on QuestLog."
            else:
                await client.leave_group(
                    self.group_id,
                    actor_user_id=actor_id,
                    idempotency_key=f"discord:{interaction.id}:lfg:leave",
                )
                message = "You left the group on QuestLog."
            await interaction.followup.send(message, ephemeral=True)
        except (AttributeError, LFGAPIError) as error:
            await interaction.followup.send(
                _canonical_error(error),
                ephemeral=True,
            )

    async def delete_group(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        actor_id = await self._actor(interaction)
        if not actor_id:
            return
        client = self._client()
        try:
            await client.delete_group(
                self.group_id,
                actor_user_id=actor_id,
                idempotency_key=f"discord:{interaction.id}:lfg:delete",
            )
            await interaction.followup.send(
                "Group deleted from active QuestLog. Its Discord and Fluxer "
                "posts are being removed now.",
                ephemeral=True,
            )
        except (AttributeError, LFGAPIError) as error:
            await interaction.followup.send(
                _canonical_error(error),
                ephemeral=True,
            )


class NetworkBroadcastsCog(commands.Cog):
    """
    Receives QuestLog Network LFG broadcasts and posts them to Discord channels.
    """

    def __init__(self, bot):
        self.bot = bot
        self._staff_board_refresh_tasks = {}
        self._last_staff_board_refresh = 0.0
        self._persistent_lfg_controls_registered = False
        self.poll_loop.start()
        logger.info("[NetworkBroadcasts] Cog loaded, poll loop starting.")

    def cog_unload(self):
        self.poll_loop.cancel()
        for task in self._staff_board_refresh_tasks.values():
            task.cancel()
        logger.info("[NetworkBroadcasts] Cog unloaded.")

    # ------------------------------------------------------------------
    # Background poll loop
    # ------------------------------------------------------------------

    @tasks.loop(seconds=POLL_INTERVAL)
    async def poll_loop(self):
        """Fetch pending broadcasts from DB and post to Discord channels."""
        try:
            await self._process_pending_broadcasts()
            now = time.monotonic()
            if now - self._last_staff_board_refresh >= 60:
                self._last_staff_board_refresh = now
                guild_id = self._configured_staff_board_guild_id()
                guild = self.bot.get_guild(guild_id) if guild_id else None
                if guild:
                    await self.refresh_staff_board_for_guild(guild)
        except Exception as e:
            logger.error(f"[NetworkBroadcasts] poll_loop error: {e}", exc_info=True)

    @poll_loop.before_loop
    async def before_poll_loop(self):
        await self.bot.wait_until_ready()
        await self._register_persistent_lfg_controls()

    @staticmethod
    def _role_schema(value: Any) -> list:
        if isinstance(value, list):
            return value
        if not value:
            return []
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except (TypeError, ValueError):
            return []

    def _lfg_control_view(self, embed_data: dict):
        group_id = embed_data.get("track_group_id")
        controls = embed_data.get("lfg_controls") or {}
        if not group_id or controls.get("enabled") is False:
            return None
        return CanonicalLFGControls(
            self.bot,
            int(group_id),
            self._role_schema(controls.get("role_schema")),
            controls.get("use_roles", False),
        )

    async def _register_persistent_lfg_controls(self):
        """Restore buttons after restart and upgrade active existing cards."""
        if self._persistent_lfg_controls_registered:
            return
        self._persistent_lfg_controls_registered = True
        try:
            with db_session_scope() as session:
                rows = session.execute(text(
                    "SELECT DISTINCT r.canonical_group_id,r.guild_id,"
                    "r.channel_id,r.message_id,g.role_schema,g.use_roles "
                    "FROM lfg_delivery_receipts r "
                    "JOIN web_lfg_groups g ON g.id=r.canonical_group_id "
                    "WHERE r.status='delivered' AND r.message_id IS NOT NULL "
                    "AND g.status IN ('open','full','started')"
                )).fetchall()
        except Exception as error:
            logger.warning(
                "[NetworkBroadcasts] Could not load persistent LFG controls: %s",
                error,
            )
            return

        seen = set()
        restored = 0
        for group_id, guild_id, channel_id, message_id, role_schema, use_roles in rows:
            key = (int(group_id), int(message_id))
            if key in seen:
                continue
            seen.add(key)
            view = CanonicalLFGControls(
                self.bot,
                int(group_id),
                self._role_schema(role_schema),
                bool(use_roles),
            )
            self.bot.add_view(view, message_id=int(message_id))
            restored += 1

            # Cards created before controls were introduced need their
            # components attached once. Failure here must not prevent their
            # already-registered callbacks from serving newer cards.
            try:
                guild = self.bot.get_guild(int(guild_id))
                if not guild:
                    continue
                channel = guild.get_channel(int(channel_id))
                if channel is None:
                    channel = await guild.fetch_channel(int(channel_id))
                message = await channel.fetch_message(int(message_id))
                await message.edit(view=view)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                continue
        if restored:
            logger.info(
                "[NetworkBroadcasts] Restored controls for %s canonical LFG cards",
                restored,
            )

    async def _process_pending_broadcasts(self):
        now_ts = int(time.time())
        stale_cutoff = now_ts - MAX_STALE_SECONDS

        with db_session_scope() as session:
            rows = session.execute(
                text(
                    "SELECT id, guild_id, channel_id, payload, created_at "
                    "FROM discord_pending_broadcasts "
                    "ORDER BY id ASC LIMIT 50"
                )
            ).fetchall()

            if not rows:
                return

        # Never hold a database transaction open across Discord or HTTP calls.
        for row in rows:
            row_id, guild_id, channel_id, payload_json, created_at = row
            try:
                embed_data = json.loads(payload_json)
                if not isinstance(embed_data, dict):
                    raise TypeError("payload must be a JSON object")
            except (json.JSONDecodeError, TypeError):
                logger.warning(
                    f"[NetworkBroadcasts] Row {row_id} has invalid JSON payload"
                )
                # Leave a fresh row available for inspection and upstream
                # retry. Only discard malformed legacy data after the stale
                # window, since no callback can be recovered from it.
                if created_at < stale_cutoff:
                    self._delete_queue_row(row_id)
                continue

            delivery_job_id = embed_data.get("delivery_job_id")
            callback_url = embed_data.get("delivery_callback_url")

            # Old queue rows have no callback contract. Preserve compatibility,
            # but do not let stale legacy rows live forever.
            if not delivery_job_id and created_at < stale_cutoff:
                logger.debug(
                    f"[NetworkBroadcasts] Dropping stale legacy row {row_id} "
                    f"(age={now_ts - created_at}s)"
                )
                self._delete_queue_row(row_id)
                continue

            if delivery_job_id:
                outcome = await self._process_canonical_delivery(
                    row_id,
                    str(delivery_job_id),
                    int(guild_id),
                    int(channel_id),
                    embed_data,
                    callback_url,
                    payload_json,
                )
                if outcome:
                    self._delete_queue_row(row_id)
                continue

            # Legacy work has no callback that can preserve a failure result.
            # Delete it only after Discord accepts the operation.
            outcome = await self._post_embed(
                row_id, int(guild_id), int(channel_id), embed_data
            )
            if outcome.status == "delivered":
                self._delete_queue_row(row_id)

    @staticmethod
    def _payload_hash(payload_json: str) -> str:
        return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

    @staticmethod
    def _delete_queue_row(row_id: int):
        with db_session_scope() as session:
            session.execute(
                text("DELETE FROM discord_pending_broadcasts WHERE id = :id"),
                {"id": int(row_id)},
            )

    @staticmethod
    def _load_delivery_receipt(delivery_job_id: str):
        with db_session_scope() as session:
            receipt = session.get(LFGDeliveryReceipt, delivery_job_id)
            if not receipt:
                return None
            return {
                "status": receipt.status,
                "payload_hash": receipt.payload_hash,
                "message_id": receipt.message_id,
                "thread_id": receipt.thread_id,
                "error_code": receipt.error_code,
                "error_message": receipt.error_message,
            }

    @staticmethod
    def _save_delivery_receipt(
        delivery_job_id: str,
        guild_id: int,
        channel_id: int,
        callback_url: Optional[str],
        payload_hash: str,
        embed_data: dict,
        outcome: DeliveryOutcome,
    ):
        now = int(time.time())
        canonical_group_id = embed_data.get("canonical_group_id") or embed_data.get(
            "track_group_id"
        )
        try:
            canonical_group_id = (
                int(canonical_group_id) if canonical_group_id is not None else None
            )
        except (TypeError, ValueError):
            canonical_group_id = None

        with db_session_scope() as session:
            receipt = session.get(LFGDeliveryReceipt, delivery_job_id)
            if not receipt:
                receipt = LFGDeliveryReceipt(
                    delivery_job_id=delivery_job_id,
                    created_at=now,
                )
                session.add(receipt)
            receipt.canonical_group_id = canonical_group_id
            receipt.action = embed_data.get("action", "post")
            receipt.status = outcome.status
            receipt.guild_id = guild_id
            receipt.channel_id = channel_id
            receipt.message_id = outcome.message_id
            receipt.thread_id = outcome.thread_id
            receipt.callback_url = callback_url
            receipt.payload_hash = payload_hash
            receipt.error_code = outcome.error_code
            receipt.error_message = outcome.error_message
            receipt.updated_at = now

    @staticmethod
    def _record_delivery_progress(
        delivery_job_id: Optional[str],
        *,
        message_id: Optional[int] = None,
        thread_id: Optional[int] = None,
    ):
        """Persist Discord IDs immediately to make crash recovery deduplicate."""
        if not delivery_job_id:
            return
        with db_session_scope() as session:
            receipt = session.get(LFGDeliveryReceipt, delivery_job_id)
            if not receipt:
                return
            if message_id is not None:
                receipt.message_id = message_id
            if thread_id is not None:
                receipt.thread_id = thread_id
            receipt.updated_at = int(time.time())

    @staticmethod
    def _delivery_nonce(delivery_job_id: Optional[str]) -> Optional[int]:
        if not delivery_job_id:
            return None
        digest = hashlib.sha256(delivery_job_id.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)

    @staticmethod
    def _claim_delivery(
        delivery_job_id: str,
        guild_id: int,
        channel_id: int,
        callback_url: Optional[str],
        payload_hash: str,
        embed_data: dict,
    ) -> bool:
        """Atomically claim a job, allowing recovery after ten minutes."""
        now = int(time.time())
        stale_before = now - 600
        canonical_group_id = embed_data.get("canonical_group_id") or embed_data.get(
            "track_group_id"
        )
        try:
            canonical_group_id = (
                int(canonical_group_id) if canonical_group_id is not None else None
            )
        except (TypeError, ValueError):
            canonical_group_id = None

        try:
            with db_session_scope() as session:
                receipt = session.get(LFGDeliveryReceipt, delivery_job_id)
                if receipt:
                    if receipt.status == "processing" and receipt.updated_at > stale_before:
                        return False
                    receipt.status = "processing"
                    receipt.updated_at = now
                    receipt.payload_hash = payload_hash
                    receipt.callback_url = callback_url
                    return True

                session.add(LFGDeliveryReceipt(
                    delivery_job_id=delivery_job_id,
                    canonical_group_id=canonical_group_id,
                    action=embed_data.get("action", "post"),
                    status="processing",
                    guild_id=guild_id,
                    channel_id=channel_id,
                    callback_url=callback_url,
                    payload_hash=payload_hash,
                    created_at=now,
                    updated_at=now,
                ))
                session.flush()
                return True
        except IntegrityError:
            # Another Warden process claimed the same unique job ID first.
            return False

    async def _process_canonical_delivery(
        self,
        row_id: int,
        delivery_job_id: str,
        guild_id: int,
        channel_id: int,
        embed_data: dict,
        callback_url: Optional[str],
        payload_json: str,
    ) -> bool:
        """Deliver once, persist the result, then acknowledge QuestLog."""
        payload_hash = self._payload_hash(payload_json)
        receipt = self._load_delivery_receipt(delivery_job_id)

        if receipt and receipt.get("status") in TERMINAL_DELIVERY_STATUSES:
            # The Discord operation already reached a terminal state. Retry
            # only the callback, never the platform send. QuestLog can enqueue
            # the same delivery job again with a refreshed presentation
            # payload after an acknowledgement timeout; the terminal receipt
            # still wins because this job attempt already has a final outcome.
            outcome = DeliveryOutcome(
                receipt["status"],
                message_id=receipt.get("message_id"),
                thread_id=receipt.get("thread_id"),
                error_code=receipt.get("error_code"),
                error_message=receipt.get("error_message"),
            )
        elif receipt and receipt.get("payload_hash") not in (None, payload_hash):
            outcome = DeliveryOutcome(
                "failed",
                error_code="delivery_job_payload_mismatch",
                error_message="A delivery job ID was reused with a different payload",
            )
        elif receipt and receipt.get("message_id") and (
            receipt.get("thread_id")
            or embed_data.get("action", "post") in {
                "direct", "upsert_direct", "edit", "delete"
            }
        ):
            # Discord accepted the core message before an ancillary operation
            # failed or the process stopped. Do not send it twice.
            outcome = DeliveryOutcome(
                "delivered",
                message_id=receipt.get("message_id"),
                thread_id=receipt.get("thread_id"),
            )
            self._save_delivery_receipt(
                delivery_job_id,
                guild_id,
                channel_id,
                callback_url,
                payload_hash,
                embed_data,
                outcome,
            )
        else:
            if not self._claim_delivery(
                delivery_job_id,
                guild_id,
                channel_id,
                callback_url,
                payload_hash,
                embed_data,
            ):
                logger.debug(
                    f"[NetworkBroadcasts] Delivery {delivery_job_id} is "
                    "already being processed"
                )
                return False
            outcome = await self._post_embed(
                row_id,
                guild_id,
                channel_id,
                embed_data,
                delivery_job_id=delivery_job_id,
                resume_thread_id=(receipt or {}).get("thread_id"),
                resume_message_id=(receipt or {}).get("message_id"),
            )
            self._save_delivery_receipt(
                delivery_job_id,
                guild_id,
                channel_id,
                callback_url,
                payload_hash,
                embed_data,
                outcome,
            )

        api_cog = self.bot.get_cog("LFGAPICog")
        client = api_cog.client if api_cog else None
        if not client or not client.configured:
            logger.error(
                f"[NetworkBroadcasts] Cannot acknowledge delivery "
                f"{delivery_job_id}: LFG API client is not configured"
            )
            return False

        details = {
            key: value for key, value in {
                "platform": "discord",
                "guild_id": str(guild_id),
                "channel_id": str(channel_id),
                "message_id": str(outcome.message_id) if outcome.message_id else None,
                "thread_id": str(outcome.thread_id) if outcome.thread_id else None,
                "error_code": outcome.error_code,
                "error_message": outcome.error_message,
            }.items() if value is not None
        }
        try:
            await client.acknowledge_delivery(
                delivery_job_id,
                outcome.status,
                idempotency_key=(
                    f"discord:delivery:{delivery_job_id}:{outcome.status}"
                ),
                callback_url=callback_url,
                details=details,
            )
            logger.info(
                f"[NetworkBroadcasts] Delivery {delivery_job_id} "
                f"acknowledged as {outcome.status}"
            )
            return True
        except LFGAPIError as error:
            if (
                error.code == "idempotency_conflict"
                and outcome.status in TERMINAL_DELIVERY_STATUSES
            ):
                # A delivery job ID represents one immutable attempt.  The
                # canonical service has already accepted a terminal result for
                # this ID, so retaining a duplicate queue row can only create
                # an endless callback loop.  A real retry must use a new job ID.
                logger.warning(
                    f"[NetworkBroadcasts] Delivery {delivery_job_id} was "
                    "already finalized by QuestLog; discarding duplicate "
                    "queue work"
                )
                return True
            logger.error(
                f"[NetworkBroadcasts] Callback failed for {delivery_job_id}: "
                f"{error.code}"
            )
            return False

    def _build_embed(self, embed_data: dict) -> discord.Embed:
        """Build a discord.Embed from a payload dict."""
        embed_url = _discord_http_url(embed_data.get("url"))
        if embed_data.get("url") and not embed_url:
            logger.warning(
                "[NetworkBroadcasts] Ignoring malformed embed URL in site payload"
            )
        embed = discord.Embed(
            title=embed_data.get("title", "New LFG Post"),
            description=embed_data.get("description", ""),
            url=embed_url,
            color=embed_data.get("color", BRAND_COLOR),
        )
        for field in embed_data.get("fields", []):
            embed.add_field(
                name=field.get("name", ""),
                value=field.get("value", ""),
                inline=field.get("inline", True),
            )
        thumbnail_url = _discord_http_url(embed_data.get("thumbnail"))
        if thumbnail_url:
            embed.set_thumbnail(url=thumbnail_url)
        elif embed_data.get("thumbnail"):
            logger.warning(
                "[NetworkBroadcasts] Ignoring malformed thumbnail URL in site payload"
            )
        embed.set_footer(text=embed_data.get("footer", "QuestLog Network"))
        return embed

    @staticmethod
    def _staff_board_state_key(guild_id):
        return f"{STAFF_BOARD_MESSAGE_KEY}_discord_{guild_id}"

    @staticmethod
    def _embed_signature(embed_data):
        visual = {
            key: embed_data.get(key)
            for key in (
                "title", "description", "url", "color", "fields",
                "thumbnail", "footer",
            )
        }
        return json.dumps(visual, sort_keys=True, separators=(",", ":"))

    def _load_staff_board_state(self, guild_id):
        try:
            with db_session_scope() as session:
                row = session.execute(
                    text("SELECT value FROM web_site_config WHERE `key` = :key LIMIT 1"),
                    {"key": self._staff_board_state_key(guild_id)},
                ).fetchone()
            return json.loads(row[0]) if row and row[0] else {}
        except Exception as error:
            logger.warning(
                f"[NetworkBroadcasts] Could not load staff board state "
                f"for guild {guild_id}: {error}"
            )
            return {}

    def _save_staff_board_state(self, guild_id, channel_id, message_id, signature):
        state = json.dumps({
            "channel_id": str(channel_id),
            "message_id": str(message_id),
            "signature": signature,
        }, separators=(",", ":"))
        now = int(time.time())
        with db_session_scope() as session:
            session.execute(
                text(
                    "INSERT INTO web_site_config (`key`, value, updated_at) "
                    "VALUES (:key, :value, :updated_at) "
                    "ON DUPLICATE KEY UPDATE "
                    "value = :value, updated_at = :updated_at"
                ),
                {
                    "key": self._staff_board_state_key(guild_id),
                    "value": state,
                    "updated_at": now,
                },
            )

    async def _upsert_direct_embed(self, guild, channel, embed_data):
        """Create a direct embed once, then edit that same channel message."""
        signature = self._embed_signature(embed_data)
        state = self._load_staff_board_state(guild.id)
        message = None

        if (
            str(state.get("channel_id")) == str(channel.id)
            and state.get("message_id")
        ):
            try:
                message = await channel.fetch_message(int(state["message_id"]))
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                message = None

        # Adopt a direct board posted before message tracking was added.
        if message is None:
            expected_title = embed_data.get("title", "")
            expected_footer = embed_data.get("footer", "QuestLog Network")
            try:
                async for candidate in channel.history(limit=50):
                    if candidate.author.id != self.bot.user.id or not candidate.embeds:
                        continue
                    current = candidate.embeds[0]
                    if (
                        current.title == expected_title
                        and current.footer
                        and current.footer.text == expected_footer
                    ):
                        message = candidate
                        break
            except (discord.Forbidden, discord.HTTPException):
                pass

        embed = self._build_embed(embed_data)
        if message is None:
            message = await channel.send(embed=embed)
            logger.info(
                f"[NetworkBroadcasts] Created staff board in "
                f"{guild.name} #{channel.name}"
            )
        elif state.get("signature") != signature:
            await message.edit(embed=embed)
            logger.info(
                f"[NetworkBroadcasts] Updated staff board in "
                f"{guild.name} #{channel.name}"
            )

        self._save_staff_board_state(
            guild.id, channel.id, message.id, signature
        )
        return message

    def _selected_staff_role_ids(self, guild_id):
        try:
            with db_session_scope() as session:
                row = session.execute(
                    text("SELECT value FROM web_site_config WHERE `key` = :key LIMIT 1"),
                    {"key": STAFF_BOARD_CONFIG_KEY},
                ).fetchone()
            config = json.loads(row[0]) if row and row[0] else {}
            target = config.get("discord", {})
            if (
                not target.get("enabled")
                or str(target.get("guild_id")) != str(guild_id)
            ):
                return set()
            return {
                str(role.get("id"))
                for role in target.get("roles", [])
                if role.get("id")
            }
        except Exception as error:
            logger.warning(
                f"[NetworkBroadcasts] Could not load selected staff roles "
                f"for guild {guild_id}: {error}"
            )
            return set()

    def _configured_staff_board_guild_id(self):
        try:
            with db_session_scope() as session:
                row = session.execute(
                    text("SELECT value FROM web_site_config WHERE `key` = :key LIMIT 1"),
                    {"key": STAFF_BOARD_CONFIG_KEY},
                ).fetchone()
            config = json.loads(row[0]) if row and row[0] else {}
            target = config.get("discord", {})
            if not target.get("enabled") or not target.get("guild_id"):
                return None
            return int(target["guild_id"])
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

    async def refresh_staff_board_for_guild(self, guild):
        """Resolve selected Discord roles live and update the tracked board."""
        try:
            with db_session_scope() as session:
                row = session.execute(
                    text("SELECT value FROM web_site_config WHERE `key` = :key LIMIT 1"),
                    {"key": STAFF_BOARD_CONFIG_KEY},
                ).fetchone()
            config = json.loads(row[0]) if row and row[0] else {}
            target = config.get("discord", {})
            if (
                not target.get("enabled")
                or str(target.get("guild_id")) != str(guild.id)
            ):
                return

            channel_id = target.get("channel_id")
            if not channel_id:
                return
            channel = guild.get_channel(int(channel_id))
            if not channel or not isinstance(channel, discord.TextChannel):
                return

            fields = []
            for selected in target.get("roles", []):
                role_id = selected.get("id")
                role = guild.get_role(int(role_id)) if role_id else None
                if role is None:
                    continue
                members = [
                    member for member in role.members if not member.bot
                ]
                value = (
                    "\n".join(f"• <@{member.id}>" for member in members)
                    if members else "No members currently assigned."
                )
                fields.append({
                    "name": f"{selected.get('emoji', '🛡️')} {role.name}",
                    "value": value,
                    "inline": False,
                })

            if not fields:
                return
            embed_data = {
                "action": "upsert_direct",
                "message_key": STAFF_BOARD_MESSAGE_KEY,
                "title": config.get("title", "Meet the Team"),
                "description": config.get("description", ""),
                "color": int(
                    str(config.get("color", "#5865F2")).lstrip("#"), 16
                ),
                "fields": fields,
                "footer": config.get("footer", "QuestLog"),
            }
            await self._upsert_direct_embed(guild, channel, embed_data)
        except Exception as error:
            logger.error(
                f"[NetworkBroadcasts] Staff board refresh failed for "
                f"guild {guild.id}: {error}",
                exc_info=True,
            )

    def _queue_staff_board_refresh(self, guild):
        existing = self._staff_board_refresh_tasks.get(guild.id)
        if existing and not existing.done():
            existing.cancel()

        async def refresh_after_role_events_settle():
            await asyncio.sleep(2)
            await self.refresh_staff_board_for_guild(guild)

        self._staff_board_refresh_tasks[guild.id] = asyncio.create_task(
            refresh_after_role_events_settle()
        )

    @commands.Cog.listener()
    async def on_member_update(self, before, after):
        before_roles = {str(role.id) for role in before.roles}
        after_roles = {str(role.id) for role in after.roles}
        changed_roles = before_roles ^ after_roles
        if changed_roles & self._selected_staff_role_ids(after.guild.id):
            self._queue_staff_board_refresh(after.guild)

    @commands.Cog.listener()
    async def on_member_join(self, member):
        member_roles = {str(role.id) for role in member.roles}
        if member_roles & self._selected_staff_role_ids(member.guild.id):
            self._queue_staff_board_refresh(member.guild)

    @commands.Cog.listener()
    async def on_member_remove(self, member):
        member_roles = {str(role.id) for role in member.roles}
        if member_roles & self._selected_staff_role_ids(member.guild.id):
            self._queue_staff_board_refresh(member.guild)

    @staticmethod
    def _find_canonical_binding(canonical_group_id, guild_id):
        if canonical_group_id is None:
            return None
        try:
            with db_session_scope() as session:
                receipt = session.execute(
                    text(
                        "SELECT message_id, thread_id, channel_id "
                        "FROM lfg_delivery_receipts "
                        "WHERE canonical_group_id=:group_id AND guild_id=:guild_id "
                        "AND status='delivered' AND message_id IS NOT NULL "
                        "ORDER BY updated_at DESC LIMIT 1"
                    ),
                    {
                        "group_id": int(canonical_group_id),
                        "guild_id": int(guild_id),
                    },
                ).fetchone()
                if receipt:
                    return receipt
                # Discord-created groups bind their canonical ID directly to
                # the bot-owned thread during the compatibility window. They
                # do not necessarily have an outbox delivery receipt.
                return session.execute(
                    text(
                        "SELECT management_message_id, thread_id, NULL "
                        "FROM lfg_groups WHERE canonical_group_id=:group_id "
                        "AND guild_id=:guild_id ORDER BY id DESC LIMIT 1"
                    ),
                    {
                        "group_id": int(canonical_group_id),
                        "guild_id": int(guild_id),
                    },
                ).fetchone()
        except (TypeError, ValueError):
            return None

    async def _post_embed(
        self,
        row_id,
        guild_id,
        channel_id,
        embed_data,
        *,
        delivery_job_id: Optional[str] = None,
        resume_thread_id: Optional[int] = None,
        resume_message_id: Optional[int] = None,
    ):
        """Build and post a Discord embed from the site payload, creating a thread."""
        sent = None
        thread = None
        nonce = self._delivery_nonce(delivery_job_id)
        try:
            guild = self.bot.get_guild(guild_id)
            if not guild:
                return DeliveryOutcome(
                    "destination_missing",
                    error_code="bot_not_installed",
                    error_message=f"Discord bot is not installed in guild {guild_id}",
                )

            action = embed_data.get("action", "post")
            track_group_id = embed_data.get("track_group_id")
            track_group_platform = embed_data.get("track_group_platform", "web")

            # --- Delete a bound LFG thread/message ---
            # Deletion is deliberately different from completion: completed
            # threads remain as history, while an explicit delete removes the
            # Discord surface. Missing bindings and already-deleted threads are
            # successful final states, making retries idempotent.
            if action == "delete" and track_group_id:
                existing = self._find_canonical_binding(track_group_id, guild_id)
                if not existing and LFG_LEGACY_WRITES_ENABLED:
                    with db_session_scope() as session:
                        existing = session.execute(
                            text(
                                "SELECT message_id, thread_id, channel_id "
                                "FROM web_lfg_channel_messages "
                                "WHERE group_id=:gid AND group_platform=:gp "
                                "AND platform='discord' AND guild_id=:guild "
                                "ORDER BY created_at DESC LIMIT 1"
                            ),
                            {
                                "gid": int(track_group_id),
                                "gp": track_group_platform,
                                "guild": str(guild_id),
                            },
                        ).fetchone()
                if not existing:
                    return DeliveryOutcome("delivered")

                stored_msg_id, stored_thread_id, stored_ch_id = existing
                if stored_thread_id:
                    try:
                        get_thread = getattr(guild, "get_thread", None)
                        target = get_thread(int(stored_thread_id)) if get_thread else None
                        target = target or guild.get_channel(int(stored_thread_id))
                        if target is None:
                            target = await guild.fetch_channel(int(stored_thread_id))
                        # Pycord's Thread.delete() does not accept the audit-log
                        # ``reason`` keyword (unlike TextChannel.delete()).
                        await target.delete()
                    except discord.NotFound:
                        pass
                if stored_msg_id and stored_ch_id:
                    try:
                        target = guild.get_channel(int(stored_ch_id))
                        if target is None:
                            target = await guild.fetch_channel(int(stored_ch_id))
                        message = await target.fetch_message(int(stored_msg_id))
                        await message.delete()
                    except discord.NotFound:
                        # Legacy bindings store the embed inside the thread;
                        # deleting that thread already removed the message.
                        pass
                return DeliveryOutcome(
                    "delivered",
                    message_id=int(stored_msg_id) if stored_msg_id else None,
                    thread_id=int(stored_thread_id) if stored_thread_id else None,
                )

            # --- Direct channel post (no thread) ---
            if action in ("direct", "upsert_direct"):
                channel = guild.get_channel(channel_id)
                if not channel or not isinstance(channel, discord.TextChannel):
                    return DeliveryOutcome(
                        "destination_missing",
                        error_code="channel_not_found",
                        error_message=f"Discord channel {channel_id} was not found",
                    )
                if action == "upsert_direct":
                    sent = await self._upsert_direct_embed(
                        guild, channel, embed_data
                    )
                    self._record_delivery_progress(
                        delivery_job_id, message_id=sent.id
                    )
                    return DeliveryOutcome("delivered", message_id=sent.id)
                embed = self._build_embed(embed_data)
                sent = await channel.send(
                    embed=embed,
                    nonce=nonce,
                    enforce_nonce=nonce is not None,
                )
                self._record_delivery_progress(
                    delivery_job_id, message_id=sent.id
                )
                logger.info(f"[NetworkBroadcasts] Posted direct embed to {guild.name} #{channel.name}")
                return DeliveryOutcome("delivered", message_id=sent.id)

            # --- Edit existing thread message in-place ---
            if action == "edit" and track_group_id:
                existing = self._find_canonical_binding(track_group_id, guild_id)
                if not existing and LFG_LEGACY_WRITES_ENABLED:
                    try:
                        with db_session_scope() as session:
                            existing = session.execute(
                                text(
                                    "SELECT message_id, thread_id, channel_id FROM web_lfg_channel_messages "
                                    "WHERE group_id=:gid AND group_platform=:gp AND platform='discord' AND guild_id=:guild "
                                    "LIMIT 1"
                                ),
                                {"gid": int(track_group_id), "gp": track_group_platform, "guild": str(guild_id)},
                            ).fetchone()
                    except Exception as db_err:
                        logger.warning(f"[NetworkBroadcasts] DB lookup failed for edit row {row_id}: {db_err}")
                        return DeliveryOutcome(
                            "retrying",
                            error_code="binding_lookup_failed",
                            error_message=str(db_err),
                        )

                if not existing:
                    logger.debug(f"[NetworkBroadcasts] No stored message for group {track_group_id} in guild {guild_id}, skipping edit")
                    return DeliveryOutcome(
                        "destination_missing",
                        error_code="message_binding_not_found",
                        error_message="No Discord message is bound to this LFG group",
                    )

                stored_msg_id, stored_thread_id, stored_ch_id = existing
                embed = self._build_embed(embed_data)
                try:
                    # New LFG broadcasts keep the visible card in the parent
                    # channel. Older bindings kept it inside the thread, so
                    # retain that as a compatibility fallback.
                    msg = None
                    if stored_ch_id:
                        parent = guild.get_channel(int(stored_ch_id))
                        if parent is None:
                            parent = await guild.fetch_channel(int(stored_ch_id))
                        try:
                            msg = await parent.fetch_message(int(stored_msg_id))
                        except discord.NotFound:
                            msg = None
                    if msg is None and stored_thread_id:
                        target_thread = guild.get_channel(int(stored_thread_id))
                        if target_thread is None:
                            target_thread = await guild.fetch_channel(int(stored_thread_id))
                        msg = await target_thread.fetch_message(int(stored_msg_id))
                    if msg is None:
                        return DeliveryOutcome(
                            "destination_missing",
                            error_code="bound_message_not_found",
                            error_message=f"Discord message {stored_msg_id} was not found",
                        )
                    edit_kwargs = {"embed": embed}
                    controls = self._lfg_control_view(embed_data)
                    if controls is not None:
                        edit_kwargs["view"] = controls
                    await msg.edit(**edit_kwargs)
                    self._record_delivery_progress(
                        delivery_job_id,
                        message_id=msg.id,
                        thread_id=(
                            int(stored_thread_id) if stored_thread_id else None
                        ),
                    )
                    pin_state = embed_data.get("pin_state")
                    if pin_state == "unpin":
                        await msg.unpin()
                    elif pin_state == "pin":
                        await msg.pin()
                    logger.info(f"[NetworkBroadcasts] Edited embed for group {track_group_id} in guild {guild.name}")
                    return DeliveryOutcome(
                        "delivered",
                        message_id=msg.id,
                        thread_id=int(stored_thread_id) if stored_thread_id else None,
                    )
                except discord.NotFound:
                    return DeliveryOutcome(
                        "destination_missing",
                        error_code="bound_message_not_found",
                        error_message=f"Discord message {stored_msg_id} was not found",
                    )

            # --- Post new: visible parent card with an attached discussion thread ---
            channel = guild.get_channel(channel_id)
            if not channel or not isinstance(channel, discord.TextChannel):
                return DeliveryOutcome(
                    "destination_missing",
                    error_code="channel_not_found",
                    error_message=f"Discord channel {channel_id} was not found",
                )

            thread_name = embed_data.get("thread_name") or embed_data.get("title", "LFG Group")
            thread_name = thread_name[:100]  # Discord 100-char limit

            embed = self._build_embed(embed_data)
            if resume_message_id:
                sent = await channel.fetch_message(int(resume_message_id))
            else:
                sent = await channel.send(
                    embed=embed,
                    view=self._lfg_control_view(embed_data),
                    nonce=nonce,
                    enforce_nonce=nonce is not None,
                )
                self._record_delivery_progress(
                    delivery_job_id, message_id=sent.id
                )

            if resume_thread_id:
                thread = guild.get_channel(int(resume_thread_id))
                if thread is None:
                    thread = await guild.fetch_channel(int(resume_thread_id))
            else:
                thread = await sent.create_thread(
                    name=thread_name,
                    auto_archive_duration=10080,  # 7 days
                )
                self._record_delivery_progress(
                    delivery_job_id,
                    message_id=sent.id,
                    thread_id=thread.id,
                )
            logger.info(
                f"[NetworkBroadcasts] Posted visible LFG card and attached "
                f"thread '{thread.name}' in {guild.name} #{channel.name}"
            )
            await sent.pin()
            logger.info(f"[NetworkBroadcasts] Pinned LFG card {sent.id}")

            # Store thread_id and message_id
            if track_group_id and sent:
                now_ts = int(time.time())
                try:
                    if not LFG_LEGACY_WRITES_ENABLED:
                        raise RuntimeError("legacy binding writes disabled")
                    with db_session_scope() as session:
                        session.execute(
                            text(
                                "DELETE FROM web_lfg_channel_messages "
                                "WHERE group_id=:gid AND group_platform=:gp AND platform='discord' AND guild_id=:guild"
                            ),
                            {"gid": int(track_group_id), "gp": track_group_platform, "guild": str(guild_id)},
                        )
                        session.execute(
                            text(
                                "INSERT INTO web_lfg_channel_messages "
                                "(group_id, group_platform, platform, guild_id, channel_id, message_id, thread_id, created_at) "
                                "VALUES (:gid, :gp, 'discord', :guild, :ch, :mid, :tid, :ts)"
                            ),
                            {
                                "gid": int(track_group_id),
                                "gp": track_group_platform,
                                "guild": str(guild_id),
                                "ch": str(channel_id),
                                "mid": str(sent.id),
                                "tid": str(thread.id),
                                "ts": now_ts,
                            },
                        )
                except Exception as db_err:
                    if LFG_LEGACY_WRITES_ENABLED:
                        logger.warning(f"[NetworkBroadcasts] Failed to store thread/message ID for group {track_group_id}: {db_err}")

            return DeliveryOutcome(
                "delivered", message_id=sent.id, thread_id=thread.id
            )

        except discord.Forbidden:
            logger.warning(f"[NetworkBroadcasts] No permission in channel {channel_id} (guild {guild_id})")
            return DeliveryOutcome(
                "permission_missing",
                message_id=sent.id if sent else None,
                thread_id=thread.id if thread else None,
                error_code="discord_permission_missing",
                error_message="Discord denied the required channel or thread permission",
            )
        except discord.NotFound:
            return DeliveryOutcome(
                "destination_missing",
                message_id=sent.id if sent else None,
                thread_id=thread.id if thread else None,
                error_code="discord_destination_missing",
                error_message="The Discord destination no longer exists",
            )
        except discord.HTTPException as e:
            logger.error(f"[NetworkBroadcasts] HTTP error for {channel_id}: {e}")
            retryable = e.status == 429 or e.status >= 500
            return DeliveryOutcome(
                "retrying" if retryable else "failed",
                message_id=sent.id if sent else None,
                thread_id=thread.id if thread else None,
                error_code=f"discord_http_{e.status}",
                error_message=str(e),
            )
        except Exception as e:
            logger.error(f"[NetworkBroadcasts] Unexpected error posting row {row_id}: {e}", exc_info=True)
            return DeliveryOutcome(
                "failed",
                message_id=sent.id if sent else None,
                thread_id=thread.id if thread else None,
                error_code="discord_delivery_failed",
                error_message=str(e),
            )

    # ------------------------------------------------------------------
    # Slash command group: /questlog-network
    # ------------------------------------------------------------------

    ql_network = discord.SlashCommandGroup(
        "questlog-network",
        "QuestLog Network - receive LFG broadcasts from the QuestLog community",
    )

    @ql_network.command(name="setup", description="Subscribe this server to QuestLog Network LFG broadcasts")
    @discord.default_permissions(administrator=True)
    @discord.option("channel", discord.TextChannel, description="Channel to receive LFG broadcast embeds", required=True)
    async def network_setup(self, ctx: discord.ApplicationContext, channel: discord.TextChannel):
        """Subscribe this Discord guild to QuestLog Network LFG broadcasts."""
        await ctx.defer(ephemeral=True)

        guild_id = str(ctx.guild.id)
        channel_id = str(channel.id)
        now_ts = int(time.time())

        try:
            with db_session_scope() as session:
                existing = session.execute(
                    text(
                        "SELECT id FROM web_community_bot_configs "
                        "WHERE platform='discord' AND guild_id=:gid AND event_type='lfg_announce' "
                        "LIMIT 1"
                    ),
                    {"gid": guild_id}
                ).fetchone()

                if existing:
                    session.execute(
                        text(
                            "UPDATE web_community_bot_configs "
                            "SET channel_id=:cid, channel_name=:cname, guild_name=:gname, "
                            "    is_enabled=1, updated_at=:now "
                            "WHERE platform='discord' AND guild_id=:gid AND event_type='lfg_announce'"
                        ),
                        {
                            "cid": channel_id,
                            "cname": channel.name,
                            "gname": ctx.guild.name,
                            "now": now_ts,
                            "gid": guild_id,
                        }
                    )
                    action = "updated"
                else:
                    session.execute(
                        text(
                            "INSERT INTO web_community_bot_configs "
                            "(platform, guild_id, guild_name, channel_id, channel_name, event_type, is_enabled, created_at, updated_at) "
                            "VALUES ('discord', :gid, :gname, :cid, :cname, 'lfg_announce', 1, :now, :now)"
                        ),
                        {
                            "gid": guild_id,
                            "gname": ctx.guild.name,
                            "cid": channel_id,
                            "cname": channel.name,
                            "now": now_ts,
                        }
                    )
                    action = "registered"

            embed = discord.Embed(
                title="QuestLog Network - LFG Broadcasts Enabled",
                description=(
                    f"This server is now **{action}** to receive QuestLog Network LFG broadcasts.\n\n"
                    f"New LFG groups posted at **casual-heroes.com/ql/lfg/** will be "
                    f"forwarded to {channel.mention} when their creators click 'Broadcast to Network'."
                ),
                color=discord.Color.green(),
            )
            embed.add_field(name="Channel", value=channel.mention, inline=True)
            embed.add_field(name="Event", value="LFG Announce", inline=True)
            embed.set_footer(text="casual-heroes.com/ql/ - QuestLog Network")
            await ctx.respond(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"[NetworkBroadcasts] setup error for guild {guild_id}: {e}", exc_info=True)
            await ctx.respond("Something went wrong setting up network broadcasts. Please try again.", ephemeral=True)

    @ql_network.command(name="status", description="Check this server's QuestLog Network subscription status")
    @discord.default_permissions(administrator=True)
    async def network_status(self, ctx: discord.ApplicationContext):
        """Show current QuestLog Network subscription config for this guild."""
        await ctx.defer(ephemeral=True)

        guild_id = str(ctx.guild.id)

        try:
            with db_session_scope() as session:
                row = session.execute(
                    text(
                        "SELECT channel_id, channel_name, is_enabled, updated_at "
                        "FROM web_community_bot_configs "
                        "WHERE platform='discord' AND guild_id=:gid AND event_type='lfg_announce' "
                        "LIMIT 1"
                    ),
                    {"gid": guild_id}
                ).fetchone()

            if not row:
                embed = discord.Embed(
                    title="QuestLog Network - Not Subscribed",
                    description=(
                        "This server is not subscribed to QuestLog Network LFG broadcasts.\n\n"
                        "Use `/questlog-network setup` to start receiving LFG posts from the network."
                    ),
                    color=discord.Color.light_grey(),
                )
                embed.set_footer(text="casual-heroes.com/ql/")
                await ctx.respond(embed=embed, ephemeral=True)
                return

            channel_id, channel_name, is_enabled, updated_at = row
            channel = ctx.guild.get_channel(int(channel_id)) if channel_id else None
            status_str = "Active" if is_enabled else "Paused"

            embed = discord.Embed(
                title="QuestLog Network - Subscription Status",
                color=discord.Color.green() if is_enabled else discord.Color.orange(),
            )
            embed.add_field(name="Status", value=status_str, inline=True)
            embed.add_field(
                name="Channel",
                value=channel.mention if channel else f"#{channel_name or channel_id} (not found)",
                inline=True,
            )
            embed.add_field(name="Event", value="LFG Announce", inline=True)
            embed.set_footer(text="Use /questlog-network setup to change channel | casual-heroes.com/ql/")
            await ctx.respond(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"[NetworkBroadcasts] status error for guild {guild_id}: {e}", exc_info=True)
            await ctx.respond("Something went wrong checking status.", ephemeral=True)

    @ql_network.command(name="disable", description="Stop receiving QuestLog Network LFG broadcasts")
    @discord.default_permissions(administrator=True)
    async def network_disable(self, ctx: discord.ApplicationContext):
        """Unsubscribe this Discord guild from QuestLog Network LFG broadcasts."""
        await ctx.defer(ephemeral=True)

        guild_id = str(ctx.guild.id)
        now_ts = int(time.time())

        try:
            with db_session_scope() as session:
                result = session.execute(
                    text(
                        "UPDATE web_community_bot_configs SET is_enabled=0, updated_at=:now "
                        "WHERE platform='discord' AND guild_id=:gid AND event_type='lfg_announce'"
                    ),
                    {"now": now_ts, "gid": guild_id}
                )
                affected = result.rowcount

            if affected:
                await ctx.respond(
                    "QuestLog Network LFG broadcasts have been **disabled** for this server.\n"
                    "Use `/questlog-network setup` to re-enable at any time.",
                    ephemeral=True,
                )
            else:
                await ctx.respond(
                    "This server wasn't subscribed to QuestLog Network broadcasts.",
                    ephemeral=True,
                )

        except Exception as e:
            logger.error(f"[NetworkBroadcasts] disable error for guild {guild_id}: {e}", exc_info=True)
            await ctx.respond("Something went wrong. Please try again.", ephemeral=True)


def setup(bot: commands.Bot):
    bot.add_cog(NetworkBroadcastsCog(bot))
