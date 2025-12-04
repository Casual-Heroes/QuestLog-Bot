# cogs/action_processor.py - Website Action Queue Processor
"""
Processes actions queued from the Django web dashboard.

This cog polls the pending_actions table and executes Discord actions
triggered by users from the website. This enables full MEE6-style
functionality where users can do everything from the website that
they can do with bot commands.

ARCHITECTURE:
- Website writes PendingAction records to database
- This cog polls every 2 seconds for new actions
- Actions are processed in priority order (1=highest)
- Results are written back to the database
- Failed actions are retried up to max_retries times
"""

import json
import time
import asyncio
import discord
import aiohttp
import urllib.parse
from discord.ext import commands, tasks

from config import db_session_scope, logger
from models import (
    PendingAction, ActionStatus, ActionType,
    GuildMember, Guild, Warning, WarningType,
    BulkImportJob, DiscoveryConfig
)


class ActionProcessorCog(commands.Cog):
    """Processes pending actions from the web dashboard."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.processing_lock = asyncio.Lock()
        # Start the processing loop
        self.process_actions_loop.start()

    def cog_unload(self):
        """Stop the loop when cog unloads."""
        self.process_actions_loop.cancel()
        logger.info("ActionProcessor: Stopped action queue processor")

    async def _lookup_steam_url(self, game_name: str) -> str:
        """
        Lookup Steam store page URL via Steam API.

        Args:
            game_name: Name of the game to search for

        Returns:
            Direct Steam store URL if found, otherwise search URL
        """
        try:
            # Use Steam's store search API
            search_term = urllib.parse.quote(game_name)
            api_url = f"https://store.steampowered.com/api/storesearch/?term={search_term}&cc=US"

            async with aiohttp.ClientSession() as session:
                async with session.get(api_url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                    if response.status == 200:
                        data = await response.json()

                        # Check if we got results
                        if data.get('total', 0) > 0 and data.get('items'):
                            # Get the first result (usually the best match)
                            first_result = data['items'][0]
                            app_id = first_result.get('id')

                            if app_id:
                                # Construct direct Steam store page URL
                                steam_url = f"https://store.steampowered.com/app/{app_id}/"
                                logger.info(f"Steam API: Found direct link for '{game_name}': {steam_url}")
                                return steam_url

            # Fallback to search URL if API call failed or no results
            logger.debug(f"Steam API: No direct link found for '{game_name}', using search URL")

        except asyncio.TimeoutError:
            logger.warning(f"Steam API: Timeout looking up '{game_name}'")
        except Exception as e:
            logger.warning(f"Steam API: Error looking up '{game_name}': {e}")

        # Fallback to search URL
        search_term = urllib.parse.quote(game_name)
        return f"https://store.steampowered.com/search/?term={search_term}"

    @tasks.loop(seconds=2.0)
    async def process_actions_loop(self):
        """Poll for and process pending actions every 2 seconds."""
        if self.processing_lock.locked():
            return  # Skip if still processing previous batch

        async with self.processing_lock:
            try:
                await self._process_pending_actions()
            except Exception as e:
                logger.error(f"ActionProcessor: Error in processing loop: {e}")

    @process_actions_loop.before_loop
    async def before_process_loop(self):
        """Wait for bot to be ready before starting."""
        await self.bot.wait_until_ready()
        logger.info("ActionProcessor: Bot ready, starting action queue processor")

    async def _process_pending_actions(self):
        """Fetch and process all pending actions."""
        with db_session_scope() as session:
            # Get pending actions ordered by priority and creation time
            actions = session.query(PendingAction).filter(
                PendingAction.status == ActionStatus.PENDING
            ).order_by(
                PendingAction.priority,
                PendingAction.created_at
            ).limit(10).all()  # Process up to 10 at a time

            for action in actions:
                await self._process_single_action(session, action)

    async def _process_single_action(self, session, action: PendingAction):
        """Process a single pending action."""
        try:
            # Mark as processing
            action.status = ActionStatus.PROCESSING
            action.started_at = int(time.time())
            session.commit()

            # Parse payload
            payload = json.loads(action.payload) if action.payload else {}

            # Get the guild
            guild = self.bot.get_guild(action.guild_id)
            if not guild:
                raise ValueError(f"Guild {action.guild_id} not found (bot may not be in guild)")

            # Process based on action type
            result = await self._execute_action(guild, action.action_type, payload)

            # Mark as completed
            action.status = ActionStatus.COMPLETED
            action.completed_at = int(time.time())
            action.result = json.dumps(result) if result else None

            logger.info(f"ActionProcessor: Completed {action.action_type.value} for guild {action.guild_id}")

        except Exception as e:
            # Handle failure
            action.retry_count += 1
            action.error_message = str(e)

            if action.retry_count >= action.max_retries:
                action.status = ActionStatus.FAILED
                action.completed_at = int(time.time())
                logger.error(f"ActionProcessor: Failed {action.action_type.value} for guild {action.guild_id}: {e}")
            else:
                # Reset to pending for retry
                action.status = ActionStatus.PENDING
                logger.warning(f"ActionProcessor: Retrying {action.action_type.value} (attempt {action.retry_count})")

        finally:
            session.commit()

    async def _execute_action(self, guild: discord.Guild, action_type: ActionType, payload: dict) -> dict:
        """Execute the action and return result."""

        # Role Management
        if action_type == ActionType.ROLE_ADD:
            return await self._action_role_add(guild, payload)
        elif action_type == ActionType.ROLE_REMOVE:
            return await self._action_role_remove(guild, payload)
        elif action_type == ActionType.ROLE_BULK_ADD:
            return await self._action_role_bulk_add(guild, payload)
        elif action_type == ActionType.ROLE_BULK_REMOVE:
            return await self._action_role_bulk_remove(guild, payload)

        # XP Management
        elif action_type == ActionType.XP_ADD:
            return await self._action_xp_modify(guild, payload, "add")
        elif action_type == ActionType.XP_REMOVE:
            return await self._action_xp_modify(guild, payload, "remove")
        elif action_type == ActionType.XP_SET:
            return await self._action_xp_modify(guild, payload, "set")
        elif action_type == ActionType.XP_BULK_SET:
            return await self._action_xp_bulk_set(guild, payload)
        elif action_type == ActionType.TOKENS_ADD:
            return await self._action_tokens_modify(guild, payload, "add")
        elif action_type == ActionType.TOKENS_REMOVE:
            return await self._action_tokens_modify(guild, payload, "remove")

        # Member Management
        elif action_type == ActionType.MEMBER_KICK:
            return await self._action_member_kick(guild, payload)
        elif action_type == ActionType.MEMBER_BAN:
            return await self._action_member_ban(guild, payload)
        elif action_type == ActionType.MEMBER_UNBAN:
            return await self._action_member_unban(guild, payload)
        elif action_type == ActionType.MEMBER_TIMEOUT:
            return await self._action_member_timeout(guild, payload)
        elif action_type == ActionType.MEMBER_UNTIMEOUT:
            return await self._action_member_untimeout(guild, payload)

        # Moderation
        elif action_type == ActionType.WARNING_ADD:
            return await self._action_warning_add(guild, payload)
        elif action_type == ActionType.WARNING_PARDON:
            return await self._action_warning_pardon(guild, payload)

        # Messages
        elif action_type == ActionType.MESSAGE_SEND:
            return await self._action_message_send(guild, payload)
        elif action_type == ActionType.DM_SEND:
            return await self._action_dm_send(guild, payload)

        # Channel Management
        elif action_type == ActionType.CHANNEL_TOPIC_SET:
            return await self._action_channel_topic(guild, payload)

        # Discovery/Self-Promo
        elif action_type == ActionType.FORCE_FEATURE:
            return await self._action_force_feature(guild, payload)
        elif action_type == ActionType.CLEAR_FEATURED:
            return await self._action_clear_featured(guild, payload)
        elif action_type == ActionType.TEST_CHANNEL_EMBED:
            return await self._action_test_channel_embed(guild, payload)
        elif action_type == ActionType.TEST_FORUM_EMBED:
            return await self._action_test_forum_embed(guild, payload)
        elif action_type == ActionType.CHECK_GAMES:
            return await self._action_check_games(guild, payload)

        # Flair Management
        elif action_type == ActionType.FLAIR_ASSIGN:
            return await self._action_flair_assign(guild, payload)

        # Template Management
        elif action_type == ActionType.CHANNEL_CREATE:
            return await self._action_channel_create(guild, payload)
        elif action_type == ActionType.ROLE_CREATE:
            return await self._action_role_create(guild, payload)

        else:
            raise ValueError(f"Unknown action type: {action_type}")

    # ═══════════════════════════════════════════════════════════════
    # ROLE ACTIONS
    # ═══════════════════════════════════════════════════════════════

    async def _action_role_add(self, guild: discord.Guild, payload: dict) -> dict:
        """Add a role to a user."""
        user_id = int(payload["user_id"])
        role_id = int(payload["role_id"])
        reason = payload.get("reason", "Added via web dashboard")

        member = guild.get_member(user_id)
        if not member:
            raise ValueError(f"Member {user_id} not found in guild")

        role = guild.get_role(role_id)
        if not role:
            raise ValueError(f"Role {role_id} not found in guild")

        await member.add_roles(role, reason=reason)
        return {"success": True, "member": str(member), "role": role.name}

    async def _action_role_remove(self, guild: discord.Guild, payload: dict) -> dict:
        """Remove a role from a user."""
        user_id = int(payload["user_id"])
        role_id = int(payload["role_id"])
        reason = payload.get("reason", "Removed via web dashboard")

        member = guild.get_member(user_id)
        if not member:
            raise ValueError(f"Member {user_id} not found in guild")

        role = guild.get_role(role_id)
        if not role:
            raise ValueError(f"Role {role_id} not found in guild")

        await member.remove_roles(role, reason=reason)
        return {"success": True, "member": str(member), "role": role.name}

    async def _action_role_bulk_add(self, guild: discord.Guild, payload: dict) -> dict:
        """Add a role to multiple users (CSV import use case)."""
        role_id = int(payload["role_id"])
        user_ids = [int(uid) for uid in payload["user_ids"]]
        reason = payload.get("reason", "Bulk add via web dashboard")

        role = guild.get_role(role_id)
        if not role:
            raise ValueError(f"Role {role_id} not found in guild")

        success = []
        failed = []

        for user_id in user_ids:
            try:
                member = guild.get_member(user_id)
                if member:
                    await member.add_roles(role, reason=reason)
                    success.append(user_id)
                else:
                    failed.append({"user_id": user_id, "error": "Member not found"})
            except Exception as e:
                failed.append({"user_id": user_id, "error": str(e)})

            # Small delay to avoid rate limits
            await asyncio.sleep(0.5)

        return {"success_count": len(success), "failed_count": len(failed), "failed": failed}

    async def _action_role_bulk_remove(self, guild: discord.Guild, payload: dict) -> dict:
        """Remove a role from multiple users."""
        role_id = int(payload["role_id"])
        user_ids = [int(uid) for uid in payload["user_ids"]]
        reason = payload.get("reason", "Bulk remove via web dashboard")

        role = guild.get_role(role_id)
        if not role:
            raise ValueError(f"Role {role_id} not found in guild")

        success = []
        failed = []

        for user_id in user_ids:
            try:
                member = guild.get_member(user_id)
                if member:
                    await member.remove_roles(role, reason=reason)
                    success.append(user_id)
                else:
                    failed.append({"user_id": user_id, "error": "Member not found"})
            except Exception as e:
                failed.append({"user_id": user_id, "error": str(e)})

            await asyncio.sleep(0.5)

        return {"success_count": len(success), "failed_count": len(failed), "failed": failed}

    # ═══════════════════════════════════════════════════════════════
    # XP ACTIONS
    # ═══════════════════════════════════════════════════════════════

    async def _action_xp_modify(self, guild: discord.Guild, payload: dict, operation: str) -> dict:
        """Add, remove, or set XP for a user."""
        user_id = int(payload["user_id"])
        amount = float(payload["amount"])

        with db_session_scope() as session:
            member = session.query(GuildMember).filter_by(
                guild_id=guild.id,
                user_id=user_id
            ).first()

            if not member:
                # Create member record if doesn't exist
                logger.warning(
                    f"[DUPLICATE TRACKER] action_processor._action_xp_modify CREATING GuildMember: "
                    f"guild_id={guild.id}, user_id={user_id}, user_id_type={type(user_id)}, "
                    f"payload_user_id={payload['user_id']}, payload_user_id_type={type(payload['user_id'])}"
                )
                member = GuildMember(guild_id=guild.id, user_id=user_id)
                session.add(member)

            old_xp = member.xp

            if operation == "add":
                member.xp += amount
            elif operation == "remove":
                member.xp = max(0, member.xp - amount)
            elif operation == "set":
                member.xp = max(0, amount)

            session.commit()

            return {
                "success": True,
                "user_id": user_id,
                "old_xp": old_xp,
                "new_xp": member.xp,
                "operation": operation
            }

    async def _action_xp_bulk_set(self, guild: discord.Guild, payload: dict) -> dict:
        """Set XP for multiple users (CSV import)."""
        users = payload["users"]  # [{"user_id": 123, "xp": 1000}, ...]

        success = []
        failed = []

        with db_session_scope() as session:
            for user_data in users:
                try:
                    user_id = int(user_data["user_id"])
                    xp = float(user_data["xp"])

                    member = session.query(GuildMember).filter_by(
                        guild_id=guild.id,
                        user_id=user_id
                    ).first()

                    if not member:
                        logger.warning(
                            f"[DUPLICATE TRACKER] action_processor._action_xp_bulk_set CREATING GuildMember: "
                            f"guild_id={guild.id}, user_id={user_id}, user_id_type={type(user_id)}, "
                            f"user_data_user_id={user_data['user_id']}, user_data_user_id_type={type(user_data['user_id'])}"
                        )
                        member = GuildMember(guild_id=guild.id, user_id=user_id)
                        session.add(member)

                    member.xp = max(0, xp)
                    success.append(user_id)

                except Exception as e:
                    failed.append({"user_id": user_data.get("user_id"), "error": str(e)})

            session.commit()

        return {"success_count": len(success), "failed_count": len(failed), "failed": failed}

    async def _action_tokens_modify(self, guild: discord.Guild, payload: dict, operation: str) -> dict:
        """Add or remove Hero Tokens."""
        user_id = int(payload["user_id"])
        amount = int(payload["amount"])

        with db_session_scope() as session:
            member = session.query(GuildMember).filter_by(
                guild_id=guild.id,
                user_id=user_id
            ).first()

            if not member:
                logger.warning(
                    f"[DUPLICATE TRACKER] action_processor._action_tokens_modify CREATING GuildMember: "
                    f"guild_id={guild.id}, user_id={user_id}, user_id_type={type(user_id)}, "
                    f"payload_user_id={payload['user_id']}, payload_user_id_type={type(payload['user_id'])}"
                )
                member = GuildMember(guild_id=guild.id, user_id=user_id)
                session.add(member)

            old_tokens = member.hero_tokens

            if operation == "add":
                member.hero_tokens += amount
            elif operation == "remove":
                member.hero_tokens = max(0, member.hero_tokens - amount)

            session.commit()

            return {
                "success": True,
                "user_id": user_id,
                "old_tokens": old_tokens,
                "new_tokens": member.hero_tokens,
                "operation": operation
            }

    # ═══════════════════════════════════════════════════════════════
    # MEMBER MODERATION ACTIONS
    # ═══════════════════════════════════════════════════════════════

    async def _action_member_kick(self, guild: discord.Guild, payload: dict) -> dict:
        """Kick a member from the guild."""
        user_id = int(payload["user_id"])
        reason = payload.get("reason", "Kicked via web dashboard")

        member = guild.get_member(user_id)
        if not member:
            raise ValueError(f"Member {user_id} not found in guild")

        await member.kick(reason=reason)
        return {"success": True, "member": str(member)}

    async def _action_member_ban(self, guild: discord.Guild, payload: dict) -> dict:
        """Ban a member from the guild."""
        user_id = int(payload["user_id"])
        reason = payload.get("reason", "Banned via web dashboard")
        delete_days = payload.get("delete_message_days", 0)

        member = guild.get_member(user_id)
        if member:
            await member.ban(reason=reason, delete_message_seconds=delete_days * 86400)  # Convert days to seconds
            return {"success": True, "member": str(member)}
        else:
            # Ban by ID even if not in guild
            await guild.ban(discord.Object(id=user_id), reason=reason, delete_message_seconds=delete_days * 86400)
            return {"success": True, "user_id": user_id}

    async def _action_member_unban(self, guild: discord.Guild, payload: dict) -> dict:
        """Unban a user from the guild."""
        user_id = int(payload["user_id"])
        reason = payload.get("reason", "Unbanned via web dashboard")

        await guild.unban(discord.Object(id=user_id), reason=reason)
        return {"success": True, "user_id": user_id}

    async def _action_member_timeout(self, guild: discord.Guild, payload: dict) -> dict:
        """Timeout a member."""
        from datetime import timedelta

        user_id = int(payload["user_id"])
        duration_minutes = int(payload.get("duration_minutes", 60))
        reason = payload.get("reason", "Timed out via web dashboard")

        member = guild.get_member(user_id)
        if not member:
            raise ValueError(f"Member {user_id} not found in guild")

        await member.timeout_for(timedelta(minutes=duration_minutes), reason=reason)
        return {"success": True, "member": str(member), "duration_minutes": duration_minutes}

    async def _action_member_untimeout(self, guild: discord.Guild, payload: dict) -> dict:
        """Remove timeout from a member."""
        user_id = int(payload["user_id"])
        reason = payload.get("reason", "Timeout removed via web dashboard")

        member = guild.get_member(user_id)
        if not member:
            raise ValueError(f"Member {user_id} not found in guild")

        await member.timeout(None, reason=reason)
        return {"success": True, "member": str(member)}

    # ═══════════════════════════════════════════════════════════════
    # WARNING ACTIONS
    # ═══════════════════════════════════════════════════════════════

    async def _action_warning_add(self, guild: discord.Guild, payload: dict) -> dict:
        """Add a warning to a user."""
        user_id = int(payload["user_id"])
        reason = payload["reason"]
        severity = payload.get("severity", 1)
        issued_by = payload.get("issued_by")
        issued_by_name = payload.get("issued_by_name", "Web Dashboard")

        with db_session_scope() as session:
            warning = Warning(
                guild_id=guild.id,
                user_id=user_id,
                warning_type=WarningType.MANUAL,
                reason=reason,
                severity=severity,
                issued_by=issued_by,
                issued_by_name=issued_by_name
            )
            session.add(warning)

            # Update member warn count
            member = session.query(GuildMember).filter_by(
                guild_id=guild.id,
                user_id=user_id
            ).first()
            if member:
                member.warn_count += 1

            session.commit()

            return {"success": True, "warning_id": warning.id, "user_id": user_id}

    async def _action_warning_pardon(self, guild: discord.Guild, payload: dict) -> dict:
        """Pardon a warning."""
        warning_id = int(payload["warning_id"])
        pardoned_by = payload.get("pardoned_by")
        pardon_reason = payload.get("reason", "Pardoned via web dashboard")

        with db_session_scope() as session:
            warning = session.query(Warning).filter_by(
                id=warning_id,
                guild_id=guild.id
            ).first()

            if not warning:
                raise ValueError(f"Warning {warning_id} not found")

            warning.is_active = False
            warning.pardoned = True
            warning.pardoned_by = pardoned_by
            warning.pardoned_at = int(time.time())
            warning.pardon_reason = pardon_reason

            # Update member warn count
            member = session.query(GuildMember).filter_by(
                guild_id=guild.id,
                user_id=warning.user_id
            ).first()
            if member and member.warn_count > 0:
                member.warn_count -= 1

            session.commit()

            return {"success": True, "warning_id": warning_id}

    # ═══════════════════════════════════════════════════════════════
    # MESSAGE ACTIONS
    # ═══════════════════════════════════════════════════════════════

    async def _action_message_send(self, guild: discord.Guild, payload: dict) -> dict:
        """Send a message to a channel (includes test welcome/goodbye messages)."""
        message_type = payload.get("type")

        # Handle test welcome/goodbye messages
        if message_type in ("test_welcome", "test_goodbye"):
            return await self._send_test_message(guild, message_type, payload)

        # Regular message sending
        channel_id = int(payload["channel_id"])
        content = payload.get("content")
        embed_data = payload.get("embed")

        channel = guild.get_channel(channel_id)
        if not channel:
            raise ValueError(f"Channel {channel_id} not found")

        embed = None
        if embed_data:
            embed = discord.Embed(
                title=embed_data.get("title"),
                description=embed_data.get("description"),
                color=embed_data.get("color", 0x5865F2)
            )
            if embed_data.get("footer"):
                embed.set_footer(text=embed_data["footer"])

        msg = await channel.send(content=content, embed=embed)
        return {"success": True, "message_id": msg.id}

    async def _send_test_message(self, guild: discord.Guild, message_type: str, payload: dict) -> dict:
        """Send a test welcome or goodbye message."""
        from config import db_session_scope
        from models import WelcomeConfig, Guild as GuildModel
        from datetime import datetime, timezone

        target_user_id = payload.get("target_user_id")
        if not target_user_id:
            raise ValueError("target_user_id required for test messages")

        member = guild.get_member(int(target_user_id))
        if not member:
            raise ValueError(f"Member {target_user_id} not found")

        with db_session_scope() as session:
            db_guild = session.get(GuildModel, guild.id)
            config = session.get(WelcomeConfig, guild.id)

            if not config:
                raise ValueError("Welcome config not found")

            if message_type == "test_welcome":
                # Get welcome channel and message
                channel_id = db_guild.welcome_channel_id if db_guild else None
                if not channel_id:
                    raise ValueError("Welcome channel not configured")

                channel = guild.get_channel(channel_id)
                if not channel:
                    raise ValueError("Welcome channel not found")

                # Format message
                from cogs.welcome import format_message
                formatted_message = format_message(config.channel_message, member)

                # Send with embed if enabled
                if config.channel_embed_enabled:
                    embed = discord.Embed(
                        title=format_message(config.channel_embed_title, member) if config.channel_embed_title else None,
                        description=formatted_message,
                        color=discord.Color(config.channel_embed_color),
                        timestamp=datetime.now(timezone.utc)
                    )
                    if config.channel_embed_thumbnail:
                        embed.set_thumbnail(url=member.display_avatar.url)
                    if config.channel_embed_footer:
                        embed.set_footer(text=format_message(config.channel_embed_footer, member))

                    msg = await channel.send(content="**[TEST WELCOME]** This is a test welcome message:", embed=embed)
                else:
                    msg = await channel.send(f"**[TEST WELCOME]** {formatted_message}")

                return {"success": True, "message_id": msg.id, "channel_id": channel_id}

            elif message_type == "test_goodbye":
                # Get goodbye channel and message
                channel_id = config.goodbye_channel_id or (db_guild.welcome_channel_id if db_guild else None)
                if not channel_id:
                    raise ValueError("Goodbye channel not configured")

                channel = guild.get_channel(channel_id)
                if not channel:
                    raise ValueError("Goodbye channel not found")

                # Format goodbye message (can't use mention for goodbye)
                formatted = config.goodbye_message.replace("{user}", f"**{member.display_name}**")
                formatted = formatted.replace("{username}", member.display_name)
                formatted = formatted.replace("{server}", guild.name)
                formatted = formatted.replace("{member_count}", str(guild.member_count))

                embed = discord.Embed(
                    description=formatted,
                    color=discord.Color.greyple(),
                    timestamp=datetime.now(timezone.utc)
                )

                msg = await channel.send(content="**[TEST GOODBYE]** This is a test goodbye message:", embed=embed)
                return {"success": True, "message_id": msg.id, "channel_id": channel_id}

        raise ValueError(f"Unknown message type: {message_type}")

    async def _action_dm_send(self, guild: discord.Guild, payload: dict) -> dict:
        """Send a DM to a user."""
        user_id = int(payload["user_id"])
        content = payload.get("content")

        member = guild.get_member(user_id)
        if not member:
            raise ValueError(f"Member {user_id} not found in guild")

        try:
            await member.send(content=content)
            return {"success": True, "user_id": user_id}
        except discord.Forbidden:
            return {"success": False, "error": "Cannot DM user (DMs disabled)"}

    async def _action_channel_topic(self, guild: discord.Guild, payload: dict) -> dict:
        """Set a channel topic."""
        channel_id = int(payload["channel_id"])
        topic = payload["topic"]

        channel = guild.get_channel(channel_id)
        if not channel:
            raise ValueError(f"Channel {channel_id} not found")

        await channel.edit(topic=topic)
        return {"success": True, "channel_id": channel_id, "topic": topic}

    # ═══════════════════════════════════════════════════════════════
    # DISCOVERY/SELF-PROMO ACTIONS
    # ═══════════════════════════════════════════════════════════════

    async def _action_force_feature(self, guild: discord.Guild, payload: dict) -> dict:
        """Force feature a winner from the discovery pool (Feature Now button)."""
        # Get the Discovery cog
        discovery_cog = self.bot.get_cog("DiscoveryCog")
        if not discovery_cog:
            raise ValueError("Discovery cog not loaded")

        # Call the select and feature winner method
        result = await discovery_cog.select_and_feature_winner(guild.id)

        if not result.get('success'):
            raise ValueError(result.get('error', 'Unknown error'))

        return result

    async def _action_clear_featured(self, guild: discord.Guild, payload: dict) -> dict:
        """Clear the current featured person from the featured pool."""
        from config import db_session_scope
        from models import DiscoveryConfig, FeaturedPool

        cleared_count = 0
        deleted_messages = 0

        with db_session_scope() as session:
            config = session.get(DiscoveryConfig, guild.id)
            if not config or not config.selfpromo_channel_id:
                raise ValueError("Discovery is not configured for this server")

            # Find currently featured entries
            featured_entries = (
                session.query(FeaturedPool)
                .filter_by(guild_id=guild.id, was_selected=True)
                .all()
            )

            if not featured_entries:
                return {"success": True, "cleared": 0, "messages_deleted": 0, "message": "No one currently featured"}

            for entry in featured_entries:
                # Try to delete the featured message
                if entry.featured_message_id:
                    try:
                        channel = self.bot.get_channel(config.selfpromo_channel_id)
                        if channel:
                            msg = await channel.fetch_message(entry.featured_message_id)
                            await msg.delete()
                            deleted_messages += 1
                    except discord.NotFound:
                        pass  # Message already deleted
                    except Exception as e:
                        logger.error(f"Error deleting featured message {entry.featured_message_id}: {e}")

                # Remove from featured pool
                session.delete(entry)
                cleared_count += 1

            # Clear last featured user tracking
            config.last_featured_user_id = None

        return {
            "success": True,
            "cleared": cleared_count,
            "messages_deleted": deleted_messages
        }

    async def _action_test_channel_embed(self, guild: discord.Guild, payload: dict) -> dict:
        """Send a test embed to the selected test channel."""
        import discord

        # Get channel_id from payload (sent from website)
        channel_id = payload.get('channel_id')
        if not channel_id:
            raise ValueError("No channel_id provided in payload")

        channel = self.bot.get_channel(int(channel_id))
        if not channel:
            raise ValueError(f"Channel {channel_id} not found")

        # Create test embed
        embed = discord.Embed(
            title="🧪 Test Channel Embed",
            description="This is a test of the feature announcement channel embed. If you can see this, the channel embed is working correctly!",
            color=0x00FF00,  # Green
            timestamp=discord.utils.utcnow()
        )
        embed.add_field(name="Status", value="✅ Working", inline=True)
        embed.add_field(name="Channel", value=channel.mention, inline=True)
        embed.set_footer(text="Test triggered from website")

        await channel.send(embed=embed)

        return {"success": True, "message": "Test embed sent to channel"}

    async def _action_test_forum_embed(self, guild: discord.Guild, payload: dict) -> dict:
        """Send a test embed to the selected test forum channel."""
        import discord

        # Get channel_id from payload (sent from website)
        channel_id = payload.get('channel_id')
        if not channel_id:
            raise ValueError("No channel_id provided in payload")

        forum = self.bot.get_channel(int(channel_id))
        if not forum:
            raise ValueError(f"Channel {channel_id} not found")

        # Create test embed
        embed = discord.Embed(
            title="🧪 Test Forum Embed",
            description="This is a test of the intro forum embed. If you can see this, the forum embed is working correctly!",
            color=0xFFFF00,  # Yellow
            timestamp=discord.utils.utcnow()
        )
        embed.add_field(name="Status", value="✅ Working", inline=True)
        embed.add_field(name="Forum", value=forum.mention, inline=True)
        embed.set_footer(text="Test triggered from website")

        # For forums, we need to create a thread with the embed
        thread = await forum.create_thread(
            name="Test Forum Embed",
            content="Test embed from website",
            embed=embed
        )

        return {"success": True, "message": "Test embed sent to forum"}

    async def _action_check_games(self, guild: discord.Guild, payload: dict) -> dict:
        """Manually trigger game discovery check using all enabled search configs."""
        from config import db_session_scope
        from models import DiscoveryConfig, AnnouncedGame, GameSearchConfig, FoundGame
        from utils import igdb
        import json
        import time
        import uuid

        now = int(time.time())
        total_found = 0
        new_games_count = 0
        announced = 0
        check_id = str(uuid.uuid4())  # Unique ID for this check run

        with db_session_scope() as session:
            config = session.get(DiscoveryConfig, guild.id)
            if not config or not config.game_discovery_enabled:
                raise ValueError("Game discovery is not enabled")

            # Get public and private channels (at least one must be configured)
            public_channel = guild.get_channel(config.public_game_channel_id) if config.public_game_channel_id else None
            private_channel = guild.get_channel(config.private_game_channel_id) if config.private_game_channel_id else None

            if not public_channel and not private_channel:
                raise ValueError("No public or private game discovery channels configured")

            # Use whichever channel is available for summary messages
            summary_channel = public_channel or private_channel

            # Get all enabled search configurations
            search_configs = (
                session.query(GameSearchConfig)
                .filter(GameSearchConfig.guild_id == guild.id, GameSearchConfig.enabled == True)
                .all()
            )

            if not search_configs:
                raise ValueError("No enabled search configurations found. Please add at least one search via the dashboard.")

            # Get Discovery cog for embed creation
            discovery_cog = self.bot.get_cog("DiscoveryCog")
            if not discovery_cog:
                raise ValueError("Discovery cog not loaded")

            # Track games per search (don't deduplicate across searches for privacy)
            searches_and_games = []  # List of (search_config, games_list)

            # Run each search configuration
            for search_config in search_configs:
                try:
                    logger.info(f"Manual check: Running search '{search_config.name}' for guild {guild.id}")

                    # Parse filters
                    genres = json.loads(search_config.genres) if search_config.genres else None
                    themes = json.loads(search_config.themes) if search_config.themes else None
                    modes = json.loads(search_config.game_modes) if search_config.game_modes else None
                    platforms = json.loads(search_config.platforms) if search_config.platforms else None
                    announcement_window = search_config.days_ahead or 30
                    min_hype = search_config.min_hype
                    min_rating = search_config.min_rating

                    # Fetch games from IGDB
                    games = await igdb.search_upcoming_games(
                        days_ahead=365,  # Search wide window
                        days_behind=0,
                        genres=genres,
                        themes=themes,
                        game_modes=modes,
                        platforms=platforms,
                        min_hype=min_hype,
                        min_rating=min_rating,
                        limit=100
                    )

                    # Filter to announcement window for this search
                    announcement_cutoff = now + (announcement_window * 24 * 60 * 60)
                    games = [g for g in games if g.release_date and g.release_date <= announcement_cutoff]

                    # Store games for this search
                    searches_and_games.append((search_config, games))
                    logger.info(f"Search '{search_config.name}' found {len(games)} games for announcement")

                except Exception as e:
                    logger.error(f"Error running search '{search_config.name}': {e}")
                    continue

            # Calculate total
            total_found = sum(len(games) for _, games in searches_and_games)

            # Clear previous found games for this guild (keep last 3 checks)
            old_checks = session.query(FoundGame).filter(
                FoundGame.guild_id == guild.id
            ).order_by(FoundGame.found_at.desc()).offset(300).all()  # Keep ~3 checks worth
            for old_check in old_checks:
                session.delete(old_check)

            # Process each search configuration with privacy settings
            public_games_to_announce = {}  # For main channel announcements

            for search_config, games in searches_and_games:
                if not games:
                    continue

                # PUBLIC SEARCH: Save to database for website display
                if search_config.show_on_website:
                    logger.info(f"Processing PUBLIC search '{search_config.name}': {len(games)} games")

                    new_games_saved = 0
                    for game in games:
                        # Check if this game already exists for this search config
                        existing = session.query(FoundGame).filter(
                            FoundGame.guild_id == guild.id,
                            FoundGame.search_config_id == search_config.id,
                            FoundGame.igdb_id == game.id
                        ).first()

                        if existing:
                            logger.debug(f"Game '{game.name}' already exists for search '{search_config.name}', skipping")
                            # Still add to announcement pool (will check announced_games separately)
                            public_games_to_announce[game.id] = game
                            continue

                        # Extract Steam URL from websites
                        steam_url = None
                        if hasattr(game, 'websites') and game.websites:
                            for website in game.websites:
                                if website.get('category') == 13:  # Steam
                                    steam_url = website.get('url')
                                    break

                        # If IGDB doesn't provide Steam URL, lookup via Steam API
                        if not steam_url:
                            steam_url = await self._lookup_steam_url(game.name)

                        found_game = FoundGame(
                            guild_id=guild.id,
                            igdb_id=game.id,
                            igdb_slug=game.slug if hasattr(game, 'slug') else None,
                            game_name=game.name,
                            release_date=game.release_date,
                            summary=game.summary if hasattr(game, 'summary') else None,
                            genres=json.dumps(game.genres) if hasattr(game, 'genres') else None,
                            themes=json.dumps(game.themes) if hasattr(game, 'themes') else None,
                            game_modes=json.dumps(game.game_modes) if hasattr(game, 'game_modes') else None,
                            platforms=json.dumps(game.platforms) if hasattr(game, 'platforms') else None,
                            cover_url=game.cover_url,
                            igdb_url=game.igdb_url if hasattr(game, 'igdb_url') else None,
                            steam_url=steam_url,
                            hypes=game.hypes if hasattr(game, 'hypes') else None,
                            rating=game.rating if hasattr(game, 'rating') else None,
                            search_config_id=search_config.id,
                            search_config_name=search_config.name,
                            found_at=now,
                            check_id=check_id
                        )
                        session.add(found_game)
                        new_games_saved += 1

                        # Add to public announcement pool
                        public_games_to_announce[game.id] = game

                    logger.info(f"Saved {new_games_saved} new games from '{search_config.name}' to database ({len(games) - new_games_saved} duplicates skipped)")

                # PRIVATE SEARCH: Post to Discord thread only
                else:
                    if not private_channel:
                        logger.warning(f"Skipping private search '{search_config.name}' - no private channel configured")
                        continue

                    logger.info(f"Processing PRIVATE search '{search_config.name}': {len(games)} games")

                    try:
                        # Get or create persistent thread
                        thread = None
                        if search_config.discovery_thread_id:
                            try:
                                thread = await private_channel.guild.fetch_channel(search_config.discovery_thread_id)
                                logger.info(f"Found existing thread {thread.id} for '{search_config.name}'")
                            except discord.NotFound:
                                logger.warning(f"Thread {search_config.discovery_thread_id} not found, creating new one")
                                search_config.discovery_thread_id = None

                        # Create new thread if needed
                        if not thread:
                            thread = await private_channel.create_thread(
                                name=f"🔒 {search_config.name} - Private Discoveries",
                                auto_archive_duration=10080  # 7 days
                            )
                            search_config.discovery_thread_id = thread.id
                            logger.info(f"Created new private thread {thread.id} for '{search_config.name}'")

                            # Send intro message
                            await thread.send(
                                f"🎮 **Private Game Discovery: {search_config.name}**\n"
                                f"This thread contains games matching your search criteria. "
                                f"These games are **not shown on the website** for privacy."
                            )

                            # Auto-join members with specified role (if configured)
                            if search_config.auto_join_role_id:
                                try:
                                    role = private_channel.guild.get_role(search_config.auto_join_role_id)
                                    if role:
                                        added_count = 0
                                        for member in role.members:
                                            try:
                                                await thread.add_user(member)
                                                added_count += 1
                                            except Exception as e:
                                                logger.warning(f"Failed to add {member.name} to thread: {e}")
                                        logger.info(f"Auto-joined {added_count} members with role '{role.name}' to private thread")
                                    else:
                                        logger.warning(f"Auto-join role {search_config.auto_join_role_id} not found")
                                except Exception as e:
                                    logger.error(f"Error auto-joining role members to thread: {e}")

                        # Post ALL games to thread (no limit for private searches)
                        posted_count = 0
                        for game in games:
                            # Check if already announced (reuse announced_games table for private threads too)
                            already_posted = session.query(AnnouncedGame).filter(
                                AnnouncedGame.guild_id == guild.id,
                                AnnouncedGame.igdb_id == game.id
                            ).first()

                            if already_posted:
                                logger.debug(f"Game '{game.name}' already posted to thread, skipping")
                                continue

                            try:
                                # Add Steam URL lookup for private games
                                steam_url = None
                                if hasattr(game, 'websites') and game.websites:
                                    # Check if IGDB already has Steam URL
                                    for website in game.websites:
                                        if website.get('category') == 13:  # Steam
                                            steam_url = website.get('url')
                                            break

                                # If IGDB doesn't provide Steam URL, lookup via Steam API
                                if not steam_url:
                                    steam_url = await self._lookup_steam_url(game.name)

                                    # Add Steam URL to game's websites for the embed
                                    if steam_url and "/app/" in steam_url:  # Only add direct links
                                        if not hasattr(game, 'websites') or not game.websites:
                                            game.websites = []
                                        game.websites.append({
                                            'category': 13,  # Steam
                                            'url': steam_url
                                        })

                                embed = discovery_cog.create_game_announcement_embed(game)
                                message = await thread.send(embed=embed)
                                new_games_count += 1
                                posted_count += 1

                                # Track that we posted this game (PRIVACY: Mask sensitive fields for private searches)
                                announced_game = AnnouncedGame(
                                    guild_id=guild.id,
                                    igdb_id=game.id,  # Keep for deduplication
                                    igdb_slug="[PRIVATE]",  # Masked
                                    game_name="[PRIVATE]",  # Masked
                                    release_date=None,  # Masked
                                    genres=None,  # Masked
                                    platforms=None,  # Masked
                                    cover_url=None,  # Masked
                                    announced_at=now,
                                    announcement_message_id=message.id
                                )
                                session.add(announced_game)

                            except Exception as e:
                                logger.error(f"Failed to post game '{game.name}' to thread: {e}")
                                continue

                        logger.info(f"Posted {posted_count} new games to private thread for '{search_config.name}' ({len(games) - posted_count} duplicates skipped)")

                    except Exception as e:
                        logger.error(f"Failed to handle private search '{search_config.name}': {e}")
                        continue

            # Announce public games to public channel (limit to 10)
            logger.info(f"Attempting to announce up to 10 games from {len(public_games_to_announce)} public games")

            if public_channel and public_games_to_announce:
                for game_id, game in list(public_games_to_announce.items())[:10]:
                    already_announced = session.query(AnnouncedGame).filter(
                        AnnouncedGame.guild_id == guild.id,
                        AnnouncedGame.igdb_id == game.id
                    ).first()

                    if already_announced:
                        logger.debug(f"Game '{game.name}' already announced, skipping")
                        continue

                    try:
                        # Create and post announcement
                        embed = discovery_cog.create_game_announcement_embed(game)
                        message = await public_channel.send(embed=embed)
                        logger.info(f"✅ Announced '{game.name}' to public channel {public_channel.name}")

                        # Record announcement
                        announced_game = AnnouncedGame(
                            guild_id=guild.id,
                            igdb_id=game.id,
                            igdb_slug=game.slug if hasattr(game, 'slug') else None,
                            game_name=game.name,
                            release_date=game.release_date,
                            genres=json.dumps(game.genres),
                            platforms=json.dumps(game.platforms),
                            cover_url=game.cover_url,
                            announced_at=now,
                            announcement_message_id=message.id
                        )
                        session.add(announced_game)
                        announced += 1
                        new_games_count += 1

                    except Exception as e:
                        logger.error(f"Failed to announce game '{game.name}': {e}")
                        continue

            logger.info(f"Announced {announced} games to main channel")

            # Count public vs private games
            public_searches_count = sum(1 for s, _ in searches_and_games if s.show_on_website)
            private_searches_count = sum(1 for s, _ in searches_and_games if not s.show_on_website)
            public_games_count = sum(len(g) for s, g in searches_and_games if s.show_on_website)
            private_games_count = sum(len(g) for s, g in searches_and_games if not s.show_on_website)

            # Post summary message to Discord
            summary_embed = discord.Embed(
                title="🎮 Game Discovery Check Complete",
                description=f"Found **{total_found} games** across {len(searches_and_games)} search configurations.",
                color=0x5865F2  # Discord blurple
            )

            if public_games_count > 0:
                summary_embed.add_field(name="📢 Announced", value=f"{announced} games to this channel", inline=True)
                summary_embed.add_field(name="💾 Saved", value=f"{public_games_count} games total", inline=True)
                dashboard_url = f"https://dashboard.casual-heroes.com/warden/guild/{guild.id}/found-games/"
                summary_embed.add_field(
                    name="🔗 View Public Games",
                    value=f"[Click here to view all {public_games_count} games on the dashboard]({dashboard_url})",
                    inline=False
                )

            if private_games_count > 0:
                summary_embed.add_field(
                    name="🔒 Private Discoveries",
                    value=f"{private_games_count} games posted to {private_searches_count} private thread(s)",
                    inline=False
                )

            summary_embed.set_footer(text="Public games → Dashboard • Private games → Discord threads")

            try:
                await summary_channel.send(embed=summary_embed)
                logger.info(f"Posted summary: {public_games_count} public, {private_games_count} private")
            except Exception as e:
                logger.error(f"Failed to post summary embed: {e}")

            # Update last check time
            config.last_game_check_at = now

        # Build result message based on public vs private searches
        result_message = f"🎮 Found {total_found} games across {len(searches_and_games)} searches\n"

        if public_games_count > 0:
            result_message += f"📢 Announced {announced} public games to Discord\n"
            dashboard_url = f"https://dashboard.casual-heroes.com/warden/guild/{guild.id}/found-games/"
            result_message += f"🔗 View all public games: {dashboard_url}"
        else:
            dashboard_url = None

        if private_games_count > 0:
            if public_games_count > 0:
                result_message += "\n"
            result_message += f"🔒 {private_games_count} private games posted to Discord threads"

        return {
            "success": True,
            "total_found": total_found,
            "new_games": new_games_count,
            "announced": announced,
            "check_id": check_id,
            "message": result_message,
            "dashboard_url": dashboard_url,
            "public_count": public_games_count,
            "private_count": private_games_count
        }

    # ═══════════════════════════════════════════════════════════════
    # FLAIR ACTIONS
    # ═══════════════════════════════════════════════════════════════

    async def _action_flair_assign(self, guild: discord.Guild, payload: dict) -> dict:
        """
        Assign a flair role to a member.
        Removes old flair roles and assigns the new one.

        Payload:
            target_user_id: ID of the user
            flair_name: Name of the flair (e.g., "[🎮 Casual Legend]")
        """
        user_id = payload.get("target_user_id")
        flair_name = payload.get("flair_name")

        if not user_id or not flair_name:
            raise ValueError("Missing target_user_id or flair_name in payload")

        # Get member
        member = guild.get_member(user_id)
        if not member:
            raise ValueError(f"Member {user_id} not found in guild")

        # Construct role name: "Flair: {flair_name}"
        role_name = f"Flair: {flair_name}"

        # Find the flair role
        flair_role = discord.utils.get(guild.roles, name=role_name)
        if not flair_role:
            # If role doesn't exist, log warning but don't fail
            # The role should be created manually by admins
            logger.warning(f"Flair role '{role_name}' not found in guild {guild.name}. Role must be created manually.")
            return {
                "success": True,
                "message": f"Flair updated to {flair_name}, but role '{role_name}' needs to be created in Discord",
                "warning": f"Role not found: {role_name}"
            }

        # Remove any existing flair roles (roles that start with "Flair: ")
        old_flair_roles = [r for r in member.roles if r.name.startswith("Flair: ")]
        if old_flair_roles:
            await member.remove_roles(*old_flair_roles, reason="Removing old flair roles")
            logger.info(f"Removed old flair roles from {member.display_name}: {[r.name for r in old_flair_roles]}")

        # Assign new flair role
        await member.add_roles(flair_role, reason=f"Assigned flair: {flair_name}")
        logger.info(f"Assigned flair role '{role_name}' to {member.display_name} in {guild.name}")

        return {
            "success": True,
            "message": f"Assigned flair '{flair_name}' to {member.display_name}"
        }

    # ═══════════════════════════════════════════════════════════════
    # TEMPLATE ACTIONS
    # ═══════════════════════════════════════════════════════════════

    async def _action_channel_create(self, guild: discord.Guild, payload: dict) -> dict:
        """Create channels from a template with category-level role permissions."""
        import json

        template_data = json.loads(payload["template_data"])
        categories_data = template_data if isinstance(template_data, list) else []

        created_channels = []
        errors = []

        # Map permission names to discord.Permissions attributes
        PERM_MAP = {
            'view_channel': 'view_channel', 'manage_channels': 'manage_channels',
            'manage_permissions': 'manage_permissions', 'manage_webhooks': 'manage_webhooks',
            'create_instant_invite': 'create_instant_invite', 'send_messages': 'send_messages',
            'send_messages_in_threads': 'send_messages_in_threads',
            'create_public_threads': 'create_public_threads', 'create_private_threads': 'create_private_threads',
            'embed_links': 'embed_links', 'attach_files': 'attach_files', 'add_reactions': 'add_reactions',
            'use_external_emojis': 'use_external_emojis', 'use_external_stickers': 'use_external_stickers',
            'mention_everyone': 'mention_everyone', 'manage_messages': 'manage_messages',
            'manage_threads': 'manage_threads', 'read_message_history': 'read_message_history',
            'send_tts_messages': 'send_tts_messages', 'use_application_commands': 'use_application_commands',
            'connect': 'connect', 'speak': 'speak', 'stream': 'stream',
            'use_voice_activation': 'use_voice_activation', 'priority_speaker': 'priority_speaker',
            'mute_members': 'mute_members', 'deafen_members': 'deafen_members', 'move_members': 'move_members',
        }

        def build_overwrites(role_overrides):
            """Build permission overwrites from role_overrides list."""
            overwrites = {}
            if role_overrides:
                for override in role_overrides:
                    role_id = int(override.get("role_id"))
                    allow_perms = override.get("allow", [])
                    deny_perms = override.get("deny", [])

                    # Find the role in the guild
                    if role_id == guild.id:
                        role = guild.default_role
                    else:
                        role = guild.get_role(role_id)

                    if not role:
                        logger.warning(f"Role {role_id} not found in guild {guild.name}, skipping")
                        continue

                    # Create permission overwrite
                    overwrite = discord.PermissionOverwrite()

                    # Set allowed permissions
                    for perm_name in allow_perms:
                        if perm_name in PERM_MAP:
                            setattr(overwrite, PERM_MAP[perm_name], True)

                    # Set denied permissions
                    for perm_name in deny_perms:
                        if perm_name in PERM_MAP:
                            setattr(overwrite, PERM_MAP[perm_name], False)

                    overwrites[role] = overwrite
            return overwrites

        # Process each category with its channels
        for category_data in categories_data:
            category_name = category_data.get("category_name")
            category_role_overrides = category_data.get("role_overrides", [])
            channels = category_data.get("channels", [])

            if not category_name:
                continue

            # Build category permission overwrites
            category_overwrites = build_overwrites(category_role_overrides)

            # Create or find category
            existing_category = discord.utils.get(guild.categories, name=category_name)
            if existing_category:
                category = existing_category
                logger.info(f"Using existing category '{category_name}' in {guild.name}")
            else:
                try:
                    # Create category with role permissions
                    category = await guild.create_category(category_name, overwrites=category_overwrites)
                    logger.info(f"Created category '{category_name}' with {len(category_overwrites)} role permissions in {guild.name}")
                except Exception as e:
                    logger.error(f"Failed to create category '{category_name}': {e}")
                    errors.append(f"Category '{category_name}': {str(e)}")
                    continue

            # Create channels in this category (they inherit category permissions)
            for channel_data in channels:
                try:
                    name = channel_data["name"]
                    channel_type = channel_data.get("type", "text")
                    topic = channel_data.get("topic")

                    # Map channel type names to discord.ChannelType
                    type_map = {
                        "text": discord.ChannelType.text,
                        "voice": discord.ChannelType.voice,
                        "announcement": discord.ChannelType.news,
                        "stage": discord.ChannelType.stage_voice,
                        "forum": discord.ChannelType.forum,
                    }

                    discord_type = type_map.get(channel_type, discord.ChannelType.text)

                    # Create channel - permissions inherited from category automatically
                    if discord_type == discord.ChannelType.text:
                        channel = await guild.create_text_channel(
                            name=name,
                            category=category,
                            topic=topic,
                            reason=f"Created from template under category '{category_name}'"
                        )
                    elif discord_type == discord.ChannelType.voice:
                        channel = await guild.create_voice_channel(
                            name=name,
                            category=category,
                            reason=f"Created from template under category '{category_name}'"
                        )
                    elif discord_type == discord.ChannelType.news:
                        channel = await guild.create_text_channel(
                            name=name,
                            category=category,
                            topic=topic,
                            reason=f"Created from template under category '{category_name}'"
                    )
                        await channel.edit(type=discord.ChannelType.news)
                    elif discord_type == discord.ChannelType.stage_voice:
                        channel = await guild.create_stage_channel(
                            name=name,
                            category=category,
                            reason=f"Created from template under category '{category_name}'"
                        )
                    elif discord_type == discord.ChannelType.forum:
                        channel = await guild.create_forum_channel(
                            name=name,
                            category=category,
                            topic=topic,
                            reason=f"Created from template under category '{category_name}'"
                        )
                    else:
                        channel = await guild.create_text_channel(
                            name=name,
                            category=category,
                            topic=topic,
                            reason=f"Created from template under category '{category_name}'"
                        )

                    created_channels.append(channel.name)
                    logger.info(f"Created channel '{name}' ({channel_type}) in category '{category_name}' in {guild.name}")

                except Exception as e:
                    logger.error(f"Failed to create channel '{channel_data.get('name', 'unknown')}': {e}")
                    errors.append(f"Channel '{channel_data.get('name', 'unknown')}': {str(e)}")

        message = f"Created {len(created_channels)} channel(s)"
        if errors:
            message += f" with {len(errors)} error(s)"

        return {
            "success": len(created_channels) > 0 or len(errors) == 0,
            "message": message,
            "created": created_channels,
            "errors": errors if errors else None
        }

    async def _action_role_create(self, guild: discord.Guild, payload: dict) -> dict:
        """Create roles from a template."""
        import json

        template_data = json.loads(payload["template_data"])
        roles = template_data if isinstance(template_data, list) else []

        created_roles = []
        errors = []

        # Map permission names to discord.Permissions attributes
        ROLE_PERM_MAP = {
            'administrator': 'administrator', 'manage_guild': 'manage_guild', 'manage_roles': 'manage_roles',
            'manage_channels': 'manage_channels', 'kick_members': 'kick_members', 'ban_members': 'ban_members',
            'create_instant_invite': 'create_instant_invite', 'change_nickname': 'change_nickname',
            'manage_nicknames': 'manage_nicknames', 'manage_emojis_and_stickers': 'manage_emojis_and_stickers',
            'manage_webhooks': 'manage_webhooks', 'view_audit_log': 'view_audit_log',
            'view_guild_insights': 'view_guild_insights', 'view_channel': 'view_channel',
            'send_messages': 'send_messages', 'send_messages_in_threads': 'send_messages_in_threads',
            'create_public_threads': 'create_public_threads', 'create_private_threads': 'create_private_threads',
            'send_tts_messages': 'send_tts_messages', 'manage_messages': 'manage_messages',
            'manage_threads': 'manage_threads', 'embed_links': 'embed_links', 'attach_files': 'attach_files',
            'read_message_history': 'read_message_history', 'mention_everyone': 'mention_everyone',
            'use_external_emojis': 'use_external_emojis', 'use_external_stickers': 'use_external_stickers',
            'add_reactions': 'add_reactions', 'use_application_commands': 'use_application_commands',
            'connect': 'connect', 'speak': 'speak', 'stream': 'stream',
            'use_voice_activation': 'use_voice_activation', 'priority_speaker': 'priority_speaker',
            'mute_members': 'mute_members', 'deafen_members': 'deafen_members', 'move_members': 'move_members',
            'moderate_members': 'moderate_members', 'request_to_speak': 'request_to_speak',
        }

        for role_data in roles:
            try:
                name = role_data["name"]
                color_str = role_data.get("color", "")
                hoist = role_data.get("hoist", False)
                mentionable = role_data.get("mentionable", False)
                permissions_list = role_data.get("permissions", [])

                # Parse color (hex string like "#FF5733" or "FF5733")
                color = discord.Color.default()
                if color_str:
                    try:
                        # Remove # if present
                        color_hex = color_str.lstrip("#")
                        color = discord.Color(int(color_hex, 16))
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid color '{color_str}' for role '{name}', using default")

                # Build permissions object
                permissions = discord.Permissions()
                if permissions_list:
                    for perm_name in permissions_list:
                        if perm_name in ROLE_PERM_MAP:
                            setattr(permissions, ROLE_PERM_MAP[perm_name], True)

                # Create role with permissions
                role = await guild.create_role(
                    name=name,
                    color=color,
                    hoist=hoist,
                    mentionable=mentionable,
                    permissions=permissions,
                    reason="Created from template"
                )

                created_roles.append(role.name)
                logger.info(f"Created role '{name}' in {guild.name}")

            except Exception as e:
                logger.error(f"Failed to create role '{role_data.get('name', 'unknown')}': {e}")
                errors.append(f"Role '{role_data.get('name', 'unknown')}': {str(e)}")

        message = f"Created {len(created_roles)} role(s)"
        if errors:
            message += f" with {len(errors)} error(s)"

        return {
            "success": len(created_roles) > 0 or len(errors) == 0,
            "message": message,
            "created": created_roles,
            "errors": errors if errors else None
        }


def setup(bot: commands.Bot):
    bot.add_cog(ActionProcessorCog(bot))
