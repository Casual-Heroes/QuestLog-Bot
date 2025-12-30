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
        Lookup Steam store page URL via Steam API with improved matching.
        For DLC/seasons, links to the base game's Steam page.

        Args:
            game_name: Name of the game to search for

        Returns:
            Direct Steam store URL if found, otherwise None (to avoid bad matches)
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
                            # Try to find a good match by comparing names
                            game_name_lower = game_name.lower().strip()

                            # Remove common suffixes that might not be in Steam name
                            search_base = game_name_lower
                            is_dlc_or_season = False
                            dlc_suffixes = [
                                ': season of', ': season ', ': episode ', '- dlc', '- expansion',
                                ': the ', ' - the ', ': fate of', ': divine intervention'
                            ]
                            for suffix in dlc_suffixes:
                                if suffix in search_base:
                                    search_base = search_base.split(suffix)[0].strip()
                                    is_dlc_or_season = True
                                    break

                            for item in data['items'][:5]:  # Check top 5 results
                                result_name = item.get('name', '').lower().strip()
                                app_id = item.get('id')

                                if not app_id:
                                    continue

                                # Exact match (case-insensitive)
                                if result_name == game_name_lower:
                                    steam_url = f"https://store.steampowered.com/app/{app_id}/"
                                    logger.info(f"Steam API: Exact match for '{game_name}': {steam_url}")
                                    return steam_url

                                # Check if the base name matches (for DLC/seasons)
                                if search_base in result_name or result_name in search_base:
                                    # Only accept if it's a very close match (>70% similarity)
                                    # Simple similarity: check if most words match
                                    game_words = set(search_base.split())
                                    result_words = set(result_name.split())
                                    if game_words and len(game_words & result_words) / len(game_words) >= 0.7:
                                        steam_url = f"https://store.steampowered.com/app/{app_id}/"
                                        if is_dlc_or_season:
                                            logger.info(f"Steam API: DLC/Season '{game_name}' -> Base game '{item.get('name')}': {steam_url}")
                                        else:
                                            logger.info(f"Steam API: Close match for '{game_name}' -> '{item.get('name')}': {steam_url}")
                                        return steam_url

                            # If we detected DLC/season and didn't find exact match, try searching just the base name
                            if is_dlc_or_season:
                                base_search_term = urllib.parse.quote(search_base)
                                base_api_url = f"https://store.steampowered.com/api/storesearch/?term={base_search_term}&cc=US"

                                async with session.get(base_api_url, timeout=aiohttp.ClientTimeout(total=5)) as base_response:
                                    if base_response.status == 200:
                                        base_data = await base_response.json()

                                        if base_data.get('total', 0) > 0 and base_data.get('items'):
                                            # Take the first result for the base game
                                            first_item = base_data['items'][0]
                                            base_app_id = first_item.get('id')
                                            base_name = first_item.get('name', '')

                                            if base_app_id:
                                                steam_url = f"https://store.steampowered.com/app/{base_app_id}/"
                                                logger.info(f"Steam API: DLC/Season '{game_name}' -> Base game search '{base_name}': {steam_url}")
                                                return steam_url

            # No good match found - return None instead of search URL
            logger.debug(f"Steam API: No confident match found for '{game_name}'")
            return None

        except asyncio.TimeoutError:
            logger.warning(f"Steam API: Timeout looking up '{game_name}'")
        except Exception as e:
            logger.warning(f"Steam API: Error looking up '{game_name}': {e}")

        # Return None on error (don't save bad links)
        return None

    async def _trigger_immediate_sync(self, guild_id: int):
        """
        Trigger an immediate guild sync to update database cache.

        This bypasses the normal 5-second cooldown to ensure cache is updated
        immediately after bulk operations like creating default flair roles.

        Args:
            guild_id: Discord guild ID to sync
        """
        try:
            import os

            bot_api_port = int(os.getenv('BOT_API_PORT', 8001))
            url = f"http://localhost:{bot_api_port}/api/sync/{guild_id}"

            async with aiohttp.ClientSession() as session:
                async with session.post(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('success'):
                            logger.info(f"✅ Immediate sync triggered for guild {guild_id} after action completion")
                        else:
                            logger.error(f"Failed to sync guild {guild_id}: {data.get('error', 'Unknown error')}")
                    else:
                        logger.error(f"Failed to sync guild {guild_id}: HTTP {response.status}")
        except Exception as e:
            logger.error(f"Error triggering immediate sync for guild {guild_id}: {e}", exc_info=True)

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

            # Trigger immediate sync for actions that modify Discord resources
            # This ensures the database cache is updated immediately
            if action.action_type in [ActionType.FLAIR_SEED_ROLES]:
                await self._trigger_immediate_sync(action.guild_id)

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
        elif action_type == ActionType.TOKENS_SET:
            return await self._action_tokens_modify(guild, payload, "set")

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

        # XP Boost Events
        elif action_type == ActionType.BOOST_EVENT_START:
            return await self._action_boost_event_start(guild, payload)

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
            result = await self._action_check_games(guild, payload)

        # Flair Management
        elif action_type == ActionType.FLAIR_ASSIGN:
            return await self._action_flair_assign(guild, payload)
        elif action_type == ActionType.FLAIR_SEED_ROLES:
            return await self._action_flair_seed_roles(guild, payload)

        # LFG System
        elif action_type == ActionType.LFG_THREAD_CREATE:
            return await self._action_lfg_thread_create(guild, payload)
        elif action_type == ActionType.LFG_THREAD_UPDATE:
            return await self._action_lfg_thread_update(guild, payload)
        elif action_type == ActionType.LFG_THREAD_DELETE:
            return await self._action_lfg_thread_delete(guild, payload)

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
        """Add, remove, or set Hero Tokens."""
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
            elif operation == "set":
                member.hero_tokens = max(0, amount)

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
        """Send/edit messages and embeds; broadcast; includes test welcome/goodbye."""
        message_type = payload.get("type")
        mode = payload.get("mode", "send")

        # Handle test welcome/goodbye messages
        if message_type in ("test_welcome", "test_goodbye"):
            return await self._send_test_message(guild, message_type, payload)

        # Safe default: allow user/role mentions but NOT @everyone/@here (prevent abuse)
        allowed_mentions = discord.AllowedMentions.none() if payload.get("silent") else discord.AllowedMentions(everyone=False, roles=True, users=True)
        silent = payload.get("silent", False)

        # Send message
        if mode == "send":
            channel_id = int(payload["channel_id"])
            channel = guild.get_channel(channel_id)
            if not channel:
                raise ValueError(f"Channel {channel_id} not found")
            msg = await channel.send(content=payload.get("content"), allowed_mentions=allowed_mentions, silent=silent)
            return {"success": True, "message_id": msg.id}

        # Send embed
        if mode == "send_embed":
            channel_id = int(payload["channel_id"])
            channel = guild.get_channel(channel_id)
            if not channel:
                raise ValueError(f"Channel {channel_id} not found")
            embed_data = payload.get("embed") or {}
            embed = discord.Embed(
                title=embed_data.get("title"),
                description=embed_data.get("description"),
                color=embed_data.get("color", 0x5865F2)
            )
            if embed_data.get("footer"):
                embed.set_footer(text=embed_data.get("footer"))
            msg = await channel.send(embed=embed, allowed_mentions=allowed_mentions, silent=silent)
            return {"success": True, "message_id": msg.id}

        # Edit message
        if mode == "edit":
            channel_id = int(payload["channel_id"])
            message_id = int(payload["message_id"])
            channel = guild.get_channel(channel_id)
            if not channel:
                raise ValueError(f"Channel {channel_id} not found")
            message = await channel.fetch_message(message_id)
            if message.author.id != self.bot.user.id:
                raise ValueError("Cannot edit messages not sent by the bot")
            await message.edit(content=payload.get("content"), allowed_mentions=allowed_mentions)
            return {"success": True, "message_id": message.id}

        # Edit embed
        if mode == "edit_embed":
            message_id = int(payload["message_id"])
            channel = guild.get_channel(int(payload["channel_id"])) if payload.get("channel_id") else None
            message = None
            if channel:
                message = await channel.fetch_message(message_id)
            else:
                # try to find message across text channels
                for ch in guild.text_channels:
                    try:
                        message = await ch.fetch_message(message_id)
                        channel = ch
                        break
                    except Exception:
                        continue
            if not message:
                raise ValueError("Message not found")
            if message.author.id != self.bot.user.id:
                raise ValueError("Cannot edit messages not sent by the bot")
            embed_data = payload.get("embed") or {}
            old = message.embeds[0] if message.embeds else None
            embed = discord.Embed(
                title=embed_data.get("title") or (old.title if old else None),
                description=embed_data.get("description") or (old.description if old else None),
                color=embed_data.get("color", old.color.value if old else 0x5865F2)
            )
            footer = embed_data.get("footer") or (old.footer.text if old and old.footer else None)
            if footer:
                embed.set_footer(text=footer)
            await message.edit(embed=embed, allowed_mentions=allowed_mentions)
            return {"success": True, "message_id": message.id}

        # Broadcast to category
        if mode == "broadcast":
            category_id = int(payload["category_id"])
            category = discord.utils.get(guild.categories, id=category_id)
            if not category:
                raise ValueError(f"Category {category_id} not found")
            sent = 0
            for ch in category.channels:
                if isinstance(ch, discord.TextChannel):
                    try:
                        await ch.send(content=payload.get("content"), allowed_mentions=allowed_mentions, silent=silent)
                        sent += 1
                    except Exception:
                        continue
            return {"success": True, "sent": sent}

        raise ValueError("Invalid message mode")

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

    async def _action_boost_event_start(self, guild: discord.Guild, payload: dict) -> dict:
        """Send announcement when XP boost event is activated."""
        event_name = payload.get("event_name", "XP Boost Event")
        multiplier = payload.get("multiplier", 2.0)
        description = payload.get("description", "")
        channel_id = int(payload["channel_id"])
        role_id = payload.get("role_id")
        end_time = payload.get("end_time")

        # Get announcement channel
        channel = guild.get_channel(channel_id)
        if not channel:
            raise ValueError(f"Announcement channel {channel_id} not found")

        # Format role ping
        role_mention = ""
        if role_id:
            role = guild.get_role(int(role_id))
            if role:
                role_mention = role.mention + " "

        # Create embed
        embed = discord.Embed(
            title=f"🚀 {event_name} is Now Active!",
            description=description or "Get boosted XP for your server activity!",
            color=discord.Color.orange(),
            timestamp=discord.utils.utcnow()
        )
        embed.add_field(
            name="⚡ XP Multiplier",
            value=f"**{multiplier}x** XP for all activity!",
            inline=False
        )

        # Show end time with Discord's automatic timezone conversion
        if end_time:
            embed.add_field(
                name="⏰ Event Duration",
                value=f"**Ends:** <t:{end_time}:F>\n*(<t:{end_time}:R>)*",
                inline=False
            )
        else:
            embed.add_field(
                name="⏰ Event Duration",
                value="**No end time** - Active until manually disabled",
                inline=False
            )

        embed.set_footer(text="Take advantage of this boost while it lasts!")

        # Send announcement
        await channel.send(
            content=role_mention if role_mention else None,
            embed=embed
        )

        return {"success": True, "channel_id": channel_id, "event_name": event_name}

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
        """Send a test embed to the selected channel."""
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
            title="🧪 Test Embed",
            description="This is a test embed. If you can see this, the embed is working correctly!",
            color=0xFFFF00,  # Yellow
            timestamp=discord.utils.utcnow()
        )
        embed.add_field(name="Status", value="✅ Working", inline=True)
        embed.add_field(name="Channel", value=channel.mention, inline=True)
        embed.set_footer(text="Test triggered from website")

        # Send embed directly to the channel
        await channel.send(embed=embed)

        return {"success": True, "message": "Test embed sent to channel"}

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
        announced_count = 0
        check_id = str(uuid.uuid4())  # Unique ID for this check run

        with db_session_scope() as session:
            config = session.get(DiscoveryConfig, guild.id)
            if not config or not config.game_discovery_enabled:
                raise ValueError("Game discovery is not enabled")

            # Get the ONE discovery channel for all announcements
            discovery_channel = guild.get_channel(config.public_game_channel_id) if config.public_game_channel_id else None

            if not discovery_channel:
                raise ValueError("No game discovery channel configured")

            # Get all enabled search configurations
            search_configs = (
                session.query(GameSearchConfig)
                .filter(GameSearchConfig.guild_id == guild.id, GameSearchConfig.enabled == True)
                .all()
            )

            if not search_configs:
                raise ValueError("No enabled search configurations found. Please add at least one search via the dashboard.")

            # Track all games found across searches
            all_games_to_announce = {}  # Key: IGDB ID, Value: {game, is_public}

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

                    # Add to master list (avoid duplicates across searches), track privacy
                    for game in games:
                        if game.id not in all_games_to_announce:
                            all_games_to_announce[game.id] = {
                                "game": game,
                                "is_public": bool(search_config.show_on_website),
                                "search_config": search_config
                            }

                    logger.info(f"Search '{search_config.name}' found {len(games)} games")

                except Exception as e:
                    logger.error(f"Error running search '{search_config.name}': {e}")
                    continue

            # Calculate total
            total_found = len(all_games_to_announce)

            # Clear previous found games for this guild (keep last 3 checks)
            old_checks = session.query(FoundGame).filter(
                FoundGame.guild_id == guild.id
            ).order_by(FoundGame.found_at.desc()).offset(300).all()  # Keep ~3 checks worth
            for old_check in old_checks:
                session.delete(old_check)

            # Process ALL games - save to FoundGame database
            # Track ALL new games for local Discord embed
            # ONLY add public games to Discovery Network (AnnouncedGame table)
            new_games_for_embed = []  # ALL new games (public + private) for local announcement

            for game_id, meta in all_games_to_announce.items():
                game = meta["game"]
                is_public = meta["is_public"]
                search_config = meta["search_config"]

                # Check if this game already exists for this search config
                existing = session.query(FoundGame).filter(
                    FoundGame.guild_id == guild.id,
                    FoundGame.search_config_id == search_config.id,
                    FoundGame.igdb_id == game.id
                ).first()

                if existing:
                    logger.debug(f"Game '{game.name}' already exists in FoundGame, skipping save")
                else:
                    # Save to FoundGame database (for ALL games, public or not)
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
                    new_games_count += 1
                    # Track ALL new games (both public and private) for local Discord embed
                    new_games_for_embed.append((game, search_config, is_public))
                    logger.info(f"Saved game '{game.name}' to FoundGame database (public={is_public})")

                # ONLY add to Discovery Network for games with "Share on Discovery Network" enabled
                if is_public:
                    # Check if already announced to Discovery Network
                    already_announced = session.query(AnnouncedGame).filter(
                        AnnouncedGame.guild_id == guild.id,
                        AnnouncedGame.igdb_id == game.id
                    ).first()

                    if not already_announced:
                        # Record announcement in Discovery Network database
                        try:
                            announced = AnnouncedGame(
                                guild_id=guild.id,
                                igdb_id=game.id,
                                igdb_slug=game.slug if hasattr(game, 'slug') else None,
                                steam_id=None,
                                game_name=game.name,
                                release_date=game.release_date,
                                genres=json.dumps(game.genres) if hasattr(game, 'genres') else None,
                                platforms=json.dumps(game.platforms),
                                cover_url=game.cover_url,
                                announced_at=now,
                                announcement_message_id=None  # No individual message
                            )
                            session.add(announced)
                            announced_count += 1
                            logger.info(f"Added game '{game.name}' (IGDB:{game.id}) to Discovery Network")
                        except Exception as e:
                            logger.error(f"Failed to add game '{game.name}' to Discovery Network in guild {guild.id}: {e}")

            # Send ONE summary embed for ALL new games (both public and private) to local Discord
            if new_games_count > 0 and discovery_channel:
                try:
                    dash_url = f"https://dashboard.casual-heroes.com/questlog/guild/{guild.id}/found-games/"

                    # Collect statistics about ALL new games (from tracked new games)
                    games_by_search = {}
                    all_platforms = set()
                    all_genres = set()
                    top_rated = []
                    most_hyped = []
                    earliest_release = None
                    latest_release = None
                    cover_image = None

                    # Track game with most hypes for thumbnail
                    most_hyped_game = None
                    max_hypes = 0

                    for game, search_config, is_public in new_games_for_embed:
                        search_name = search_config.name

                        # Group by search config
                        if search_name not in games_by_search:
                            games_by_search[search_name] = 0
                        games_by_search[search_name] += 1

                        # Collect platforms and genres
                        if hasattr(game, 'platforms') and game.platforms:
                            all_platforms.update(game.platforms)
                        if hasattr(game, 'genres') and game.genres:
                            all_genres.update(game.genres)

                        # Track top rated and most hyped
                        if hasattr(game, 'rating') and game.rating:
                            top_rated.append((game.name, game.rating))
                        if hasattr(game, 'hypes') and game.hypes:
                            most_hyped.append((game.name, game.hypes))
                            # Track game with most hypes for thumbnail
                            if game.hypes > max_hypes:
                                max_hypes = game.hypes
                                most_hyped_game = game

                        # Track release date range
                        if game.release_date:
                            if earliest_release is None or game.release_date < earliest_release:
                                earliest_release = game.release_date
                            if latest_release is None or game.release_date > latest_release:
                                latest_release = game.release_date

                    # Use cover from most hyped game, fallback to first game with cover
                    cover_image = None
                    if most_hyped_game and hasattr(most_hyped_game, 'cover_url') and most_hyped_game.cover_url:
                        cover_image = most_hyped_game.cover_url
                    else:
                        # Fallback: use first game with a cover
                        for game, _, _ in new_games_for_embed:
                            if hasattr(game, 'cover_url') and game.cover_url:
                                cover_image = game.cover_url
                                break

                    # Sort and limit top games
                    top_rated.sort(key=lambda x: x[1], reverse=True)
                    most_hyped.sort(key=lambda x: x[1], reverse=True)

                    # Build enhanced embed
                    summary_embed = discord.Embed(
                        title="New Games Discovered",
                        description=f"Found **{new_games_count}** new game{'s' if new_games_count != 1 else ''} matching your discovery searches.",
                        color=0x5865F2  # Discord blurple
                    )

                    # Add thumbnail if available
                    if cover_image:
                        summary_embed.set_thumbnail(url=cover_image)

                    # Show breakdown by search config (limit to 3 for space)
                    if games_by_search:
                        breakdown_lines = []
                        for search_name, count in sorted(games_by_search.items(), key=lambda x: x[1], reverse=True)[:3]:
                            breakdown_lines.append(f"**{search_name}**: {count} game{'s' if count != 1 else ''}")
                        if len(games_by_search) > 3:
                            remaining = len(games_by_search) - 3
                            breakdown_lines.append(f"*+{remaining} more search{'es' if remaining != 1 else ''}*")
                        summary_embed.add_field(
                            name="By Search Configuration",
                            value="\n".join(breakdown_lines),
                            inline=False
                        )

                    # Show top rated or most hyped (show if we have ANY data)
                    if len(top_rated) >= 1:
                        # Show up to 3 top rated games
                        top_games = "\n".join([f"**{name}** — {int(rating)}/100" for name, rating in top_rated[:3]])
                        summary_embed.add_field(
                            name="Top Rated",
                            value=top_games,
                            inline=True
                        )
                    elif len(most_hyped) >= 1:
                        # Show up to 3 most hyped games
                        hyped_games = "\n".join([f"**{name}** — {int(hypes)} follows" for name, hypes in most_hyped[:3]])
                        summary_embed.add_field(
                            name="Most Anticipated",
                            value=hyped_games,
                            inline=True
                        )

                    # Show platform/genre stats
                    stats_lines = []
                    if all_platforms:
                        platform_count = len(all_platforms)
                        platform_preview = ", ".join(sorted(list(all_platforms))[:3])
                        if platform_count > 3:
                            platform_preview += f" +{platform_count - 3} more"
                        stats_lines.append(f"**Platforms**: {platform_preview}")
                    if all_genres:
                        genre_count = len(all_genres)
                        genre_preview = ", ".join(sorted(list(all_genres))[:3])
                        if genre_count > 3:
                            genre_preview += f" +{genre_count - 3} more"
                        stats_lines.append(f"**Genres**: {genre_preview}")

                    if stats_lines:
                        summary_embed.add_field(
                            name="Quick Stats",
                            value="\n".join(stats_lines),
                            inline=True
                        )

                    # Release date range
                    if earliest_release and latest_release:
                        from datetime import datetime
                        earliest_str = datetime.fromtimestamp(earliest_release).strftime("%b %Y")
                        latest_str = datetime.fromtimestamp(latest_release).strftime("%b %Y")
                        if earliest_str == latest_str:
                            release_info = f"Releasing in **{earliest_str}**"
                        else:
                            release_info = f"Releasing **{earliest_str}** - **{latest_str}**"
                        summary_embed.add_field(
                            name="Release Window",
                            value=release_info,
                            inline=False
                        )

                    # Dashboard link (prominent)
                    summary_embed.add_field(
                        name="View Full Details",
                        value=f"[Browse all {new_games_count} games on the dashboard]({dash_url})",
                        inline=False
                    )

                    # Footer with timestamp
                    from datetime import datetime
                    check_time = datetime.fromtimestamp(now).strftime("%b %d, %Y at %I:%M %p UTC")
                    summary_embed.set_footer(text=f"Discovery check completed {check_time}")

                    # Add role ping if configured
                    ping_content = None
                    if config.public_game_ping_role_id:
                        ping_content = f"<@&{config.public_game_ping_role_id}>"

                    await discovery_channel.send(content=ping_content, embed=summary_embed)
                    logger.info(f"Sent enhanced game summary for {new_games_count} games ({announced_count} public, {new_games_count - announced_count} private) in guild {guild.id}")
                except Exception as e:
                    logger.warning(f"Failed to send game summary in guild {guild.id}: {e}")

            # Update last check time
            config.last_game_check_at = now

        # Build result message
        dashboard_url = f"https://dashboard.casual-heroes.com/questlog/guild/{guild.id}/found-games/"
        result_message = f"🎮 Found {total_found} games across {len(search_configs)} searches\n"
        result_message += f"💾 Saved {new_games_count} new games to Found Games\n"

        if announced_count > 0:
            result_message += f"📢 Announced {announced_count} games shared on Discovery Network\n"

        result_message += f"🔗 View all games: {dashboard_url}"

        return {
            "success": True,
            "total_found": total_found,
            "new_games": new_games_count,
            "announced": announced_count,
            "check_id": check_id,
            "message": result_message,
            "dashboard_url": dashboard_url
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
        remove_flag = payload.get("remove_flair")

        if not user_id:
            raise ValueError("Missing target_user_id in payload")

        # Removal: handle when flair_name is falsy OR explicit remove flag is sent
        if not flair_name or remove_flag:
            member = guild.get_member(user_id)
            if not member:
                raise ValueError(f"Member {user_id} not found in guild")
            old_flair_roles = [r for r in member.roles if r.name.startswith("Flair: ")]
            if old_flair_roles:
                await member.remove_roles(*old_flair_roles, reason="Removing flair (requested)")
                logger.info(f"Removed flair roles from {member.display_name}: {[r.name for r in old_flair_roles]}")
            else:
                logger.info(f"No flair roles to remove for {member.display_name}")
            return {
                "success": True,
                "message": f"Removed flair from {member.display_name}"
            }

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

    async def _action_flair_seed_roles(self, guild: discord.Guild, payload: dict) -> dict:
        """
        Create default flair roles in the guild and hoist them to the top.
        Also ensure GuildFlair records exist for all default flairs.
        """
        from cogs.flair_cog import NORMAL_FLAIRS, SEASONAL_FLAIRS
        from models import GuildFlair

        created_roles = []
        moved_roles = []
        now = int(time.time())
        renames = payload.get("renames") or []

        # Use current DB flairs if present (so renamed defaults are respected), otherwise fall back to built-in lists
        # Only use normal/seasonal flairs, not custom flairs (custom flairs have their own creation flow)
        try:
            with db_session_scope() as session:
                db_flairs = session.query(GuildFlair).filter_by(guild_id=guild.id).filter(
                    GuildFlair.flair_type.in_(['normal', 'seasonal'])
                ).all()
                if db_flairs:
                    default_flairs = [(f.flair_name, f.cost, f.flair_type) for f in db_flairs]
                else:
                    default_flairs = [(name, cost, "normal") for name, cost in NORMAL_FLAIRS.items()] + \
                                     [(name, cost, "seasonal") for name, cost in SEASONAL_FLAIRS.items()]
        except Exception as e:
            logger.warning(f"Failed to load guild flairs for seed in {guild.name}: {e}")
            default_flairs = [(name, cost, "normal") for name, cost in NORMAL_FLAIRS.items()] + \
                             [(name, cost, "seasonal") for name, cost in SEASONAL_FLAIRS.items()]

        # First handle renames (e.g., admin renamed a flair in the dashboard)
        for rename in renames:
            old = rename.get("old_name")
            new = rename.get("new_name")
            if not old or not new or old == new:
                continue
            try:
                role = discord.utils.get(guild.roles, name=f"Flair: {old}")
                if role:
                    await role.edit(name=f"Flair: {new}", reason="Rename flair role")
                    moved_roles.append(f"Renamed {old} -> {new}")
                else:
                    # If missing, create with new name
                    role = await guild.create_role(
                        name=f"Flair: {new}",
                        hoist=True,
                        mentionable=False,
                        reason="Create renamed flair role"
                    )
                    created_roles.append(f"Flair: {new}")
            except Exception as e:
                logger.warning(f"Failed to rename/create flair role {old}->{new} in {guild.name}: {e}")

        # Find the bot's highest role position (flair roles should go just below this)
        bot_member = guild.get_member(self.bot.user.id)
        if bot_member and bot_member.top_role:
            # Position just below bot's highest role
            # Leave some room (subtract 2) in case there are admin roles we shouldn't touch
            target_position = max(bot_member.top_role.position - 2, 1)
        else:
            # Fallback: try to position near top of role list
            target_position = max(len(guild.roles) - 5, 1)

        logger.info(f"Creating/moving {len(default_flairs)} flair roles to position {target_position} in {guild.name}")

        # Create missing roles first, then batch position them
        new_roles = []
        for flair_name, cost, flair_type in default_flairs:
            role_name = f"Flair: {flair_name}"
            role = discord.utils.get(guild.roles, name=role_name)

            if not role:
                try:
                    role = await guild.create_role(
                        name=role_name,
                        hoist=True,
                        mentionable=False,
                        color=discord.Color.default(),  # No color by default (users see their flair color)
                        reason="Create default flair role (vanity role for display)"
                    )
                    created_roles.append(role_name)
                    new_roles.append(role)
                    logger.info(f"Created flair role: {role_name} in {guild.name}")
                except Exception as e:
                    logger.warning(f"Failed to create flair role {role_name} in {guild.name}: {e}")
                    continue

            # Sync GuildFlair record for this flair
            try:
                with db_session_scope() as session:
                    gf = (
                        session.query(GuildFlair)
                        .filter_by(guild_id=guild.id, flair_name=flair_name)
                        .first()
                    )
                    if not gf:
                        gf = GuildFlair(
                            guild_id=guild.id,
                            flair_name=flair_name,
                            flair_type=flair_type or ("normal" if flair_name in NORMAL_FLAIRS else "seasonal"),
                            cost=int(cost),
                            enabled=True,
                            display_order=0,
                            created_at=now,
                            updated_at=now,
                        )
                        session.add(gf)
                    else:
                        gf.cost = int(cost)
                        gf.enabled = True
                        gf.updated_at = now
            except Exception as e:
                logger.warning(f"Failed to sync GuildFlair for {flair_name} in {guild.name}: {e}")

        # Batch move all flair roles to the top
        # Get all existing flair roles
        all_flair_roles = [r for r in guild.roles if r.name.startswith("Flair: ")]

        if all_flair_roles:
            try:
                # Build position dict for Discord API
                # Discord.py expects: {role_object: position_int, ...}
                positions = {}
                current_pos = target_position

                # Position flair roles from high to low
                for role in sorted(all_flair_roles, key=lambda r: r.name):
                    positions[role] = current_pos
                    current_pos -= 1

                # Send batch position update to Discord
                await guild.edit_role_positions(positions=positions, reason="Position flair roles at top for visibility")
                moved_roles.extend([r.name for r in all_flair_roles])
                logger.info(f"✅ Positioned {len(all_flair_roles)} flair roles at top of {guild.name}")
            except Exception as e:
                logger.warning(f"Failed to batch position flair roles in {guild.name}: {e}")
                # Fallback: try individual positioning
                for role in all_flair_roles:
                    try:
                        await role.edit(position=target_position, reason="Hoist flair role to top")
                        moved_roles.append(role.name)
                    except Exception as e2:
                        logger.debug(f"Could not move role {role.name} in {guild.name}: {e2}")

        return {
            "success": True,
            "created_roles": created_roles,
            "moved_roles": moved_roles,
            "message": f"Created {len(created_roles)} roles; hoisted {len(moved_roles)} flair roles."
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

    # ═══════════════════════════════════════════════════════════════
    # LFG ACTIONS
    # ═══════════════════════════════════════════════════════════════

    async def _action_lfg_thread_create(self, guild: discord.Guild, payload: dict) -> dict:
        """Create an LFG thread with interactive management view."""
        import json as json_lib
        from models import LFGGroup, LFGGame, LFGConfig, LFGMember
        from db import get_db_session

        logger.info(f"🔵 BOT: Starting LFG thread creation - payload: {payload}")

        group_id = payload.get('group_id')
        if not group_id:
            raise ValueError("group_id is required")

        # Get group and game data from database
        with get_db_session() as session:
            group = session.query(LFGGroup).filter_by(id=group_id).first()
            if not group:
                logger.error(f"❌ BOT: LFG group {group_id} not found in database")
                raise ValueError(f"LFG group {group_id} not found")

            logger.info(f"✅ BOT: Found group {group_id}: {group.thread_name}")

            game = session.query(LFGGame).filter_by(id=group.game_id).first()
            if not game:
                logger.error(f"❌ BOT: Game {group.game_id} not found in database")
                raise ValueError(f"Game {group.game_id} not found")

            logger.info(f"✅ BOT: Found game {game.id}: {game.game_name}")

            config = session.query(LFGConfig).filter_by(guild_id=guild.id).first()

            # Parse custom options
            custom_options = json_lib.loads(game.custom_options) if game.custom_options else []

            # Get channel
            channel_id = payload.get('channel_id') or (config.browser_notify_channel_id if config else None)
            if not channel_id:
                logger.error(f"❌ BOT: No channel configured for LFG notifications")
                raise ValueError("No channel configured for LFG notifications")

            logger.info(f"🔵 BOT: Using channel_id: {channel_id}")

            channel = guild.get_channel(int(channel_id))
            if not channel:
                logger.error(f"❌ BOT: Channel {channel_id} not found in guild {guild.name}")
                raise ValueError(f"Channel {channel_id} not found")

            logger.info(f"✅ BOT: Found channel: {channel.name} (#{channel.id})")

            # Get creator's data from database to build thread name
            creator_member = session.query(LFGMember).filter_by(
                group_id=group.id,
                is_creator=True
            ).first()

            # Get creator's Discord display name (not username)
            creator_name = "Unknown"
            if creator_member:
                # Try to get the actual display name from Discord member object
                discord_member = guild.get_member(int(creator_member.user_id))
                if discord_member:
                    # Pycord: Use nick (server nickname) if set, otherwise try display_name
                    # display_name in pycord returns the "display name" which is the global display name
                    creator_name = discord_member.nick if discord_member.nick else str(discord_member.display_name)
                else:
                    creator_name = creator_member.display_name  # Fallback to stored name

            # Build thread name: "Title - Game - Creator"
            title = group.thread_name or "LFG Group"
            thread_name = f"{title} - {game.game_name} - {creator_name}"

            # Create thread
            logger.info(f"🔵 BOT: Creating thread '{thread_name[:100]}' in channel {channel.name}")
            thread = await channel.create_thread(
                name=thread_name[:100],  # Discord 100 char limit
                type=discord.ChannelType.public_thread,
                auto_archive_duration=10080  # 7 days (in minutes)
            )
            logger.info(f"✅ BOT: Thread created! ID: {thread.id}, Name: {thread.name}")

            # Update group with thread ID
            group.thread_id = thread.id
            session.commit()
            logger.info(f"✅ BOT: Updated group {group_id} with thread_id {thread.id}")

            # Ping role and auto-invite members if ping_role_id is set
            if group.ping_role_id:
                try:
                    role = guild.get_role(int(group.ping_role_id))
                    if role:
                        # Send ping message
                        await thread.send(f"{role.mention} - New LFG group created!")

                        # Add all members with this role to the thread
                        for member in guild.members:
                            if role in member.roles:
                                try:
                                    await thread.add_user(member)
                                except Exception as e:
                                    logger.debug(f"Could not add {member.id} to thread: {e}")
                except Exception as e:
                    logger.warning(f"Failed to ping role {group.ping_role_id}: {e}")

            # Import GroupManagementView from lfg_cog
            from cogs.lfg_cog import GroupManagementView

            # Create the interactive view
            view = GroupManagementView(game, group, custom_options, config)

            if creator_member:
                # Load creator's selections
                creator_selections = json_lib.loads(creator_member.selections) if creator_member.selections else {}
                view.member_data[group.creator_id] = {
                    "rank": creator_member.rank_value,
                    "options": creator_selections
                }

            # Load other active members (left_at is None)
            members = session.query(LFGMember).filter_by(group_id=group.id).filter(LFGMember.left_at == None).all()
            for member in members:
                if not member.is_creator:  # Already added creator
                    member_selections = json_lib.loads(member.selections) if member.selections else {}
                    view.member_data[member.user_id] = {
                        "rank": member.rank_value,
                        "options": member_selections
                    }

            # Add co-leaders to the thread
            co_leaders = [m for m in members if m.is_co_leader and not m.is_creator]
            for co_leader in co_leaders:
                try:
                    user = guild.get_member(int(co_leader.user_id))
                    if user:
                        await thread.add_user(user)
                        logger.info(f"Added co-leader {co_leader.user_id} to thread {thread.id}")
                except Exception as e:
                    logger.warning(f"Failed to add co-leader {co_leader.user_id} to thread: {e}")

            # Send the embed with interactive view
            embed = view.build_embed()
            message = await thread.send(embed=embed, view=view)
            view.message = message

        return {
            "success": True,
            "thread_id": thread.id,
            "message": f"Created LFG thread: {thread.name}"
        }

    async def _action_lfg_thread_update(self, guild: discord.Guild, payload: dict) -> dict:
        """Update an existing LFG thread's embed."""
        import json as json_lib
        from models import LFGGroup, LFGGame, LFGConfig, LFGMember
        from db import get_db_session

        group_id = payload.get('group_id')
        if not group_id:
            raise ValueError("group_id is required")

        with get_db_session() as session:
            group = session.query(LFGGroup).filter_by(id=group_id).first()
            if not group:
                raise ValueError(f"LFG group {group_id} not found")

            if not group.thread_id:
                raise ValueError(f"Group {group_id} has no thread to update")

            game = session.query(LFGGame).filter_by(id=group.game_id).first()
            if not game:
                raise ValueError(f"Game {group.game_id} not found")

            config = session.query(LFGConfig).filter_by(guild_id=guild.id).first()

            # Get the thread
            thread = guild.get_thread(group.thread_id)
            if not thread:
                # Try fetching it
                try:
                    thread = await guild.fetch_channel(group.thread_id)
                except:
                    raise ValueError(f"Thread {group.thread_id} not found")

            # Add user to thread if requested (when joining from website)
            add_user_id = payload.get('add_user_to_thread')
            if add_user_id:
                try:
                    user = guild.get_member(int(add_user_id))
                    if user:
                        await thread.add_user(user)
                        logger.info(f"Added user {add_user_id} to thread {thread.id}")
                except Exception as e:
                    logger.warning(f"Failed to add user {add_user_id} to thread: {e}")

            # Remove users from thread if requested (when co-leaders are removed)
            remove_user_ids = payload.get('remove_users_from_thread', [])
            for user_id in remove_user_ids:
                try:
                    user = guild.get_member(int(user_id))
                    if user:
                        await thread.remove_user(user)
                        logger.info(f"Removed user {user_id} from thread {thread.id}")
                except Exception as e:
                    logger.warning(f"Failed to remove user {user_id} from thread: {e}")

            # Sync co-leaders with thread - add any co-leaders who aren't in the thread yet
            co_leaders = session.query(LFGMember).filter_by(
                group_id=group.id,
                is_co_leader=True
            ).filter(LFGMember.left_at == None).all()

            for co_leader in co_leaders:
                if co_leader.is_creator:  # Skip creator, they're already in the thread
                    continue
                try:
                    user = guild.get_member(int(co_leader.user_id))
                    if user:
                        await thread.add_user(user)
                        logger.info(f"Synced co-leader {co_leader.user_id} to thread {thread.id}")
                except Exception as e:
                    logger.debug(f"Could not add co-leader {co_leader.user_id} to thread: {e}")

            # Parse custom options
            custom_options = json_lib.loads(game.custom_options) if game.custom_options else []

            # Recreate the view
            from cogs.lfg_cog import GroupManagementView
            view = GroupManagementView(game, group, custom_options, config)

            # Load all active members (left_at is None)
            members = session.query(LFGMember).filter_by(group_id=group.id).filter(LFGMember.left_at == None).all()
            for member in members:
                member_selections = json_lib.loads(member.selections) if member.selections else {}
                view.member_data[member.user_id] = {
                    "rank": member.rank_value,
                    "options": member_selections
                }

            # Find the original message (first message in thread)
            async for message in thread.history(limit=10, oldest_first=True):
                if message.author.id == self.bot.user.id and message.embeds:
                    # Update the embed
                    embed = view.build_embed()
                    await message.edit(embed=embed, view=view)
                    view.message = message
                    break

        return {
            "success": True,
            "message": f"Updated LFG thread: {thread.name}"
        }

    async def _action_lfg_thread_delete(self, guild: discord.Guild, payload: dict) -> dict:
        """Delete an LFG thread and send cancellation message."""
        import json as json_lib
        from models import LFGGroup, LFGGame, LFGConfig
        from db import get_db_session

        group_id = payload.get('group_id')
        thread_id = payload.get('thread_id')  # Get from payload since group may be deleted
        thread_name = payload.get('thread_name', 'Unknown')
        game_name = payload.get('game_name', 'Unknown')
        game_emoji = payload.get('game_emoji', '🎮')
        deleted_by_id = payload.get('deleted_by_id')
        channel_id = payload.get('channel_id')

        if not group_id:
            raise ValueError("group_id is required")

        # Delete the thread if it exists
        if thread_id:
            try:
                thread = guild.get_thread(thread_id)
                if not thread:
                    thread = await guild.fetch_channel(thread_id)

                if thread:
                    await thread.delete()
            except Exception as e:
                logger.warning(f"Failed to delete thread {thread_id}: {e}")

        # Send cancellation message to the notification channel
        if channel_id:
            try:
                channel = guild.get_channel(int(channel_id))
                if channel:
                    embed = discord.Embed(
                        title=f"🚫 LFG Group Cancelled",
                        description=f"**{game_emoji} {game_name}**\n{thread_name}",
                        color=0xFF0000,  # Red
                        timestamp=discord.utils.utcnow()
                    )
                    if deleted_by_id:
                        embed.add_field(name="Cancelled by", value=f"<@{deleted_by_id}>", inline=True)

                    await channel.send(embed=embed)
            except Exception as e:
                logger.warning(f"Failed to send cancellation message: {e}")

        return {
            "success": True,
            "message": f"Deleted LFG thread and sent cancellation notice"
        }


def setup(bot: commands.Bot):
    bot.add_cog(ActionProcessorCog(bot))
