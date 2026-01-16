# lfg_cog.py - Generic LFG (Looking For Group) System
# Works with any game configured in the dashboard
# FREE: IGDB search, custom games, custom options (weapons/classes/ranks), groups, threads
# PREMIUM/PRO: Attendance tracking, no-show reporting, reliability scores, member stats, flake detection

import asyncio
import json
import time
import logging
import pytz
import discord
from datetime import datetime, date, timedelta
from discord.ext import commands, tasks
from typing import Optional, List, Dict

import sys
sys.path.insert(0, '..')
from db import get_db_session
from models import (
    LFGGame, LFGGroup, LFGMember, Guild, GuildModule,
    LFGAttendance, LFGMemberStats, LFGConfig, AttendanceStatus
)
from utils import igdb

# Don't call basicConfig() - config.py already set up logging
logger = logging.getLogger("lfg")

# Timezone aliases
TIMEZONE_ALIASES = {
    "est": "America/New_York", "edt": "America/New_York",
    "cst": "America/Chicago", "cdt": "America/Chicago",
    "mst": "America/Denver", "mdt": "America/Denver",
    "pst": "America/Los_Angeles", "pdt": "America/Los_Angeles",
    "cet": "Europe/Paris", "cest": "Europe/Paris",
    "gmt": "Europe/London", "uk": "Europe/London",
    "jst": "Asia/Tokyo", "tokyo": "Asia/Tokyo",
}

DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


async def _auto_delete_after(interaction_or_message, delay: int = 5):
    """
    Auto-delete an ephemeral message after a delay.

    Args:
        interaction_or_message: discord.Interaction, discord.WebhookMessage, or discord.Message
        delay: Seconds to wait before deletion (default 5)
    """
    try:
        msg_type = type(interaction_or_message).__name__
        logger.debug(f"Scheduled ephemeral deletion in {delay}s (type: {msg_type})")

        await asyncio.sleep(delay)

        if isinstance(interaction_or_message, discord.Interaction):
            await interaction_or_message.delete_original_response()
            logger.info(f"✅ Deleted ephemeral interaction response")
        else:
            # It's a WebhookMessage from followup.send()
            await interaction_or_message.delete()
            logger.info(f"✅ Deleted ephemeral webhook message")

    except discord.errors.NotFound:
        logger.debug(f"Ephemeral message already deleted")
    except discord.errors.Forbidden:
        logger.warning(f"⚠️ Missing permission to delete ephemeral message")
    except Exception as e:
        logger.error(f"❌ Error deleting ephemeral message: {e}", exc_info=True)


async def _auto_confirm_attendance(session, group_id: int, user_id: int) -> bool:
    """
    Auto-confirm attendance when a user joins a group (if attendance tracking is enabled).

    Args:
        session: Database session
        group_id: LFG group ID
        user_id: Discord user ID

    Returns:
        bool: True if attendance was confirmed, False if not enabled or error
    """
    try:
        from models import LFGAttendance, AttendanceStatus

        # Check if user already has an attendance record
        attendance = session.query(LFGAttendance).filter_by(
            group_id=group_id,
            user_id=user_id
        ).first()

        now = int(time.time())
        if not attendance:
            # Create new attendance record with CONFIRMED status
            attendance = LFGAttendance(
                group_id=group_id,
                user_id=user_id,
                status=AttendanceStatus.CONFIRMED,
                confirmed_at=now,
                joined_at=now
            )
            session.add(attendance)
        else:
            # Update existing record to CONFIRMED
            attendance.status = AttendanceStatus.CONFIRMED
            attendance.confirmed_at = now
            if not attendance.joined_at:
                attendance.joined_at = now

        session.commit()
        logger.info(f"✅ Auto-confirmed attendance for user {user_id} in group {group_id}")
        return True

    except Exception as e:
        logger.error(f"❌ Error auto-confirming attendance: {e}", exc_info=True)
        return False


async def _delete_thread_system_message(thread: discord.Thread, user_id: int, delay: int = 5):
    """
    Delete the system message when a user joins/leaves a thread.

    Args:
        thread: The thread where the system message was posted
        user_id: The ID of the user who joined/left
        delay: Seconds to wait before deletion (default 5)
    """
    try:
        # Wait a moment for Discord to create the system message
        await asyncio.sleep(1.0)  # Increased wait time for Discord to generate the message

        # Fetch recent messages and find the system message
        found = False
        async for message in thread.history(limit=15):
            # System messages about thread members
            if message.type in (discord.MessageType.thread_member_join, discord.MessageType.recipient_add, discord.MessageType.recipient_remove):
                # For system messages, the author is the user who joined/left
                # OR check mentions if the message mentions the user
                is_target_user = False

                if message.author and message.author.id == user_id:
                    is_target_user = True
                elif hasattr(message, 'mentions') and message.mentions:
                    # Check if user is mentioned in the system message
                    is_target_user = any(u.id == user_id for u in message.mentions)

                if is_target_user:
                    # Wait remaining time then delete
                    remaining_delay = max(0, delay - 1.0)
                    await asyncio.sleep(remaining_delay)
                    await message.delete()
                    logger.info(f"Deleted thread system message for user {user_id} in thread {thread.id}")
                    found = True
                    break

        if not found:
            logger.debug(f"System message not found for user {user_id} in thread {thread.id}")

    except discord.errors.Forbidden:
        logger.warning(f"Missing permissions to delete system message in thread {thread.id}")
    except Exception as e:
        logger.error(f"Error deleting thread system message: {e}", exc_info=True)


def local_time_to_timestamp(time_str: str, tz_str: str, day_name: str = None) -> int:
    """
    Convert time + timezone + day to Unix timestamp.
    Supports both 12-hour (8:30am, 8:30 PM) and 24-hour (20:00) formats.

    Args:
        time_str: Time string (e.g., "8:30pm", "20:00")
        tz_str: Timezone string (e.g., "America/New_York", "EST")
        day_name: Day of week (e.g., "Monday", "Thursday"). If None, uses today.
    """
    alias_key = tz_str.strip().lower()
    if alias_key in TIMEZONE_ALIASES:
        tz_str = TIMEZONE_ALIASES[alias_key]

    try:
        local_tz = pytz.timezone(tz_str)
    except pytz.UnknownTimeZoneError:
        raise ValueError(f"Unknown timezone: {tz_str}")

    # Try to parse time - support both 12-hour and 24-hour formats
    time_str = time_str.strip().lower()

    # Check if it's 12-hour format (has am/pm)
    if 'am' in time_str or 'pm' in time_str:
        # Parse 12-hour format: "8:30am", "8:30 pm", etc.
        for fmt in ["%I:%M%p", "%I:%M %p"]:  # Try with and without space
            try:
                naive_time = datetime.strptime(time_str, fmt).time()
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"Invalid time format. Use '8:30am' or '8:30 pm'")
    else:
        # Parse 24-hour format: "20:00", "8:30"
        try:
            naive_time = datetime.strptime(time_str, "%H:%M").time()
        except ValueError:
            raise ValueError(f"Invalid time format. Use '8:30am' or '20:00'")

    # Calculate target date based on selected day
    if day_name:
        # Find next occurrence of the selected day
        today = date.today()
        current_weekday = today.weekday()  # 0=Monday, 6=Sunday
        target_weekday = DAYS.index(day_name)  # Get index from DAYS list

        # Calculate days until target day
        days_ahead = target_weekday - current_weekday
        if days_ahead < 0:  # Target day already passed this week
            days_ahead += 7
        elif days_ahead == 0:  # Same day - check if time has passed
            # Create temp datetime to check
            temp_dt = datetime.combine(today, naive_time)
            temp_dt = local_tz.localize(temp_dt)
            if temp_dt < datetime.now(local_tz):
                # Time has passed, use next week
                days_ahead = 7

        target_date = today + timedelta(days=days_ahead)
    else:
        target_date = date.today()

    local_dt = datetime.combine(target_date, naive_time)
    local_dt = local_tz.localize(local_dt)
    utc_dt = local_dt.astimezone(pytz.utc)
    return int(utc_dt.timestamp())


# =============================================================================
# PERSISTENT GROUP MANAGEMENT VIEW (in thread)
# =============================================================================

class RankModal(discord.ui.Modal):
    """Modal for entering rank/level."""
    def __init__(self, view, game: LFGGame):
        super().__init__(title=f"Enter Your {game.rank_label}")
        self.parent_view = view
        self.game = game
        self.rank_input = discord.ui.InputText(
            label=game.rank_label,
            placeholder=f"Enter {game.rank_min} - {game.rank_max}",
            required=True
        )
        self.add_item(self.rank_input)

    async def callback(self, interaction: discord.Interaction):
        try:
            rank = int(self.rank_input.value.strip())
            if not (self.game.rank_min <= rank <= self.game.rank_max):
                raise ValueError
        except ValueError:
            await interaction.response.send_message(
                f"Invalid. Must be {self.game.rank_min}-{self.game.rank_max}.",
                ephemeral=True
            )
            return

        self.parent_view.member_data[interaction.user.id]["rank"] = rank
        await self.parent_view.update_embed()
        await interaction.response.send_message(
            f"Your {self.game.rank_label} set to **{rank}**!",
            ephemeral=True
        )


class GroupManagementView(discord.ui.View):
    """Persistent view in the LFG thread for managing the group."""
    def __init__(self, game: LFGGame, group: LFGGroup, custom_options: List[Dict], config: 'LFGConfig' = None):
        super().__init__(timeout=None)
        self.game = game
        self.group = group
        self.custom_options = custom_options
        self.config = config
        self.message = None

        # user_id -> {"rank": int, "options": {option_name: value}}
        self.member_data = {}

        # Add components
        self._add_components()

    def _add_components(self, user_id: int = None):
        """Add all UI components (rank, options, buttons)."""
        # Add rank button if required
        if self.game.require_rank:
            self.add_item(SetRankButton(self, self.game))

        # Add custom option selects - with user context for conditional filtering
        for i, option in enumerate(self.custom_options[:3]):
            self.add_item(OptionSelect(self, option, i, user_id=user_id))

        # Join and Leave buttons
        self.add_item(JoinGroupButton(self))
        self.add_item(LeaveGroupButton(self))

    async def rebuild_for_user(self, user_id: int):
        """Rebuild view with conditional dropdowns filtered for specific user."""
        # Create new view with same data
        new_view = GroupManagementView(self.game, self.group, self.custom_options, self.config)
        new_view.member_data = self.member_data
        new_view.message = self.message

        # Clear and rebuild components with user context
        new_view.clear_items()
        new_view._add_components(user_id=user_id)

        return new_view

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title=f"{self.game.game_emoji or ''} {self.game.game_name} LFG",
            description=f"Group by <@{self.group.creator_id}>",
            color=discord.Color.green()
        )

        if self.group.scheduled_time:
            embed.add_field(
                name="Scheduled Time",
                value=f"<t:{self.group.scheduled_time}:F>",
                inline=True
            )

        if self.group.event_duration_hours:
            # Format duration nicely
            duration = self.group.event_duration_hours
            if duration == int(duration):
                duration_text = f"{int(duration)} hour{'s' if duration != 1 else ''}"
            else:
                duration_text = f"{duration} hours"

            embed.add_field(
                name="Event Duration",
                value=f"⏰ {duration_text}",
                inline=True
            )

        if self.group.description:
            embed.add_field(name="Description", value=self.group.description, inline=False)

        # Show live player count (privacy-focused - just count, no names)
        if hasattr(self.game, 'current_player_count') and self.game.current_player_count is not None:
            count_text = f"{self.game.current_player_count} members currently playing"
            if self.game.current_player_count == 0:
                count_text = "No one currently playing"
            elif self.game.current_player_count == 1:
                count_text = "1 member currently playing"

            embed.add_field(
                name="🎮 Activity",
                value=count_text,
                inline=True
            )

        # Member list
        member_count = len(self.member_data)
        max_size = self.group.max_group_size or self.game.max_group_size  # Use group's custom size or game default

        if member_count >= max_size:
            embed.description += "\n**Group is FULL!**"

        if not self.member_data:
            embed.add_field(name="Members (0)", value="No one joined yet.", inline=False)
        else:
            lines = []
            for uid, data in self.member_data.items():
                line = f"<@{uid}>"
                if self.game.require_rank and data.get("rank"):
                    line += f" ({self.game.rank_label}: {data['rank']})"
                options = data.get("options", {})
                if options:
                    # Format options nicely - extract from lists and clean up
                    formatted_parts = []

                    # Special handling for Class + Specialization combo (show Spec - Class)
                    if "Specialization" in options and "Class" in options:
                        spec = options["Specialization"][0] if isinstance(options["Specialization"], list) else options["Specialization"]
                        cls = options["Class"][0] if isinstance(options["Class"], list) else options["Class"]
                        formatted_parts.append(f"{spec} - {cls}")
                        # Skip these in the general loop below
                        skip_keys = {"Specialization", "Class"}
                    else:
                        skip_keys = set()

                    # Handle all other options
                    for k, v in options.items():
                        if k in skip_keys:
                            continue
                        # Extract from list if it's a list
                        value = v[0] if isinstance(v, list) and len(v) > 0 else v
                        formatted_parts.append(f"{value}")

                    if formatted_parts:
                        line += f" - {', '.join(formatted_parts)}"
                lines.append(line)
            embed.add_field(
                name=f"Members ({member_count}/{max_size})",
                value="\n".join(lines),
                inline=False
            )

        # Show attendance confirmation status (if enabled)
        if self.config and self.config.attendance_tracking_enabled and self.member_data:
            try:
                with get_db_session() as session:
                    # Get confirmed attendance records
                    confirmed_attendances = session.query(LFGAttendance).filter_by(
                        group_id=self.group.id,
                        status=AttendanceStatus.CONFIRMED
                    ).all()
                    confirmed_ids = [a.user_id for a in confirmed_attendances]

                    if confirmed_ids:
                        confirmed_text = " ".join([f"<@{uid}>" for uid in confirmed_ids[:10]])
                        if len(confirmed_ids) > 10:
                            confirmed_text += f"\n+{len(confirmed_ids) - 10} more"
                        embed.add_field(
                            name=f"📋 Confirmed Attendance ({len(confirmed_ids)}/{member_count})",
                            value=confirmed_text,
                            inline=False
                        )
                    else:
                        embed.add_field(
                            name="📋 Attendance",
                            value="No one has confirmed yet. Click the button below!",
                            inline=False
                        )
            except Exception as e:
                logger.error(f"Error fetching attendance for embed: {e}", exc_info=True)

        embed.set_footer(text=f"Group ID: {self.group.id}")
        return embed

    async def update_embed(self):
        if self.message:
            try:
                await self.message.edit(embed=self.build_embed(), view=self)
            except:
                pass


class SetRankButton(discord.ui.Button):
    def __init__(self, view, game):
        super().__init__(
            label=f"Set {game.rank_label}",
            style=discord.ButtonStyle.blurple,
            custom_id=f"lfg_set_rank_{game.id}"
        )
        self.parent_view = view
        self.game = game

    async def callback(self, interaction: discord.Interaction):
        # Ensure user is in group
        if interaction.user.id not in self.parent_view.member_data:
            self.parent_view.member_data[interaction.user.id] = {"options": {}}
        modal = RankModal(self.parent_view, self.game)
        await interaction.response.send_modal(modal)


class OptionSelect(discord.ui.Select):
    def __init__(self, view, option: Dict, index: int, user_id: int = None):
        self.parent_view = view
        self.option_name = option.get("name", f"Option {index}")
        self.option_config = option
        self.user_id = user_id

        # Get choices - handle both list and conditional dict
        raw_choices = option.get("choices", [])
        depends_on = option.get("depends_on")

        # If choices is a dict (conditional dropdown), filter by parent selection
        if isinstance(raw_choices, dict) and depends_on:
            # Try to get user's parent selection
            parent_value = None
            if user_id and user_id in view.member_data:
                user_options = view.member_data[user_id].get("options", {})
                parent_value = user_options.get(depends_on)

            # Filter choices based on parent selection
            if parent_value and parent_value in raw_choices:
                choices = raw_choices[parent_value][:25]
            else:
                # No parent selected yet - show all possible choices
                all_choices = []
                for parent_val, child_choices in raw_choices.items():
                    if isinstance(child_choices, list):
                        all_choices.extend(child_choices)
                choices = list(set(all_choices))[:25]
        else:
            # Simple list of choices
            choices = raw_choices[:25] if isinstance(raw_choices, list) else []

        options = [discord.SelectOption(label=c) for c in choices] if choices else [discord.SelectOption(label="No options", value="none")]
        super().__init__(
            placeholder=f"Select {self.option_name}",
            min_values=1,
            max_values=1,
            options=options,
            custom_id=f"lfg_option_{index}_{view.group.id}"
        )

    async def callback(self, interaction: discord.Interaction):
        # Auto-join group if user is not already in it
        if interaction.user.id not in self.parent_view.member_data:
            # Check if group is full
            max_size = self.parent_view.group.max_group_size or self.parent_view.game.max_group_size
            if len(self.parent_view.member_data) >= max_size:
                await interaction.response.send_message("❌ This group is full!", ephemeral=True)
                asyncio.create_task(_auto_delete_after(interaction))
                return

            # Add to group
            self.parent_view.member_data[interaction.user.id] = {"options": {}}

            # Add to database
            try:
                with get_db_session() as session:
                    import json as json_lib
                    from models import LFGMember, LFGGroup, LFGConfig

                    # Create member record in database
                    existing_member = session.query(LFGMember).filter_by(
                        group_id=self.parent_view.group.id,
                        user_id=interaction.user.id
                    ).first()

                    if not existing_member:
                        new_member = LFGMember(
                            group_id=self.parent_view.group.id,
                            user_id=interaction.user.id,
                            display_name=interaction.user.display_name,
                            rank_value=None,
                            selections=None,
                            is_creator=False,
                            is_co_leader=False
                        )
                        session.add(new_member)

                        # Update group member count
                        group = session.query(LFGGroup).filter_by(id=self.parent_view.group.id).first()
                        if group:
                            group.member_count += 1
                            if group.member_count >= group.max_group_size:
                                group.is_full = True

                        session.commit()
                        logger.info(f"Created database member record for user {interaction.user.id}")

                    # Auto-confirm attendance if tracking is enabled
                    config = session.query(LFGConfig).filter_by(
                        guild_id=self.parent_view.group.guild_id
                    ).first()

                    if config and config.attendance_tracking_enabled:
                        await _auto_confirm_attendance(session, self.parent_view.group.id, interaction.user.id)
            except Exception as e:
                logger.error(f"Error during auto-join on role selection: {e}")

            # Add to thread
            try:
                thread = interaction.channel
                if isinstance(thread, discord.Thread):
                    await thread.add_user(interaction.user)
            except Exception as e:
                logger.error(f"Failed to add user to thread: {e}")

        selected_value = self.values[0]
        depends_on = self.option_config.get("depends_on")
        raw_choices = self.option_config.get("choices", [])

        # Validate conditional dropdown selection
        if isinstance(raw_choices, dict) and depends_on:
            user_options = self.parent_view.member_data[interaction.user.id].get("options", {})
            parent_value = user_options.get(depends_on)

            if not parent_value:
                # Error: Must select parent first
                await interaction.response.send_message(
                    f"❌ Please select **{depends_on}** first before choosing {self.option_name}!",
                    ephemeral=True
                )
                asyncio.create_task(_auto_delete_after(interaction))
                return

            # Check if selected value is valid for the parent's choices
            valid_choices = raw_choices.get(parent_value, [])
            if selected_value not in valid_choices:
                # Error: Invalid selection
                await interaction.response.send_message(
                    f"❌ **{selected_value}** is not a valid {self.option_name} for {depends_on}: **{parent_value}**!\n"
                    f"Valid options: {', '.join(valid_choices)}",
                    ephemeral=True
                )
                asyncio.create_task(_auto_delete_after(interaction))
                return

        # Validation passed - save the selection
        self.parent_view.member_data[interaction.user.id]["options"][self.option_name] = selected_value

        # Save selection to database
        try:
            with get_db_session() as session:
                import json as json_lib
                from models import LFGMember

                # Get or create member record
                member = session.query(LFGMember).filter_by(
                    group_id=self.parent_view.group.id,
                    user_id=interaction.user.id
                ).first()

                if member:
                    # Update selections
                    member.selections = json_lib.dumps(self.parent_view.member_data[interaction.user.id]["options"])
                    session.commit()
                    logger.info(f"Updated role selection for user {interaction.user.id}: {selected_value}")
        except Exception as e:
            logger.error(f"Error saving role selection to database: {e}")

        # Check if any fields depend on this one
        has_dependents = any(
            opt.get("depends_on") == self.option_name
            for opt in self.parent_view.custom_options
        )

        if has_dependents:
            # Rebuild view with updated dependent dropdowns
            await self.parent_view.update_embed()
            await interaction.response.edit_message(
                embed=self.parent_view.build_embed(),
                view=await self.parent_view.rebuild_for_user(interaction.user.id)
            )
            msg = await interaction.followup.send(
                f"✅ {self.option_name} set to **{selected_value}**!",
                ephemeral=True
            )
            asyncio.create_task(_auto_delete_after(msg))
        else:
            await self.parent_view.update_embed()
            await interaction.response.send_message(
                f"{self.option_name} set to **{selected_value}**!",
                ephemeral=True
            )
            asyncio.create_task(_auto_delete_after(interaction))


class JoinGroupButton(discord.ui.Button):
    def __init__(self, view):
        super().__init__(
            label="Join Group",
            style=discord.ButtonStyle.success,
            custom_id=f"lfg_join_{view.group.id}",
            emoji="✅"
        )
        self.parent_view = view

    async def callback(self, interaction: discord.Interaction):
        # Check if group is full
        max_size = self.parent_view.group.max_group_size or self.parent_view.game.max_group_size
        if len(self.parent_view.member_data) >= max_size:
            await interaction.response.send_message("❌ This group is full!", ephemeral=True)
            asyncio.create_task(_auto_delete_after(interaction))
            return

        # Check if already in group
        if interaction.user.id in self.parent_view.member_data:
            await interaction.response.send_message("You're already in this group!", ephemeral=True)
            asyncio.create_task(_auto_delete_after(interaction))
            return

        # Check if user is blacklisted
        try:
            with get_db_session() as session:
                from models import LFGMemberStats
                stats = session.query(LFGMemberStats).filter_by(
                    guild_id=self.parent_view.group.guild_id,
                    user_id=interaction.user.id
                ).first()

                if stats and stats.is_blacklisted:
                    blacklist_reason = stats.blacklist_reason or "Too many no-shows"
                    await interaction.response.send_message(
                        f"❌ You are blacklisted and cannot join groups.\nReason: {blacklist_reason}",
                        ephemeral=True
                    )
                    asyncio.create_task(_auto_delete_after(interaction))
                    return
        except Exception as e:
            logger.error(f"Error checking blacklist status: {e}")

        # Add to group (in-memory)
        self.parent_view.member_data[interaction.user.id] = {"options": {}}

        # Add to database
        try:
            with get_db_session() as session:
                from models import LFGMember, LFGGroup, LFGConfig
                import json as json_lib

                # Add member to database
                existing_member = session.query(LFGMember).filter_by(
                    group_id=self.parent_view.group.id,
                    user_id=interaction.user.id
                ).first()

                if not existing_member:
                    new_member = LFGMember(
                        group_id=self.parent_view.group.id,
                        user_id=interaction.user.id,
                        display_name=interaction.user.display_name,
                        rank_value=None,
                        selections=None,
                        is_creator=False,
                        is_co_leader=False
                    )
                    session.add(new_member)

                    # Update group member count
                    group = session.query(LFGGroup).filter_by(id=self.parent_view.group.id).first()
                    if group:
                        group.member_count += 1
                        if group.member_count >= group.max_group_size:
                            group.is_full = True

                    session.commit()

                # Auto-confirm attendance if tracking is enabled
                config = session.query(LFGConfig).filter_by(
                    guild_id=self.parent_view.group.guild_id
                ).first()

                if config and config.attendance_tracking_enabled:
                    await _auto_confirm_attendance(session, self.parent_view.group.id, interaction.user.id)
        except Exception as e:
            logger.error(f"Error adding member to database: {e}")

        # Add to thread
        try:
            thread = interaction.channel
            if isinstance(thread, discord.Thread):
                await thread.add_user(interaction.user)
        except Exception as e:
            logger.error(f"Failed to add user to thread: {e}")

        await self.parent_view.update_embed()
        await interaction.response.defer()


class LeaveGroupButton(discord.ui.Button):
    def __init__(self, view):
        super().__init__(
            label="Leave Group",
            style=discord.ButtonStyle.danger,
            custom_id=f"lfg_leave_{view.group.id}",
            emoji="🚪"
        )
        self.parent_view = view

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id in self.parent_view.member_data:
            # Remove from in-memory group data
            del self.parent_view.member_data[interaction.user.id]

            # Remove from database
            try:
                with get_db_session() as session:
                    from models import LFGGroup, LFGAttendance

                    # Remove from LFGMember table
                    session.query(LFGMember).filter_by(
                        group_id=self.parent_view.group.id,
                        user_id=interaction.user.id
                    ).delete()

                    # Remove any attendance confirmations (don't affect reliability)
                    session.query(LFGAttendance).filter_by(
                        group_id=self.parent_view.group.id,
                        user_id=interaction.user.id
                    ).delete()

                    # Update group member count
                    group = session.query(LFGGroup).filter_by(id=self.parent_view.group.id).first()
                    if group and group.member_count > 0:
                        group.member_count -= 1
                        group.is_full = False  # No longer full if someone left

                    session.commit()
                    logger.info(f"User {interaction.user.id} left group {self.parent_view.group.id} - removed from DB")

            except Exception as e:
                logger.error(f"Failed to remove user from database: {e}")

            # Remove from thread
            try:
                thread = interaction.channel
                if isinstance(thread, discord.Thread):
                    await thread.remove_user(interaction.user)
            except Exception as e:
                logger.error(f"Failed to remove user from thread: {e}")

            await self.parent_view.update_embed()
            await interaction.response.defer()
        else:
            # Keep error message since there's no system message for this
            await interaction.response.send_message("You're not in this group.", ephemeral=True)
            asyncio.create_task(_auto_delete_after(interaction))


class ConfirmAttendanceButton(discord.ui.Button):
    """Button to confirm attendance for LFG group (Premium feature)."""
    def __init__(self, view):
        super().__init__(
            label="Confirm Attendance",
            style=discord.ButtonStyle.success,
            custom_id=f"lfg_confirm_{view.group.id}",
            emoji="📋"
        )
        self.parent_view = view

    async def callback(self, interaction: discord.Interaction):
        """Handle attendance confirmation."""
        await interaction.response.defer(ephemeral=True)

        try:
            with get_db_session() as session:
                # Check if user is in the group
                if interaction.user.id not in self.parent_view.member_data:
                    msg = await interaction.followup.send(
                        "You're not in this group! Join first.",
                        ephemeral=True
                    )
                    asyncio.create_task(_auto_delete_after(msg))
                    return

                # Create or update attendance record
                attendance = session.query(LFGAttendance).filter_by(
                    group_id=self.parent_view.group.id,
                    user_id=interaction.user.id
                ).first()

                now = int(time.time())
                if not attendance:
                    attendance = LFGAttendance(
                        group_id=self.parent_view.group.id,
                        user_id=interaction.user.id,
                        status=AttendanceStatus.CONFIRMED,
                        confirmed_at=now,
                        joined_at=now
                    )
                    session.add(attendance)
                else:
                    # Already confirmed
                    if attendance.status == AttendanceStatus.CONFIRMED:
                        msg = await interaction.followup.send(
                            "You've already confirmed your attendance!",
                            ephemeral=True
                        )
                        asyncio.create_task(_auto_delete_after(msg))
                        return

                    attendance.status = AttendanceStatus.CONFIRMED
                    attendance.confirmed_at = now

                # Commit changes to database
                session.commit()

            # Send confirmation message
            msg = await interaction.followup.send(
                "✅ Attendance confirmed! See you there!",
                ephemeral=True
            )
            asyncio.create_task(_auto_delete_after(msg))

            # Update the main embed to show confirmed status (after commit!)
            await self.parent_view.update_embed()

        except Exception as e:
            logger.error(f"Attendance confirm error: {e}")
            msg = await interaction.followup.send(
                "Error confirming attendance!",
                ephemeral=True
            )
            asyncio.create_task(_auto_delete_after(msg))


# =============================================================================
# GROUP CREATION VIEW (ephemeral)
# =============================================================================

class TimezoneSelectView(discord.ui.View):
    """View with timezone dropdown selector."""
    def __init__(self, parent_view, time_str: str):
        super().__init__(timeout=180)
        self.parent_view = parent_view
        self.time_str = time_str

        # Create timezone select menu
        select = discord.ui.Select(
            placeholder="Select your timezone...",
            options=[
                discord.SelectOption(label="🇺🇸 Eastern Time (EST/EDT)", value="America/New_York", description="UTC-5/-4 • New York, Miami, Toronto"),
                discord.SelectOption(label="🇺🇸 Central Time (CST/CDT)", value="America/Chicago", description="UTC-6/-5 • Chicago, Houston, Dallas"),
                discord.SelectOption(label="🇺🇸 Mountain Time (MST/MDT)", value="America/Denver", description="UTC-7/-6 • Denver, Phoenix, Calgary"),
                discord.SelectOption(label="🇺🇸 Pacific Time (PST/PDT)", value="America/Los_Angeles", description="UTC-8/-7 • LA, Seattle, Vancouver"),
                discord.SelectOption(label="🇬🇧 UK Time (GMT/BST)", value="Europe/London", description="UTC+0/+1 • London, Dublin, Lisbon"),
                discord.SelectOption(label="🇪🇺 Central Europe (CET/CEST)", value="Europe/Paris", description="UTC+1/+2 • Paris, Berlin, Rome"),
                discord.SelectOption(label="🇦🇺 Australian Eastern (AEST/AEDT)", value="Australia/Sydney", description="UTC+10/+11 • Sydney, Melbourne"),
                discord.SelectOption(label="🇯🇵 Japan Time (JST)", value="Asia/Tokyo", description="UTC+9 • Tokyo, Seoul"),
                discord.SelectOption(label="🌍 UTC (Universal)", value="UTC", description="Coordinated Universal Time"),
            ]
        )
        select.callback = self.timezone_selected
        self.add_item(select)

    async def timezone_selected(self, interaction: discord.Interaction):
        tz_str = interaction.data['values'][0]
        try:
            # Get selected day from parent view (if set)
            selected_day = getattr(self.parent_view, 'selected_day', None)
            ts = local_time_to_timestamp(self.time_str, tz_str, selected_day)
            self.parent_view.scheduled_time = ts

            # Get timezone display name
            tz_display = next(opt.label for opt in self.children[0].options if opt.value == tz_str)

            await interaction.response.edit_message(
                content=f"✅ **Time set:** <t:{ts}:F> (<t:{ts}:R>)\n*Everyone will see this in their local timezone*",
                view=None
            )
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {e}", ephemeral=True)


class TimeModal(discord.ui.Modal):
    def __init__(self, view):
        super().__init__(title="Enter Play Time")
        self.parent_view = view
        self.add_item(discord.ui.InputText(
            label="Time in YOUR local time",
            placeholder="e.g., 8:30pm or 10:00am",
            required=True
        ))

    async def callback(self, interaction: discord.Interaction):
        time_str = self.children[0].value.strip()

        # Validate time format (supports both 12-hour and 24-hour)
        # Check if format looks valid before showing timezone selector
        time_lower = time_str.lower()
        has_ampm = 'am' in time_lower or 'pm' in time_lower
        has_colon = ':' in time_str

        if not has_colon:
            await interaction.response.send_message(
                "❌ Invalid time format. Please use 12-hour format (e.g., 8:30pm, 10:00am) or 24-hour format (e.g., 20:00)",
                ephemeral=True
            )
            return

        # Show timezone selector (actual validation happens in local_time_to_timestamp)
        view = TimezoneSelectView(self.parent_view, time_str)
        await interaction.response.send_message(
            f"⏰ You entered: **{time_str}**\nNow select your timezone:",
            view=view,
            ephemeral=True
        )


class DescriptionModal(discord.ui.Modal):
    def __init__(self, view):
        super().__init__(title="Group Description")
        self.parent_view = view
        self.add_item(discord.ui.InputText(
            label="Description",
            placeholder="What are you planning to do?",
            style=discord.InputTextStyle.long,
            required=False
        ))

    async def callback(self, interaction: discord.Interaction):
        self.parent_view.description = self.children[0].value
        await interaction.response.send_message("Description saved!", ephemeral=True)
        asyncio.create_task(_auto_delete_after(interaction))


class ThreadTitleModal(discord.ui.Modal):
    def __init__(self, view):
        super().__init__(title="Custom Thread Title")
        self.parent_view = view
        self.add_item(discord.ui.InputText(
            label="Thread Title",
            placeholder="e.g., Raid Night, Nightfall Farming, etc.",
            max_length=50,  # Leave room for name + game
            required=False
        ))

    async def callback(self, interaction: discord.Interaction):
        custom_title = self.children[0].value.strip()
        self.parent_view.custom_thread_title = custom_title if custom_title else None

        if custom_title:
            await interaction.response.send_message(f"✅ Thread title set: **{custom_title}**", ephemeral=True)
        else:
            await interaction.response.send_message("✅ Thread title cleared", ephemeral=True)
        asyncio.create_task(_auto_delete_after(interaction))


class MaxSizeModal(discord.ui.Modal):
    def __init__(self, view, game: LFGGame):
        super().__init__(title="Set Max Group Size")
        self.parent_view = view
        self.game = game
        self.add_item(discord.ui.InputText(
            label=f"Max Size (default: {game.max_group_size})",
            placeholder=f"Enter 2-{game.max_group_size}",
            required=True
        ))

    async def callback(self, interaction: discord.Interaction):
        try:
            size = int(self.children[0].value.strip())
            if not (2 <= size <= 50):
                raise ValueError("Size must be between 2 and 50")

            self.parent_view.max_size = size
            await interaction.response.send_message(
                f"✅ Max group size set to **{size}** (default is {self.game.max_group_size})",
                ephemeral=True
            )
            asyncio.create_task(_auto_delete_after(interaction))
        except ValueError:
            await interaction.response.send_message(
                "❌ Invalid size. Enter a number between 2 and 50.",
                ephemeral=True
            )
            asyncio.create_task(_auto_delete_after(interaction))


class SetThreadTitleButton(discord.ui.Button):
    def __init__(self, view):
        super().__init__(label="Thread Title", style=discord.ButtonStyle.secondary, emoji="📝")
        self.parent_view = view

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(ThreadTitleModal(self.parent_view))


class CustomEventDurationModal(discord.ui.Modal):
    """Modal for custom event duration input."""
    def __init__(self, view):
        super().__init__(title="Custom Event Duration")
        self.parent_view = view
        self.add_item(discord.ui.InputText(
            label="Duration in hours",
            placeholder="e.g., 1, 2.5, 3, 7, 10...",
            max_length=5,
            required=True,
            style=discord.InputTextStyle.short
        ))

    async def callback(self, interaction: discord.Interaction):
        duration_str = self.children[0].value.strip()

        try:
            duration_hours = float(duration_str)
            if not (0.5 <= duration_hours <= 24):
                raise ValueError("Duration must be between 0.5 and 24 hours")

            self.parent_view.event_duration = duration_hours

            # Format display
            if duration_hours == int(duration_hours):
                display = f"{int(duration_hours)} hour{'s' if duration_hours != 1 else ''}"
            else:
                display = f"{duration_hours} hours"

            await interaction.response.send_message(
                f"✅ Event duration set to **{display}**",
                ephemeral=True
            )
            asyncio.create_task(_auto_delete_after(interaction))
        except ValueError:
            await interaction.response.send_message(
                "❌ Invalid duration. Enter a number between 0.5 and 24 hours.",
                ephemeral=True
            )
            asyncio.create_task(_auto_delete_after(interaction))


class EventDurationSelect(discord.ui.Select):
    """Dropdown for selecting event duration."""
    def __init__(self, view):
        options = [
            discord.SelectOption(label="30 minutes", value="0.5", emoji="⏰"),
            discord.SelectOption(label="1 hour", value="1", emoji="⏰"),
            discord.SelectOption(label="2 hours", value="2", emoji="⏰"),
            discord.SelectOption(label="3 hours", value="3", emoji="⏰"),
            discord.SelectOption(label="4 hours", value="4", emoji="⏰"),
            discord.SelectOption(label="5 hours", value="5", emoji="⏰"),
            discord.SelectOption(label="6 hours", value="6", emoji="⏰"),
            discord.SelectOption(label="Custom...", value="custom", emoji="✏️"),
        ]
        super().__init__(
            placeholder="Event duration (optional)",
            min_values=1,
            max_values=1,
            options=options
        )
        self.parent_view = view

    async def callback(self, interaction: discord.Interaction):
        selected_value = self.values[0]

        if selected_value == "custom":
            # Open custom duration modal
            await interaction.response.send_modal(CustomEventDurationModal(self.parent_view))
        else:
            duration_hours = float(selected_value)
            self.parent_view.event_duration = duration_hours

            # Format display
            if duration_hours < 1:
                display = f"{int(duration_hours * 60)} minutes"
            elif duration_hours == int(duration_hours):
                display = f"{int(duration_hours)} hour{'s' if duration_hours != 1 else ''}"
            else:
                display = f"{duration_hours} hours"

            await interaction.response.send_message(
                f"✅ Event duration set to **{display}**",
                ephemeral=True
            )
            asyncio.create_task(_auto_delete_after(interaction))


class CreationView(discord.ui.View):
    """Ephemeral view for creating a new LFG group."""
    def __init__(self, game: LFGGame, custom_options: List[Dict], message=None):
        super().__init__(timeout=300)
        self.game = game
        self.custom_options = custom_options
        self.scheduled_time = None
        self.description = None
        self.custom_thread_title = None  # Custom thread title
        self.rank = None
        self.max_size = None  # Custom max size (overrides game default)
        self.thread_duration = None  # Custom thread auto-archive duration (in minutes)
        self.event_duration = None  # Event duration display (in hours)
        self.selections = {}
        self.message = message  # Store message reference for editing

        # Day selector
        self.add_item(DaySelect(self))

        # Add custom option dropdowns
        self._add_custom_options()

        # Time selector (dropdown)
        self.add_item(TimeSelect(self))

        # Event duration dropdown
        self.add_item(EventDurationSelect(self))

        # Buttons
        if game.require_rank:
            self.add_item(SetRankCreationButton(self, game))
        self.add_item(SetThreadTitleButton(self))
        self.add_item(SetMaxSizeButton(self, game))
        self.add_item(SetDescriptionButton(self))
        self.add_item(SubmitButton(self))

    def _add_custom_options(self):
        """Add custom option dropdowns, respecting dependencies."""
        for i, option in enumerate(self.custom_options[:3]):
            depends_on = option.get("depends_on")

            # If this field depends on another, only add it if parent has been selected
            if depends_on:
                if depends_on in self.selections:
                    # Parent selected - add this field with filtered choices
                    self.add_item(CreationOptionSelect(self, option, i))
            else:
                # No dependency - always add
                self.add_item(CreationOptionSelect(self, option, i))

    async def rebuild(self, interaction: discord.Interaction):
        """Rebuild the view with updated dependent dropdowns."""
        # Clear all items except buttons
        self.clear_items()

        # Re-add day selector
        self.add_item(DaySelect(self))

        # Re-add custom options (now with dependent fields if parent is selected)
        self._add_custom_options()

        # Re-add time selector
        self.add_item(TimeSelect(self))

        # Re-add event duration dropdown
        self.add_item(EventDurationSelect(self))

        # Re-add buttons
        if self.game.require_rank:
            self.add_item(SetRankCreationButton(self, self.game))
        self.add_item(SetThreadTitleButton(self))
        self.add_item(SetMaxSizeButton(self, self.game))
        self.add_item(SetDescriptionButton(self))
        self.add_item(SubmitButton(self))

        # Edit the message with updated view
        try:
            await interaction.edit_original_response(view=self)
        except:
            pass  # If edit fails, continue silently


class DaySelect(discord.ui.Select):
    def __init__(self, view):
        options = [discord.SelectOption(label=d) for d in DAYS]
        super().__init__(placeholder="Select day", min_values=1, max_values=1, options=options)
        self.parent_view = view

    async def callback(self, interaction: discord.Interaction):
        self.parent_view.selected_day = self.values[0]
        await interaction.response.send_message(f"Day: {self.values[0]}", ephemeral=True)
        asyncio.create_task(_auto_delete_after(interaction))


class CreationOptionSelect(discord.ui.Select):
    def __init__(self, view, option: Dict, index: int):
        self.parent_view = view
        self.option_name = option.get("name", f"Option {index}")
        self.option_config = option  # Store full config for dependency checks

        # Get choices - handle both list and conditional dict
        raw_choices = option.get("choices", [])
        depends_on = option.get("depends_on")

        # If choices is a dict (conditional dropdown), filter by parent selection
        if isinstance(raw_choices, dict) and depends_on:
            parent_value = view.selections.get(depends_on, [None])[0]  # Get first value if list
            choices = raw_choices.get(parent_value, [])[:25]
        else:
            # Simple list of choices
            choices = raw_choices[:25] if isinstance(raw_choices, list) else []

        options = [discord.SelectOption(label=c) for c in choices]
        max_vals = option.get("max_select", 1)
        super().__init__(
            placeholder=f"Select {self.option_name}",
            min_values=1,
            max_values=min(max_vals, len(choices)) if choices else 1,
            options=options if options else [discord.SelectOption(label="No options", value="none")]
        )

    async def callback(self, interaction: discord.Interaction):
        self.parent_view.selections[self.option_name] = self.values

        # Check if any fields depend on this one
        has_dependents = any(
            opt.get("depends_on") == self.option_name
            for opt in self.parent_view.custom_options
        )

        if has_dependents:
            # Rebuild view to show dependent dropdowns
            await interaction.response.defer()
            await self.parent_view.rebuild(interaction)
            msg = await interaction.followup.send(
                f"✅ {self.option_name}: {', '.join(self.values)}",
                ephemeral=True
            )
            asyncio.create_task(_auto_delete_after(msg))
        else:
            # Normal response
            await interaction.response.send_message(
                f"{self.option_name}: {', '.join(self.values)}",
                ephemeral=True
            )
            asyncio.create_task(_auto_delete_after(interaction))


class TimeSelect(discord.ui.Select):
    """Dropdown for selecting common times or custom time."""
    def __init__(self, view):
        options = [
            discord.SelectOption(label="🕐 12:00 PM (Noon)", value="12:00pm"),
            discord.SelectOption(label="🕐 1:00 PM", value="1:00pm"),
            discord.SelectOption(label="🕑 2:00 PM", value="2:00pm"),
            discord.SelectOption(label="🕒 3:00 PM", value="3:00pm"),
            discord.SelectOption(label="🕓 4:00 PM", value="4:00pm"),
            discord.SelectOption(label="🕔 5:00 PM", value="5:00pm"),
            discord.SelectOption(label="🕕 6:00 PM", value="6:00pm"),
            discord.SelectOption(label="🕖 7:00 PM", value="7:00pm"),
            discord.SelectOption(label="🕗 8:00 PM", value="8:00pm"),
            discord.SelectOption(label="🕘 9:00 PM", value="9:00pm"),
            discord.SelectOption(label="🕙 10:00 PM", value="10:00pm"),
            discord.SelectOption(label="🕚 11:00 PM", value="11:00pm"),
            discord.SelectOption(label="✏️ Custom Time...", value="custom", description="Enter a specific time"),
        ]
        super().__init__(
            placeholder="⏰ Select event time...",
            options=options,
            custom_id="time_select"
        )
        self.parent_view = view

    async def callback(self, interaction: discord.Interaction):
        selected = self.values[0]

        if selected == "custom":
            # Open modal for custom time
            await interaction.response.send_modal(TimeModal(self.parent_view))
        else:
            # Use preset time - show timezone selector
            view = TimezoneSelectView(self.parent_view, selected)
            await interaction.response.send_message(
                f"⏰ Selected time: **{selected}**\nNow select your timezone:",
                view=view,
                ephemeral=True
            )


class SetRankCreationButton(discord.ui.Button):
    def __init__(self, view, game):
        super().__init__(label=f"Set {game.rank_label}", style=discord.ButtonStyle.secondary)
        self.parent_view = view
        self.game = game

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(RankModal(self.parent_view, self.game))


class SetDescriptionButton(discord.ui.Button):
    def __init__(self, view):
        super().__init__(label="Add Description", style=discord.ButtonStyle.secondary)
        self.parent_view = view

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(DescriptionModal(self.parent_view))


class SetMaxSizeButton(discord.ui.Button):
    def __init__(self, view, game: LFGGame):
        super().__init__(label=f"Max Size ({game.max_group_size})", style=discord.ButtonStyle.secondary)
        self.parent_view = view
        self.game = game

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(MaxSizeModal(self.parent_view, self.game))


class SubmitButton(discord.ui.Button):
    def __init__(self, view):
        super().__init__(label="Create Group", style=discord.ButtonStyle.success)
        self.parent_view = view

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        view = self.parent_view
        game = view.game

        # Validate
        if not view.scheduled_time:
            msg = await interaction.followup.send("Please set a time first!", ephemeral=True)
            asyncio.create_task(_auto_delete_after(msg))
            return

        # Get LFG channel
        if not game.lfg_channel_id:
            msg = await interaction.followup.send("No LFG channel configured!", ephemeral=True)
            asyncio.create_task(_auto_delete_after(msg))
            return

        channel = interaction.guild.get_channel(game.lfg_channel_id)
        if not channel:
            msg = await interaction.followup.send("LFG channel not found!", ephemeral=True)
            asyncio.create_task(_auto_delete_after(msg))
            return

        # Create thread
        thread_name = f"{interaction.user.display_name} - {game.game_name}"
        if view.custom_thread_title:
            thread_name += f" - {view.custom_thread_title}"
        elif view.rank:
            thread_name += f" - {game.rank_label} {view.rank}"

        try:
            thread = await channel.create_thread(
                name=thread_name[:100],
                type=discord.ChannelType.public_thread,
                auto_archive_duration=game.thread_auto_archive_hours * 60
            )
            await thread.join()
            await thread.add_user(interaction.user)
        except Exception as e:
            logger.error(f"Error creating thread: {e}")
            msg = await interaction.followup.send("Error creating thread!", ephemeral=True)
            asyncio.create_task(_auto_delete_after(msg))
            return

        # Save to database
        try:
            with get_db_session() as session:
                group = LFGGroup(
                    guild_id=interaction.guild.id,
                    game_id=game.id,
                    thread_id=thread.id,
                    thread_name=thread_name,
                    creator_id=interaction.user.id,
                    creator_name=interaction.user.display_name,
                    scheduled_time=view.scheduled_time,
                    description=view.description,
                    custom_data=json.dumps(view.selections) if view.selections else None,
                    max_group_size=view.max_size,  # Custom size or None (uses game default)
                    event_duration_hours=view.event_duration,  # Custom event duration
                )
                session.add(group)
                session.flush()
                group_id = group.id

                # Add creator as member
                member = LFGMember(
                    group_id=group_id,
                    user_id=interaction.user.id,
                    display_name=interaction.user.display_name,
                    rank_value=view.rank,
                    selections=json.dumps(view.selections) if view.selections else None,
                    is_creator=True
                )
                session.add(member)

                # Auto-confirm attendance for creator if tracking is enabled
                lfg_config = session.query(LFGConfig).filter_by(guild_id=interaction.guild.id).first()
                if lfg_config and lfg_config.attendance_tracking_enabled:
                    await _auto_confirm_attendance(session, group_id, interaction.user.id)

        except Exception as e:
            logger.error(f"DB error: {e}")
            msg = await interaction.followup.send("Database error!", ephemeral=True)
            asyncio.create_task(_auto_delete_after(msg))
            return

        # Ping role if configured
        if game.notify_role_id:
            ping_msg = await thread.send(f"<@&{game.notify_role_id}> New {game.game_name} group!")
            # Auto-delete ping message after 5 seconds
            asyncio.create_task(_auto_delete_after(ping_msg, 5))

        # Get config for attendance tracking
        lfg_config = None
        try:
            with get_db_session() as session:
                lfg_config = session.query(LFGConfig).filter_by(guild_id=interaction.guild.id).first()
        except Exception as e:
            logger.error(f"Error fetching LFG config: {e}")

        # Create management view (with attendance button if enabled)
        custom_options = json.loads(game.custom_options) if game.custom_options else []
        mgmt_view = GroupManagementView(game, group, custom_options, lfg_config)
        mgmt_view.member_data[interaction.user.id] = {
            "rank": view.rank,
            "options": view.selections
        }

        msg = await thread.send(embed=mgmt_view.build_embed(), view=mgmt_view)
        mgmt_view.message = msg

        # Update DB with message ID
        try:
            with get_db_session() as session:
                g = session.query(LFGGroup).filter_by(id=group_id).first()
                if g:
                    g.management_message_id = msg.id
        except:
            pass

        # No success message needed - "started a thread" notification already shows this

        # Auto-archive after timeout
        async def archive_thread():
            await asyncio.sleep(game.thread_auto_archive_hours * 3600)
            try:
                await thread.edit(archived=True)
            except:
                pass
        asyncio.create_task(archive_thread())


# =============================================================================
# IGDB GAME SEARCH RESULT VIEW
# =============================================================================

class GameSearchResultView(discord.ui.View):
    """View for selecting a game from IGDB search results."""
    def __init__(self, games: List[igdb.IGDBGame], author_id: int):
        super().__init__(timeout=300)
        self.games = games
        self.author_id = author_id

        # Create select menu with games
        options = []
        for game in games[:25]:  # Discord limit
            year = f" ({game.release_year})" if game.release_year else ""
            platforms = ", ".join(game.platforms[:3]) if game.platforms else ""
            description = platforms[:100] if platforms else "No platform info"
            options.append(discord.SelectOption(
                label=f"{game.name[:95]}{year}",
                value=str(game.id),
                description=description
            ))

        if options:
            self.add_item(GameSelectMenu(options, self))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("This isn't your search!", ephemeral=True)
            return False
        return True


class GameSelectMenu(discord.ui.Select):
    def __init__(self, options: List[discord.SelectOption], view):
        super().__init__(
            placeholder="Select a game to add...",
            min_values=1,
            max_values=1,
            options=options
        )
        self.parent_view = view

    async def callback(self, interaction: discord.Interaction):
        selected_id = int(self.values[0])

        # Find the selected game
        selected_game = None
        for game in self.parent_view.games:
            if game.id == selected_id:
                selected_game = game
                break

        if not selected_game:
            await interaction.response.send_message("Game not found!", ephemeral=True)
            return

        # Show the game info and next steps
        embed = discord.Embed(
            title=f"Selected: {selected_game.name}",
            description="Use the command below to add this game to LFG:",
            color=discord.Color.green()
        )

        if selected_game.cover_url:
            embed.set_thumbnail(url=selected_game.cover_url)

        platforms = ", ".join(selected_game.platforms) if selected_game.platforms else "Unknown"
        embed.add_field(name="Platforms", value=platforms, inline=True)
        embed.add_field(name="IGDB ID", value=str(selected_game.id), inline=True)

        # Suggest a short code
        words = selected_game.name.split()
        suggested_code = "".join(w[0].upper() for w in words[:4] if w[0].isalpha())
        if len(suggested_code) < 2:
            suggested_code = selected_game.name[:3].upper()

        embed.add_field(
            name="Next Step",
            value=f"```\n/lfg_add igdb_id:{selected_game.id} short_code:{suggested_code} channel:#your-lfg-channel\n```",
            inline=False
        )

        await interaction.response.edit_message(embed=embed, view=None)
        self.parent_view.stop()


# =============================================================================
# MAIN COG
# =============================================================================

class LFGCog(commands.Cog):
    """Generic LFG system that works with any game."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.attendance_check_loop.start()  # Start attendance tracking task
        self.reconnect_persistent_views.start()  # Reconnect to existing LFG threads

    def cog_unload(self):
        """Cancel background tasks when cog is unloaded."""
        self.attendance_check_loop.cancel()
        self.reconnect_persistent_views.cancel()

    @tasks.loop(count=1)
    async def reconnect_persistent_views(self):
        """Reconnect to all active LFG group views on bot startup."""
        await self.bot.wait_until_ready()
        logger.info("Reconnecting to persistent LFG views...")

        try:
            with get_db_session() as session:
                # ===== RECONNECT PERSISTENT LFG MENU EMBEDS =====
                # Get all guilds with LFG configured
                all_guilds = session.query(LFGGame.guild_id).distinct().all()

                for (guild_id,) in all_guilds:
                    try:
                        # Get all enabled games for this guild
                        games = session.query(LFGGame).filter(
                            LFGGame.guild_id == guild_id,
                            LFGGame.enabled == True
                        ).all()

                        if games:
                            # Create a PersistentLFGView for this guild's games
                            # Player counts will update when buttons are clicked
                            player_counts = {game.id: 0 for game in games}  # Placeholder
                            menu_view = PersistentLFGView(games, player_counts)
                            self.bot.add_view(menu_view)
                            logger.debug(f"Reconnected persistent menu for guild {guild_id} ({len(games)} games)")

                    except Exception as e:
                        logger.error(f"Failed to reconnect persistent menu for guild {guild_id}: {e}")

                # ===== RECONNECT ACTIVE LFG GROUP THREADS =====
                active_groups = session.query(LFGGroup).filter(
                    LFGGroup.is_active == True
                ).all()

                logger.info(f"Found {len(active_groups)} active LFG groups to reconnect")

                for group in active_groups:
                    try:
                        # Get game config
                        game = session.query(LFGGame).filter_by(id=group.game_id).first()
                        if not game:
                            logger.warning(f"Game not found for group {group.id}")
                            continue

                        # Get LFG config for attendance tracking
                        config = session.query(LFGConfig).filter_by(guild_id=game.guild_id).first()

                        # Parse custom options
                        custom_options = []
                        if game.custom_options:
                            try:
                                custom_options = json.loads(game.custom_options)
                            except:
                                pass

                        # Recreate the view
                        view = GroupManagementView(game, group, custom_options, config)

                        # Load existing members into view
                        members = session.query(LFGMember).filter_by(group_id=group.id).all()
                        for member in members:
                            # Reconstruct member data from JSON selections
                            member_options = {}
                            if member.selections:
                                try:
                                    member_options = json.loads(member.selections)
                                except:
                                    pass

                            view.member_data[member.user_id] = {
                                "rank": member.rank_value,
                                "options": member_options
                            }

                        # Add view to bot (reconnects to existing message)
                        self.bot.add_view(view)
                        logger.debug(f"Reconnected view for group {group.id} ({game.game_name})")

                    except Exception as e:
                        logger.error(f"Failed to reconnect group {group.id}: {e}")

                logger.info("✅ Persistent view reconnection complete")

        except Exception as e:
            logger.error(f"Error reconnecting persistent views: {e}")

    @tasks.loop(minutes=15)
    async def attendance_check_loop(self):
        """Background task to auto-mark no-shows and enforce attendance policies."""
        try:
            await self._check_attendance_and_enforce_policies()
        except Exception as e:
            logger.error(f"[AttendanceCheck] Error in loop: {e}", exc_info=True)

    @attendance_check_loop.before_loop
    async def before_attendance_check(self):
        """Wait for bot to be ready before starting loop."""
        await self.bot.wait_until_ready()
        logger.info("[AttendanceCheck] Bot ready, starting attendance check loop")

    async def _check_attendance_and_enforce_policies(self):
        """Check for overdue groups and enforce attendance policies."""
        with get_db_session() as session:
            now = int(time.time())

            # Get all guilds with attendance tracking enabled
            configs = session.query(LFGConfig).filter(
                LFGConfig.attendance_tracking_enabled == True
            ).all()

            for config in configs:
                try:
                    # Skip if auto no-show is disabled
                    if not config.auto_noshow_hours or config.auto_noshow_hours <= 0:
                        continue

                    # Find groups past their time + grace period
                    cutoff_time = now - (config.auto_noshow_hours * 3600)

                    # Get overdue groups for this guild
                    overdue_groups = session.query(LFGGroup).filter(
                        LFGGroup.guild_id == config.guild_id,
                        LFGGroup.is_active == True,
                        LFGGroup.scheduled_time <= cutoff_time
                    ).all()

                    logger.debug(f"[AttendanceCheck] Guild {config.guild_id}: found {len(overdue_groups)} overdue groups")

                    for group in overdue_groups:
                        # Get all members
                        members = session.query(LFGMember).filter_by(group_id=group.id).all()

                        for member in members:
                            # Check attendance record
                            attendance = session.query(LFGAttendance).filter_by(
                                group_id=group.id,
                                user_id=member.user_id
                            ).first()

                            # Auto-mark as no-show if:
                            # 1. No attendance record exists, OR
                            # 2. Attendance exists but status is PENDING, OR
                            # 3. Attendance exists but not marked as SHOWED
                            should_mark_noshow = (
                                not attendance or
                                attendance.status == AttendanceStatus.PENDING or
                                (attendance.status != AttendanceStatus.SHOWED and
                                 attendance.status != AttendanceStatus.CANCELLED)
                            )

                            if should_mark_noshow:
                                if not attendance:
                                    attendance = LFGAttendance(
                                        group_id=group.id,
                                        user_id=member.user_id,
                                        status=AttendanceStatus.NO_SHOW,
                                        marked_by=0  # System
                                    )
                                    session.add(attendance)
                                else:
                                    attendance.status = AttendanceStatus.NO_SHOW
                                    attendance.marked_by = 0  # System

                                # Update member stats
                                stats = self._update_member_stats(
                                    session,
                                    config.guild_id,
                                    member.user_id,
                                    AttendanceStatus.NO_SHOW
                                )

                                logger.info(f"[AttendanceCheck] Auto-marked {member.user_id} as no-show for group {group.id}")

                                # Check for auto-warning
                                if config.warn_at_reliability and stats.reliability_score <= config.warn_at_reliability:
                                    await self._send_reliability_warning(member.user_id, stats, config)

                                # Check for auto-blacklist
                                if config.auto_blacklist_noshows and stats.total_no_shows >= config.auto_blacklist_noshows:
                                    if not stats.is_blacklisted:
                                        stats.is_blacklisted = True
                                        stats.blacklisted_at = now
                                        stats.blacklisted_by = 0  # System
                                        stats.blacklist_reason = f"Auto-blacklisted after {stats.total_no_shows} no-shows"
                                        logger.info(f"[AttendanceCheck] Auto-blacklisted user {member.user_id}")
                                        await self._send_blacklist_notification(member.user_id, stats, config)

                        # Mark group as inactive
                        group.is_active = False

                except Exception as e:
                    logger.error(f"[AttendanceCheck] Error processing guild {config.guild_id}: {e}", exc_info=True)

    async def _send_reliability_warning(self, user_id: int, stats: LFGMemberStats, config: LFGConfig):
        """Send DM warning when reliability drops below threshold."""
        try:
            user = await self.bot.fetch_user(user_id)
            if not user:
                return

            guild = self.bot.get_guild(config.guild_id)
            if not guild:
                return

            embed = discord.Embed(
                title="⚠️ Reliability Warning",
                description=f"Your attendance reliability in **{guild.name}** has dropped below the required threshold.",
                color=discord.Color.orange()
            )
            embed.add_field(
                name="Current Reliability",
                value=f"{stats.reliability_score}%",
                inline=True
            )
            embed.add_field(
                name="Required Minimum",
                value=f"{config.warn_at_reliability}%",
                inline=True
            )
            embed.add_field(
                name="Stats",
                value=f"Shows: {stats.total_showed}\nNo-shows: {stats.total_no_shows}",
                inline=False
            )
            embed.set_footer(text="Confirm attendance and show up to events to improve your score!")

            await user.send(embed=embed)
            logger.info(f"[AttendanceCheck] Sent reliability warning to user {user_id}")

        except discord.Forbidden:
            logger.debug(f"[AttendanceCheck] Cannot DM user {user_id} (DMs disabled)")
        except Exception as e:
            logger.error(f"[AttendanceCheck] Error sending warning to {user_id}: {e}")

    async def _send_blacklist_notification(self, user_id: int, stats: LFGMemberStats, config: LFGConfig):
        """Send DM notification when user is blacklisted."""
        try:
            user = await self.bot.fetch_user(user_id)
            if not user:
                return

            guild = self.bot.get_guild(config.guild_id)
            if not guild:
                return

            embed = discord.Embed(
                title="🚫 LFG Blacklisted",
                description=f"You have been blacklisted from LFG in **{guild.name}** due to excessive no-shows.",
                color=discord.Color.red()
            )
            embed.add_field(
                name="Reason",
                value=stats.blacklist_reason or "Too many no-shows",
                inline=False
            )
            embed.add_field(
                name="Total No-Shows",
                value=str(stats.total_no_shows),
                inline=True
            )
            embed.add_field(
                name="Reliability Score",
                value=f"{stats.reliability_score}%",
                inline=True
            )
            embed.set_footer(text="Contact a server administrator to appeal this decision.")

            await user.send(embed=embed)
            logger.info(f"[AttendanceCheck] Sent blacklist notification to user {user_id}")

        except discord.Forbidden:
            logger.debug(f"[AttendanceCheck] Cannot DM user {user_id} (DMs disabled)")
        except Exception as e:
            logger.error(f"[AttendanceCheck] Error sending blacklist to {user_id}: {e}")

    async def game_autocomplete(self, ctx: discord.AutocompleteContext):
        """Autocomplete for available games."""
        try:
            with get_db_session() as session:
                games = session.query(LFGGame).filter(
                    LFGGame.guild_id == ctx.interaction.guild.id,
                    LFGGame.enabled == True
                ).all()
                return [
                    discord.OptionChoice(name=g.game_name, value=g.game_short)
                    for g in games
                ]
        except:
            return []

    @discord.slash_command(name="lfg", description="Create a Looking For Group post")
    @discord.option("game", description="Which game?", autocomplete=game_autocomplete)
    async def lfg_command(self, ctx: discord.ApplicationContext, game: str):
        """Start the LFG creation flow."""
        try:
            with get_db_session() as session:
                game_config = session.query(LFGGame).filter(
                    LFGGame.guild_id == ctx.guild.id,
                    LFGGame.game_short.ilike(game),
                    LFGGame.enabled == True
                ).first()

                if not game_config:
                    await ctx.respond(
                        f"Game '{game}' not found or not enabled. Ask an admin to configure it.",
                        ephemeral=True
                    )
                    return

                # Parse custom options
                custom_options = []
                if game_config.custom_options:
                    try:
                        custom_options = json.loads(game_config.custom_options)
                    except:
                        pass

                embed = discord.Embed(
                    title=f"{game_config.game_emoji or ''} {game_config.game_name} LFG",
                    description=(
                        "**Set up your group:**\n"
                        "1. Select a day and your options\n"
                        "2. Click 'Set Time' to enter your play time\n"
                        "3. Click 'Create Group' when ready!"
                    ),
                    color=discord.Color.blurple()
                )

                view = CreationView(game_config, custom_options)
                await ctx.respond(embed=embed, view=view, ephemeral=True)

        except Exception as e:
            logger.error(f"LFG error: {e}")
            await ctx.respond("Something went wrong!", ephemeral=True)

    # =============================================================================
    # HELPER: Check premium status
    # =============================================================================

    def _has_lfg_access(self, session, guild_id: int) -> bool:
        """Check if guild has LFG access (Complete tier, VIP, or LFG module)."""
        guild = session.query(Guild).filter_by(guild_id=guild_id).first()
        if not guild:
            return False
        if guild.is_vip or guild.subscription_tier == 'complete':
            return True
        # Check for LFG module subscription
        has_lfg_module = session.query(GuildModule).filter_by(
            guild_id=guild_id,
            module_name='lfg',
            enabled=True
        ).first() is not None
        return has_lfg_module

    # =============================================================================
    # ADMIN COMMANDS
    # =============================================================================

    @discord.slash_command(name="lfg_search", description="Search IGDB for a game to add (Admin)")
    @discord.default_permissions(administrator=True)
    @discord.option("query", description="Game name to search for")
    async def lfg_search(self, ctx: discord.ApplicationContext, query: str):
        """Search IGDB for games to add to LFG."""
        await ctx.defer(ephemeral=True)

        if not igdb.is_configured():
            await ctx.respond(
                "IGDB is not configured. Ask the bot owner to set TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET.",
                ephemeral=True
            )
            return

        games = await igdb.search_games(query, limit=10)

        if not games:
            await ctx.respond(
                f"No games found for '{query}'.\n"
                "Use `/lfg_custom` to add a custom game (Premium).",
                ephemeral=True
            )
            return

        # Build selection view
        embed = discord.Embed(
            title=f"Search Results: {query}",
            description="Select a game to add to your LFG system:",
            color=discord.Color.blurple()
        )

        for i, game in enumerate(games[:10], 1):
            platforms = ", ".join(game.platforms[:5]) if game.platforms else "Unknown"
            year = f" ({game.release_year})" if game.release_year else ""
            embed.add_field(
                name=f"{i}. {game.name}{year}",
                value=f"Platforms: {platforms}",
                inline=False
            )

        view = GameSearchResultView(games, ctx.author.id)
        await ctx.respond(embed=embed, view=view, ephemeral=True)

    @discord.slash_command(name="lfg_add", description="Add a game from IGDB to LFG (Admin)")
    @discord.default_permissions(administrator=True)
    @discord.option("igdb_id", description="IGDB game ID (from /lfg_search)")
    @discord.option("short_code", description="Short code for commands (e.g., MHW)")
    @discord.option("channel", description="Channel for LFG threads")
    @discord.option("notify_role", description="Role to ping for new groups", required=False)
    @discord.option("max_size", description="Max group size", required=False, default=4)
    async def lfg_add(
        self,
        ctx: discord.ApplicationContext,
        igdb_id: int,
        short_code: str,
        channel: discord.TextChannel,
        notify_role: discord.Role = None,
        max_size: int = 4
    ):
        """Add an IGDB game to the LFG system."""
        await ctx.defer(ephemeral=True)

        # Fetch game from IGDB
        game_data = await igdb.get_game_by_id(igdb_id)
        if not game_data:
            await ctx.respond("Game not found on IGDB!", ephemeral=True)
            return

        try:
            with get_db_session() as session:
                # Check if already exists
                existing = session.query(LFGGame).filter(
                    LFGGame.guild_id == ctx.guild.id,
                    LFGGame.game_short.ilike(short_code)
                ).first()

                if existing:
                    await ctx.respond(
                        f"A game with short code '{short_code}' already exists!",
                        ephemeral=True
                    )
                    return

                # Create new game
                game = LFGGame(
                    guild_id=ctx.guild.id,
                    game_name=game_data.name,
                    game_short=short_code.upper(),
                    igdb_id=game_data.id,
                    igdb_slug=game_data.slug,
                    cover_url=game_data.cover_url,
                    platforms=",".join(game_data.platforms[:10]) if game_data.platforms else None,
                    is_custom_game=False,
                    lfg_channel_id=channel.id,
                    notify_role_id=notify_role.id if notify_role else None,
                    max_group_size=max_size,
                    created_by=ctx.author.id
                )
                session.add(game)

                embed = discord.Embed(
                    title="Game Added to LFG!",
                    description=f"**{game_data.name}** is now available for LFG.",
                    color=discord.Color.green()
                )
                if game_data.cover_url:
                    embed.set_thumbnail(url=game_data.cover_url)
                embed.add_field(name="Short Code", value=short_code.upper(), inline=True)
                embed.add_field(name="Channel", value=channel.mention, inline=True)
                embed.add_field(name="Max Size", value=str(max_size), inline=True)

                embed.add_field(
                    name="Next Steps",
                    value=(
                        f"Use `/lfg_options {short_code.upper()}` to add custom dropdowns (classes, weapons, etc.)\n"
                        f"Use `/lfg_rank {short_code.upper()}` to require rank/level input"
                    ),
                    inline=False
                )

                await ctx.respond(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Error adding game: {e}")
            await ctx.respond("Error adding game!", ephemeral=True)

    @discord.slash_command(name="lfg_custom", description="Add a custom game not in IGDB (Admin)")
    @discord.default_permissions(administrator=True)
    @discord.option("game_name", description="Full game name")
    @discord.option("short_code", description="Short code for commands")
    @discord.option("channel", description="Channel for LFG threads")
    @discord.option("notify_role", description="Role to ping", required=False)
    @discord.option("max_size", description="Max group size", required=False, default=4)
    async def lfg_custom(
        self,
        ctx: discord.ApplicationContext,
        game_name: str,
        short_code: str,
        channel: discord.TextChannel,
        notify_role: discord.Role = None,
        max_size: int = 4
    ):
        """Add a custom game (for games not in IGDB)."""
        try:
            with get_db_session() as session:
                # Check if exists
                existing = session.query(LFGGame).filter(
                    LFGGame.guild_id == ctx.guild.id,
                    LFGGame.game_short.ilike(short_code)
                ).first()

                if existing:
                    await ctx.respond(f"Short code '{short_code}' already exists!", ephemeral=True)
                    return

                game = LFGGame(
                    guild_id=ctx.guild.id,
                    game_name=game_name,
                    game_short=short_code.upper(),
                    is_custom_game=True,
                    lfg_channel_id=channel.id,
                    notify_role_id=notify_role.id if notify_role else None,
                    max_group_size=max_size,
                    created_by=ctx.author.id
                )
                session.add(game)

                await ctx.respond(
                    f"Created custom game **{game_name}**!\n"
                    f"Use `/lfg_options {short_code}` to add custom options (classes, weapons, etc.).",
                    ephemeral=True
                )

        except Exception as e:
            logger.error(f"Error creating custom game: {e}")
            await ctx.respond("Error creating game!", ephemeral=True)

    @discord.slash_command(name="lfg_options", description="Add custom options to a game (Admin)")
    @discord.default_permissions(administrator=True)
    @discord.option("game", description="Game short code")
    @discord.option("options_json", description='JSON: [{"name": "Class", "choices": ["Warrior", "Mage"]}]')
    async def lfg_options(self, ctx: discord.ApplicationContext, game: str, options_json: str):
        """Set custom options for a game (classes, weapons, monsters, etc.)."""
        try:
            with get_db_session() as session:
                options = json.loads(options_json)

                game_config = session.query(LFGGame).filter(
                    LFGGame.guild_id == ctx.guild.id,
                    LFGGame.game_short.ilike(game)
                ).first()

                if not game_config:
                    await ctx.respond("Game not found!", ephemeral=True)
                    return

                game_config.custom_options = json.dumps(options)
                await ctx.respond(f"Options updated for **{game_config.game_name}**!", ephemeral=True)

        except json.JSONDecodeError:
            await ctx.respond("Invalid JSON format!", ephemeral=True)
        except Exception as e:
            logger.error(f"Options error: {e}")
            await ctx.respond("Error updating options!", ephemeral=True)

    @discord.slash_command(name="lfg_rank", description="Enable rank/level requirement for a game (Admin)")
    @discord.default_permissions(administrator=True)
    @discord.option("game", description="Game short code")
    @discord.option("enabled", description="Enable rank requirement?")
    @discord.option("label", description="What to call it (e.g., 'Hunter Rank')", required=False)
    @discord.option("min_val", description="Minimum value", required=False, default=1)
    @discord.option("max_val", description="Maximum value", required=False, default=999)
    async def lfg_rank(
        self,
        ctx: discord.ApplicationContext,
        game: str,
        enabled: bool,
        label: str = "Rank",
        min_val: int = 1,
        max_val: int = 999
    ):
        """Configure rank/level requirement for a game."""
        try:
            with get_db_session() as session:
                game_config = session.query(LFGGame).filter(
                    LFGGame.guild_id == ctx.guild.id,
                    LFGGame.game_short.ilike(game)
                ).first()

                if not game_config:
                    await ctx.respond("Game not found!", ephemeral=True)
                    return

                game_config.require_rank = enabled
                game_config.rank_label = label
                game_config.rank_min = min_val
                game_config.rank_max = max_val

                status = "enabled" if enabled else "disabled"
                await ctx.respond(
                    f"Rank requirement {status} for **{game_config.game_name}**.\n"
                    f"Label: {label}, Range: {min_val}-{max_val}",
                    ephemeral=True
                )

        except Exception as e:
            logger.error(f"Rank config error: {e}")
            await ctx.respond("Error updating rank config!", ephemeral=True)

    @discord.slash_command(name="lfg_list", description="List all configured LFG games")
    async def lfg_list(self, ctx: discord.ApplicationContext):
        """Show all games configured for LFG in this server."""
        try:
            with get_db_session() as session:
                games = session.query(LFGGame).filter(
                    LFGGame.guild_id == ctx.guild.id
                ).all()

                if not games:
                    await ctx.respond(
                        "No games configured for LFG yet!\n"
                        "Admins can use `/lfg_search` to add games.",
                        ephemeral=True
                    )
                    return

                embed = discord.Embed(
                    title="LFG Games",
                    description=f"{len(games)} games available",
                    color=discord.Color.blurple()
                )

                for game in games:
                    status = "Enabled" if game.enabled else "Disabled"
                    source = "Custom" if game.is_custom_game else "IGDB"
                    channel = f"<#{game.lfg_channel_id}>" if game.lfg_channel_id else "Not set"

                    info = f"Code: `{game.game_short}` | {status} | {source}\nChannel: {channel}"
                    if game.custom_options:
                        info += " | Has custom options"

                    embed.add_field(
                        name=f"{game.game_emoji or ''} {game.game_name}",
                        value=info,
                        inline=False
                    )

                await ctx.respond(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"List error: {e}")
            await ctx.respond("Error listing games!", ephemeral=True)

    @discord.slash_command(name="lfg_remove", description="Remove a game from LFG (Admin)")
    @discord.default_permissions(administrator=True)
    @discord.option("game", description="Game short code to remove")
    async def lfg_remove(self, ctx: discord.ApplicationContext, game: str):
        """Remove a game from LFG."""
        try:
            with get_db_session() as session:
                game_config = session.query(LFGGame).filter(
                    LFGGame.guild_id == ctx.guild.id,
                    LFGGame.game_short.ilike(game)
                ).first()

                if not game_config:
                    await ctx.respond("Game not found!", ephemeral=True)
                    return

                game_name = game_config.game_name
                session.delete(game_config)
                await ctx.respond(f"Removed **{game_name}** from LFG.", ephemeral=True)

        except Exception as e:
            logger.error(f"Remove error: {e}")
            await ctx.respond("Error removing game!", ephemeral=True)

    # =============================================================================
    # PREMIUM: ATTENDANCE TRACKING COMMANDS
    # =============================================================================

    def _update_member_stats(self, session, guild_id: int, user_id: int, status: AttendanceStatus):
        """Update member stats after attendance is marked."""
        stats = session.query(LFGMemberStats).filter_by(
            guild_id=guild_id, user_id=user_id
        ).first()

        if not stats:
            stats = LFGMemberStats(
                guild_id=guild_id,
                user_id=user_id,
                total_signups=0,
                total_showed=0,
                total_no_shows=0,
                total_cancelled=0,
                total_late=0,
                reliability_score=100,
                current_show_streak=0,
                best_show_streak=0,
                current_noshow_streak=0,
                is_blacklisted=False
            )
            session.add(stats)

        now = int(time.time())
        # Handle NULL values from existing records
        stats.total_signups = (stats.total_signups or 0) + 1
        stats.last_event = now
        if not stats.first_event:
            stats.first_event = now

        if status == AttendanceStatus.SHOWED:
            stats.total_showed = (stats.total_showed or 0) + 1
            stats.current_show_streak = (stats.current_show_streak or 0) + 1
            stats.current_noshow_streak = 0
            if (stats.current_show_streak or 0) > (stats.best_show_streak or 0):
                stats.best_show_streak = stats.current_show_streak
        elif status == AttendanceStatus.NO_SHOW:
            stats.total_no_shows = (stats.total_no_shows or 0) + 1
            stats.current_noshow_streak = (stats.current_noshow_streak or 0) + 1
            stats.current_show_streak = 0
        elif status == AttendanceStatus.LATE:
            stats.total_late = (stats.total_late or 0) + 1
            stats.current_show_streak = (stats.current_show_streak or 0) + 1  # Late still counts as showing
            stats.current_noshow_streak = 0
        elif status == AttendanceStatus.CANCELLED:
            stats.total_cancelled = (stats.total_cancelled or 0) + 1
            # Cancelling doesn't affect streaks

        # Recalculate reliability score (0-100)
        total = (stats.total_showed or 0) + (stats.total_no_shows or 0) + (stats.total_late or 0)
        if total > 0:
            # Shows count full, late counts 80%, no-shows count 0%
            score = (((stats.total_showed or 0) * 100) + ((stats.total_late or 0) * 80)) / total
            stats.reliability_score = int(score)

        stats.updated_at = now

        # Check auto-blacklist threshold
        config = session.query(LFGConfig).filter_by(guild_id=guild_id).first()
        if config and config.auto_blacklist_noshows > 0:
            if (stats.total_no_shows or 0) >= config.auto_blacklist_noshows and not stats.is_blacklisted:
                stats.is_blacklisted = True
                stats.blacklisted_at = now
                stats.blacklist_reason = f"Auto-blacklisted: {stats.total_no_shows or 0} no-shows"

        return stats

    @discord.slash_command(name="lfg_mark", description="Mark attendance for LFG group members (Premium)")
    @discord.default_permissions(administrator=True)
    @discord.option("user", description="Member to mark")
    @discord.option("status", description="Attendance status", choices=[
        discord.OptionChoice(name="Showed Up", value="showed"),
        discord.OptionChoice(name="No Show", value="no_show"),
        discord.OptionChoice(name="Late", value="late"),
        discord.OptionChoice(name="Cancelled", value="cancelled"),
    ])
    @discord.option("group_id", description="Group ID (from thread)", required=False)
    async def lfg_mark(
        self,
        ctx: discord.ApplicationContext,
        user: discord.Member,
        status: str,
        group_id: int = None
    ):
        """Mark a member's attendance for an LFG group."""
        try:
            with get_db_session() as session:
                # Check LFG access (Complete tier or LFG module)
                if not self._has_lfg_access(session, ctx.guild.id):
                    await ctx.respond(
                        "Attendance tracking requires **Complete tier** or the **LFG Module**!\n"
                        "Upgrade to track reliability and identify flaky members.",
                        ephemeral=True
                    )
                    return

                # Find the group
                if group_id:
                    group = session.query(LFGGroup).filter_by(
                        id=group_id, guild_id=ctx.guild.id
                    ).first()
                elif isinstance(ctx.channel, discord.Thread):
                    group = session.query(LFGGroup).filter_by(
                        thread_id=ctx.channel.id
                    ).first()
                else:
                    await ctx.respond(
                        "Use this in an LFG thread or provide a group_id.",
                        ephemeral=True
                    )
                    return

                if not group:
                    await ctx.respond("LFG group not found!", ephemeral=True)
                    return

                # Map status string to enum
                status_map = {
                    "showed": AttendanceStatus.SHOWED,
                    "no_show": AttendanceStatus.NO_SHOW,
                    "late": AttendanceStatus.LATE,
                    "cancelled": AttendanceStatus.CANCELLED,
                }
                att_status = status_map.get(status, AttendanceStatus.PENDING)

                # Create or update attendance record
                attendance = session.query(LFGAttendance).filter_by(
                    group_id=group.id, user_id=user.id
                ).first()

                now = int(time.time())
                if not attendance:
                    attendance = LFGAttendance(
                        group_id=group.id,
                        user_id=user.id,
                        status=att_status,
                        marked_by=ctx.author.id
                    )
                    session.add(attendance)
                else:
                    attendance.status = att_status
                    attendance.marked_by = ctx.author.id

                # Update timestamps based on status
                if att_status == AttendanceStatus.SHOWED:
                    attendance.showed_at = now
                elif att_status == AttendanceStatus.NO_SHOW:
                    attendance.no_show_at = now
                elif att_status == AttendanceStatus.CANCELLED:
                    attendance.cancelled_at = now
                elif att_status == AttendanceStatus.LATE:
                    attendance.late_at = now
                    attendance.showed_at = now  # Late still counts as showing

                # Update member stats
                stats = self._update_member_stats(session, ctx.guild.id, user.id, att_status)

                # Build response
                status_emoji = {
                    AttendanceStatus.SHOWED: "✅",
                    AttendanceStatus.NO_SHOW: "❌",
                    AttendanceStatus.LATE: "⏰",
                    AttendanceStatus.CANCELLED: "🚫",
                }

                embed = discord.Embed(
                    title=f"{status_emoji.get(att_status, '')} Attendance Marked",
                    color=discord.Color.green() if att_status == AttendanceStatus.SHOWED else discord.Color.red()
                )
                embed.add_field(name="Member", value=user.mention, inline=True)
                embed.add_field(name="Status", value=att_status.value.replace("_", " ").title(), inline=True)
                embed.add_field(
                    name="Reliability Score",
                    value=f"{stats.reliability_score}%",
                    inline=True
                )
                embed.add_field(
                    name="Stats",
                    value=f"Shows: {stats.total_showed or 0} | No-shows: {stats.total_no_shows or 0} | Late: {stats.total_late or 0} | Cancelled: {stats.total_cancelled or 0}",
                    inline=False
                )

                if stats.is_blacklisted:
                    embed.add_field(
                        name="⚠️ Blacklisted",
                        value=stats.blacklist_reason or "No reason provided",
                        inline=False
                    )

                await ctx.respond(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Mark attendance error: {e}")
            await ctx.respond("Error marking attendance!", ephemeral=True)

    @discord.slash_command(name="lfg_stats", description="View LFG reliability stats for a member (Premium)")
    @discord.option("user", description="Member to check (leave blank for yourself)", required=False)
    async def lfg_stats(self, ctx: discord.ApplicationContext, user: discord.Member = None):
        """View LFG reliability statistics for a member."""
        target = user or ctx.author

        try:
            with get_db_session() as session:
                # Check LFG access (Complete tier or LFG module)
                if not self._has_lfg_access(session, ctx.guild.id):
                    await ctx.respond(
                        "Reliability stats require **Complete tier** or the **LFG Module**!\n"
                        "Upgrade to track attendance and reliability.",
                        ephemeral=True
                    )
                    return

                stats = session.query(LFGMemberStats).filter_by(
                    guild_id=ctx.guild.id, user_id=target.id
                ).first()

                if not stats:
                    await ctx.respond(
                        f"{target.display_name} has no LFG history yet.",
                        ephemeral=True
                    )
                    return

                # Reliability color
                if stats.reliability_score >= 80:
                    color = discord.Color.green()
                    tier = "⭐ Reliable"
                elif stats.reliability_score >= 50:
                    color = discord.Color.yellow()
                    tier = "⚠️ Average"
                else:
                    color = discord.Color.red()
                    tier = "🚨 Unreliable"

                embed = discord.Embed(
                    title=f"LFG Stats: {target.display_name}",
                    color=color
                )
                embed.set_thumbnail(url=target.display_avatar.url)

                embed.add_field(
                    name="Reliability Score",
                    value=f"**{stats.reliability_score}%** {tier}",
                    inline=False
                )

                embed.add_field(name="✅ Shows", value=str(stats.total_showed or 0), inline=True)
                embed.add_field(name="❌ No-Shows", value=str(stats.total_no_shows or 0), inline=True)
                embed.add_field(name="⏰ Late", value=str(stats.total_late or 0), inline=True)
                embed.add_field(name="🚫 Cancelled", value=str(stats.total_cancelled or 0), inline=True)
                embed.add_field(name="📊 Total Events", value=str(stats.total_signups or 0), inline=True)

                embed.add_field(
                    name="Streaks",
                    value=f"Current: {stats.current_show_streak or 0} shows | Best: {stats.best_show_streak or 0}",
                    inline=False
                )

                if stats.is_blacklisted:
                    embed.add_field(
                        name="🚫 BLACKLISTED",
                        value=stats.blacklist_reason or "No reason provided",
                        inline=False
                    )
                    embed.color = discord.Color.dark_red()

                if stats.first_event:
                    embed.set_footer(text=f"First event: ")
                    embed.timestamp = datetime.fromtimestamp(stats.first_event)

                await ctx.respond(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Stats error: {e}")
            await ctx.respond("Error fetching stats!", ephemeral=True)

    @discord.slash_command(name="lfg_leaderboard", description="View LFG reliability leaderboard (Premium)")
    @discord.option("show", description="What to show", choices=[
        discord.OptionChoice(name="Most Reliable", value="reliable"),
        discord.OptionChoice(name="Most Active", value="active"),
        discord.OptionChoice(name="Flakiest", value="flaky"),
    ])
    async def lfg_leaderboard(self, ctx: discord.ApplicationContext, show: str = "reliable"):
        """View the LFG reliability leaderboard."""
        try:
            with get_db_session() as session:
                if not self._has_lfg_access(session, ctx.guild.id):
                    await ctx.respond(
                        "Leaderboards require **Complete tier** or the **LFG Module**!",
                        ephemeral=True
                    )
                    return

                query = session.query(LFGMemberStats).filter(
                    LFGMemberStats.guild_id == ctx.guild.id,
                    LFGMemberStats.total_signups >= 3  # Min 3 events
                )

                if show == "reliable":
                    query = query.order_by(LFGMemberStats.reliability_score.desc())
                    title = "🏆 Most Reliable LFG Members"
                elif show == "active":
                    query = query.order_by(LFGMemberStats.total_signups.desc())
                    title = "📊 Most Active LFG Members"
                else:  # flaky
                    query = query.order_by(LFGMemberStats.total_no_shows.desc())
                    title = "💨 Flakiest LFG Members"

                members = query.limit(10).all()

                if not members:
                    await ctx.respond("No LFG stats yet!", ephemeral=True)
                    return

                embed = discord.Embed(title=title, color=discord.Color.gold())

                lines = []
                for i, m in enumerate(members, 1):
                    medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                    if show == "reliable":
                        lines.append(f"{medal} <@{m.user_id}> - **{m.reliability_score}%** ({m.total_showed}/{m.total_signups})")
                    elif show == "active":
                        lines.append(f"{medal} <@{m.user_id}> - **{m.total_signups}** events ({m.reliability_score}%)")
                    else:
                        lines.append(f"{medal} <@{m.user_id}> - **{m.total_no_shows}** no-shows ({m.reliability_score}%)")

                embed.description = "\n".join(lines)
                embed.set_footer(text="Min 3 events to appear")

                await ctx.respond(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Leaderboard error: {e}")
            await ctx.respond("Error fetching leaderboard!", ephemeral=True)

    @discord.slash_command(name="lfg_blacklist", description="Blacklist/unblacklist a member from LFG (Premium)")
    @discord.default_permissions(administrator=True)
    @discord.option("user", description="Member to blacklist/unblacklist")
    @discord.option("action", description="Action", choices=[
        discord.OptionChoice(name="Blacklist", value="add"),
        discord.OptionChoice(name="Remove from Blacklist", value="remove"),
    ])
    @discord.option("reason", description="Reason for blacklist", required=False)
    async def lfg_blacklist(
        self,
        ctx: discord.ApplicationContext,
        user: discord.Member,
        action: str,
        reason: str = None
    ):
        """Blacklist or unblacklist a member from LFG."""
        try:
            with get_db_session() as session:
                if not self._has_lfg_access(session, ctx.guild.id):
                    await ctx.respond(
                        "Blacklist management requires **Complete tier** or the **LFG Module**!",
                        ephemeral=True
                    )
                    return

                stats = session.query(LFGMemberStats).filter_by(
                    guild_id=ctx.guild.id, user_id=user.id
                ).first()

                if not stats:
                    stats = LFGMemberStats(guild_id=ctx.guild.id, user_id=user.id)
                    session.add(stats)

                now = int(time.time())

                if action == "add":
                    stats.is_blacklisted = True
                    stats.blacklisted_at = now
                    stats.blacklisted_by = ctx.author.id
                    stats.blacklist_reason = reason or "No reason provided"

                    await ctx.respond(
                        f"🚫 **{user.display_name}** has been blacklisted from LFG.\n"
                        f"Reason: {stats.blacklist_reason}",
                        ephemeral=True
                    )
                else:
                    stats.is_blacklisted = False
                    stats.blacklisted_at = None
                    stats.blacklisted_by = None
                    stats.blacklist_reason = None

                    await ctx.respond(
                        f"✅ **{user.display_name}** has been removed from the blacklist.",
                        ephemeral=True
                    )

        except Exception as e:
            logger.error(f"Blacklist error: {e}")
            await ctx.respond("Error updating blacklist!", ephemeral=True)

    @discord.slash_command(name="lfg_config", description="Configure LFG attendance settings (Premium)")
    @discord.default_permissions(administrator=True)
    @discord.option("attendance_tracking", description="Enable attendance tracking?", required=False)
    @discord.option("auto_noshow_hours", description="Hours after event to auto-mark no-show (0=disabled)", required=False)
    @discord.option("min_reliability", description="Min reliability score to join groups (0=disabled)", required=False)
    @discord.option("auto_blacklist", description="Auto-blacklist after X no-shows (0=disabled)", required=False)
    async def lfg_config(
        self,
        ctx: discord.ApplicationContext,
        attendance_tracking: bool = None,
        auto_noshow_hours: int = None,
        min_reliability: int = None,
        auto_blacklist: int = None
    ):
        """Configure LFG attendance settings."""
        try:
            with get_db_session() as session:
                if not self._has_lfg_access(session, ctx.guild.id):
                    await ctx.respond(
                        "LFG configuration requires **Complete tier** or the **LFG Module**!\n"
                        "Upgrade to customize attendance tracking.",
                        ephemeral=True
                    )
                    return

                config = session.query(LFGConfig).filter_by(guild_id=ctx.guild.id).first()
                if not config:
                    config = LFGConfig(guild_id=ctx.guild.id)
                    session.add(config)

                # Update provided values
                if attendance_tracking is not None:
                    config.attendance_tracking_enabled = attendance_tracking
                if auto_noshow_hours is not None:
                    config.auto_noshow_hours = max(0, auto_noshow_hours)
                if min_reliability is not None:
                    config.min_reliability_score = max(0, min(100, min_reliability))
                if auto_blacklist is not None:
                    config.auto_blacklist_noshows = max(0, auto_blacklist)

                config.updated_at = int(time.time())

                # Show current config
                embed = discord.Embed(
                    title="⚙️ LFG Configuration",
                    color=discord.Color.blurple()
                )
                embed.add_field(
                    name="Attendance Tracking",
                    value="✅ Enabled" if config.attendance_tracking_enabled else "❌ Disabled",
                    inline=True
                )
                embed.add_field(
                    name="Auto No-Show Timer",
                    value=f"{config.auto_noshow_hours}h" if config.auto_noshow_hours > 0 else "Disabled",
                    inline=True
                )
                embed.add_field(
                    name="Min Reliability Score",
                    value=f"{config.min_reliability_score}%" if config.min_reliability_score > 0 else "Disabled",
                    inline=True
                )
                embed.add_field(
                    name="Auto-Blacklist Threshold",
                    value=f"{config.auto_blacklist_noshows} no-shows" if config.auto_blacklist_noshows > 0 else "Disabled",
                    inline=True
                )

                await ctx.respond(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Config error: {e}")
            await ctx.respond("Error updating config!", ephemeral=True)

    @discord.slash_command(name="lfg_setup", description="Post persistent LFG menu (Admin)")
    @discord.default_permissions(administrator=True)
    async def lfg_setup(self, ctx: discord.ApplicationContext):
        """Post a persistent LFG menu with buttons for all configured games."""
        await ctx.defer(ephemeral=True)

        try:
            with get_db_session() as session:
                # Get all enabled games for this guild
                games = session.query(LFGGame).filter(
                    LFGGame.guild_id == ctx.guild.id,
                    LFGGame.enabled == True
                ).order_by(LFGGame.game_name).all()

                if not games:
                    await ctx.respond(
                        "No games configured! Use `/lfg_add` or `/lfg_custom` to add games first.",
                        ephemeral=True
                    )
                    return

                # Get player counts from Discord Activity
                player_counts = await self._get_player_counts(ctx.guild, games)

                # Create embed
                embed = discord.Embed(
                    title="🎮 Looking For Group",
                    description=(
                        "Click a game button below to create an LFG group!\n\n"
                        "**FREE Tier**: Create unlimited LFG threads for up to 5 games\n"
                        "**PRO/Premium**: Access web dashboard for advanced features"
                    ),
                    color=discord.Color.purple()
                )

                # Add games list to embed
                games_list = []
                for i, game in enumerate(games[:5], 1):  # Show first 5 for FREE tier
                    player_count = player_counts.get(game.id, 0)
                    emoji = game.game_emoji or "🎮"
                    games_list.append(f"{emoji} **{game.game_name}** - {player_count} playing now")

                embed.add_field(
                    name="Available Games",
                    value="\n".join(games_list) if games_list else "No games configured",
                    inline=False
                )

                embed.set_footer(text="This menu stays active until deleted by an admin")

                # Create persistent view
                view = PersistentLFGView(games, player_counts)

                # Post to channel (not ephemeral)
                await ctx.channel.send(embed=embed, view=view)

                # Confirm to admin
                await ctx.respond(
                    f"✅ LFG menu posted in {ctx.channel.mention}!\n"
                    f"Members can now create LFG groups for {len(games[:5])} games.",
                    ephemeral=True
                )

        except Exception as e:
            logger.error(f"Error posting LFG setup: {e}")
            await ctx.respond("Error posting LFG menu!", ephemeral=True)

    async def _get_player_counts(self, guild: discord.Guild, games: List[LFGGame]) -> Dict[int, int]:
        """Get the number of members currently playing each game via Discord Activity."""
        player_counts = {}

        try:
            for game in games:
                count = 0

                # Check members' activities
                for member in guild.members:
                    if member.bot:
                        continue

                    for activity in member.activities:
                        # Check if activity name matches game name (case insensitive)
                        if hasattr(activity, 'name') and activity.name:
                            if game.game_name.lower() in activity.name.lower():
                                count += 1
                                break

                player_counts[game.id] = count

        except Exception as e:
            logger.error(f"Error getting player counts: {e}")

        return player_counts


# =============================================================================
# ATTENDANCE CONFIRMATION VIEW
# =============================================================================

class AttendanceConfirmView(discord.ui.View):
    """View with button to confirm attendance for an LFG group."""
    def __init__(self, group_id: int, guild_id: int):
        super().__init__(timeout=None)  # Persistent view
        self.group_id = group_id
        self.guild_id = guild_id

    @discord.ui.button(
        label="✅ Confirm Attendance",
        style=discord.ButtonStyle.success,
        custom_id="lfg_confirm_attendance"
    )
    async def confirm_button(self, button: discord.ui.Button, interaction: discord.Interaction):
        """Handle attendance confirmation."""
        await interaction.response.defer(ephemeral=True)

        try:
            with get_db_session() as session:
                # Check if attendance tracking is enabled
                config = session.query(LFGConfig).filter_by(guild_id=self.guild_id).first()
                if not config or not config.attendance_tracking_enabled:
                    await interaction.followup.send(
                        "Attendance tracking is not enabled for this server.",
                        ephemeral=True
                    )
                    return

                # Check if user is in the group
                member = session.query(LFGMember).filter_by(
                    group_id=self.group_id,
                    user_id=interaction.user.id
                ).first()

                if not member:
                    await interaction.followup.send(
                        "You're not in this group!",
                        ephemeral=True
                    )
                    return

                # Create or update attendance record
                attendance = session.query(LFGAttendance).filter_by(
                    group_id=self.group_id,
                    user_id=interaction.user.id
                ).first()

                now = int(time.time())
                if not attendance:
                    attendance = LFGAttendance(
                        group_id=self.group_id,
                        user_id=interaction.user.id,
                        status=AttendanceStatus.CONFIRMED,
                        confirmed_at=now,
                        joined_at=now
                    )
                    session.add(attendance)
                else:
                    # Already confirmed
                    if attendance.status == AttendanceStatus.CONFIRMED:
                        await interaction.followup.send(
                            "You've already confirmed your attendance!",
                            ephemeral=True
                        )
                        return

                    attendance.status = AttendanceStatus.CONFIRMED
                    attendance.confirmed_at = now

                await interaction.followup.send(
                    "✅ Attendance confirmed! See you there!",
                    ephemeral=True
                )

                # Update the message to show confirmed members
                await self._update_attendance_message(interaction.message, session)

        except Exception as e:
            logger.error(f"Attendance confirm error: {e}")
            await interaction.followup.send(
                "Error confirming attendance!",
                ephemeral=True
            )

    async def _update_attendance_message(self, message: discord.Message, session):
        """Update the attendance message with current confirmations."""
        try:
            # Get all confirmed members for this group
            confirmed = session.query(LFGAttendance).filter_by(
                group_id=self.group_id,
                status=AttendanceStatus.CONFIRMED
            ).all()

            confirmed_ids = [a.user_id for a in confirmed]

            # Get group info
            group = session.query(LFGGroup).filter_by(id=self.group_id).first()
            if not group:
                return

            # Get all members
            members = session.query(LFGMember).filter_by(group_id=self.group_id).all()
            total_members = len(members)
            confirmed_count = len(confirmed_ids)

            # Build embed
            embed = discord.Embed(
                title="📋 Attendance Confirmation",
                description="Click the button below to confirm you're coming!",
                color=discord.Color.green()
            )

            if confirmed_ids:
                confirmed_text = "\n".join([f"✅ <@{uid}>" for uid in confirmed_ids[:10]])
                if len(confirmed_ids) > 10:
                    confirmed_text += f"\n... and {len(confirmed_ids) - 10} more"
                embed.add_field(
                    name=f"Confirmed ({confirmed_count}/{total_members})",
                    value=confirmed_text,
                    inline=False
                )
            else:
                embed.add_field(
                    name=f"Confirmed (0/{total_members})",
                    value="No one has confirmed yet.",
                    inline=False
                )

            embed.set_footer(text="Confirming helps track attendance and reliability!")

            await message.edit(embed=embed, view=self)

        except Exception as e:
            logger.error(f"Error updating attendance message: {e}")


# =============================================================================
# PERSISTENT LFG EMBED (FREE TIER FEATURE)
# =============================================================================

class GameButton(discord.ui.Button):
    """Button to create LFG for a specific game."""
    def __init__(self, game: LFGGame, player_count: int = 0):
        # Use game emoji if available, otherwise use game controller emoji
        emoji = game.game_emoji if game.game_emoji else "🎮"
        label = f"{game.game_name} ({player_count} playing)"

        super().__init__(
            style=discord.ButtonStyle.primary,
            label=label,
            emoji=emoji,
            custom_id=f"lfg_game_{game.id}"
        )
        self.game = game

    async def callback(self, interaction: discord.Interaction):
        """Open LFG creation view for this game."""
        try:
            with get_db_session() as session:
                # Check guild subscription - requires Complete tier OR LFG module
                guild_record = session.query(Guild).filter_by(guild_id=interaction.guild.id).first()
                if guild_record and not guild_record.is_vip:
                    # Check for LFG module subscription
                    has_lfg_module = session.query(GuildModule).filter_by(
                        guild_id=interaction.guild.id,
                        module_name='lfg',
                        enabled=True
                    ).first() is not None

                    has_access = guild_record.subscription_tier == 'complete' or has_lfg_module

                    if not has_access:
                        await interaction.response.send_message(
                            "❌ **LFG Groups require Complete tier or LFG Module**\n\n"
                            "The LFG Browser feature requires a subscription.\n"
                            "Upgrade your server to create and manage LFG groups!\n\n"
                            "Visit the dashboard to upgrade: https://dashboard.casual-heroes.com",
                            ephemeral=True
                        )
                        return

                # Reload game config from DB
                game_config = session.query(LFGGame).filter_by(id=self.game.id).first()
                if not game_config or not game_config.enabled:
                    await interaction.response.send_message(
                        "This game is no longer available for LFG.",
                        ephemeral=True
                    )
                    return

                # Parse custom options
                custom_options = []
                if game_config.custom_options:
                    try:
                        custom_options = json.loads(game_config.custom_options)
                    except:
                        pass

                # Create the LFG setup embed
                embed = discord.Embed(
                    title=f"{game_config.game_emoji or '🎮'} {game_config.game_name} LFG",
                    description=(
                        "**Set up your group:**\n"
                        "1. Select a day and your options\n"
                        "2. Click 'Set Time' to enter your play time\n"
                        "3. Click 'Create Group' when ready!"
                    ),
                    color=discord.Color.blurple()
                )

                view = CreationView(game_config, custom_options)
                await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

        except Exception as e:
            logger.error(f"Error opening LFG creation: {e}")
            await interaction.response.send_message(
                "Something went wrong! Please try again.",
                ephemeral=True
            )


class PersistentLFGView(discord.ui.View):
    """Persistent view with buttons for each configured game."""
    def __init__(self, games: List[LFGGame], player_counts: Dict[int, int]):
        super().__init__(timeout=None)  # Persistent view - no timeout

        # Add a button for each game, one per row (max 5 rows)
        for i, game in enumerate(games[:5]):  # Limit to 5 games for FREE tier
            player_count = player_counts.get(game.id, 0)
            button = GameButton(game, player_count)
            button.row = i  # Stack buttons vertically, one per row
            self.add_item(button)


def setup(bot: commands.Bot):
    bot.add_cog(LFGCog(bot))
