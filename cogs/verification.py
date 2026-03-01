# cogs/verification.py - Member Verification System
"""
Full verification system for QuestLog.

VERIFICATION TYPES:
- NONE: No verification required
- BUTTON: Click button to agree to rules (FREE)
- CAPTCHA: Solve random captcha (FREE)
- ACCOUNT_AGE: Auto-verify if account old enough (FREE)
- MULTI_STEP: Combination of above + intro message (PREMIUM)

FEATURES:
- Auto-quarantine new members until verified
- Account age checking
- Timeout kicks for unverified members
- Verification logging
"""

import json
import time
import secrets  # SECURITY FIX: Use secrets instead of random for CAPTCHA
import string
import asyncio
import io
import discord
from discord.ext import commands, tasks
from discord import SlashCommandGroup
from discord.ui import View, Button, Modal, InputText
from captcha.image import ImageCaptcha

from config import (
    db_session_scope, logger, get_debug_guilds,
    DefaultVerificationSettings, FeatureLimits
)
from models import (
    Guild, GuildModule, GuildMember, VerificationConfig, VerificationType,
    AuditLog, AuditAction
)


def get_guild_tier(session, guild_id: int) -> str:
    """Get the subscription tier for a guild."""
    db_guild = session.get(Guild, guild_id)
    if not db_guild:
        return "FREE"
    if db_guild.is_vip:
        return "PRO"
    return db_guild.subscription_tier.upper() if db_guild.subscription_tier else "FREE"


def has_moderation_access(session, guild_id: int) -> bool:
    """Check if guild has moderation access (Complete tier, VIP, or Moderation module)."""
    db_guild = session.get(Guild, guild_id)
    if not db_guild:
        return False
    # All guilds have full access
    if True:
        return True
    # Check for Moderation module subscription
    has_mod_module = session.query(GuildModule).filter_by(
        guild_id=guild_id,
        module_name='moderation',
        enabled=True
    ).first() is not None
    return has_mod_module


def generate_captcha(length: int = 6) -> str:
    """Generate a cryptographically secure random captcha code."""
    chars = string.ascii_uppercase + string.digits
    # Remove confusing characters
    chars = chars.replace("O", "").replace("0", "").replace("I", "").replace("1", "")
    # SECURITY FIX: Use secrets.choice() for cryptographic randomness
    return "".join(secrets.choice(chars) for _ in range(length))


def generate_captcha_image(code: str) -> io.BytesIO:
    """
    Generate a distorted captcha image that's hard for bots to OCR.

    Args:
        code: The captcha code to render

    Returns:
        BytesIO buffer containing the PNG image
    """
    # Create image captcha generator with custom settings
    # width=280, height=90 is good for Discord embeds
    image_captcha = ImageCaptcha(width=280, height=90)

    # Generate the image
    image = image_captcha.generate_image(code)

    # Save to BytesIO buffer
    buffer = io.BytesIO()
    image.save(buffer, format='PNG')
    buffer.seek(0)

    return buffer


# Server-side captcha storage with expiry
# Format: {(guild_id, user_id): {'code': str, 'expires': float}}
_pending_captchas: dict = {}


def check_account_age(user: discord.User, min_days: int) -> tuple[bool, int]:
    """
    Check if account is old enough.
    Returns (is_old_enough, account_age_days)
    """
    account_age = discord.utils.utcnow() - user.created_at
    age_days = account_age.days
    return (age_days >= min_days, age_days)


# ==================== UI COMPONENTS ====================

class VerifyButtonView(View):
    """Button for simple verification."""

    def __init__(self, guild_id: int, button_text: str = "I agree to the rules"):
        super().__init__(timeout=None)
        self.guild_id = guild_id

        # Create the button with custom ID for persistence
        button = Button(
            label=button_text,
            style=discord.ButtonStyle.success,
            custom_id=f"verify_button_{guild_id}",
            emoji="✅"
        )
        button.callback = self.verify_callback
        self.add_item(button)

    async def verify_callback(self, interaction: discord.Interaction):
        """Handle verification button click."""
        await process_verification(interaction, "button")


class CaptchaModal(Modal):
    """Modal for captcha input - code is stored server-side, NOT in modal."""

    def __init__(self, guild_id: int, captcha_length: int = 6):
        super().__init__(title="Verification Captcha")
        self.guild_id = guild_id

        self.captcha_input = InputText(
            label="Enter the code shown in the image above",
            placeholder="Type the code exactly as shown",
            min_length=captcha_length,
            max_length=captcha_length,
        )
        self.add_item(self.captcha_input)

    async def callback(self, interaction: discord.Interaction):
        """Handle captcha submission - verify against server-side stored code."""
        user_input = self.captcha_input.value.upper().strip()

        # Look up the stored captcha code
        key = (self.guild_id, interaction.user.id)
        stored = _pending_captchas.get(key)

        if not stored:
            await interaction.response.send_message(
                "Your captcha has expired. Please click the button again to get a new one.",
                ephemeral=True
            )
            return

        # Check if expired (5 minute timeout)
        if time.time() > stored['expires']:
            # Clean up expired entry
            _pending_captchas.pop(key, None)
            await interaction.response.send_message(
                "Your captcha has expired. Please click the button again to get a new one.",
                ephemeral=True
            )
            return

        if user_input == stored['code']:
            # Clean up used captcha
            _pending_captchas.pop(key, None)
            await process_verification(interaction, "captcha")
        else:
            # Don't remove on failure - let them retry with same image
            await interaction.response.send_message(
                "Incorrect code. Check the image and try again, or click the button for a new captcha.",
                ephemeral=True
            )


class CaptchaButtonView(View):
    """Button to start captcha verification with image-based challenge."""

    def __init__(self, guild_id: int, captcha_length: int = 6):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.captcha_length = captcha_length

        button = Button(
            label="Start Verification",
            style=discord.ButtonStyle.primary,
            custom_id=f"captcha_button_{guild_id}",
            emoji="🔐"
        )
        button.callback = self.captcha_callback
        self.add_item(button)

        # Add a secondary button to submit the answer
        submit_button = Button(
            label="Submit Answer",
            style=discord.ButtonStyle.success,
            custom_id=f"captcha_submit_{guild_id}",
            emoji="✅"
        )
        submit_button.callback = self.submit_callback
        self.add_item(submit_button)

    async def captcha_callback(self, interaction: discord.Interaction):
        """Generate and send captcha image, store code server-side."""
        # Generate captcha code and image
        captcha_code = generate_captcha(self.captcha_length)
        image_buffer = generate_captcha_image(captcha_code)

        # Store code server-side with 5-minute expiry
        key = (self.guild_id, interaction.user.id)
        _pending_captchas[key] = {
            'code': captcha_code,
            'expires': time.time() + 300  # 5 minutes
        }

        # Clean up old expired entries periodically (every 10th request)
        if len(_pending_captchas) % 10 == 0:
            current_time = time.time()
            expired_keys = [k for k, v in _pending_captchas.items() if current_time > v['expires']]
            for k in expired_keys:
                _pending_captchas.pop(k, None)

        # Create embed with instructions
        embed = discord.Embed(
            title="🔐 Captcha Verification",
            description=(
                "**Enter the code shown in the image below.**\n\n"
                "• The code is case-insensitive\n"
                "• You have 5 minutes to complete this\n"
                "• Click **Submit Answer** when ready"
            ),
            color=discord.Color.blue()
        )
        embed.set_footer(text="If you can't read the code, click 'Start Verification' for a new one")

        # Send image as attachment
        file = discord.File(image_buffer, filename="captcha.png")
        embed.set_image(url="attachment://captcha.png")

        await interaction.response.send_message(
            embed=embed,
            file=file,
            ephemeral=True
        )

    async def submit_callback(self, interaction: discord.Interaction):
        """Show modal to submit captcha answer."""
        # Check if user has a pending captcha
        key = (self.guild_id, interaction.user.id)
        stored = _pending_captchas.get(key)

        if not stored:
            await interaction.response.send_message(
                "You don't have an active captcha. Click **Start Verification** first to get one.",
                ephemeral=True
            )
            return

        if time.time() > stored['expires']:
            _pending_captchas.pop(key, None)
            await interaction.response.send_message(
                "Your captcha has expired. Click **Start Verification** for a new one.",
                ephemeral=True
            )
            return

        # Show modal (code length from stored captcha)
        modal = CaptchaModal(self.guild_id, len(stored['code']))
        await interaction.response.send_modal(modal)


class MultiStepView(View):
    """Multi-step verification view (Premium)."""

    def __init__(self, guild_id: int, require_rules: bool, require_intro: bool):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.require_rules = require_rules
        self.require_intro = require_intro

        # Step 1: Rules button
        rules_button = Button(
            label="I've read the rules",
            style=discord.ButtonStyle.secondary,
            custom_id=f"multistep_rules_{guild_id}",
            emoji="📜",
            row=0
        )
        rules_button.callback = self.rules_callback
        self.add_item(rules_button)

        # Step 2: Verify button
        verify_button = Button(
            label="Complete Verification",
            style=discord.ButtonStyle.success,
            custom_id=f"multistep_verify_{guild_id}",
            emoji="✅",
            row=1
        )
        verify_button.callback = self.verify_callback
        self.add_item(verify_button)

    async def rules_callback(self, interaction: discord.Interaction):
        """Mark rules as read."""
        with db_session_scope() as session:
            db_member = session.get(GuildMember, (interaction.guild.id, interaction.user.id))
            if db_member:
                _save_step(db_member, "rules")

        await interaction.response.send_message(
            "Rules acknowledged! Now click **Complete Verification** to finish.",
            ephemeral=True
        )

    async def verify_callback(self, interaction: discord.Interaction):
        """Complete multi-step verification."""
        with db_session_scope() as session:
            config = session.get(VerificationConfig, interaction.guild.id)
            db_member = session.get(GuildMember, (interaction.guild.id, interaction.user.id))

            # Check if rules were read (if required)
            if config and config.require_rules_read:
                if not db_member or "rules" not in _parse_steps(db_member):
                    await interaction.response.send_message(
                        "Please click **I've read the rules** first!",
                        ephemeral=True
                    )
                    return

            # Check if intro was posted (if required)
            if config and config.require_intro_message and config.intro_channel_id:
                intro_channel = interaction.guild.get_channel(config.intro_channel_id)
                if intro_channel:
                    # Check if user posted in intro channel
                    found_intro = False
                    async for message in intro_channel.history(limit=100):
                        if message.author.id == interaction.user.id:
                            found_intro = True
                            break

                    if not found_intro:
                        await interaction.response.send_message(
                            f"Please post an introduction in {intro_channel.mention} first!",
                            ephemeral=True
                        )
                        return

        await process_verification(interaction, "multi_step")


# ==================== HELPER FUNCTIONS ====================

def _parse_steps(db_member: GuildMember) -> set:
    """Parse multi-step progress from verification_method field."""
    if not db_member or not db_member.verification_method:
        return set()
    method = db_member.verification_method
    if method.startswith("ms:"):
        parts = method[3:].split(",")
        return {p.strip() for p in parts if p.strip()}
    return {method}


def _save_step(db_member: GuildMember, step: str):
    """Persist a completed step onto the member record."""
    steps = _parse_steps(db_member)
    steps.add(step)
    db_member.verification_method = "ms:" + ",".join(sorted(steps))


async def process_verification(interaction: discord.Interaction, method: str):
    """Process successful verification for a member."""
    guild = interaction.guild
    member = interaction.user

    with db_session_scope() as session:
        db_guild = session.get(Guild, guild.id)
        config = session.get(VerificationConfig, guild.id)

        if not db_guild:
            await interaction.response.send_message(
                "Guild not configured. Please contact an admin.",
                ephemeral=True
            )
            return

        # NOTE: We do NOT check account age here anymore.
        # Young accounts are quarantined and NEED to click the button to prove they're human.
        # The button click IS their verification - blocking them would make verification impossible.

        # Get or create member record
        db_member = session.get(GuildMember, (guild.id, member.id))
        if not db_member:
            logger.warning(
                f"[DUPLICATE TRACKER] verification.process_verification CREATING GuildMember: "
                f"guild_id={guild.id}, user_id={member.id}, user_id_type={type(member.id)}, "
                f"display_name={member.display_name}, source=Discord.Member"
            )
            db_member = GuildMember(
                guild_id=guild.id,
                user_id=member.id,
                display_name=member.display_name,
            )
            session.add(db_member)

        # Mark as verified
        db_member.is_verified = True
        db_member.verified_at = int(time.time())
        if method == "multi_step":
            _save_step(db_member, "verified")
        else:
            db_member.verification_method = method
        db_member.is_quarantined = False
        db_member.quarantined_at = None

        # Log verification
        audit = AuditLog(
            guild_id=guild.id,
            action=AuditAction.VERIFICATION_PASSED,
            actor_id=member.id,
            actor_name=str(member),
            target_id=member.id,
            target_name=str(member),
            details=f"Verification method: {method}"
        )
        session.add(audit)

        verified_role_id = db_guild.verified_role_id
        quarantine_role_id = db_guild.quarantine_role_id
        verified_message = config.verified_message if config else None

        # Get saved roles for persistence (returning members)
        saved_role_ids = []
        excluded_role_ids = set()

        if db_guild.role_persistence_enabled and db_member.saved_roles:
            try:
                saved_role_ids = json.loads(db_member.saved_roles)
                # Clear saved roles now that we're restoring them
                db_member.saved_roles = None
                db_member.left_at = None
            except (json.JSONDecodeError, TypeError):
                saved_role_ids = []

            # Build exclusion set
            if quarantine_role_id:
                excluded_role_ids.add(quarantine_role_id)

            # Add admin-configured excluded roles
            excluded_roles_json = getattr(db_guild, 'role_persistence_excluded', None)
            if excluded_roles_json:
                try:
                    excluded_role_ids.update(json.loads(excluded_roles_json))
                except (json.JSONDecodeError, TypeError):
                    pass

    # Assign verified role
    if verified_role_id:
        verified_role = guild.get_role(verified_role_id)
        if verified_role:
            try:
                await member.add_roles(verified_role, reason=f"Verified via {method}")
            except discord.Forbidden:
                logger.warning(f"Cannot add verified role in {guild.name}")

    # Remove quarantine role
    if quarantine_role_id:
        quarantine_role = guild.get_role(quarantine_role_id)
        if quarantine_role and quarantine_role in member.roles:
            try:
                await member.remove_roles(quarantine_role, reason="Verified")
            except discord.Forbidden:
                pass

    # Restore saved roles for returning members (role persistence)
    if saved_role_ids:
        # Dangerous permissions that should NEVER be auto-restored
        DANGEROUS_PERMISSIONS = (
            # Admin-level
            'administrator',
            'manage_guild',
            'manage_roles',
            'manage_channels',
            'ban_members',
            'kick_members',
            'manage_webhooks',
            'manage_expressions',
            'mention_everyone',
            # Mod-level
            'moderate_members',
            'mute_members',
            'deafen_members',
            'move_members',
            'manage_nicknames',
            'manage_events',
            'view_audit_log',
            'view_guild_insights',
        )

        roles_to_add = []
        for role_id in saved_role_ids:
            # Skip excluded roles
            if role_id in excluded_role_ids:
                continue

            role = guild.get_role(role_id)
            if not role or role.managed or role.id == guild.id:
                continue

            # Skip roles with dangerous permissions
            if any(getattr(role.permissions, perm, False) for perm in DANGEROUS_PERMISSIONS):
                logger.warning(f"[ROLE PERSIST] Skipping dangerous role '{role.name}' for {member} after verification")
                continue

            # Don't restore roles higher than bot's top role
            if role < guild.me.top_role:
                roles_to_add.append(role)

        if roles_to_add:
            try:
                await member.add_roles(*roles_to_add, reason="Role persistence - restored on rejoin after verification")
                logger.info(f"[ROLE PERSIST] Restored {len(roles_to_add)} roles for {member} after verification in {guild.name}")
            except discord.Forbidden:
                logger.warning(f"[ROLE PERSIST] Cannot restore roles for {member} in {guild.name} - missing permissions")
            except Exception as e:
                logger.error(f"[ROLE PERSIST] Error restoring roles: {e}")

    # Send success message
    success_msg = verified_message or "You've been verified! Welcome to the server."
    try:
        await interaction.response.send_message(
            f"✅ **Verification Complete!**\n\n{success_msg}",
            ephemeral=True
        )
    except discord.InteractionResponded:
        await interaction.followup.send(
            f"✅ **Verification Complete!**\n\n{success_msg}",
            ephemeral=True
        )

    logger.info(f"Verified {member} in {guild.name} via {method}")

    # Send welcome message now that verification is complete
    try:
        bot = interaction.client
        welcome_cog = bot.get_cog("WelcomeCog")
        if welcome_cog:
            await welcome_cog.send_welcome_message(member)
    except Exception as e:
        logger.error(f"Failed to send post-verification welcome message: {e}")


# ==================== COG ====================

class VerificationCog(commands.Cog):
    """Full member verification system."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.pending_captchas = {}  # {(guild_id, user_id): captcha_code}
        self.verification_timeout_task.start()

        # Register persistent views
        self.bot.loop.create_task(self.register_views())

    def cog_unload(self):
        self.verification_timeout_task.cancel()

    async def register_views(self):
        """Register persistent button views for all guilds."""
        await self.bot.wait_until_ready()

        with db_session_scope() as session:
            configs = session.query(VerificationConfig).all()
            for config in configs:
                guild = self.bot.get_guild(config.guild_id)
                if not guild:
                    continue

                if config.verification_type == VerificationType.BUTTON:
                    view = VerifyButtonView(config.guild_id, config.button_text or "I agree to the rules")
                    self.bot.add_view(view)
                elif config.verification_type == VerificationType.CAPTCHA:
                    view = CaptchaButtonView(config.guild_id, config.captcha_length or 6)
                    self.bot.add_view(view)
                elif config.verification_type == VerificationType.MULTI_STEP:
                    view = MultiStepView(
                        config.guild_id,
                        config.require_rules_read,
                        config.require_intro_message
                    )
                    self.bot.add_view(view)

        logger.info("Registered verification views")

    # ==================== BACKGROUND TASKS ====================

    @tasks.loop(minutes=30)
    async def verification_timeout_task(self):
        """Kick unverified members who have timed out."""
        logger.debug("Running verification timeout task...")

        with db_session_scope() as session:
            # Get all guilds with kick_on_timeout enabled
            configs = (
                session.query(VerificationConfig)
                .filter(VerificationConfig.kick_on_timeout == True)
                .all()
            )

            for config in configs:
                try:
                    guild = self.bot.get_guild(config.guild_id)
                    if not guild:
                        continue

                    db_guild = session.get(Guild, config.guild_id)
                    if not db_guild or not db_guild.verification_enabled:
                        continue

                    timeout_seconds = config.verification_timeout_hours * 3600
                    cutoff_time = int(time.time()) - timeout_seconds

                    # Find unverified members past timeout
                    # IMPORTANT: Only kick members who were EXPLICITLY quarantined (is_quarantined=True)
                    # This prevents kicking existing members who joined before verification was enabled
                    unverified = (
                        session.query(GuildMember)
                        .filter(
                            GuildMember.guild_id == config.guild_id,
                            GuildMember.is_verified == False,
                            GuildMember.is_quarantined == True,  # Must be explicitly quarantined
                            GuildMember.quarantined_at != None,  # Must have quarantine timestamp
                            GuildMember.quarantined_at < cutoff_time  # Check quarantine time, not first_seen
                        )
                        .all()
                    )

                    for db_member in unverified:
                        member = guild.get_member(db_member.user_id)
                        if member:
                            try:
                                hours_quarantined = (int(time.time()) - db_member.quarantined_at) // 3600
                                await member.kick(
                                    reason=f"Verification timeout - quarantined for {hours_quarantined}h (limit: {config.verification_timeout_hours}h)"
                                )
                                logger.info(f"Kicked {member} from {guild.name} for verification timeout (quarantined {hours_quarantined}h)")

                                # Log the action
                                audit = AuditLog(
                                    guild_id=guild.id,
                                    action=AuditAction.MEMBER_KICK,
                                    actor_id=self.bot.user.id,
                                    actor_name="QuestLog",
                                    target_id=member.id,
                                    target_name=str(member),
                                    reason="Verification timeout"
                                )
                                session.add(audit)

                            except discord.Forbidden:
                                logger.warning(f"Cannot kick {member} in {guild.name}")

                except Exception as e:
                    logger.error(f"Error in verification timeout for guild {config.guild_id}: {e}")

    @verification_timeout_task.before_loop
    async def before_timeout_task(self):
        await self.bot.wait_until_ready()

    # ==================== EVENT LISTENERS ====================

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Track intro posts for multi-step verification."""
        try:
            if message.author.bot or not message.guild:
                return

            with db_session_scope() as session:
                config = session.get(VerificationConfig, message.guild.id)
                if not config or not config.require_intro_message or not config.intro_channel_id:
                    return

                if message.channel.id != config.intro_channel_id:
                    return

                db_member = session.get(GuildMember, (message.guild.id, message.author.id))
                if not db_member:
                    return

                _save_step(db_member, "intro")
                logger.info(f"[VERIFY] Marked intro complete for {message.author} in {message.guild.name}")
        except Exception as e:
            logger.error(f"Error tracking intro message: {e}")

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """Handle new member join - apply quarantine and start verification."""
        logger.info(f"[VERIFY] on_member_join triggered for {member} in {member.guild.name}")

        if member.bot:
            logger.debug(f"[VERIFY] Skipping bot: {member}")
            return
        # Ensure bot is ready to resolve channels/roles
        await self.bot.wait_until_ready()

        with db_session_scope() as session:
            db_guild = session.get(Guild, member.guild.id)
            config = session.get(VerificationConfig, member.guild.id)

            logger.info(f"[VERIFY] Guild {member.guild.name}: db_guild exists: {db_guild is not None}, verification_enabled: {db_guild.verification_enabled if db_guild else 'N/A'}")

            if not db_guild or not db_guild.verification_enabled:
                logger.info(f"[VERIFY] Skipping - verification not enabled for {member.guild.name}")
                return

            if not config:
                logger.info(f"[VERIFY] Skipping - no VerificationConfig for {member.guild.name}")
                return

            logger.info(f"[VERIFY] Config found: type={config.verification_type}, require_age={config.require_account_age}, min_days={config.min_account_age_days}")

            # Create or get member record
            db_member = session.get(GuildMember, (member.guild.id, member.id))
            if not db_member:
                logger.warning(
                    f"[DUPLICATE TRACKER] verification.on_member_join CREATING GuildMember: "
                    f"guild_id={member.guild.id}, user_id={member.id}, user_id_type={type(member.id)}, "
                    f"display_name={member.display_name}, source=Discord.Member"
                )
                db_member = GuildMember(
                    guild_id=member.guild.id,
                    user_id=member.id,
                    display_name=member.display_name,
                    is_verified=False,
                )
                session.add(db_member)
            else:
                # RETURNING MEMBER - Check if they were previously verified or have saved roles
                logger.info(f"[VERIFY] Existing member rejoined: {member}, was_verified={db_member.is_verified}, has_saved_roles={bool(getattr(db_member, 'saved_roles', None))}")

                # Check if role persistence is enabled and they have saved roles
                role_persistence_enabled = getattr(db_guild, 'role_persistence_enabled', False)
                saved_roles = getattr(db_member, 'saved_roles', None)
                was_verified = db_member.is_verified

                # If they have saved roles, they were a legitimate member before - restore and skip verification
                # This covers both:
                # 1. Members who verified (is_verified=True)
                # 2. Members who met account age and never needed to verify (is_verified=False but had roles)
                if role_persistence_enabled and saved_roles:
                    # RETURNING MEMBER WITH SAVED ROLES - Skip verification, restore roles, welcome them
                    # They had roles before = they were a legitimate member, NEVER quarantine them
                    logger.info(f"[VERIFY] Returning member {member} has saved roles - restoring and skipping verification")

                    # Get roles to exclude from restoration
                    quarantine_role_id = db_guild.quarantine_role_id
                    excluded_role_ids = set()

                    # Add quarantine role to exclusion
                    if quarantine_role_id:
                        excluded_role_ids.add(quarantine_role_id)

                    # Add admin-configured excluded roles
                    excluded_roles_json = getattr(db_guild, 'role_persistence_excluded', None)
                    if excluded_roles_json:
                        try:
                            excluded_role_ids.update(json.loads(excluded_roles_json))
                        except (json.JSONDecodeError, TypeError):
                            pass

                    # Dangerous permissions that should NEVER be auto-restored
                    # These could allow a kicked admin/mod to regain control
                    DANGEROUS_PERMISSIONS = (
                        # Admin-level
                        'administrator',
                        'manage_guild',
                        'manage_roles',
                        'manage_channels',
                        'ban_members',
                        'kick_members',
                        'manage_webhooks',
                        'manage_expressions',  # Emojis/stickers
                        'mention_everyone',
                        # Mod-level
                        'moderate_members',  # Timeout
                        'mute_members',  # Voice mute
                        'deafen_members',  # Voice deafen
                        'move_members',  # Move between voice channels
                        'manage_nicknames',
                        'manage_events',
                        'view_audit_log',
                        'view_guild_insights',
                    )

                    try:
                        saved_role_ids = json.loads(saved_roles)
                        roles_to_add = []
                        skipped_dangerous = []

                        for role_id in saved_role_ids:
                            # Skip excluded roles (quarantine, admin-configured)
                            if role_id in excluded_role_ids:
                                logger.info(f"[ROLE PERSIST] Skipping excluded role {role_id} for {member}")
                                continue

                            role = member.guild.get_role(role_id)
                            if not role or role.managed or role.id == member.guild.id:
                                continue

                            # Skip roles with dangerous permissions
                            if any(getattr(role.permissions, perm, False) for perm in DANGEROUS_PERMISSIONS):
                                skipped_dangerous.append(role.name)
                                logger.warning(f"[ROLE PERSIST] Skipping dangerous role '{role.name}' for {member} (has elevated permissions)")
                                continue

                            # Don't restore roles higher than bot's top role
                            if role < member.guild.me.top_role:
                                roles_to_add.append(role)

                        if roles_to_add:
                            await member.add_roles(*roles_to_add, reason="Role persistence - returning member")
                            logger.info(f"[ROLE PERSIST] Restored {len(roles_to_add)} roles for returning member {member}")

                        if skipped_dangerous:
                            logger.info(f"[ROLE PERSIST] Skipped {len(skipped_dangerous)} dangerous roles for {member}: {skipped_dangerous}")

                    except (json.JSONDecodeError, TypeError) as e:
                        logger.error(f"[ROLE PERSIST] Error parsing saved roles for {member}: {e}")
                    except discord.Forbidden:
                        logger.warning(f"[ROLE PERSIST] Cannot restore roles for {member} - missing permissions")
                    except Exception as e:
                        logger.error(f"[ROLE PERSIST] Error restoring roles for returning member: {e}")

                    # ALWAYS clear saved roles and skip verification for returning members with saved roles
                    # Even if role restoration failed, they were a member before - don't quarantine them
                    db_member.saved_roles = None
                    db_member.left_at = None
                    db_member.is_quarantined = False
                    session.commit()

                    # Send welcome message (in separate try block so it doesn't affect the return)
                    try:
                        welcome_cog = self.bot.get_cog("WelcomeCog")
                        if welcome_cog:
                            await welcome_cog.send_welcome_message(member)
                            logger.info(f"[VERIFY] Sent welcome message for returning member {member}")
                    except Exception as e:
                        logger.error(f"[VERIFY] Failed to send welcome for returning member: {e}")

                    return  # Done - returning member with saved roles, NEVER quarantine

                # RETURNING VERIFIED MEMBER (no saved roles but was verified before)
                # They should NEVER get quarantine role - just welcome them back
                if was_verified:
                    logger.info(f"[VERIFY] Returning verified member {member} (no saved roles) - skipping quarantine, just welcoming")
                    db_member.is_quarantined = False
                    db_member.quarantined_at = None
                    db_member.quarantine_reason = None
                    session.commit()

                    # Send welcome message
                    try:
                        welcome_cog = self.bot.get_cog("WelcomeCog")
                        if welcome_cog:
                            await welcome_cog.send_welcome_message(member)
                            logger.info(f"[VERIFY] Sent welcome message for returning verified member {member} (no saved roles)")
                    except Exception as e:
                        logger.error(f"[VERIFY] Failed to send welcome for returning verified member: {e}")

                    return  # Done - returning verified member, no verification needed

                # Not a returning verified member OR role persistence failed - reset state
                logger.info(f"[VERIFY] Resetting verification state for rejoining member {member}")
                db_member.verification_method = None
                db_member.is_verified = False
                db_member.is_quarantined = False
                db_member.quarantined_at = None
                db_member.quarantine_reason = None

            # Check account age - applies to ALL verification types when require_account_age is enabled
            # OR always for ACCOUNT_AGE type
            account_too_new = False
            age_days = 0
            min_days = config.min_account_age_days or 0

            should_check_age = (
                config.verification_type == VerificationType.ACCOUNT_AGE or
                config.require_account_age
            )

            if should_check_age and min_days > 0:
                is_old_enough, age_days = check_account_age(member, min_days)
                logger.info(f"[VERIFY] Account age check for {member}: {age_days} days, required: {min_days}, old_enough: {is_old_enough}")

                if is_old_enough:
                    # ACCOUNT OLD ENOUGH - No verification needed!
                    # Just welcome them and give them the auto-join role (NOT the verified role)
                    # They are trusted based on account age alone
                    logger.info(f"[VERIFY] Account {member} is old enough ({age_days} days >= {min_days}), skipping verification - just welcoming")

                    # Mark as not needing verification (but NOT as "verified" - that's for people who went through verification)
                    db_member.is_verified = False  # They didn't verify, they just met age requirement
                    db_member.is_quarantined = False
                    db_member.verification_method = None

                    # Commit DB changes
                    session.commit()

                    # Send welcome message (WelcomeCog handles the auto-join role from welcome settings)
                    try:
                        welcome_cog = self.bot.get_cog("WelcomeCog")
                        if welcome_cog:
                            await welcome_cog.send_welcome_message(member)
                            logger.info(f"[VERIFY] Sent welcome message for {member} (account age met, no verification needed)")
                        else:
                            logger.warning(f"[VERIFY] WelcomeCog not found - cannot send welcome for {member}")
                    except Exception as e:
                        logger.error(f"[VERIFY] Failed to send welcome message for {member}: {e}", exc_info=True)

                    return  # Done - no quarantine, no verification needed

                else:
                    # ACCOUNT TOO NEW - Must verify first, THEN get welcomed
                    # Quarantine them until they complete verification (button/captcha/etc)
                    account_too_new = True
                    db_member.is_verified = False
                    db_member.is_quarantined = True
                    db_member.quarantined_at = int(time.time())
                    db_member.quarantine_reason = f"Account too new ({age_days} days, required: {min_days})"
                    logger.info(f"[VERIFY] Account {member} is too new ({age_days} days < {min_days}), requiring verification before welcome")

            # Apply quarantine for ALL verification types (not just ACCOUNT_AGE)
            # This ensures timeout tracking works for Button, Captcha, etc.
            try:
                logger.info(f"[VERIFY] >>> Entering quarantine check section for {member}")
                logger.info(f"[VERIFY] Checking quarantine: is_verified={db_member.is_verified}, type={config.verification_type}")

                # Commit DB changes before role operations
                session.commit()
                logger.info(f"[VERIFY] DB session committed for {member}")

                if not db_member.is_verified and config.verification_type != VerificationType.NONE:
                    logger.info(f"[VERIFY] Applying quarantine for {member} in {member.guild.name} (type: {config.verification_type.value})")

                    # Mark as quarantined if not already
                    if not db_member.is_quarantined:
                        db_member.is_quarantined = True
                        db_member.quarantined_at = int(time.time())
                        db_member.quarantine_reason = f"Pending {config.verification_type.value} verification"
                        session.commit()
                        logger.info(f"[VERIFY] Set quarantine flag for {member}")

                    # Apply quarantine role
                    quarantine_role_id = db_guild.quarantine_role_id
                    logger.info(f"[VERIFY] Quarantine role ID from DB: {quarantine_role_id}")

                    if quarantine_role_id:
                        quarantine_role = member.guild.get_role(quarantine_role_id)
                        logger.info(f"[VERIFY] Quarantine role object: {quarantine_role}")

                        if quarantine_role:
                            try:
                                await member.add_roles(quarantine_role, reason="Pending verification")
                                logger.info(f"[VERIFY] Successfully assigned quarantine role to {member} in {member.guild.name}")
                            except discord.Forbidden:
                                logger.warning(f"[VERIFY] Cannot add quarantine role in {member.guild.name} - missing permissions")
                            except Exception as role_err:
                                logger.error(f"[VERIFY] Error adding quarantine role in {member.guild.name}: {role_err}")
                        else:
                            logger.warning(f"[VERIFY] Quarantine role {quarantine_role_id} not found in guild {member.guild.name}")
                    else:
                        logger.warning(f"[VERIFY] No quarantine role configured for {member.guild.name}")
                else:
                    logger.info(f"[VERIFY] Skipping quarantine - is_verified={db_member.is_verified} or type={config.verification_type}")
            except Exception as quarantine_err:
                logger.error(f"[VERIFY] EXCEPTION in quarantine section for {member}: {quarantine_err}", exc_info=True)

            verification_channel_id = db_guild.verification_channel_id
            instructions = config.verification_instructions

        # Send DM with verification instructions (if channel not set)
        if not verification_channel_id:
            try:
                dm_msg = (
                    f"Welcome to **{member.guild.name}**!\n\n"
                    f"{instructions or 'Please verify yourself using `/verify me` in the server.'}"
                )
                await member.send(dm_msg)
            except discord.Forbidden:
                pass

    # ==================== SLASH COMMANDS ====================

    verify = SlashCommandGroup(
        name="verify",
        description="Verification commands",
        
    )

    @verify.command(name="me", description="Verify yourself to access the server")
    async def verify_me(self, ctx: discord.ApplicationContext):
        """Member self-verification."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)
            config = session.get(VerificationConfig, ctx.guild.id)
            db_member = session.get(GuildMember, (ctx.guild.id, ctx.author.id))

            if not db_guild or not db_guild.verification_enabled:
                await ctx.respond(
                    "Verification is not enabled on this server.",
                    ephemeral=True
                )
                return

            if db_member and db_member.is_verified:
                await ctx.respond(
                    "You're already verified!",
                    ephemeral=True
                )
                return

            if not config:
                # Default to button verification
                await process_verification(ctx.interaction, "button")
                return

            verification_type = config.verification_type

        # Handle different verification types
        if verification_type == VerificationType.NONE:
            await process_verification(ctx.interaction, "none")

        elif verification_type == VerificationType.BUTTON:
            await process_verification(ctx.interaction, "button")

        elif verification_type == VerificationType.CAPTCHA:
            # Generate image-based captcha (bot-resistant)
            captcha_length = config.captcha_length if config else 6
            captcha_code = generate_captcha(captcha_length)
            image_buffer = generate_captcha_image(captcha_code)

            # Store code server-side with 5-minute expiry
            key = (ctx.guild.id, ctx.author.id)
            _pending_captchas[key] = {
                'code': captcha_code,
                'expires': time.time() + 300  # 5 minutes
            }

            # Create embed with instructions
            embed = discord.Embed(
                title="🔐 Captcha Verification",
                description=(
                    "**Enter the code shown in the image below.**\n\n"
                    "• The code is case-insensitive\n"
                    "• You have 5 minutes to complete this\n"
                    "• Use the button below to submit your answer"
                ),
                color=discord.Color.blue()
            )
            embed.set_footer(text="If you can't read the code, run /verify me again for a new one")

            # Create a view with submit button
            class SubmitCaptchaView(View):
                def __init__(self, guild_id: int, length: int):
                    super().__init__(timeout=300)
                    self.guild_id = guild_id
                    self.length = length

                @discord.ui.button(label="Submit Answer", style=discord.ButtonStyle.success, emoji="✅")
                async def submit_button(self, button: discord.ui.Button, interaction: discord.Interaction):
                    modal = CaptchaModal(self.guild_id, self.length)
                    await interaction.response.send_modal(modal)

            # Send image as attachment
            file = discord.File(image_buffer, filename="captcha.png")
            embed.set_image(url="attachment://captcha.png")

            await ctx.respond(
                embed=embed,
                file=file,
                view=SubmitCaptchaView(ctx.guild.id, captcha_length),
                ephemeral=True
            )

        elif verification_type == VerificationType.ACCOUNT_AGE:
            # Check account age
            min_days = config.min_account_age_days if config else DefaultVerificationSettings.MIN_ACCOUNT_AGE_DAYS
            is_old_enough, age_days = check_account_age(ctx.author, min_days)

            if is_old_enough:
                await process_verification(ctx.interaction, "account_age")
            else:
                await ctx.respond(
                    f"Your account must be at least **{min_days}** days old. "
                    f"Your account is only **{age_days}** days old.\n\n"
                    f"Please contact a moderator if you need assistance.",
                    ephemeral=True
                )

        elif verification_type == VerificationType.MULTI_STEP:
            # Check access for multi-step (Complete tier, VIP, or Moderation module)
            if not has_moderation_access(session, ctx.guild.id):
                await ctx.respond(
                    "Multi-step verification requires **Complete tier** or the **Moderation Module**.\n"
                    "Please contact an admin to upgrade or use basic verification.",
                    ephemeral=True
                )
                return

            # Show multi-step instructions
            steps = ["1. Click **I've read the rules** after reading the server rules"]
            if config and config.require_intro_message and config.intro_channel_id:
                intro_channel = ctx.guild.get_channel(config.intro_channel_id)
                if intro_channel:
                    steps.append(f"2. Post an introduction in {intro_channel.mention}")
            steps.append(f"{len(steps)+1}. Click **Complete Verification** to finish")

            await ctx.respond(
                "**Multi-Step Verification**\n\n" +
                "\n".join(steps) +
                "\n\nUse the buttons in the verification channel to proceed.",
                ephemeral=True
            )

    @verify.command(name="user", description="Manually verify a user (Admin)")
    @discord.default_permissions(manage_roles=True)
    @commands.has_permissions(manage_roles=True)
    @discord.option("member", discord.Member, description="Member to verify")
    @discord.option("reason", str, description="Reason for manual verification", required=False)
    async def verify_user(
        self,
        ctx: discord.ApplicationContext,
        member: discord.Member,
        reason: str = None
    ):
        """Manually verify a member."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)

            if not db_guild:
                await ctx.respond("Guild not configured.", ephemeral=True)
                return

            # Get or create member
            db_member = session.get(GuildMember, (ctx.guild.id, member.id))
            if not db_member:
                logger.warning(
                    f"[DUPLICATE TRACKER] verification.verify_user CREATING GuildMember: "
                    f"guild_id={ctx.guild.id}, user_id={member.id}, user_id_type={type(member.id)}, "
                    f"display_name={member.display_name}, source=Discord.Member"
                )
                db_member = GuildMember(
                    guild_id=ctx.guild.id,
                    user_id=member.id,
                    display_name=member.display_name,
                )
                session.add(db_member)

            if db_member.is_verified:
                await ctx.respond(f"{member.mention} is already verified.", ephemeral=True)
                return

            # Verify the member
            db_member.is_verified = True
            db_member.verified_at = int(time.time())
            db_member.verification_method = f"manual:{ctx.author.id}"
            db_member.is_quarantined = False

            # Log
            audit = AuditLog(
                guild_id=ctx.guild.id,
                action=AuditAction.VERIFICATION_PASSED,
                actor_id=ctx.author.id,
                actor_name=str(ctx.author),
                target_id=member.id,
                target_name=str(member),
                reason=reason,
                details=f"Manually verified by {ctx.author}"
            )
            session.add(audit)

            verified_role_id = db_guild.verified_role_id
            quarantine_role_id = db_guild.quarantine_role_id

            # Get saved roles for persistence (returning members)
            saved_role_ids = []
            if db_guild.role_persistence_enabled and db_member.saved_roles:
                try:
                    saved_role_ids = json.loads(db_member.saved_roles)
                    # Clear saved roles now that we're restoring them
                    db_member.saved_roles = None
                    db_member.left_at = None
                except (json.JSONDecodeError, TypeError):
                    saved_role_ids = []

        # Update roles
        if verified_role_id:
            verified_role = ctx.guild.get_role(verified_role_id)
            if verified_role:
                try:
                    await member.add_roles(verified_role, reason=f"Manually verified by {ctx.author}")
                except discord.Forbidden:
                    pass

        if quarantine_role_id:
            quarantine_role = ctx.guild.get_role(quarantine_role_id)
            if quarantine_role and quarantine_role in member.roles:
                try:
                    await member.remove_roles(quarantine_role, reason="Manually verified")
                except discord.Forbidden:
                    pass

        # Restore saved roles for returning members (role persistence)
        if saved_role_ids:
            roles_to_add = []
            for role_id in saved_role_ids:
                role = ctx.guild.get_role(role_id)
                if role and not role.managed and role.id != ctx.guild.id:
                    if role < ctx.guild.me.top_role:
                        roles_to_add.append(role)

            if roles_to_add:
                try:
                    await member.add_roles(*roles_to_add, reason="Role persistence - restored on manual verification")
                    logger.info(f"[ROLE PERSIST] Restored {len(roles_to_add)} roles for {member} on manual verify in {ctx.guild.name}")
                except discord.Forbidden:
                    logger.warning(f"[ROLE PERSIST] Cannot restore roles for {member} in {ctx.guild.name}")
                except Exception as e:
                    logger.error(f"[ROLE PERSIST] Error restoring roles on manual verify: {e}")

        await ctx.respond(
            f"✅ **{member.mention}** has been manually verified." +
            (f"\nReason: {reason}" if reason else ""),
            ephemeral=True
        )
        logger.info(f"Manually verified {member} in {ctx.guild.name} by {ctx.author}")

    @verify.command(name="pending", description="View pending verifications")
    @discord.default_permissions(manage_roles=True)
    @commands.has_permissions(manage_roles=True)
    async def verify_pending(self, ctx: discord.ApplicationContext):
        """View members pending verification."""
        with db_session_scope() as session:
            pending = (
                session.query(GuildMember)
                .filter(
                    GuildMember.guild_id == ctx.guild.id,
                    GuildMember.is_verified == False
                )
                .order_by(GuildMember.first_seen.desc())
                .limit(25)
                .all()
            )

            if not pending:
                await ctx.respond("No pending verifications.", ephemeral=True)
                return

            embed = discord.Embed(
                title="Pending Verifications",
                color=discord.Color.orange()
            )

            for db_member in pending:
                member = ctx.guild.get_member(db_member.user_id)
                if member:
                    joined = f"<t:{db_member.first_seen}:R>"
                    status = "Quarantined" if db_member.is_quarantined else "Pending"
                    embed.add_field(
                        name=f"{member.display_name}",
                        value=f"Joined: {joined}\nStatus: {status}",
                        inline=True
                    )

            embed.set_footer(text=f"Showing up to 25 pending members")

        await ctx.respond(embed=embed, ephemeral=True)

    @verify.command(name="kick-unverified", description="Kick all unverified members (Admin)")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    @discord.option("older_than_hours", int, description="Only kick if joined more than X hours ago", default=24)
    async def verify_kick_unverified(
        self,
        ctx: discord.ApplicationContext,
        older_than_hours: int = 24
    ):
        """Kick all unverified members past the timeout."""
        await ctx.defer(ephemeral=True)

        cutoff_time = int(time.time()) - (older_than_hours * 3600)

        with db_session_scope() as session:
            unverified = (
                session.query(GuildMember)
                .filter(
                    GuildMember.guild_id == ctx.guild.id,
                    GuildMember.is_verified == False,
                    GuildMember.first_seen < cutoff_time
                )
                .all()
            )

            kicked = 0
            failed = 0

            for db_member in unverified:
                member = ctx.guild.get_member(db_member.user_id)
                if member:
                    try:
                        await member.kick(reason=f"Mass kick unverified by {ctx.author}")
                        kicked += 1
                        await asyncio.sleep(0.5)
                    except discord.Forbidden:
                        failed += 1

        await ctx.followup.send(
            f"**Kicked {kicked}** unverified members (joined >{older_than_hours}h ago)." +
            (f"\n{failed} failed due to permissions." if failed else ""),
            ephemeral=True
        )

    @verify.command(name="setup", description="Send verification embed to a channel (Admin)")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    @discord.option("channel", discord.TextChannel, description="Channel to send verification message")
    async def verify_setup(
        self,
        ctx: discord.ApplicationContext,
        channel: discord.TextChannel
    ):
        """Send verification embed with buttons to a channel."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)
            config = session.get(VerificationConfig, ctx.guild.id)

            if not db_guild:
                await ctx.respond("Run `/questlog setup` first.", ephemeral=True)
                return

            verification_type = config.verification_type if config else VerificationType.BUTTON
            button_text = config.button_text if config else "I agree to the rules"
            captcha_length = config.captcha_length if config else 6
            instructions = config.verification_instructions if config else None

        # Create embed
        embed = discord.Embed(
            title="Server Verification",
            description=instructions or (
                "Welcome! Please verify yourself to access the server.\n\n"
                "Click the button below to complete verification."
            ),
            color=discord.Color.blue()
        )
        embed.set_footer(text="Powered by QuestLog")

        # Create appropriate view based on verification type
        if verification_type == VerificationType.BUTTON:
            view = VerifyButtonView(ctx.guild.id, button_text)
        elif verification_type == VerificationType.CAPTCHA:
            view = CaptchaButtonView(ctx.guild.id, captcha_length)
            embed.description += "\n\n*You will be asked to solve a captcha.*"
        elif verification_type == VerificationType.MULTI_STEP:
            if not has_moderation_access(session, ctx.guild.id):
                await ctx.respond(
                    "Multi-step verification requires **Complete tier** or the **Moderation Module**.",
                    ephemeral=True
                )
                return
            view = MultiStepView(ctx.guild.id, True, config.require_intro_message if config else False)
            embed.description = (
                "**Multi-Step Verification Required**\n\n"
                "1. Read the server rules\n"
                "2. Click **I've read the rules**\n"
            )
            if config and config.require_intro_message and config.intro_channel_id:
                intro_ch = ctx.guild.get_channel(config.intro_channel_id)
                if intro_ch:
                    embed.description += f"3. Post an introduction in {intro_ch.mention}\n"
            embed.description += f"4. Click **Complete Verification**"
        else:
            # Account age - just informational
            embed.description = (
                "Verification is automatic based on account age.\n\n"
                "If you're seeing this, your account may be too new. "
                "Please contact a moderator for manual verification."
            )
            view = None

        try:
            await channel.send(embed=embed, view=view)
            await ctx.respond(
                f"Verification message sent to {channel.mention}!",
                ephemeral=True
            )

            # Update guild verification channel
            with db_session_scope() as session:
                db_guild = session.get(Guild, ctx.guild.id)
                if db_guild:
                    db_guild.verification_channel_id = channel.id

        except discord.Forbidden:
            await ctx.respond(
                f"I don't have permission to send messages in {channel.mention}.",
                ephemeral=True
            )

    @verify.command(name="config", description="Configure verification settings (Admin)")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    @discord.option(
        "type",
        str,
        description="Verification type",
        required=False,
        choices=["none", "button", "captcha", "account_age", "multi_step"]
    )
    @discord.option("min_account_age", int, description="Minimum account age in days", required=False)
    @discord.option("button_text", str, description="Custom button text", required=False)
    @discord.option("timeout_hours", int, description="Hours before kicking unverified", required=False)
    @discord.option("kick_on_timeout", bool, description="Auto-kick on timeout", required=False)
    async def verify_config(
        self,
        ctx: discord.ApplicationContext,
        type: str = None,
        min_account_age: int = None,
        button_text: str = None,
        timeout_hours: int = None,
        kick_on_timeout: bool = None
    ):
        """Configure verification settings."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)
            if not db_guild:
                await ctx.respond("Run `/questlog setup` first.", ephemeral=True)
                return

            config = session.get(VerificationConfig, ctx.guild.id)
            if not config:
                config = VerificationConfig(guild_id=ctx.guild.id)
                session.add(config)

            changes = []

            if type:
                # Check access for multi_step (Complete tier, VIP, or Moderation module)
                if type == "multi_step":
                    if not has_moderation_access(session, ctx.guild.id):
                        await ctx.respond(
                            "Multi-step verification requires **Complete tier** or the **Moderation Module**!",
                            ephemeral=True
                        )
                        return

                config.verification_type = VerificationType(type)
                changes.append(f"Type: **{type}**")

            if min_account_age is not None:
                config.min_account_age_days = min_account_age
                config.require_account_age = min_account_age > 0
                changes.append(f"Min account age: **{min_account_age} days**")

            if button_text:
                config.button_text = button_text[:100]
                changes.append(f"Button text: **{button_text[:50]}**")

            if timeout_hours is not None:
                config.verification_timeout_hours = timeout_hours
                changes.append(f"Timeout: **{timeout_hours} hours**")

            if kick_on_timeout is not None:
                config.kick_on_timeout = kick_on_timeout
                changes.append(f"Kick on timeout: **{'Yes' if kick_on_timeout else 'No'}**")

            if not changes:
                # Show current config
                embed = discord.Embed(
                    title="Verification Settings",
                    color=discord.Color.blue()
                )
                embed.add_field(
                    name="Type",
                    value=config.verification_type.value,
                    inline=True
                )
                embed.add_field(
                    name="Min Account Age",
                    value=f"{config.min_account_age_days} days" if config.require_account_age else "Disabled",
                    inline=True
                )
                embed.add_field(
                    name="Timeout",
                    value=f"{config.verification_timeout_hours}h",
                    inline=True
                )
                embed.add_field(
                    name="Kick on Timeout",
                    value="Yes" if config.kick_on_timeout else "No",
                    inline=True
                )
                embed.add_field(
                    name="Button Text",
                    value=config.button_text or "Default",
                    inline=True
                )
                await ctx.respond(embed=embed, ephemeral=True)
            else:
                await ctx.respond(
                    "**Settings updated:**\n" + "\n".join(changes) +
                    "\n\nRun `/verify setup #channel` to update the verification message.",
                    ephemeral=True
                )

    @verify.command(name="intro-channel", description="Set intro channel for multi-step (Premium)")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    @discord.option("channel", discord.TextChannel, description="Channel for introductions")
    @discord.option("required", bool, description="Require intro post to verify", default=True)
    async def verify_intro_channel(
        self,
        ctx: discord.ApplicationContext,
        channel: discord.TextChannel,
        required: bool = True
    ):
        """Set intro channel for multi-step verification."""
        with db_session_scope() as session:
            if not has_moderation_access(session, ctx.guild.id):
                await ctx.respond(
                    "Multi-step verification requires **Complete tier** or the **Moderation Module**!",
                    ephemeral=True
                )
                return

            config = session.get(VerificationConfig, ctx.guild.id)
            if not config:
                config = VerificationConfig(guild_id=ctx.guild.id)
                session.add(config)

            config.intro_channel_id = channel.id
            config.require_intro_message = required

        await ctx.respond(
            f"Intro channel set to {channel.mention}.\n"
            f"Intro required: **{'Yes' if required else 'No'}**",
            ephemeral=True
        )

    @verify.command(name="lockdown", description="Auto-configure channel permissions for quarantine role (Admin)")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    @discord.option("verification_channel", discord.TextChannel, description="Channel where unverified users can see the verify button")
    async def verify_lockdown(
        self,
        ctx: discord.ApplicationContext,
        verification_channel: discord.TextChannel
    ):
        """
        Automatically set up channel permissions for verification:
        - Quarantine role: DENY view on all channels/categories
        - Quarantine role: ALLOW view on verification channel only
        """
        await ctx.defer(ephemeral=True)

        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)
            if not db_guild or not db_guild.quarantine_role_id:
                await ctx.followup.send(
                    "Please set a **Quarantine Role** in the verification settings first!",
                    ephemeral=True
                )
                return

            quarantine_role_id = db_guild.quarantine_role_id

        quarantine_role = ctx.guild.get_role(quarantine_role_id)
        if not quarantine_role:
            await ctx.followup.send(
                "Quarantine role not found. Please check your verification settings.",
                ephemeral=True
            )
            return

        updated_categories = 0
        updated_channels = 0
        errors = []

        # Update all categories
        for category in ctx.guild.categories:
            try:
                # Deny view for quarantine role on all categories
                await category.set_permissions(
                    quarantine_role,
                    view_channel=False,
                    reason="Verification lockdown - hide from unverified"
                )
                updated_categories += 1
                await asyncio.sleep(0.5)  # Rate limit protection
            except discord.Forbidden:
                errors.append(f"Category: {category.name} (no permission)")
            except Exception as e:
                errors.append(f"Category: {category.name} ({str(e)[:30]})")

        # Update ALL channels (including those in categories)
        # This handles channels that have been un-synced from their category permissions
        for channel in ctx.guild.channels:
            # Skip categories (already handled above) and the verification channel
            if isinstance(channel, discord.CategoryChannel):
                continue
            if channel.id == verification_channel.id:
                continue

            try:
                await channel.set_permissions(
                    quarantine_role,
                    view_channel=False,
                    reason="Verification lockdown - hide from unverified"
                )
                updated_channels += 1
                await asyncio.sleep(0.5)  # Rate limit protection
            except discord.Forbidden:
                errors.append(f"Channel: {channel.name} (no permission)")
            except Exception as e:
                errors.append(f"Channel: {channel.name} ({str(e)[:30]})")

        # Allow view on verification channel
        try:
            await verification_channel.set_permissions(
                quarantine_role,
                view_channel=True,
                read_message_history=True,
                send_messages=False,  # They can only click the button, not chat
                reason="Verification lockdown - allow unverified to see verify button"
            )
        except discord.Forbidden:
            errors.append(f"Verification channel: {verification_channel.name} (no permission)")

        # Build response
        result = (
            f"**Lockdown Complete!**\n\n"
            f"✅ Updated **{updated_categories}** categories\n"
            f"✅ Updated **{updated_channels}** standalone channels\n"
            f"✅ Allowed access to {verification_channel.mention}\n\n"
            f"Unverified members with the `{quarantine_role.name}` role can now only see the verification channel."
        )

        if errors:
            result += f"\n\n⚠️ **{len(errors)} errors:**\n" + "\n".join(errors[:10])
            if len(errors) > 10:
                result += f"\n...and {len(errors) - 10} more"

        await ctx.followup.send(result, ephemeral=True)
        logger.info(f"Verification lockdown completed in {ctx.guild.name}: {updated_categories} categories, {updated_channels} channels")

    @verify.command(name="debug", description="Show verification debug info (Admin)")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    async def verify_debug(self, ctx: discord.ApplicationContext):
        """Show all verification-related database values for debugging."""
        await ctx.defer(ephemeral=True)

        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)
            config = session.get(VerificationConfig, ctx.guild.id)

            lines = ["**🔍 Verification Debug Info**\n"]

            # Guild table values
            lines.append("**Guild Table:**")
            if db_guild:
                lines.append(f"• `verification_enabled`: **{db_guild.verification_enabled}**")
                lines.append(f"• `verification_channel_id`: {db_guild.verification_channel_id or 'Not set'}")
                lines.append(f"• `verified_role_id`: {db_guild.verified_role_id or 'Not set'}")
                lines.append(f"• `quarantine_role_id`: {db_guild.quarantine_role_id or 'Not set'}")

                # Check if roles exist
                if db_guild.verified_role_id:
                    role = ctx.guild.get_role(db_guild.verified_role_id)
                    lines.append(f"  → Verified role exists: {role.name if role else '❌ NOT FOUND'}")
                if db_guild.quarantine_role_id:
                    role = ctx.guild.get_role(db_guild.quarantine_role_id)
                    lines.append(f"  → Quarantine role exists: {role.name if role else '❌ NOT FOUND'}")
            else:
                lines.append("❌ No Guild record found in database!")

            lines.append("")

            # VerificationConfig table values
            lines.append("**VerificationConfig Table:**")
            if config:
                lines.append(f"• `verification_type`: **{config.verification_type.value}**")
                lines.append(f"• `require_account_age`: {config.require_account_age}")
                lines.append(f"• `min_account_age_days`: {config.min_account_age_days}")
                lines.append(f"• `button_text`: {config.button_text}")
                lines.append(f"• `verification_timeout_hours`: {config.verification_timeout_hours}")
                lines.append(f"• `kick_on_timeout`: {config.kick_on_timeout}")
            else:
                lines.append("❌ No VerificationConfig record found!")

            lines.append("")

            # Summary diagnosis
            lines.append("**🩺 Diagnosis:**")
            issues = []

            if not db_guild:
                issues.append("No guild record - save verification settings on the web dashboard")
            elif not db_guild.verification_enabled:
                issues.append("**verification_enabled is FALSE** - set verification type to anything other than 'None' and save")

            if not config:
                issues.append("No VerificationConfig - save verification settings on the web dashboard")
            elif config.verification_type == VerificationType.NONE:
                issues.append("verification_type is NONE - change to Button, Captcha, etc.")

            if db_guild and not db_guild.quarantine_role_id:
                issues.append("No quarantine role set - select one in the web dashboard")

            if db_guild and not db_guild.verification_channel_id:
                issues.append("No verification channel set - run `/verify setup #channel` or set in dashboard")

            if issues:
                for issue in issues:
                    lines.append(f"⚠️ {issue}")
            else:
                lines.append("✅ Configuration looks correct!")
                lines.append("\nIf verification still isn't working, check the bot logs for `[VERIFY]` entries.")

        await ctx.followup.send("\n".join(lines), ephemeral=True)


def setup(bot: commands.Bot):
    bot.add_cog(VerificationCog(bot))
