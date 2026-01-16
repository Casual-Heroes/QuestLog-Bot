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

import time
import secrets  # SECURITY FIX: Use secrets instead of random for CAPTCHA
import string
import asyncio
import discord
from discord.ext import commands, tasks
from discord import SlashCommandGroup
from discord.ui import View, Button, Modal, InputText

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
    if db_guild.is_vip or db_guild.subscription_tier == 'complete':
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
    """Modal for captcha input."""

    def __init__(self, captcha_code: str, guild_id: int):
        super().__init__(title="Verification Captcha")
        self.captcha_code = captcha_code
        self.guild_id = guild_id

        self.captcha_input = InputText(
            label=f"Enter the code: {captcha_code}",
            placeholder="Type the code exactly as shown",
            min_length=len(captcha_code),
            max_length=len(captcha_code),
        )
        self.add_item(self.captcha_input)

    async def callback(self, interaction: discord.Interaction):
        """Handle captcha submission."""
        user_input = self.captcha_input.value.upper().strip()

        if user_input == self.captcha_code:
            await process_verification(interaction, "captcha")
        else:
            await interaction.response.send_message(
                "Incorrect code. Please try again with `/verify me`.",
                ephemeral=True
            )


class CaptchaButtonView(View):
    """Button to start captcha verification."""

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

    async def captcha_callback(self, interaction: discord.Interaction):
        """Show captcha modal."""
        captcha_code = generate_captcha(self.captcha_length)
        modal = CaptchaModal(captcha_code, self.guild_id)
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

        # Check account age if required
        if config and config.require_account_age:
            is_old_enough, age_days = check_account_age(member, config.min_account_age_days)
            if not is_old_enough:
                await interaction.response.send_message(
                    f"Your account must be at least **{config.min_account_age_days}** days old. "
                    f"Your account is only **{age_days}** days old.\n\n"
                    f"Please try again later or contact a moderator.",
                    ephemeral=True
                )
                return

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
                    unverified = (
                        session.query(GuildMember)
                        .filter(
                            GuildMember.guild_id == config.guild_id,
                            GuildMember.is_verified == False,
                            GuildMember.first_seen < cutoff_time
                        )
                        .all()
                    )

                    for db_member in unverified:
                        member = guild.get_member(db_member.user_id)
                        if member:
                            try:
                                await member.kick(
                                    reason=f"Verification timeout ({config.verification_timeout_hours}h)"
                                )
                                logger.info(f"Kicked {member} from {guild.name} for verification timeout")

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
        if member.bot:
            return
        # Ensure bot is ready to resolve channels/roles
        await self.bot.wait_until_ready()

        with db_session_scope() as session:
            db_guild = session.get(Guild, member.guild.id)
            config = session.get(VerificationConfig, member.guild.id)

            if not db_guild or not db_guild.verification_enabled:
                return

            if not config:
                return

            # Create member record
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
                # Reset multi-step progress when rejoining
                db_member.verification_method = None

            # Check account age for auto-verification
            if config.verification_type == VerificationType.ACCOUNT_AGE:
                if config.require_account_age:
                    is_old_enough, age_days = check_account_age(member, config.min_account_age_days)
                    if is_old_enough:
                        # Auto-verify
                        db_member.is_verified = True
                        db_member.verified_at = int(time.time())
                        db_member.verification_method = "account_age"

                        # Assign verified role
                        if db_guild.verified_role_id:
                            verified_role = member.guild.get_role(db_guild.verified_role_id)
                            if verified_role:
                                try:
                                    await member.add_roles(verified_role, reason="Auto-verified (account age)")
                                except discord.Forbidden:
                                    pass

                        logger.info(f"Auto-verified {member} in {member.guild.name} (account age: {age_days} days)")
                        return
                    else:
                        # Quarantine new account
                        db_member.is_quarantined = True
                        db_member.quarantined_at = int(time.time())
                        db_member.quarantine_reason = f"Account too new ({age_days} days)"

            # Apply quarantine role
            if db_guild.quarantine_role_id and not db_member.is_verified:
                quarantine_role = member.guild.get_role(db_guild.quarantine_role_id)
                if quarantine_role:
                    try:
                        await member.add_roles(quarantine_role, reason="Pending verification")
                    except discord.Forbidden:
                        logger.warning(f"Cannot add quarantine role in {member.guild.name}")
                    except Exception as role_err:
                        logger.error(f"Error adding quarantine role in {member.guild.name}: {role_err}")

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
            captcha_code = generate_captcha(config.captcha_length if config else 6)
            modal = CaptchaModal(captcha_code, ctx.guild.id)
            await ctx.send_modal(modal)

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


def setup(bot: commands.Bot):
    bot.add_cog(VerificationCog(bot))
