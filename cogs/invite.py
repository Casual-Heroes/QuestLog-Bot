# cogs/invite.py - Early access invite code generator for Discord
#
# /invite - Get a personal early-access invite code for QuestLog (DM'd to you)
#
# Only works in guilds listed in EARLY_ACCESS_GUILD_IDS env var (comma-separated Discord guild IDs).
# Each Discord user gets one code (reuses existing unused code if they already have one).
# Codes are tagged platform='discord' - separate pool from Fluxer codes.

import os
import time
import secrets

import discord
from discord.ext import commands
from sqlalchemy import text

from config import db_session_scope, logger

# Per-user cooldown: one invite lookup per hour
_invite_cooldowns: dict[int, float] = {}
_INVITE_COOLDOWN = 3600.0  # 1 hour

# Guilds where /invite is allowed (comma-separated Discord guild IDs in env)
_raw = os.getenv('EARLY_ACCESS_GUILD_IDS', '').strip()
EARLY_ACCESS_GUILD_IDS: set[int] = {int(g.strip()) for g in _raw.split(',') if g.strip().isdigit()}

_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'  # no O/0 or I/1


def _gen_code() -> str:
    return ''.join(secrets.choice(_ALPHABET) for _ in range(10))


class InviteCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @discord.slash_command(name="invite", description="Get your personal QuestLog early-access invite code")
    async def invite(self, ctx: discord.ApplicationContext):
        await ctx.defer(ephemeral=True)

        user_id = ctx.author.id
        guild_id = ctx.guild.id if ctx.guild else None
        now = time.time()

        # Guild check
        if EARLY_ACCESS_GUILD_IDS and guild_id not in EARLY_ACCESS_GUILD_IDS:
            await ctx.respond("This command is only available in the official Casual Heroes community.", ephemeral=True)
            return

        # Cooldown check
        if now - _invite_cooldowns.get(user_id, 0) < _INVITE_COOLDOWN:
            await ctx.respond("You already requested an invite code recently. Check your DMs.", ephemeral=True)
            return
        _invite_cooldowns[user_id] = now

        notes = f'discord:{user_id}'

        with db_session_scope() as db:
            # Reuse existing unused code if they already have one
            existing = db.execute(text(
                "SELECT code FROM web_early_access_codes "
                "WHERE notes = :notes AND used_by_user_id IS NULL AND is_revoked = 0 "
                "LIMIT 1"
            ), {'notes': notes}).fetchone()

            if existing:
                code_str = existing[0]
                action = 'existing'
            else:
                # Generate a unique code
                code_str = None
                for _ in range(10):
                    candidate = _gen_code()
                    clash = db.execute(text(
                        "SELECT 1 FROM web_early_access_codes WHERE code = :code"
                    ), {'code': candidate}).fetchone()
                    if not clash:
                        code_str = candidate
                        break

                if not code_str:
                    await ctx.respond("Could not generate a code right now - please try again.", ephemeral=True)
                    return

                db.execute(text(
                    "INSERT INTO web_early_access_codes (code, platform, notes, created_at) "
                    "VALUES (:code, 'discord', :notes, :now)"
                ), {'code': code_str, 'notes': notes, 'now': int(time.time())})
                db.commit()
                action = 'new'

        logger.info(f"InviteCog: {action} code {code_str} sent to Discord user {user_id}")

        embed = discord.Embed(
            title="Your QuestLog Invite Code",
            description=(
                f"Here's your personal early-access invite code:\n\n"
                f"**`{code_str}`**\n\n"
                f"Head to [casual-heroes.com/ql/register/](https://casual-heroes.com/ql/register/) "
                f"and enter this code when creating your account.\n\n"
                f"*One-time use - keep it to yourself!*"
            ),
            color=0x6366F1,
        )
        embed.set_footer(text="QuestLog Early Access | casual-heroes.com/ql/")

        try:
            await ctx.author.send(embed=embed)
            await ctx.respond("Check your DMs! I sent you your invite code.", ephemeral=True)
        except discord.Forbidden:
            logger.warning(f"InviteCog: could not DM Discord user {user_id}")
            await ctx.respond(
                f"Couldn't DM you - make sure your DMs are open.\n"
                f"Your code: **`{code_str}`**\n"
                f"Register at https://casual-heroes.com/ql/register/",
                ephemeral=True,
            )


def setup(bot):
    bot.add_cog(InviteCog(bot))
