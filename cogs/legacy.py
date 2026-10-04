# cogs/legacy.py - Legacy points for Discord server behavior
#
# Ported from QuestLogFluxer/cogs/legacy.py (June 2026)
# Translated from Fluxer Cog pattern to discord.py Cog pattern.
# Key difference: uses discord_id (not fluxer_id) to look up web_users.
#
# Two systems:
#
# 1. Star reactions (⭐)
#    - When a member reacts ⭐ to someone else's message, the message author
#      earns +5 Legacy (comment_helpful). Anti-abuse: one ⭐ per reactor per
#      message (ref_id = "star_{message_id}_{reactor_id}"). Bots ignored.
#      No daily cap - ref_id dedup in _award_web_legacy handles it.
#
# 2. Clean record milestones (background loop, runs every 6 hours)
#    - Checks guild_members.first_seen for users linked to a QuestLog account
#    - Awards clean_record_30d / 60d / 90d if:
#        a) They have been a member >= that many days
#        b) They have NO negative legacy events (report_upheld, temp_ban) since joining
#        c) Not already awarded (ref_id dedup)
#
# Source for all awards: 'discord'

import asyncio
import time

import discord
from discord.ext import commands
from sqlalchemy import text as sa_text

from config import db_session_scope, logger, QUESTLOG_PROGRESSION_API_ENABLED

STAR_EMOJI = '⭐'  # ⭐

CHECK_INTERVAL = 6 * 3600  # 6 hours

CLEAN_RECORD_MILESTONES = [
    (30,  'clean_record_30d'),
    (60,  'clean_record_60d'),
    (90,  'clean_record_90d'),
]

LEGACY_POINTS = {
    'lfg_group_filled':       10,
    'lfg_completed':          15,
    'comment_helpful':         5,
    'clean_record_30d':       10,
    'clean_record_60d':       15,
    'clean_record_90d':       20,
    'report_upheld':         -20,
    'temp_ban':              -50,
    'ingame_boss_kill':       50,
    'ingame_boss_assist':     15,
    'ingame_boss_solo_kill': 100,
}


def _award_web_legacy(discord_user_id: str, action_type: str, ref_id: str, source: str = 'discord') -> int:
    """
    Award Legacy points to the unified QuestLog profile for a linked Discord user.
    Looks up web_users by discord_id (not fluxer_id).
    Returns points awarded (0 if not linked, duplicate, or unknown action).
    """
    if not ref_id:
        logger.error(f"_award_web_legacy called without ref_id for action={action_type}, refusing")
        return 0

    pts = LEGACY_POINTS.get(action_type)
    if pts is None:
        logger.warning(f"_award_web_legacy: unknown action_type '{action_type}'")
        return 0

    try:
        with db_session_scope() as db:
            row = db.execute(
                sa_text("SELECT id FROM web_users WHERE discord_id = :did AND is_banned = 0 LIMIT 1"),
                {"did": discord_user_id},
            ).fetchone()
            if not row:
                return 0  # Not linked or banned

            web_user_id = row[0]

            # Dedup check
            dup = db.execute(
                sa_text(
                    "SELECT id FROM web_legacy_events "
                    "WHERE user_id = :uid AND action_type = :at AND ref_id = :ref LIMIT 1"
                ),
                {"uid": web_user_id, "at": action_type, "ref": ref_id},
            ).fetchone()
            if dup:
                return 0

            now = int(time.time())
            db.execute(
                sa_text(
                    "INSERT INTO web_legacy_events "
                    "(user_id, action_type, points, source, ref_id, created_at) "
                    "VALUES (:uid, :at, :pts, :src, :ref, :now)"
                ),
                {"uid": web_user_id, "at": action_type, "pts": pts, "src": source, "ref": ref_id, "now": now},
            )
            db.execute(
                sa_text("UPDATE web_users SET legacy_score = GREATEST(0, legacy_score + :pts) WHERE id = :uid"),
                {"pts": pts, "uid": web_user_id},
            )

            # Recalculate legacy tier
            score_row = db.execute(
                sa_text("SELECT legacy_score FROM web_users WHERE id = :uid"), {"uid": web_user_id}
            ).fetchone()
            if score_row:
                score = score_row[0] or 0
                if score >= 25000:
                    tier = 5
                elif score >= 7500:
                    tier = 4
                elif score >= 2000:
                    tier = 3
                elif score >= 500:
                    tier = 2
                else:
                    tier = 1
                db.execute(
                    sa_text("UPDATE web_users SET legacy_tier = :tier WHERE id = :uid"),
                    {"tier": tier, "uid": web_user_id},
                )
            db.commit()
            logger.info(f"_award_web_legacy: web_user {web_user_id} +{pts} for {action_type} ref={ref_id}")
            return pts
    except Exception as e:
        logger.warning(f"_award_web_legacy failed for discord_id={discord_user_id}: {e}")
    return 0


class LegacyCog(commands.Cog):
    """Legacy points for Discord server behavior: star reactions + clean record milestones."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._task: asyncio.Task | None = None

    async def _report_questlog_legacy(
        self, *, guild_id: int, user_id: int, action_type: str,
        evidence_id: str, occurred_at: int,
    ) -> int:
        api_cog = self.bot.get_cog("ProgressionAPICog")
        if not api_cog:
            logger.error(
                "QuestLog Legacy event could not be queued: canonical "
                "progression is enabled but ProgressionAPICog is unavailable"
            )
            return 0
        try:
            result = await api_cog.submit_event(
                user_id=user_id,
                guild_id=guild_id,
                event_type=action_type,
                evidence_id=evidence_id,
                occurred_at=occurred_at,
            )
            return result.awarded_legacy
        except Exception as error:
            logger.error(
                "QuestLog Legacy enqueue failed: guild=%s user=%s event=%s error=%s",
                guild_id, user_id, action_type, error,
            )
            return 0

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info("LegacyCog ready - starting clean record loop")
        if self._task is None or self._task.done():
            self._task = asyncio.ensure_future(self._clean_record_loop())

    # ------------------------------------------------------------------
    # Star reaction handler
    # ------------------------------------------------------------------

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        """Award Legacy when someone reacts ⭐ to another member's message."""
        if str(payload.emoji) != STAR_EMOJI:
            return

        if not payload.guild_id:
            return

        reactor_id = payload.user_id

        # Ignore bot reactions
        if self.bot.user and reactor_id == self.bot.user.id:
            return

        # Fetch the message to get the author
        try:
            channel = self.bot.get_channel(payload.channel_id)
            if channel is None:
                channel = await self.bot.fetch_channel(payload.channel_id)
            message = await channel.fetch_message(payload.message_id)
        except Exception as e:
            logger.debug(f"LegacyCog: could not fetch message {payload.message_id}: {e}")
            return

        if not message or not message.author or message.author.bot:
            return

        author_id = message.author.id

        # Don't award self-stars
        if author_id == reactor_id:
            return

        ref_id = f"star_{payload.message_id}_{reactor_id}"

        if QUESTLOG_PROGRESSION_API_ENABLED:
            pts = await self._report_questlog_legacy(
                guild_id=payload.guild_id,
                user_id=author_id,
                action_type='comment_helpful',
                evidence_id=ref_id,
                occurred_at=int(time.time()),
            )
        else:
            pts = _award_web_legacy(str(author_id), 'comment_helpful', ref_id=ref_id)
        if pts:
            logger.debug(f"LegacyCog: ⭐ {reactor_id} -> {author_id} +{pts} legacy (msg {payload.message_id})")

    # ------------------------------------------------------------------
    # Clean record background loop
    # ------------------------------------------------------------------

    async def _clean_record_loop(self):
        await asyncio.sleep(60)  # brief startup delay
        while True:
            try:
                pending = await asyncio.get_event_loop().run_in_executor(
                    None, self._check_clean_records
                )
                for guild_id, discord_user_id, action_type, evidence_id, observed_at in pending:
                    pts = await self._report_questlog_legacy(
                        guild_id=guild_id,
                        user_id=int(discord_user_id),
                        action_type=action_type,
                        evidence_id=evidence_id,
                        occurred_at=observed_at,
                    )
                    if pts:
                        logger.info(
                            "LegacyCog: %s -> discord_user %s +%s through QuestLog",
                            action_type, discord_user_id, pts,
                        )
            except Exception as e:
                logger.error(f"LegacyCog: clean record loop error: {e}")
            await asyncio.sleep(CHECK_INTERVAL)

    def _check_clean_records(self):
        """Sync: find members past 30/60/90 day milestones with no negative legacy events."""
        now = int(time.time())
        day_secs = 86400

        try:
            with db_session_scope() as db:
                # Find all linked members via discord_id
                rows = db.execute(
                    sa_text(
                        "SELECT gm.user_id, gm.first_seen, wu.id AS web_user_id, gm.guild_id "
                        "FROM guild_members gm "
                        "JOIN web_users wu ON wu.discord_id = CAST(gm.user_id AS CHAR) COLLATE utf8mb4_unicode_ci "
                        "WHERE gm.first_seen IS NOT NULL "
                        "  AND gm.left_at IS NULL "
                        "  AND wu.is_banned = 0"
                    )
                ).fetchall()
        except Exception as e:
            logger.error(f"LegacyCog: clean record DB fetch failed: {e}")
            return []

        awarded = 0
        pending = []
        for row in rows:
            discord_user_id = str(row[0])
            joined_at = row[1]
            web_user_id = row[2]
            guild_id = int(row[3])

            if not joined_at:
                continue

            days_member = (now - int(joined_at)) // day_secs

            for days_required, action_type in CLEAN_RECORD_MILESTONES:
                if days_member < days_required:
                    continue

                ref_id = f"{action_type}_{web_user_id}"

                try:
                    with db_session_scope() as db:
                        # Already awarded?
                        already = db.execute(
                            sa_text(
                                "SELECT id FROM web_legacy_events "
                                "WHERE user_id = :uid AND action_type = :act LIMIT 1"
                            ),
                            {"uid": web_user_id, "act": action_type},
                        ).fetchone()
                        if already:
                            continue

                        # Any negative events since joining?
                        neg = db.execute(
                            sa_text(
                                "SELECT id FROM web_legacy_events "
                                "WHERE user_id = :uid "
                                "  AND action_type IN ('report_upheld', 'temp_ban') "
                                "  AND created_at >= :joined "
                                "LIMIT 1"
                            ),
                            {"uid": web_user_id, "joined": int(joined_at)},
                        ).fetchone()
                        if neg:
                            continue
                except Exception as e:
                    logger.error(f"LegacyCog: check failed for user {web_user_id}: {e}")
                    continue

                if QUESTLOG_PROGRESSION_API_ENABLED:
                    pending.append((
                        guild_id, discord_user_id, action_type, ref_id, now,
                    ))
                    continue

                pts = _award_web_legacy(discord_user_id, action_type, ref_id=ref_id)
                if pts:
                    awarded += 1
                    logger.info(f"LegacyCog: {action_type} -> web_user {web_user_id} +{pts}")

        if awarded:
            logger.info(f"LegacyCog: clean record pass complete - awarded {awarded} milestones")
        return pending


def setup(bot: commands.Bot):
    bot.add_cog(LegacyCog(bot))
