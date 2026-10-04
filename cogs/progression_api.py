"""Lifecycle owner for Warden's QuestLog progression API client."""

import asyncio
import time

from discord.ext import commands, tasks

from config import (
    QUESTLOG_PROGRESSION_API_TOKEN,
    QUESTLOG_PROGRESSION_API_URL,
    db_session_scope,
    logger,
)
from utils.progression_api import ProgressionAPIClient
from utils.progression_outbox import ProgressionOutbox


class ProgressionAPICog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.client = ProgressionAPIClient(
            QUESTLOG_PROGRESSION_API_URL,
            QUESTLOG_PROGRESSION_API_TOKEN,
        )
        self.outbox = ProgressionOutbox(self.client, db_session_scope, logger)
        self._last_prune_at = 0
        if self.client.configuration_error:
            logger.error("QuestLog progression API configuration error: %s", self.client.configuration_error)
        elif self.client.configured:
            logger.info("QuestLog progression API client configured")
        else:
            logger.warning(
                "QuestLog progression API is not configured; unified Discord "
                "XP evidence will remain in the durable retry queue"
            )
        self.retry_progression_outbox.start()

    async def submit_event(
        self, *, user_id, guild_id, event_type, evidence_id, occurred_at
    ):
        """Persist an event before attempting immediate QuestLog delivery."""
        return await self.outbox.submit(
            user_id=user_id,
            guild_id=guild_id,
            event_type=event_type,
            evidence_id=evidence_id,
            occurred_at=occurred_at,
        )

    @tasks.loop(seconds=30)
    async def retry_progression_outbox(self):
        try:
            keys = self.outbox.due_event_keys(limit=50)
            for event_key in keys:
                await self.outbox.deliver(event_key)

            now = int(time.time())
            if now - self._last_prune_at >= 24 * 60 * 60:
                removed = self.outbox.prune_delivered(now=now)
                self._last_prune_at = now
                if removed:
                    logger.info(
                        "Pruned %s delivered QuestLog progression outbox rows",
                        removed,
                    )
        except Exception:
            logger.exception(
                "QuestLog progression outbox retry pass failed; queued rows "
                "remain durable for the next pass"
            )

    @retry_progression_outbox.before_loop
    async def before_retry_progression_outbox(self):
        await self.bot.wait_until_ready()

    def cog_unload(self):
        self.retry_progression_outbox.cancel()
        asyncio.create_task(self.client.close())


def setup(bot):
    bot.add_cog(ProgressionAPICog(bot))
