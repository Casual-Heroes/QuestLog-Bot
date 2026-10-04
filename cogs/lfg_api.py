"""Lifecycle owner for Warden's shared canonical LFG API client."""

import asyncio

from discord.ext import commands

from config import QUESTLOG_LFG_API_TOKEN, QUESTLOG_LFG_API_URL, logger
from utils.lfg_api import LFGAPIClient


class LFGAPICog(commands.Cog):
    """Expose one shared LFG API client to Discord adapter cogs."""

    def __init__(self, bot):
        self.bot = bot
        self.client = LFGAPIClient(
            QUESTLOG_LFG_API_URL,
            QUESTLOG_LFG_API_TOKEN,
        )
        if self.client.configuration_error:
            logger.error(
                "Canonical QuestLog LFG API configuration error: %s",
                self.client.configuration_error,
            )
        elif self.client.configured:
            logger.info("Canonical QuestLog LFG API client configured")
        else:
            logger.warning(
                "Canonical QuestLog LFG API client is not configured; "
                "legacy compatibility mode remains available"
            )

    def cog_unload(self):
        asyncio.create_task(self.client.close())


def setup(bot: commands.Bot):
    bot.add_cog(LFGAPICog(bot))
