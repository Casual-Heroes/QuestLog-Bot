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
  4. This cog polls every 10 seconds, fetches pending rows, posts embeds,
     deletes processed rows

Setup command:
  /questlog-network setup channel:#lfg-channel
  /questlog-network status
  /questlog-network disable
"""

import json
import time
import asyncio

import discord
from discord.ext import commands, tasks
from sqlalchemy import text

from config import db_session_scope, logger


BRAND_COLOR = 0xFEE75C   # gold - matches the LFG embed color on the site
POLL_INTERVAL = 10        # seconds between DB polls
MAX_STALE_SECONDS = 300   # drop rows older than 5 minutes (bot was offline)


class NetworkBroadcastsCog(commands.Cog):
    """
    Receives QuestLog Network LFG broadcasts and posts them to Discord channels.
    """

    def __init__(self, bot):
        self.bot = bot
        self.poll_loop.start()
        logger.info("[NetworkBroadcasts] Cog loaded, poll loop starting.")

    def cog_unload(self):
        self.poll_loop.cancel()
        logger.info("[NetworkBroadcasts] Cog unloaded.")

    # ------------------------------------------------------------------
    # Background poll loop
    # ------------------------------------------------------------------

    @tasks.loop(seconds=POLL_INTERVAL)
    async def poll_loop(self):
        """Fetch pending broadcasts from DB and post to Discord channels."""
        try:
            await self._process_pending_broadcasts()
        except Exception as e:
            logger.error(f"[NetworkBroadcasts] poll_loop error: {e}", exc_info=True)

    @poll_loop.before_loop
    async def before_poll_loop(self):
        await self.bot.wait_until_ready()

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

            ids_to_delete = []

            for row in rows:
                row_id, guild_id, channel_id, payload_json, created_at = row

                # Always delete - either we post it or it's stale
                ids_to_delete.append(row_id)

                if created_at < stale_cutoff:
                    logger.debug(f"[NetworkBroadcasts] Dropping stale row {row_id} (age={(now_ts - created_at)}s)")
                    continue

                try:
                    embed_data = json.loads(payload_json)
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"[NetworkBroadcasts] Row {row_id} has invalid JSON payload, skipping")
                    continue

                # Post asynchronously - don't block the DB loop
                asyncio.create_task(
                    self._post_embed(row_id, guild_id, channel_id, embed_data)
                )

            if ids_to_delete:
                session.execute(
                    text("DELETE FROM discord_pending_broadcasts WHERE id IN :ids"),
                    {"ids": tuple(ids_to_delete)}
                )

    async def _post_embed(self, row_id, guild_id, channel_id, embed_data):
        """Build and post a Discord embed from the site payload."""
        try:
            guild = self.bot.get_guild(guild_id)
            if not guild:
                logger.debug(f"[NetworkBroadcasts] Guild {guild_id} not in cache, skipping row {row_id}")
                return

            channel = guild.get_channel(channel_id)
            if not channel or not isinstance(channel, discord.TextChannel):
                logger.debug(f"[NetworkBroadcasts] Channel {channel_id} not found in guild {guild_id}")
                return

            embed = discord.Embed(
                title=embed_data.get("title", "New LFG Post"),
                description=embed_data.get("description", ""),
                url=embed_data.get("url") or None,
                color=embed_data.get("color", BRAND_COLOR),
            )

            for field in embed_data.get("fields", []):
                embed.add_field(
                    name=field.get("name", ""),
                    value=field.get("value", ""),
                    inline=field.get("inline", True),
                )

            if embed_data.get("thumbnail"):
                embed.set_thumbnail(url=embed_data["thumbnail"])

            footer = embed_data.get("footer", "QuestLog Network")
            embed.set_footer(text=footer)

            await channel.send(embed=embed)
            logger.info(f"[NetworkBroadcasts] Posted LFG embed to {guild.name} #{channel.name}")

        except discord.Forbidden:
            logger.warning(f"[NetworkBroadcasts] No permission to post in channel {channel_id} (guild {guild_id})")
        except discord.HTTPException as e:
            logger.error(f"[NetworkBroadcasts] HTTP error posting to {channel_id}: {e}")
        except Exception as e:
            logger.error(f"[NetworkBroadcasts] Unexpected error posting row {row_id}: {e}", exc_info=True)

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
