# cogs/core.py - Core Bot Events & Commands
"""
Core cog for QuestLog.
Handles basic bot events and essential commands.
"""

import time
import discord
from discord.ext import commands
from discord import SlashCommandGroup

from config import (
    db_session_scope,
    logger,
    IS_PRODUCTION,
    get_debug_guilds,
)
from models import Guild, GuildMember

# URLs (centralized for easy updating)
WEBSITE_URL = "https://casual-heroes.com/questlog/overview/"
SUPPORT_INVITE = "https://discord.gg/exRgR9YGyy"  # Casual Heroes Hosting Services (Support)
DASHBOARD_URL = "https://dashboard.casual-heroes.com/questlog/"


class CoreCog(commands.Cog):
    """Core bot functionality - events and basic commands."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # Slash command groups
    questlog = SlashCommandGroup(
        name="questlog",
        description="QuestLog commands",

    )

    # Basic commands
    @discord.slash_command(name="ping", description="Check if QuestLog is responsive")
    async def ping(self, ctx: discord.ApplicationContext):
        """Check bot latency."""
        latency = round(self.bot.latency * 1000)
        await ctx.respond(f"🏓 Pong! Latency: **{latency}ms**", ephemeral=True)

    @discord.slash_command(name="help", description="Get help with QuestLog commands")
    async def help(self, ctx: discord.ApplicationContext):
        """Show help information."""
        embed = discord.Embed(
            title="🛡️ QuestLog Help",
            description="Your all-in-one Discord security & engagement platform.",
            color=discord.Color.brand_green()
        )

        # Free features
        embed.add_field(
            name="🆓 Free Features",
            value=(
                "`/questlog setup` - Quick setup wizard\n"
                "`/xp profile` - View your XP & level\n"
                "`/xp leaderboard` - Server leaderboard\n"
                "`/verify me` - Verify yourself\n"
                "`/roles menu` - Self-assign roles"
            ),
            inline=True
        )

        # Security features
        embed.add_field(
            name="🔒 Security (Mods)",
            value=(
                "`/raid status` - Check raid status\n"
                "`/raid lockdown` - Lock server\n"
                "`/audit search` - Search audit logs\n"
                "`/raid config` - Security settings"
            ),
            inline=True
        )

        # Engagement features
        embed.add_field(
            name="🎮 Engagement",
            value=(
                "`/promo post` - Self-promotion\n"
                "`/promo featured` - Enter featured pool\n"
                "`/discovery browse` - Browse creators\n"
                "`/lfg create` - Create LFG events"
            ),
            inline=True
        )

        # Admin
        embed.add_field(
            name="⚙️ Admin",
            value=f"[Web Dashboard]({DASHBOARD_URL}) - Manage settings",
            inline=False
        )

        embed.set_footer(text=f"💡 Need more help? Join {SUPPORT_INVITE}")

        await ctx.respond(embed=embed, ephemeral=True)

    @discord.slash_command(name="info", description="View QuestLog information")
    async def info(self, ctx: discord.ApplicationContext):
        """Show bot information and stats."""
        uptime_seconds = int(time.time() - self.bot.start_time) if self.bot.start_time else 0
        hours, remainder = divmod(uptime_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        embed = discord.Embed(
            title="🛡️ QuestLog",
            description="All-in-one Discord security & engagement platform for gaming communities.",
            color=discord.Color.brand_green()
        )

        embed.add_field(
            name="📊 Stats",
            value=(
                f"Servers: **{len(self.bot.guilds)}**\n"
                f"Uptime: **{hours}h {minutes}m {seconds}s**\n"
                f"Latency: **{round(self.bot.latency * 1000)}ms**"
            ),
            inline=True
        )

        embed.add_field(
            name="🔗 Links",
            value=(
                f"[Website]({WEBSITE_URL})\n"
                f"[Support Server]({SUPPORT_INVITE})\n"
                f"[Dashboard]({DASHBOARD_URL})"
            ),
            inline=True
        )

        embed.set_thumbnail(url=self.bot.user.display_avatar.url)
        embed.set_footer(text=f"Version 0.1.0 | Pycord {discord.__version__}")

        await ctx.respond(embed=embed, ephemeral=True)

    @discord.slash_command(name="status", description="View server status")
    async def status(self, ctx: discord.ApplicationContext):
        """Show guild status."""
        with db_session_scope() as session:
            guild = session.get(Guild, ctx.guild.id)

            if not guild:
                await ctx.respond("❌ Guild not found in database.", ephemeral=True)
                return

            # Count members in DB
            member_count = (
                session.query(GuildMember)
                .filter(GuildMember.guild_id == ctx.guild.id)
                .count()
            )

            embed = discord.Embed(
                title=f"🏠 {ctx.guild.name}",
                color=discord.Color.blurple()
            )

            embed.add_field(
                name="📊 Status",
                value=(
                    f"Members Tracked: **{member_count}**\n"
                    f"XP System: **{'✅ Enabled' if guild.xp_enabled else '❌ Disabled'}**\n"
                    f"Anti-Raid: **{'✅ Enabled' if guild.anti_raid_enabled else '❌ Disabled'}**"
                ),
                inline=True
            )

            # Feature status
            features = []
            if guild.xp_enabled:
                features.append("✅ XP & Leveling")
            if guild.anti_raid_enabled:
                features.append("✅ Anti-Raid Protection")
            if guild.verification_enabled:
                features.append("✅ Verification")
            if guild.audit_logging_enabled:
                features.append("✅ Audit Logging")
            if guild.discovery_enabled:
                features.append("✅ Discovery Network")

            embed.add_field(
                name="🔧 Features",
                value="\n".join(features) if features else "None enabled",
                inline=True
            )

        await ctx.respond(embed=embed, ephemeral=True)

    # Guild setup command
    @questlog.command(name="setup", description="Quick setup wizard for QuestLog")
    @discord.default_permissions(administrator=True)
    @commands.has_permissions(administrator=True)
    async def setup(self, ctx: discord.ApplicationContext):
        """Interactive setup wizard."""
        # Defer to prevent 3-second timeout
        await ctx.defer(ephemeral=True)

        # TODO: Implement interactive setup with buttons/modals
        embed = discord.Embed(
            title="⚙️ QuestLog Setup",
            description="Let's get QuestLog configured for your server!",
            color=discord.Color.brand_green()
        )

        embed.add_field(
            name="📊 Step 1: Configure from Dashboard",
            value=(
                f"Visit the [Dashboard]({DASHBOARD_URL}guild/{ctx.guild.id}) to:\n"
                "• Set up notification channels\n"
                "• Configure verification settings\n"
                "• Enable/disable modules"
            ),
            inline=False
        )

        embed.add_field(
            name="🎮 Step 2: Test Features",
            value=(
                "Try these commands:\n"
                "• `/xp profile` - Check XP system\n"
                "• `/verify me` - Test verification\n"
                "• `/roles menu` - Set up self-roles"
            ),
            inline=False
        )

        embed.add_field(
            name="🔒 Step 3: Security (Moderators)",
            value=(
                "Configure security:\n"
                "• `/raid config` - Anti-raid settings\n"
                "• `/raid status` - Check protection\n"
                "• `/audit search` - Review logs"
            ),
            inline=False
        )

        embed.set_footer(text=f"Need help? Use /help or join {SUPPORT_INVITE}")

        await ctx.followup.send(embed=embed, ephemeral=True)


def setup(bot: commands.Bot):
    """Load the cog."""
    bot.add_cog(CoreCog(bot))
