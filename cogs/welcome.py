# cogs/welcome.py - Welcome & Goodbye Messages
"""
Welcome message system for Warden bot.

Features:
- Customizable welcome messages in channel
- Optional DM welcome messages
- Goodbye messages when members leave
- Variable substitution: {user}, {username}, {server}, {member_count}
- Embed support with custom colors and thumbnails
- Auto-role on join
"""

import time
from datetime import datetime, timezone

import discord
from discord.ext import commands

from config import db_session_scope, logger, get_debug_guilds
from models import Guild, GuildMember, WelcomeConfig


# Available variables for message templates
WELCOME_VARIABLES = {
    "{user}": "User mention (@username)",
    "{username}": "Username without mention",
    "{discriminator}": "User's discriminator (if any)",
    "{user_id}": "User's ID",
    "{server}": "Server name",
    "{member_count}": "Total member count",
    "{member_count_ord}": "Member count with ordinal (1st, 2nd, etc.)",
    "{created_at}": "When the account was created",
    "{avatar_url}": "User's avatar URL",
}


def ordinal(n: int) -> str:
    """Convert number to ordinal string (1st, 2nd, 3rd, etc.)."""
    if 11 <= (n % 100) <= 13:
        suffix = "th"
    else:
        suffix = ["th", "st", "nd", "rd", "th"][min(n % 10, 4)]
    return f"{n}{suffix}"


def format_message(template: str, member: discord.Member) -> str:
    """Format a message template with member/guild variables."""
    guild = member.guild
    member_count = guild.member_count or len(guild.members)

    display_name = member.display_name or member.name
    replacements = {
        "{user}": member.mention,           # mention (legacy/default)
        "{username}": display_name,         # plain display name
        "{discriminator}": member.discriminator if member.discriminator != "0" else "",
        "{user_id}": str(member.id),
        "{server}": guild.name,
        "{member_count}": str(member_count),
        "{member_count_ord}": ordinal(member_count),
        "{created_at}": f"<t:{int(member.created_at.timestamp())}:R>",
        "{avatar_url}": member.display_avatar.url,
    }

    result = template
    for var, value in replacements.items():
        result = result.replace(var, value)

    return result


class WelcomeCog(commands.Cog):
    """Welcome and goodbye message system."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # Slash command group
    welcome = discord.SlashCommandGroup(
        name="welcome",
        description="Welcome message configuration",
        
    )

    def get_welcome_config(self, session, guild_id: int) -> WelcomeConfig:
        """Get or create welcome config for a guild."""
        config = session.get(WelcomeConfig, guild_id)
        if not config:
            config = WelcomeConfig(guild_id=guild_id)
            session.add(config)
            session.flush()
        return config

    # Event listeners

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """Send welcome message when a member joins."""
        if member.bot:
            return

        guild = member.guild

        with db_session_scope() as session:
            db_guild = session.get(Guild, guild.id)
            if not db_guild:
                return

            config = self.get_welcome_config(session, guild.id)
            if not config.enabled:
                return

            # Store config values before session closes
            channel_enabled = config.channel_message_enabled
            channel_message = config.channel_message
            embed_enabled = config.channel_embed_enabled
            embed_title = config.channel_embed_title
            embed_color = config.channel_embed_color
            embed_thumbnail = config.channel_embed_thumbnail
            embed_footer = config.channel_embed_footer
            dm_enabled = config.dm_enabled
            dm_message = config.dm_message
            auto_role_id = config.auto_role_id
            welcome_channel_id = db_guild.welcome_channel_id

        # Send channel welcome message (text or forum)
        if channel_enabled and welcome_channel_id:
            welcome_channel = guild.get_channel(welcome_channel_id)
            try:
                formatted_message = format_message(channel_message, member)

                async def send_payload(dest_channel):
                    if embed_enabled:
                        embed = discord.Embed(
                            title=format_message(embed_title, member) if embed_title else None,
                            description=formatted_message,
                            color=discord.Color(embed_color),
                            timestamp=datetime.now(timezone.utc)
                        )
                        if embed_thumbnail:
                            embed.set_thumbnail(url=member.display_avatar.url)
                        if embed_footer:
                            embed.set_footer(text=format_message(embed_footer, member))
                        await dest_channel.send(embed=embed)
                    else:
                        await dest_channel.send(formatted_message)

                if isinstance(welcome_channel, discord.TextChannel):
                    await send_payload(welcome_channel)
                elif isinstance(welcome_channel, discord.ForumChannel):
                    thread = await welcome_channel.create_thread(
                        name=f"Welcome **{member.display_name or member.name}**",
                        content=None
                    )
                    await send_payload(thread)
                else:
                    logger.warning(f"Welcome channel {welcome_channel_id} is not text/forum in guild {guild.id}")

            except discord.Forbidden:
                logger.warning(f"Cannot send welcome message in {guild.name} - missing permissions")
            except Exception as e:
                logger.error(f"Error sending welcome message: {e}")

        # Send DM welcome message
        if dm_enabled and dm_message:
            try:
                formatted_dm = format_message(dm_message, member)
                embed = discord.Embed(
                    description=formatted_dm,
                    color=discord.Color.blurple()
                )
                embed.set_author(name=guild.name, icon_url=guild.icon.url if guild.icon else None)
                await member.send(embed=embed)
            except discord.Forbidden:
                pass  # User has DMs disabled
            except Exception as e:
                logger.warning(f"Failed to DM welcome to {member}: {e}")

        # Auto-role
        if auto_role_id:
            auto_role = guild.get_role(auto_role_id)
            if auto_role:
                try:
                    await member.add_roles(auto_role, reason="Auto-role on join")
                except discord.Forbidden:
                    logger.warning(f"Cannot assign auto-role in {guild.name}")

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Send goodbye message when a member leaves."""
        if member.bot:
            return

        guild = member.guild

        with db_session_scope() as session:
            db_guild = session.get(Guild, guild.id)
            if not db_guild:
                return

            config = self.get_welcome_config(session, guild.id)
            if not config.enabled or not config.goodbye_enabled:
                return

            goodbye_message = config.goodbye_message
            goodbye_channel_id = config.goodbye_channel_id or db_guild.welcome_channel_id  # Fallback to welcome channel

        if not goodbye_channel_id or not goodbye_message:
            return

        goodbye_channel = guild.get_channel(goodbye_channel_id)
        if not goodbye_channel or not isinstance(goodbye_channel, discord.TextChannel):
            return

        try:
            # Format goodbye message (can't use mention since they left)
            formatted = goodbye_message.replace("{user}", f"**{member.display_name}**")
            formatted = formatted.replace("{username}", member.display_name)
            formatted = formatted.replace("{server}", guild.name)
            formatted = formatted.replace("{member_count}", str(guild.member_count or len(guild.members)))

            embed = discord.Embed(
                description=formatted,
                color=discord.Color.greyple(),
                timestamp=datetime.now(timezone.utc)
            )

            await goodbye_channel.send(embed=embed)
        except discord.Forbidden:
            pass
        except Exception as e:
            logger.error(f"Error sending goodbye message: {e}")

    # Slash commands

    @welcome.command(name="test", description="Test the welcome message")
    @commands.has_permissions(manage_guild=True)
    async def welcome_test(self, ctx: discord.ApplicationContext):
        """Test welcome message by sending it for yourself."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)
            if not db_guild or not db_guild.welcome_channel_id:
                await ctx.respond(
                    "Welcome channel not set. Use `/settings channel welcome #channel` first.",
                    ephemeral=True
                )
                return

            config = self.get_welcome_config(session, ctx.guild.id)

            channel_message = config.channel_message
            embed_enabled = config.channel_embed_enabled
            embed_title = config.channel_embed_title
            embed_color = config.channel_embed_color
            embed_thumbnail = config.channel_embed_thumbnail
            embed_footer = config.channel_embed_footer
            welcome_channel_id = db_guild.welcome_channel_id

        welcome_channel = ctx.guild.get_channel(welcome_channel_id)
        if not welcome_channel:
            await ctx.respond("Welcome channel not found.", ephemeral=True)
            return

        formatted_message = format_message(channel_message, ctx.author)

        if embed_enabled:
            embed = discord.Embed(
                title=format_message(embed_title, ctx.author) if embed_title else None,
                description=formatted_message,
                color=discord.Color(embed_color),
                timestamp=datetime.now(timezone.utc)
            )
            if embed_thumbnail:
                embed.set_thumbnail(url=ctx.author.display_avatar.url)
            if embed_footer:
                embed.set_footer(text=format_message(embed_footer, ctx.author))

            await welcome_channel.send(content="**[TEST]** This is a test welcome message:", embed=embed)
        else:
            await welcome_channel.send(f"**[TEST]** {formatted_message}")

        await ctx.respond(f"Test welcome message sent to {welcome_channel.mention}!", ephemeral=True)

    @welcome.command(name="config", description="Configure welcome messages")
    @commands.has_permissions(manage_guild=True)
    @discord.option("enabled", bool, description="Enable/disable welcome messages", required=False)
    @discord.option("channel_message", bool, description="Enable channel welcome message", required=False)
    @discord.option("dm_message", bool, description="Enable DM welcome message", required=False)
    @discord.option("goodbye", bool, description="Enable goodbye messages", required=False)
    @discord.option("embed", bool, description="Use embed for channel message", required=False)
    async def welcome_config(self, ctx: discord.ApplicationContext,
                              enabled: bool = None,
                              channel_message: bool = None,
                              dm_message: bool = None,
                              goodbye: bool = None,
                              embed: bool = None):
        """Configure welcome message settings."""
        with db_session_scope() as session:
            config = self.get_welcome_config(session, ctx.guild.id)

            if enabled is not None:
                config.enabled = enabled
            if channel_message is not None:
                config.channel_message_enabled = channel_message
            if dm_message is not None:
                config.dm_enabled = dm_message
            if goodbye is not None:
                config.goodbye_enabled = goodbye
            if embed is not None:
                config.channel_embed_enabled = embed

            config.updated_at = int(time.time())

            # Build status embed
            status_embed = discord.Embed(
                title="Welcome Message Configuration",
                color=discord.Color.blurple()
            )

            status_embed.add_field(
                name="Status",
                value=f"{'Enabled' if config.enabled else 'Disabled'}",
                inline=True
            )

            status_embed.add_field(
                name="Channel Message",
                value=f"{'Enabled' if config.channel_message_enabled else 'Disabled'}",
                inline=True
            )

            status_embed.add_field(
                name="Use Embed",
                value=f"{'Yes' if config.channel_embed_enabled else 'No'}",
                inline=True
            )

            status_embed.add_field(
                name="DM Message",
                value=f"{'Enabled' if config.dm_enabled else 'Disabled'}",
                inline=True
            )

            status_embed.add_field(
                name="Goodbye Message",
                value=f"{'Enabled' if config.goodbye_enabled else 'Disabled'}",
                inline=True
            )

        await ctx.respond(embed=status_embed, ephemeral=True)

    @welcome.command(name="set-message", description="Set the welcome message")
    @commands.has_permissions(manage_guild=True)
    @discord.option("message_type", str, description="Which message to set",
                    choices=["channel", "dm", "goodbye"])
    @discord.option("message", str, description="The message (use {user}, {server}, {member_count})")
    async def welcome_set_message(self, ctx: discord.ApplicationContext, message_type: str, message: str):
        """Set welcome/goodbye message content."""
        with db_session_scope() as session:
            config = self.get_welcome_config(session, ctx.guild.id)

            if message_type == "channel":
                config.channel_message = message
            elif message_type == "dm":
                config.dm_message = message
            elif message_type == "goodbye":
                config.goodbye_message = message

            config.updated_at = int(time.time())

        # Show preview
        preview = format_message(message, ctx.author)

        embed = discord.Embed(
            title=f"Updated {message_type.title()} Message",
            color=discord.Color.green()
        )
        embed.add_field(name="Template", value=message[:1024], inline=False)
        embed.add_field(name="Preview", value=preview[:1024], inline=False)

        await ctx.respond(embed=embed, ephemeral=True)

    @welcome.command(name="set-embed", description="Configure welcome embed appearance")
    @commands.has_permissions(manage_guild=True)
    @discord.option("title", str, description="Embed title", required=False)
    @discord.option("color", str, description="Hex color (e.g., #5865F2)", required=False)
    @discord.option("footer", str, description="Footer text", required=False)
    @discord.option("show_avatar", bool, description="Show user avatar as thumbnail", required=False)
    async def welcome_set_embed(self, ctx: discord.ApplicationContext,
                                 title: str = None,
                                 color: str = None,
                                 footer: str = None,
                                 show_avatar: bool = None):
        """Configure welcome embed appearance."""
        with db_session_scope() as session:
            config = self.get_welcome_config(session, ctx.guild.id)

            if title is not None:
                config.channel_embed_title = title if title else None
            if color is not None:
                # Parse hex color
                try:
                    color_int = int(color.replace("#", ""), 16)
                    config.channel_embed_color = color_int
                except ValueError:
                    await ctx.respond("Invalid color format. Use hex like #5865F2.", ephemeral=True)
                    return
            if footer is not None:
                config.channel_embed_footer = footer if footer else None
            if show_avatar is not None:
                config.channel_embed_thumbnail = show_avatar

            config.updated_at = int(time.time())

            # Show preview
            embed = discord.Embed(
                title=format_message(config.channel_embed_title, ctx.author) if config.channel_embed_title else None,
                description=format_message(config.channel_message, ctx.author),
                color=discord.Color(config.channel_embed_color)
            )
            if config.channel_embed_thumbnail:
                embed.set_thumbnail(url=ctx.author.display_avatar.url)
            if config.channel_embed_footer:
                embed.set_footer(text=format_message(config.channel_embed_footer, ctx.author))

        await ctx.respond("Embed settings updated! Preview:", embed=embed, ephemeral=True)

    @welcome.command(name="auto-role", description="Set a role to auto-assign on join")
    @commands.has_permissions(manage_roles=True)
    @discord.option("role", discord.Role, description="Role to auto-assign (leave empty to disable)", required=False)
    async def welcome_auto_role(self, ctx: discord.ApplicationContext, role: discord.Role = None):
        """Set auto-role for new members."""
        with db_session_scope() as session:
            config = self.get_welcome_config(session, ctx.guild.id)

            if role:
                # Check if bot can assign this role
                if role >= ctx.guild.me.top_role:
                    await ctx.respond("I cannot assign a role that is higher than my top role.", ephemeral=True)
                    return

                config.auto_role_id = role.id
                await ctx.respond(f"Auto-role set to {role.mention}. New members will receive this role.", ephemeral=True)
            else:
                config.auto_role_id = None
                await ctx.respond("Auto-role disabled.", ephemeral=True)

    @welcome.command(name="variables", description="Show available message variables")
    async def welcome_variables(self, ctx: discord.ApplicationContext):
        """Show available variables for welcome messages."""
        embed = discord.Embed(
            title="Welcome Message Variables",
            description="Use these in your welcome/goodbye messages:",
            color=discord.Color.blurple()
        )

        var_list = "\n".join([f"`{var}` - {desc}" for var, desc in WELCOME_VARIABLES.items()])
        embed.add_field(name="Available Variables", value=var_list, inline=False)

        embed.add_field(
            name="Example",
            value=(
                "**Template:** Welcome to **{server}**, {user}! You are our {member_count_ord} member!\n\n"
                f"**Result:** Welcome to **{ctx.guild.name}**, {ctx.author.mention}! "
                f"You are our {ordinal(ctx.guild.member_count)}th member!"
            ),
            inline=False
        )

        await ctx.respond(embed=embed, ephemeral=True)

    @welcome.command(name="status", description="View current welcome configuration")
    @commands.has_permissions(manage_guild=True)
    async def welcome_status(self, ctx: discord.ApplicationContext):
        """View current welcome message configuration."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)
            config = self.get_welcome_config(session, ctx.guild.id)

            embed = discord.Embed(
                title="Welcome System Status",
                color=discord.Color.green() if config.enabled else discord.Color.red()
            )

            # Status
            embed.add_field(
                name="System Status",
                value=f"{'Enabled' if config.enabled else 'Disabled'}",
                inline=True
            )

            # Channel
            welcome_channel = f"<#{db_guild.welcome_channel_id}>" if db_guild and db_guild.welcome_channel_id else "Not set"
            embed.add_field(name="Welcome Channel", value=welcome_channel, inline=True)

            # Auto-role
            auto_role = f"<@&{config.auto_role_id}>" if config.auto_role_id else "Disabled"
            embed.add_field(name="Auto-Role", value=auto_role, inline=True)

            # Features
            features = []
            if config.channel_message_enabled:
                features.append(f"Channel Message {'(Embed)' if config.channel_embed_enabled else '(Text)'}")
            if config.dm_enabled:
                features.append("DM Message")
            if config.goodbye_enabled:
                features.append("Goodbye Message")

            embed.add_field(
                name="Active Features",
                value="\n".join(features) if features else "None",
                inline=False
            )

            # Messages preview
            embed.add_field(
                name="Channel Message Preview",
                value=config.channel_message[:200] + ("..." if len(config.channel_message) > 200 else ""),
                inline=False
            )

            if config.dm_enabled:
                embed.add_field(
                    name="DM Message Preview",
                    value=config.dm_message[:200] + ("..." if len(config.dm_message) > 200 else ""),
                    inline=False
                )

        await ctx.respond(embed=embed, ephemeral=True)


def setup(bot: commands.Bot):
    bot.add_cog(WelcomeCog(bot))
