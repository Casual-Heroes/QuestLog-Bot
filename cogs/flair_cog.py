# cogs/flair_cog.py - Flair Store System
"""
Flair Store Cog for QuestLog.
Allows members to purchase and equip flairs using Hero Tokens.
Flairs are displayed as Discord roles with the "Flair: " prefix.

Features:
- Normal Flairs: 11 options ranging from 0-250 tokens
- Seasonal Flairs: 13 options at 50 tokens each
- Discord UI with select dropdowns
- Token-based economy
- Role management with automatic cleanup of old flairs
"""

import discord
from discord import ui, Embed, Interaction
from discord.ext import commands
from discord.commands import SlashCommandGroup

from config import db_session_scope, logger, get_debug_guilds
from models import GuildMember, Guild

# Define available Normal Flairs (flair_name: cost)
NORMAL_FLAIRS = {
    "[🕹️ Gamer]": 0,  # Default, free
    "[💼 Professional Noob]": 10,
    "[🎮 Casual Legend]": 15,
    "[🍕 AFK Snacker]": 30,
    "[☕ Coffee & Cooldowns]": 50,
    "[👑 Boss Fight Survivor]": 65,
    "[🛋️ Couch Commander]": 80,
    "[📅 Weekend Warrior]": 100,
    "[👑 Boss Fight Survivor]": 140,
    "[🏆 Achievement Hunter]": 170,
    "[🚀 Noob to Pro]": 200,
    "[🌟 Gaming OG]": 250,
}

# Define available Seasonal Flairs (flair_name: cost)
SEASONAL_FLAIRS = {
    "[🎃 Pumpkin Spice Gamer]": 50,
    "[❄️ Winter Warrior]": 50,
    "[🕎 Festival of Games]": 50,
    "[🎁 Casual Claus]": 50,
    "[🎮 New Year, New Game]": 50,
    "[🍀 Lucky Loot Goblin]": 50,
    "[🌸 Cherry Blossom Grinder]": 50,
    "[🥚 Eggstra Tryhard]": 50,
    "[🌞 Sunburned Speedrunner]": 50,
    "[🎆 Casual Fireworks]": 50,
    "[🍗 Turkey Time]": 50,
    "[🏴‍☠️ Pirate Booty Hoarder]": 50,
    "[👻 Ghost in the Game]": 50,
}


def get_flair_role(guild: discord.Guild, flair_name: str) -> discord.Role:
    """
    Builds the Discord role name as: Flair: {flair_name}
    Then retrieves that role from the guild.
    e.g., if flair_name = "[🎮 Casual Legend]", role_name = "Flair: [🎮 Casual Legend]"
    """
    role_name = f"Flair: {flair_name}"
    return discord.utils.get(guild.roles, name=role_name)


async def assign_flair_role(member: discord.Member, flair_name: str):
    """
    Removes any old flair roles from the user and assigns the new one (if found).
    The new role is "Flair: {flair_name}".
    Example: "Flair: [🎮 Casual Legend]"
    """
    # Remove any existing flair roles that start with "Flair: "
    old_flair_roles = [r for r in member.roles if r.name.startswith("Flair: ")]
    if old_flair_roles:
        await member.remove_roles(*old_flair_roles, reason="Removing old flair roles")
        logger.debug(f"Removed old flair roles from {member.display_name}: {[r.name for r in old_flair_roles]}")

    # Find the new role
    new_role = get_flair_role(member.guild, flair_name)
    if new_role:
        await member.add_roles(new_role, reason=f"Assigned flair role {new_role.name}")
        logger.info(f"Assigned flair {new_role.name} to {member.display_name}")
    else:
        logger.warning(f"No role named 'Flair: {flair_name}' found in {member.guild.name}.")


class NormalFlairSelect(ui.Select):
    """Dropdown selector for Normal Flairs."""

    def __init__(self, token_name: str = "Hero Tokens"):
        self.token_name = token_name
        # Build dropdown options
        options = []
        for flair_name, cost in NORMAL_FLAIRS.items():
            options.append(discord.SelectOption(
                label=flair_name,
                description=f"Cost: {cost} {token_name}",
                value=flair_name
            ))
        super().__init__(
            placeholder="Choose your Normal Flair...",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="normal_flair_select"
        )

    async def callback(self, interaction: Interaction):
        """Handle flair selection and purchase."""
        flair_name = self.values[0]
        cost = NORMAL_FLAIRS[flair_name]

        try:
            with db_session_scope() as session:
                # Get member record
                member_record = session.get(GuildMember, (interaction.guild_id, interaction.user.id))
                if not member_record:
                    await interaction.response.send_message(
                        "You don't have an XP record yet! Please interact in the server first.",
                        ephemeral=True
                    )
                    return

                # Check if user has enough tokens
                if member_record.hero_tokens < cost:
                    await interaction.response.send_message(
                        f"Sorry, you need {cost} {self.token_name} for **{flair_name}**, "
                        f"but you only have {member_record.hero_tokens}.",
                        ephemeral=True
                    )
                    return

                # Deduct tokens if necessary
                if cost > 0:
                    member_record.hero_tokens -= cost
                    logger.info(
                        f"{interaction.user.display_name} purchased {flair_name} "
                        f"for {cost} tokens in {interaction.guild.name}"
                    )

                # Store the flair in DB
                member_record.flair = flair_name

                # Session commit happens automatically via context manager

            # Assign the flair role
            member = interaction.guild.get_member(interaction.user.id)
            if member:
                await assign_flair_role(member, flair_name)

            await interaction.response.send_message(
                f"Success! Your Normal Flair is now **{flair_name}**. Check your roles!",
                ephemeral=True
            )

        except Exception as e:
            logger.error(f"Error setting Normal Flair for {interaction.user.display_name}: {e}", exc_info=True)
            await interaction.response.send_message(
                "An error occurred while setting your flair. Please try again later.",
                ephemeral=True
            )


class SeasonalFlairSelect(ui.Select):
    """Dropdown selector for Seasonal Flairs."""

    def __init__(self, token_name: str = "Hero Tokens"):
        self.token_name = token_name
        options = []
        for flair_name, cost in SEASONAL_FLAIRS.items():
            options.append(discord.SelectOption(
                label=flair_name,
                description=f"Cost: {cost} {token_name}",
                value=flair_name
            ))
        super().__init__(
            placeholder="Choose your Seasonal Flair...",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="seasonal_flair_select"
        )

    async def callback(self, interaction: Interaction):
        """Handle seasonal flair selection and purchase."""
        flair_name = self.values[0]
        cost = SEASONAL_FLAIRS[flair_name]

        try:
            with db_session_scope() as session:
                # Get member record
                member_record = session.get(GuildMember, (interaction.guild_id, interaction.user.id))
                if not member_record:
                    await interaction.response.send_message(
                        "You don't have an XP record yet! Please interact in the server first.",
                        ephemeral=True
                    )
                    return

                # Check if user has enough tokens
                if member_record.hero_tokens < cost:
                    await interaction.response.send_message(
                        f"Sorry, you need {cost} {self.token_name} for **{flair_name}**, "
                        f"but you only have {member_record.hero_tokens}.",
                        ephemeral=True
                    )
                    return

                # Deduct tokens
                member_record.hero_tokens -= cost
                logger.info(
                    f"{interaction.user.display_name} purchased {flair_name} "
                    f"for {cost} tokens in {interaction.guild.name}"
                )

                # Store the flair in DB
                member_record.flair = flair_name

                # Session commit happens automatically via context manager

            # Assign the flair role
            member = interaction.guild.get_member(interaction.user.id)
            if member:
                await assign_flair_role(member, flair_name)

            await interaction.response.send_message(
                f"Success! Your Seasonal Flair is now **{flair_name}**. Check your roles!",
                ephemeral=True
            )

        except Exception as e:
            logger.error(f"Error setting Seasonal Flair for {interaction.user.display_name}: {e}", exc_info=True)
            await interaction.response.send_message(
                "An error occurred while setting your seasonal flair. Please try again later.",
                ephemeral=True
            )


class FlairView(ui.View):
    """View containing both Normal and Seasonal flair selectors."""

    def __init__(self, token_name: str = "Hero Tokens"):
        super().__init__(timeout=300)  # 5 minute timeout
        self.add_item(NormalFlairSelect(token_name))
        self.add_item(SeasonalFlairSelect(token_name))


class FlairCog(commands.Cog):
    """Flair Store - Let members customize their profile with flairs."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        logger.info("✅ Flair Store Cog loaded")

    flair = SlashCommandGroup(
        name="flair",
        description="Flair store commands",
    )

    @flair.command(
        name="store",
        description="View and purchase profile flairs using tokens",
    )
    async def flairstore(self, ctx: discord.ApplicationContext):
        """Display the flair store with all available flairs."""
        try:
            # Get guild settings for token name
            with db_session_scope() as session:
                guild_record = session.query(Guild).filter(Guild.guild_id == ctx.guild_id).first()
                token_name = guild_record.token_name if guild_record and guild_record.token_name else "Hero Tokens"

            # Build the description
            description_lines = []
            description_lines.append("**Normal Flairs:**")
            for flair_name, cost in NORMAL_FLAIRS.items():
                description_lines.append(f"**{flair_name}** - {cost} {token_name}")

            description_lines.append("\n**Seasonal Flairs:**")
            for flair_name, cost in SEASONAL_FLAIRS.items():
                description_lines.append(f"**{flair_name}** - {cost} {token_name}")

            # Create embed
            embed = Embed(
                title="🎨 Flair Store",
                description="\n".join(description_lines),
                color=discord.Color.blurple()
            )
            embed.set_footer(text="Select your flair from the dropdowns below.")

            # Create view with dropdowns
            view = FlairView(token_name)
            await ctx.respond(embed=embed, view=view, ephemeral=True)
            logger.info(f"{ctx.author.display_name} opened the flair store in {ctx.guild.name}")

        except Exception as e:
            logger.error(f"Error showing flair store: {e}", exc_info=True)
            await ctx.respond(
                "An error occurred while loading the flair store. Please try again later.",
                ephemeral=True
            )

    @flair.command(
        name="current",
        description="View your current flair",
    )
    async def current_flair(self, ctx: discord.ApplicationContext):
        """Show the user's currently equipped flair."""
        try:
            with db_session_scope() as session:
                member_record = session.get(GuildMember, (ctx.guild_id, ctx.author.id))
                if not member_record or not member_record.flair:
                    await ctx.respond(
                        "You don't have a flair equipped yet! Use `/flair store` to get one.",
                        ephemeral=True
                    )
                    return

                # Get guild settings for token name
                guild_record = session.query(Guild).filter(Guild.guild_id == ctx.guild_id).first()
                token_name = guild_record.token_name if guild_record and guild_record.token_name else "Hero Tokens"

                embed = Embed(
                    title="Your Current Flair",
                    description=f"**{member_record.flair}**",
                    color=discord.Color.green()
                )
                embed.add_field(name=f"🪙 {token_name}", value=f"{member_record.hero_tokens}")
                await ctx.respond(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Error showing current flair: {e}", exc_info=True)
            await ctx.respond(
                "An error occurred while retrieving your flair.",
                ephemeral=True
            )

    @flair.command(
        name="remove",
        description="Remove your current flair",
    )
    async def remove_flair(self, ctx: discord.ApplicationContext):
        """Remove the user's equipped flair and flair role."""
        try:
            with db_session_scope() as session:
                member_record = session.get(GuildMember, (ctx.guild_id, ctx.author.id))
                if not member_record or not member_record.flair:
                    await ctx.respond(
                        "You don't have a flair equipped!",
                        ephemeral=True
                    )
                    return

                # Clear flair from DB
                old_flair = member_record.flair
                member_record.flair = None

            # Remove flair roles
            member = ctx.guild.get_member(ctx.author.id)
            if member:
                old_flair_roles = [r for r in member.roles if r.name.startswith("Flair: ")]
                if old_flair_roles:
                    await member.remove_roles(*old_flair_roles, reason="User removed flair")

            await ctx.respond(
                f"Your flair **{old_flair}** has been removed.",
                ephemeral=True
            )
            logger.info(f"{ctx.author.display_name} removed their flair in {ctx.guild.name}")

        except Exception as e:
            logger.error(f"Error removing flair: {e}", exc_info=True)
            await ctx.respond(
                "An error occurred while removing your flair.",
                ephemeral=True
            )


def setup(bot: commands.Bot):
    bot.add_cog(FlairCog(bot))
