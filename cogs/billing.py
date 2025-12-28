# cogs/billing.py - Stripe Billing & Subscriptions
"""
Stripe integration for QuestLog modular subscriptions.

Features:
- Webhook handler for subscription lifecycle events
- View subscription plan command
- All subscription management handled via dashboard

Webhook Events Handled:
- checkout.session.completed - New subscription started
- customer.subscription.updated - Plan changes, renewals
- customer.subscription.deleted - Cancellation
- invoice.payment_failed - Failed payment
- invoice.paid - Successful payment

Setup:
1. Set STRIPE_API_KEY and STRIPE_WEBHOOK_SECRET in environment
2. Configure webhook endpoint in Stripe Dashboard to point to your server
"""

import time
import json
from datetime import datetime, timezone

import discord
from discord.ext import commands

from config import (
    db_session_scope, logger, get_debug_guilds,
    STRIPE_API_KEY, STRIPE_WEBHOOK_SECRET, STRIPE_PRICES,
    Pricing, FeatureLimits
)
from models import Guild, Subscription, SubscriptionTier

# Import Stripe if available
try:
    import stripe
    # Check if we have a valid Stripe API key (not placeholder)
    if STRIPE_API_KEY and not STRIPE_API_KEY.endswith('_your_key_here'):
        stripe.api_key = STRIPE_API_KEY
        STRIPE_AVAILABLE = True
    else:
        STRIPE_AVAILABLE = False
        logger.warning("Stripe API key not configured - billing features disabled")
except ImportError:
    STRIPE_AVAILABLE = False
    stripe = None
    logger.warning("Stripe library not installed - billing features disabled")


class BillingCog(commands.Cog):
    """Stripe billing and subscription management."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # Slash command group
    billing = discord.SlashCommandGroup(
        name="billing",
        description="Subscription and billing commands",
        
    )

    # Helper methods

    def _tier_emoji(self, tier: str) -> str:
        """Get emoji for tier."""
        return {"FREE": "", "PREMIUM": "", "PRO": ""}.get(tier.upper(), "")

    async def _update_guild_subscription(self, guild_id: int, tier: SubscriptionTier,
                                          expires_at: int = None,
                                          stripe_customer_id: str = None,
                                          stripe_subscription_id: str = None):
        """Update guild subscription in database."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, guild_id)
            if not db_guild:
                logger.warning(f"Guild {guild_id} not found for subscription update")
                return False

            db_guild.subscription_tier = tier
            db_guild.subscription_expires = expires_at

            if stripe_customer_id:
                db_guild.stripe_customer_id = stripe_customer_id
            if stripe_subscription_id:
                db_guild.stripe_subscription_id = stripe_subscription_id

            logger.info(f"Updated guild {guild_id} to {tier.value} tier")
            return True

    async def _log_subscription_event(self, guild_id: int, event_type: str,
                                        details: str = None):
        """Log subscription event to database."""
        with db_session_scope() as session:
            sub = Subscription(
                guild_id=guild_id,
                tier=SubscriptionTier.COMPLETE.value,
                started_at=int(time.time()),
            )
            # Just log for now - full tracking in Subscription table
            logger.info(f"Subscription event for guild {guild_id}: {event_type} - {details}")

    # Webhook handler (called from external web server)

    async def handle_stripe_webhook(self, payload: bytes, sig_header: str) -> dict:
        """
        Handle incoming Stripe webhook event.

        This should be called from your web server (e.g., aiohttp, FastAPI).

        Args:
            payload: Raw request body bytes
            sig_header: Stripe-Signature header value

        Returns:
            dict with 'success' and 'message' keys
        """
        if not STRIPE_AVAILABLE:
            return {"success": False, "message": "Stripe not configured"}

        try:
            event = stripe.Webhook.construct_event(
                payload, sig_header, STRIPE_WEBHOOK_SECRET
            )
        except ValueError:
            return {"success": False, "message": "Invalid payload"}
        except stripe.error.SignatureVerificationError:
            return {"success": False, "message": "Invalid signature"}

        event_type = event["type"]
        data = event["data"]["object"]

        logger.info(f"Stripe webhook received: {event_type}")

        # Handle different event types
        if event_type == "checkout.session.completed":
            await self._handle_checkout_completed(data)

        elif event_type == "customer.subscription.updated":
            await self._handle_subscription_updated(data)

        elif event_type == "customer.subscription.deleted":
            await self._handle_subscription_deleted(data)

        elif event_type == "invoice.payment_failed":
            await self._handle_payment_failed(data)

        elif event_type == "invoice.paid":
            await self._handle_invoice_paid(data)

        return {"success": True, "message": f"Handled {event_type}"}

    async def _handle_checkout_completed(self, session: dict):
        """Handle successful checkout session."""
        # Get metadata with guild_id
        metadata = session.get("metadata", {})
        guild_id = metadata.get("guild_id")

        if not guild_id:
            logger.warning("Checkout completed without guild_id in metadata")
            return

        guild_id = int(guild_id)
        customer_id = session.get("customer")
        subscription_id = session.get("subscription")
        mode = session.get("mode")

        if mode == "subscription":
            # Recurring subscription
            sub = stripe.Subscription.retrieve(subscription_id)
            price_id = sub["items"]["data"][0]["price"]["id"]

            # All subscriptions are now Complete tier
            tier = SubscriptionTier.COMPLETE.value

            expires_at = sub["current_period_end"]

            await self._update_guild_subscription(
                guild_id, tier, expires_at, customer_id, subscription_id
            )

            # DM the guild owner
            await self._notify_subscription_start(guild_id, tier)

        elif mode == "payment":
            # One-time payment (lifetime)
            await self._update_guild_subscription(
                guild_id, SubscriptionTier.COMPLETE.value,
                expires_at=None,  # Never expires
                stripe_customer_id=customer_id
            )

            # Mark as lifetime in database
            with db_session_scope() as session:
                db_guild = session.get(Guild, guild_id)
                if db_guild:
                    db_guild.billing_cycle = 'lifetime'
                    db_guild.vip_note = "Lifetime Complete purchase"
                    db_guild.vip_granted_at = int(time.time())

            await self._notify_subscription_start(guild_id, SubscriptionTier.COMPLETE.value, lifetime=True)

    async def _handle_subscription_updated(self, subscription: dict):
        """Handle subscription updates (upgrades, downgrades, renewals)."""
        customer_id = subscription.get("customer")

        # Find guild by customer ID
        with db_session_scope() as session:
            db_guild = (
                session.query(Guild)
                .filter(Guild.stripe_customer_id == customer_id)
                .first()
            )
            if not db_guild:
                logger.warning(f"No guild found for customer {customer_id}")
                return
            guild_id = db_guild.guild_id

        # All subscriptions are now Complete tier
        tier = SubscriptionTier.COMPLETE.value

        expires_at = subscription["current_period_end"]

        await self._update_guild_subscription(
            guild_id, tier, expires_at,
            stripe_subscription_id=subscription["id"]
        )

    async def _handle_subscription_deleted(self, subscription: dict):
        """Handle subscription cancellation."""
        customer_id = subscription.get("customer")

        with db_session_scope() as session:
            db_guild = (
                session.query(Guild)
                .filter(Guild.stripe_customer_id == customer_id)
                .first()
            )
            if not db_guild:
                return
            guild_id = db_guild.guild_id

        # Downgrade to free tier
        await self._update_guild_subscription(guild_id, SubscriptionTier.FREE.value)

        # Notify guild owner
        await self._notify_subscription_end(guild_id)

    async def _handle_payment_failed(self, invoice: dict):
        """Handle failed payment."""
        customer_id = invoice.get("customer")

        with db_session_scope() as session:
            db_guild = (
                session.query(Guild)
                .filter(Guild.stripe_customer_id == customer_id)
                .first()
            )
            if not db_guild:
                return
            guild_id = db_guild.guild_id

        # Notify guild owner about failed payment
        await self._notify_payment_failed(guild_id, invoice.get("hosted_invoice_url"))

    async def _handle_invoice_paid(self, invoice: dict):
        """Handle successful payment (renewal)."""
        customer_id = invoice.get("customer")
        subscription_id = invoice.get("subscription")

        if not subscription_id:
            return  # Not a subscription invoice

        with db_session_scope() as session:
            db_guild = (
                session.query(Guild)
                .filter(Guild.stripe_customer_id == customer_id)
                .first()
            )
            if not db_guild:
                return

            # Update expiration from subscription
            sub = stripe.Subscription.retrieve(subscription_id)
            db_guild.subscription_expires = sub["current_period_end"]

    # Notification helpers

    async def _notify_subscription_start(self, guild_id: int, tier: SubscriptionTier,
                                          lifetime: bool = False):
        """Notify guild owner about new subscription."""
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return

        owner = guild.owner
        if not owner:
            return

        embed = discord.Embed(
            title=f"{self._tier_emoji(tier.value)} Subscription Activated!",
            description=(
                f"**{guild.name}** has been upgraded to **{tier.value.title()}**"
                + (" (Lifetime)" if lifetime else "") + "!"
            ),
            color=discord.Color.gold()
        )

        # All Complete tier features
        features = [
            "All 5 premium modules included",
            "Unlimited XP tracking",
            "Unlimited reaction roles",
            "XP Boost Events",
            "Full moderation suite",
            "Discovery & promotions",
            "LFG & event tracking",
            "Role management",
            "Advanced customization",
        ]

        if features:
            embed.add_field(
                name="Unlocked Features",
                value="\n".join(f"{f}" for f in features),
                inline=False
            )

        try:
            await owner.send(embed=embed)
        except discord.Forbidden:
            pass

    async def _notify_subscription_end(self, guild_id: int):
        """Notify guild owner about subscription end."""
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return

        owner = guild.owner
        if not owner:
            return

        embed = discord.Embed(
            title="Subscription Ended",
            description=(
                f"Your subscription for **{guild.name}** has ended.\n\n"
                "The server has been downgraded to the **Free** tier. "
                "All your data is preserved, but some features are now limited.\n\n"
                f"[Reactivate your subscription](https://questlog.gg/guild/{guild_id}/billing)"
            ),
            color=discord.Color.red()
        )

        try:
            await owner.send(embed=embed)
        except discord.Forbidden:
            pass

    async def _notify_payment_failed(self, guild_id: int, invoice_url: str = None):
        """Notify guild owner about failed payment."""
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return

        owner = guild.owner
        if not owner:
            return

        embed = discord.Embed(
            title="Payment Failed",
            description=(
                f"We couldn't process your payment for **{guild.name}**.\n\n"
                "Please update your payment method to keep your subscription active."
            ),
            color=discord.Color.red()
        )

        if invoice_url:
            embed.add_field(
                name="Pay Invoice",
                value=f"[Click here to pay]({invoice_url})",
                inline=False
            )

        try:
            await owner.send(embed=embed)
        except discord.Forbidden:
            pass

    # Slash commands

    @billing.command(name="plan", description="View your server's subscription plan")
    @discord.default_permissions(manage_guild=True)
    @commands.has_permissions(manage_guild=True)
    async def billing_plan(self, ctx: discord.ApplicationContext):
        """View current subscription plan and active modules."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)

            if not db_guild:
                await ctx.respond("Guild not found in database.", ephemeral=True)
                return

            # Get active modules
            active_modules = []
            if db_guild.engagement_enabled:
                active_modules.append("Engagement Suite")
            if db_guild.roles_enabled:
                active_modules.append("Role Management")
            if db_guild.mod_enabled:
                active_modules.append("Moderation & Security")
            if db_guild.discovery_enabled:
                active_modules.append("Discovery & Promotion")
            if db_guild.lfg_enabled:
                active_modules.append("Events & Attendance")

            # Check if complete bundle (all 5 modules)
            has_complete = len(active_modules) == 5

            # Get billing cycle and expiration
            billing_cycle = db_guild.billing_cycle or "monthly"
            expires = db_guild.subscription_expires
            is_lifetime = billing_cycle == "lifetime" or db_guild.is_vip

            embed = discord.Embed(
                title="📋 Your Subscription Plan",
                description=(
                    f"**{ctx.guild.name}** is using QuestLog's modular subscription system.\n"
                    f"Visit the [dashboard](https://questlog.gg/guild/{ctx.guild.id}/billing) to manage your subscription."
                ),
                color=discord.Color.gold() if active_modules else discord.Color.greyple()
            )

            # Current plan
            if has_complete:
                plan_name = "Complete Suite"
                if is_lifetime:
                    plan_value = "Complete Suite (Lifetime)"
                else:
                    cycle_display = billing_cycle.replace("3month", "3-Month").replace("6month", "6-Month").title()
                    plan_value = f"Complete Suite ({cycle_display})"
            elif active_modules:
                plan_name = f"{len(active_modules)} Module{'s' if len(active_modules) > 1 else ''}"
                plan_value = ", ".join(active_modules)
            else:
                plan_name = "Free Tier"
                plan_value = "No paid modules active"

            embed.add_field(
                name="Current Plan",
                value=plan_value,
                inline=False
            )

            # Expiration/renewal
            if expires and not is_lifetime:
                if expires > int(time.time()):
                    embed.add_field(
                        name="Renews",
                        value=f"<t:{expires}:R>",
                        inline=True
                    )
                else:
                    embed.add_field(
                        name="Status",
                        value=f"Expired <t:{expires}:R>",
                        inline=True
                    )
            elif is_lifetime:
                embed.add_field(
                    name="Expires",
                    value="Never (Lifetime)",
                    inline=True
                )

            # Active modules list
            if active_modules:
                modules_text = "\n".join([f"✅ {module}" for module in active_modules])
                embed.add_field(
                    name=f"Active Modules ({len(active_modules)}/5)",
                    value=modules_text,
                    inline=False
                )

            # Manage subscription button
            view = discord.ui.View()
            view.add_item(discord.ui.Button(
                label="Manage Subscription",
                url=f"https://questlog.gg/guild/{ctx.guild.id}/billing",
                style=discord.ButtonStyle.link,
                emoji="💳"
            ))

            if not active_modules:
                embed.add_field(
                    name="Get Started",
                    value="Visit the dashboard to subscribe and unlock premium features!",
                    inline=False
                )

        await ctx.respond(embed=embed, view=view, ephemeral=True)


def setup(bot: commands.Bot):
    bot.add_cog(BillingCog(bot))
