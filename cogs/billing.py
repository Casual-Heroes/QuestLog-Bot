# cogs/billing.py - Stripe Billing & Subscriptions
"""
Stripe integration for Warden bot subscriptions.

Features:
- Checkout session creation for Premium/Pro upgrades
- Webhook handler for subscription lifecycle events
- Subscription management commands
- Lifetime purchase handling

Webhook Events Handled:
- checkout.session.completed - New subscription started
- customer.subscription.updated - Plan changes, renewals
- customer.subscription.deleted - Cancellation
- invoice.payment_failed - Failed payment
- invoice.paid - Successful payment

Setup:
1. Set STRIPE_API_KEY and STRIPE_WEBHOOK_SECRET in environment
2. Set STRIPE_PRICE_* environment variables for each plan
3. Configure webhook endpoint in Stripe Dashboard to point to your server
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
    stripe.api_key = STRIPE_API_KEY
    STRIPE_AVAILABLE = bool(STRIPE_API_KEY)
except ImportError:
    STRIPE_AVAILABLE = False
    stripe = None


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
                "Use `/billing upgrade` to reactivate your subscription!"
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

    @billing.command(name="status", description="View your server's subscription status")
    @commands.has_permissions(manage_guild=True)
    async def billing_status(self, ctx: discord.ApplicationContext):
        """View subscription status."""
        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)

            if not db_guild:
                await ctx.respond("Guild not found in database.", ephemeral=True)
                return

            tier = db_guild.subscription_tier.upper() if db_guild.subscription_tier else "FREE"
            is_vip = db_guild.is_vip
            expires = db_guild.subscription_expires

            embed = discord.Embed(
                title=f"{self._tier_emoji(tier)} Subscription Status",
                color=discord.Color.gold() if tier != "FREE" else discord.Color.greyple()
            )

            embed.add_field(
                name="Current Plan",
                value=f"**{tier}**" + (" (VIP)" if is_vip else ""),
                inline=True
            )

            if expires and not is_vip:
                expires_dt = datetime.fromtimestamp(expires, tz=timezone.utc)
                if expires > int(time.time()):
                    embed.add_field(
                        name="Renews",
                        value=f"<t:{expires}:R>",
                        inline=True
                    )
                else:
                    embed.add_field(
                        name="Expired",
                        value=f"<t:{expires}:R>",
                        inline=True
                    )
            elif is_vip:
                embed.add_field(
                    name="Expires",
                    value="Never (Lifetime)",
                    inline=True
                )

            # Show limits
            limits = FeatureLimits.get_limits(tier)
            key_limits = [
                ("Bulk Operations", limits.get("bulk_users_per_action") or "Unlimited"),
                ("Audit Log Retention", f"{limits.get('mod_log_days', 7)} days"),
                ("Daily Self-Promo", limits.get("self_promo_per_day") or "Unlimited"),
                ("Templates", limits.get("templates") or "Unlimited"),
            ]

            limit_text = "\n".join([f"**{k}:** {v}" for k, v in key_limits])
            embed.add_field(name="Current Limits", value=limit_text, inline=False)

            if tier == "FREE":
                embed.add_field(
                    name="Upgrade",
                    value="Use `/billing upgrade` to unlock more features!",
                    inline=False
                )

        await ctx.respond(embed=embed, ephemeral=True)

    @billing.command(name="plans", description="View available subscription plans")
    async def billing_plans(self, ctx: discord.ApplicationContext):
        """View available plans and pricing."""
        embed = discord.Embed(
            title="Warden Subscription Plans",
            description="All features available on all tiers - only quantities differ!",
            color=discord.Color.gold()
        )

        # Free tier
        embed.add_field(
            name=" FREE",
            value=(
                "**$0/month**\n"
                "10 users per bulk op\n"
                "5 templates\n"
                "7-day audit logs\n"
                "2 self-promo/day"
            ),
            inline=True
        )

        # Premium tier
        embed.add_field(
            name=" PREMIUM",
            value=(
                f"**${Pricing.PREMIUM_MONTHLY}/month**\n"
                f"*${Pricing.PREMIUM_YEARLY}/year (30% off)*\n\n"
                "100 users per bulk op\n"
                "25 templates\n"
                "30-day audit logs\n"
                "10 self-promo/day\n"
                "Featured pool access\n"
                "Game server sync"
            ),
            inline=True
        )

        # Pro tier
        embed.add_field(
            name=" PRO",
            value=(
                f"**${Pricing.PRO_MONTHLY}/month**\n"
                f"*${Pricing.PRO_YEARLY}/year (30% off)*\n"
                f"*${Pricing.PRO_LIFETIME} lifetime*\n\n"
                "**UNLIMITED** everything\n"
                "90-day audit logs\n"
                "Discovery network\n"
                "Custom branding\n"
                "API access\n"
                "Priority support"
            ),
            inline=True
        )

        # Discounts
        discount_text = "\n".join([
            " **Veterans/First Responders:** 30-35% off",
            " **Yearly billing:** 30% off",
            " **Game server bundle:** 40% off (stacks with yearly!)",
        ])
        embed.add_field(name="Available Discounts", value=discount_text, inline=False)

        embed.set_footer(text="Use /billing upgrade to get started!")

        await ctx.respond(embed=embed, ephemeral=True)

    @billing.command(name="upgrade", description="Upgrade your server's subscription")
    @commands.has_permissions(administrator=True)
    @discord.option("plan", str, description="Plan to upgrade to",
                    choices=["premium_monthly", "premium_yearly", "pro_monthly", "pro_yearly", "pro_lifetime"])
    async def billing_upgrade(self, ctx: discord.ApplicationContext, plan: str):
        """Create Stripe checkout session for upgrade."""
        if not STRIPE_AVAILABLE:
            await ctx.respond(
                "Billing is not configured. Please contact the bot owner.",
                ephemeral=True
            )
            return

        price_id = STRIPE_PRICES.get(plan.replace("pro_lifetime", "lifetime"))
        if not price_id:
            await ctx.respond("Invalid plan selected.", ephemeral=True)
            return

        # Check if already subscribed
        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)
            if db_guild and db_guild.is_vip:
                await ctx.respond("This server already has lifetime access!", ephemeral=True)
                return

        await ctx.defer(ephemeral=True)

        try:
            # Create checkout session
            mode = "payment" if "lifetime" in plan else "subscription"

            checkout_session = stripe.checkout.Session.create(
                payment_method_types=["card"],
                line_items=[{"price": price_id, "quantity": 1}],
                mode=mode,
                success_url=f"https://discord.com/channels/{ctx.guild.id}?payment=success",
                cancel_url=f"https://discord.com/channels/{ctx.guild.id}?payment=cancelled",
                metadata={
                    "guild_id": str(ctx.guild.id),
                    "guild_name": ctx.guild.name,
                    "user_id": str(ctx.author.id),
                    "plan": plan,
                },
                customer_email=None,  # Let Stripe collect
                allow_promotion_codes=True,
            )

            embed = discord.Embed(
                title="Complete Your Purchase",
                description=(
                    f"Click the button below to upgrade **{ctx.guild.name}** to **{plan.replace('_', ' ').title()}**!\n\n"
                    "You'll be redirected to our secure payment page."
                ),
                color=discord.Color.gold()
            )

            # Create button view
            view = discord.ui.View()
            view.add_item(discord.ui.Button(
                label="Proceed to Checkout",
                url=checkout_session.url,
                style=discord.ButtonStyle.link
            ))

            await ctx.respond(embed=embed, view=view, ephemeral=True)

        except stripe.error.StripeError as e:
            logger.error(f"Stripe error creating checkout: {e}")
            await ctx.respond(
                f"Error creating checkout session: {e.user_message or 'Unknown error'}",
                ephemeral=True
            )

    @billing.command(name="manage", description="Manage your subscription (billing portal)")
    @commands.has_permissions(administrator=True)
    async def billing_manage(self, ctx: discord.ApplicationContext):
        """Open Stripe billing portal for subscription management."""
        if not STRIPE_AVAILABLE:
            await ctx.respond("Billing is not configured.", ephemeral=True)
            return

        with db_session_scope() as session:
            db_guild = session.get(Guild, ctx.guild.id)
            if not db_guild or not db_guild.stripe_customer_id:
                await ctx.respond(
                    "No billing account found. Use `/billing upgrade` first.",
                    ephemeral=True
                )
                return
            customer_id = db_guild.stripe_customer_id

        await ctx.defer(ephemeral=True)

        try:
            portal_session = stripe.billing_portal.Session.create(
                customer=customer_id,
                return_url=f"https://discord.com/channels/{ctx.guild.id}",
            )

            embed = discord.Embed(
                title="Manage Your Subscription",
                description=(
                    "Click below to open the billing portal where you can:\n"
                    " Update payment method\n"
                    " Change plan\n"
                    " View invoices\n"
                    " Cancel subscription"
                ),
                color=discord.Color.blurple()
            )

            view = discord.ui.View()
            view.add_item(discord.ui.Button(
                label="Open Billing Portal",
                url=portal_session.url,
                style=discord.ButtonStyle.link
            ))

            await ctx.respond(embed=embed, view=view, ephemeral=True)

        except stripe.error.StripeError as e:
            logger.error(f"Stripe error creating portal session: {e}")
            await ctx.respond(f"Error: {e.user_message or 'Unknown error'}", ephemeral=True)

    @billing.command(name="cancel", description="Cancel your subscription")
    @commands.has_permissions(administrator=True)
    async def billing_cancel(self, ctx: discord.ApplicationContext):
        """Cancel subscription (redirects to billing portal)."""
        # Just redirect to manage portal
        await self.billing_manage(ctx)


def setup(bot: commands.Bot):
    bot.add_cog(BillingCog(bot))
