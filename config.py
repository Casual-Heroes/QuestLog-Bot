# config.py - Configuration for Warden Bot (Multi-Tenant)
# Designed for 1000+ Discord guilds with MySQL + SQLAlchemy + Pycord

import os
import logging
from contextlib import contextmanager
from urllib.parse import quote_plus

import discord
from discord.ext import commands
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv

load_dotenv(override=True)  # Force override existing environment variables

# Logging setup
LOG_PATH = os.getenv("LOG_PATH", ".")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Configure logging handlers
import sys

# Global flag to ensure we only configure logging ONCE per process
_logging_configured = False

# Get root logger
root_logger = logging.getLogger()

# ONLY configure if not already done (prevents duplicates on re-import)
if not _logging_configured:
    # Clear ALL existing handlers from root logger AND all child loggers
    for logger_name in list(logging.Logger.manager.loggerDict.keys()):
        logging.getLogger(logger_name).handlers.clear()
    root_logger.handlers.clear()

    # Create stdout handler - systemd will redirect this to the log file
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(getattr(logging, LOG_LEVEL))
    stdout_formatter = logging.Formatter(
        '[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    stdout_handler.setFormatter(stdout_formatter)

    # Configure root logger with ONLY stdout handler
    # (systemd redirects stdout to log file, so no need for separate file handler)
    root_logger.setLevel(getattr(logging, LOG_LEVEL))
    root_logger.addHandler(stdout_handler)

    # Mark as configured
    _logging_configured = True

logger = logging.getLogger("warden")
# Prevent propagation duplicates by ensuring warden logger doesn't have its own handlers
logger.handlers.clear()
logger.propagate = True  # Ensure it propagates to root (which has our handlers)


# Database configuration
_engine = None
_session_factory = None


def get_database_url() -> str:
    """Build MySQL connection URL from environment variables."""
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT", "3306")
    DB_SOCKET = os.getenv("DB_SOCKET")  # Unix socket path
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME", "warden")

    if not all([DB_USERNAME, DB_PASSWORD]):
        raise ValueError(
            "Database connection details are not fully set. "
            "Please set DB_USERNAME and DB_PASSWORD environment variables."
        )

    encoded_password = quote_plus(DB_PASSWORD)

    # Use Unix socket if specified, otherwise use TCP
    if DB_SOCKET:
        return (
            f"mysql+mysqlconnector://{DB_USERNAME}:{encoded_password}"
            f"@/{DB_NAME}"
            f"?unix_socket={DB_SOCKET}&charset=utf8mb4&collation=utf8mb4_unicode_ci"
        )
    else:
        if not DB_HOST:
            raise ValueError("Either DB_HOST or DB_SOCKET must be set.")
        return (
            f"mysql+mysqlconnector://{DB_USERNAME}:{encoded_password}"
            f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
            f"?charset=utf8mb4&collation=utf8mb4_unicode_ci"
        )


def get_engine():
    """
    Get or create the database engine (Singleton).
    Optimized for 1000+ guilds with connection pooling.
    """
    global _engine
    if _engine is None:
        logger.info("Creating database engine...")

        _engine = create_engine(
            get_database_url(),
            echo=os.getenv("DB_ECHO", "false").lower() == "true",

            # Connection Pool Settings for Scale
            poolclass=QueuePool,
            pool_size=30,              # Base connections
            max_overflow=20,           # Extra connections under load (50 total max)
            pool_pre_ping=True,        # Verify connection health
            pool_recycle=1800,         # Recycle connections every 30 min
            pool_timeout=30,           # Wait 30s for connection before error

            # Connection settings
            connect_args={
                "connect_timeout": 10,
                "charset": "utf8mb4",
                "autocommit": False,
            }
        )

        # Log connection pool events for debugging
        @event.listens_for(_engine, "checkout")
        def receive_checkout(dbapi_connection, connection_record, connection_proxy):
            logger.debug("Connection checked out from pool")

        @event.listens_for(_engine, "checkin")
        def receive_checkin(dbapi_connection, connection_record):
            logger.debug("Connection returned to pool")

        logger.info("✅ Database engine created successfully")

    return _engine


def get_session_factory():
    """
    Get or create the session factory (Singleton).
    Uses scoped_session for thread-safety.
    """
    global _session_factory
    if _session_factory is None:
        engine = get_engine()
        _session_factory = scoped_session(
            sessionmaker(
                bind=engine,
                autocommit=False,
                autoflush=True,
                expire_on_commit=False  # Keep objects usable after commit
            )
        )
    return _session_factory


def get_db_session():
    """Get a new database session. Remember to close it when done!"""
    return get_session_factory()()


@contextmanager
def db_session_scope():
    """
    Context manager for database sessions.
    Auto-commits on success, rolls back on error, closes session.

    Usage:
        with db_session_scope() as session:
            member = session.get(GuildMember, (guild_id, user_id))
            member.xp += 10
            # Auto-commits on exit
    """
    session = get_db_session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Database transaction failed: {e}", exc_info=True)
        raise
    finally:
        session.close()


def init_database():
    """
    Initialize database tables. Call once on bot startup.
    """
    from models import Base

    engine = get_engine()
    logger.info("Creating database tables if they don't exist...")
    Base.metadata.create_all(engine)
    logger.info("✅ Database tables ready")

# Discord intents and bot setup
intents = discord.Intents.default()
intents.message_content = True
intents.members = True 
intents.messages = True 
intents.presences = True
intents.guilds = True
intents.guild_messages = True
intents.guild_reactions = True
intents.voice_states = True 


# start the bot
bot = commands.Bot(command_prefix="!", intents=intents)


# Bot configuration
def get_bot_token() -> str:
    """Get the Discord bot token from environment."""
    token = os.getenv("WARDEN_BOT_TOKEN") or os.getenv("DISCORD_TOKEN")
    if not token:
        raise ValueError(
            "Bot token not set. Please set WARDEN_BOT_TOKEN environment variable."
        )
    return token


# Feature limits (Freemium with Limits Model)
# All features available to all tiers, but with quantity limits
class FeatureLimits:
    """Feature limits by subscription tier. None = unlimited."""

    # Free tier - All features, limited quantities
    FREE = {
        # IAM & Bulk Operations
        "bulk_users_per_action": 10,       # Mass assign/remove roles
        "templates": 5,                     # Role + Channel templates combined
        "active_temp_roles": 10,            # Concurrent temp role assignments
        "export_members": 100,              # Members per CSV export

        # Audit & Logging
        "mod_log_days": 7,                  # Days of mod action history

        # Self-Promo & Discovery
        "self_promo_per_day": 2,            # Daily self-promo posts
        "featured_pool": False,             # Premium: Enter featured rotation
        "discovery_network": False,         # Pro: Cross-server discovery

        # Legacy/XP Settings
        "max_members": 2500,
        "react_roles": 10,
        "level_roles": 10,
        "xp_excluded_channels": 10,
        "xp_excluded_roles": 10,

        # Premium Features
        "game_server_sync": False,
        "analytics": False,
        "custom_branding": False,
        "api_access": False,
        "priority_support": False,
    }

    # Premium tier ($6.99/mo) - Higher limits, featured pool access
    PREMIUM = {
        # IAM & Bulk Operations
        "bulk_users_per_action": 100,
        "templates": 25,
        "active_temp_roles": 50,
        "export_members": 1000,

        # Audit & Logging
        "mod_log_days": 30,

        # Self-Promo & Discovery
        "self_promo_per_day": 10,
        "featured_pool": True,
        "discovery_network": False,

        # Legacy/XP Settings
        "max_members": None,
        "react_roles": None,
        "level_roles": None,
        "xp_excluded_channels": None,
        "xp_excluded_roles": None,

        # Premium Features
        "game_server_sync": True,
        "analytics": True,
        "custom_branding": False,
        "api_access": False,
        "priority_support": False,
    }

    # Pro tier ($12.99/mo) - Unlimited everything, cross-server discovery
    PRO = {
        # IAM & Bulk Operations
        "bulk_users_per_action": None,      # Unlimited
        "templates": None,
        "active_temp_roles": None,
        "export_members": None,

        # Audit & Logging
        "mod_log_days": 90,

        # Self-Promo & Discovery
        "self_promo_per_day": None,         # Unlimited
        "featured_pool": True,
        "discovery_network": True,          # Pro exclusive

        # Legacy/XP Settings
        "max_members": None,
        "react_roles": None,
        "level_roles": None,
        "xp_excluded_channels": None,
        "xp_excluded_roles": None,

        # Premium Features
        "game_server_sync": True,
        "analytics": True,
        "custom_branding": True,
        "api_access": True,
        "priority_support": True,
    }

    @classmethod
    def get_limits(cls, tier: str) -> dict:
        """Get limits for a subscription tier."""
        tier = tier.upper()
        if tier == "PRO":
            return cls.PRO
        elif tier == "PREMIUM":
            return cls.PREMIUM
        else:
            return cls.FREE

    @classmethod
    def get_limit(cls, tier: str, feature: str) -> int | bool | None:
        """Get a specific limit value for a tier."""
        limits = cls.get_limits(tier)
        return limits.get(feature)

    @classmethod
    def check_limit(cls, tier: str, feature: str, current_count: int) -> tuple[bool, int | None]:
        """
        Check if an action is within limits.

        Returns:
            tuple: (is_allowed, limit_value)
                - is_allowed: True if action is permitted
                - limit_value: The limit (None = unlimited)
        """
        limit = cls.get_limit(tier, feature)
        if limit is None:
            return (True, None)
        if isinstance(limit, bool):
            return (limit, limit)
        return (current_count < limit, limit)

    @classmethod
    def get_upgrade_message(cls, feature: str, current_tier: str) -> str:
        """Get a friendly upgrade message when a limit is hit."""
        current_limit = cls.get_limit(current_tier, feature)

        if current_tier.upper() == "FREE":
            premium_limit = cls.get_limit("PREMIUM", feature)
            if premium_limit is None:
                return f"Upgrade to Premium for unlimited {feature.replace('_', ' ')}!"
            return f"Upgrade to Premium for up to {premium_limit} {feature.replace('_', ' ')}!"
        elif current_tier.upper() == "PREMIUM":
            return f"Upgrade to Pro for unlimited {feature.replace('_', ' ')}!"
        return ""


# Pricing configuration (Aggressive Volume Play)
class Pricing:
    """
    Warden subscription pricing.
    Strategy: Aggressive pricing to capture market share, Warden as hook for game server sales.
    """

    # Base monthly prices
    PREMIUM_MONTHLY = 6.99
    PRO_MONTHLY = 12.99

    # Yearly pricing (30% off)
    YEARLY_DISCOUNT = 0.30
    PREMIUM_YEARLY = round(PREMIUM_MONTHLY * 12 * (1 - YEARLY_DISCOUNT), 2)  # $58.71
    PRO_YEARLY = round(PRO_MONTHLY * 12 * (1 - YEARLY_DISCOUNT), 2)          # $109.11

    # Lifetime (Pro only)
    PRO_LIFETIME = 79.99

    # Discount types (DO NOT STACK, except bundle)
    DISCOUNTS = {
        "yearly": 0.30,              # Built into yearly pricing
        "veterans": 0.35,            # Requires verification
        "first_responder": 0.30,     # Requires verification
        "birthday": 0.20,            # One-time yearly, requires DOB
        "holiday": 0.25,             # Black Friday, Cyber Monday, Christmas, etc.
        "flash_sale": 0.15,          # Random/urgency
        "game_server_bundle": 0.50,  # STACKS with yearly only
    }

    # Holidays that qualify for holiday discount
    HOLIDAY_EVENTS = [
        "black_friday",
        "cyber_monday",
        "christmas",
        "new_year",
        "presidents_day",
        "hanukkah",
        "july_4th",
    ]

    @classmethod
    def get_discounted_price(cls, base_price: float, discount_type: str) -> float:
        """Calculate discounted price."""
        discount = cls.DISCOUNTS.get(discount_type, 0)
        return round(base_price * (1 - discount), 2)

    @classmethod
    def get_bundle_price(cls, tier: str, yearly: bool = False) -> float:
        """Get price with game server bundle (50% off, stacks with yearly)."""
        if tier.upper() == "PREMIUM":
            base = cls.PREMIUM_YEARLY if yearly else cls.PREMIUM_MONTHLY
        else:
            base = cls.PRO_YEARLY if yearly else cls.PRO_MONTHLY

        return round(base * (1 - cls.DISCOUNTS["game_server_bundle"]), 2)


# Default XP settings
class DefaultXPSettings:
    """Default XP rates and cooldowns for new guilds."""

    # XP Rates (per action)
    MESSAGE_XP = 1.5
    MEDIA_MULTIPLIER = 1.3
    REACTION_XP = 1.0
    VOICE_XP_PER_INTERVAL = 1.3
    COMMAND_XP = 1.0
    GAMING_XP_PER_INTERVAL = 1.2
    INVITE_XP = 50.0
    JOIN_XP = 25.0

    # Token conversion
    TOKENS_PER_100_XP_ACTIVE = 15
    TOKENS_PER_100_XP_PASSIVE = 5

    # Cooldowns (in seconds)
    MESSAGE_COOLDOWN = 60
    MEDIA_COOLDOWN = 60
    REACTION_COOLDOWN = 60
    VOICE_INTERVAL = 5400      # 90 minutes
    GAMING_INTERVAL = 5400     # 90 minutes
    COMMAND_COOLDOWN = 60
    GAME_LAUNCH_COOLDOWN = 7200  # 2 hours

    # Level settings
    MAX_LEVEL = 99

    # Self-promo costs
    SELF_PROMO_COST = 0           # FREE for all members
    FEATURED_POOL_COST = 15       # PREMIUM: 15 tokens to enter featured pool
    FEATURED_DURATION_DAYS = 3    # Featured for 3 days


# Default raid protection settings
class DefaultRaidSettings:
    """Default anti-raid settings for new guilds."""

    MIN_ACCOUNT_AGE_DAYS = 7
    FLAG_NEW_ACCOUNTS = True
    AUTO_QUARANTINE_NEW_ACCOUNTS = False

    MASS_JOIN_THRESHOLD = 10       # X joins
    MASS_JOIN_WINDOW_SECONDS = 60  # in Y seconds
    MASS_JOIN_ACTION = "alert"     # alert, lockdown, quarantine

    JOIN_RATE_LIMIT = 30           # Max joins per minute
    AUTO_LOCKDOWN_ENABLED = False
    LOCKDOWN_DURATION_MINUTES = 30


# Default verification settings
class DefaultVerificationSettings:
    """Default verification settings for new guilds."""

    VERIFICATION_TYPE = "button"  # none, button, captcha, account_age, multi_step
    REQUIRE_ACCOUNT_AGE = True
    MIN_ACCOUNT_AGE_DAYS = 7

    BUTTON_TEXT = "✅ I agree to the rules"
    CAPTCHA_LENGTH = 6
    CAPTCHA_TIMEOUT_SECONDS = 300

    VERIFICATION_TIMEOUT_HOURS = 24
    KICK_ON_TIMEOUT = False


# Stripe configuration (Premium Subscriptions)
STRIPE_API_KEY = os.getenv("STRIPE_API_KEY")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")

# Price IDs from Stripe Dashboard
STRIPE_PRICES = {
    "premium_monthly": os.getenv("STRIPE_PRICE_PREMIUM_MONTHLY"),
    "premium_yearly": os.getenv("STRIPE_PRICE_PREMIUM_YEARLY"),
    "pro_monthly": os.getenv("STRIPE_PRICE_PRO_MONTHLY"),
    "pro_yearly": os.getenv("STRIPE_PRICE_PRO_YEARLY"),
    "lifetime": os.getenv("STRIPE_PRICE_LIFETIME"),
}


# Environment
IS_PRODUCTION = os.getenv("ENVIRONMENT", "development").lower() == "production"
DEBUG_GUILD_ID = int(os.getenv("DEBUG_GUILD_ID", 0)) if os.getenv("DEBUG_GUILD_ID") else None

# For development: limit slash commands to specific guild for faster sync
def get_debug_guilds():
    """Get list of guild IDs for debug slash command registration."""
    if DEBUG_GUILD_ID:
        return [DEBUG_GUILD_ID]
    return None  # Global registration (auto-sync handles it)
