# warden/__init__.py
"""
QuestLog Bot - Discord Security & Engagement Platform

Open source bot for gaming communities featuring:
- Anti-raid protection and moderation
- Member verification
- XP, leveling, and flair system
- Discovery network and creator spotlight
- LFG, streaming monitor, and more

Built with Pycord + SQLAlchemy + MySQL
"""

__version__ = "0.1.0"
__author__ = "Casual Heroes"

from warden.config import (
    get_bot_token,
    get_db_session,
    db_session_scope,
    init_database,
    intents,
    logger,
    FeatureLimits,
    DefaultXPSettings,
    DefaultRaidSettings,
    DefaultVerificationSettings,
    IS_PRODUCTION,
)

from warden.models import (
    Guild,
    GuildMember,
    XPConfig,
    LevelRole,
    RaidConfig,
    VerificationConfig,
    AuditLog,
    ReactRole,
    PromoPost,
    FeaturedPool,
    DiscoveryNetwork,
    Subscription,
    SubscriptionTier,
    VerificationType,
    AuditAction,
    PromoTier,
)

__all__ = [
    # Config
    "get_bot_token",
    "get_db_session",
    "db_session_scope",
    "init_database",
    "intents",
    "logger",
    "FeatureLimits",
    "DefaultXPSettings",
    "DefaultRaidSettings",
    "DefaultVerificationSettings",
    "IS_PRODUCTION",
    # Models
    "Guild",
    "GuildMember",
    "XPConfig",
    "LevelRole",
    "RaidConfig",
    "VerificationConfig",
    "AuditLog",
    "ReactRole",
    "PromoPost",
    "FeaturedPool",
    "DiscoveryNetwork",
    "Subscription",
    # Enums
    "SubscriptionTier",
    "VerificationType",
    "AuditAction",
    "PromoTier",
]
