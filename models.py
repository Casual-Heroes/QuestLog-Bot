# warden/models.py
# Database models for QuestLog - Multi-tenant design for 1000+ guilds

from sqlalchemy import (
    Column, Integer, String, BigInteger, Boolean, Text, Float,
    ForeignKey, Index, UniqueConstraint, Enum as SQLEnum
)
from sqlalchemy.orm import declarative_base, relationship
from enum import Enum
import time

Base = declarative_base()


# Enums

class SubscriptionTier(str, Enum):
    FREE = "free"
    COMPLETE = "complete"
    # Note: Individual module subscriptions are tracked in guild_modules table, not here

class VerificationType(str, Enum):
    NONE = "none"
    BUTTON = "button"
    CAPTCHA = "captcha"
    ACCOUNT_AGE = "account_age"
    MULTI_STEP = "multi_step"

class AuditAction(str, Enum):
    MEMBER_JOIN = "member_join"
    MEMBER_LEAVE = "member_leave"
    MEMBER_BAN = "member_ban"
    MEMBER_UNBAN = "member_unban"
    MEMBER_KICK = "member_kick"
    MEMBER_TIMEOUT = "member_timeout"
    ROLE_ADD = "role_add"
    ROLE_REMOVE = "role_remove"
    ROLE_CREATE = "role_create"
    ROLE_DELETE = "role_delete"
    CHANNEL_CREATE = "channel_create"
    CHANNEL_DELETE = "channel_delete"
    CHANNEL_UPDATE = "channel_update"
    PERMISSION_UPDATE = "permission_update"
    MESSAGE_DELETE = "message_delete"
    MESSAGE_BULK_DELETE = "message_bulk_delete"
    RAID_DETECTED = "raid_detected"
    LOCKDOWN_ACTIVATED = "lockdown_activated"
    LOCKDOWN_DEACTIVATED = "lockdown_deactivated"
    VERIFICATION_PASSED = "verification_passed"
    VERIFICATION_FAILED = "verification_failed"

class PromoTier(str, Enum):
    BASIC = "basic"       # FREE - regular self-promo
    FEATURED = "featured" # PREMIUM - featured pool (15 tokens)

class FlairType(str, Enum):
    NORMAL = "normal"     # Default flairs included with bot
    SEASONAL = "seasonal" # Seasonal/event flairs
    CUSTOM = "custom"     # Guild-specific custom flairs (Premium feature)


class ActionStatus(str, Enum):
    """Status of a pending action in the queue."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ActionType(str, Enum):
    """Types of actions that can be queued from the website."""
    # Role Management
    ROLE_ADD = "role_add"
    ROLE_REMOVE = "role_remove"
    ROLE_BULK_ADD = "role_bulk_add"
    ROLE_BULK_REMOVE = "role_bulk_remove"

    # XP Management
    XP_ADD = "xp_add"
    XP_REMOVE = "xp_remove"
    XP_SET = "xp_set"
    XP_BULK_SET = "xp_bulk_set"
    LEVEL_SET = "level_set"
    TOKENS_ADD = "tokens_add"
    TOKENS_REMOVE = "tokens_remove"
    TOKENS_SET = "tokens_set"

    # Member Management
    MEMBER_KICK = "member_kick"
    MEMBER_BAN = "member_ban"
    MEMBER_UNBAN = "member_unban"
    MEMBER_TIMEOUT = "member_timeout"
    MEMBER_UNTIMEOUT = "member_untimeout"
    MEMBER_JAIL = "member_jail"
    MEMBER_UNJAIL = "member_unjail"

    # Moderation
    WARNING_ADD = "warning_add"
    WARNING_PARDON = "warning_pardon"
    WARNING_BULK_CLEAR = "warning_bulk_clear"

    # Messages
    MESSAGE_SEND = "message_send"
    MESSAGE_DELETE = "message_delete"
    EMBED_SEND = "embed_send"
    DM_SEND = "dm_send"

    # XP Boost Events
    BOOST_EVENT_START = "boost_event_start"

    # Channel Management
    CHANNEL_TOPIC_SET = "channel_topic_set"

    # Template Management
    CHANNEL_CREATE = "channel_create"
    ROLE_CREATE = "role_create"

    # Discovery/Self-Promo
    FORCE_FEATURE = "force_feature"
    CLEAR_FEATURED = "clear_featured"
    TEST_CHANNEL_EMBED = "test_channel_embed"
    TEST_FORUM_EMBED = "test_forum_embed"
    CHECK_GAMES = "check_games"

    # Flair Management
    FLAIR_ASSIGN = "flair_assign"
    FLAIR_SEED_ROLES = "flair_seed_roles"

    # LFG System
    LFG_THREAD_CREATE = "lfg_thread_create"
    LFG_THREAD_UPDATE = "lfg_thread_update"
    LFG_THREAD_DELETE = "lfg_thread_delete"

    # Sync Operations
    SYNC_ROLES = "sync_roles"
    SYNC_MEMBERS = "sync_members"

    # RSS Feeds
    RSS_TEST_SEND = "rss_test_send"


# Guild

class Guild(Base):
    """Master table for all guilds using QuestLog."""
    __tablename__ = "guilds"

    guild_id = Column(BigInteger, primary_key=True)
    guild_name = Column(String(255), nullable=True)
    owner_id = Column(BigInteger, nullable=True)

    # Subscription
    # Use explicit enum values to match database (SQLAlchemy would use enum NAMES otherwise)
    subscription_tier = Column(
        SQLEnum('free', 'complete', name='subscriptiontier'),
        default='free'
    )
    billing_cycle = Column(
        SQLEnum('monthly', '3month', '6month', 'yearly', 'lifetime', name='billingcycle'),
        nullable=True
    )
    subscription_expires = Column(BigInteger, nullable=True)
    stripe_customer_id = Column(String(255), nullable=True)
    stripe_subscription_id = Column(String(255), nullable=True)

    # VIP flag - unlocks premium for free (friends & family)
    is_vip = Column(Boolean, default=False)
    vip_granted_by = Column(BigInteger, nullable=True)
    vip_granted_at = Column(BigInteger, nullable=True)
    vip_note = Column(String(255), nullable=True)

    # Settings
    bot_prefix = Column(String(10), default="/questlog")
    language = Column(String(10), default="en")
    timezone = Column(String(50), default="UTC")

    # Token Customization (rename "Hero Tokens" to anything)
    token_name = Column(String(50), default="Hero Tokens")
    token_emoji = Column(String(20), default=":coin:")

    # Feature toggles
    xp_enabled = Column(Boolean, default=True)
    anti_raid_enabled = Column(Boolean, default=True)
    verification_enabled = Column(Boolean, default=False)
    audit_logging_enabled = Column(Boolean, nullable=False, default=False, server_default='0')
    audit_event_config = Column(Text, nullable=True)  # JSON map of event toggles
    mod_enabled = Column(Boolean, nullable=False, default=False, server_default='0')
    discovery_enabled = Column(Boolean, default=False)

    # Cached Discord Resources (JSON text - synced by bot to reduce API calls)
    cached_channels = Column(Text, nullable=True)  # JSON array of channel objects
    cached_roles = Column(Text, nullable=True)  # JSON array of role objects
    cached_emojis = Column(Text, nullable=True)  # JSON array of emoji objects
    cached_members = Column(Text, nullable=True)  # JSON array of member objects (id, username, discriminator, roles, avatar)
    guild_icon_hash = Column(String(255), nullable=True)  # Discord guild icon hash for CDN URL

    # Cached Member Stats (synced by bot from Discord presence data)
    member_count = Column(Integer, nullable=True)  # Total members (excluding bots)
    online_count = Column(Integer, nullable=True)  # Currently online members

    # Channel IDs
    log_channel_id = Column(BigInteger, nullable=True)
    welcome_channel_id = Column(BigInteger, nullable=True)
    level_up_channel_id = Column(BigInteger, nullable=True)
    verification_channel_id = Column(BigInteger, nullable=True)
    self_promo_channel_id = Column(BigInteger, nullable=True)

    # Roles
    verified_role_id = Column(BigInteger, nullable=True)
    quarantine_role_id = Column(BigInteger, nullable=True)
    muted_role_id = Column(BigInteger, nullable=True)
    jail_role_id = Column(BigInteger, nullable=True)  # Hides all channels, only sees jail channel

    # Moderation Channels
    jail_channel_id = Column(BigInteger, nullable=True)  # Where jailed users go for review
    mod_log_channel_id = Column(BigInteger, nullable=True)  # Mod action log channel

    # Bot lifecycle tracking
    bot_present = Column(Boolean, default=True)  # Is bot currently in this guild?
    left_at = Column(BigInteger, nullable=True)  # When bot was removed (null if present)

    # Timestamps
    joined_at = Column(BigInteger, default=lambda: int(time.time()))
    updated_at = Column(BigInteger, default=lambda: int(time.time()), onupdate=lambda: int(time.time()))

    # Relationships
    members = relationship("GuildMember", back_populates="guild", cascade="all, delete-orphan")
    audit_logs = relationship("AuditLog", back_populates="guild", cascade="all, delete-orphan")
    xp_config = relationship("XPConfig", back_populates="guild", uselist=False, cascade="all, delete-orphan")
    raid_config = relationship("RaidConfig", back_populates="guild", uselist=False, cascade="all, delete-orphan")
    verification_config = relationship("VerificationConfig", back_populates="guild", uselist=False, cascade="all, delete-orphan")
    level_roles = relationship("LevelRole", back_populates="guild", cascade="all, delete-orphan")
    react_roles = relationship("ReactRole", back_populates="guild", cascade="all, delete-orphan")
    creator_profiles = relationship("CreatorProfile", back_populates="guild", cascade="all, delete-orphan")

    def is_premium(self) -> bool:
        """Check if guild has active premium or VIP status."""
        if self.is_vip:
            return True
        if self.subscription_tier == SubscriptionTier.FREE:
            return False
        if self.subscription_expires and self.subscription_expires < int(time.time()):
            return False
        return True

    def __repr__(self):
        return f"<Guild(id={self.guild_id}, name={self.guild_name}, tier={self.subscription_tier})>"


# Guild Modules (Modular Subscription System)

class GuildModule(Base):
    """
    Tracks which modules each guild has subscribed to.
    Supports modular pricing where guilds can subscribe to individual modules.
    """
    __tablename__ = "guild_modules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=False)
    module_name = Column(String(50), nullable=False)  # 'engagement', 'roles', 'moderation', 'discovery', 'lfg'
    enabled = Column(Boolean, nullable=False, default=True)

    # Stripe subscription info for this specific module
    stripe_subscription_id = Column(String(255), nullable=True)
    stripe_product_id = Column(String(255), nullable=True)
    stripe_price_id = Column(String(255), nullable=True)

    # Expiration
    expires_at = Column(BigInteger, nullable=True)

    # Activation tracking
    activated_at = Column(BigInteger, nullable=False, default=lambda: int(time.time()))
    activated_by = Column(BigInteger, nullable=True)  # User ID who activated

    __table_args__ = (
        Index("idx_guild_module_guild", "guild_id"),
        Index("idx_guild_module_name", "module_name"),
        Index("idx_guild_module_subscription", "stripe_subscription_id"),
        UniqueConstraint("guild_id", "module_name", name="uq_guild_module"),
    )

    def __repr__(self):
        return f"<GuildModule(guild_id={self.guild_id}, module={self.module_name}, enabled={self.enabled})>"


# Guild Member

class GuildMember(Base):
    """Per-guild member data with XP tracking."""
    __tablename__ = "guild_members"

    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), primary_key=True)
    user_id = Column(BigInteger, primary_key=True)

    # User info
    display_name = Column(String(255), nullable=True)
    username = Column(String(255), nullable=True)
    avatar_hash = Column(String(255), nullable=True)
    is_bot = Column(Boolean, default=False)  # Is this user a bot?

    # XP & Leveling
    xp = Column(Float, default=0.0)
    level = Column(Integer, default=0)
    hero_tokens = Column(Integer, default=0)
    flair = Column(String(100), nullable=True)  # User's selected flair (e.g., "[🎮 Casual Legend]")

    # Activity tracking
    message_count = Column(Integer, default=0)
    media_count = Column(Integer, default=0)
    voice_minutes = Column(Integer, default=0)
    reaction_count = Column(Integer, default=0)
    invite_count = Column(Integer, default=0)
    command_count = Column(Integer, default=0)

    # Cooldowns
    last_message_ts = Column(BigInteger, default=0)
    last_media_ts = Column(BigInteger, default=0)
    last_voice_join_ts = Column(BigInteger, default=0)
    last_voice_bonus_ts = Column(BigInteger, default=0)
    last_react_ts = Column(BigInteger, default=0)
    last_invite_ts = Column(BigInteger, default=0)
    last_command_ts = Column(BigInteger, default=0)
    last_gaming_ts = Column(BigInteger, default=0)
    last_game_launch_ts = Column(BigInteger, default=0)

    # Verification
    is_verified = Column(Boolean, default=False)
    verified_at = Column(BigInteger, nullable=True)
    verification_method = Column(String(50), nullable=True)

    # Moderation
    is_quarantined = Column(Boolean, default=False)
    quarantined_at = Column(BigInteger, nullable=True)
    quarantine_reason = Column(String(500), nullable=True)
    quarantined_roles = Column(Text, nullable=True)  # JSON array of role IDs to restore on unjail
    warn_count = Column(Integer, default=0)

    # Timestamps
    first_seen = Column(BigInteger, default=lambda: int(time.time()))
    last_active = Column(BigInteger, default=lambda: int(time.time()))

    guild = relationship("Guild", back_populates="members")

    __table_args__ = (
        Index("idx_guild_member_xp", "guild_id", "xp"),
        Index("idx_guild_member_level", "guild_id", "level"),
        Index("idx_guild_member_active", "guild_id", "last_active"),
        Index("idx_user_across_guilds", "user_id"),
    )

    def __repr__(self):
        return f"<GuildMember(guild={self.guild_id}, user={self.user_id}, level={self.level})>"


# XP Config

class XPConfig(Base):
    """Per-guild XP settings."""
    __tablename__ = "xp_configs"

    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), primary_key=True)

    # System toggle
    xp_enabled = Column(Boolean, default=False)  # XP system disabled by default

    # XP rates
    message_xp = Column(Float, default=1.5)
    media_multiplier = Column(Float, default=1.3)
    reaction_xp = Column(Float, default=1.0)
    voice_xp_per_interval = Column(Float, default=1.3)
    command_xp = Column(Float, default=1.0)
    gaming_xp_per_interval = Column(Float, default=1.2)
    invite_xp = Column(Float, default=50.0)
    join_xp = Column(Float, default=25.0)
    xp_enabled = Column(Boolean, nullable=False, default=True, server_default='1')

    # XP source toggles (opt-in)
    track_messages = Column(Boolean, nullable=False, default=False, server_default='0')
    track_media = Column(Boolean, nullable=False, default=False, server_default='0')
    track_reactions = Column(Boolean, nullable=False, default=False, server_default='0')
    track_voice = Column(Boolean, nullable=False, default=False, server_default='0')
    track_gaming = Column(Boolean, nullable=False, default=False, server_default='0')
    track_game_launch = Column(Boolean, nullable=False, default=False, server_default='0')

    # Token conversion
    tokens_per_100_xp_active = Column(Integer, default=15)
    tokens_per_100_xp_passive = Column(Integer, default=5)

    # Cooldowns (seconds)
    message_cooldown = Column(Integer, default=60)
    media_cooldown = Column(Integer, default=60)
    reaction_cooldown = Column(Integer, default=60)
    voice_interval = Column(Integer, default=5400)
    gaming_interval = Column(Integer, default=5400)
    command_cooldown = Column(Integer, default=60)
    game_launch_cooldown = Column(Integer, default=7200)

    # Level settings
    max_level = Column(Integer, default=99)
    level_formula = Column(String(100), default="7 * (level ^ 1.5)")

    # Self-promo
    self_promo_cost = Column(Integer, default=0)
    featured_pool_cost = Column(Integer, default=15)

    guild = relationship("Guild", back_populates="xp_config")

    def __repr__(self):
        return f"<XPConfig(guild={self.guild_id})>"


class LevelRole(Base):
    """Auto-assign roles at certain levels."""
    __tablename__ = "level_roles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))
    level = Column(Integer, nullable=False)
    role_id = Column(BigInteger, nullable=False)
    role_name = Column(String(255), nullable=True)
    remove_previous = Column(Boolean, default=True)

    guild = relationship("Guild", back_populates="level_roles")

    __table_args__ = (
        UniqueConstraint("guild_id", "level", name="uq_guild_level"),
        Index("idx_level_roles_guild", "guild_id"),
    )


class XPExcludedChannel(Base):
    """Channels where XP is not earned."""
    __tablename__ = "xp_excluded_channels"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))
    channel_id = Column(BigInteger, nullable=False)

    __table_args__ = (
        UniqueConstraint("guild_id", "channel_id", name="uq_guild_channel_xp"),
        Index("idx_xp_excluded_guild", "guild_id"),
    )


class XPExcludedRole(Base):
    """Roles that don't earn XP."""
    __tablename__ = "xp_excluded_roles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))
    role_id = Column(BigInteger, nullable=False)

    __table_args__ = (
        UniqueConstraint("guild_id", "role_id", name="uq_guild_role_xp"),
        Index("idx_xp_excluded_role_guild", "guild_id"),
    )


# Anti-Raid

class RaidConfig(Base):
    """Per-guild anti-raid settings."""
    __tablename__ = "raid_configs"

    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), primary_key=True)

    # Account age
    min_account_age_days = Column(Integer, default=7)
    flag_new_accounts = Column(Boolean, default=True)
    auto_quarantine_new_accounts = Column(Boolean, default=False)

    # Mass join detection
    mass_join_threshold = Column(Integer, default=10)
    mass_join_window_seconds = Column(Integer, default=60)
    mass_join_action = Column(String(50), default="alert")

    # Rate limiting
    join_rate_limit = Column(Integer, default=30)
    auto_lockdown_enabled = Column(Boolean, default=False)
    lockdown_duration_minutes = Column(Integer, default=30)

    # Lockdown state
    is_locked_down = Column(Boolean, default=False)
    lockdown_started_at = Column(BigInteger, nullable=True)
    lockdown_ends_at = Column(BigInteger, nullable=True)
    lockdown_reason = Column(String(500), nullable=True)

    # Alerts
    alert_channel_id = Column(BigInteger, nullable=True)
    ping_role_id = Column(BigInteger, nullable=True)
    dm_owner_on_raid = Column(Boolean, default=True)

    # Premium features
    detect_vpn = Column(Boolean, default=False)
    detect_similar_names = Column(Boolean, default=False)
    honeypot_channel_id = Column(BigInteger, nullable=True)

    guild = relationship("Guild", back_populates="raid_config")


class RaidEvent(Base):
    """Log of detected raid events."""
    __tablename__ = "raid_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))

    detected_at = Column(BigInteger, default=lambda: int(time.time()))
    join_count = Column(Integer, default=0)
    window_seconds = Column(Integer, default=60)
    action_taken = Column(String(50), nullable=True)
    resolved = Column(Boolean, default=False)
    resolved_at = Column(BigInteger, nullable=True)
    resolved_by = Column(BigInteger, nullable=True)
    notes = Column(Text, nullable=True)

    __table_args__ = (
        Index("idx_raid_events_guild", "guild_id", "detected_at"),
    )


# Verification

class VerificationConfig(Base):
    """Per-guild verification settings."""
    __tablename__ = "verification_configs"

    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), primary_key=True)

    verification_type = Column(SQLEnum(VerificationType, values_callable=lambda x: [e.value for e in x]), default=VerificationType.BUTTON)

    # Account age
    require_account_age = Column(Boolean, default=True)
    min_account_age_days = Column(Integer, default=7)

    # Button
    button_text = Column(String(100), default="I agree to the rules")

    # Captcha
    captcha_length = Column(Integer, default=6)
    captcha_timeout_seconds = Column(Integer, default=300)

    # Multi-step (Premium)
    require_rules_read = Column(Boolean, default=False)
    require_intro_message = Column(Boolean, default=False)
    intro_channel_id = Column(BigInteger, nullable=True)
    require_external_verify = Column(Boolean, default=False)

    # Messages
    welcome_message = Column(Text, nullable=True)
    verification_instructions = Column(Text, nullable=True)
    verified_message = Column(Text, nullable=True)

    # Timeout
    verification_timeout_hours = Column(Integer, default=24)
    kick_on_timeout = Column(Boolean, default=False)

    guild = relationship("Guild", back_populates="verification_config")


# Audit Log

class AuditLog(Base):
    """Security and moderation audit log."""
    __tablename__ = "audit_logs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))

    action = Column(SQLEnum(AuditAction, native_enum=False, length=50), nullable=False)
    action_category = Column(String(50), nullable=True)

    actor_id = Column(BigInteger, nullable=True)
    actor_name = Column(String(255), nullable=True)

    target_id = Column(BigInteger, nullable=True)
    target_name = Column(String(255), nullable=True)
    target_type = Column(String(50), nullable=True)

    reason = Column(Text, nullable=True)
    details = Column(Text, nullable=True)

    timestamp = Column(BigInteger, default=lambda: int(time.time()))

    guild = relationship("Guild", back_populates="audit_logs")

    __table_args__ = (
        Index("idx_audit_guild_time", "guild_id", "timestamp"),
        Index("idx_audit_guild_action", "guild_id", "action"),
        Index("idx_audit_guild_actor", "guild_id", "actor_id"),
        Index("idx_audit_guild_target", "guild_id", "target_id"),
    )


# React Roles

class ReactRole(Base):
    """Reaction role configuration."""
    __tablename__ = "react_roles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))

    message_id = Column(BigInteger, nullable=False)
    channel_id = Column(BigInteger, nullable=False)
    emoji = Column(String(100), nullable=False)
    role_id = Column(BigInteger, nullable=False)
    role_name = Column(String(255), nullable=True)

    remove_on_unreact = Column(Boolean, default=True)
    exclusive_group = Column(String(100), nullable=True)

    guild = relationship("Guild", back_populates="react_roles")

    __table_args__ = (
        UniqueConstraint("message_id", "emoji", name="uq_message_emoji"),
        Index("idx_react_roles_guild", "guild_id"),
        Index("idx_react_roles_message", "message_id"),
    )


# Promo Posts

class PromoPost(Base):
    """Self-promotion posts."""
    __tablename__ = "promo_posts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))
    user_id = Column(BigInteger, nullable=False)

    content = Column(Text, nullable=False)
    link_url = Column(String(500), nullable=True)
    platform = Column(String(50), nullable=True)

    promo_tier = Column(SQLEnum(PromoTier), default=PromoTier.BASIC)
    tokens_spent = Column(Integer, default=0)

    is_featured = Column(Boolean, default=False)
    featured_at = Column(BigInteger, nullable=True)
    featured_until = Column(BigInteger, nullable=True)
    featured_message_id = Column(BigInteger, nullable=True)

    created_at = Column(BigInteger, default=lambda: int(time.time()))

    __table_args__ = (
        Index("idx_promo_guild", "guild_id", "created_at"),
        Index("idx_promo_featured", "is_featured", "featured_until"),
        Index("idx_promo_user", "user_id"),
    )


class FeaturedPool(Base):
    """Users in the featured pool waiting for random selection."""
    __tablename__ = "featured_pool"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))
    user_id = Column(BigInteger, nullable=False)

    # The content they want to promote (from /post command)
    content = Column(Text, nullable=True)  # Optional message/description
    link_url = Column(String(500), nullable=True)  # Link to stream/video/content
    platform = Column(String(50), nullable=True)  # twitch, youtube, twitter, etc.

    # Original message info (for reference)
    original_message_id = Column(BigInteger, nullable=True)
    original_channel_id = Column(BigInteger, nullable=True)
    forum_thread_id = Column(BigInteger, nullable=True)  # For forum-based entries

    # Pool management
    entered_at = Column(BigInteger, default=lambda: int(time.time()))
    expires_at = Column(BigInteger, nullable=False)  # When entry expires from pool

    # Selection status
    was_selected = Column(Boolean, default=False)
    selected_at = Column(BigInteger, nullable=True)
    featured_message_id = Column(BigInteger, nullable=True)  # The shoutout message

    __table_args__ = (
        Index("idx_featured_pool_guild", "guild_id", "expires_at"),
        Index("idx_featured_pool_active", "guild_id", "was_selected", "expires_at"),
        Index("idx_featured_pool_user", "guild_id", "user_id"),
    )


class FeaturedCreator(Base):
    """
    Global record of featured creators for public display (Hall of Fame).
    One entry per Discord user, regardless of how many guilds they're in.
    """
    __tablename__ = "featured_creators"

    user_id = Column(BigInteger, primary_key=True)  # Discord user ID (global unique)

    # Multi-guild tracking
    guilds = Column(Text, nullable=False, default='[]')  # JSON array of guild_ids: [123, 456, 789]
    primary_guild_id = Column(BigInteger, nullable=True)  # Which guild's intro to display
    auto_select_primary = Column(Boolean, default=True)  # Auto-update primary to most recent?

    # Activity tracking
    is_active = Column(Boolean, default=True)  # False if they left all guilds
    inactive_since = Column(BigInteger, nullable=True)  # Timestamp when they left all guilds

    # Discord profile data (cached for display)
    username = Column(String(255))
    display_name = Column(String(255))
    avatar_url = Column(Text)  # Discord profile picture

    # Featured data - GLOBAL across all guilds
    first_featured_at = Column(BigInteger)  # When they were first featured (any guild)
    last_featured_at = Column(BigInteger)  # Most recent feature (any guild)
    times_featured_total = Column(Integer, default=0)  # Total times featured across ALL guilds

    # Social links (from their featured pool entries)
    twitch_url = Column(Text, nullable=True)
    youtube_url = Column(Text, nullable=True)
    twitter_url = Column(Text, nullable=True)
    tiktok_url = Column(Text, nullable=True)
    instagram_url = Column(Text, nullable=True)
    bsky_url = Column(Text, nullable=True)
    other_links = Column(Text, nullable=True)  # JSON array of other links

    # Content/bio
    bio = Column(Text, nullable=True)  # Latest featured content/bio

    # Source tracking (forum-based system)
    source = Column(String(50), default='forum')  # Source: 'forum', 'selfpromo', 'manual'
    forum_thread_id = Column(BigInteger, nullable=True)  # Discord forum thread ID (from primary guild)
    forum_tag_name = Column(String(255), nullable=True)  # Forum tag when featured

    # Discord connected accounts (from Discord API)
    discord_connections = Column(Text, nullable=True)  # JSON: {platform: {type, id, name, verified}}

    # Metadata
    created_at = Column(BigInteger, default=lambda: int(time.time()))
    updated_at = Column(BigInteger, default=lambda: int(time.time()))

    __table_args__ = (
        Index("idx_featured_creators_user", "user_id"),
        Index("idx_featured_creators_last", "last_featured_at"),
        Index("idx_featured_creators_active", "is_active"),
    )


class DiscoveryConfig(Base):
    """Per-guild Discovery/Self-Promo settings."""
    __tablename__ = "discovery_configs"

    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), primary_key=True)

    # Feature toggles
    enabled = Column(Boolean, default=False)  # Master toggle for discovery system
    channel_enabled = Column(Boolean, default=True)  # Enable channel-based discovery
    forum_enabled = Column(Boolean, default=False)  # Enable forum-based discovery

    # Channel configuration
    selfpromo_channel_id = Column(BigInteger, nullable=True)  # Where users post (bot ignores regular posts)
    feature_channel_id = Column(BigInteger, nullable=True)  # Where featured users are shouted out
    intro_forum_channel_id = Column(BigInteger, nullable=True)  # Forum channel for creator intros (website featured creators)
    intro_announcement_channel_id = Column(BigInteger, nullable=True)  # Where to post Discord embeds for forum creators

    # Forum scanning settings
    intro_scan_interval_hours = Column(Integer, default=1)  # Hours between forum scans (separate from quick feature interval)
    last_intro_scan_at = Column(BigInteger, default=0)  # Last time forum was scanned

    # Discord-only quick feature toggle
    selfpromo_quick_feature = Column(Boolean, default=False)  # Enable Discord-only embeds for selfpromo (not website)

    # Creator of the Month (PREMIUM)
    cotm_enabled = Column(Boolean, default=False)  # Enable Creator of the Month feature
    cotm_channel_id = Column(BigInteger, nullable=True)  # Channel to post COTM announcement
    cotm_last_message_id = Column(BigInteger, nullable=True)  # Last COTM message ID (for deletion)
    cotm_last_posted_at = Column(BigInteger, nullable=True)  # Unix timestamp of last COTM post
    cotm_last_featured_user_id = Column(BigInteger, nullable=True)  # Last featured user (to avoid repeats)
    cotm_auto_rotate = Column(Boolean, default=False)  # Enable automatic monthly rotation
    cotm_rotation_day = Column(Integer, default=1)  # Day of month to rotate (1-31)

    # Creator of the Week (PRO)
    cotw_enabled = Column(Boolean, default=False)  # Enable Creator of the Week feature
    cotw_channel_id = Column(BigInteger, nullable=True)  # Channel to post COTW announcement
    cotw_last_message_id = Column(BigInteger, nullable=True)  # Last COTW message ID (for deletion)
    cotw_last_posted_at = Column(BigInteger, nullable=True)  # Unix timestamp of last COTW post
    cotw_last_featured_user_id = Column(BigInteger, nullable=True)  # Last featured user (to avoid repeats)
    cotw_auto_rotate = Column(Boolean, default=False)  # Enable automatic weekly rotation
    cotw_rotation_day = Column(Integer, default=1)  # Day of week to rotate (0=Monday, 6=Sunday)

    # Feature rotation settings
    feature_interval_hours = Column(Integer, default=3)  # Hours between random feature picks
    last_feature_at = Column(BigInteger, nullable=True)  # Timestamp of last feature
    last_featured_user_id = Column(BigInteger, nullable=True)  # Last user who was featured
    last_featured_message_id = Column(BigInteger, nullable=True)  # Message ID of last featured post (for deletion)

    # Message Response Channel
    message_response_channel_id = Column(BigInteger, nullable=True)  # Channel to send automated messages (instead of replying in self-promo)

    # Game Discovery Notifications
    public_game_ping_role_id = Column(BigInteger, nullable=True)  # Role to ping when public games are found

    # Featured Reminder Scheduling
    reminder_schedule = Column(String(50), default='disabled')  # disabled, hourly, every_6_hours, daily, weekly, monthly
    last_reminder_sent_at = Column(BigInteger, nullable=True)  # Timestamp of last reminder

    # Messages
    how_to_enter_response = Column(Text, default="💬 Thanks for sharing! To enter the featured pool, you need **{token_cost} Hero Tokens**.\nYou currently have **{hero_tokens} Hero Tokens**. Stay active to earn more! 🎮")
    post_response = Column(Text, default="Good luck on being featured! You've been added to the feature pool.")
    feature_message = Column(Text, default="Shoutout to {user}! Check out their content!")
    cooldown_message = Column(Text, default="⏰ You're on cooldown! Can enter the featured pool again in {time_left}.\n💰 Your {token_cost} hero_tokens were saved.")
    use_embed = Column(Boolean, default=True)
    embed_color = Column(Integer, default=0x5865F2)

    # Optional: require tokens to enter pool
    require_tokens = Column(Boolean, default=False)
    token_cost = Column(Integer, default=0)

    # Pool settings
    pool_entry_duration_hours = Column(Integer, default=24)  # How long entries stay in pool
    remove_after_feature = Column(Boolean, default=True)  # Remove from pool after being featured
    feature_cooldown_hours = Column(Integer, default=72)  # Hours before same user can be featured again
    entry_cooldown_hours = Column(Integer, default=24)  # Hours before user can enter pool again (rate limiting)

    # Game Discovery settings
    game_discovery_enabled = Column(Boolean, default=False)  # Enable game discovery notifications
    public_game_channel_id = Column(BigInteger, nullable=True)  # Channel for public game discoveries (show_on_website=True)
    private_game_channel_id = Column(BigInteger, nullable=True)  # Channel for private game discoveries (show_on_website=False)
    game_api_sources = Column(Text, nullable=True)  # JSON array: ["igdb", "steam"] - which APIs to use
    game_genres = Column(Text, nullable=True)  # JSON array of genres: ["ARPG", "MMO", "FPS", "RPG"]
    game_themes = Column(Text, nullable=True)  # JSON array of theme slugs (Action, Fantasy, Horror, etc.)
    game_modes = Column(Text, nullable=True)  # JSON array of modes: ["singleplayer", "coop", "multiplayer"]
    game_platforms = Column(Text, nullable=True)  # JSON array of platforms: ["PC", "Steam", "PlayStation"]
    game_os_filter = Column(Text, nullable=True)  # JSON array: ["windows", "mac", "linux"]
    game_check_interval_hours = Column(Integer, default=24)  # Hours between game discovery checks
    game_days_ahead = Column(Integer, default=30)  # How many days ahead to search for upcoming games
    game_days_behind = Column(Integer, default=0)  # How many days behind (past) to search for recently released games
    game_min_hype = Column(Integer, nullable=True)  # Minimum hype score (follows before release) for game announcements
    game_min_rating = Column(Integer, nullable=True)  # Minimum rating (IGDB Double field) for game announcements
    last_game_check_at = Column(BigInteger, nullable=True)  # Last time we checked for new games

    # Network Creator Announcements (opt-in to see Network COTW/COTM in your server)
    network_announcements_enabled = Column(Boolean, default=False)  # Receive Network COTW/COTM announcements
    network_announcement_channel_id = Column(BigInteger, nullable=True)  # Channel to post network creator announcements

    # Custom Role Flair System (badges/prefixes for creators)
    role_flair_config = Column(Text, nullable=True)  # JSON: [{"role_id": 123, "flair_text": "Verified Streamer", "flair_icon": "🎮", "color": "#5865F2"}]

    # Timestamps
    created_at = Column(BigInteger, default=lambda: int(time.time()))
    updated_at = Column(BigInteger, default=lambda: int(time.time()))

    __table_args__ = (
        Index("idx_discovery_config_guild", "guild_id"),
    )


class CreatorOfTheMonth(Base):
    """Creator of the Month history (PREMIUM feature)."""
    __tablename__ = "creator_of_the_month"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, nullable=False)
    user_id = Column(BigInteger, nullable=False)
    username = Column(String(255))
    display_name = Column(String(255))
    avatar_url = Column(Text)
    bio = Column(Text)
    month = Column(Integer, nullable=False)
    year = Column(Integer, nullable=False)
    message_id = Column(BigInteger)
    channel_id = Column(BigInteger)
    featured_at = Column(BigInteger, nullable=False)
    created_at = Column(BigInteger, nullable=False)

    __table_args__ = (
        Index("idx_cotm_guild", "guild_id"),
        Index("idx_cotm_user", "user_id"),
        Index("idx_cotm_date", "year", "month"),
    )


class CreatorOfTheWeek(Base):
    """Creator of the Week history (PRO feature)."""
    __tablename__ = "creator_of_the_week"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, nullable=False)
    user_id = Column(BigInteger, nullable=False)
    username = Column(String(255))
    display_name = Column(String(255))
    avatar_url = Column(Text)
    bio = Column(Text)
    week = Column(Integer, nullable=False)
    year = Column(Integer, nullable=False)
    message_id = Column(BigInteger)
    channel_id = Column(BigInteger)
    featured_at = Column(BigInteger, nullable=False)
    created_at = Column(BigInteger, nullable=False)

    __table_args__ = (
        Index("idx_cotw_guild", "guild_id"),
        Index("idx_cotw_user", "user_id"),
        Index("idx_cotw_date", "year", "week"),
    )


class AnnouncedGame(Base):
    """Track games that have been announced to prevent duplicates."""
    __tablename__ = "announced_games"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))

    # Game identifiers
    igdb_id = Column(Integer, nullable=True)  # IGDB game ID
    igdb_slug = Column(String(255), nullable=True)  # IGDB URL slug
    steam_id = Column(Integer, nullable=True)  # Steam app ID
    game_name = Column(String(255), nullable=False)

    # Game details
    release_date = Column(BigInteger, nullable=True)  # Unix timestamp
    genres = Column(Text, nullable=True)  # JSON array
    platforms = Column(Text, nullable=True)  # JSON array
    cover_url = Column(String(500), nullable=True)

    # Announcement details
    announced_at = Column(BigInteger, default=lambda: int(time.time()))
    announcement_message_id = Column(BigInteger, nullable=True)

    __table_args__ = (
        Index("idx_announced_game_guild", "guild_id", "igdb_id"),
        Index("idx_announced_game_steam", "guild_id", "steam_id"),
    )


class GameSearchConfig(Base):
    """Individual game search configurations for game discovery."""
    __tablename__ = "game_search_configs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=False)

    # Search configuration
    name = Column(String(100), nullable=False)  # e.g., "Souls-like RPGs", "MMO Games"
    enabled = Column(Boolean, default=True, nullable=False)

    # Filters (JSON arrays)
    genres = Column(Text, nullable=True)  # JSON array of genre names
    themes = Column(Text, nullable=True)  # JSON array of theme names
    keywords = Column(Text, nullable=True)  # JSON array of IGDB keywords (Souls-like, Metroidvania, etc.)
    game_modes = Column(Text, nullable=True)  # JSON array of mode names
    platforms = Column(Text, nullable=True)  # JSON array of platform names

    # Quality filters
    min_hype = Column(Integer, nullable=True)  # Minimum hype score
    min_rating = Column(Float, nullable=True)  # Minimum rating

    # Announcement settings
    days_ahead = Column(Integer, default=30, nullable=False)  # How far ahead to announce

    # Privacy settings
    show_on_website = Column(Boolean, default=True, nullable=False)  # If False, only posts to Discord thread
    discovery_thread_id = Column(BigInteger, nullable=True)  # Discord thread ID for private searches
    auto_join_role_id = Column(BigInteger, nullable=True)  # Role ID to auto-join members to private thread

    # Timestamps
    created_at = Column(BigInteger, default=lambda: int(time.time()))
    updated_at = Column(BigInteger, default=lambda: int(time.time()), onupdate=lambda: int(time.time()))

    __table_args__ = (
        Index("idx_game_search_guild", "guild_id"),
        Index("idx_game_search_enabled", "guild_id", "enabled"),
    )


class FoundGame(Base):
    """Cache of games found during discovery checks (for dashboard display)."""
    __tablename__ = "found_games"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=False)

    # Game identification
    igdb_id = Column(Integer, nullable=False)
    igdb_slug = Column(String(255), nullable=True)
    game_name = Column(String(255), nullable=False)

    # Game details
    release_date = Column(BigInteger, nullable=True)  # Unix timestamp
    summary = Column(Text, nullable=True)  # Game description
    genres = Column(Text, nullable=True)  # JSON array
    themes = Column(Text, nullable=True)  # JSON array
    keywords = Column(Text, nullable=True)  # JSON array (Souls-like, Metroidvania, etc.)
    game_modes = Column(Text, nullable=True)  # JSON array
    platforms = Column(Text, nullable=True)  # JSON array

    # Media & Links
    cover_url = Column(String(500), nullable=True)
    igdb_url = Column(String(500), nullable=True)
    steam_url = Column(String(500), nullable=True)  # Direct Steam store link

    # Quality metrics
    hypes = Column(Integer, nullable=True)  # Pre-release follows
    rating = Column(Float, nullable=True)  # IGDB rating

    # Discovery metadata
    search_config_id = Column(Integer, ForeignKey("game_search_configs.id", ondelete="SET NULL"), nullable=True)  # Which search found it
    search_config_name = Column(String(100), nullable=True)  # Search name (for display)
    found_at = Column(BigInteger, nullable=False)  # When it was found
    check_id = Column(String(50), nullable=True)  # Unique ID for each check run

    __table_args__ = (
        Index("idx_found_game_guild", "guild_id"),
        Index("idx_found_game_check", "guild_id", "check_id"),
        Index("idx_found_game_igdb", "guild_id", "igdb_id"),
        Index("idx_found_game_search", "guild_id", "search_config_id"),
    )


class DiscoveryNetwork(Base):
    """Guilds in the cross-server discovery network."""
    __tablename__ = "discovery_network"

    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), primary_key=True)

    is_active = Column(Boolean, default=True)
    allow_incoming = Column(Boolean, default=True)
    allow_outgoing = Column(Boolean, default=True)

    network_channel_id = Column(BigInteger, nullable=True)
    categories = Column(String(500), default="gaming,streaming,content")

    joined_at = Column(BigInteger, default=lambda: int(time.time()))


class ServerListing(Base):
    """Server listings for the discovery directory (PRO only)."""
    __tablename__ = "server_listings"

    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), primary_key=True)

    # Listing details
    title = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)
    invite_code = Column(String(50), nullable=True)
    banner_url = Column(String(500), nullable=True)

    # Categories (comma-separated)
    categories = Column(String(500), default="gaming")
    tags = Column(String(500), nullable=True)

    # Stats (updated periodically)
    member_count = Column(Integer, default=0)
    online_count = Column(Integer, default=0)
    boost_level = Column(Integer, default=0)

    # Visibility
    is_published = Column(Boolean, default=False)
    is_verified = Column(Boolean, default=False)
    is_nsfw = Column(Boolean, default=False)

    # Metrics
    views = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    joins_from_discovery = Column(Integer, default=0)

    created_at = Column(BigInteger, default=lambda: int(time.time()))
    updated_at = Column(BigInteger, default=lambda: int(time.time()))

    __table_args__ = (
        Index("idx_listing_published", "is_published", "categories"),
        Index("idx_listing_members", "member_count"),
    )


# Subscriptions

class Subscription(Base):
    """Premium subscription tracking."""
    __tablename__ = "subscriptions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))

    stripe_customer_id = Column(String(255), nullable=True)
    stripe_subscription_id = Column(String(255), nullable=True)

    tier = Column(SQLEnum(SubscriptionTier), default=SubscriptionTier.COMPLETE)
    price_cents = Column(Integer, default=1499)
    billing_period = Column(String(20), default="monthly")

    started_at = Column(BigInteger, default=lambda: int(time.time()))
    current_period_start = Column(BigInteger, nullable=True)
    current_period_end = Column(BigInteger, nullable=True)
    canceled_at = Column(BigInteger, nullable=True)

    is_active = Column(Boolean, default=True)
    cancel_at_period_end = Column(Boolean, default=False)

    __table_args__ = (
        Index("idx_subscription_guild", "guild_id"),
        Index("idx_subscription_stripe", "stripe_subscription_id"),
    )


# Level Requirements

class LevelRequirement(Base):
    """XP required for each level (shared across guilds)."""
    __tablename__ = "level_requirements"

    level = Column(Integer, primary_key=True)
    xp_required = Column(Integer, nullable=False)


class XPBoostEvent(Base):
    """XP boost events with multipliers and token bonuses."""
    __tablename__ = "xp_boost_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    multiplier = Column(Float, default=2.0, nullable=False)
    start_time = Column(BigInteger, nullable=True)
    end_time = Column(BigInteger, nullable=True)
    is_active = Column(Boolean, default=False, nullable=False)
    is_default = Column(Boolean, default=False, nullable=False)
    scope = Column(String(50), default='server', nullable=False)  # 'server', 'role', 'channel'
    scope_id = Column(BigInteger, nullable=True)  # role_id or channel_id
    token_bonus = Column(Integer, default=0, nullable=False)  # Bonus tokens per 100 XP
    announcement_channel_id = Column(BigInteger, nullable=True)
    announcement_role_id = Column(BigInteger, nullable=True)
    created_at = Column(BigInteger, default=lambda: int(time.time()), nullable=False)
    updated_at = Column(BigInteger, default=lambda: int(time.time()), onupdate=lambda: int(time.time()), nullable=False)


# Migration Log

class MigrationLog(Base):
    """Track data migrations."""
    __tablename__ = "migration_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    migration_name = Column(String(255), nullable=False)
    started_at = Column(BigInteger, default=lambda: int(time.time()))
    completed_at = Column(BigInteger, nullable=True)
    records_migrated = Column(Integer, default=0)
    errors = Column(Text, nullable=True)
    status = Column(String(50), default="running")


# IAM - Temporary Roles

class TempRole(Base):
    """Temporary role assignments with auto-expiry."""
    __tablename__ = "temp_roles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))
    user_id = Column(BigInteger, nullable=False)
    role_id = Column(BigInteger, nullable=False)

    # Assignment info
    assigned_by = Column(BigInteger, nullable=False)
    assigned_at = Column(BigInteger, default=lambda: int(time.time()))
    expires_at = Column(BigInteger, nullable=False)
    reason = Column(String(500), nullable=True)

    # Event info (for charity events, etc.)
    event_name = Column(String(255), nullable=True)
    is_active = Column(Boolean, default=True)
    revoked_at = Column(BigInteger, nullable=True)
    revoked_by = Column(BigInteger, nullable=True)

    __table_args__ = (
        Index("idx_temp_role_guild", "guild_id", "expires_at"),
        Index("idx_temp_role_active", "is_active", "expires_at"),
        Index("idx_temp_role_user", "guild_id", "user_id"),
    )


class RoleRequest(Base):
    """Role request system for approval workflow."""
    __tablename__ = "role_requests"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))
    user_id = Column(BigInteger, nullable=False)
    role_id = Column(BigInteger, nullable=False)

    # Request details
    reason = Column(Text, nullable=True)
    requested_at = Column(BigInteger, default=lambda: int(time.time()))

    # For temp role requests
    is_temp_request = Column(Boolean, default=False)
    requested_duration_hours = Column(Integer, nullable=True)
    event_name = Column(String(255), nullable=True)

    # Approval status
    status = Column(String(20), default="pending")  # pending, approved, denied
    reviewed_by = Column(BigInteger, nullable=True)
    reviewed_at = Column(BigInteger, nullable=True)
    review_note = Column(String(500), nullable=True)

    # Message tracking for button interactions
    message_id = Column(BigInteger, nullable=True)
    channel_id = Column(BigInteger, nullable=True)

    __table_args__ = (
        Index("idx_role_request_guild", "guild_id", "status"),
        Index("idx_role_request_user", "guild_id", "user_id"),
    )


class ModAction(Base):
    """Track moderator actions for audit purposes."""
    __tablename__ = "mod_actions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))

    # Who did what
    mod_id = Column(BigInteger, nullable=False)
    mod_name = Column(String(255), nullable=True)
    action_type = Column(String(50), nullable=False)

    # Target
    target_id = Column(BigInteger, nullable=True)
    target_name = Column(String(255), nullable=True)
    target_type = Column(String(50), nullable=True)  # user, role, channel

    # Details
    reason = Column(Text, nullable=True)
    details = Column(Text, nullable=True)
    duration = Column(Integer, nullable=True)  # Duration in minutes (for timeouts)
    timestamp = Column(BigInteger, default=lambda: int(time.time()))

    __table_args__ = (
        Index("idx_mod_action_guild", "guild_id", "timestamp"),
        Index("idx_mod_action_mod", "guild_id", "mod_id"),
        Index("idx_mod_action_type", "guild_id", "action_type"),
    )


# Moderation - Warnings

class WarningType(str, Enum):
    """Types of warnings."""
    MANUAL = "manual"           # Mod issued manually
    AUTO_SLUR = "auto_slur"     # Auto-detected slur/ism
    AUTO_SPAM = "auto_spam"     # Auto-detected spam
    AUTO_CAPS = "auto_caps"     # Excessive caps
    AUTO_LINKS = "auto_links"   # Unauthorized links
    AUTO_MENTION = "auto_mention"  # Mass mentions


class Warning(Base):
    """User warnings for moderation tracking."""
    __tablename__ = "warnings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))
    user_id = Column(BigInteger, nullable=False)

    # Warning details
    warning_type = Column(SQLEnum(WarningType), default=WarningType.MANUAL)
    reason = Column(Text, nullable=False)
    severity = Column(Integer, default=1)  # 1=minor, 2=moderate, 3=severe

    # What triggered it
    triggered_content = Column(Text, nullable=True)  # The message that triggered
    matched_pattern = Column(String(255), nullable=True)  # Which filter matched

    # Who issued it
    issued_by = Column(BigInteger, nullable=True)  # NULL for auto-mod
    issued_by_name = Column(String(255), nullable=True)
    issued_at = Column(BigInteger, default=lambda: int(time.time()))

    # Status
    is_active = Column(Boolean, default=True)
    expires_at = Column(BigInteger, nullable=True)  # Optional expiration
    pardoned = Column(Boolean, default=False)
    pardoned_by = Column(BigInteger, nullable=True)
    pardoned_at = Column(BigInteger, nullable=True)
    pardon_reason = Column(String(500), nullable=True)

    # Actions taken
    action_taken = Column(String(50), nullable=True)  # timeout, jail, mute, etc.
    action_duration_minutes = Column(Integer, nullable=True)

    __table_args__ = (
        Index("idx_warning_guild_user", "guild_id", "user_id"),
        Index("idx_warning_guild_active", "guild_id", "is_active"),
        Index("idx_warning_guild_time", "guild_id", "issued_at"),
    )


class WelcomeConfig(Base):
    """Per-guild welcome message settings."""
    __tablename__ = "welcome_configs"

    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), primary_key=True)

    # Welcome message toggle
    enabled = Column(Boolean, default=True)

    # Channel welcome message
    channel_message_enabled = Column(Boolean, default=True)
    channel_message = Column(Text, default="Welcome to **{server}**, {user}! You are member #{member_count}.")
    channel_embed_enabled = Column(Boolean, default=True)
    channel_embed_title = Column(String(255), default="Welcome!")
    channel_embed_color = Column(Integer, default=0x5865F2)  # Discord blurple
    channel_embed_thumbnail = Column(Boolean, default=True)  # Show user avatar
    channel_embed_footer = Column(String(255), nullable=True)

    # DM welcome message
    dm_enabled = Column(Boolean, default=False)
    dm_message = Column(Text, default="Welcome to **{server}**! Please read the rules and enjoy your stay.")

    # Goodbye message
    goodbye_enabled = Column(Boolean, default=False)
    goodbye_message = Column(Text, default="**{username}** has left the server.")
    goodbye_channel_id = Column(BigInteger, nullable=True)  # Where to send goodbye messages

    # Auto-role on join (separate from verified role)
    auto_role_id = Column(BigInteger, nullable=True)

    # Timestamps
    updated_at = Column(BigInteger, default=lambda: int(time.time()))

    __table_args__ = (
        Index("idx_welcome_config_guild", "guild_id"),
    )


class LevelUpConfig(Base):
    """Per-guild level-up message settings."""
    __tablename__ = "levelup_configs"

    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), primary_key=True)

    # Master toggle
    enabled = Column(Boolean, default=True)

    # Where to send level-up messages
    # Options: 'current' (same channel), 'channel' (specific channel), 'dm' (direct message), 'none'
    destination = Column(String(20), default="current")

    # Message settings
    message = Column(Text, default="Congrats {user}! You've reached **Level {level}**!")
    use_embed = Column(Boolean, default=True)
    embed_color = Column(Integer, default=0x5865F2)
    show_progress = Column(Boolean, default=True)  # Show XP progress bar in embed
    ping_user = Column(Boolean, default=True)  # @mention the user

    # Role reward announcement
    announce_role_reward = Column(Boolean, default=True)
    role_reward_message = Column(Text, default="You've also earned the **{role}** role!")

    # Milestone settings (special messages for specific levels)
    milestone_levels = Column(Text, nullable=True)  # JSON: [10, 25, 50, 100]
    milestone_message = Column(Text, default="Incredible! You've hit the **Level {level}** milestone!")

    # Quiet hours (don't send messages during these hours)
    quiet_hours_enabled = Column(Boolean, default=False)
    quiet_hours_start = Column(Integer, default=22)  # 10 PM
    quiet_hours_end = Column(Integer, default=8)  # 8 AM

    # Timestamps
    updated_at = Column(BigInteger, default=lambda: int(time.time()))

    __table_args__ = (
        Index("idx_levelup_config_guild", "guild_id"),
    )


class ChannelTemplate(Base):
    """Reusable channel setup templates."""
    __tablename__ = "channel_templates"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))

    name = Column(String(100), nullable=False)
    description = Column(String(500), nullable=True)

    # Template data (JSON string)
    # Structure: {"channels": [{"name": "...", "type": "text|voice|category", "position": 0, "permissions": {...}}]}
    template_data = Column(Text, nullable=False)

    # Metadata
    created_by = Column(BigInteger, nullable=True)
    created_at = Column(BigInteger, default=lambda: int(time.time()))
    updated_at = Column(BigInteger, default=lambda: int(time.time()))
    use_count = Column(Integer, default=0)

    __table_args__ = (
        Index("idx_channel_template_guild", "guild_id"),
    )


class RoleTemplate(Base):
    """Reusable role hierarchy templates."""
    __tablename__ = "role_templates"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))

    name = Column(String(100), nullable=False)
    description = Column(String(500), nullable=True)

    # Template data (JSON string)
    # Structure: {"roles": [{"name": "...", "color": 0x000000, "permissions": 0, "hoist": false, "mentionable": false}]}
    template_data = Column(Text, nullable=False)

    # Metadata
    created_by = Column(BigInteger, nullable=True)
    created_at = Column(BigInteger, default=lambda: int(time.time()))
    updated_at = Column(BigInteger, default=lambda: int(time.time()))
    use_count = Column(Integer, default=0)

    __table_args__ = (
        Index("idx_role_template_guild", "guild_id"),
    )


class ChannelStatTracker(Base):
    """Track role members and game activity in channel topics."""
    __tablename__ = "channel_stat_trackers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))

    # What to track
    role_id = Column(BigInteger, nullable=False)  # Role to count members
    channel_id = Column(BigInteger, nullable=False)  # Channel to update topic

    # Display settings
    label = Column(String(100), nullable=False)  # e.g., "Pantheon Heroes"
    emoji = Column(String(100), nullable=True)  # e.g., "<:Hero:123>" or "🧙"

    # Game tracking (optional)
    game_name = Column(String(100), nullable=True)  # e.g., "pantheon" - matches Discord activity
    show_playing_count = Column(Boolean, default=False)  # Show "X currently playing"

    # Feature toggle
    enabled = Column(Boolean, default=True)
    update_interval_seconds = Column(Integer, default=60)

    # Last update tracking
    last_updated = Column(BigInteger, nullable=True)
    last_topic = Column(String(500), nullable=True)

    # Timestamps
    created_at = Column(BigInteger, default=lambda: int(time.time()))
    created_by = Column(BigInteger, nullable=True)

    __table_args__ = (
        UniqueConstraint("guild_id", "channel_id", name="uq_guild_channel_tracker"),
        Index("idx_tracker_guild", "guild_id"),
        Index("idx_tracker_enabled", "enabled"),
    )

    def __repr__(self):
        return f"<ChannelStatTracker(guild={self.guild_id}, channel={self.channel_id}, label={self.label})>"


class ModerationConfig(Base):
    """Per-guild moderation settings."""
    __tablename__ = "moderation_configs"

    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), primary_key=True)

    # Auto-mod toggles
    automod_enabled = Column(Boolean, default=True)
    filter_slurs = Column(Boolean, default=True)
    strict_slur_filter = Column(Boolean, default=False)  # Include optional patterns that may have false positives
    filter_spam = Column(Boolean, default=False)
    filter_caps = Column(Boolean, default=False)
    filter_links = Column(Boolean, default=False)
    filter_mass_mentions = Column(Boolean, default=False)

    # Filter settings
    caps_threshold = Column(Integer, default=70)  # % of message that's caps
    caps_min_length = Column(Integer, default=10)  # Min chars before checking
    mention_limit = Column(Integer, default=5)  # Max mentions per message
    link_whitelist = Column(Text, nullable=True)  # JSON list of allowed domains

    # Custom word filters (JSON arrays)
    custom_blocked_words = Column(Text, nullable=True)
    custom_blocked_patterns = Column(Text, nullable=True)  # Regex patterns

    # Escalation settings
    warnings_before_timeout = Column(Integer, default=3)
    timeout_duration_minutes = Column(Integer, default=60)  # 1 hour
    warnings_before_jail = Column(Integer, default=5)
    warning_decay_days = Column(Integer, default=30)  # Warnings older than X days don't count

    # Auto-mod actions
    slur_action = Column(String(50), default="warn_delete")  # warn, warn_delete, timeout, jail
    spam_action = Column(String(50), default="warn_delete")
    caps_action = Column(String(50), default="warn")

    # Logging
    log_deleted_messages = Column(Boolean, default=True)
    log_edits = Column(Boolean, default=True)
    dm_on_warn = Column(Boolean, default=True)
    dm_on_timeout = Column(Boolean, default=True)

    __table_args__ = (
        Index("idx_mod_config_guild", "guild_id"),
    )


# Action Queue - Website to Bot Communication

class PendingAction(Base):
    """
    Queue for actions triggered from the website that need bot execution.

    The Django website writes actions here, and the bot polls this table
    to process actions in near real-time. This enables immediate Discord
    actions from the web dashboard without direct socket communication.
    """
    __tablename__ = "pending_actions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))

    # Action details
    action_type = Column(SQLEnum(ActionType, values_callable=lambda x: [e.value for e in x]), nullable=False)
    status = Column(SQLEnum(ActionStatus, values_callable=lambda x: [e.value for e in x]), default=ActionStatus.PENDING)
    priority = Column(Integer, default=5)  # 1=highest, 10=lowest

    # Payload (JSON) - contains action-specific data
    # Examples:
    #   ROLE_ADD: {"user_id": 123, "role_id": 456}
    #   XP_BULK_SET: {"users": [{"user_id": 123, "xp": 1000}, ...]}
    #   MESSAGE_SEND: {"channel_id": 123, "content": "Hello"}
    payload = Column(Text, nullable=False)  # JSON string

    # Who triggered this action
    triggered_by = Column(BigInteger, nullable=True)  # User ID from website
    triggered_by_name = Column(String(255), nullable=True)
    source = Column(String(50), default="website")  # website, api, csv_import

    # Timestamps
    created_at = Column(BigInteger, default=lambda: int(time.time()))
    started_at = Column(BigInteger, nullable=True)  # When processing began
    completed_at = Column(BigInteger, nullable=True)

    # Result
    result = Column(Text, nullable=True)  # JSON - success details or error info
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)

    __table_args__ = (
        Index("idx_pending_guild_status", "guild_id", "status"),
        Index("idx_pending_status_priority", "status", "priority", "created_at"),
        Index("idx_pending_created", "created_at"),
    )

    def __repr__(self):
        return f"<PendingAction(id={self.id}, guild={self.guild_id}, type={self.action_type}, status={self.status})>"


class BulkImportJob(Base):
    """
    Track bulk import jobs (CSV uploads) for progress monitoring.
    Pro/Premium feature for mass operations.
    """
    __tablename__ = "bulk_import_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))

    # Job details
    job_type = Column(String(50), nullable=False)  # role_assign, xp_set, member_import
    filename = Column(String(255), nullable=True)
    status = Column(String(50), default="pending")  # pending, processing, completed, failed

    # Progress tracking
    total_records = Column(Integer, default=0)
    processed_records = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)

    # Error details (JSON array of errors)
    errors = Column(Text, nullable=True)

    # Who triggered
    triggered_by = Column(BigInteger, nullable=True)
    triggered_by_name = Column(String(255), nullable=True)

    # Timestamps
    created_at = Column(BigInteger, default=lambda: int(time.time()))
    started_at = Column(BigInteger, nullable=True)
    completed_at = Column(BigInteger, nullable=True)

    __table_args__ = (
        Index("idx_bulk_import_guild", "guild_id", "created_at"),
        Index("idx_bulk_import_status", "status"),
    )

    def __repr__(self):
        return f"<BulkImportJob(id={self.id}, guild={self.guild_id}, type={self.job_type}, status={self.status})>"


# =============================================================================
# LFG (Looking For Group) System
# =============================================================================

class LFGGame(Base):
    """Games configured for LFG in a guild."""
    __tablename__ = "lfg_games"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))

    # Game identification
    game_name = Column(String(100), nullable=False)  # e.g., "Monster Hunter: Wilds"
    game_short = Column(String(20), nullable=False)  # e.g., "MHW" - used in commands
    game_emoji = Column(String(50), nullable=True)  # Optional emoji

    # IGDB Integration (FREE feature - game search)
    igdb_id = Column(Integer, nullable=True)  # IGDB game ID
    igdb_slug = Column(String(100), nullable=True)  # IGDB URL slug
    cover_url = Column(String(500), nullable=True)  # Game cover art from IGDB
    platforms = Column(String(255), nullable=True)  # Comma-separated: "PC,PS5,Xbox"
    is_custom_game = Column(Boolean, default=False)  # True if not from IGDB

    # Channel configuration
    lfg_channel_id = Column(BigInteger, nullable=True)  # Where LFG threads are created
    notify_role_id = Column(BigInteger, nullable=True)  # Role to ping for new groups

    # Game-specific options (JSON) - PREMIUM/PRO ONLY
    # Structure: {"options": [{"name": "Weapon", "choices": ["Sword", "Bow", ...]}, ...]}
    # For role tagging: {"name": "Spec", "choices": [{"value": "Protection", "role": "tank"}, ...]}
    custom_options = Column(Text, nullable=True)

    # Role detection mode for raids:
    # - 'builtin': Use hardcoded mappings (WoW, FFXIV, Pantheon) - highest priority
    # - 'custom': Use admin-defined role tags in custom_options - overrides generic
    # - 'generic': Show Tank/Healer/DPS/Support/Flex dropdown - default fallback
    role_detection_mode = Column(String(20), default='generic')

    # Group settings
    max_group_size = Column(Integer, default=4)
    thread_auto_archive_hours = Column(Integer, default=24)

    # Feature toggles
    enabled = Column(Boolean, default=True)
    require_rank = Column(Boolean, default=False)  # Require rank/level input (PREMIUM)
    rank_label = Column(String(50), default="Rank")  # e.g., "Hunter Rank", "Power Level"
    rank_min = Column(Integer, default=1)
    rank_max = Column(Integer, default=999)

    # Live player count tracking (privacy-focused - just count, no names)
    current_player_count = Column(Integer, default=0)  # How many members are currently playing
    player_count_updated_at = Column(BigInteger, nullable=True)  # When count was last updated

    # Timestamps
    created_at = Column(BigInteger, default=lambda: int(time.time()))
    created_by = Column(BigInteger, nullable=True)

    __table_args__ = (
        UniqueConstraint("guild_id", "game_short", name="uq_guild_game_short"),
        Index("idx_lfg_game_guild", "guild_id"),
        Index("idx_lfg_game_igdb", "igdb_id"),
    )


class LFGGroup(Base):
    """Active LFG groups/threads."""
    __tablename__ = "lfg_groups"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))
    game_id = Column(Integer, ForeignKey("lfg_games.id", ondelete="CASCADE"))

    # Thread info
    thread_id = Column(BigInteger, nullable=False)
    thread_name = Column(String(255), nullable=True)
    management_message_id = Column(BigInteger, nullable=True)
    ping_role_id = Column(BigInteger, nullable=True)  # Role to ping when creating thread

    # Creator info
    creator_id = Column(BigInteger, nullable=False)
    creator_name = Column(String(255), nullable=True)

    # Group details
    scheduled_time = Column(BigInteger, nullable=True)  # Unix timestamp for scheduled play
    description = Column(Text, nullable=True)
    custom_data = Column(Text, nullable=True)  # JSON - game-specific selections
    max_group_size = Column(Integer, nullable=True)  # Override game's default max size for this specific group
    event_duration_hours = Column(Float, nullable=True)  # How long the event will last (in hours)

    # Role composition (for any group type - dungeons, raids, etc.)
    is_raid = Column(Boolean, default=False)  # Legacy - now use role fields directly
    tanks_needed = Column(Integer, nullable=True)
    healers_needed = Column(Integer, nullable=True)
    dps_needed = Column(Integer, nullable=True)
    support_needed = Column(Integer, nullable=True)
    enforce_role_limits = Column(Boolean, default=True)  # If True, can't exceed role counts

    # Status
    is_active = Column(Boolean, default=True)
    is_full = Column(Boolean, default=False)
    member_count = Column(Integer, default=1)

    # Timestamps
    created_at = Column(BigInteger, default=lambda: int(time.time()))
    archived_at = Column(BigInteger, nullable=True)

    __table_args__ = (
        Index("idx_lfg_group_guild", "guild_id", "is_active"),
        Index("idx_lfg_group_thread", "thread_id"),
    )


class LFGMember(Base):
    """Members in an LFG group."""
    __tablename__ = "lfg_members"

    id = Column(Integer, primary_key=True, autoincrement=True)
    group_id = Column(Integer, ForeignKey("lfg_groups.id", ondelete="CASCADE"))
    user_id = Column(BigInteger, nullable=False)

    # Member info
    display_name = Column(String(255), nullable=True)
    rank_value = Column(Integer, nullable=True)  # e.g., Hunter Rank 150
    selections = Column(Text, nullable=True)  # JSON - their option selections

    # Explicit role selection (for generic/custom role detection modes)
    # Values: 'tank', 'healer', 'dps', 'support', 'flex', or NULL (auto-detect)
    selected_role = Column(String(20), nullable=True)

    # Status
    is_creator = Column(Boolean, default=False)
    is_co_leader = Column(Boolean, default=False)  # Co-leaders can manage the group
    joined_at = Column(BigInteger, default=lambda: int(time.time()))
    left_at = Column(BigInteger, nullable=True)

    __table_args__ = (
        UniqueConstraint("group_id", "user_id", name="uq_group_member"),
        Index("idx_lfg_member_group", "group_id"),
        Index("idx_lfg_member_user", "user_id"),
    )


class FeedbackConfig(Base):
    """Per-guild feedback/suggestion system config."""
    __tablename__ = "feedback_configs"

    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), primary_key=True)

    enabled = Column(Boolean, default=True)
    feedback_channel_id = Column(BigInteger, nullable=True)  # Where feedback goes
    anonymous = Column(Boolean, default=True)  # Hide submitter info

    # Timestamps
    created_at = Column(BigInteger, default=lambda: int(time.time()))

    __table_args__ = (
        Index("idx_feedback_config_guild", "guild_id"),
    )


class SuggestionStatus(str, Enum):
    """Status of a suggestion."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    IMPLEMENTED = "implemented"
    UNDER_REVIEW = "under_review"


class Suggestion(Base):
    """Member suggestions/feedback submissions."""
    __tablename__ = "suggestions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))
    user_id = Column(BigInteger, nullable=False)

    # Suggestion content
    title = Column(String(200), nullable=False)
    content = Column(Text, nullable=False)
    category = Column(String(50), nullable=True)  # feature, bug, improvement, etc.

    # Message tracking
    message_id = Column(BigInteger, nullable=True)  # The suggestion message in the channel
    channel_id = Column(BigInteger, nullable=True)

    # Status
    status = Column(SQLEnum(SuggestionStatus), default=SuggestionStatus.PENDING)
    status_note = Column(String(500), nullable=True)  # Admin response/note

    # Voting
    upvotes = Column(Integer, default=0)
    downvotes = Column(Integer, default=0)

    # Admin handling
    reviewed_by = Column(BigInteger, nullable=True)
    reviewed_at = Column(BigInteger, nullable=True)

    # Timestamps
    created_at = Column(BigInteger, default=lambda: int(time.time()))
    updated_at = Column(BigInteger, default=lambda: int(time.time()))

    __table_args__ = (
        Index("idx_suggestion_guild", "guild_id", "status"),
        Index("idx_suggestion_user", "guild_id", "user_id"),
        Index("idx_suggestion_votes", "guild_id", "upvotes"),
    )


# =============================================================================
# LFG Attendance & Reliability Tracking (PREMIUM)
# =============================================================================

class AttendanceStatus(str, Enum):
    """Attendance status for LFG events."""
    PENDING = "pending"       # Hasn't responded yet
    CONFIRMED = "confirmed"   # Confirmed they're coming
    SHOWED = "showed"         # Actually showed up
    NO_SHOW = "no_show"       # Didn't show up
    CANCELLED = "cancelled"   # Cancelled in advance
    LATE = "late"             # Showed up late
    PARDONED = "pardoned"     # No-show was pardoned (valid excuse)


class LFGAttendance(Base):
    """Track attendance for LFG groups (PREMIUM feature)."""
    __tablename__ = "lfg_attendance"

    id = Column(Integer, primary_key=True, autoincrement=True)
    group_id = Column(Integer, ForeignKey("lfg_groups.id", ondelete="CASCADE"))
    user_id = Column(BigInteger, nullable=False)

    # Attendance tracking
    status = Column(SQLEnum(AttendanceStatus, values_callable=lambda x: [e.value for e in x]), default=AttendanceStatus.PENDING)
    confirmed_at = Column(BigInteger, nullable=True)  # When they confirmed
    showed_at = Column(BigInteger, nullable=True)  # When they showed up
    cancelled_at = Column(BigInteger, nullable=True)  # When they cancelled
    late_at = Column(BigInteger, nullable=True)  # When they were marked late
    no_show_at = Column(BigInteger, nullable=True)  # When they were marked no-show
    pardoned_at = Column(BigInteger, nullable=True)  # When they were pardoned

    # Notes
    cancel_reason = Column(String(500), nullable=True)
    pardon_reason = Column(String(500), nullable=True)  # Reason for pardon
    marked_by = Column(BigInteger, nullable=True)  # Who marked their attendance

    # Timestamps
    joined_at = Column(BigInteger, nullable=True)  # When they joined the group
    created_at = Column(BigInteger, default=lambda: int(time.time()))
    updated_at = Column(BigInteger, default=lambda: int(time.time()), onupdate=lambda: int(time.time()))

    __table_args__ = (
        UniqueConstraint("group_id", "user_id", name="uq_attendance_group_user"),
        Index("idx_attendance_group", "group_id"),
        Index("idx_attendance_user", "user_id"),
        Index("idx_attendance_status", "status"),
    )


class LFGMemberStats(Base):
    """Per-guild member LFG reliability stats (PREMIUM feature)."""
    __tablename__ = "lfg_member_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))
    user_id = Column(BigInteger, nullable=False)

    # Reliability metrics
    total_signups = Column(Integer, default=0)  # How many groups they joined
    total_showed = Column(Integer, default=0)  # How many they showed up to
    total_no_shows = Column(Integer, default=0)  # How many they flaked on
    total_cancelled = Column(Integer, default=0)  # How many they cancelled in advance
    total_late = Column(Integer, default=0)  # How many they were late to
    total_pardoned = Column(Integer, default=0)  # How many no-shows were pardoned

    # Calculated reliability score (0-100)
    reliability_score = Column(Integer, default=100)

    # Streaks
    current_show_streak = Column(Integer, default=0)  # Current consecutive shows
    best_show_streak = Column(Integer, default=0)  # Best ever streak
    current_noshow_streak = Column(Integer, default=0)  # Current no-show streak

    # Blacklist/Whitelist
    is_blacklisted = Column(Boolean, default=False)
    blacklisted_at = Column(BigInteger, nullable=True)
    blacklisted_by = Column(BigInteger, nullable=True)
    blacklist_reason = Column(String(500), nullable=True)
    blacklist_pardoned = Column(Boolean, default=False)  # Admin granted permanent pardon from auto-blacklist
    blacklist_pardoned_at = Column(BigInteger, nullable=True)  # When pardon was granted

    # Timestamps
    first_event = Column(BigInteger, nullable=True)
    last_event = Column(BigInteger, nullable=True)
    updated_at = Column(BigInteger, default=lambda: int(time.time()))

    __table_args__ = (
        UniqueConstraint("guild_id", "user_id", name="uq_lfg_member_stats"),
        Index("idx_lfg_stats_guild", "guild_id"),
        Index("idx_lfg_stats_user", "user_id"),
        Index("idx_lfg_stats_reliability", "guild_id", "reliability_score"),
        Index("idx_lfg_stats_blacklist", "guild_id", "is_blacklisted"),
    )


class LFGConfig(Base):
    """Per-guild LFG configuration (includes premium settings)."""
    __tablename__ = "lfg_configs"

    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), primary_key=True)

    # Premium: Attendance tracking
    attendance_tracking_enabled = Column(Boolean, default=False)
    auto_noshow_hours = Column(Integer, default=1)  # Hours after start to mark as no-show
    require_confirmation = Column(Boolean, default=False)  # Require members to confirm

    # Premium: Reliability thresholds
    min_reliability_score = Column(Integer, default=0)  # Min score to join groups
    warn_at_reliability = Column(Integer, default=50)  # Warn when below this
    auto_blacklist_noshows = Column(Integer, default=0)  # Auto-blacklist after X no-shows (0=disabled)

    # Premium: Notifications
    notify_on_noshow = Column(Boolean, default=False)  # Notify group when someone no-shows
    notify_channel_id = Column(BigInteger, nullable=True)  # Where to send reliability reports

    # LFG Browser Notifications (Pro/Premium/VIP)
    browser_notify_channel_id = Column(BigInteger, nullable=True)  # Channel for group announcements
    notify_on_group_create = Column(Boolean, default=True)  # Announce new groups
    notify_on_group_update = Column(Boolean, default=False)  # Announce group updates
    notify_on_group_delete = Column(Boolean, default=False)  # Announce group deletions
    notify_on_member_join = Column(Boolean, default=False)  # Announce when someone joins
    notify_on_member_leave = Column(Boolean, default=False)  # Announce when someone leaves
    dm_members_on_update = Column(Boolean, default=True)  # DM members when group is updated
    dm_members_on_delete = Column(Boolean, default=True)  # DM members when group is deleted
    webhook_url = Column(String(500), nullable=True)  # Optional webhook for custom integrations

    # Timestamps
    created_at = Column(BigInteger, default=lambda: int(time.time()))
    updated_at = Column(BigInteger, default=lambda: int(time.time()))

    __table_args__ = (
        Index("idx_lfg_config_guild", "guild_id"),
    )


class LFGGroupAuditLog(Base):
    """
    Audit log for LFG group changes.
    Tracks who created/edited/deleted groups, when, and what changed.
    Visible to admins and LFG Managers.
    """
    __tablename__ = "lfg_group_audit_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=False)
    group_id = Column(Integer, nullable=True)  # Nullable because group may be deleted

    # Action info
    action = Column(String(50), nullable=False)  # 'create', 'update', 'delete'
    actor_id = Column(BigInteger, nullable=False)  # Discord user ID who performed the action
    actor_name = Column(String(255), nullable=True)  # Discord username at time of action

    # Change tracking
    field_changed = Column(String(100), nullable=True)  # Which field was changed (for updates)
    old_value = Column(Text, nullable=True)  # Previous value (JSON for complex types)
    new_value = Column(Text, nullable=True)  # New value (JSON for complex types)

    # Group snapshot (for context)
    group_name = Column(String(255), nullable=True)  # Thread name at time of action
    game_name = Column(String(255), nullable=True)  # Game name at time of action

    # Timestamp
    created_at = Column(BigInteger, default=lambda: int(time.time()), nullable=False)

    __table_args__ = (
        Index("idx_audit_guild", "guild_id"),
        Index("idx_audit_group", "group_id"),
        Index("idx_audit_actor", "actor_id"),
        Index("idx_audit_created", "created_at"),
    )


# Flair Store

class GuildFlair(Base):
    """
    Configurable flairs for the flair store.
    Supports per-guild customization of flair names, costs, and types.
    Premium guilds can create custom flairs.
    """
    __tablename__ = "guild_flairs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=False)

    # Flair details
    flair_name = Column(String(100), nullable=False)
    flair_type = Column(
        SQLEnum('normal', 'seasonal', 'custom', name='flairtype'),
        nullable=False,
        default='normal'
    )
    cost = Column(Integer, nullable=False, default=0)
    enabled = Column(Boolean, default=True)

    # Custom flair tracking
    created_by = Column(BigInteger, nullable=True)  # User ID who created (null for defaults)
    display_order = Column(Integer, default=0)  # For sorting

    # Timestamps
    created_at = Column(BigInteger, default=lambda: int(time.time()))
    updated_at = Column(BigInteger, default=lambda: int(time.time()))

    __table_args__ = (
        Index("idx_guild_flair_guild", "guild_id"),
        Index("idx_guild_flair_type", "guild_id", "flair_type"),
        Index("idx_guild_flair_enabled", "guild_id", "enabled"),
        UniqueConstraint("guild_id", "flair_name", name="uq_guild_flair_name"),
    )


# Raffles (synced with dashboard)
class Raffle(Base):
    __tablename__ = "raffles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, index=True)
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    cost_tokens = Column(Integer, default=0)
    max_winners = Column(Integer, default=1)
    max_entries_per_user = Column(Integer, nullable=True)  # Max entries one person can buy (null = unlimited)
    start_at = Column(BigInteger, nullable=True)
    end_at = Column(BigInteger, nullable=True)
    auto_pick = Column(Boolean, default=False)
    active = Column(Boolean, default=True)
    winners = Column(Text, nullable=True)
    winners_announced = Column(Boolean, default=False)  # Track if winners have been announced
    announce_channel_id = Column(BigInteger, nullable=True)
    announce_role_id = Column(BigInteger, nullable=True)
    announce_message = Column(Text, nullable=True)
    winner_message = Column(Text, nullable=True)
    entry_emoji = Column(String(32), nullable=True)
    announce_message_id = Column(BigInteger, nullable=True)
    reminder_channel_id = Column(BigInteger, nullable=True)  # Channel to send admin pick reminders
    reminder_sent = Column(Boolean, default=False)  # Track if admin reminder has been sent
    created_by = Column(BigInteger, nullable=True)
    created_by_name = Column(String(255), nullable=True)
    created_at = Column(BigInteger, default=lambda: int(time.time()))

    entries = relationship("RaffleEntry", cascade="all, delete-orphan", back_populates="raffle")


class RaffleEntry(Base):
    __tablename__ = "raffle_entries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, index=True)
    raffle_id = Column(Integer, ForeignKey("raffles.id", ondelete="CASCADE"), index=True)
    user_id = Column(BigInteger, index=True)
    username = Column(String(255), nullable=True)
    tickets = Column(Integer, default=1)
    created_at = Column(BigInteger, default=lambda: int(time.time()))

    raffle = relationship("Raffle", back_populates="entries")


# Scheduled Messages
class MessageType(str, Enum):
    """Types of scheduled messages."""
    MESSAGE = "message"
    EMBED = "embed"
    BROADCAST = "broadcast"


class MessageStatus(str, Enum):
    """Status of a scheduled message."""
    PENDING = "pending"
    SENT = "sent"
    CANCELLED = "cancelled"
    FAILED = "failed"


class ScheduledMessage(Base):
    """Scheduled messages to be sent at a specific time."""
    __tablename__ = "scheduled_messages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))

    message_type = Column(SQLEnum('message', 'embed', 'broadcast', name='messagetype'), nullable=False)
    channel_id = Column(BigInteger, nullable=True)
    category_id = Column(BigInteger, nullable=True)

    scheduled_time = Column(BigInteger, nullable=False)
    timezone = Column(String(50), nullable=False, default='UTC')

    content_data = Column(Text, nullable=False)  # JSON

    status = Column(SQLEnum('pending', 'sent', 'cancelled', 'failed', name='messagestatus'), nullable=False, default='pending')
    sent_at = Column(BigInteger, nullable=True)
    error_message = Column(Text, nullable=True)

    created_by = Column(BigInteger, nullable=False)
    created_at = Column(BigInteger, nullable=False)
    updated_at = Column(BigInteger, nullable=False)

    __table_args__ = (
        Index("idx_scheduled_message_guild", "guild_id"),
        Index("idx_scheduled_message_status", "status", "scheduled_time"),
        Index("idx_scheduled_message_pending", "guild_id", "status", "scheduled_time"),
    )


# ==================== DISCOVERY NETWORK APPLICATION SYSTEM ====================

class DiscoveryNetworkApplication(Base):
    """
    Applications for Discovery Network cross-server creator listing.
    Requires admin review before approval.
    """
    __tablename__ = "discovery_network_applications"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"))

    # Application data
    bio = Column(Text, nullable=False)  # Creator bio/description
    twitch_url = Column(Text, nullable=True)
    youtube_url = Column(Text, nullable=True)
    twitter_url = Column(Text, nullable=True)
    tiktok_url = Column(Text, nullable=True)
    instagram_url = Column(Text, nullable=True)
    bsky_url = Column(Text, nullable=True)
    other_links = Column(Text, nullable=True)  # JSON array

    # Cached Discord profile data at time of application
    username = Column(String(255), nullable=False)
    display_name = Column(String(255), nullable=True)
    avatar_url = Column(Text, nullable=True)
    account_created_at = Column(BigInteger, nullable=False)  # Discord account creation timestamp

    # Guidelines acceptance
    guidelines_accepted = Column(Boolean, default=False, nullable=False)
    tos_accepted = Column(Boolean, default=False, nullable=False)
    content_policy_accepted = Column(Boolean, default=False, nullable=False)

    # Application status
    status = Column(String(20), nullable=False, default='pending')  # pending, approved, denied, banned

    # Review data
    reviewed_by = Column(BigInteger, nullable=True)  # Admin who reviewed
    reviewed_at = Column(BigInteger, nullable=True)
    review_notes = Column(Text, nullable=True)  # Admin notes
    denial_reason = Column(Text, nullable=True)  # Public reason shown to user

    # Timestamps
    applied_at = Column(BigInteger, nullable=False, default=lambda: int(time.time()))
    updated_at = Column(BigInteger, nullable=False, default=lambda: int(time.time()))

    __table_args__ = (
        Index("idx_discovery_app_user", "user_id"),
        Index("idx_discovery_app_status", "status"),
        Index("idx_discovery_app_guild", "guild_id", "status"),
        Index("idx_discovery_app_pending", "status", "applied_at"),
    )


class DiscoveryNetworkBan(Base):
    """
    Permanent bans from Discovery Network for guideline violations.
    Prevents reapplication.
    """
    __tablename__ = "discovery_network_bans"

    user_id = Column(BigInteger, primary_key=True)

    # Ban details
    reason = Column(Text, nullable=False)  # Public reason
    violation_type = Column(String(50), nullable=False)  # discord_tos, nsfw_content, harassment, spam, etc.
    evidence = Column(Text, nullable=True)  # JSON: screenshots, links, etc.

    # Ban metadata
    banned_by = Column(BigInteger, nullable=False)  # Admin who issued ban
    banned_at = Column(BigInteger, nullable=False, default=lambda: int(time.time()))

    # Appeal tracking
    appeal_allowed = Column(Boolean, default=True)  # Can they appeal?
    appeal_submitted = Column(Boolean, default=False)
    appeal_text = Column(Text, nullable=True)
    appeal_reviewed = Column(Boolean, default=False)
    appeal_approved = Column(Boolean, default=False)
    appeal_reviewed_by = Column(BigInteger, nullable=True)
    appeal_reviewed_at = Column(BigInteger, nullable=True)

    # Cached user data
    username = Column(String(255), nullable=False)

    __table_args__ = (
        Index("idx_discovery_ban_user", "user_id"),
        Index("idx_discovery_ban_appeal", "appeal_submitted", "appeal_reviewed"),
    )


# ============================================================================
# CREATOR DISCOVERY SYSTEM - Phase 1
# ============================================================================

class StreamPlatform(str, Enum):
    """Supported streaming platforms."""
    TWITCH = "twitch"
    YOUTUBE = "youtube"
    KICK = "kick"


class CreatorProfile(Base):
    """
    Creator profiles - Users who register as content creators.
    One profile per user per guild (can be in multiple guilds).
    """
    __tablename__ = "creator_profiles"

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Identity
    discord_id = Column(BigInteger, nullable=False, index=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id"), nullable=False, index=True)

    # Profile content
    display_name = Column(String(100), nullable=False)  # How they want to be shown
    bio = Column(Text, nullable=True)  # Max 500 chars enforced in form
    content_categories = Column(Text, nullable=True)  # JSON array: ["Gaming", "Art", "Music"]

    # Social media handles (not linked, just for display)
    twitter_handle = Column(String(100), nullable=True)
    tiktok_handle = Column(String(100), nullable=True)
    instagram_handle = Column(String(100), nullable=True)
    bluesky_handle = Column(String(100), nullable=True)
    twitch_handle = Column(String(100), nullable=True)
    youtube_handle = Column(String(100), nullable=True)

    # YouTube Integration (OAuth)
    youtube_channel_id = Column(String(100), nullable=True)  # Official YT channel ID
    youtube_access_token = Column(Text, nullable=True)  # Encrypted OAuth token
    youtube_refresh_token = Column(Text, nullable=True)  # Encrypted refresh token
    youtube_token_expires = Column(BigInteger, nullable=True)  # Token expiry timestamp
    youtube_subscriber_count = Column(Integer, nullable=True)  # Cached from API
    youtube_video_count = Column(Integer, nullable=True)  # Cached from API
    youtube_last_synced = Column(BigInteger, nullable=True)  # Last API sync
    avatar_url = Column(Text, nullable=True)  # YouTube avatar URL
    user_id = Column(BigInteger, nullable=True)  # Discord user ID for lookups

    # Twitch Integration (OAuth) - Future
    twitch_user_id = Column(String(100), nullable=True)  # Official Twitch user ID
    twitch_access_token = Column(Text, nullable=True)  # Encrypted OAuth token
    twitch_refresh_token = Column(Text, nullable=True)  # Encrypted refresh token
    twitch_token_expires = Column(BigInteger, nullable=True)  # Token expiry timestamp
    twitch_follower_count = Column(Integer, nullable=True)  # Cached from API
    twitch_last_synced = Column(BigInteger, nullable=True)  # Last API sync

    # Live Stream Status (updated by background job)
    is_live_youtube = Column(Boolean, default=False, nullable=False)
    is_live_twitch = Column(Boolean, default=False, nullable=False)
    current_stream_title = Column(String(255), nullable=True)
    current_stream_game = Column(String(255), nullable=True)
    current_stream_started_at = Column(BigInteger, nullable=True)
    current_stream_thumbnail = Column(Text, nullable=True)  # URL to thumbnail
    current_stream_viewer_count = Column(Integer, nullable=True)

    # Stream schedule (free text field)
    stream_schedule = Column(Text, nullable=True)

    # Hero Token Tips
    total_tips_received = Column(Integer, default=0, nullable=False)

    # Stats and metadata
    times_featured = Column(Integer, default=0)  # COTW/COTM count
    is_current_cotw = Column(Boolean, default=False)  # Currently Creator of the Week (guild-specific)
    is_current_cotm = Column(Boolean, default=False)  # Currently Creator of the Month (guild-specific)
    cotw_last_featured = Column(BigInteger, nullable=True)  # Timestamp (guild-specific)
    cotm_last_featured = Column(BigInteger, nullable=True)  # Timestamp (guild-specific)

    # Network-level featured creator status (separate from guild-specific)
    is_current_network_cotw = Column(Boolean, default=False)  # Currently Network Creator of the Week
    is_current_network_cotm = Column(Boolean, default=False)  # Currently Network Creator of the Month
    network_cotw_last_featured = Column(BigInteger, nullable=True)  # Timestamp
    network_cotm_last_featured = Column(BigInteger, nullable=True)  # Timestamp

    # Discovery Network opt-in
    share_to_network = Column(Boolean, default=False)  # Can admins share this creator to network?

    # Timestamps
    created_at = Column(BigInteger, nullable=False, default=lambda: int(time.time()))
    updated_at = Column(BigInteger, nullable=False, default=lambda: int(time.time()), onupdate=lambda: int(time.time()))

    # Relationships
    guild = relationship("Guild", back_populates="creator_profiles")
    linked_accounts = relationship("LinkedAccount", back_populates="creator_profile", cascade="all, delete-orphan")
    stream_sessions = relationship("LiveStreamSession", back_populates="creator_profile", cascade="all, delete-orphan", foreign_keys="LiveStreamSession.creator_profile_id")

    __table_args__ = (
        UniqueConstraint("discord_id", "guild_id", name="uq_creator_per_guild"),
        Index("idx_creator_guild", "guild_id"),
        Index("idx_creator_discord", "discord_id"),
        Index("idx_creator_cotw", "is_current_cotw"),
        Index("idx_creator_cotm", "is_current_cotm"),
        Index("idx_creator_network", "share_to_network"),
    )


class LinkedAccount(Base):
    """
    OAuth-linked streaming accounts (Twitch, YouTube, Kick).
    Stores encrypted tokens and platform-specific data.
    Discovery Module feature only.
    """
    __tablename__ = "linked_accounts"

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign keys
    creator_profile_id = Column(Integer, ForeignKey("creator_profiles.id"), nullable=False, index=True)

    # Platform info
    platform = Column(SQLEnum(StreamPlatform), nullable=False)
    platform_user_id = Column(String(255), nullable=False)  # Twitch user ID, YouTube channel ID, etc.
    platform_username = Column(String(255), nullable=False)  # Display name on platform
    platform_avatar_url = Column(Text, nullable=True)  # Profile picture from platform

    # OAuth tokens (encrypted in production)
    access_token = Column(Text, nullable=False)  # Encrypted
    refresh_token = Column(Text, nullable=True)  # Encrypted
    token_expires_at = Column(BigInteger, nullable=True)  # Unix timestamp

    # Verification
    is_verified = Column(Boolean, default=False)  # Verified ownership
    is_partner = Column(Boolean, default=False)  # Twitch Partner, YouTube Partner, etc.

    # Stats (cached from API)
    follower_count = Column(Integer, default=0)
    subscriber_count = Column(Integer, default=0)  # YouTube or Twitch subs
    last_follower_milestone = Column(Integer, default=0)  # Last milestone celebrated (100, 1000, etc.)

    # Polling metadata
    last_checked = Column(BigInteger, nullable=False, default=lambda: int(time.time()))
    is_currently_live = Column(Boolean, default=False, index=True)

    # Timestamps
    linked_at = Column(BigInteger, nullable=False, default=lambda: int(time.time()))
    updated_at = Column(BigInteger, nullable=False, default=lambda: int(time.time()), onupdate=lambda: int(time.time()))

    # Relationships
    creator_profile = relationship("CreatorProfile", back_populates="linked_accounts")

    __table_args__ = (
        UniqueConstraint("creator_profile_id", "platform", name="uq_one_platform_per_creator"),
        Index("idx_linked_platform", "platform"),
        Index("idx_linked_live_status", "is_currently_live"),
        Index("idx_linked_last_checked", "last_checked"),
    )


class LiveStreamSession(Base):
    """
    Tracks individual live stream sessions.
    Created when creator goes live, ended when stream stops.
    Discovery Module feature only.
    """
    __tablename__ = "live_stream_sessions"

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign keys
    creator_profile_id = Column(Integer, ForeignKey("creator_profiles.id"), nullable=False, index=True)
    linked_account_id = Column(Integer, ForeignKey("linked_accounts.id"), nullable=False, index=True)

    # Stream metadata
    platform = Column(SQLEnum(StreamPlatform), nullable=False)
    stream_title = Column(Text, nullable=True)
    game_category = Column(String(255), nullable=True)  # Game/category being streamed
    thumbnail_url = Column(Text, nullable=True)
    stream_url = Column(Text, nullable=False)  # Direct link to stream (NO EMBED)

    # Session timing
    started_at = Column(BigInteger, nullable=False, index=True)
    ended_at = Column(BigInteger, nullable=True, index=True)
    duration_seconds = Column(Integer, nullable=True)  # Calculated when ended

    # Analytics
    peak_viewers = Column(Integer, default=0)
    average_viewers = Column(Integer, default=0)

    # Discord announcement tracking
    announcement_channel_id = Column(BigInteger, nullable=True)  # Where it was announced
    announcement_message_id = Column(BigInteger, nullable=True)  # Message ID for deletion
    announcement_deleted = Column(Boolean, default=False)  # Track if we deleted it

    # Raid tracking (Phase 2 feature)
    raided_to_creator_id = Column(Integer, ForeignKey("creator_profiles.id"), nullable=True)  # Who they raided
    raided_from_creator_id = Column(Integer, ForeignKey("creator_profiles.id"), nullable=True)  # Who raided them
    raid_viewer_count = Column(Integer, nullable=True)

    # Timestamps
    created_at = Column(BigInteger, nullable=False, default=lambda: int(time.time()))

    # Relationships
    creator_profile = relationship("CreatorProfile", back_populates="stream_sessions", foreign_keys=[creator_profile_id])
    linked_account = relationship("LinkedAccount")

    __table_args__ = (
        Index("idx_stream_creator", "creator_profile_id"),
        Index("idx_stream_platform", "platform"),
        Index("idx_stream_active", "ended_at"),  # NULL = still live
        Index("idx_stream_timing", "started_at", "ended_at"),
    )


class StreamingNotificationsConfig(Base):
    """
    Per-guild configuration for streaming notifications.
    Admins control who can get stream announcements and where they go.
    """
    __tablename__ = "streaming_notifications_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=False, unique=True)

    # Feature toggle
    enabled = Column(Boolean, default=False, nullable=False)

    # Notification settings
    notification_channel_id = Column(BigInteger, nullable=True)  # Where to post notifications
    ping_role_id = Column(BigInteger, nullable=True)  # Optional role to ping

    # Access control
    minimum_level_required = Column(Integer, default=10, nullable=False)  # Min XP level to get notifications

    # Customization
    notification_title = Column(String(256), nullable=True)  # Custom title (supports {creator} placeholder)
    notification_message = Column(String(1024), nullable=True)  # Custom message (supports {creator} placeholder)
    embed_color = Column(String(7), nullable=True)  # Hex color code (e.g., #FF0000)

    # Timestamps
    created_at = Column(BigInteger, nullable=False, default=lambda: int(time.time()))
    updated_at = Column(BigInteger, nullable=False, default=lambda: int(time.time()))

    __table_args__ = (
        Index("idx_streaming_config_guild", "guild_id"),
    )


class ApprovedStreamer(Base):
    """
    Admin-approved streamers for notifications.
    Even if streaming notifications are enabled, only approved creators get announced.
    """
    __tablename__ = "approved_streamers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=False)
    creator_profile_id = Column(Integer, ForeignKey("creator_profiles.id", ondelete="CASCADE"), nullable=False)

    # Approval metadata
    approved_by_user_id = Column(BigInteger, nullable=False)  # Discord user ID of admin
    approved_at = Column(BigInteger, nullable=False, default=lambda: int(time.time()))

    # Revocation (soft delete)
    revoked = Column(Boolean, default=False, nullable=False)
    revoked_by_user_id = Column(BigInteger, nullable=True)
    revoked_at = Column(BigInteger, nullable=True)

    __table_args__ = (
        Index("idx_approved_streamer_guild", "guild_id"),
        Index("idx_approved_streamer_creator", "creator_profile_id"),
        # One approval per creator per guild
        UniqueConstraint("guild_id", "creator_profile_id", name="uq_approved_streamer"),
    )


class StreamNotificationHistory(Base):
    """
    Tracks sent streaming notifications to prevent duplicates across bot restarts.
    Used by streaming_monitor cog to ensure we don't re-announce the same stream.
    """
    __tablename__ = "stream_notification_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=False)
    creator_profile_id = Column(Integer, ForeignKey("creator_profiles.id", ondelete="CASCADE"), nullable=False)

    # Platform and stream identifier (combined = multistreaming to both)
    platform = Column(String(20), nullable=False)  # 'youtube', 'twitch', or 'combined'
    stream_started_at = Column(BigInteger, nullable=False)  # Unix timestamp when stream started

    # Notification metadata
    notified_at = Column(BigInteger, nullable=False, default=lambda: int(time.time()))
    stream_title = Column(String(500), nullable=True)

    # Message tracking for edits (update embed when second platform goes live)
    message_id = Column(BigInteger, nullable=True)  # Discord message ID for editing
    channel_id = Column(BigInteger, nullable=True)  # Channel where message was sent

    __table_args__ = (
        Index("idx_stream_notif_guild", "guild_id"),
        Index("idx_stream_notif_creator", "creator_profile_id"),
        Index("idx_stream_notif_lookup", "creator_profile_id", "platform", "stream_started_at"),
    )


class CreatorVideo(Base):
    """
    VODs, clips, and uploads from YouTube/Twitch.
    Synced periodically from creator's channels.
    """
    __tablename__ = "creator_videos"

    id = Column(Integer, primary_key=True, autoincrement=True)
    creator_profile_id = Column(Integer, ForeignKey("creator_profiles.id", ondelete="CASCADE"), nullable=False)

    # Platform info
    platform = Column(SQLEnum('youtube', 'twitch', name='videoplatform'), nullable=False)
    video_id = Column(String(100), nullable=False)  # Platform's video ID

    # Video metadata
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    thumbnail_url = Column(Text, nullable=True)
    duration = Column(Integer, nullable=True)  # Duration in seconds
    view_count = Column(Integer, nullable=True)
    like_count = Column(Integer, nullable=True)

    # Timestamps
    published_at = Column(BigInteger, nullable=False)  # When video was published
    synced_at = Column(BigInteger, nullable=False, default=lambda: int(time.time()))  # Last sync from API

    __table_args__ = (
        Index("idx_creator_video_creator", "creator_profile_id"),
        Index("idx_creator_video_platform", "platform"),
        Index("idx_creator_video_published", "published_at"),
        # Unique video per platform
        UniqueConstraint("platform", "video_id", name="uq_creator_video"),
    )


class CreatorTip(Base):
    """
    Hero Token tips from viewers to creators.
    100% goes to creator, no platform cut.
    """
    __tablename__ = "creator_tips"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Parties
    from_user_id = Column(BigInteger, nullable=False)  # Discord ID of tipper
    to_creator_profile_id = Column(Integer, ForeignKey("creator_profiles.id", ondelete="CASCADE"), nullable=False)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=False)

    # Tip details
    amount = Column(Integer, nullable=False)  # Hero Tokens
    message = Column(Text, nullable=True)  # Optional message from tipper

    # Timestamps
    tipped_at = Column(BigInteger, nullable=False, default=lambda: int(time.time()))

    __table_args__ = (
        Index("idx_creator_tip_creator", "to_creator_profile_id"),
        Index("idx_creator_tip_from", "from_user_id"),
        Index("idx_creator_tip_guild", "guild_id"),
        Index("idx_creator_tip_date", "tipped_at"),
    )


class RSSFeed(Base):
    """
    RSS feed configuration for automated Discord posts.
    Part of the Discovery module.

    Billing:
    - Free tier: 3 feeds max
    - Discovery Module / Complete / Lifetime: Unlimited
    """
    __tablename__ = "rss_feeds"

    id = Column(Integer, primary_key=True, autoincrement=True)
    guild_id = Column(BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=False)

    # Feed configuration
    name = Column(String(100), nullable=False)
    feed_url = Column(String(500), nullable=False)
    enabled = Column(Boolean, default=True, nullable=False)
    channel_id = Column(BigInteger, nullable=False)
    ping_role_id = Column(BigInteger, nullable=True)  # Optional role to ping when posting

    # Category filtering
    # category_filter_mode: 'none' (default), 'include', or 'exclude'
    # category_filters: JSON array of category strings to filter on
    category_filter_mode = Column(String(20), default='none', nullable=False)
    category_filters = Column(Text, nullable=True)  # JSON array: ["Guild Wars 2", "News"]

    # Polling configuration
    poll_interval_minutes = Column(Integer, default=15, nullable=False)
    last_polled_at = Column(BigInteger, nullable=True)
    last_entry_id = Column(String(500), nullable=True)
    last_entry_published = Column(BigInteger, nullable=True)

    # Embed customization (JSON)
    embed_config = Column(Text, nullable=True)

    # Error tracking
    consecutive_failures = Column(Integer, default=0, nullable=False)
    last_error = Column(String(500), nullable=True)
    last_error_at = Column(BigInteger, nullable=True)

    # Audit
    created_by = Column(BigInteger, nullable=True)
    created_at = Column(BigInteger, nullable=False, default=lambda: int(time.time()))
    updated_at = Column(BigInteger, nullable=False, default=lambda: int(time.time()))

    __table_args__ = (
        Index("idx_rss_feed_guild", "guild_id"),
        Index("idx_rss_feed_enabled", "guild_id", "enabled"),
        Index("idx_rss_feed_poll", "enabled", "last_polled_at"),
    )


class RSSFeedEntry(Base):
    """Track posted RSS entries to prevent duplicates and display on dashboard."""
    __tablename__ = "rss_feed_entries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    feed_id = Column(Integer, ForeignKey("rss_feeds.id", ondelete="CASCADE"), nullable=False)
    entry_guid = Column(String(500), nullable=False)
    entry_link = Column(String(500), nullable=True)
    entry_title = Column(String(500), nullable=True)
    entry_summary = Column(Text, nullable=True)  # Article summary/description
    entry_author = Column(String(256), nullable=True)  # Article author
    entry_thumbnail = Column(String(500), nullable=True)  # Thumbnail URL
    entry_categories = Column(Text, nullable=True)  # JSON array of categories/tags
    published_at = Column(BigInteger, nullable=True)
    posted_at = Column(BigInteger, nullable=False, default=lambda: int(time.time()))
    message_id = Column(BigInteger, nullable=True)

    __table_args__ = (
        Index("idx_rss_entry_feed", "feed_id"),
        Index("idx_rss_entry_guid", "feed_id", "entry_guid"),
        Index("idx_rss_entry_posted", "posted_at"),
        UniqueConstraint("feed_id", "entry_guid", name="uq_rss_feed_entry_guid"),
    )
