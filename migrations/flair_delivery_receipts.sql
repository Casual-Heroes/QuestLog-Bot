-- Incremental replacement for the global QuestLog flair fan-out.
-- Safe to apply before deploying the matching Warden code.

CREATE TABLE IF NOT EXISTS warden_flair_delivery_receipts (
    id BIGINT NOT NULL AUTO_INCREMENT,
    source_update_id BIGINT NOT NULL,
    guild_id BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    role_id BIGINT NULL,
    last_error VARCHAR(1000) NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    processed_at BIGINT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_warden_flair_delivery (source_update_id, guild_id),
    KEY idx_warden_flair_delivery_status (status, updated_at),
    KEY idx_warden_flair_delivery_guild (guild_id, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS warden_flair_role_bindings (
    id BIGINT NOT NULL AUTO_INCREMENT,
    guild_id BIGINT NOT NULL,
    flair_key CHAR(64) NOT NULL,
    role_id BIGINT NOT NULL,
    role_name VARCHAR(100) NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_warden_flair_binding_key (guild_id, flair_key),
    UNIQUE KEY uq_warden_flair_binding_role (guild_id, role_id),
    KEY idx_warden_flair_binding_guild (guild_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
