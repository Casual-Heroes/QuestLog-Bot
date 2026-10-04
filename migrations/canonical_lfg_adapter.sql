-- Canonical QuestLog LFG adapter migration for Warden (MySQL 8+)
-- Apply before setting LFG_CANONICAL_API_ENABLED=true.

ALTER TABLE lfg_groups
    ADD COLUMN canonical_group_id BIGINT NULL,
    ADD COLUMN canonical_share_token VARCHAR(100) NULL,
    ADD INDEX idx_lfg_group_canonical (canonical_group_id);

CREATE TABLE IF NOT EXISTS lfg_delivery_receipts (
    delivery_job_id VARCHAR(100) PRIMARY KEY,
    canonical_group_id BIGINT NULL,
    action VARCHAR(30) NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'processing',
    guild_id BIGINT NULL,
    channel_id BIGINT NULL,
    message_id BIGINT NULL,
    thread_id BIGINT NULL,
    callback_url VARCHAR(1000) NULL,
    payload_hash VARCHAR(64) NULL,
    error_code VARCHAR(100) NULL,
    error_message TEXT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    INDEX idx_lfg_delivery_group (canonical_group_id, guild_id),
    INDEX idx_lfg_delivery_status (status, updated_at)
);

-- Verification:
-- SHOW COLUMNS FROM lfg_groups LIKE 'canonical_group_id';
-- SHOW COLUMNS FROM lfg_groups LIKE 'canonical_share_token';
-- SHOW CREATE TABLE lfg_delivery_receipts;
