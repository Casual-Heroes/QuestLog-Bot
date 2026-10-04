-- Durable Warden -> QuestLog progression delivery queue (MySQL 8+).
-- Safe to apply before deploying the progression outbox code.

CREATE TABLE IF NOT EXISTS progression_outbox_events (
    event_key VARCHAR(120) PRIMARY KEY,
    guild_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    evidence_id VARCHAR(255) NOT NULL,
    occurred_at BIGINT NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'queued',
    attempt_count INT NOT NULL DEFAULT 0,
    next_attempt_at BIGINT NOT NULL DEFAULT 0,
    last_error_code VARCHAR(100) NULL,
    last_error_message TEXT NULL,
    result_payload TEXT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    delivered_at BIGINT NULL,
    INDEX idx_progression_outbox_due (status, next_attempt_at),
    INDEX idx_progression_outbox_member (guild_id, user_id)
);

-- Verification:
-- SHOW CREATE TABLE progression_outbox_events;
