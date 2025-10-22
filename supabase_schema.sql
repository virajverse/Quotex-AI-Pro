-- QuotexAI Pro — Supabase/Postgres Schema
-- Run this once inside Supabase SQL Editor (or psql) after setting up your project.
-- All CREATE statements are idempotent (IF NOT EXISTS) so they can be re-run safely.

-- USERS: Telegram identity, auth state, premium status
CREATE TABLE IF NOT EXISTS users (
    id                BIGSERIAL PRIMARY KEY,
    telegram_id       BIGINT UNIQUE,
    name              TEXT,
    email             TEXT UNIQUE,
    is_premium        BOOLEAN DEFAULT FALSE,
    expires_at        DATE,
    last_login        TIMESTAMPTZ,
    logged_in         BOOLEAN DEFAULT FALSE
);

-- ADMIN LOGS: captures manual actions performed via admin panel
CREATE TABLE IF NOT EXISTS admin_logs (
    id          BIGSERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    admin_id    TEXT,
    action      TEXT,
    target      TEXT
);

-- VERIFICATIONS: pending USDT transaction IDs submitted by users
CREATE TABLE IF NOT EXISTS verifications (
    id          BIGSERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    telegram_id BIGINT,
    tx_id       TEXT,
    status      TEXT
);

-- PREMIUM QUEUE: FIFO queue for matching payments to users
CREATE TABLE IF NOT EXISTS premium_queue (
    id                  BIGSERIAL PRIMARY KEY,
    telegram_id         BIGINT UNIQUE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    matched_payment_id  BIGINT
);

-- PAYMENT LOGS: on-chain transactions detected by monitors (optional future automation)
CREATE TABLE IF NOT EXISTS payment_logs (
    id                  BIGSERIAL PRIMARY KEY,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    network             TEXT,
    tx_hash             TEXT UNIQUE,
    from_address        TEXT,
    to_address          TEXT,
    amount              NUMERIC(38, 8),
    status              TEXT,
    matched_telegram_id BIGINT,
    raw_json            JSONB
);

-- PENDING VERIFICATIONS: UPI / USDT manual submissions awaiting admin review
CREATE TABLE IF NOT EXISTS pending_verifications (
    id           BIGSERIAL PRIMARY KEY,
    telegram_id  BIGINT NOT NULL,
    type         TEXT NOT NULL CHECK (type IN ('upi', 'usdt')),
    data         TEXT NOT NULL,
    timestamp    TIMESTAMPTZ DEFAULT NOW()
);

-- Helpful indexes for admin dashboards & automations
CREATE INDEX IF NOT EXISTS idx_users_last_login ON users (last_login);
CREATE INDEX IF NOT EXISTS idx_payment_logs_created ON payment_logs (created_at);
CREATE INDEX IF NOT EXISTS idx_premium_queue_created ON premium_queue (created_at);

-- Optional rollback helper: uncomment to drop everything (use with caution)
-- DROP TABLE IF EXISTS pending_verifications;
-- DROP TABLE IF EXISTS payment_logs;
-- DROP TABLE IF EXISTS premium_queue;
-- DROP TABLE IF EXISTS verifications;
-- DROP TABLE IF EXISTS admin_logs;
-- DROP TABLE IF EXISTS users;
