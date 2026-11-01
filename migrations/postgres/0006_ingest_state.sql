-- Ingest state for the scheduler (PostgreSQL). Never edit once released: add
-- 0007_*.sql. Times are unix seconds (UTC), BIGINT (Postgres INTEGER is
-- 32 bit). Mirrors migrations/sqlite/0006_ingest_state.sql.

-- The provider's own channel list per lineup, as last reported. The matcher
-- and the override file (provider_channel_id = ...) need the provider's own
-- channel id, which the canonical `channels` table does not keep. Rows are
-- only inserted or updated, never deleted when a response omits a channel
-- (absence says nothing).
CREATE TABLE provider_channels (
    provider            TEXT    NOT NULL,
    lineup              TEXT    NOT NULL,
    provider_channel_id TEXT    NOT NULL,
    number              TEXT    NOT NULL,
    call_sign           TEXT    NOT NULL,
    canonical_id        TEXT    NOT NULL,  -- model::canonical_id(call_sign)
    last_seen           BIGINT  NOT NULL,
    PRIMARY KEY (provider, lineup, provider_channel_id)
);

-- When each 6-hour window of each lineup was last fetched successfully and
-- last attempted. Persisted so a restarted process knows which windows are
-- still fresh and sends no requests for them (good-neighbor rule).
CREATE TABLE window_state (
    provider     TEXT    NOT NULL,
    lineup       TEXT    NOT NULL,
    window_start BIGINT  NOT NULL,
    last_success BIGINT,             -- NULL: never succeeded
    last_attempt BIGINT  NOT NULL,
    last_error   TEXT,               -- NULL: the last attempt succeeded
    PRIMARY KEY (provider, lineup, window_start)
);
