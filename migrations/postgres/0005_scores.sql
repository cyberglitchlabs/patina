-- Scoring history and per-provider running averages (PostgreSQL). Never
-- edit once released. Mirrors migrations/sqlite/0005_scores.sql. Differences:
--  * Times and counters are BIGINT (Postgres INTEGER is 32 bit).
--  * Fractions are DOUBLE PRECISION (Postgres REAL is only 4 bytes).
--  * ids use GENERATED ALWAYS AS IDENTITY.

CREATE TABLE provider_accuracy (
    provider   TEXT             NOT NULL,
    channel_id TEXT             NOT NULL,
    ema        DOUBLE PRECISION NOT NULL,
    samples    BIGINT           NOT NULL,
    updated_at BIGINT           NOT NULL,
    PRIMARY KEY (provider, channel_id)
);

CREATE TABLE provider_reliability (
    provider     TEXT   PRIMARY KEY,
    success      BIGINT NOT NULL,
    total        BIGINT NOT NULL,
    window_start BIGINT NOT NULL
);

CREATE TABLE score_history (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    slot_id        BIGINT           NOT NULL,
    provider       TEXT             NOT NULL,
    channel_id     TEXT             NOT NULL,
    scored_at      BIGINT           NOT NULL,
    score          DOUBLE PRECISION NOT NULL,
    breakdown_json TEXT             NOT NULL
);
CREATE INDEX score_history_scored_at ON score_history (scored_at);
