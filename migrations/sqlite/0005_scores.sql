-- Scoring history and per-provider running averages (SQLite). Never edit
-- once released: add 0006_*.sql. Times are unix seconds (UTC).
-- See src/score.rs for how these feed the score.

-- Hindsight accuracy: a running average (EMA) per provider per channel of
-- how well the provider's earlier listings matched its later ones.
CREATE TABLE provider_accuracy (
    provider   TEXT    NOT NULL,
    channel_id TEXT    NOT NULL,
    ema        REAL    NOT NULL,   -- 0..1 (SQLite REAL is 8 bytes)
    samples    INTEGER NOT NULL,   -- samples in the average; 0 = no data
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (provider, channel_id)
);

-- Fetch reliability: a rolling success/total counter per provider. When a
-- window is a week old the counts are halved and a new window starts, so
-- recent behaviour counts more. (fetch_provider_state from 0002 only keeps
-- the current failure streak, not totals, so this is not a duplicate.)
CREATE TABLE provider_reliability (
    provider     TEXT    PRIMARY KEY,
    success      INTEGER NOT NULL,
    total        INTEGER NOT NULL,
    window_start INTEGER NOT NULL
);

-- Every score given, so scoring quality can be analysed later. slot_id is
-- not a foreign key on purpose: history outlives the slots the janitor
-- deletes, and is pruned by age on its own.
CREATE TABLE score_history (
    id             INTEGER PRIMARY KEY,
    slot_id        INTEGER NOT NULL,
    provider       TEXT    NOT NULL,
    channel_id     TEXT    NOT NULL,
    scored_at      INTEGER NOT NULL,
    score          REAL    NOT NULL,
    breakdown_json TEXT    NOT NULL   -- JSON: signal name -> 0..1
);
-- The janitor deletes by age.
CREATE INDEX score_history_scored_at ON score_history (scored_at);
