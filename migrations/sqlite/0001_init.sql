-- Initial schema (SQLite). Never edit this file once released: add 0002_*.sql.
--
-- Conventions:
--  * All times are UTC, stored as integers: seconds since 1970-01-01 (unix time).
--  * "genres" is a JSON array stored as text, e.g. '["Drama","News"]'.
--  * Plain SQL only (no SQLite-only functions) so the Postgres migrations can
--    express the same tables.

-- One row per canonical channel (what Jellyfin maps in "Map Channels").
CREATE TABLE channels (
    id        TEXT PRIMARY KEY,   -- stable id, e.g. 'WCCODT.us'
    number    TEXT NOT NULL,      -- channel number as text, e.g. '4.1'
    name      TEXT NOT NULL,
    call_sign TEXT NOT NULL
);

-- Programme images. v1 only fills source_url; the other columns stay NULL until
-- a later image cache exists (spec 4.14), so no redesign is needed then.
CREATE TABLE images (
    id           INTEGER PRIMARY KEY,   -- SQLite assigns ids automatically
    source_url   TEXT NOT NULL UNIQUE,
    content_hash TEXT,
    fetched_at   INTEGER,
    local_path   TEXT
);

-- The master guide: one row per programme airing on a channel.
CREATE TABLE slots (
    id              INTEGER PRIMARY KEY,
    channel_id      TEXT NOT NULL REFERENCES channels (id),
    start_utc       INTEGER NOT NULL,
    end_utc         INTEGER NOT NULL,
    title           TEXT NOT NULL,
    subtitle        TEXT,
    description     TEXT,
    image_id        INTEGER REFERENCES images (id),
    season          INTEGER,
    episode         INTEGER,
    rating          TEXT,
    genres          TEXT NOT NULL DEFAULT '[]',
    winner_provider TEXT NOT NULL,   -- provider whose data won arbitration
    score           REAL NOT NULL,   -- winning weighted score
    breakdown       TEXT NOT NULL DEFAULT '{}',  -- JSON: signal name -> 0..1
    updated_at      INTEGER NOT NULL,
    -- A channel cannot have two programmes starting at the same instant.
    UNIQUE (channel_id, start_utc)
);

-- What each provider said about a slot, with that provider's own times and
-- text. Used later to judge stability and agreement between providers.
CREATE TABLE observations (
    id          INTEGER PRIMARY KEY,
    slot_id     INTEGER NOT NULL REFERENCES slots (id) ON DELETE CASCADE,
    provider    TEXT NOT NULL,
    start_utc   INTEGER NOT NULL,
    end_utc     INTEGER NOT NULL,
    title       TEXT NOT NULL,
    subtitle    TEXT,
    description TEXT,
    image_url   TEXT,
    season      INTEGER,
    episode     INTEGER,
    rating      TEXT,
    genres      TEXT NOT NULL DEFAULT '[]',
    fetched_at  INTEGER NOT NULL,
    -- One row per provider per slot; later fetches update it (upsert). This
    -- constraint also creates the index that finds a slot's observations.
    UNIQUE (slot_id, provider)
);
