-- Fetch layer state (PostgreSQL). Never edit this file once released: add 0003_*.sql.
-- Mirrors migrations/sqlite/0002_fetch.sql table for table. Differences:
--  * Time and counter columns are BIGINT (Postgres INTEGER is 32 bit).
--  * Raw bodies are BYTEA (SQLite: BLOB); ids use GENERATED ALWAYS AS IDENTITY.

-- Requests made per provider per UTC calendar day ('YYYY-MM-DD').
CREATE TABLE fetch_budget (
    provider TEXT   NOT NULL,
    day      TEXT   NOT NULL,
    used     BIGINT NOT NULL,
    PRIMARY KEY (provider, day)
);

-- Windows the provider rejected (HTTP 400); not retried before bad_until.
CREATE TABLE fetch_bad_windows (
    provider     TEXT   NOT NULL,
    lineup       TEXT   NOT NULL,
    window_start BIGINT NOT NULL,
    bad_until    BIGINT NOT NULL,
    PRIMARY KEY (provider, lineup, window_start)
);

-- Raw provider responses, gzip-compressed, newest wins on read.
CREATE TABLE raw_responses (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    provider     TEXT   NOT NULL,
    lineup       TEXT   NOT NULL,
    window_start BIGINT NOT NULL,
    fetched_at   BIGINT NOT NULL,
    body_gzip    BYTEA  NOT NULL
);
CREATE INDEX raw_responses_lookup
    ON raw_responses (provider, lineup, window_start, fetched_at);
CREATE INDEX raw_responses_fetched_at ON raw_responses (fetched_at);

-- Backoff / circuit breaker state, one row per provider.
CREATE TABLE fetch_provider_state (
    provider             TEXT PRIMARY KEY,
    consecutive_failures BIGINT NOT NULL,
    not_before           BIGINT,
    circuit_open_until   BIGINT,
    last_success         BIGINT,
    last_error           TEXT
);
