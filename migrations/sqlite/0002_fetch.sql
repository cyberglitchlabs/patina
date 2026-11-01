-- Fetch layer state (SQLite). Never edit this file once released: add 0003_*.sql.
-- Everything here must survive a restart, so a crash-looping process cannot
-- reset its request budget, forget a bad window, or ignore an open circuit.
-- Times are unix seconds (UTC), as in 0001_init.sql.

-- Requests made per provider per UTC calendar day ('YYYY-MM-DD').
CREATE TABLE fetch_budget (
    provider TEXT    NOT NULL,
    day      TEXT    NOT NULL,
    used     INTEGER NOT NULL,
    PRIMARY KEY (provider, day)
);

-- Windows the provider rejected (HTTP 400); not retried before bad_until.
CREATE TABLE fetch_bad_windows (
    provider     TEXT    NOT NULL,
    lineup       TEXT    NOT NULL,
    window_start INTEGER NOT NULL,
    bad_until    INTEGER NOT NULL,
    PRIMARY KEY (provider, lineup, window_start)
);

-- Raw provider responses, gzip-compressed, newest wins on read. Kept for
-- replay and debugging; the janitor deletes old rows.
CREATE TABLE raw_responses (
    id           INTEGER PRIMARY KEY,
    provider     TEXT    NOT NULL,
    lineup       TEXT    NOT NULL,
    window_start INTEGER NOT NULL,
    fetched_at   INTEGER NOT NULL,
    body_gzip    BLOB    NOT NULL
);
CREATE INDEX raw_responses_lookup
    ON raw_responses (provider, lineup, window_start, fetched_at);
CREATE INDEX raw_responses_fetched_at ON raw_responses (fetched_at);

-- Backoff / circuit breaker state, one row per provider.
CREATE TABLE fetch_provider_state (
    provider             TEXT PRIMARY KEY,
    consecutive_failures INTEGER NOT NULL,
    not_before           INTEGER,   -- no request before this (backoff, Retry-After)
    circuit_open_until   INTEGER,   -- set while the circuit is open or half-open
    last_success         INTEGER,
    last_error           TEXT
);
