-- Last-known channel list per channel source (SQLite). Never edit once released.
-- Lets the service keep serving the previous lineup when a tuner is unreachable.
-- Times are unix seconds (UTC).

-- One row per source that has ever saved a list. A source whose list is empty
-- still has a row here, which is how "saved an empty list" differs from
-- "never saved".
CREATE TABLE tuner_channel_lists (
    source_key TEXT    PRIMARY KEY,
    saved_at   INTEGER NOT NULL
);

CREATE TABLE tuner_channels (
    source_key TEXT    NOT NULL,
    position   INTEGER NOT NULL,   -- order within the list as the source gave it
    number     TEXT    NOT NULL,
    name       TEXT    NOT NULL,
    PRIMARY KEY (source_key, position)
);
