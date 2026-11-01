-- Stability bookkeeping per observation (SQLite). Never edit once released.
-- The merge planner (src/merge.rs) hands these to the scorer: how long a
-- provider has been reporting a slot, and how often its start, end or title
-- for that slot changed between fetches (flip-flopping).
-- Times are unix seconds (UTC). Existing rows get 0, i.e. "unknown".

ALTER TABLE observations ADD COLUMN first_seen INTEGER NOT NULL DEFAULT 0;
ALTER TABLE observations ADD COLUMN changes    INTEGER NOT NULL DEFAULT 0;
