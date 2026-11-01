-- Index on slots.end_utc. `Store::guide_stats` (behind /status and /metrics)
-- runs `WHERE end_utc > now` counts, `MAX(end_utc)` and a DISTINCT channel
-- list; the janitor also deletes by end time. Without this index each of
-- those scans the whole table, which grows with the guide.
CREATE INDEX IF NOT EXISTS slots_end_idx ON slots (end_utc);
