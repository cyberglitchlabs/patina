-- Stability bookkeeping per observation (PostgreSQL). Never edit once released.
-- Mirrors migrations/sqlite/0004_observation_stability.sql. first_seen is
-- BIGINT (unix seconds; Postgres INTEGER is 32 bit); changes is a small
-- counter, so INTEGER is plenty.

ALTER TABLE observations ADD COLUMN first_seen BIGINT  NOT NULL DEFAULT 0;
ALTER TABLE observations ADD COLUMN changes    INTEGER NOT NULL DEFAULT 0;
