//! PostgreSQL implementation of `Store`. Same behaviour as the SQLite backend;
//! the SQL is written separately because Postgres numbers its placeholders
//! ($1, $2, ...) where SQLite uses `?`, and has different column types.

use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Postgres, Row, Transaction};

use super::resolve_slot;
use super::{AccuracyRow, RELIABILITY_WINDOW, ReliabilityRow, ScoreHistoryRow};
use super::{
    DeletedImages, GuideStats, ProviderFetchState, RawEntry, StateTable, Store, WindowFailure,
    WindowState,
};
use super::{channel_number_key, cutoff_secs, day_key};
use crate::channels::TunerChannel;
use crate::fetch::rawcache::{gunzip, gzip};
use crate::merge::{
    MergeApplied, MergePlan, MergeStats, NewSlot, SlotWithObservations, StoredObservation,
};
use crate::model::{
    CanonicalChannel, ChannelId, MasterSlot, Observation, ProviderId, ScoreBreakdown, SlotId,
    canonical_id,
};
use crate::provider::ProviderChannel;

pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    pub async fn open(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .context("cannot connect to the PostgreSQL database")?;
        Ok(Self { pool })
    }
}

/// Unix seconds -> DateTime.
fn from_unix(secs: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp(secs, 0).with_context(|| format!("timestamp {secs} out of range"))
}

/// Slots of one channel overlapping the half-open range [from, to): a slot
/// overlaps when it starts before `to` and ends after `from`. Binds: channel,
/// to, from. LEFT JOIN keeps slots that have no image (image_id is NULL).
const SLOTS_SQL: &str = "SELECT s.id, s.channel_id, s.start_utc, s.end_utc, s.title, \
    s.subtitle, s.description, i.source_url AS image_url, s.season, s.episode, s.rating, \
    s.genres, s.winner_provider, s.score, s.breakdown, s.updated_at \
    FROM slots s LEFT JOIN images i ON i.id = s.image_id \
    WHERE s.channel_id = $1 AND s.start_utc < $2 AND s.end_utc > $3 \
    ORDER BY s.start_utc";

fn slot_from_row(r: &PgRow) -> Result<MasterSlot> {
    let genres: String = r.try_get("genres")?;
    let breakdown: String = r.try_get("breakdown")?;
    Ok(MasterSlot {
        id: SlotId(r.try_get("id")?),
        channel: ChannelId(r.try_get("channel_id")?),
        start: from_unix(r.try_get("start_utc")?)?,
        end: from_unix(r.try_get("end_utc")?)?,
        title: r.try_get("title")?,
        subtitle: r.try_get("subtitle")?,
        description: r.try_get("description")?,
        image_url: r.try_get("image_url")?,
        season: opt_u32(r.try_get("season")?)?,
        episode: opt_u32(r.try_get("episode")?)?,
        rating: r.try_get("rating")?,
        genres: serde_json::from_str(&genres).context("bad genres JSON")?,
        winner: ProviderId(r.try_get("winner_provider")?),
        score: r.try_get("score")?,
        breakdown: ScoreBreakdown(serde_json::from_str(&breakdown).context("bad breakdown JSON")?),
        updated_at: from_unix(r.try_get("updated_at")?)?,
    })
}

/// season/episode are INTEGER (32 bit) in Postgres, and Postgres is strict
/// about types, so they are read as i32; the model uses u32.
fn opt_u32(v: Option<i32>) -> Result<Option<u32>> {
    v.map(|v| u32::try_from(v).context("season/episode out of range"))
        .transpose()
}

/// The `images` row for this URL (created if new), or None for no image.
/// "DO UPDATE SET source_url = excluded.source_url" changes nothing; it is
/// there because "DO NOTHING" would return no id for an existing row.
async fn upsert_image(
    tx: &mut Transaction<'_, Postgres>,
    url: Option<&str>,
) -> Result<Option<i64>> {
    let Some(url) = url else { return Ok(None) };
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO images (source_url) VALUES ($1) \
         ON CONFLICT (source_url) DO UPDATE SET source_url = excluded.source_url RETURNING id",
    )
    .bind(url)
    .fetch_one(&mut **tx)
    .await?;
    Ok(Some(id))
}

/// u32 -> i32 for the 32-bit INTEGER columns (season, episode, changes).
fn to_i32(v: u32) -> Result<i32> {
    i32::try_from(v).context("value too large for a Postgres INTEGER column")
}

/// Optional version of `to_i32`.
fn opt_i32(v: Option<u32>) -> Result<Option<i32>> {
    v.map(to_i32).transpose()
}

#[async_trait]
impl Store for PostgresStore {
    async fn migrate(&self) -> Result<()> {
        // Migration files are compiled into the binary; sqlx records which ones
        // ran (in its _sqlx_migrations table), so calling this again is a no-op.
        sqlx::migrate!("migrations/postgres")
            .run(&self.pool)
            .await
            .context("database migration failed")
    }

    async fn ping(&self) -> Result<()> {
        // `execute` ignores the result value, so the column type (INT4 on
        // Postgres, INTEGER on SQLite) does not matter.
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    async fn guide_stats(&self, now: DateTime<Utc>) -> Result<GuideStats> {
        let now = now.timestamp();
        let slots_total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM slots")
            .fetch_one(&self.pool)
            .await?;
        let future_slots_total: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM slots WHERE end_utc > $1")
                .bind(now)
                .fetch_one(&self.pool)
                .await?;
        // MAX over no rows is NULL, hence the Option.
        let newest: Option<i64> = sqlx::query_scalar("SELECT MAX(end_utc) FROM slots")
            .fetch_one(&self.pool)
            .await?;
        let future_channels: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT channel_id FROM slots WHERE end_utc > $1 ORDER BY channel_id",
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await?;
        Ok(GuideStats {
            slots_total: u64::try_from(slots_total).unwrap_or(0),
            future_slots_total: u64::try_from(future_slots_total).unwrap_or(0),
            newest_slot_end: newest.and_then(|t| DateTime::from_timestamp(t, 0)),
            channels_with_future_slots: future_channels.len() as u64,
            future_channels: future_channels.into_iter().map(ChannelId).collect(),
        })
    }

    async fn upsert_channels(&self, channels: &[CanonicalChannel]) -> Result<()> {
        // One transaction: either all channels are written or none.
        let mut tx = self.pool.begin().await?;
        for c in channels {
            // "ON CONFLICT (id) DO UPDATE" = insert, or if the id exists
            // already, update that row. "excluded" is the row we tried to insert.
            sqlx::query(
                "INSERT INTO channels (id, number, name, call_sign) VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (id) DO UPDATE SET \
                 number = excluded.number, name = excluded.name, call_sign = excluded.call_sign",
            )
            .bind(&c.id.0)
            .bind(&c.number)
            .bind(&c.name)
            .bind(&c.call_sign)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn list_channels(&self) -> Result<Vec<CanonicalChannel>> {
        let rows = sqlx::query("SELECT id, number, name, call_sign FROM channels")
            .fetch_all(&self.pool)
            .await?;
        let mut channels: Vec<CanonicalChannel> = rows
            .iter()
            .map(|r| CanonicalChannel {
                id: ChannelId(r.get("id")),
                number: r.get("number"),
                name: r.get("name"),
                call_sign: r.get("call_sign"),
            })
            .collect();
        // Sorted in Rust (shared key function) so both backends order alike.
        channels.sort_by(|a, b| {
            channel_number_key(&a.number)
                .cmp(&channel_number_key(&b.number))
                .then_with(|| a.id.cmp(&b.id))
        });
        Ok(channels)
    }

    async fn slots(
        &self,
        channel: ChannelId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<MasterSlot>> {
        let rows = sqlx::query(SLOTS_SQL)
            .bind(&channel.0)
            .bind(to.timestamp())
            .bind(from.timestamp())
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(slot_from_row).collect()
    }

    async fn slots_with_observations(
        &self,
        channel: &ChannelId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<SlotWithObservations>> {
        // One transaction so both reads see the same database state.
        // Postgres's default (READ COMMITTED) gives each statement its own
        // snapshot; REPEATABLE READ makes both reads share one.
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *tx)
            .await?;
        let slot_rows = sqlx::query(SLOTS_SQL)
            .bind(&channel.0)
            .bind(to.timestamp())
            .bind(from.timestamp())
            .fetch_all(&mut *tx)
            .await?;
        // Observations of exactly those slots (same WHERE as SLOTS_SQL).
        let obs_rows = sqlx::query(
            "SELECT o.slot_id, o.provider, o.start_utc, o.end_utc, o.title, o.subtitle, \
                    o.description, o.image_url, o.season, o.episode, o.rating, o.genres, \
                    o.fetched_at, o.first_seen, o.changes \
             FROM observations o JOIN slots s ON s.id = o.slot_id \
             WHERE s.channel_id = $1 AND s.start_utc < $2 AND s.end_utc > $3 \
             ORDER BY o.slot_id, o.provider",
        )
        .bind(&channel.0)
        .bind(to.timestamp())
        .bind(from.timestamp())
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;

        let mut out: Vec<SlotWithObservations> = slot_rows
            .iter()
            .map(|r| {
                Ok(SlotWithObservations {
                    slot: slot_from_row(r)?,
                    observations: Vec::new(),
                })
            })
            .collect::<Result<_>>()?;
        for r in &obs_rows {
            let slot_id = SlotId(r.try_get("slot_id")?);
            if let Some(s) = out.iter_mut().find(|s| s.slot.id == slot_id) {
                let o = StoredObservation {
                    observation: Observation {
                        provider: ProviderId(r.try_get("provider")?),
                        channel: s.slot.channel.clone(),
                        start: from_unix(r.try_get("start_utc")?)?,
                        end: from_unix(r.try_get("end_utc")?)?,
                        title: r.try_get("title")?,
                        subtitle: r.try_get("subtitle")?,
                        description: r.try_get("description")?,
                        image_url: r.try_get("image_url")?,
                        season: opt_u32(r.try_get("season")?)?,
                        episode: opt_u32(r.try_get("episode")?)?,
                        rating: r.try_get("rating")?,
                        genres: serde_json::from_str(&r.try_get::<String, _>("genres")?)
                            .context("bad genres JSON")?,
                        fetched_at: from_unix(r.try_get("fetched_at")?)?,
                    },
                    first_seen: from_unix(r.try_get("first_seen")?)?,
                    changes: u32::try_from(r.try_get::<i32, _>("changes")?)?,
                };
                s.observations.push(o);
            }
        }
        Ok(out)
    }

    async fn apply_merge(&self, plan: MergePlan) -> Result<MergeApplied> {
        // Everything happens in one transaction: if any statement fails, the
        // transaction is dropped without commit and Postgres undoes all of it.
        let mut tx = self.pool.begin().await?;
        let mut stats = MergeStats {
            unchanged: plan.unchanged,
            ..MergeStats::default()
        };
        let at = plan.planned_at.timestamp();

        // 1. Removals. Their observations go too (ON DELETE CASCADE).
        for id in &plan.removals {
            let r = sqlx::query("DELETE FROM slots WHERE id = $1")
                .bind(id.0)
                .execute(&mut *tx)
                .await?;
            stats.removed += r.rows_affected() as usize;
        }

        // 2. Single observations a provider explicitly contradicted.
        for (id, provider) in &plan.superseded {
            sqlx::query("DELETE FROM observations WHERE slot_id = $1 AND provider = $2")
                .bind(id.0)
                .bind(&provider.0)
                .execute(&mut *tx)
                .await?;
        }

        // 3. Updates, in two passes. UNIQUE (channel_id, start_utc) is checked
        // after every statement (it is not DEFERRABLE), so moving slot A to the start slot B is
        // leaving would fail if A were written before B moved. Pass one parks
        // every updated slot at a temporary start of minus its id (unique, and
        // never a real time: real starts are positive unix seconds); pass two
        // writes the final values.
        for (id, _) in &plan.updates {
            sqlx::query("UPDATE slots SET start_utc = $1 WHERE id = $2")
                .bind(-id.0)
                .bind(id.0)
                .execute(&mut *tx)
                .await?;
        }
        for (id, f) in &plan.updates {
            let image_id = upsert_image(&mut tx, f.image_url.as_deref()).await?;
            let r = sqlx::query(
                "UPDATE slots SET start_utc = $1, end_utc = $2, title = $3, subtitle = $4, \
                 description = $5, image_id = $6, season = $7, episode = $8, rating = $9, genres = $10, \
                 winner_provider = $11, score = $12, breakdown = $13, updated_at = $14 WHERE id = $15",
            )
            .bind(f.start.timestamp())
            .bind(f.end.timestamp())
            .bind(&f.title)
            .bind(&f.subtitle)
            .bind(&f.description)
            .bind(image_id)
            .bind(opt_i32(f.season)?)
            .bind(opt_i32(f.episode)?)
            .bind(&f.rating)
            .bind(serde_json::to_string(&f.genres)?)
            .bind(&f.winner.0)
            .bind(f.score)
            .bind(serde_json::to_string(&f.breakdown.0)?)
            .bind(at)
            .bind(id.0)
            .execute(&mut *tx)
            .await?;
            stats.updated += r.rows_affected() as usize;
        }

        // 4. Inserts. If the channel already has a slot at that start (the
        // same plan applied twice), "DO NOTHING" keeps the row and we look up
        // its id, so the observations below still land on it.
        let mut new_ids = Vec::with_capacity(plan.inserts.len());
        for NewSlot { channel, fields: f } in &plan.inserts {
            let image_id = upsert_image(&mut tx, f.image_url.as_deref()).await?;
            let id: Option<i64> = sqlx::query_scalar(
                "INSERT INTO slots (channel_id, start_utc, end_utc, title, subtitle, description, \
                 image_id, season, episode, rating, genres, winner_provider, score, breakdown, \
                 updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15) \
                 ON CONFLICT (channel_id, start_utc) DO NOTHING RETURNING id",
            )
            .bind(&channel.0)
            .bind(f.start.timestamp())
            .bind(f.end.timestamp())
            .bind(&f.title)
            .bind(&f.subtitle)
            .bind(&f.description)
            .bind(image_id)
            .bind(opt_i32(f.season)?)
            .bind(opt_i32(f.episode)?)
            .bind(&f.rating)
            .bind(serde_json::to_string(&f.genres)?)
            .bind(&f.winner.0)
            .bind(f.score)
            .bind(serde_json::to_string(&f.breakdown.0)?)
            .bind(at)
            .fetch_optional(&mut *tx)
            .await?;
            let id = match id {
                Some(id) => {
                    stats.inserted += 1;
                    id
                }
                None => {
                    sqlx::query_scalar(
                        "SELECT id FROM slots WHERE channel_id = $1 AND start_utc = $2",
                    )
                    .bind(&channel.0)
                    .bind(f.start.timestamp())
                    .fetch_one(&mut *tx)
                    .await?
                }
            };
            new_ids.push(id);
        }

        // 5. Observations: insert, or replace this provider's row for the slot.
        for w in &plan.observations {
            let slot_id = resolve_slot(w.slot, &new_ids)?;
            let o = &w.observation;
            sqlx::query(
                "INSERT INTO observations (slot_id, provider, start_utc, end_utc, title, \
                 subtitle, description, image_url, season, episode, rating, genres, fetched_at, \
                 first_seen, changes) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15) \
                 ON CONFLICT (slot_id, provider) DO UPDATE SET \
                 start_utc = excluded.start_utc, end_utc = excluded.end_utc, \
                 title = excluded.title, subtitle = excluded.subtitle, \
                 description = excluded.description, image_url = excluded.image_url, \
                 season = excluded.season, episode = excluded.episode, rating = excluded.rating, \
                 genres = excluded.genres, fetched_at = excluded.fetched_at, \
                 first_seen = excluded.first_seen, changes = excluded.changes",
            )
            .bind(slot_id)
            .bind(&o.provider.0)
            .bind(o.start.timestamp())
            .bind(o.end.timestamp())
            .bind(&o.title)
            .bind(&o.subtitle)
            .bind(&o.description)
            .bind(&o.image_url)
            .bind(opt_i32(o.season)?)
            .bind(opt_i32(o.episode)?)
            .bind(&o.rating)
            .bind(serde_json::to_string(&o.genres)?)
            .bind(o.fetched_at.timestamp())
            .bind(w.first_seen.timestamp())
            .bind(to_i32(w.changes)?)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(MergeApplied {
            stats,
            inserted_ids: new_ids.into_iter().map(SlotId).collect(),
        })
    }

    async fn budget_used(&self, provider: &ProviderId, day: NaiveDate) -> Result<u32> {
        let used: Option<i64> =
            sqlx::query_scalar("SELECT used FROM fetch_budget WHERE provider = $1 AND day = $2")
                .bind(&provider.0)
                .bind(day_key(day))
                .fetch_optional(&self.pool)
                .await?;
        Ok(u32::try_from(used.unwrap_or(0)).unwrap_or(u32::MAX))
    }

    async fn budget_add(&self, provider: &ProviderId, day: NaiveDate, n: u32) -> Result<u32> {
        // One statement does "insert the row, or add to it if it exists" and
        // hands back the new total, so concurrent callers cannot lose updates.
        let used: i64 = sqlx::query_scalar(
            "INSERT INTO fetch_budget (provider, day, used) VALUES ($1, $2, $3) \
             ON CONFLICT (provider, day) DO UPDATE SET used = fetch_budget.used + excluded.used \
             RETURNING used",
        )
        .bind(&provider.0)
        .bind(day_key(day))
        .bind(i64::from(n))
        .fetch_one(&self.pool)
        .await?;
        Ok(u32::try_from(used).unwrap_or(u32::MAX))
    }

    async fn bad_window_mark(
        &self,
        provider: &ProviderId,
        lineup: &str,
        window_start: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO fetch_bad_windows (provider, lineup, window_start, bad_until) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (provider, lineup, window_start) DO UPDATE SET bad_until = excluded.bad_until",
        )
        .bind(&provider.0)
        .bind(lineup)
        .bind(window_start.timestamp())
        .bind(until.timestamp())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn bad_window_is_bad(
        &self,
        provider: &ProviderId,
        lineup: &str,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        let hit: Option<i64> = sqlx::query_scalar(
            "SELECT bad_until FROM fetch_bad_windows \
             WHERE provider = $1 AND lineup = $2 AND window_start = $3 AND bad_until > $4",
        )
        .bind(&provider.0)
        .bind(lineup)
        .bind(window_start.timestamp())
        .bind(now.timestamp())
        .fetch_optional(&self.pool)
        .await?;
        Ok(hit.is_some())
    }

    async fn raw_put(
        &self,
        provider: &ProviderId,
        lineup: &str,
        window_start: DateTime<Utc>,
        fetched_at: DateTime<Utc>,
        body: &[u8],
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO raw_responses (provider, lineup, window_start, fetched_at, body_gzip) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&provider.0)
        .bind(lineup)
        .bind(window_start.timestamp())
        .bind(fetched_at.timestamp())
        .bind(gzip(body)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn raw_get_latest(
        &self,
        provider: &ProviderId,
        lineup: &str,
        window_start: DateTime<Utc>,
        max_age: Duration,
        now: DateTime<Utc>,
    ) -> Result<Option<RawEntry>> {
        // Newest first; "id DESC" breaks ties between two rows fetched in the
        // same second (the later insert wins).
        let row = sqlx::query(
            "SELECT fetched_at, body_gzip FROM raw_responses \
             WHERE provider = $1 AND lineup = $2 AND window_start = $3 AND fetched_at >= $4 \
             ORDER BY fetched_at DESC, id DESC LIMIT 1",
        )
        .bind(&provider.0)
        .bind(lineup)
        .bind(window_start.timestamp())
        .bind(cutoff_secs(now, max_age))
        .fetch_optional(&self.pool)
        .await?;
        row.map(|r| {
            let z: Vec<u8> = r.get("body_gzip");
            Ok(RawEntry {
                fetched_at: from_unix(r.get("fetched_at"))?,
                body: Bytes::from(gunzip(&z)?),
            })
        })
        .transpose()
    }

    async fn fetch_state_load(&self, provider: &ProviderId) -> Result<Option<ProviderFetchState>> {
        let row = sqlx::query(
            "SELECT consecutive_failures, not_before, circuit_open_until, last_success, last_error \
             FROM fetch_provider_state WHERE provider = $1",
        )
        .bind(&provider.0)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|r| {
            let opt_time = |col: &str| r.get::<Option<i64>, _>(col).map(from_unix).transpose();
            Ok(ProviderFetchState {
                consecutive_failures: u32::try_from(r.get::<i64, _>("consecutive_failures"))
                    .unwrap_or(u32::MAX),
                not_before: opt_time("not_before")?,
                circuit_open_until: opt_time("circuit_open_until")?,
                last_success: opt_time("last_success")?,
                last_error: r.get("last_error"),
            })
        })
        .transpose()
    }

    async fn fetch_state_save(
        &self,
        provider: &ProviderId,
        state: &ProviderFetchState,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO fetch_provider_state \
             (provider, consecutive_failures, not_before, circuit_open_until, last_success, last_error) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (provider) DO UPDATE SET \
             consecutive_failures = excluded.consecutive_failures, \
             not_before = excluded.not_before, \
             circuit_open_until = excluded.circuit_open_until, \
             last_success = excluded.last_success, \
             last_error = excluded.last_error",
        )
        .bind(&provider.0)
        .bind(i64::from(state.consecutive_failures))
        .bind(state.not_before.map(|t| t.timestamp()))
        .bind(state.circuit_open_until.map(|t| t.timestamp()))
        .bind(state.last_success.map(|t| t.timestamp()))
        .bind(&state.last_error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn save_tuner_channels(
        &self,
        source_key: &str,
        channels: &[TunerChannel],
        now: DateTime<Utc>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM tuner_channels WHERE source_key = $1")
            .bind(source_key)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO tuner_channel_lists (source_key, saved_at) VALUES ($1, $2) \
             ON CONFLICT (source_key) DO UPDATE SET saved_at = excluded.saved_at",
        )
        .bind(source_key)
        .bind(now.timestamp())
        .execute(&mut *tx)
        .await?;
        for (position, c) in channels.iter().enumerate() {
            sqlx::query(
                "INSERT INTO tuner_channels (source_key, position, number, name) \
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(source_key)
            .bind(i64::try_from(position)?)
            .bind(&c.number)
            .bind(&c.name)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn load_tuner_channels(
        &self,
        source_key: &str,
    ) -> Result<Option<(Vec<TunerChannel>, DateTime<Utc>)>> {
        // Two reads, so a snapshot is not guaranteed across them; that is
        // fine because saves replace the list in one transaction and the
        // header row is read first.
        let Some(row) =
            sqlx::query("SELECT saved_at FROM tuner_channel_lists WHERE source_key = $1")
                .bind(source_key)
                .fetch_optional(&self.pool)
                .await?
        else {
            return Ok(None);
        };
        let saved_at: i64 = row.try_get("saved_at")?;
        let rows = sqlx::query(
            "SELECT number, name FROM tuner_channels WHERE source_key = $1 ORDER BY position",
        )
        .bind(source_key)
        .fetch_all(&self.pool)
        .await?;
        let channels = rows
            .iter()
            .map(|r| {
                Ok(TunerChannel {
                    number: r.try_get("number")?,
                    name: r.try_get("name")?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some((channels, from_unix(saved_at)?)))
    }

    // ---- Scoring (see src/score.rs). ----

    async fn provider_accuracy_load_all(&self) -> Result<Vec<AccuracyRow>> {
        let rows = sqlx::query(
            "SELECT provider, channel_id, ema, samples, updated_at FROM provider_accuracy \
             ORDER BY provider, channel_id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| {
                Ok(AccuracyRow {
                    provider: ProviderId(r.try_get("provider")?),
                    channel: ChannelId(r.try_get("channel_id")?),
                    ema: r.try_get("ema")?,
                    samples: u32::try_from(r.try_get::<i64, _>("samples")?).unwrap_or(u32::MAX),
                    updated_at: from_unix(r.try_get("updated_at")?)?,
                })
            })
            .collect()
    }

    async fn provider_accuracy_upsert(&self, row: &AccuracyRow) -> Result<()> {
        sqlx::query(
            "INSERT INTO provider_accuracy (provider, channel_id, ema, samples, updated_at) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (provider, channel_id) DO UPDATE SET \
             ema = excluded.ema, samples = excluded.samples, updated_at = excluded.updated_at",
        )
        .bind(&row.provider.0)
        .bind(&row.channel.0)
        .bind(row.ema)
        .bind(i64::from(row.samples))
        .bind(row.updated_at.timestamp())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn provider_reliability_record(
        &self,
        provider: &ProviderId,
        success: bool,
        at: DateTime<Utc>,
    ) -> Result<()> {
        // One statement, so concurrent callers cannot lose a count. "rolled"
        // below means: the stored window began RELIABILITY_WINDOW or more
        // before this outcome. Then the old counts are halved (integer
        // division) and the window restarts at `at`; otherwise the outcome
        // is simply added. In an UPDATE every right-hand side sees the OLD
        // row values, so the three CASEs agree with each other.
        let window = i64::try_from(RELIABILITY_WINDOW.as_secs())?;
        sqlx::query(
            "INSERT INTO provider_reliability (provider, success, total, window_start) \
             VALUES ($1, $2, 1, $3) \
             ON CONFLICT (provider) DO UPDATE SET \
             success = CASE WHEN excluded.window_start - provider_reliability.window_start >= $4 \
                 THEN provider_reliability.success / 2 + excluded.success \
                 ELSE provider_reliability.success + excluded.success END, \
             total = CASE WHEN excluded.window_start - provider_reliability.window_start >= $5 \
                 THEN provider_reliability.total / 2 + 1 \
                 ELSE provider_reliability.total + 1 END, \
             window_start = CASE WHEN excluded.window_start - provider_reliability.window_start >= $6 \
                 THEN excluded.window_start \
                 ELSE provider_reliability.window_start END",
        )
        .bind(&provider.0)
        .bind(i64::from(success))
        .bind(at.timestamp())
        .bind(window)
        .bind(window)
        .bind(window)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn provider_reliability_load_all(&self) -> Result<Vec<ReliabilityRow>> {
        let rows = sqlx::query(
            "SELECT provider, success, total, window_start FROM provider_reliability \
             ORDER BY provider",
        )
        .fetch_all(&self.pool)
        .await?;
        let count = |v: i64| u32::try_from(v).unwrap_or(u32::MAX);
        rows.iter()
            .map(|r| {
                Ok(ReliabilityRow {
                    provider: ProviderId(r.try_get("provider")?),
                    success: count(r.try_get("success")?),
                    total: count(r.try_get("total")?),
                    window_start: from_unix(r.try_get("window_start")?)?,
                })
            })
            .collect()
    }

    async fn score_history_append(&self, rows: &[ScoreHistoryRow]) -> Result<()> {
        // One transaction: a batch is stored completely or not at all.
        let mut tx = self.pool.begin().await?;
        for row in rows {
            sqlx::query(
                "INSERT INTO score_history \
                 (slot_id, provider, channel_id, scored_at, score, breakdown_json) \
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(row.slot.0)
            .bind(&row.provider.0)
            .bind(&row.channel.0)
            .bind(row.scored_at.timestamp())
            .bind(row.score)
            .bind(serde_json::to_string(&row.breakdown)?)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn score_history_delete_older_than(
        &self,
        cutoff: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64> {
        // "DELETE ... WHERE id IN (SELECT ... LIMIT n)" is the portable way to
        // delete at most n rows; ORDER BY makes it the OLDEST n. The index on
        // scored_at serves both the filter and the order.
        let done = sqlx::query(
            "DELETE FROM score_history WHERE id IN \
             (SELECT id FROM score_history WHERE scored_at < $1 ORDER BY scored_at, id LIMIT $2)",
        )
        .bind(cutoff.timestamp())
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected())
    }

    // ---- Janitor (see src/janitor.rs). ----

    async fn delete_raw_older_than(&self, cutoff: DateTime<Utc>, limit: u32) -> Result<u64> {
        // Index raw_responses_fetched_at serves the filter and the order.
        let done = sqlx::query(
            "DELETE FROM raw_responses WHERE id IN \
             (SELECT id FROM raw_responses WHERE fetched_at < $1 ORDER BY fetched_at, id LIMIT $2)",
        )
        .bind(cutoff.timestamp())
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected())
    }

    async fn delete_aired_slots_older_than(
        &self,
        cutoff: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64> {
        // Only the slot rows are named here. Their observations disappear by
        // themselves: observations.slot_id is ON DELETE CASCADE. Index
        // slots_end_idx (migration 0007) serves the filter and the order.
        let done = sqlx::query(
            "DELETE FROM slots WHERE id IN \
             (SELECT id FROM slots WHERE end_utc < $1 ORDER BY end_utc, id LIMIT $2)",
        )
        .bind(cutoff.timestamp())
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected())
    }

    async fn images_clear_for_slots_older_than(
        &self,
        cutoff: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64> {
        // Rows that were already cleared no longer match "image_id IS NOT
        // NULL", so repeating the call makes progress and ends.
        let done = sqlx::query(
            "UPDATE slots SET image_id = NULL WHERE id IN \
             (SELECT id FROM slots WHERE image_id IS NOT NULL AND end_utc < $1 \
              ORDER BY end_utc, id LIMIT $2)",
        )
        .bind(cutoff.timestamp())
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected())
    }

    async fn images_delete_unreferenced(&self, limit: u32) -> Result<DeletedImages> {
        // slots.image_id is a foreign key to images.id, so the database itself
        // refuses to delete an image a slot still points at: even if a merge
        // links a slot to this image right now, the worst outcome is an error
        // in this statement, never a slot with a dangling image.
        // Index slots_image_id_idx (migration 0008) makes NOT EXISTS cheap.
        let paths: Vec<Option<String>> = sqlx::query_scalar(
            "DELETE FROM images WHERE id IN \
             (SELECT i.id FROM images i \
              WHERE NOT EXISTS (SELECT 1 FROM slots s WHERE s.image_id = i.id) \
              ORDER BY i.id LIMIT $1) \
             RETURNING local_path",
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        Ok(DeletedImages {
            rows: paths.len() as u64,
            local_paths: paths.into_iter().flatten().collect(),
        })
    }

    async fn delete_state_older_than(
        &self,
        table: StateTable,
        cutoff: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64> {
        // These tables have composite primary keys, so "the rows to delete"
        // is chosen with a row-value IN (a, b, c) IN (SELECT a, b, c ...).
        let (sql, bind_text): (&str, Option<String>) = match table {
            StateTable::WindowState => (
                "DELETE FROM window_state WHERE (provider, lineup, window_start) IN \
                 (SELECT provider, lineup, window_start FROM window_state \
                  WHERE window_start < $1 ORDER BY window_start LIMIT $2)",
                None,
            ),
            StateTable::ProviderChannels => (
                "DELETE FROM provider_channels WHERE (provider, lineup, provider_channel_id) IN \
                 (SELECT provider, lineup, provider_channel_id FROM provider_channels \
                  WHERE last_seen < $1 ORDER BY last_seen LIMIT $2)",
                None,
            ),
            StateTable::BadWindows => (
                "DELETE FROM fetch_bad_windows WHERE (provider, lineup, window_start) IN \
                 (SELECT provider, lineup, window_start FROM fetch_bad_windows \
                  WHERE bad_until < $1 ORDER BY bad_until LIMIT $2)",
                None,
            ),
            // Days are text 'YYYY-MM-DD', which sorts like the dates do.
            StateTable::FetchBudget => (
                "DELETE FROM fetch_budget WHERE (provider, day) IN \
                 (SELECT provider, day FROM fetch_budget WHERE day < $1 ORDER BY day LIMIT $2)",
                Some(day_key(cutoff.date_naive())),
            ),
        };
        let q = sqlx::query(sql);
        let q = match bind_text {
            Some(day) => q.bind(day),
            None => q.bind(cutoff.timestamp()),
        };
        let done = q.bind(i64::from(limit)).execute(&self.pool).await?;
        Ok(done.rows_affected())
    }

    async fn approx_size_bytes(&self) -> Result<u64> {
        // Includes dead space: PostgreSQL reuses space freed by deletes for
        // new rows but does not hand it back to the OS, so this number does
        // not fall right after a delete. The janitor's disk eviction notices
        // that (see `Janitor`).
        let bytes: i64 = sqlx::query_scalar("SELECT pg_database_size(current_database())")
            .fetch_one(&self.pool)
            .await?;
        Ok(u64::try_from(bytes).unwrap_or(0))
    }

    // ---- Scheduler state (see src/scheduler.rs). ----

    async fn provider_channels_upsert(
        &self,
        provider: &ProviderId,
        lineup: &str,
        channels: &[ProviderChannel],
        now: DateTime<Utc>,
    ) -> Result<()> {
        // One transaction: a response's channel list is stored whole or not
        // at all. Channels missing from this list are left alone.
        let mut tx = self.pool.begin().await?;
        for c in channels {
            sqlx::query(
                "INSERT INTO provider_channels \
                 (provider, lineup, provider_channel_id, number, call_sign, canonical_id, last_seen) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7) \
                 ON CONFLICT (provider, lineup, provider_channel_id) DO UPDATE SET \
                 number = excluded.number, call_sign = excluded.call_sign, \
                 canonical_id = excluded.canonical_id, last_seen = excluded.last_seen",
            )
            .bind(&provider.0)
            .bind(lineup)
            .bind(&c.provider_channel_id)
            .bind(&c.number)
            .bind(&c.call_sign)
            .bind(&canonical_id(&c.call_sign).0)
            .bind(now.timestamp())
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn provider_channels_list(
        &self,
        provider: &ProviderId,
        lineup: &str,
    ) -> Result<Vec<ProviderChannel>> {
        let rows = sqlx::query(
            "SELECT provider_channel_id, number, call_sign FROM provider_channels \
             WHERE provider = $1 AND lineup = $2 ORDER BY provider_channel_id",
        )
        .bind(&provider.0)
        .bind(lineup)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| {
                Ok(ProviderChannel {
                    provider_channel_id: r.try_get("provider_channel_id")?,
                    number: r.try_get("number")?,
                    call_sign: r.try_get("call_sign")?,
                })
            })
            .collect()
    }

    async fn window_state_get(
        &self,
        provider: &ProviderId,
        lineup: &str,
        window_start: DateTime<Utc>,
    ) -> Result<Option<WindowState>> {
        let row = sqlx::query(
            "SELECT window_start, last_success, last_attempt, last_error FROM window_state \
             WHERE provider = $1 AND lineup = $2 AND window_start = $3",
        )
        .bind(&provider.0)
        .bind(lineup)
        .bind(window_start.timestamp())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(window_state_from_row).transpose()
    }

    async fn window_state_put(
        &self,
        provider: &ProviderId,
        lineup: &str,
        state: &WindowState,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO window_state \
             (provider, lineup, window_start, last_success, last_attempt, last_error) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (provider, lineup, window_start) DO UPDATE SET \
             last_success = excluded.last_success, last_attempt = excluded.last_attempt, \
             last_error = excluded.last_error",
        )
        .bind(&provider.0)
        .bind(lineup)
        .bind(state.window_start.timestamp())
        .bind(state.last_success.map(|t| t.timestamp()))
        .bind(state.last_attempt.timestamp())
        .bind(state.last_failure.as_ref().map(WindowFailure::to_stored))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn window_state_range(
        &self,
        provider: &ProviderId,
        lineup: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<WindowState>> {
        let rows = sqlx::query(
            "SELECT window_start, last_success, last_attempt, last_error FROM window_state \
             WHERE provider = $1 AND lineup = $2 AND window_start >= $3 AND window_start < $4 \
             ORDER BY window_start",
        )
        .bind(&provider.0)
        .bind(lineup)
        .bind(from.timestamp())
        .bind(to.timestamp())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(window_state_from_row).collect()
    }
}

/// One `window_state` row (columns window_start, last_success, last_attempt,
/// last_error) as a `WindowState`.
fn window_state_from_row(r: &PgRow) -> Result<WindowState> {
    Ok(WindowState {
        window_start: from_unix(r.try_get("window_start")?)?,
        last_success: r
            .try_get::<Option<i64>, _>("last_success")?
            .map(from_unix)
            .transpose()?,
        last_attempt: from_unix(r.try_get("last_attempt")?)?,
        last_failure: r
            .try_get::<Option<String>, _>("last_error")?
            .as_deref()
            .map(WindowFailure::from_stored),
    })
}
