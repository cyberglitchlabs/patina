//! One SQL store, generic over the database. SQLite and PostgreSQL run the same
//! statements, written once with `?` placeholders; [`Backend`] holds the few
//! places the two differ (connecting, migrations, the placeholder style, the
//! read snapshot and the database size).

use jiff::Timestamp;
use jiff::civil::Date;
use jiff::tz::TimeZone;
use std::borrow::Cow;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use sqlx::query::{Query, QueryScalar};
use sqlx::{
    AssertSqlSafe, ColumnIndex, Database, Decode, Encode, Executor, IntoArguments, Pool, Row,
    Transaction, Type,
};

use super::resolve_slot;
use super::{AccuracyRow, RELIABILITY_WINDOW, ReliabilityRow, ScoreHistoryRow};
use super::{
    DeletedImages, GuideStats, ProviderFetchState, RawEntry, StateTable, Store, WindowFailure,
    WindowState,
};
use super::{channel_number_key, cutoff_secs, day_key};
use crate::channels::TunerChannel;
use crate::merge::{
    MergeApplied, MergePlan, MergeStats, NewSlot, SlotWithObservations, StoredObservation,
};
use crate::model::{
    CanonicalChannel, ChannelId, MasterSlot, Observation, ProviderId, ScoreBreakdown, SlotId,
    canonical_id,
};
use crate::provider::ProviderChannel;
use crate::store::rawbody::{gunzip, gzip};

/// What differs between SQLite and PostgreSQL.
#[async_trait]
pub trait Backend: Database {
    /// Open a connection pool for `database_url`.
    async fn connect(database_url: &str) -> Result<Pool<Self>>;
    /// Run this backend's migrations (they are compiled into the binary).
    async fn migrate(pool: &Pool<Self>) -> Result<()>;
    /// Rewrite a statement written with `?` placeholders for this database.
    fn sql(sql: &'static str) -> Cow<'static, str>;
    /// Make the reads that follow in this transaction see one snapshot.
    async fn snapshot(tx: &mut Transaction<'_, Self>) -> Result<()>;
    /// Rows a statement changed.
    fn rows_affected(result: &Self::QueryResult) -> u64;
    /// Bytes of data the database holds.
    async fn size_bytes(pool: &Pool<Self>) -> Result<u64>;
}

fn query<D: Backend>(sql: &'static str) -> Query<'static, D, D::Arguments> {
    sqlx::query(AssertSqlSafe(D::sql(sql)))
}

fn query_scalar<D: Backend, O>(sql: &'static str) -> QueryScalar<'static, D, O, D::Arguments>
where
    (O,): for<'r> sqlx::FromRow<'r, D::Row>,
{
    sqlx::query_scalar(AssertSqlSafe(D::sql(sql)))
}

pub struct SqlStore<D: Database> {
    pool: Pool<D>,
}

impl<D: Backend> SqlStore<D> {
    pub async fn open(database_url: &str) -> Result<Self> {
        Ok(Self {
            pool: D::connect(database_url).await?,
        })
    }
}

/// Unix seconds -> Timestamp.
fn from_unix(secs: i64) -> Result<Timestamp> {
    Timestamp::from_second(secs).with_context(|| format!("timestamp {secs} out of range"))
}

/// Slots of one channel overlapping the half-open range [from, to): a slot
/// overlaps when it starts before `to` and ends after `from`. Binds: channel,
/// to, from. LEFT JOIN keeps slots that have no image (image_id is NULL).
const SLOTS_SQL: &str = "SELECT s.id, s.channel_id, s.start_utc, s.end_utc, s.title, \
    s.subtitle, s.description, i.source_url AS image_url, s.season, s.episode, s.rating, \
    s.genres, s.winner_provider, s.score, s.breakdown, s.updated_at \
    FROM slots s LEFT JOIN images i ON i.id = s.image_id \
    WHERE s.channel_id = ? AND s.start_utc < ? AND s.end_utc > ? \
    ORDER BY s.start_utc";

fn slot_from_row<D>(r: &D::Row) -> Result<MasterSlot>
where
    D: Database,
    for<'a> &'a str: ColumnIndex<D::Row>,
    for<'r> i64: Decode<'r, D>,
    for<'r> i32: Decode<'r, D>,
    for<'r> f64: Decode<'r, D>,
    for<'r> String: Decode<'r, D>,
    i64: Type<D>,
    i32: Type<D>,
    f64: Type<D>,
    String: Type<D>,
{
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

/// season/episode are 32-bit INTEGER columns; the model uses u32.
fn opt_u32(v: Option<i32>) -> Result<Option<u32>> {
    v.map(|v| u32::try_from(v).context("season/episode out of range"))
        .transpose()
}

/// The `images` row for this URL (created if new), or None for no image.
/// "DO UPDATE SET source_url = excluded.source_url" changes nothing; it is
/// there because "DO NOTHING" would return no id for an existing row.
async fn upsert_image<D>(tx: &mut Transaction<'_, D>, url: Option<&str>) -> Result<Option<i64>>
where
    D: Backend,
    for<'c> &'c mut D::Connection: Executor<'c, Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'q> &'q str: Encode<'q, D>,
    for<'r> i64: Decode<'r, D>,
    i64: Type<D>,
    str: Type<D>,
    (i64,): for<'r> sqlx::FromRow<'r, D::Row>,
{
    let Some(url) = url else { return Ok(None) };
    let id: i64 = query_scalar::<D, _>(
        "INSERT INTO images (source_url) VALUES (?) \
         ON CONFLICT (source_url) DO UPDATE SET source_url = excluded.source_url RETURNING id",
    )
    .bind(url)
    .fetch_one(&mut **tx)
    .await?;
    Ok(Some(id))
}

#[async_trait]
impl<D> Store for SqlStore<D>
where
    D: Backend,
    for<'e> &'e Pool<D>: Executor<'e, Database = D>,
    for<'c> &'c mut D::Connection: Executor<'c, Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'a> &'a str: ColumnIndex<D::Row>,
    for<'a> usize: ColumnIndex<D::Row>,
    for<'q> Option<String>: Encode<'q, D>,
    for<'q> Option<i64>: Encode<'q, D>,
    for<'q> Option<i32>: Encode<'q, D>,
    for<'q> Vec<u8>: Encode<'q, D>,
    for<'r> Option<i64>: Decode<'r, D>,
    for<'r> Option<String>: Decode<'r, D>,
    Option<i64>: Type<D>,
    Option<String>: Type<D>,
    (i64,): for<'r> sqlx::FromRow<'r, D::Row>,
    (Option<i64>,): for<'r> sqlx::FromRow<'r, D::Row>,
    (String,): for<'r> sqlx::FromRow<'r, D::Row>,
    (Option<String>,): for<'r> sqlx::FromRow<'r, D::Row>,
    (bool,): for<'r> sqlx::FromRow<'r, D::Row>,
    for<'q> i64: Encode<'q, D>,
    for<'q> i32: Encode<'q, D>,
    for<'q> f64: Encode<'q, D>,
    for<'q> bool: Encode<'q, D>,
    for<'q> String: Encode<'q, D>,
    for<'q> &'q str: Encode<'q, D>,
    for<'q> &'q [u8]: Encode<'q, D>,
    for<'r> i64: Decode<'r, D>,
    for<'r> i32: Decode<'r, D>,
    for<'r> f64: Decode<'r, D>,
    for<'r> bool: Decode<'r, D>,
    for<'r> String: Decode<'r, D>,
    for<'r> Vec<u8>: Decode<'r, D>,
    i64: Type<D>,
    i32: Type<D>,
    f64: Type<D>,
    bool: Type<D>,
    String: Type<D>,
    str: Type<D>,
    [u8]: Type<D>,
    Vec<u8>: Type<D>,
{
    async fn migrate(&self) -> Result<()> {
        // The migration files are compiled into the binary. sqlx records which
        // ones ran, so calling this again does nothing.
        D::migrate(&self.pool)
            .await
            .context("database migration failed")
    }

    async fn ping(&self) -> Result<()> {
        // `execute` ignores the result value, so the column type (INT4 on
        // Postgres, INTEGER on SQLite) does not matter.
        query::<D>("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    async fn guide_stats(&self, now: Timestamp) -> Result<GuideStats> {
        let now = now.as_second();
        let slots_total: i64 = query_scalar::<D, _>("SELECT COUNT(*) FROM slots")
            .fetch_one(&self.pool)
            .await?;
        let future_slots_total: i64 =
            query_scalar::<D, _>("SELECT COUNT(*) FROM slots WHERE end_utc > ?")
                .bind(now)
                .fetch_one(&self.pool)
                .await?;
        // MAX over no rows is NULL, hence the Option.
        let newest: Option<i64> = query_scalar::<D, _>("SELECT MAX(end_utc) FROM slots")
            .fetch_one(&self.pool)
            .await?;
        let future_channels: Vec<String> = query_scalar::<D, _>(
            "SELECT DISTINCT channel_id FROM slots WHERE end_utc > ? ORDER BY channel_id",
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await?;
        Ok(GuideStats {
            slots_total: u64::try_from(slots_total).unwrap_or(0),
            future_slots_total: u64::try_from(future_slots_total).unwrap_or(0),
            newest_slot_end: newest.and_then(|t| Timestamp::from_second(t).ok()),
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
            query::<D>(
                "INSERT INTO channels (id, number, name, call_sign) VALUES (?, ?, ?, ?) \
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
        let rows = query::<D>("SELECT id, number, name, call_sign FROM channels")
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
        // Sorted in Rust, because "11.1" < "4.1" as text and SQL cannot easily
        // sort "major.minor" numerically in a portable way.
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
        from: Timestamp,
        to: Timestamp,
    ) -> Result<Vec<MasterSlot>> {
        let rows = query::<D>(SLOTS_SQL)
            .bind(&channel.0)
            .bind(to.as_second())
            .bind(from.as_second())
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(slot_from_row).collect()
    }

    async fn slots_with_observations(
        &self,
        channel: &ChannelId,
        from: Timestamp,
        to: Timestamp,
    ) -> Result<Vec<SlotWithObservations>> {
        // One transaction so both reads see the same database state.
        let mut tx = self.pool.begin().await?;
        D::snapshot(&mut tx).await?;
        let slot_rows = query::<D>(SLOTS_SQL)
            .bind(&channel.0)
            .bind(to.as_second())
            .bind(from.as_second())
            .fetch_all(&mut *tx)
            .await?;
        // Observations of exactly those slots (same WHERE as SLOTS_SQL).
        let obs_rows = query::<D>(
            "SELECT o.slot_id, o.provider, o.start_utc, o.end_utc, o.title, o.subtitle, \
                    o.description, o.image_url, o.season, o.episode, o.rating, o.genres, \
                    o.fetched_at, o.first_seen, o.changes \
             FROM observations o JOIN slots s ON s.id = o.slot_id \
             WHERE s.channel_id = ? AND s.start_utc < ? AND s.end_utc > ? \
             ORDER BY o.slot_id, o.provider",
        )
        .bind(&channel.0)
        .bind(to.as_second())
        .bind(from.as_second())
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
        // transaction is dropped without commit and the database undoes all of it.
        let mut tx = self.pool.begin().await?;
        let mut stats = MergeStats {
            unchanged: plan.unchanged,
            ..MergeStats::default()
        };
        let at = plan.planned_at.as_second();

        // 1. Removals. Their observations go too (ON DELETE CASCADE).
        for id in &plan.removals {
            let r = query::<D>("DELETE FROM slots WHERE id = ?")
                .bind(id.0)
                .execute(&mut *tx)
                .await?;
            stats.removed += D::rows_affected(&r) as usize;
        }

        // 2. Single observations a provider explicitly contradicted.
        for (id, provider) in &plan.superseded {
            query::<D>("DELETE FROM observations WHERE slot_id = ? AND provider = ?")
                .bind(id.0)
                .bind(&provider.0)
                .execute(&mut *tx)
                .await?;
        }

        // 3. Updates, in two passes. UNIQUE (channel_id, start_utc) is checked
        // after every statement, so moving slot A to the start slot B is
        // leaving would fail if A were written before B moved. Pass one parks
        // every updated slot at a temporary start of minus its id (unique, and
        // never a real time: real starts are positive unix seconds); pass two
        // writes the final values.
        for (id, _) in &plan.updates {
            query::<D>("UPDATE slots SET start_utc = ? WHERE id = ?")
                .bind(-id.0)
                .bind(id.0)
                .execute(&mut *tx)
                .await?;
        }
        for (id, f) in &plan.updates {
            let image_id = upsert_image(&mut tx, f.image_url.as_deref()).await?;
            let r = query::<D>(
                "UPDATE slots SET start_utc = ?, end_utc = ?, title = ?, subtitle = ?, \
                 description = ?, image_id = ?, season = ?, episode = ?, rating = ?, genres = ?, \
                 winner_provider = ?, score = ?, breakdown = ?, updated_at = ? WHERE id = ?",
            )
            .bind(f.start.as_second())
            .bind(f.end.as_second())
            .bind(&f.title)
            .bind(&f.subtitle)
            .bind(&f.description)
            .bind(image_id)
            .bind(f.season.map(i32::try_from).transpose()?)
            .bind(f.episode.map(i32::try_from).transpose()?)
            .bind(&f.rating)
            .bind(serde_json::to_string(&f.genres)?)
            .bind(&f.winner.0)
            .bind(f.score)
            .bind(serde_json::to_string(&f.breakdown.0)?)
            .bind(at)
            .bind(id.0)
            .execute(&mut *tx)
            .await?;
            stats.updated += D::rows_affected(&r) as usize;
        }

        // 4. Inserts. If the channel already has a slot at that start (the
        // same plan applied twice), "DO NOTHING" keeps the row and we look up
        // its id, so the observations below still land on it.
        let mut new_ids = Vec::with_capacity(plan.inserts.len());
        for NewSlot { channel, fields: f } in &plan.inserts {
            let image_id = upsert_image(&mut tx, f.image_url.as_deref()).await?;
            let id: Option<i64> = query_scalar::<D, _>(
                "INSERT INTO slots (channel_id, start_utc, end_utc, title, subtitle, description, \
                 image_id, season, episode, rating, genres, winner_provider, score, breakdown, \
                 updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                 ON CONFLICT (channel_id, start_utc) DO NOTHING RETURNING id",
            )
            .bind(&channel.0)
            .bind(f.start.as_second())
            .bind(f.end.as_second())
            .bind(&f.title)
            .bind(&f.subtitle)
            .bind(&f.description)
            .bind(image_id)
            .bind(f.season.map(i32::try_from).transpose()?)
            .bind(f.episode.map(i32::try_from).transpose()?)
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
                    query_scalar::<D, _>(
                        "SELECT id FROM slots WHERE channel_id = ? AND start_utc = ?",
                    )
                    .bind(&channel.0)
                    .bind(f.start.as_second())
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
            query::<D>(
                "INSERT INTO observations (slot_id, provider, start_utc, end_utc, title, \
                 subtitle, description, image_url, season, episode, rating, genres, fetched_at, \
                 first_seen, changes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
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
            .bind(o.start.as_second())
            .bind(o.end.as_second())
            .bind(&o.title)
            .bind(&o.subtitle)
            .bind(&o.description)
            .bind(&o.image_url)
            .bind(o.season.map(i32::try_from).transpose()?)
            .bind(o.episode.map(i32::try_from).transpose()?)
            .bind(&o.rating)
            .bind(serde_json::to_string(&o.genres)?)
            .bind(o.fetched_at.as_second())
            .bind(w.first_seen.as_second())
            .bind(i32::try_from(w.changes)?)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(MergeApplied {
            stats,
            inserted_ids: new_ids.into_iter().map(SlotId).collect(),
        })
    }

    async fn budget_used(&self, provider: &ProviderId, day: Date) -> Result<u32> {
        let used: Option<i64> =
            query_scalar::<D, _>("SELECT used FROM fetch_budget WHERE provider = ? AND day = ?")
                .bind(&provider.0)
                .bind(day_key(day))
                .fetch_optional(&self.pool)
                .await?;
        Ok(u32::try_from(used.unwrap_or(0)).unwrap_or(u32::MAX))
    }

    async fn budget_add(&self, provider: &ProviderId, day: Date, n: u32) -> Result<u32> {
        // One statement does "insert the row, or add to it if it exists" and
        // hands back the new total, so concurrent callers cannot lose updates.
        let used: i64 = query_scalar::<D, _>(
            "INSERT INTO fetch_budget (provider, day, used) VALUES (?, ?, ?) \
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
        window_start: Timestamp,
        until: Timestamp,
    ) -> Result<()> {
        query::<D>(
            "INSERT INTO fetch_bad_windows (provider, lineup, window_start, bad_until) \
             VALUES (?, ?, ?, ?) \
             ON CONFLICT (provider, lineup, window_start) DO UPDATE SET bad_until = excluded.bad_until",
        )
        .bind(&provider.0)
        .bind(lineup)
        .bind(window_start.as_second())
        .bind(until.as_second())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn bad_window_is_bad(
        &self,
        provider: &ProviderId,
        lineup: &str,
        window_start: Timestamp,
        now: Timestamp,
    ) -> Result<bool> {
        let hit: Option<i64> = query_scalar::<D, _>(
            "SELECT bad_until FROM fetch_bad_windows \
             WHERE provider = ? AND lineup = ? AND window_start = ? AND bad_until > ?",
        )
        .bind(&provider.0)
        .bind(lineup)
        .bind(window_start.as_second())
        .bind(now.as_second())
        .fetch_optional(&self.pool)
        .await?;
        Ok(hit.is_some())
    }

    async fn raw_put(
        &self,
        provider: &ProviderId,
        lineup: &str,
        window_start: Timestamp,
        fetched_at: Timestamp,
        body: &[u8],
    ) -> Result<()> {
        query::<D>(
            "INSERT INTO raw_responses (provider, lineup, window_start, fetched_at, body_gzip) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&provider.0)
        .bind(lineup)
        .bind(window_start.as_second())
        .bind(fetched_at.as_second())
        .bind(gzip(body)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn raw_get_latest(
        &self,
        provider: &ProviderId,
        lineup: &str,
        window_start: Timestamp,
        max_age: Duration,
        now: Timestamp,
    ) -> Result<Option<RawEntry>> {
        // Newest first; "id DESC" breaks ties between two rows fetched in the
        // same second (the later insert wins).
        let row = query::<D>(
            "SELECT fetched_at, body_gzip FROM raw_responses \
             WHERE provider = ? AND lineup = ? AND window_start = ? AND fetched_at >= ? \
             ORDER BY fetched_at DESC, id DESC LIMIT 1",
        )
        .bind(&provider.0)
        .bind(lineup)
        .bind(window_start.as_second())
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
        let row = query::<D>(
            "SELECT consecutive_failures, not_before, circuit_open_until, last_success, last_error \
             FROM fetch_provider_state WHERE provider = ?",
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
        query::<D>(
            "INSERT INTO fetch_provider_state \
             (provider, consecutive_failures, not_before, circuit_open_until, last_success, last_error) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT (provider) DO UPDATE SET \
             consecutive_failures = excluded.consecutive_failures, \
             not_before = excluded.not_before, \
             circuit_open_until = excluded.circuit_open_until, \
             last_success = excluded.last_success, \
             last_error = excluded.last_error",
        )
        .bind(&provider.0)
        .bind(i64::from(state.consecutive_failures))
        .bind(state.not_before.map(|t| t.as_second()))
        .bind(state.circuit_open_until.map(|t| t.as_second()))
        .bind(state.last_success.map(|t| t.as_second()))
        .bind(&state.last_error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn save_tuner_channels(
        &self,
        source_key: &str,
        channels: &[TunerChannel],
        now: Timestamp,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        query::<D>("DELETE FROM tuner_channels WHERE source_key = ?")
            .bind(source_key)
            .execute(&mut *tx)
            .await?;
        query::<D>(
            "INSERT INTO tuner_channel_lists (source_key, saved_at) VALUES (?, ?) \
             ON CONFLICT (source_key) DO UPDATE SET saved_at = excluded.saved_at",
        )
        .bind(source_key)
        .bind(now.as_second())
        .execute(&mut *tx)
        .await?;
        for (position, c) in channels.iter().enumerate() {
            query::<D>(
                "INSERT INTO tuner_channels (source_key, position, number, name) \
                 VALUES (?, ?, ?, ?)",
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
    ) -> Result<Option<(Vec<TunerChannel>, Timestamp)>> {
        // Two reads, so a snapshot is not guaranteed across them; that is
        // fine because saves replace the list in one transaction and the
        // header row is read first.
        let Some(row) = query::<D>("SELECT saved_at FROM tuner_channel_lists WHERE source_key = ?")
            .bind(source_key)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };
        let saved_at: i64 = row.try_get("saved_at")?;
        let rows = query::<D>(
            "SELECT number, name FROM tuner_channels WHERE source_key = ? ORDER BY position",
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
        let rows = query::<D>(
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
        query::<D>(
            "INSERT INTO provider_accuracy (provider, channel_id, ema, samples, updated_at) \
             VALUES (?, ?, ?, ?, ?) \
             ON CONFLICT (provider, channel_id) DO UPDATE SET \
             ema = excluded.ema, samples = excluded.samples, updated_at = excluded.updated_at",
        )
        .bind(&row.provider.0)
        .bind(&row.channel.0)
        .bind(row.ema)
        .bind(i64::from(row.samples))
        .bind(row.updated_at.as_second())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn provider_reliability_record(
        &self,
        provider: &ProviderId,
        success: bool,
        at: Timestamp,
    ) -> Result<()> {
        // One statement, so concurrent callers cannot lose a count. "rolled"
        // below means: the stored window began RELIABILITY_WINDOW or more
        // before this outcome. Then the old counts are halved (integer
        // division) and the window restarts at `at`; otherwise the outcome
        // is simply added. In an UPDATE every right-hand side sees the OLD
        // row values, so the three CASEs agree with each other.
        let window = i64::try_from(RELIABILITY_WINDOW.as_secs())?;
        query::<D>(
            "INSERT INTO provider_reliability (provider, success, total, window_start) \
             VALUES (?, ?, 1, ?) \
             ON CONFLICT (provider) DO UPDATE SET \
             success = CASE WHEN excluded.window_start - provider_reliability.window_start >= ? \
                 THEN provider_reliability.success / 2 + excluded.success \
                 ELSE provider_reliability.success + excluded.success END, \
             total = CASE WHEN excluded.window_start - provider_reliability.window_start >= ? \
                 THEN provider_reliability.total / 2 + 1 \
                 ELSE provider_reliability.total + 1 END, \
             window_start = CASE WHEN excluded.window_start - provider_reliability.window_start >= ? \
                 THEN excluded.window_start \
                 ELSE provider_reliability.window_start END",
        )
        .bind(&provider.0)
        .bind(i64::from(success))
        .bind(at.as_second())
        .bind(window)
        .bind(window)
        .bind(window)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn provider_reliability_load_all(&self) -> Result<Vec<ReliabilityRow>> {
        let rows = query::<D>(
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
            query::<D>(
                "INSERT INTO score_history \
                 (slot_id, provider, channel_id, scored_at, score, breakdown_json) \
                 VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(row.slot.0)
            .bind(&row.provider.0)
            .bind(&row.channel.0)
            .bind(row.scored_at.as_second())
            .bind(row.score)
            .bind(serde_json::to_string(&row.breakdown)?)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn score_history_delete_older_than(&self, cutoff: Timestamp, limit: u32) -> Result<u64> {
        // "DELETE ... WHERE id IN (SELECT ... LIMIT n)" is the portable way to
        // delete at most n rows; ORDER BY makes it the OLDEST n. The index on
        // scored_at serves both the filter and the order.
        let done = query::<D>(
            "DELETE FROM score_history WHERE id IN \
             (SELECT id FROM score_history WHERE scored_at < ? ORDER BY scored_at, id LIMIT ?)",
        )
        .bind(cutoff.as_second())
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await?;
        Ok(D::rows_affected(&done))
    }

    // ---- Janitor (see src/janitor.rs). ----

    async fn delete_raw_older_than(&self, cutoff: Timestamp, limit: u32) -> Result<u64> {
        // Index raw_responses_fetched_at serves the filter and the order.
        let done = query::<D>(
            "DELETE FROM raw_responses WHERE id IN \
             (SELECT id FROM raw_responses WHERE fetched_at < ? ORDER BY fetched_at, id LIMIT ?)",
        )
        .bind(cutoff.as_second())
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await?;
        Ok(D::rows_affected(&done))
    }

    async fn delete_aired_slots_older_than(&self, cutoff: Timestamp, limit: u32) -> Result<u64> {
        // Only the slot rows are named here. Their observations disappear by
        // themselves: observations.slot_id is ON DELETE CASCADE and this
        // database enforces foreign keys on every connection (SQLite: set in sqlite.rs). Index
        // slots_end_idx (migration 0007) serves the filter and the order.
        let done = query::<D>(
            "DELETE FROM slots WHERE id IN \
             (SELECT id FROM slots WHERE end_utc < ? ORDER BY end_utc, id LIMIT ?)",
        )
        .bind(cutoff.as_second())
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await?;
        Ok(D::rows_affected(&done))
    }

    async fn images_clear_for_slots_older_than(
        &self,
        cutoff: Timestamp,
        limit: u32,
    ) -> Result<u64> {
        // Rows that were already cleared no longer match "image_id IS NOT
        // NULL", so repeating the call makes progress and ends.
        let done = query::<D>(
            "UPDATE slots SET image_id = NULL WHERE id IN \
             (SELECT id FROM slots WHERE image_id IS NOT NULL AND end_utc < ? \
              ORDER BY end_utc, id LIMIT ?)",
        )
        .bind(cutoff.as_second())
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await?;
        Ok(D::rows_affected(&done))
    }

    async fn images_delete_unreferenced(&self, limit: u32) -> Result<DeletedImages> {
        // slots.image_id is a foreign key to images.id, so the database itself
        // refuses to delete an image a slot still points at: even if a merge
        // links a slot to this image right now, the worst outcome is an error
        // in this statement, never a slot with a dangling image.
        // Index slots_image_id_idx (migration 0008) makes NOT EXISTS cheap.
        let paths: Vec<Option<String>> = query_scalar::<D, _>(
            "DELETE FROM images WHERE id IN \
             (SELECT i.id FROM images i \
              WHERE NOT EXISTS (SELECT 1 FROM slots s WHERE s.image_id = i.id) \
              ORDER BY i.id LIMIT ?) \
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
        cutoff: Timestamp,
        limit: u32,
    ) -> Result<u64> {
        // These tables have composite primary keys, so "the rows to delete"
        // is chosen with a row-value IN (a, b, c) IN (SELECT a, b, c ...).
        let (sql, bind_text): (&str, Option<String>) = match table {
            StateTable::WindowState => (
                "DELETE FROM window_state WHERE (provider, lineup, window_start) IN \
                 (SELECT provider, lineup, window_start FROM window_state \
                  WHERE window_start < ? ORDER BY window_start LIMIT ?)",
                None,
            ),
            StateTable::ProviderChannels => (
                "DELETE FROM provider_channels WHERE (provider, lineup, provider_channel_id) IN \
                 (SELECT provider, lineup, provider_channel_id FROM provider_channels \
                  WHERE last_seen < ? ORDER BY last_seen LIMIT ?)",
                None,
            ),
            StateTable::BadWindows => (
                "DELETE FROM fetch_bad_windows WHERE (provider, lineup, window_start) IN \
                 (SELECT provider, lineup, window_start FROM fetch_bad_windows \
                  WHERE bad_until < ? ORDER BY bad_until LIMIT ?)",
                None,
            ),
            // Days are text 'YYYY-MM-DD', which sorts like the dates do.
            StateTable::FetchBudget => (
                "DELETE FROM fetch_budget WHERE (provider, day) IN \
                 (SELECT provider, day FROM fetch_budget WHERE day < ? ORDER BY day LIMIT ?)",
                Some(day_key(cutoff.to_zoned_datetime(TimeZone::UTC).date())),
            ),
        };
        let q = query::<D>(sql);
        let q = match bind_text {
            Some(day) => q.bind(day),
            None => q.bind(cutoff.as_second()),
        };
        let done = q.bind(i64::from(limit)).execute(&self.pool).await?;
        Ok(D::rows_affected(&done))
    }

    async fn approx_size_bytes(&self) -> Result<u64> {
        D::size_bytes(&self.pool).await
    }

    // ---- Scheduler state (see src/scheduler.rs). ----

    async fn provider_channels_upsert(
        &self,
        provider: &ProviderId,
        lineup: &str,
        channels: &[ProviderChannel],
        now: Timestamp,
    ) -> Result<()> {
        // One transaction: a response's channel list is stored whole or not
        // at all. Channels missing from this list are left alone.
        let mut tx = self.pool.begin().await?;
        for c in channels {
            query::<D>(
                "INSERT INTO provider_channels \
                 (provider, lineup, provider_channel_id, number, call_sign, canonical_id, last_seen) \
                 VALUES (?, ?, ?, ?, ?, ?, ?) \
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
            .bind(now.as_second())
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
        let rows = query::<D>(
            "SELECT provider_channel_id, number, call_sign FROM provider_channels \
             WHERE provider = ? AND lineup = ? ORDER BY provider_channel_id",
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
        window_start: Timestamp,
    ) -> Result<Option<WindowState>> {
        let row = query::<D>(
            "SELECT window_start, last_success, last_attempt, last_error FROM window_state \
             WHERE provider = ? AND lineup = ? AND window_start = ?",
        )
        .bind(&provider.0)
        .bind(lineup)
        .bind(window_start.as_second())
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
        query::<D>(
            "INSERT INTO window_state \
             (provider, lineup, window_start, last_success, last_attempt, last_error) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT (provider, lineup, window_start) DO UPDATE SET \
             last_success = excluded.last_success, last_attempt = excluded.last_attempt, \
             last_error = excluded.last_error",
        )
        .bind(&provider.0)
        .bind(lineup)
        .bind(state.window_start.as_second())
        .bind(state.last_success.map(|t| t.as_second()))
        .bind(state.last_attempt.as_second())
        .bind(state.last_failure.as_ref().map(WindowFailure::to_stored))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn window_state_range(
        &self,
        provider: &ProviderId,
        lineup: &str,
        from: Timestamp,
        to: Timestamp,
    ) -> Result<Vec<WindowState>> {
        let rows = query::<D>(
            "SELECT window_start, last_success, last_attempt, last_error FROM window_state \
             WHERE provider = ? AND lineup = ? AND window_start >= ? AND window_start < ? \
             ORDER BY window_start",
        )
        .bind(&provider.0)
        .bind(lineup)
        .bind(from.as_second())
        .bind(to.as_second())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(window_state_from_row).collect()
    }
}

/// One `window_state` row (columns window_start, last_success, last_attempt,
/// last_error) as a `WindowState`.
fn window_state_from_row<D>(r: &D::Row) -> Result<WindowState>
where
    D: Database,
    for<'a> &'a str: ColumnIndex<D::Row>,
    for<'r> i64: Decode<'r, D>,
    for<'r> String: Decode<'r, D>,
    i64: Type<D>,
    String: Type<D>,
{
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
