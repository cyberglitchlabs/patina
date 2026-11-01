//! Storage interface. The rest of the program only talks to the `Store` trait;
//! each database (SQLite and PostgreSQL) implements it in its own file.

pub mod postgres;
pub mod sqlite;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};

use crate::channels::TunerChannel;
use crate::merge::{MergeApplied, MergePlan, SlotRef, SlotWithObservations};
use crate::model::{CanonicalChannel, ChannelId, MasterSlot, ProviderId, ScoreBreakdown, SlotId};
use crate::provider::ProviderChannel;

/// One cached raw provider response (already decompressed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawEntry {
    pub fetched_at: DateTime<Utc>,
    pub body: Bytes,
}

/// The fetch layer's per-provider backoff and circuit breaker state, stored
/// so that a restarted process keeps honoring it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProviderFetchState {
    /// Failed attempts in a row; reset by a success.
    pub consecutive_failures: u32,
    /// Do not send a request before this time (backoff or `Retry-After`).
    pub not_before: Option<DateTime<Utc>>,
    /// Set while the circuit breaker is open: no request before this time,
    /// then one probe is allowed. Cleared when a request succeeds.
    pub circuit_open_until: Option<DateTime<Utc>>,
    pub last_success: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

/// The running hindsight-accuracy average of one provider on one channel
/// (table `provider_accuracy`). See `score::ProviderChannelAccuracy`.
#[derive(Debug, Clone, PartialEq)]
pub struct AccuracyRow {
    pub provider: ProviderId,
    pub channel: ChannelId,
    /// Exponential moving average of hindsight samples, 0..1.
    pub ema: f64,
    /// How many samples went into `ema` (0 means "no data yet").
    pub samples: u32,
    pub updated_at: DateTime<Utc>,
}

/// A provider's recent fetch outcomes (table `provider_reliability`): a
/// rolling counter, see `Store::provider_reliability_record`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReliabilityRow {
    pub provider: ProviderId,
    pub success: u32,
    pub total: u32,
    /// When the current counting window began.
    pub window_start: DateTime<Utc>,
}

/// One scored observation, kept so scoring quality can be analysed later
/// (table `score_history`, pruned by the janitor).
#[derive(Debug, Clone, PartialEq)]
pub struct ScoreHistoryRow {
    pub slot: SlotId,
    pub provider: ProviderId,
    pub channel: ChannelId,
    pub scored_at: DateTime<Utc>,
    pub score: f64,
    pub breakdown: ScoreBreakdown,
}

/// The scheduler's memory of one fetch window of one lineup (table
/// `window_state`). Persisted so that a restart does not re-fetch windows
/// that are still fresh.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowState {
    /// Start of the 6-hour window (on the UTC 6-hour grid).
    pub window_start: DateTime<Utc>,
    /// When the window was last fetched and merged successfully; `None` if
    /// it never was.
    pub last_success: Option<DateTime<Utc>>,
    /// When a request for it last got an answer (success or failure).
    pub last_attempt: DateTime<Utc>,
    /// What went wrong on the last attempt; `None` if it succeeded.
    pub last_failure: Option<WindowFailure>,
}

/// What went wrong on a window's last attempt, as the scheduler needs to
/// know it. Typed, so no rule ever depends on the wording of an error
/// message (the merge-retry floor used to compare `last_error` with an
/// English sentence).
///
/// Stored in the existing `window_state.last_error` TEXT column as a stable
/// machine code, optionally followed by `": "` and a free-text detail for
/// people reading the database (see [`WindowFailure::to_stored`]). Only the
/// code is ever interpreted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowFailure {
    /// The response arrived and parsed, but storing its merge failed. The
    /// only kind with a rule of its own: the merge-retry floor.
    MergeFailed,
    /// The provider rejected this window (HTTP 400).
    BadWindow,
    /// HTTP 200, but the body could not be understood (detail: why).
    ParseFailed(String),
    /// Any other failed request: 5xx, 403, timeout, redirect, ... (detail:
    /// the error text). Also what an unknown stored value reads back as.
    Failed(String),
}

impl WindowFailure {
    // The machine codes. Never change one once released: rows written by
    // an older version must keep meaning the same thing.
    const MERGE_FAILED: &'static str = "merge_failed";
    const BAD_WINDOW: &'static str = "bad_window";
    const PARSE_FAILED: &'static str = "parse_failed";
    const FAILED: &'static str = "failed";

    /// The text stored in `window_state.last_error`: `code` or
    /// `code: detail`, for example `merge_failed` or `failed: HTTP 500`.
    pub fn to_stored(&self) -> String {
        match self {
            WindowFailure::MergeFailed => Self::MERGE_FAILED.to_string(),
            WindowFailure::BadWindow => Self::BAD_WINDOW.to_string(),
            WindowFailure::ParseFailed(d) => format!("{}: {d}", Self::PARSE_FAILED),
            WindowFailure::Failed(d) => format!("{}: {d}", Self::FAILED),
        }
    }

    /// Read back a stored value. Never fails and never panics: text that
    /// does not start with a known code (a row from before the codes, or
    /// a hand-edited one) is a generic `Failed` carrying the whole text.
    /// It then gets no special treatment, which is the safe direction:
    /// the window is simply due by the normal refresh rules.
    pub fn from_stored(text: &str) -> WindowFailure {
        // `split_once` cuts at the FIRST ": ", so a detail may itself
        // contain ": " (or even look like a code) and still round-trip.
        let (code, detail) = match text.split_once(": ") {
            Some((code, detail)) => (code, Some(detail)),
            None => (text, None),
        };
        match (code, detail) {
            (Self::MERGE_FAILED, None) => WindowFailure::MergeFailed,
            (Self::BAD_WINDOW, None) => WindowFailure::BadWindow,
            (Self::PARSE_FAILED, Some(d)) => WindowFailure::ParseFailed(d.to_string()),
            (Self::FAILED, Some(d)) => WindowFailure::Failed(d.to_string()),
            _ => WindowFailure::Failed(text.to_string()),
        }
    }
}

/// Aggregate facts about the master guide, for `/status` and `/metrics`.
/// "Future" means a slot that has not ended yet (`end > now`): it is either
/// on air right now or still to come.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GuideStats {
    /// Every stored slot.
    pub slots_total: u64,
    /// Slots with `end > now`.
    pub future_slots_total: u64,
    /// The latest slot end of any slot; `None` for an empty guide.
    pub newest_slot_end: Option<DateTime<Utc>>,
    /// How many distinct channels have at least one future slot.
    pub channels_with_future_slots: u64,
    /// The channels counted by `channels_with_future_slots`, sorted. Coverage
    /// of the channels a feed serves is computed from this list, because the
    /// guide also holds channels that no source matched.
    pub future_channels: Vec<ChannelId>,
}

/// Length of one reliability counting window. When a fetch outcome arrives
/// and the current window is at least this old, the old counts are halved
/// (so recent behaviour weighs more, but history is not thrown away at once)
/// and a new window starts.
pub const RELIABILITY_WINDOW: Duration = Duration::from_secs(7 * 24 * 3600);

#[async_trait]
pub trait Store: Send + Sync {
    /// Bring the database schema up to date. Safe to call repeatedly.
    async fn migrate(&self) -> Result<()>;

    /// Cheap liveness check of the database connection (`SELECT 1`). `Err`
    /// when the database cannot be reached.
    async fn ping(&self) -> Result<()>;

    /// Aggregate facts about the master guide as of `now` (see `GuideStats`).
    async fn guide_stats(&self, now: DateTime<Utc>) -> Result<GuideStats>;

    /// Insert channels, or update name/number/call sign of ones whose id exists.
    async fn upsert_channels(&self, channels: &[CanonicalChannel]) -> Result<()>;

    /// All channels, ordered by channel number: numerically by major part then
    /// minor part (so 4.1 comes before 11.1, and 11.1 before 11.2). A number
    /// without a minor part counts as minor 0; numbers that do not parse sort
    /// last. Ties are broken by channel id so the order is always stable.
    async fn list_channels(&self) -> Result<Vec<CanonicalChannel>>;

    /// Slots of one channel that overlap the half-open range `[from, to)`,
    /// ordered by start time. Unknown channels simply have no slots.
    async fn slots(
        &self,
        channel: ChannelId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<MasterSlot>>;

    /// Like `slots`, but each slot comes with every provider's observation of
    /// it (sorted by provider). This is what `merge::plan_merge` needs. Both
    /// reads happen in one transaction, so slots and observations match.
    async fn slots_with_observations(
        &self,
        channel: &ChannelId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<SlotWithObservations>>;

    /// Write a merge plan to the master guide in ONE transaction: either all
    /// of it is applied or (on any error) none of it. Order: removals,
    /// superseded observations, updates, inserts, observation upserts.
    /// Applying the same plan twice leaves the same data (an insert whose
    /// channel and start already exist reuses that slot).
    ///
    /// Returns the counts and the id of every inserted slot, in the order of
    /// `plan.inserts` (see `MergeApplied`). On an error nothing was written
    /// and no ids exist.
    ///
    /// Callers must serialize load -> plan -> apply per channel (one scheduler
    /// task doing them in turn). A plan is computed from a snapshot read by
    /// `slots_with_observations`; `apply_merge` does not detect that the data
    /// changed since, nor that two plans for the same channel overlap.
    async fn apply_merge(&self, plan: MergePlan) -> Result<MergeApplied>;

    // ---- Fetch layer state (see src/fetch). ----

    /// Requests already made to `provider` on UTC day `day` (0 if none).
    async fn budget_used(&self, provider: &ProviderId, day: NaiveDate) -> Result<u32>;

    /// Atomically add `n` to the day's count and return the new total. Safe
    /// under concurrency: two callers adding 1 at once always end at +2.
    async fn budget_add(&self, provider: &ProviderId, day: NaiveDate, n: u32) -> Result<u32>;

    /// Remember that the provider rejected this window, until `until`.
    async fn bad_window_mark(
        &self,
        provider: &ProviderId,
        lineup: &str,
        window_start: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<()>;

    /// Whether the window is still remembered as bad at time `now`.
    async fn bad_window_is_bad(
        &self,
        provider: &ProviderId,
        lineup: &str,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool>;

    /// Store a raw response body. The store compresses it (gzip); callers
    /// always see the original bytes.
    async fn raw_put(
        &self,
        provider: &ProviderId,
        lineup: &str,
        window_start: DateTime<Utc>,
        fetched_at: DateTime<Utc>,
        body: &[u8],
    ) -> Result<()>;

    /// The newest stored response for the window fetched no longer than
    /// `max_age` before `now`, if any.
    async fn raw_get_latest(
        &self,
        provider: &ProviderId,
        lineup: &str,
        window_start: DateTime<Utc>,
        max_age: Duration,
        now: DateTime<Utc>,
    ) -> Result<Option<RawEntry>>;

    /// Saved backoff/circuit state for a provider, if any was ever saved.
    async fn fetch_state_load(&self, provider: &ProviderId) -> Result<Option<ProviderFetchState>>;

    /// Replace the saved backoff/circuit state for a provider.
    async fn fetch_state_save(
        &self,
        provider: &ProviderId,
        state: &ProviderFetchState,
    ) -> Result<()>;

    // ---- Channel sources (see src/channels). ----

    /// Replace the whole last-known channel list of one source, atomically
    /// (one transaction: readers never see a half-written list). An empty
    /// list is valid and is remembered as empty.
    async fn save_tuner_channels(
        &self,
        source_key: &str,
        channels: &[TunerChannel],
        now: DateTime<Utc>,
    ) -> Result<()>;

    /// The last saved list (in its original order) and when it was saved;
    /// `None` if this source never saved one. An empty list is `Some`.
    async fn load_tuner_channels(
        &self,
        source_key: &str,
    ) -> Result<Option<(Vec<TunerChannel>, DateTime<Utc>)>>;

    // ---- Scoring (see src/score.rs). ----

    /// Every stored hindsight-accuracy average, ordered by provider then
    /// channel.
    async fn provider_accuracy_load_all(&self) -> Result<Vec<AccuracyRow>>;

    /// Insert or replace the average for (provider, channel).
    async fn provider_accuracy_upsert(&self, row: &AccuracyRow) -> Result<()>;

    /// Count one fetch outcome for a provider, in one atomic statement. If
    /// the provider's window began `RELIABILITY_WINDOW` or more before `at`,
    /// the old counts are halved (integer division) and a new window starts
    /// at `at` before this outcome is added.
    async fn provider_reliability_record(
        &self,
        provider: &ProviderId,
        success: bool,
        at: DateTime<Utc>,
    ) -> Result<()>;

    /// Every provider's reliability counter, ordered by provider.
    async fn provider_reliability_load_all(&self) -> Result<Vec<ReliabilityRow>>;

    /// Append scored observations to the history.
    async fn score_history_append(&self, rows: &[ScoreHistoryRow]) -> Result<()>;

    /// Delete up to `limit` history rows scored strictly before `cutoff`,
    /// oldest first; returns how many rows were deleted. The janitor calls
    /// this in a loop until it returns less than `limit` (see the "Janitor"
    /// section below for why every delete is bounded).
    async fn score_history_delete_older_than(
        &self,
        cutoff: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64>;

    // ---- Scheduler state (see src/scheduler.rs). ----

    /// Insert or update the provider's own channels for one lineup (keyed by
    /// provider channel id), stamping them `last_seen = now`. Channels not in
    /// `channels` are kept: absence from one response deletes nothing.
    async fn provider_channels_upsert(
        &self,
        provider: &ProviderId,
        lineup: &str,
        channels: &[ProviderChannel],
        now: DateTime<Utc>,
    ) -> Result<()>;

    /// Every channel ever reported for (provider, lineup), ordered by
    /// provider channel id.
    async fn provider_channels_list(
        &self,
        provider: &ProviderId,
        lineup: &str,
    ) -> Result<Vec<ProviderChannel>>;

    /// The saved state of one window, if it was ever attempted.
    async fn window_state_get(
        &self,
        provider: &ProviderId,
        lineup: &str,
        window_start: DateTime<Utc>,
    ) -> Result<Option<WindowState>>;

    /// Insert or replace (every field) the state of `state.window_start`.
    async fn window_state_put(
        &self,
        provider: &ProviderId,
        lineup: &str,
        state: &WindowState,
    ) -> Result<()>;

    /// Saved states of the windows starting in `[from, to)`, ordered by
    /// window start (one query for a whole scheduling pass).
    async fn window_state_range(
        &self,
        provider: &ProviderId,
        lineup: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<WindowState>>;

    // ---- Janitor (see src/janitor.rs). ----
    //
    // Every delete below removes AT MOST `limit` rows in one statement (one
    // short transaction), oldest first, and returns how many it removed. The
    // janitor repeats the call until it returns less than `limit`. Bounding
    // each statement keeps write locks short: on SQLite one long DELETE would
    // stall the scheduler's merges and the feeds. "Older than a cutoff" always
    // means STRICTLY older: a row exactly at the cutoff is kept.

    /// Delete raw provider responses fetched strictly before `cutoff`.
    async fn delete_raw_older_than(&self, cutoff: DateTime<Utc>, limit: u32) -> Result<u64>;

    /// Delete slots whose `end_utc` is strictly before `cutoff`, together with
    /// their observations (`ON DELETE CASCADE`). The caller must pass a cutoff
    /// that is not after "now", so only slots that have already aired can
    /// match; the janitor never passes a later one. `score_history` is not
    /// touched (it has no foreign key on purpose and is pruned by age).
    async fn delete_aired_slots_older_than(&self, cutoff: DateTime<Utc>, limit: u32)
    -> Result<u64>;

    /// Detach images from slots whose `end_utc` is strictly before `cutoff`
    /// (`image_id` becomes NULL); returns how many slots were changed. The
    /// slots themselves stay. Step one of image retention.
    async fn images_clear_for_slots_older_than(
        &self,
        cutoff: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64>;

    /// Delete `images` rows that no slot references. Step two of image
    /// retention. The result carries the `local_path` of every deleted row
    /// that had one, so a future image file store can delete the files.
    async fn images_delete_unreferenced(&self, limit: u32) -> Result<DeletedImages>;

    /// Delete rows of one of the scheduler/fetch bookkeeping tables that are
    /// older than `cutoff` (what "older" means per table: see `StateTable`).
    async fn delete_state_older_than(
        &self,
        table: StateTable,
        cutoff: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64>;

    /// Bytes of database the data actually uses, for the disk cap. SQLite:
    /// `(page_count - freelist_count) * page_size` (pages freed by a delete
    /// count as free). PostgreSQL: `pg_database_size`, which only shrinks when
    /// the server gives space back to the OS.
    async fn approx_size_bytes(&self) -> Result<u64>;
}

/// What `Store::images_delete_unreferenced` removed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeletedImages {
    /// How many `images` rows were deleted.
    pub rows: u64,
    /// `local_path` of the deleted rows that had one (v1 never sets it).
    pub local_paths: Vec<String>,
}

/// The bookkeeping tables the janitor prunes by age. Each one names the
/// column the cutoff is compared with (always strictly less than).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateTable {
    /// `window_state.window_start`: windows long past are never due again.
    WindowState,
    /// `provider_channels.last_seen`: channels the provider stopped listing.
    ProviderChannels,
    /// `fetch_bad_windows.bad_until`: memory of a rejected window that has
    /// expired (pass "now" as the cutoff).
    BadWindows,
    /// `fetch_budget.day`: the cutoff's UTC calendar day; rows of earlier
    /// days go.
    FetchBudget,
}

/// `now - max_age` as unix seconds, saturating instead of overflowing on
/// absurd ages.
pub(crate) fn cutoff_secs(now: DateTime<Utc>, max_age: Duration) -> i64 {
    now.timestamp()
        .saturating_sub(i64::try_from(max_age.as_secs()).unwrap_or(i64::MAX))
}

/// The database id an observation write refers to: an existing slot's id, or
/// the id the insert at that index of the plan got (`new_ids`).
pub(crate) fn resolve_slot(slot: SlotRef, new_ids: &[i64]) -> Result<i64> {
    match slot {
        SlotRef::Existing(id) => Ok(id.0),
        SlotRef::New(i) => match new_ids.get(i) {
            Some(id) => Ok(*id),
            None => bail!(
                "merge plan refers to insert #{i}, but has only {}",
                new_ids.len()
            ),
        },
    }
}

/// Day key stored in `fetch_budget.day`, e.g. `2026-11-01`.
pub(crate) fn day_key(day: NaiveDate) -> String {
    day.format("%Y-%m-%d").to_string()
}

/// Open the database named by `database_url`, run migrations, and return it.
/// The backend is chosen from the URL scheme.
pub async fn connect(database_url: &str) -> Result<Arc<dyn Store>> {
    let store: Arc<dyn Store> = if database_url.starts_with("sqlite:") {
        Arc::new(sqlite::SqliteStore::open(database_url).await?)
    } else if database_url.starts_with("postgres://") || database_url.starts_with("postgresql://") {
        Arc::new(postgres::PostgresStore::open(database_url).await?)
    } else {
        let scheme = database_url.split(':').next().unwrap_or("");
        bail!("unsupported database URL scheme {scheme:?} (supported: sqlite, postgres)");
    };
    store.migrate().await?;
    Ok(store)
}

/// `"sqlite"` or `"postgres"`, from the scheme of a database URL that
/// `connect` accepted.
pub fn backend_of(database_url: &str) -> &'static str {
    if database_url.starts_with("sqlite:") {
        "sqlite"
    } else {
        "postgres"
    }
}

/// Sort key for channel numbers; see `Store::list_channels`.
pub(crate) fn channel_number_key(number: &str) -> (u32, u32) {
    let mut parts = number.trim().splitn(2, '.');
    let major = parts.next().and_then(|p| p.parse::<u32>().ok());
    let minor = match parts.next() {
        Some(p) => p.parse::<u32>().ok(),
        None => Some(0),
    };
    match (major, minor) {
        (Some(a), Some(b)) => (a, b),
        _ => (u32::MAX, u32::MAX),
    }
}

#[cfg(test)]
mod tests {
    use super::channel_number_key;

    #[test]
    fn numbers_sort_numerically() {
        assert!(channel_number_key("4.1") < channel_number_key("11.1"));
        assert!(channel_number_key("11.1") < channel_number_key("11.2"));
        assert!(channel_number_key("4") < channel_number_key("4.1"));
        assert!(channel_number_key("99.1") < channel_number_key("abc"));
    }
}
