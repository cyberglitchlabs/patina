//! The janitor: one background task that keeps the database from growing
//! forever (spec 4.15). It deletes old data per category, then, if a disk cap
//! is configured and still exceeded, evicts the oldest data that is safe to
//! lose.
//!
//! # The safety rule
//!
//! The janitor never deletes a slot whose `end` is after "now" (a programme
//! that is on air or still to come), whatever the retention settings or the
//! disk cap say. Every slot delete uses a cutoff that is clamped to "now" and
//! the store compares `end_utc < cutoff` (strictly), so a future slot cannot
//! match. Nothing a feed still needs is removed either: image links are only
//! cleared for programmes that aired days ago (feeds start 3 hours back), and
//! the fetch layer's budget of today and yesterday is never touched.
//!
//! # Rules per category (cutoff = now minus the retention; "older" is strict)
//!
//! | category     | what goes                                                    |
//! |--------------|--------------------------------------------------------------|
//! | `raw`        | raw provider responses fetched before `now - raw_days`       |
//! | `programmes` | slots that ended before `now - programmes_days` (+ their observations) |
//! | `images`     | image links of slots that ended before `now - images_days`, then every `images` row no slot uses |
//! | `history`    | `score_history` rows scored before `now - history_days`      |
//! | `state`      | `window_state` older than 1 day, `provider_channels` not seen for 30 days, expired `fetch_bad_windows`, `fetch_budget` days older than 3 |
//! | `disk`       | only when over the cap: oldest raw, then oldest history, then oldest slots that ended before `now - feeds::LOOKBACK` (3 h) |
//!
//! `provider_accuracy`, `provider_reliability`, the tuner channel lists, the
//! channel table and the fetch layer's backoff/circuit row are small and
//! valuable, so they are never pruned.
//!
//! # How it deletes
//!
//! Every delete is chunked: at most `chunk_size` rows (default 500) per
//! statement, repeated until a statement removes fewer than that. Short
//! write locks keep the scheduler's merges and the feeds responsive, and a
//! cancelled run stops between chunks, never in the middle of a statement.
//! One failing category is recorded in the report (generic text; the real
//! error goes to the log) and the run goes on with the next one.

use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Result, anyhow};
use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use serde_json::json;
use tokio_util::sync::CancellationToken;

use crate::clock::Clock;
use crate::config::Retention;
use crate::metrics::{JanitorCategory, Metrics};
use crate::store::{StateTable, Store};

/// Rows deleted per statement.
pub const DEFAULT_CHUNK_SIZE: u32 = 500;
/// First run this long after start, so it does not compete with the first
/// ingest for the database.
pub const DEFAULT_START_DELAY: Duration = Duration::from_secs(60);
/// Time between runs (each wait is jittered by +-10%).
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(24 * 3600);
/// Rest between two chunks so other writers get the database in between.
const CHUNK_PAUSE: Duration = Duration::from_millis(25);
/// Disk eviction stops after this many chunks per run (guards against loops).
const MAX_EVICT_CHUNKS: u32 = 100;
/// Normal passes stop after this many chunks of one category (a safety net
/// that a bug cannot turn into an endless loop; far above any real backlog).
const MAX_CHUNKS_PER_CATEGORY: u32 = 100_000;
/// Eviction gives up after this many chunks in a row that did not make the
/// database any smaller (PostgreSQL does not return freed space to the OS).
const EVICT_STALL_LIMIT: u32 = 5;

/// Window state older than this is never due again.
const WINDOW_STATE_KEEP: TimeDelta = TimeDelta::days(1);
/// Provider channels not listed by the provider for this long are dropped.
const PROVIDER_CHANNEL_KEEP: TimeDelta = TimeDelta::days(30);
/// Budget counters older than this are dropped. Today's and yesterday's are
/// needed (the budget is per UTC day), so this is deliberately generous.
const BUDGET_KEEP: TimeDelta = TimeDelta::days(3);

/// What one run did. Also what `/status` shows (see `to_status`).
#[derive(Debug, Clone, PartialEq)]
pub struct JanitorReport {
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub raw_deleted: u64,
    pub programmes_deleted: u64,
    /// `images` rows deleted.
    pub images_deleted: u64,
    /// Slots whose image link was cleared (the slots stay).
    pub images_unlinked: u64,
    pub history_deleted: u64,
    /// Bookkeeping rows (window state, bad windows, budget days, channels).
    pub state_deleted: u64,
    /// Rows removed by the disk cap.
    pub disk_evicted: u64,
    /// Local files a future image store should delete (`images.local_path` of
    /// the deleted rows). v1 has no files, so this is empty in practice.
    pub files_to_delete: Vec<String>,
    pub size_before: Option<u64>,
    pub size_after: Option<u64>,
    /// One generic line per failed category; details are in the log.
    pub errors: Vec<String>,
    /// The run saw the shutdown signal before it finished all its work.
    pub interrupted: bool,
}

impl JanitorReport {
    fn new(at: DateTime<Utc>) -> Self {
        JanitorReport {
            started_at: at,
            finished_at: at,
            raw_deleted: 0,
            programmes_deleted: 0,
            images_deleted: 0,
            images_unlinked: 0,
            history_deleted: 0,
            state_deleted: 0,
            disk_evicted: 0,
            files_to_delete: Vec::new(),
            size_before: None,
            size_after: None,
            errors: Vec::new(),
            interrupted: false,
        }
    }

    /// The `janitor` object of `/status`.
    pub fn to_status(&self) -> serde_json::Value {
        json!({
            "last_run": self.finished_at.to_rfc3339_opts(SecondsFormat::Secs, true),
            "deleted": {
                "raw": self.raw_deleted,
                "programmes": self.programmes_deleted,
                "images": self.images_deleted,
                "history": self.history_deleted,
                "state": self.state_deleted,
                "disk": self.disk_evicted,
            },
            "size_before": self.size_before,
            "size_after": self.size_after,
            "errors": self.errors,
        })
    }
}

/// Where the latest report is published for `/status` (`None` until the
/// first run has finished).
pub type ReportSink = Arc<RwLock<Option<JanitorReport>>>;

pub struct Janitor {
    store: Arc<dyn Store>,
    retention: Retention,
    disk_cap_bytes: Option<u64>,
    clock: Arc<dyn Clock>,
    metrics: Option<Arc<Metrics>>,
    sink: Option<ReportSink>,
    chunk_size: u32,
    start_delay: Duration,
    interval: Duration,
}

impl Janitor {
    pub fn new(
        store: Arc<dyn Store>,
        retention: Retention,
        disk_cap_bytes: Option<u64>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Janitor {
            store,
            retention,
            disk_cap_bytes,
            clock,
            metrics: None,
            sink: None,
            chunk_size: DEFAULT_CHUNK_SIZE,
            start_delay: DEFAULT_START_DELAY,
            interval: DEFAULT_INTERVAL,
        }
    }

    /// Count deletions in `patina_janitor_deleted_total`.
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Publish every finished report here (for `/status`).
    pub fn with_report_sink(mut self, sink: ReportSink) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Rows per delete statement (at least 1). Disk eviction uses a fifth of
    /// this so it stops close to the cap.
    pub fn with_chunk_size(mut self, rows: u32) -> Self {
        self.chunk_size = rows.max(1);
        self
    }

    /// When the first run happens and how long between runs (before jitter).
    pub fn with_timing(mut self, start_delay: Duration, interval: Duration) -> Self {
        self.start_delay = start_delay;
        self.interval = interval;
        self
    }

    /// Run until `shutdown`: the first run after the start delay, then one
    /// per interval (+-10%). A run never starts after cancellation, and a run
    /// in progress stops between chunks.
    ///
    /// Waits use the real tokio timer, not the injected `Clock`: the `Clock`
    /// decides what is old (cutoffs); how long to sleep is not a data
    /// decision, and a fake clock that jumps forward would make this loop spin.
    pub async fn run(self, shutdown: CancellationToken) {
        let mut wait = self.start_delay;
        loop {
            tokio::select! {
                // `biased` checks the shutdown branch first, so a token that
                // is already cancelled wins even against a zero-length wait.
                biased;
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(wait) => {}
            }
            self.run_once_until(&shutdown).await;
            wait = jittered(self.interval, fastrand::f64());
        }
    }

    /// One complete pass, not cancellable.
    pub async fn run_once(&self) -> JanitorReport {
        self.run_once_until(&CancellationToken::new()).await
    }

    /// One pass that stops between chunks once `cancel` is cancelled.
    /// Logs one line, bumps the metrics and publishes the report.
    pub async fn run_once_until(&self, cancel: &CancellationToken) -> JanitorReport {
        let now = self.clock.now();
        let mut rep = JanitorReport::new(now);

        rep.size_before = match self.store.approx_size_bytes().await {
            Ok(s) => Some(s),
            Err(e) => {
                note_failure(&mut rep, "size", &anyhow!(e));
                None
            }
        };

        self.pass_raw(cancel, now, &mut rep).await;
        self.pass_programmes(cancel, now, &mut rep).await;
        self.pass_images(cancel, now, &mut rep).await;
        self.pass_history(cancel, now, &mut rep).await;
        self.pass_state(cancel, now, &mut rep).await;
        self.pass_disk(cancel, now, &mut rep).await;

        rep.size_after = self.store.approx_size_bytes().await.ok();
        rep.finished_at = self.clock.now();
        self.publish(&rep);
        rep
    }

    // ------------------------------------------------------------ passes

    /// Raw provider responses (kept for replay and debugging).
    async fn pass_raw(
        &self,
        cancel: &CancellationToken,
        now: DateTime<Utc>,
        rep: &mut JanitorReport,
    ) {
        let cutoff = cutoff_days(now, self.retention.raw_days);
        let r = drain(
            cancel,
            self.chunk_size,
            &mut rep.interrupted,
            &mut rep.raw_deleted,
            |n| self.store.delete_raw_older_than(cutoff, n),
        )
        .await;
        if let Err(e) = r {
            note_failure(rep, "raw", &e);
        }
    }

    /// Aired slots. The cutoff can never be after `now`, so a slot that is on
    /// air (`end > now`) or still to come cannot match `end < cutoff`.
    async fn pass_programmes(
        &self,
        cancel: &CancellationToken,
        now: DateTime<Utc>,
        rep: &mut JanitorReport,
    ) {
        let cutoff = cutoff_days(now, self.retention.programmes_days);
        let r = drain(
            cancel,
            self.chunk_size,
            &mut rep.interrupted,
            &mut rep.programmes_deleted,
            |n| self.store.delete_aired_slots_older_than(cutoff, n),
        )
        .await;
        if let Err(e) = r {
            note_failure(rep, "programmes", &e);
        }
    }

    /// Images, in two steps. First cut the image link of programmes that
    /// aired long ago (no feed shows them: feeds start 3 hours back), then
    /// delete every image row nothing links to any more. Because step two
    /// removes ANY unreferenced row, images orphaned by the programmes pass
    /// or by a merge that replaced a slot's image go in the same run.
    async fn pass_images(
        &self,
        cancel: &CancellationToken,
        now: DateTime<Utc>,
        rep: &mut JanitorReport,
    ) {
        let cutoff = cutoff_days(now, self.retention.images_days);
        let mut failed = None;
        if let Err(e) = drain(
            cancel,
            self.chunk_size,
            &mut rep.interrupted,
            &mut rep.images_unlinked,
            |n| self.store.images_clear_for_slots_older_than(cutoff, n),
        )
        .await
        {
            failed = Some(e);
        }

        // Step two has its own loop because it also collects file paths.
        let mut chunks = 0;
        loop {
            if cancel.is_cancelled() {
                rep.interrupted = true;
                break;
            }
            match self.store.images_delete_unreferenced(self.chunk_size).await {
                Ok(d) => {
                    rep.images_deleted += d.rows;
                    rep.files_to_delete.extend(d.local_paths);
                    if d.rows < u64::from(self.chunk_size) {
                        break;
                    }
                }
                Err(e) => {
                    failed = Some(e);
                    break;
                }
            }
            chunks += 1;
            if chunks >= MAX_CHUNKS_PER_CATEGORY {
                failed = Some(anyhow!("image cleanup did not finish"));
                break;
            }
            tokio::time::sleep(CHUNK_PAUSE).await;
        }
        if let Some(e) = failed {
            note_failure(rep, "images", &e);
        }
    }

    /// Score history. It has no link to slots, so it is pruned by its own age.
    async fn pass_history(
        &self,
        cancel: &CancellationToken,
        now: DateTime<Utc>,
        rep: &mut JanitorReport,
    ) {
        let cutoff = cutoff_days(now, self.retention.history_days);
        let r = drain(
            cancel,
            self.chunk_size,
            &mut rep.interrupted,
            &mut rep.history_deleted,
            |n| self.store.score_history_delete_older_than(cutoff, n),
        )
        .await;
        if let Err(e) = r {
            note_failure(rep, "history", &e);
        }
    }

    /// Scheduler and fetch-layer bookkeeping that would otherwise grow for
    /// ever. Each table has its own rule (see the module docs); all four are
    /// counted as `state`. One failing table does not skip the others.
    async fn pass_state(
        &self,
        cancel: &CancellationToken,
        now: DateTime<Utc>,
        rep: &mut JanitorReport,
    ) {
        let rules = [
            // A 6-hour window that started over a day ago is not fetched again.
            (StateTable::WindowState, now - WINDOW_STATE_KEEP),
            // Channels a provider has not listed for a month.
            (StateTable::ProviderChannels, now - PROVIDER_CHANNEL_KEEP),
            // A bad-window memory whose `bad_until` has passed no longer
            // blocks anything (the lookup already ignores it); `now` itself
            // so only expired rows go.
            (StateTable::BadWindows, now),
            // Budget counters are per UTC day; today and yesterday are needed
            // to enforce the daily limit, anything older than 3 days is not.
            (StateTable::FetchBudget, now - BUDGET_KEEP),
        ];
        let mut failed = None;
        for (table, cutoff) in rules {
            if let Err(e) = drain(
                cancel,
                self.chunk_size,
                &mut rep.interrupted,
                &mut rep.state_deleted,
                |n| self.store.delete_state_older_than(table, cutoff, n),
            )
            .await
            {
                failed = Some(e);
            }
        }
        if let Some(e) = failed {
            note_failure(rep, "state", &e);
        }
    }

    /// The disk cap. Runs after the normal passes; does nothing unless a cap
    /// is set and the data still uses more than it. Evicts oldest first, in
    /// this order: raw responses, score history, aired slots. Aired slots use
    /// cutoff `now - feeds::LOOKBACK`, so a slot a feed still shows (ended
    /// less than 3 hours ago, on air, or in the future) is never evicted; if
    /// the cap still cannot be met, it is logged and left alone.
    ///
    /// Each chunk is followed by a new size measurement. Stops when under the
    /// cap, when nothing evictable is left, after `MAX_EVICT_CHUNKS`, or when
    /// several chunks in a row did not shrink the database (PostgreSQL keeps
    /// freed space, so deleting more would only lose data for nothing).
    async fn pass_disk(
        &self,
        cancel: &CancellationToken,
        now: DateTime<Utc>,
        rep: &mut JanitorReport,
    ) {
        let Some(cap) = self.disk_cap_bytes else {
            return;
        };
        let mut size = match self.store.approx_size_bytes().await {
            Ok(s) => s,
            Err(e) => {
                note_failure(rep, "disk", &e);
                return;
            }
        };
        if size <= cap {
            return;
        }
        tracing::warn!(
            size_bytes = size,
            cap_bytes = cap,
            "disk cap exceeded; evicting oldest data"
        );

        #[derive(Clone, Copy)]
        enum Kind {
            Raw,
            History,
            AiredSlots,
        }
        let chunk = (self.chunk_size / 5).max(1);
        let slot_cutoff = now - crate::feeds::LOOKBACK;
        let mut chunks = 0;
        let mut stalled = 0;
        'kinds: for kind in [Kind::Raw, Kind::History, Kind::AiredSlots] {
            loop {
                if cancel.is_cancelled() {
                    rep.interrupted = true;
                    return;
                }
                if chunks >= MAX_EVICT_CHUNKS {
                    tracing::warn!(chunks, "disk eviction stopped at the per-run chunk limit");
                    break 'kinds;
                }
                // Raw responses and history use `now` as the cutoff:
                // "everything older than now", taken oldest first and `chunk`
                // rows at a time. Slots use `now - feeds::LOOKBACK`: the feed
                // still shows programmes that ended up to 3 hours ago, and
                // eviction must never remove something a feed serves. So
                // only slots that ended before that are taken.
                let deleted = match kind {
                    Kind::Raw => self.store.delete_raw_older_than(now, chunk).await,
                    Kind::History => self.store.score_history_delete_older_than(now, chunk).await,
                    Kind::AiredSlots => {
                        self.store
                            .delete_aired_slots_older_than(slot_cutoff, chunk)
                            .await
                    }
                };
                chunks += 1;
                let deleted = match deleted {
                    Ok(n) => n,
                    Err(e) => {
                        note_failure(rep, "disk", &e);
                        break 'kinds;
                    }
                };
                rep.disk_evicted += deleted;
                if deleted == 0 {
                    break; // this kind is used up; try the next one
                }
                let new_size = match self.store.approx_size_bytes().await {
                    Ok(s) => s,
                    Err(e) => {
                        note_failure(rep, "disk", &e);
                        break 'kinds;
                    }
                };
                if new_size <= cap {
                    size = new_size;
                    break 'kinds;
                }
                if new_size >= size {
                    stalled += 1;
                    if stalled >= EVICT_STALL_LIMIT {
                        tracing::warn!(
                            size_bytes = new_size,
                            cap_bytes = cap,
                            "disk usage did not fall after eviction (the database keeps freed space); stopping"
                        );
                        size = new_size;
                        break 'kinds;
                    }
                } else {
                    stalled = 0;
                }
                size = new_size;
                tokio::time::sleep(CHUNK_PAUSE).await;
            }
        }
        if size > cap {
            tracing::warn!(
                size_bytes = size,
                cap_bytes = cap,
                "disk cap still exceeded; nothing evictable left"
            );
        }
    }

    // ------------------------------------------------------ observability

    /// One log line, the metrics and the `/status` report.
    fn publish(&self, rep: &JanitorReport) {
        tracing::info!(
            raw = rep.raw_deleted,
            programmes = rep.programmes_deleted,
            images = rep.images_deleted,
            images_unlinked = rep.images_unlinked,
            history = rep.history_deleted,
            state = rep.state_deleted,
            disk = rep.disk_evicted,
            files_to_delete = rep.files_to_delete.len(),
            size_before = ?rep.size_before,
            size_after = ?rep.size_after,
            errors = rep.errors.len(),
            interrupted = rep.interrupted,
            "janitor run finished"
        );
        if let Some(m) = &self.metrics {
            m.janitor_deleted(JanitorCategory::Raw, rep.raw_deleted);
            m.janitor_deleted(JanitorCategory::Programmes, rep.programmes_deleted);
            m.janitor_deleted(JanitorCategory::Images, rep.images_deleted);
            m.janitor_deleted(JanitorCategory::History, rep.history_deleted);
            m.janitor_deleted(JanitorCategory::State, rep.state_deleted);
            m.janitor_deleted(JanitorCategory::Disk, rep.disk_evicted);
        }
        if let Some(sink) = &self.sink
            && let Ok(mut slot) = sink.write()
        {
            *slot = Some(rep.clone());
        }
    }
}

/// Repeat `delete(chunk)` until it deletes less than `chunk` rows, adding
/// what it deleted to `total` after every chunk (so the count stays right
/// even if a later chunk fails). Checks `cancel` before every chunk, never
/// during one.
async fn drain<F, Fut>(
    cancel: &CancellationToken,
    chunk: u32,
    interrupted: &mut bool,
    total: &mut u64,
    mut delete: F,
) -> Result<()>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<u64>>,
{
    let mut chunks = 0;
    loop {
        if cancel.is_cancelled() {
            *interrupted = true;
            return Ok(());
        }
        let n = delete(chunk).await?;
        *total += n;
        if n < u64::from(chunk) {
            return Ok(());
        }
        chunks += 1;
        if chunks >= MAX_CHUNKS_PER_CATEGORY {
            return Err(anyhow!("cleanup did not finish after {chunks} chunks"));
        }
        // Let other writers (ingest, feeds) in between two chunks.
        tokio::time::sleep(CHUNK_PAUSE).await;
    }
}

/// `now - days`, never after `now`. Saturates for absurd day counts instead
/// of overflowing.
fn cutoff_days(now: DateTime<Utc>, days: u32) -> DateTime<Utc> {
    TimeDelta::try_days(i64::from(days))
        .and_then(|d| now.checked_sub_signed(d))
        .unwrap_or(DateTime::<Utc>::MIN_UTC)
        .min(now)
}

/// Record a failed category: the full error goes to the log (it may contain
/// driver text), the report only gets a generic line.
fn note_failure(rep: &mut JanitorReport, category: &str, error: &anyhow::Error) {
    tracing::error!(category, error = %format!("{error:#}"), "janitor category failed");
    rep.errors
        .push(format!("{category}: cleanup failed (see logs)"));
}

/// `base` scaled by a factor between 0.9 and 1.1; `unit` is a random number
/// in `[0, 1)`.
pub fn jittered(base: Duration, unit: f64) -> Duration {
    base.mul_f64(0.9 + 0.2 * unit.clamp(0.0, 1.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn jitter_stays_within_ten_percent() {
        let day = Duration::from_secs(86_400);
        assert_eq!(jittered(day, 0.0), day.mul_f64(0.9));
        assert_eq!(jittered(day, 0.5), day);
        assert!(jittered(day, 0.999_999) <= day.mul_f64(1.1));
        assert_eq!(
            jittered(day, 5.0),
            day.mul_f64(1.1),
            "out-of-range input is clamped"
        );
    }

    #[test]
    fn cutoff_is_never_after_now_and_saturates() {
        let now = Utc.with_ymd_and_hms(2026, 11, 1, 12, 0, 0).unwrap();
        assert_eq!(cutoff_days(now, 0), now);
        assert_eq!(cutoff_days(now, 7), now - TimeDelta::days(7));
        assert!(cutoff_days(now, u32::MAX) < now);
    }
}
