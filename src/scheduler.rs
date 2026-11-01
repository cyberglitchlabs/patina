//! The tiered refresh scheduler: decides which provider windows are due,
//! fetches them through the ONE shared fetch layer, and merges every clean
//! response into the master guide.
//!
//! Its two jobs, in order of importance:
//!
//! 1. **Be a good neighbor.** Never cause a request that is not needed:
//!    a window is only asked for when its data is old enough (tiering,
//!    below), the "last success" of every window is stored so a restart does
//!    not re-fetch everything, and all the hard limits (daily budget,
//!    backoff, circuit breaker, bad-window memory, raw cache) live in the
//!    fetch layer, which the scheduler simply asks. The scheduler never
//!    builds its own HTTP client.
//! 2. **Never make the served guide worse.** Only a successful, cleanly
//!    parsed response reaches the master guide (the `ingest` module, which
//!    plans and stores the merge). A failed fetch, a skipped fetch or a
//!    response that could not be parsed changes no slot at all.
//!
//! **Windows.** Guide data is fetched in 6-hour windows aligned to the UTC
//! 6-hour grid (00:00, 06:00, 12:00, 18:00 UTC), from the window containing
//! "now" up to `now + horizon_days`.
//!
//! **Tiering (spec 4.13).** A window starting within the next 24 hours is
//! due when its last success is 3 hours old (or it never succeeded); a later
//! window when it is 24 hours old (`ledger::is_due` has the full rule).
//! Nearer windows go first, so a short budget is spent on the data that
//! matters most. A window that failed stays due and is asked for again at
//! most once per tick; whether a request is actually sent is up to the
//! fetch layer (backoff, circuit, budget).
//!
//! **One window, step by step.** For every due window, nearest first:
//! skip it if its lineup is in a bad-window hold; ask the fetch layer;
//! sort the outcome (`ledger::classify`); hand a usable answer to
//! `Scheduler::ingest` (channels, quality check, then `ingest::Ingest` per
//! listed channel); ask the window ledger what it all means
//! (`ledger::decide`); then apply that decision: provider reliability,
//! window state, tick counters, the lineup's streak and hold, and logs. All
//! the RULES live in `ledger` (pure, table-tested); this module only does
//! the I/O.
//!
//! **One task.** The scheduler runs as a single task and handles windows and
//! channels one after the other. That matters: `Store::apply_merge` does not
//! detect that the data changed since a plan was computed, so the steps
//! load -> plan -> apply for a channel (all inside `Ingest::channel`) must
//! never interleave with another plan for the same channel. Doing everything
//! sequentially guarantees it.
//!
//! **Hindsight samples** come back from `Ingest::channel` only after the
//! channel's merge was stored. They are folded into the in-memory averages
//! at once and written in one batch at the end of the tick.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use tokio_util::sync::CancellationToken;

use crate::clock::Clock;
use crate::config::{LineupCfg, MAX_HORIZON_DAYS, Weights};
use crate::fetch::FetchLayer;
use crate::ingest::{ChannelResponse, Ingest, store_channels};
use crate::ledger::{
    self, Attempt, Classified, Decision, LineupRun, Reliability, SavedWindow, Tally, WindowWrite,
};
use crate::metrics::Metrics;
use crate::model::{ChannelId, Observation, ProviderId};
use crate::provider::ParsedWindow;
use crate::score::{
    AccuracySnapshot, TimelineReport, WeightedScorer, next_accuracy_row, timeline_integrity,
};
use crate::store::{AccuracyRow, Store, WindowState};

/// Length of one fetch window (the only `timespan` proven to work).
pub const WINDOW: TimeDelta = TimeDelta::hours(6);

/// A provider the scheduler drives, and the lineups configured for it.
#[derive(Debug, Clone)]
pub struct ProviderRef {
    pub id: ProviderId,
    pub lineups: Vec<LineupCfg>,
}

/// Everything the scheduler needs. The fetch layer is shared (one per
/// process, built in `main`).
pub struct SchedulerDeps {
    pub store: Arc<dyn Store>,
    pub fetch: Arc<FetchLayer>,
    pub providers: Vec<ProviderRef>,
    pub horizon_days: u32,
    pub weights: Weights,
    /// Pause between two ticks in `run` (60 s in production).
    pub tick_interval: Duration,
    /// Shared with the XMLTV feed cache: bumped after every successful
    /// `apply_merge`, so a fresh merge is visible in the feeds at once
    /// instead of after the cache's TTL. `None` when no feeds are served.
    pub feed_generation: Option<Arc<AtomicU64>>,
}

/// Why windows were not fetched in a tick, by reason.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SkipCounts {
    /// Today's request budget is used up (no request sent).
    pub budget_exhausted: usize,
    /// The circuit breaker is open (no request sent).
    pub circuit_open: usize,
    /// Backing off (no request sent), or the provider answered 429 just
    /// now (a request was sent; counted once as a reliability failure).
    pub rate_limited: usize,
    /// The window answered HTTP 400 recently (no request sent).
    pub known_bad_window: usize,
    /// The window answered HTTP 400 just now.
    pub bad_window: usize,
    /// Not asked: its lineup is in a bad-window hold after repeated 400s
    /// (`ledger::BAD_WINDOW_HOLD`).
    pub halted: usize,
    /// The store failed at one of the fetch layer's gates, so no request
    /// was sent (fail closed). Not held against the provider.
    pub store_unavailable: usize,
}

/// What one tick did.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TickReport {
    /// Windows that were due (and so asked for, unless halted or cancelled).
    pub windows_due: usize,
    /// Fresh responses from the provider.
    pub fetched: usize,
    /// Answers served from the raw cache (no request sent).
    pub from_cache: usize,
    pub skipped: SkipCounts,
    /// Requests that failed (5xx, 403, timeout, redirect, ...) or whose
    /// body could not be parsed.
    pub failed: usize,
    /// The part of `failed` that were parse failures.
    pub parse_failed: usize,
    pub slots_inserted: usize,
    pub slots_updated: usize,
    pub slots_removed: usize,
    /// Distinct channels listed by this tick's successful responses.
    pub channels_seen: usize,
    /// Channels whose merge failed in the store (logged; retried later).
    pub merge_errors: usize,
    /// Lineups whose sweep stopped after repeated HTTP 400s (a bad-window
    /// hold started in this tick).
    pub lineup_halted_bad_windows: Vec<(ProviderId, String)>,
    /// Gap/overlap check of the last successful response.
    pub last_integrity: Option<TimelineReport>,
    /// The tick stopped early because shutdown was requested.
    pub cancelled: bool,
}

impl TickReport {
    /// Add one window to the counter the ledger named.
    fn count(&mut self, tally: Tally) {
        match tally {
            Tally::Fetched => self.fetched += 1,
            Tally::FromCache => self.from_cache += 1,
            Tally::BudgetExhausted => self.skipped.budget_exhausted += 1,
            Tally::CircuitOpen => self.skipped.circuit_open += 1,
            Tally::RateLimited => self.skipped.rate_limited += 1,
            Tally::KnownBadWindow => self.skipped.known_bad_window += 1,
            Tally::BadWindow => self.skipped.bad_window += 1,
            Tally::StoreUnavailable => self.skipped.store_unavailable += 1,
            Tally::Halted => self.skipped.halted += 1,
            Tally::Failed => self.failed += 1,
            Tally::ParseFailed => {
                self.failed += 1;
                self.parse_failed += 1;
            }
        }
    }
}

/// The latest response-quality check per provider, for `/status`.
#[derive(Debug, Clone, PartialEq)]
pub struct ResponseQuality {
    pub lineup: String,
    pub window_start: DateTime<Utc>,
    pub checked_at: DateTime<Utc>,
    pub integrity: TimelineReport,
    /// False when the gaps or overlaps exceeded the limits (then it was
    /// counted as a reliability failure).
    pub acceptable: bool,
}

pub struct Scheduler {
    store: Arc<dyn Store>,
    fetch: Arc<FetchLayer>,
    providers: Vec<ProviderRef>,
    horizon_days: u32,
    weights: Weights,
    tick_interval: Duration,
    feed_generation: Option<Arc<AtomicU64>>,
    clock: Arc<dyn Clock>,
    /// Shared with the HTTP status handler (see [`SchedulerStatus`]).
    status: Arc<SchedulerStatus>,
    /// Counters for `/metrics`; `None` in tests that do not need them.
    metrics: Option<Arc<Metrics>>,
}

/// What the scheduler has observed, shared (behind an `Arc`) with the HTTP
/// status handler, because `Scheduler::run` consumes the scheduler itself.
/// Plain mutexes: held only briefly, never across `.await`.
#[derive(Default)]
pub struct SchedulerStatus {
    quality: Mutex<BTreeMap<ProviderId, ResponseQuality>>,
    last_tick: Mutex<Option<(DateTime<Utc>, TickReport)>>,
    /// Each lineup's 400-streak and bad-window hold, keyed by (provider,
    /// lineup name). Kept in memory only, on purpose: after a restart the
    /// hold is gone, which costs at most the 3 requests that set it again
    /// (`ledger::BAD_WINDOW_STREAK_LIMIT`). Here (not on `Scheduler`) so
    /// `/status` can show the active holds.
    lineups: Mutex<BTreeMap<(ProviderId, String), LineupRun>>,
}

impl SchedulerStatus {
    /// The latest response-quality check for each provider.
    pub fn response_quality(&self) -> BTreeMap<ProviderId, ResponseQuality> {
        self.quality.lock().unwrap().clone()
    }

    /// When the latest finished tick ended and what it did; `None` before
    /// the first tick finished.
    pub fn last_tick(&self) -> Option<(DateTime<Utc>, TickReport)> {
        self.last_tick.lock().unwrap().clone()
    }

    /// The bad-window holds active at `now`: per provider, (lineup name,
    /// hold end), in lineup-name order. Ended holds are left out even before
    /// the next tick forgets them.
    pub fn lineup_holds(
        &self,
        now: DateTime<Utc>,
    ) -> BTreeMap<ProviderId, Vec<(String, DateTime<Utc>)>> {
        let mut out: BTreeMap<ProviderId, Vec<(String, DateTime<Utc>)>> = BTreeMap::new();
        for ((provider, lineup), run) in self.lineups.lock().unwrap().iter() {
            if let Some(until) = run.held_until(now) {
                out.entry(provider.clone())
                    .or_default()
                    .push((lineup.clone(), until));
            }
        }
        out
    }

    /// One lineup's streak and hold (the default when it has none).
    fn lineup(&self, key: &(ProviderId, String)) -> LineupRun {
        self.lineups
            .lock()
            .unwrap()
            .get(key)
            .copied()
            .unwrap_or_default()
    }

    fn set_lineup(&self, key: (ProviderId, String), run: LineupRun) {
        let mut map = self.lineups.lock().unwrap();
        if run == LineupRun::default() {
            map.remove(&key); // nothing to remember
        } else {
            map.insert(key, run);
        }
    }

    /// Begin a tick: streaks restart at 0, ended holds are forgotten (see
    /// `LineupRun::start_tick`). Holds that are still on stay, also when
    /// the tick is cancelled.
    fn start_tick(&self, now: DateTime<Utc>) {
        let mut map = self.lineups.lock().unwrap();
        for run in map.values_mut() {
            *run = run.start_tick(now);
        }
        map.retain(|_, run| *run != LineupRun::default());
    }
}

/// Start of the 6-hour grid window containing `t`.
pub fn window_floor(t: DateTime<Utc>) -> DateTime<Utc> {
    let len = WINDOW.num_seconds();
    // div_euclid rounds towards minus infinity, also for times before 1970.
    let start = t.timestamp().div_euclid(len) * len;
    DateTime::from_timestamp(start, 0).expect("grid start is in range")
}

/// `now + horizon_days`, without any chance of a panic: the config already
/// limits the value to `MAX_HORIZON_DAYS`, but this clamps it again and
/// saturates at the latest representable time, so no value can crash a
/// tick or a feed request.
pub fn horizon_end(now: DateTime<Utc>, horizon_days: u32) -> DateTime<Utc> {
    let days = horizon_days.min(MAX_HORIZON_DAYS);
    now.checked_add_signed(TimeDelta::days(i64::from(days)))
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}

/// Every window of the horizon: from the one containing `now`, while the
/// window starts before `now + horizon_days`, in time order.
pub fn horizon_windows(now: DateTime<Utc>, horizon_days: u32) -> Vec<DateTime<Utc>> {
    let end = horizon_end(now, horizon_days);
    let mut out = Vec::new();
    let mut w = window_floor(now);
    while w < end {
        out.push(w);
        // Stop at the end of representable time instead of panicking.
        match w.checked_add_signed(WINDOW) {
            Some(next) => w = next,
            None => break,
        }
    }
    out
}

/// Per-tick scratch space shared by all windows of the tick.
struct TickCtx {
    /// Built once per tick over the accuracy/reliability snapshot loaded at
    /// the start of the tick (not per slot or per channel).
    scorer: WeightedScorer,
    /// Hindsight averages, updated in memory during the tick and written in
    /// one batch at the end (`dirty` says which).
    accuracy: BTreeMap<(ProviderId, ChannelId), AccuracyRow>,
    dirty: BTreeSet<(ProviderId, ChannelId)>,
    channels_seen: BTreeSet<ChannelId>,
}

impl Scheduler {
    pub fn new(deps: SchedulerDeps, clock: Arc<dyn Clock>) -> Self {
        Scheduler {
            store: deps.store,
            fetch: deps.fetch,
            providers: deps.providers,
            horizon_days: deps.horizon_days,
            weights: deps.weights,
            tick_interval: deps.tick_interval,
            feed_generation: deps.feed_generation,
            clock,
            status: Arc::new(SchedulerStatus::default()),
            metrics: None,
        }
    }

    /// Count dropped events in `metrics` from now on.
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// A handle for `/status` that stays valid after `run` consumes `self`.
    pub fn status_handle(&self) -> Arc<SchedulerStatus> {
        self.status.clone()
    }

    /// The latest response-quality check for each provider.
    pub fn response_quality(&self) -> BTreeMap<ProviderId, ResponseQuality> {
        self.status.response_quality()
    }

    /// One full pass: decide which windows are due, fetch, parse, merge.
    pub async fn tick(&self) -> TickReport {
        self.tick_until(None).await
    }

    /// Tick, then wait `tick_interval`, until `shutdown` is cancelled. A tick
    /// is never started after cancellation. A running tick checks for
    /// cancellation only between windows, so a window's merges are always
    /// finished (never cancelled half-way through applying a plan).
    pub async fn run(self, shutdown: CancellationToken) {
        let interval = TimeDelta::from_std(self.tick_interval).unwrap_or(TimeDelta::seconds(60));
        loop {
            if shutdown.is_cancelled() {
                break;
            }
            let r = self.tick_until(Some(&shutdown)).await;
            if r.fetched + r.failed > 0 {
                tracing::info!(report = ?r, "scheduler tick");
            } else {
                tracing::debug!(report = ?r, "scheduler tick");
            }
            let next = self.clock.now() + interval;
            // Abandoning the wait is safe: dropping a sleep loses nothing.
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = self.clock.sleep_until(next) => {}
            }
        }
        tracing::info!("scheduler stopped");
    }

    async fn tick_until(&self, shutdown: Option<&CancellationToken>) -> TickReport {
        let mut report = TickReport::default();
        let now = self.clock.now();

        // Scoring inputs, loaded once per tick. If they cannot be read the
        // scorer uses neutral values; merging still works.
        let acc_rows = self
            .store
            .provider_accuracy_load_all()
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = %format!("{e:#}"), "cannot load accuracy; using neutral values");
                Vec::new()
            });
        let rel_rows = self
            .store
            .provider_reliability_load_all()
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = %format!("{e:#}"), "cannot load reliability; using neutral values");
                Vec::new()
            });
        let snapshot = AccuracySnapshot::from_rows(&acc_rows, &rel_rows);
        let mut ctx = TickCtx {
            scorer: WeightedScorer::new(self.weights.clone(), Arc::new(snapshot)),
            accuracy: acc_rows
                .into_iter()
                .map(|r| ((r.provider.clone(), r.channel.clone()), r))
                .collect(),
            dirty: BTreeSet::new(),
            channels_seen: BTreeSet::new(),
        };

        // Which windows are due, for every provider lineup.
        let due = self.due_windows(now).await;
        report.windows_due = due.len();

        self.status.start_tick(now);

        for (w, pi, li) in due {
            // Only between windows: a window's merges always finish.
            if shutdown.is_some_and(|t| t.is_cancelled()) {
                report.cancelled = true;
                break;
            }
            let p = &self.providers[pi];
            let lineup = &p.lineups[li];
            let key = (p.id.clone(), lineup.name.clone());
            let run = self.status.lineup(&key);
            if run.held_until(self.clock.now()).is_some() {
                report.count(Tally::Halted);
                continue;
            }

            let outcome = self.fetch.fetch(&p.id, lineup, w).await;
            let now = self.clock.now();
            let attempt = match ledger::classify(outcome) {
                Classified::Settled(attempt) => attempt,
                Classified::Usable { parsed, fresh } => {
                    if fresh && let Some(m) = &self.metrics {
                        // Only fresh responses: a cached answer was counted
                        // when it was fetched, and would be counted again.
                        m.dropped(&parsed.dropped);
                    }
                    let (integrity, stored) = self
                        .ingest(p, lineup, w, &parsed, now, &mut ctx, &mut report)
                        .await;
                    Attempt::Answered {
                        fresh,
                        integrity,
                        stored,
                    }
                }
            };
            let decision = ledger::decide(run, &attempt, now);
            self.apply(p, lineup, w, now, decision, &mut report).await;
        }

        // Batched hindsight-accuracy writes (one per provider and channel).
        for key in &ctx.dirty {
            if let Some(row) = ctx.accuracy.get(key)
                && let Err(e) = self.store.provider_accuracy_upsert(row).await
            {
                tracing::warn!(provider = %key.0.0, channel = %key.1.0, error = %format!("{e:#}"), "cannot store hindsight accuracy");
            }
        }
        report.channels_seen = ctx.channels_seen.len();
        *self.status.last_tick.lock().unwrap() = Some((self.clock.now(), report.clone()));
        report
    }

    /// Carry out what the ledger decided for one window: the lineup's
    /// streak and hold, the tick counters, the provider's reliability and
    /// the window's stored state. Store errors are logged, never fatal.
    async fn apply(
        &self,
        p: &ProviderRef,
        lineup: &LineupCfg,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
        d: Decision,
        report: &mut TickReport,
    ) {
        self.status
            .set_lineup((p.id.clone(), lineup.name.clone()), d.lineup);
        report.count(d.tally);
        if let Some(until) = d.hold_started {
            // Logged once per hold: while it lasts, this lineup's windows
            // are skipped before they reach the fetch layer.
            tracing::warn!(
                provider = %p.id.0,
                lineup = %lineup.name,
                bad_windows = d.lineup.streak,
                until = %until,
                "provider rejected several windows in a row; leaving this lineup alone until the hold ends"
            );
            report
                .lineup_halted_bad_windows
                .push((p.id.clone(), lineup.name.clone()));
        }
        match d.reliability {
            Reliability::Nothing => {}
            Reliability::Success => self.record_reliability(&p.id, true, now).await,
            Reliability::Failure => self.record_reliability(&p.id, false, now).await,
        }
        self.write_window(&p.id, lineup, window_start, &d.window, now)
            .await;
    }

    /// Due windows of every provider lineup, nearest first, as
    /// (window start, provider index, lineup index).
    async fn due_windows(&self, now: DateTime<Utc>) -> Vec<(DateTime<Utc>, usize, usize)> {
        let windows = horizon_windows(now, self.horizon_days);
        let (Some(&first), Some(&last)) = (windows.first(), windows.last()) else {
            return Vec::new();
        };
        let mut due = Vec::new();
        for (pi, p) in self.providers.iter().enumerate() {
            for (li, lineup) in p.lineups.iter().enumerate() {
                let states = match self
                    .store
                    .window_state_range(&p.id, &lineup.name, first, last + WINDOW)
                    .await
                {
                    Ok(s) => s,
                    Err(e) => {
                        // Without the saved state fresh windows cannot be told
                        // from stale ones; fetching all of them could become
                        // a request storm, so this lineup waits a tick.
                        tracing::warn!(provider = %p.id.0, lineup = %lineup.name, error = %format!("{e:#}"), "cannot read window state; not fetching this lineup");
                        continue;
                    }
                };
                let mut by_start: HashMap<DateTime<Utc>, WindowState> = HashMap::new();
                for mut st in states {
                    // Times in the future mean the clock jumped backwards
                    // (or was wrong when they were written): store them
                    // clamped to now (see `ledger::clamp_future`).
                    if let Some(fixed) = ledger::clamp_future(&st, now) {
                        tracing::warn!(provider = %p.id.0, lineup = %lineup.name, window = %st.window_start, "window state lies in the future; treating it as now");
                        if let Err(e) = self
                            .store
                            .window_state_put(&p.id, &lineup.name, &fixed)
                            .await
                        {
                            tracing::warn!(provider = %p.id.0, error = %format!("{e:#}"), "cannot save window state");
                        }
                        st = fixed;
                    }
                    by_start.insert(st.window_start, st);
                }
                for &w in &windows {
                    if ledger::is_due(w, now, by_start.get(&w)) {
                        due.push((w, pi, li));
                    }
                }
            }
        }
        // Nearest first; the sort is stable, so provider/lineup order holds.
        due.sort_by_key(|&(w, _, _)| w);
        due
    }

    /// Store one clean response. Returns the response's gap/overlap check
    /// (`None` if storing failed before it was reached) and whether
    /// everything was stored. What either means for reliability and the
    /// window's state is the ledger's call (`ledger::decide`).
    #[allow(clippy::too_many_arguments)]
    async fn ingest(
        &self,
        p: &ProviderRef,
        lineup: &LineupCfg,
        window_start: DateTime<Utc>,
        parsed: &ParsedWindow,
        now: DateTime<Utc>,
        ctx: &mut TickCtx,
        report: &mut TickReport,
    ) -> (Option<TimelineReport>, bool) {
        let store = self.store.as_ref();

        // (1) Channels: the provider's own list (for matching and overrides)
        // and the canonical channels that slots refer to.
        let listed = match store_channels(store, &p.id, &lineup.name, &parsed.channels, now).await {
            Ok(listed) => listed,
            Err(e) => {
                tracing::warn!(provider = %p.id.0, error = %format!("{e:#}"), "cannot store channels");
                return (None, false);
            }
        };

        // (2) Response quality, once per response. Gaps or overlaps beyond
        // the limits count against the provider's reliability (the ledger
        // records that), but the data is still merged: holes are not
        // contradictions, and the merge never lets a response make the
        // guide worse.
        let integrity = timeline_integrity(&parsed.observations);
        let acceptable = ledger::acceptable(&integrity);
        tracing::debug!(provider = %p.id.0, lineup = %lineup.name, window = %window_start, ?integrity, acceptable, "response timeline check");
        self.status.quality.lock().unwrap().insert(
            p.id.clone(),
            ResponseQuality {
                lineup: lineup.name.clone(),
                window_start,
                checked_at: now,
                integrity,
                acceptable,
            },
        );
        report.last_integrity = Some(integrity);

        // (3) Merge, channel by channel, one after the other (module docs).
        // Only channels this response lists are ingested: channels seen in
        // earlier responses but absent from this one get no coverage.
        let mut by_channel: BTreeMap<ChannelId, Vec<Observation>> = BTreeMap::new();
        for o in &parsed.observations {
            by_channel
                .entry(o.channel.clone())
                .or_default()
                .push(o.clone());
        }
        let mut all_ok = true;
        for channel in listed {
            ctx.channels_seen.insert(channel.clone());
            let incoming = by_channel.remove(&channel).unwrap_or_default();
            let guide = Ingest {
                store,
                scorer: &ctx.scorer,
                feed_generation: self.feed_generation.as_deref(),
            };
            let response = ChannelResponse {
                provider: &p.id,
                channel: &channel,
                window: window_start..window_start + WINDOW,
                observations: &incoming,
            };
            match guide.channel(response, now).await {
                Ok(done) => {
                    // The merge is stored: now the hindsight samples count.
                    commit_samples(ctx, &p.id, &channel, &done.samples, now);
                    report.slots_inserted += done.stats.inserted;
                    report.slots_updated += done.stats.updated;
                    report.slots_removed += done.stats.removed;
                }
                Err(e) => {
                    tracing::warn!(provider = %p.id.0, channel = %channel.0, error = %format!("{e:#}"), "merge failed; master unchanged for this channel");
                    report.merge_errors += 1;
                    all_ok = false;
                }
            }
        }
        (Some(integrity), all_ok)
    }

    async fn record_reliability(&self, provider: &ProviderId, success: bool, at: DateTime<Utc>) {
        if let Err(e) = self
            .store
            .provider_reliability_record(provider, success, at)
            .await
        {
            tracing::warn!(provider = %provider.0, error = %format!("{e:#}"), "cannot record reliability");
        }
    }

    /// Store the window row the ledger asked for. Reads the saved state
    /// first only when the row needs it (a failure keeps the previous last
    /// success); if that read fails, nothing is written.
    async fn write_window(
        &self,
        provider: &ProviderId,
        lineup: &LineupCfg,
        window_start: DateTime<Utc>,
        write: &WindowWrite,
        now: DateTime<Utc>,
    ) {
        // Holds the row read from the store, so `saved` can borrow it.
        let read: Option<WindowState>;
        let saved = if !write.needs_saved_state() {
            SavedWindow::Absent // not looked at by `row`
        } else {
            match self
                .store
                .window_state_get(provider, &lineup.name, window_start)
                .await
            {
                Ok(row) => {
                    read = row;
                    read.as_ref()
                        .map_or(SavedWindow::Absent, SavedWindow::Present)
                }
                Err(e) => {
                    tracing::warn!(provider = %provider.0, lineup = %lineup.name, window = %window_start, error = %format!("{e:#}"), "cannot read window state; not updating it");
                    SavedWindow::Unreadable
                }
            }
        };
        let Some(row) = write.row(window_start, saved, now) else {
            return;
        };
        if let Err(e) = self
            .store
            .window_state_put(provider, &lineup.name, &row)
            .await
        {
            tracing::warn!(provider = %provider.0, error = %format!("{e:#}"), "cannot save window state");
        }
    }
}

/// Fold one channel's hindsight samples into the in-memory averages (written
/// to the store in one batch at the end of the tick). Called only with the
/// samples of a successful `Ingest::channel` (the merge was stored, or there
/// was nothing to store).
fn commit_samples(
    ctx: &mut TickCtx,
    provider: &ProviderId,
    channel: &ChannelId,
    samples: &[f64],
    now: DateTime<Utc>,
) {
    let key = (provider.clone(), channel.clone());
    for &sample in samples {
        let row = next_accuracy_row(ctx.accuracy.get(&key), provider, channel, sample, now);
        ctx.accuracy.insert(key.clone(), row);
        ctx.dirty.insert(key.clone());
    }
}
