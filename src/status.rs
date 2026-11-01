//! The data behind `GET /status` (JSON) and `GET /metrics` (Prometheus).
//!
//! [`Admin`] bundles everything the two endpoints read: the store, the fetch
//! layer, the feeds, the scheduler's status handle and the metrics. `http.rs`
//! only does routing and the bearer-token check; the numbers come from here.
//!
//! Nothing here ever includes a secret: no tokens, no database URL or
//! password. Error texts shown for the database are generic on purpose (the
//! real error is logged), because driver errors can echo connection details.

use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::clock::Clock;
use crate::feeds::{Feeds, SourceMatch};
use crate::fetch::{CircuitState, FetchLayer};
use crate::janitor::ReportSink;
use crate::metrics::Metrics;
use crate::model::{ChannelId, ProviderId};
use crate::scheduler::{SchedulerStatus, TickReport};
use crate::store::{GuideStats, Store};

/// All store work done for ONE `/status` or `/metrics` request must finish
/// within this long (one shared deadline, not per call). A database that is
/// down would otherwise make every sequential call wait out the driver's
/// connect timeout (about 30 s each) and the scraper would give up first.
/// A timeout is treated exactly like a store error.
pub const ADMIN_STORE_TIMEOUT: Duration = Duration::from_secs(5);
/// How long the guide figures (guide stats and per-source match counts) are
/// reused between requests. Measured with the injected clock.
pub const ADMIN_STATS_TTL: Duration = Duration::from_secs(15);

/// The `/status` document. Times are RFC 3339 UTC strings (`...Z`).
///
/// ```json
/// {
///   "version": "0.1.0",
///   "uptime_seconds": 3600,
///   "store": { "backend": "sqlite", "ok": true, "error": null },
///   "providers": [{
///     "name": "gracenote",
///     "circuit": "closed",                   // closed | open | half_open | unknown
///     "budget": { "used": 12, "limit": 150, "day": "2026-11-01" },
///     "last_success": "2026-11-01T10:00:00Z",
///     "last_error": null,
///     "consecutive_failures": 0,
///     "success_rate": 0.97,                  // null until a fetch was counted
///     "response_quality": { "gap_fraction": 0.0, "overlap_fraction": 0.0,
///                           "observed_minutes": 4800.0, "at": "..." },  // or null
///     "lineup_holds": [{ "lineup": "ota", "until": "2026-11-01T16:00:00Z" }],  // usually []
///     "error": null                          // set (circuit "unknown") when the state could not be read
///   }],
///   "sources": [{ "name": "hdhomerun-1", "kind": "hdhomerun",
///                 "channels_total": 107, "channels_matched": 83,
///                 "stale": false, "loaded_at": "...", "error": null }],
///   "guide": { "horizon_hours": 167.5, "newest_slot_end": "...",
///              "channels_with_data": 88, "coverage_pct": 100.0, "slots_total": 9000 },
///   "scheduler": { "last_tick_at": "...", "last_tick": { ...counts... } },
///   "janitor": null                          // null until the first janitor run, then:
///              // { "last_run": "...", "deleted": { "raw": 0, "programmes": 0, "images": 0,
///              //   "history": 0, "state": 0, "disk": 0 },
///              //   "size_before": 123456, "size_after": 120000, "errors": [] }
/// }
/// ```
///
/// `guide.*` is zero/null when the store cannot be read (`store.ok` says
/// so). `coverage_pct` is the share of matched channels (all sources,
/// de-duplicated by canonical id) that have a slot which has not ended.
#[derive(Debug, Serialize)]
pub struct Status {
    pub version: String,
    pub uptime_seconds: u64,
    pub store: StoreStatus,
    pub providers: Vec<ProviderStatus>,
    pub sources: Vec<SourceStatus>,
    pub guide: GuideStatus,
    pub scheduler: SchedulerSection,
    /// `null` until the janitor has finished its first run (see
    /// `JanitorReport::to_status`).
    pub janitor: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct StoreStatus {
    /// `"sqlite"` or `"postgres"`.
    pub backend: String,
    pub ok: bool,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct BudgetStatus {
    pub used: u32,
    pub limit: u32,
    /// The UTC day the count is for.
    pub day: String,
}

#[derive(Debug, Serialize)]
pub struct QualityStatus {
    pub gap_fraction: f64,
    pub overlap_fraction: f64,
    pub observed_minutes: f64,
    pub at: DateTime<Utc>,
}

/// A lineup left alone after repeated HTTP 400s (see
/// `ledger::BAD_WINDOW_HOLD`): none of its windows is asked for before
/// `until`. Kept in memory only, so a restart clears it.
#[derive(Debug, Serialize)]
pub struct LineupHoldStatus {
    pub lineup: String,
    pub until: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct ProviderStatus {
    pub name: String,
    pub circuit: String,
    pub budget: BudgetStatus,
    pub last_success: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub consecutive_failures: u32,
    /// Share of recent counted fetches that were good, 0..1.
    pub success_rate: Option<f64>,
    pub response_quality: Option<QualityStatus>,
    /// Active bad-window holds of this provider's lineups (empty normally).
    pub lineup_holds: Vec<LineupHoldStatus>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SourceStatus {
    pub name: String,
    pub kind: String,
    pub channels_total: usize,
    pub channels_matched: usize,
    pub stale: bool,
    pub loaded_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct GuideStatus {
    pub horizon_hours: f64,
    pub newest_slot_end: Option<DateTime<Utc>>,
    pub channels_with_data: u64,
    pub coverage_pct: f64,
    pub slots_total: u64,
}

#[derive(Debug, Serialize)]
pub struct SchedulerSection {
    pub last_tick_at: Option<DateTime<Utc>>,
    pub last_tick: Option<TickSummary>,
}

/// A `TickReport` reduced to plain numbers.
#[derive(Debug, Serialize)]
pub struct TickSummary {
    pub windows_due: usize,
    pub fetched: usize,
    pub from_cache: usize,
    pub failed: usize,
    pub parse_failed: usize,
    pub skipped_budget: usize,
    pub skipped_circuit: usize,
    pub skipped_backoff: usize,
    pub skipped_bad_window: usize,
    pub bad_window: usize,
    pub halted: usize,
    /// Windows not asked because the database failed at a fetch gate.
    pub skipped_store: usize,
    pub slots_inserted: usize,
    pub slots_updated: usize,
    pub slots_removed: usize,
    pub channels_seen: usize,
    pub merge_errors: usize,
    pub cancelled: bool,
}

impl From<&TickReport> for TickSummary {
    fn from(r: &TickReport) -> Self {
        TickSummary {
            windows_due: r.windows_due,
            fetched: r.fetched,
            from_cache: r.from_cache,
            failed: r.failed,
            parse_failed: r.parse_failed,
            skipped_budget: r.skipped.budget_exhausted,
            skipped_circuit: r.skipped.circuit_open,
            skipped_backoff: r.skipped.rate_limited,
            skipped_bad_window: r.skipped.known_bad_window,
            bad_window: r.skipped.bad_window,
            halted: r.skipped.halted,
            skipped_store: r.skipped.store_unavailable,
            slots_inserted: r.slots_inserted,
            slots_updated: r.slots_updated,
            slots_removed: r.slots_removed,
            channels_seen: r.channels_seen,
            merge_errors: r.merge_errors,
            cancelled: r.cancelled,
        }
    }
}

/// Everything the admin endpoints need, built once in `main`.
pub struct AdminDeps {
    /// `Config.admin_token`; `None` means the endpoints are open.
    pub token: Option<String>,
    pub store: Arc<dyn Store>,
    /// `"sqlite"` or `"postgres"` (see `store::backend_of`).
    pub backend: &'static str,
    pub fetch: Arc<FetchLayer>,
    pub feeds: Arc<Feeds>,
    pub scheduler: Arc<SchedulerStatus>,
    pub metrics: Arc<Metrics>,
    pub clock: Arc<dyn Clock>,
    /// The configured providers, in config order.
    pub providers: Vec<ProviderId>,
    pub version: &'static str,
    /// Deadline for the store work of one request (`ADMIN_STORE_TIMEOUT` in
    /// production; tests use a shorter one).
    pub store_timeout: Duration,
}

/// When the guide figures were computed, and the result (`None` = failed).
type CachedFigures = (DateTime<Utc>, Option<Arc<GuideFigures>>);

pub struct Admin {
    deps: AdminDeps,
    /// Where the janitor publishes its latest report; `None` when no janitor
    /// is wired in (then `janitor` stays null).
    janitor: Option<ReportSink>,
    started_at: DateTime<Utc>,
    /// The last computed guide figures and when (`None` inside = the
    /// computation failed; failures are reused for the TTL too, so a dead
    /// database is not retried by every scrape). The async mutex is held
    /// while computing: concurrent requests wait and then share the result
    /// (single flight) instead of each querying the store.
    figures: Mutex<Option<CachedFigures>>,
}

/// The guide figures both endpoints need.
struct GuideFigures {
    stats: GuideStats,
    matches: Vec<SourceMatch>,
}

impl GuideFigures {
    /// (matched channels, matched channels with a slot that has not ended),
    /// counted once per canonical id across all sources.
    fn coverage_counts(&self) -> (usize, usize) {
        let with_data: HashSet<&ChannelId> = self.stats.future_channels.iter().collect();
        let matched: HashSet<&ChannelId> = self.matches.iter().flat_map(|m| &m.matched).collect();
        let covered = matched.iter().filter(|c| with_data.contains(*c)).count();
        (matched.len(), covered)
    }

    /// Covered / matched, 0..1; 0 when nothing is matched.
    fn coverage_ratio(&self) -> f64 {
        let (matched, covered) = self.coverage_counts();
        if matched == 0 {
            0.0
        } else {
            covered as f64 / matched as f64
        }
    }

    /// Seconds from `now` to the newest slot end, never negative.
    fn horizon_seconds(&self, now: DateTime<Utc>) -> f64 {
        self.stats
            .newest_slot_end
            .map_or(0.0, |end| (end - now).num_seconds().max(0) as f64)
    }
}

impl Admin {
    pub fn new(deps: AdminDeps) -> Self {
        let started_at = deps.clock.now();
        Admin {
            deps,
            janitor: None,
            started_at,
            figures: Mutex::new(None),
        }
    }

    /// Show the janitor's latest report in `/status`.
    pub fn with_janitor(mut self, sink: ReportSink) -> Self {
        self.janitor = Some(sink);
        self
    }

    /// Run a store call under the request's shared `deadline`. `None` for an
    /// error or a timeout (both logged; the log never reaches a client).
    async fn within<T>(
        &self,
        deadline: Instant,
        what: &str,
        fut: impl Future<Output = anyhow::Result<T>>,
    ) -> Option<T> {
        match tokio::time::timeout_at(deadline, fut).await {
            Ok(Ok(v)) => Some(v),
            Ok(Err(e)) => {
                tracing::warn!(what, error = %format!("{e:#}"), "store call failed");
                None
            }
            Err(_) => {
                tracing::warn!(what, "store call timed out");
                None
            }
        }
    }

    /// The configured admin token, if any.
    pub fn token(&self) -> Option<&str> {
        self.deps.token.as_deref()
    }

    /// The guide figures, from the cache when younger than
    /// [`ADMIN_STATS_TTL`], else computed under `deadline`.
    async fn guide_figures(
        &self,
        now: DateTime<Utc>,
        deadline: Instant,
    ) -> Option<Arc<GuideFigures>> {
        let mut cache = self.figures.lock().await;
        if let Some((at, figures)) = cache.as_ref()
            && now >= *at
            && (now - *at).to_std().is_ok_and(|age| age < ADMIN_STATS_TTL)
        {
            return figures.clone();
        }
        let computed = async {
            let stats = self
                .within(deadline, "guide_stats", self.deps.store.guide_stats(now))
                .await?;
            // `source_matches` reads the store too (logging its own errors);
            // it shares the deadline.
            let matches = tokio::time::timeout_at(deadline, self.deps.feeds.source_matches())
                .await
                .ok()?;
            Some(Arc::new(GuideFigures { stats, matches }))
        }
        .await;
        *cache = Some((now, computed.clone()));
        computed
    }

    /// Build the `/status` document.
    pub async fn status(&self) -> Status {
        let deadline = Instant::now() + self.deps.store_timeout;
        let now = self.deps.clock.now();
        let uptime = (now - self.started_at).num_seconds().max(0);

        let store = match self.within(deadline, "ping", self.deps.store.ping()).await {
            Some(()) => StoreStatus {
                backend: self.deps.backend.to_string(),
                ok: true,
                error: None,
            },
            None => {
                StoreStatus {
                    backend: self.deps.backend.to_string(),
                    ok: false,
                    // Generic on purpose: see the module comment.
                    error: Some("database check failed (see the logs)".to_string()),
                }
            }
        };

        // success_rate from the reliability counters; unknown on error.
        let reliability: BTreeMap<ProviderId, (u32, u32)> = self
            .within(
                deadline,
                "reliability",
                self.deps.store.provider_reliability_load_all(),
            )
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(|r| (r.provider, (r.success, r.total)))
                    .collect()
            })
            .unwrap_or_default();
        let quality = self.deps.scheduler.response_quality();
        let mut providers = Vec::new();
        for id in &self.deps.providers {
            providers.push(
                self.provider_status(id, now, deadline, &reliability, &quality)
                    .await,
            );
        }

        let figures = self.guide_figures(now, deadline).await;
        let loaded = self.deps.feeds.source_status();
        let sources = loaded
            .into_iter()
            .map(|(name, st)| {
                let matched = figures
                    .as_ref()
                    .and_then(|f| f.matches.iter().find(|m| m.name == name))
                    .map_or(0, |m| m.matched.len());
                // Names are derived as `<kind>-<n>` (see `feeds::source_names`).
                let kind = name
                    .rsplit_once('-')
                    .map_or(name.as_str(), |(k, _)| k)
                    .to_string();
                match st {
                    Some(st) => SourceStatus {
                        name,
                        kind,
                        channels_total: st.channels_total,
                        channels_matched: matched,
                        stale: st.stale,
                        loaded_at: st.loaded_at,
                        error: st.error,
                    },
                    None => SourceStatus {
                        name,
                        kind,
                        channels_total: 0,
                        channels_matched: 0,
                        stale: false,
                        loaded_at: None,
                        error: Some("channel list not loaded yet".to_string()),
                    },
                }
            })
            .collect();

        let guide = match &figures {
            Some(f) => GuideStatus {
                horizon_hours: round2(f.horizon_seconds(now) / 3600.0),
                newest_slot_end: f.stats.newest_slot_end,
                channels_with_data: f.stats.channels_with_future_slots,
                coverage_pct: round2(f.coverage_ratio() * 100.0),
                slots_total: f.stats.slots_total,
            },
            None => GuideStatus {
                horizon_hours: 0.0,
                newest_slot_end: None,
                channels_with_data: 0,
                coverage_pct: 0.0,
                slots_total: 0,
            },
        };

        let last = self.deps.scheduler.last_tick();
        Status {
            version: self.deps.version.to_string(),
            uptime_seconds: u64::try_from(uptime).unwrap_or(0),
            store,
            providers,
            sources,
            guide,
            scheduler: SchedulerSection {
                last_tick_at: last.as_ref().map(|(at, _)| *at),
                last_tick: last.as_ref().map(|(_, r)| TickSummary::from(r)),
            },
            janitor: self
                .janitor
                .as_ref()
                .and_then(|sink| sink.read().ok()?.as_ref().map(|r| r.to_status())),
        }
    }

    async fn provider_status(
        &self,
        id: &ProviderId,
        now: DateTime<Utc>,
        deadline: Instant,
        reliability: &BTreeMap<ProviderId, (u32, u32)>,
        quality: &BTreeMap<ProviderId, crate::scheduler::ResponseQuality>,
    ) -> ProviderStatus {
        let success_rate = reliability
            .get(id)
            .filter(|(_, total)| *total > 0)
            .map(|(ok, total)| round2(f64::from(*ok) / f64::from(*total)));
        let response_quality = quality.get(id).map(|q| QualityStatus {
            gap_fraction: q.integrity.gap_fraction,
            overlap_fraction: q.integrity.overlap_fraction,
            observed_minutes: q.integrity.observed_minutes,
            at: q.checked_at,
        });
        let lineup_holds: Vec<LineupHoldStatus> = self
            .deps
            .scheduler
            .lineup_holds(now)
            .remove(id)
            .unwrap_or_default()
            .into_iter()
            .map(|(lineup, until)| LineupHoldStatus { lineup, until })
            .collect();
        let day = now.date_naive().format("%Y-%m-%d").to_string();
        let state = self
            .within(
                deadline,
                "provider_state",
                self.deps.fetch.provider_state(id),
            )
            .await;
        match state {
            Some(st) => ProviderStatus {
                name: id.0.clone(),
                circuit: match st.circuit {
                    CircuitState::Closed => "closed",
                    CircuitState::Open { .. } => "open",
                    CircuitState::HalfOpen => "half_open",
                }
                .to_string(),
                budget: BudgetStatus {
                    used: st.budget_used,
                    limit: st.budget_limit,
                    day,
                },
                last_success: st.last_success,
                last_error: st.last_error,
                consecutive_failures: st.consecutive_failures,
                success_rate,
                response_quality,
                lineup_holds,
                error: None,
            },
            None => ProviderStatus {
                name: id.0.clone(),
                circuit: "unknown".to_string(),
                budget: BudgetStatus {
                    used: 0,
                    limit: 0,
                    day,
                },
                last_success: None,
                last_error: None,
                consecutive_failures: 0,
                success_rate,
                response_quality,
                lineup_holds,
                error: Some("provider state unavailable (see the logs)".to_string()),
            },
        }
    }

    /// Refresh the state-mirroring gauges from the store, then render. A
    /// store error leaves the previous gauge values in place; the counters
    /// are always there, so a scrape never fails because of the database.
    pub async fn metrics_text(&self) -> String {
        let deadline = Instant::now() + self.deps.store_timeout;
        let now = self.deps.clock.now();
        for id in &self.deps.providers {
            let state = self
                .within(
                    deadline,
                    "provider_state",
                    self.deps.fetch.provider_state(id),
                )
                .await;
            if let Some(st) = state {
                self.deps.metrics.set_provider_state(id, &st);
            }
        }
        if let Some(f) = self.guide_figures(now, deadline).await {
            self.deps.metrics.set_guide(
                f.horizon_seconds(now),
                f.coverage_ratio(),
                f.stats.slots_total,
            );
            for m in &f.matches {
                self.deps
                    .metrics
                    .set_source_channels(&m.name, m.matched.len(), m.unmatched);
            }
        }
        self.deps.metrics.render()
    }
}

/// Two decimals, for readable JSON.
fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}
