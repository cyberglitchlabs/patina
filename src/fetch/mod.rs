//! The shared fetch layer: the ONLY code that sends requests to providers.
//!
//! Its main job is being a good neighbor to endpoints we do not own (the
//! Gracenote one is unofficial). Every request passes these gates, in order:
//!
//! 1. **Raw cache.** If a clean copy of this window was fetched within
//!    `cache_max_age`, it is re-parsed and returned as `FromCache`. This is
//!    checked first on purpose: it costs no budget and sends nothing, so an
//!    exhausted budget or an open circuit must not block it.
//! 2. **Per-provider turn.** One attempt at a time per provider. The lock is
//!    held until the response is handled, so whoever is next sees the latest
//!    state (an opened circuit, a new `Retry-After`) instead of a queue of
//!    requests firing blindly. (After getting the turn the cache is checked
//!    again, in case the previous holder just fetched this same window.)
//! 3. **Daily budget** (persisted, per provider, UTC day).
//! 4. **Circuit breaker**, then **backoff / Retry-After** (persisted).
//! 5. **Known-bad window** (HTTP 400 remembered for `bad_window_memory`).
//! 6. **Rate limiter**: wait until `min_interval` (± jitter) after the
//!    previous request.
//! 7. **Charge the budget** (atomic increment in the store), then send. The
//!    charge happens *before* the request, so a crash mid-request still
//!    counts. There is no other path to `Provider::fetch_window`.
//!
//! State that must survive a restart (budget, bad windows, raw cache,
//! backoff and circuit timestamps) lives in the store. Only the rate
//! limiter's "last request" time is in memory; restarts take longer than
//! the interval anyway, and the budget bounds the total.

pub mod breaker;
pub mod budget;
pub mod limiter;
pub mod rawcache;

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

pub use crate::clock::{Clock, SystemClock};
use crate::config::LineupCfg;
use crate::metrics::{Metrics, Outcome};
use crate::model::ProviderId;
use crate::provider::{FetchError, ParsedWindow, Provider, RawResponse};
use crate::store::{ProviderFetchState, Store};
pub use breaker::CircuitState;
use breaker::{Failure, Hold, Policy};
use limiter::Limiter;
pub use limiter::{Jitter, RandomJitter, SequenceJitter};

/// Fetch layer settings. `FetchCfg::default()` gives the documented defaults.
#[derive(Clone)]
pub struct FetchCfg {
    /// Daily request budget for providers not listed in `daily_budgets`.
    pub default_daily_budget: u32,
    /// Per-provider daily budgets (from the provider config).
    pub daily_budgets: HashMap<ProviderId, u32>,
    /// Minimum time between two requests to the same provider (before jitter).
    pub min_interval: Duration,
    /// How much jitter may stretch or shrink `min_interval` (0.5 = ±50%).
    pub jitter_fraction: f64,
    /// Where jitter samples come from (random by default; fixed in tests).
    pub jitter: Arc<dyn Jitter>,
    /// First backoff after a failure; doubles with each failure in a row.
    pub backoff_base: Duration,
    /// Longest backoff.
    pub backoff_max: Duration,
    /// Failures in a row that open the circuit breaker.
    pub circuit_threshold: u32,
    /// Minimum time the circuit stays open before one probe is allowed.
    pub circuit_cooldown: Duration,
    /// How long a window that got HTTP 400 is left alone.
    pub bad_window_memory: Duration,
    /// A cached response younger than this is served instead of fetching.
    /// Kept short on purpose: it only stops *redundant* fetches (restarts,
    /// two sources sharing a lineup); the scheduler's refreshes must still
    /// reach the provider.
    pub cache_max_age: Duration,
    /// How long raw responses are kept (used by the janitor, not for hits).
    pub raw_retention: Duration,
    /// Give up on a request that takes longer than this.
    pub request_timeout: Duration,
}

impl Default for FetchCfg {
    fn default() -> Self {
        FetchCfg {
            default_daily_budget: 150,
            daily_budgets: HashMap::new(),
            min_interval: Duration::from_secs(2),
            jitter_fraction: 0.5,
            jitter: Arc::new(RandomJitter),
            backoff_base: Duration::from_secs(30),
            backoff_max: Duration::from_secs(3600),
            circuit_threshold: 5,
            circuit_cooldown: Duration::from_secs(15 * 60),
            bad_window_memory: Duration::from_secs(6 * 3600),
            cache_max_age: Duration::from_secs(3600),
            raw_retention: Duration::from_secs(7 * 86400),
            request_timeout: Duration::from_secs(60),
        }
    }
}

// Written by hand because `jitter` (a trait object) cannot be printed.
impl fmt::Debug for FetchCfg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FetchCfg")
            .field("default_daily_budget", &self.default_daily_budget)
            .field("daily_budgets", &self.daily_budgets)
            .field("min_interval", &self.min_interval)
            .field("jitter_fraction", &self.jitter_fraction)
            .field("backoff_base", &self.backoff_base)
            .field("backoff_max", &self.backoff_max)
            .field("circuit_threshold", &self.circuit_threshold)
            .field("circuit_cooldown", &self.circuit_cooldown)
            .field("bad_window_memory", &self.bad_window_memory)
            .field("cache_max_age", &self.cache_max_age)
            .field("raw_retention", &self.raw_retention)
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

/// Result of one `fetch` call.
///
/// Every case a caller must treat differently is its own variant, so nobody
/// has to guess from an error text. In particular the variants say whether a
/// request really reached the provider:
///
/// | Variant | Request sent? |
/// | --- | --- |
/// | `Fresh` | yes, and the answer parsed cleanly |
/// | `FromCache` | no (raw cache hit) |
/// | `Skipped(BudgetExhausted / CircuitOpen / BackingOffUntil / KnownBadWindow / StoreUnavailable)` | no |
/// | `Skipped(RateLimited / Failed / ParseFailed)` | yes, and the answer was not usable |
#[derive(Debug, Clone)]
pub enum FetchOutcome {
    /// Fetched from the provider just now. `parse_error` is always `None`
    /// (a body that does not parse is returned as `Skipped(ParseFailed)`).
    Fresh(ParsedWindow),
    /// Served from the raw cache; no request was sent. `parse_error` is
    /// always `None` (a cached body that no longer parses is a cache miss).
    FromCache(ParsedWindow),
    /// Nothing usable; the reason says why.
    Skipped(SkipReason),
}

impl FetchOutcome {
    /// The skip reason, if this is a `Skipped` outcome.
    pub fn skip_reason(&self) -> Option<&SkipReason> {
        match self {
            FetchOutcome::Skipped(r) => Some(r),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    /// Today's request budget for this provider is used up. No request sent.
    BudgetExhausted,
    /// Too many failures; no requests until `until`. No request sent.
    CircuitOpen { until: DateTime<Utc> },
    /// The provider rejected this window recently (HTTP 400). No request sent.
    KnownBadWindow,
    /// Holding off this provider until the given time, because of our own
    /// backoff after an earlier failure (or an earlier `Retry-After`). No
    /// request sent: this says nothing new about the provider.
    BackingOffUntil(DateTime<Utc>),
    /// The store could not be read or written at one of the gates, so we
    /// cannot prove the request is allowed and do not send it ("fail
    /// closed"). No request sent: this is OUR problem, not the provider's.
    StoreUnavailable,
    /// The request was sent and the provider answered HTTP 429 ("too many
    /// requests"). Backoff and circuit already counted it as a failure; no
    /// request goes out before `not_before` (the larger of our own backoff
    /// and the server's `Retry-After`).
    RateLimited { not_before: DateTime<Utc> },
    /// The request was sent and failed (or could not be prepared).
    /// `FetchError::BadWindow` is the provider's HTTP 400 for this window;
    /// `FetchError::RateLimited` never appears here (see `RateLimited`).
    Failed(FetchError),
    /// The request was sent, HTTP 200, but the body could not be understood.
    ParseFailed(String),
}

/// Read-only view of one provider, for status and metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderState {
    pub circuit: CircuitState,
    pub budget_used: u32,
    pub budget_limit: u32,
    pub last_success: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub consecutive_failures: u32,
}

struct Entry {
    provider: Arc<dyn Provider>,
    budget_limit: u32,
    /// Held for a whole attempt: one request in flight per provider. A tokio
    /// mutex because it is held across `.await`.
    turn: tokio::sync::Mutex<Limiter>,
    /// Backoff/circuit state; `None` until first loaded from the store. A
    /// plain mutex, only ever held briefly and never across `.await`.
    state: Mutex<Option<ProviderFetchState>>,
}

pub struct FetchLayer {
    store: Arc<dyn Store>,
    clock: Arc<dyn Clock>,
    cfg: FetchCfg,
    entries: HashMap<ProviderId, Entry>,
    /// Where outcomes are counted for `/metrics`; `None` in tests that do
    /// not look at metrics.
    metrics: Option<Arc<Metrics>>,
}

impl FetchLayer {
    pub fn new(
        store: Arc<dyn Store>,
        providers: Vec<Arc<dyn Provider>>,
        cfg: FetchCfg,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let entries = providers
            .into_iter()
            .map(|p| {
                let id = p.id().clone();
                let budget_limit = cfg
                    .daily_budgets
                    .get(&id)
                    .copied()
                    .unwrap_or(cfg.default_daily_budget);
                let limiter =
                    Limiter::new(cfg.min_interval, cfg.jitter_fraction, cfg.jitter.clone());
                let entry = Entry {
                    provider: p,
                    budget_limit,
                    turn: tokio::sync::Mutex::new(limiter),
                    state: Mutex::new(None),
                };
                (id, entry)
            })
            .collect();
        FetchLayer {
            store,
            clock,
            cfg,
            entries,
            metrics: None,
        }
    }

    /// Count every fetch outcome in `metrics` from now on.
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Count one outcome for `pid` (a configured provider: callers only
    /// have `pid`s taken from `entries`).
    fn count(&self, pid: &ProviderId, outcome: Outcome) {
        if let Some(m) = &self.metrics {
            m.request(pid, outcome);
        }
    }

    /// Get one window of guide data for a lineup, if the rules allow it.
    pub async fn fetch(
        &self,
        provider: &ProviderId,
        lineup: &LineupCfg,
        window_start: DateTime<Utc>,
    ) -> FetchOutcome {
        let Some(entry) = self.entries.get(provider) else {
            tracing::warn!(provider = %provider.0, "fetch for unknown provider");
            return FetchOutcome::Skipped(SkipReason::Failed(FetchError::Fatal(format!(
                "unknown provider {}",
                provider.0
            ))));
        };

        if let Some(w) = self.cached_window(entry, lineup, window_start).await {
            self.count(provider, Outcome::Cache);
            return FetchOutcome::FromCache(w);
        }

        let mut limiter = entry.turn.lock().await;
        // While we waited for our turn, someone may have fetched this window.
        if let Some(w) = self.cached_window(entry, lineup, window_start).await {
            self.count(provider, Outcome::Cache);
            return FetchOutcome::FromCache(w);
        }
        let outcome = self
            .attempt(entry, &mut limiter, lineup, window_start)
            .await;
        if let FetchOutcome::Skipped(reason) = &outcome {
            tracing::debug!(
                provider = %provider.0,
                lineup = %lineup.name,
                window = %window_start,
                reason = ?reason,
                "fetch skipped"
            );
        }
        outcome
    }

    /// Status of one provider (budget used today, circuit, last result).
    pub async fn provider_state(&self, provider: &ProviderId) -> Result<ProviderState> {
        let entry = self
            .entries
            .get(provider)
            .with_context(|| format!("unknown provider {}", provider.0))?;
        let now = self.clock.now();
        let st = self.load_state(entry).await?;
        let used = self
            .store
            .budget_used(provider, budget::budget_day(now))
            .await?;
        Ok(ProviderState {
            circuit: breaker::circuit_state(&st, now),
            budget_used: used,
            budget_limit: entry.budget_limit,
            last_success: st.last_success,
            last_error: st.last_error,
            consecutive_failures: st.consecutive_failures,
        })
    }

    /// Gates 3 to 7 (see the module comment). Caller holds the provider turn.
    async fn attempt(
        &self,
        entry: &Entry,
        limiter: &mut Limiter,
        lineup: &LineupCfg,
        window_start: DateTime<Utc>,
    ) -> FetchOutcome {
        let pid = entry.provider.id();
        let store = self.store.as_ref();
        let now = self.clock.now();

        // A store error at any gate means we cannot prove the request is
        // allowed, so we do not send it ("fail closed").
        match budget::exhausted(store, pid, now, entry.budget_limit).await {
            Ok(true) => {
                self.count(pid, Outcome::SkippedBudget);
                return FetchOutcome::Skipped(SkipReason::BudgetExhausted);
            }
            Ok(false) => {}
            Err(e) => return store_unavailable(pid, e),
        }

        let state = match self.load_state(entry).await {
            Ok(s) => s,
            Err(e) => return store_unavailable(pid, e),
        };
        match breaker::hold(&state, now) {
            Some(Hold::CircuitOpen(until)) => {
                self.count(pid, Outcome::SkippedCircuit);
                return FetchOutcome::Skipped(SkipReason::CircuitOpen { until });
            }
            Some(Hold::NotBefore(t)) => {
                self.count(pid, Outcome::SkippedBackoff);
                return FetchOutcome::Skipped(SkipReason::BackingOffUntil(t));
            }
            None => {}
        }

        match store
            .bad_window_is_bad(pid, &lineup.name, window_start, now)
            .await
        {
            Ok(true) => {
                self.count(pid, Outcome::SkippedBadWindow);
                return FetchOutcome::Skipped(SkipReason::KnownBadWindow);
            }
            Ok(false) => {}
            Err(e) => return store_unavailable(pid, e),
        }

        if let Some(at) = limiter.next_allowed() {
            self.clock.sleep_until(at).await;
        }

        // Charge first, send second. This is the only call to fetch_window.
        let sent_at = self.clock.now();
        match budget::charge(store, pid, sent_at, entry.budget_limit).await {
            Ok(true) => {}
            Ok(false) => {
                // Lost a race for the last request of the day.
                self.count(pid, Outcome::SkippedBudget);
                return FetchOutcome::Skipped(SkipReason::BudgetExhausted);
            }
            Err(e) => return store_unavailable(pid, e),
        }
        limiter.mark_sent(sent_at);
        tracing::debug!(provider = %pid.0, lineup = %lineup.name, window = %window_start, "sending request");
        let result = match tokio::time::timeout(
            self.cfg.request_timeout,
            entry.provider.fetch_window(lineup, window_start),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => Err(FetchError::Transient(format!(
                "request timed out after {:?}",
                self.cfg.request_timeout
            ))),
        };

        let done_at = self.clock.now();
        self.handle_result(entry, lineup, window_start, done_at, result)
            .await
    }

    async fn handle_result(
        &self,
        entry: &Entry,
        lineup: &LineupCfg,
        window_start: DateTime<Utc>,
        done_at: DateTime<Utc>,
        result: Result<RawResponse, FetchError>,
    ) -> FetchOutcome {
        let pid = entry.provider.id();
        match result {
            Ok(raw) => {
                let parsed = entry.provider.parse(lineup, &raw, done_at);
                if let Some(err) = parsed.parse_error.clone() {
                    // Not cached: a bad body must never replace a good one.
                    self.count(pid, Outcome::ParseFailed);
                    self.record_failure(entry, done_at, Failure::ParseFailed, &err)
                        .await;
                    return FetchOutcome::Skipped(SkipReason::ParseFailed(err));
                }
                if raw.status == 200 {
                    let put = self
                        .store
                        .raw_put(pid, &lineup.name, window_start, done_at, &raw.body)
                        .await;
                    if let Err(e) = put {
                        tracing::warn!(provider = %pid.0, error = %format!("{e:#}"), "cannot cache raw response");
                    }
                }
                self.count(pid, Outcome::Ok);
                self.record_success(entry, done_at).await;
                tracing::info!(
                    provider = %pid.0,
                    lineup = %lineup.name,
                    window = %window_start,
                    channels = parsed.channels.len(),
                    programmes = parsed.observations.len(),
                    "fetched window"
                );
                FetchOutcome::Fresh(parsed)
            }
            Err(FetchError::BadWindow) => {
                self.count(pid, Outcome::BadWindow);
                // The provider answered, just not for this window: remember
                // the window, but it is not a provider failure. If this was
                // the half-open probe, the provider is evidently answering
                // normally, so it counts as a success for the circuit (closes
                // it); otherwise the backoff/circuit state is left alone.
                let half_open = entry.state.lock().unwrap().as_ref().is_some_and(|st| {
                    breaker::circuit_state(st, done_at) == CircuitState::HalfOpen
                });
                if half_open {
                    self.record_success(entry, done_at).await;
                }
                let until = breaker::add(done_at, self.cfg.bad_window_memory);
                let mark = self
                    .store
                    .bad_window_mark(pid, &lineup.name, window_start, until)
                    .await;
                if let Err(e) = mark {
                    tracing::warn!(provider = %pid.0, error = %format!("{e:#}"), "cannot remember bad window");
                }
                tracing::info!(provider = %pid.0, lineup = %lineup.name, window = %window_start, "provider rejected window (HTTP 400)");
                FetchOutcome::Skipped(SkipReason::Failed(FetchError::BadWindow))
            }
            Err(e) => {
                let failure = match &e {
                    FetchError::RateLimited { retry_after } => Failure::RateLimited(*retry_after),
                    FetchError::Fatal(_) => Failure::Fatal,
                    _ => Failure::Transient,
                };
                self.count(
                    pid,
                    match &e {
                        FetchError::RateLimited { .. } => Outcome::RateLimited,
                        FetchError::Fatal(_) => Outcome::Fatal,
                        _ => Outcome::Transient,
                    },
                );
                let not_before = self
                    .record_failure(entry, done_at, failure, &e.to_string())
                    .await;
                tracing::warn!(
                    provider = %pid.0,
                    lineup = %lineup.name,
                    window = %window_start,
                    error = %e,
                    retry_not_before = %not_before,
                    "fetch failed"
                );
                match e {
                    // A real 429 answer: its own variant, so callers can tell
                    // it from our backoff hold (`BackingOffUntil`).
                    FetchError::RateLimited { .. } => {
                        FetchOutcome::Skipped(SkipReason::RateLimited { not_before })
                    }
                    other => FetchOutcome::Skipped(SkipReason::Failed(other)),
                }
            }
        }
    }

    /// A clean cached copy of the window, if one is young enough.
    async fn cached_window(
        &self,
        entry: &Entry,
        lineup: &LineupCfg,
        window_start: DateTime<Utc>,
    ) -> Option<ParsedWindow> {
        let pid = entry.provider.id();
        let now = self.clock.now();
        let cached = match self
            .store
            .raw_get_latest(pid, &lineup.name, window_start, self.cfg.cache_max_age, now)
            .await
        {
            Ok(c) => c?,
            Err(e) => {
                // Treat as a miss; the normal gates still apply to the fetch.
                tracing::warn!(provider = %pid.0, error = %format!("{e:#}"), "cannot read raw cache");
                return None;
            }
        };
        let raw = RawResponse {
            status: 200,
            body: cached.body,
        };
        // Observations keep the time the data was really fetched.
        let parsed = entry.provider.parse(lineup, &raw, cached.fetched_at);
        if parsed.parse_error.is_some() {
            // Only clean bodies are cached, but a newer parser could reject
            // an old one; then fetch normally.
            return None;
        }
        tracing::info!(provider = %pid.0, lineup = %lineup.name, window = %window_start, "served window from raw cache");
        Some(parsed)
    }

    /// The provider's backoff/circuit state, loaded from the store once.
    async fn load_state(&self, entry: &Entry) -> Result<ProviderFetchState> {
        let cached = entry.state.lock().unwrap().clone();
        if let Some(st) = cached {
            return Ok(st);
        }
        let loaded = self
            .store
            .fetch_state_load(entry.provider.id())
            .await?
            .unwrap_or_default();
        // Only fill an empty slot: if something loaded and changed it in the
        // meantime, that newer value wins.
        let mut slot = entry.state.lock().unwrap();
        Ok(slot.get_or_insert(loaded).clone())
    }

    async fn record_success(&self, entry: &Entry, now: DateTime<Utc>) {
        let snapshot = {
            let mut slot = entry.state.lock().unwrap();
            let st = slot.get_or_insert_with(ProviderFetchState::default);
            if breaker::on_success(st, now) {
                tracing::warn!(provider = %entry.provider.id().0, "circuit breaker closed: probe succeeded");
            }
            st.clone()
        };
        self.save_state(entry, &snapshot).await;
    }

    /// Returns when the provider may be tried again.
    async fn record_failure(
        &self,
        entry: &Entry,
        now: DateTime<Utc>,
        failure: Failure,
        message: &str,
    ) -> DateTime<Utc> {
        let policy = Policy {
            backoff_base: self.cfg.backoff_base,
            backoff_max: self.cfg.backoff_max,
            threshold: self.cfg.circuit_threshold,
            cooldown: self.cfg.circuit_cooldown,
        };
        let pid = &entry.provider.id().0;
        let (snapshot, not_before) = {
            let mut slot = entry.state.lock().unwrap();
            let st = slot.get_or_insert_with(ProviderFetchState::default);
            let was_half_open = breaker::circuit_state(st, now) == CircuitState::HalfOpen;
            let effect = breaker::on_failure(st, now, failure, message.to_owned(), &policy);
            if let Some(until) = st.circuit_open_until {
                if effect.opened {
                    tracing::warn!(provider = %pid, failures = st.consecutive_failures, until = %until, "circuit breaker opened");
                } else if was_half_open {
                    tracing::warn!(provider = %pid, failures = st.consecutive_failures, until = %until, "circuit breaker probe failed; open again");
                }
            }
            (st.clone(), effect.not_before)
        };
        self.save_state(entry, &snapshot).await;
        not_before
    }

    async fn save_state(&self, entry: &Entry, st: &ProviderFetchState) {
        // The in-memory copy stays authoritative for this process; a failed
        // save only means a restart would forget this backoff.
        if let Err(e) = self.store.fetch_state_save(entry.provider.id(), st).await {
            tracing::warn!(provider = %entry.provider.id().0, error = %format!("{e:#}"), "cannot save fetch state");
        }
    }
}

fn store_unavailable(pid: &ProviderId, e: anyhow::Error) -> FetchOutcome {
    tracing::warn!(provider = %pid.0, error = %format!("{e:#}"), "store unavailable; not fetching");
    // Not a `Failed(..)`: no request was sent and the provider did nothing
    // wrong, so callers must not count this against it.
    FetchOutcome::Skipped(SkipReason::StoreUnavailable)
}
