//! Prometheus metrics (`GET /metrics`).
//!
//! Library choice: the `prometheus` crate (without its protobuf feature, we
//! only need the text format). It has labelled counters and gauges and a
//! text encoder, which is all this needs.
//!
//! Two kinds of series:
//! - **Counters** (`patina_provider_requests_total`, `patina_merge_dropped_total`,
//!   `patina_janitor_deleted_total`) are incremented where the event happens
//!   (fetch layer, scheduler, janitor), through a shared `Arc<Metrics>`.
//! - **Gauges that mirror state** (budget, circuit, guide horizon, coverage,
//!   slot count, source channels) are refreshed by the `/metrics` handler on
//!   every scrape from the store, so they can never drift from the truth.
//!
//! Label hygiene: label values are only ever configured provider and source
//! names, or fixed words from the enums below; never anything derived from
//! a request. Every labelled series is created at zero in [`Metrics::new`],
//! so it exists from the first scrape (an alert on `rate(...)` needs the
//! series to be there before the first event) and the number of series is
//! fixed for the life of the process.

use prometheus::{
    Encoder, Gauge, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder,
};

use crate::fetch::{CircuitState, ProviderState};
use crate::model::ProviderId;
use crate::provider::DropCounts;

/// The value of the `Content-Type` header for the text exposition format.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// What happened to one `FetchLayer::fetch` call, the `outcome` label of
/// `patina_provider_requests_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// A request was sent and the answer was good.
    Ok,
    /// Served from the raw cache; no request sent.
    Cache,
    /// The provider answered HTTP 400 for this window.
    BadWindow,
    /// The provider answered 429 (or asked us to wait).
    RateLimited,
    /// A request failed in a way worth retrying (5xx, timeout, network).
    Transient,
    /// A request failed in a way retrying will not fix (403, ...).
    Fatal,
    /// A request got HTTP 200 but the body could not be understood.
    ParseFailed,
    /// Not sent: today's budget is used up.
    SkippedBudget,
    /// Not sent: the circuit breaker is open.
    SkippedCircuit,
    /// Not sent: the window was recently rejected with HTTP 400.
    SkippedBadWindow,
    /// Not sent: backing off after a failure or a `Retry-After`.
    SkippedBackoff,
}

impl Outcome {
    pub const ALL: [Outcome; 11] = [
        Outcome::Ok,
        Outcome::Cache,
        Outcome::BadWindow,
        Outcome::RateLimited,
        Outcome::Transient,
        Outcome::Fatal,
        Outcome::ParseFailed,
        Outcome::SkippedBudget,
        Outcome::SkippedCircuit,
        Outcome::SkippedBadWindow,
        Outcome::SkippedBackoff,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Outcome::Ok => "ok",
            Outcome::Cache => "cache",
            Outcome::BadWindow => "bad_window",
            Outcome::RateLimited => "rate_limited",
            Outcome::Transient => "transient",
            Outcome::Fatal => "fatal",
            Outcome::ParseFailed => "parse_failed",
            Outcome::SkippedBudget => "skipped_budget",
            Outcome::SkippedCircuit => "skipped_circuit",
            Outcome::SkippedBadWindow => "skipped_bad_window",
            Outcome::SkippedBackoff => "skipped_backoff",
        }
    }
}

/// What the janitor deleted, the `category` label of
/// `patina_janitor_deleted_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JanitorCategory {
    Raw,
    Programmes,
    Images,
    History,
    /// Fetch/scheduler bookkeeping rows (window state, expired bad windows,
    /// old budget days, stale provider channels).
    State,
    Disk,
}

impl JanitorCategory {
    pub const ALL: [JanitorCategory; 6] = [
        JanitorCategory::Raw,
        JanitorCategory::Programmes,
        JanitorCategory::Images,
        JanitorCategory::History,
        JanitorCategory::State,
        JanitorCategory::Disk,
    ];

    pub fn label(self) -> &'static str {
        match self {
            JanitorCategory::Raw => "raw",
            JanitorCategory::Programmes => "programmes",
            JanitorCategory::Images => "images",
            JanitorCategory::History => "history",
            JanitorCategory::State => "state",
            JanitorCategory::Disk => "disk",
        }
    }
}

/// Reasons an event was dropped by the parser (`DropCounts` fields).
const DROP_REASONS: [&str; 3] = ["invalid_time", "missing_title", "duplicate"];
/// `state` label of `patina_source_channels`.
const SOURCE_STATES: [&str; 2] = ["matched", "unmatched"];

pub struct Metrics {
    registry: Registry,
    requests: IntCounterVec,
    budget_used: IntGaugeVec,
    budget_limit: IntGaugeVec,
    circuit_open: IntGaugeVec,
    horizon_seconds: Gauge,
    coverage_ratio: Gauge,
    slots_total: IntGauge,
    janitor_deleted: IntCounterVec,
    merge_dropped: IntCounterVec,
    source_channels: IntGaugeVec,
}

impl Metrics {
    /// Register every series. `providers` and `sources` are the configured
    /// names (the only label values these series will ever have).
    pub fn new(version: &str, providers: &[ProviderId], sources: &[String]) -> Self {
        let registry = Registry::new();
        // Registration only fails for duplicate or malformed names, which
        // would be a bug in this file, so `expect` (caught by every test
        // that builds a `Metrics`).
        let counter_vec = |name: &str, help: &str, labels: &[&str]| {
            let c = IntCounterVec::new(Opts::new(name, help), labels).expect("valid metric");
            registry.register(Box::new(c.clone())).expect("register");
            c
        };
        let gauge_vec = |name: &str, help: &str, labels: &[&str]| {
            let g = IntGaugeVec::new(Opts::new(name, help), labels).expect("valid metric");
            registry.register(Box::new(g.clone())).expect("register");
            g
        };
        let requests = counter_vec(
            "patina_provider_requests_total",
            "Fetch attempts per provider by outcome (skipped_* outcomes sent no request).",
            &["provider", "outcome"],
        );
        let budget_used = gauge_vec(
            "patina_provider_budget_used",
            "Requests made to the provider today (UTC day).",
            &["provider"],
        );
        let budget_limit = gauge_vec(
            "patina_provider_budget_limit",
            "Daily request budget of the provider.",
            &["provider"],
        );
        let circuit_open = gauge_vec(
            "patina_circuit_open",
            "1 when the provider's circuit breaker is open or half-open, else 0.",
            &["provider"],
        );
        let janitor_deleted = counter_vec(
            "patina_janitor_deleted_total",
            "Rows or files deleted by the janitor, by category.",
            &["category"],
        );
        let merge_dropped = counter_vec(
            "patina_merge_dropped_total",
            "Provider events discarded by the parser, by reason.",
            &["reason"],
        );
        let source_channels = gauge_vec(
            "patina_source_channels",
            "Channels of each channel source by match state.",
            &["source", "state"],
        );
        let build_info = gauge_vec(
            "patina_build_info",
            "Always 1; the version label carries the build.",
            &["version"],
        );
        let horizon_seconds = Gauge::new(
            "patina_guide_horizon_seconds",
            "Seconds from now to the end of the latest guide slot (0 if none).",
        )
        .expect("valid metric");
        let coverage_ratio = Gauge::new(
            "patina_guide_coverage_ratio",
            "Share (0..1) of matched channels that have a slot that has not ended.",
        )
        .expect("valid metric");
        let slots_total = IntGauge::new("patina_slots_total", "Slots in the master guide.")
            .expect("valid metric");
        registry
            .register(Box::new(horizon_seconds.clone()))
            .expect("register");
        registry
            .register(Box::new(coverage_ratio.clone()))
            .expect("register");
        registry
            .register(Box::new(slots_total.clone()))
            .expect("register");

        build_info.with_label_values(&[version]).set(1);
        for p in providers {
            for o in Outcome::ALL {
                requests.with_label_values(&[&p.0, o.label()]).inc_by(0);
            }
            budget_used.with_label_values(&[&p.0]).set(0);
            budget_limit.with_label_values(&[&p.0]).set(0);
            circuit_open.with_label_values(&[&p.0]).set(0);
        }
        for c in JanitorCategory::ALL {
            janitor_deleted.with_label_values(&[c.label()]).inc_by(0);
        }
        for r in DROP_REASONS {
            merge_dropped.with_label_values(&[r]).inc_by(0);
        }
        for s in sources {
            for st in SOURCE_STATES {
                source_channels.with_label_values(&[s, st]).set(0);
            }
        }
        Metrics {
            registry,
            requests,
            budget_used,
            budget_limit,
            circuit_open,
            horizon_seconds,
            coverage_ratio,
            slots_total,
            janitor_deleted,
            merge_dropped,
            source_channels,
        }
    }

    // ------------------------------------------------------------ counters

    /// Count one fetch outcome. Only call with a configured provider.
    pub fn request(&self, provider: &ProviderId, outcome: Outcome) {
        self.requests
            .with_label_values(&[&provider.0, outcome.label()])
            .inc();
    }

    /// Count the events a fresh response's parser discarded.
    pub fn dropped(&self, d: &DropCounts) {
        let by_reason = [
            ("invalid_time", d.invalid_time),
            ("missing_title", d.missing_title),
            ("duplicate", d.duplicate),
        ];
        for (reason, n) in by_reason {
            self.merge_dropped
                .with_label_values(&[reason])
                .inc_by(u64::from(n));
        }
    }

    /// The janitor deleted `n` things of one category.
    pub fn janitor_deleted(&self, category: JanitorCategory, n: u64) {
        self.janitor_deleted
            .with_label_values(&[category.label()])
            .inc_by(n);
    }

    // -------------------------------------------------------------- gauges

    /// Mirror one provider's state (called on every scrape).
    pub fn set_provider_state(&self, provider: &ProviderId, st: &ProviderState) {
        let p = [provider.0.as_str()];
        self.budget_used
            .with_label_values(&p)
            .set(i64::from(st.budget_used));
        self.budget_limit
            .with_label_values(&p)
            .set(i64::from(st.budget_limit));
        let open = st.circuit != CircuitState::Closed;
        self.circuit_open.with_label_values(&p).set(i64::from(open));
    }

    /// Mirror the guide figures (called on every scrape).
    pub fn set_guide(&self, horizon_seconds: f64, coverage_ratio: f64, slots_total: u64) {
        self.horizon_seconds.set(horizon_seconds);
        self.coverage_ratio.set(coverage_ratio);
        self.slots_total
            .set(i64::try_from(slots_total).unwrap_or(i64::MAX));
    }

    /// Mirror one source's channel counts (called on every scrape). Only
    /// call with a configured source name.
    pub fn set_source_channels(&self, source: &str, matched: usize, unmatched: usize) {
        let to_i64 = |n: usize| i64::try_from(n).unwrap_or(i64::MAX);
        self.source_channels
            .with_label_values(&[source, "matched"])
            .set(to_i64(matched));
        self.source_channels
            .with_label_values(&[source, "unmatched"])
            .set(to_i64(unmatched));
    }

    /// The text exposition of every series.
    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        // Encoding into a Vec cannot fail.
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .expect("text encoding");
        String::from_utf8(buf).expect("exposition is UTF-8")
    }
}
