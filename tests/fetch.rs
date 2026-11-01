//! Tests for the shared fetch layer, against the fake provider server and a
//! controllable clock. No test sleeps in real time for more than a fraction
//! of a second: waiting is done by moving `FakeClock` forward.

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use patina::clock::{Clock, FakeClock};
use patina::config::LineupCfg;
use patina::fetch::{CircuitState, FetchCfg, FetchLayer, FetchOutcome, SequenceJitter, SkipReason};
use patina::model::ProviderId;
use patina::provider::{FetchError, ParsedWindow, Provider, RawResponse};
use patina::store::{Store, connect};
use support::fake_provider::{Behavior, FakeProvider};

fn lineup() -> LineupCfg {
    LineupCfg {
        name: "ota".into(),
        lineup_id: "USA-OTA55401".into(),
        postal_code: "55401".into(),
        country: "USA".into(),
    }
}

fn gn() -> ProviderId {
    ProviderId("gracenote".into())
}

/// 2026-11-01 12:00 UTC: an ordinary moment, far from a day boundary.
fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 1, 12, 0, 0).unwrap()
}

/// The start of the n-th 6-hour window from t0 (distinct windows never hit
/// each other's raw cache).
fn window(n: i64) -> DateTime<Utc> {
    t0() + TimeDelta::hours(6 * n)
}

/// Settings for tests: defaults, except no minimum interval (so the limiter
/// does not move the clock unless a test wants it to) and a given budget.
fn cfg(budget: u32) -> FetchCfg {
    FetchCfg {
        default_daily_budget: budget,
        min_interval: Duration::ZERO,
        ..FetchCfg::default()
    }
}

async fn store() -> Arc<dyn Store> {
    connect("sqlite::memory:").await.unwrap()
}

struct Harness {
    fake: FakeProvider,
    store: Arc<dyn Store>,
    clock: Arc<FakeClock>,
    layer: FetchLayer,
}

async fn harness(cfg: FetchCfg) -> Harness {
    let fake = FakeProvider::start().await;
    let store = store().await;
    let clock = Arc::new(FakeClock::new(t0()));
    let layer = FetchLayer::new(
        store.clone(),
        vec![Arc::new(fake.gracenote())],
        cfg,
        clock.clone(),
    );
    Harness {
        fake,
        store,
        clock,
        layer,
    }
}

impl Harness {
    async fn fetch(&self, n: i64) -> FetchOutcome {
        self.layer.fetch(&gn(), &lineup(), window(n)).await
    }
}

fn assert_fresh(o: &FetchOutcome) {
    match o {
        FetchOutcome::Fresh(w) => assert!(w.channels.len() > 50, "fixture has 89 channels"),
        other => panic!("expected Fresh, got {other:?}"),
    }
}

fn secs(n: i64) -> TimeDelta {
    TimeDelta::seconds(n)
}

#[tokio::test]
async fn budget_hard_stop_after_n_requests() {
    let h = harness(cfg(3)).await;
    for n in 0..3 {
        assert_fresh(&h.fetch(n).await);
    }
    assert_eq!(h.fake.request_count(), 3);

    // The 4th (and any later) request is refused without touching the network.
    for n in 3..10 {
        match h.fetch(n).await {
            FetchOutcome::Skipped(SkipReason::BudgetExhausted) => {}
            other => panic!("expected BudgetExhausted, got {other:?}"),
        }
    }
    assert_eq!(h.fake.request_count(), 3, "zero extra HTTP calls");
    let st = h.layer.provider_state(&gn()).await.unwrap();
    assert_eq!((st.budget_used, st.budget_limit), (3, 3));
}

#[tokio::test]
async fn budget_survives_restart() {
    let fake = FakeProvider::start().await;
    let store = store().await;
    let clock = Arc::new(FakeClock::new(t0()));
    let new_layer = || {
        FetchLayer::new(
            store.clone(),
            vec![Arc::new(fake.gracenote())],
            cfg(5),
            clock.clone(),
        )
    };

    // First process uses 2 requests, then "restarts".
    let first = new_layer();
    assert_fresh(&first.fetch(&gn(), &lineup(), window(0)).await);
    assert_fresh(&first.fetch(&gn(), &lineup(), window(1)).await);
    drop(first);
    let second = new_layer();
    assert_eq!(
        second.provider_state(&gn()).await.unwrap().budget_used,
        2,
        "used count carries over"
    );
    drop(second);

    // A crash loop: 50 fresh layers, each making one attempt on a new window.
    let mut refused = 0;
    for n in 2..52 {
        let layer = new_layer();
        match layer.fetch(&gn(), &lineup(), window(n)).await {
            FetchOutcome::Fresh(_) => {}
            FetchOutcome::Skipped(SkipReason::BudgetExhausted) => refused += 1,
            other => panic!("unexpected {other:?}"),
        }
        assert!(fake.request_count() <= 5, "budget exceeded after restart");
    }
    assert_eq!(fake.request_count(), 5);
    assert_eq!(refused, 47);
}

#[tokio::test]
async fn budget_resets_at_utc_day_boundary() {
    let h = harness(cfg(2)).await;
    // 23:59:00 UTC on 2026-11-01.
    let late = Utc.with_ymd_and_hms(2026, 11, 1, 23, 59, 0).unwrap();
    h.clock.set(late);
    assert_fresh(&h.fetch(0).await);
    assert_fresh(&h.fetch(1).await);
    assert!(matches!(
        h.fetch(2).await,
        FetchOutcome::Skipped(SkipReason::BudgetExhausted)
    ));

    // One second before midnight: still the same day.
    h.clock
        .set(Utc.with_ymd_and_hms(2026, 11, 1, 23, 59, 59).unwrap());
    assert!(matches!(
        h.fetch(2).await,
        FetchOutcome::Skipped(SkipReason::BudgetExhausted)
    ));

    // Midnight UTC: a new budget day.
    h.clock
        .set(Utc.with_ymd_and_hms(2026, 11, 2, 0, 0, 0).unwrap());
    assert_eq!(h.layer.provider_state(&gn()).await.unwrap().budget_used, 0);
    assert_fresh(&h.fetch(2).await);
    assert_eq!(h.fake.request_count(), 3);
    let day1 = chrono::NaiveDate::from_ymd_opt(2026, 11, 1).unwrap();
    assert_eq!(h.store.budget_used(&gn(), day1).await.unwrap(), 2);
}

#[tokio::test]
async fn retry_after_is_honored() {
    let h = harness(cfg(100)).await;
    h.fake.script([Behavior::RetryAfter(120)]);

    match h.fetch(0).await {
        FetchOutcome::Skipped(SkipReason::RateLimited { not_before }) => {
            assert_eq!(not_before, t0() + secs(120));
        }
        other => panic!("expected RateLimited, got {other:?}"),
    }
    assert_eq!(h.fake.request_count(), 1);

    // Any window of that provider is held off until +120 s (our own hold:
    // no request sent, a different variant from the real 429 above).
    h.clock.advance(Duration::from_secs(119));
    for n in 0..3 {
        assert!(matches!(
            h.fetch(n).await,
            FetchOutcome::Skipped(SkipReason::BackingOffUntil(u)) if u == t0() + secs(120)
        ));
    }
    assert_eq!(
        h.fake.request_count(),
        1,
        "no call before Retry-After passes"
    );

    h.clock.advance(Duration::from_secs(1));
    assert_fresh(&h.fetch(0).await);
    assert_eq!(h.fake.request_count(), 2);
}

#[tokio::test]
async fn backoff_grows_then_caps() {
    let h = harness(FetchCfg {
        backoff_base: Duration::from_secs(30),
        backoff_max: Duration::from_secs(240),
        // Keep the circuit out of the way; this test is about backoff only.
        circuit_threshold: 100,
        ..cfg(100)
    })
    .await;
    h.fake.set_default(Behavior::Status(500));

    let expected = [30, 60, 120, 240, 240, 240];
    for (i, wait) in expected.into_iter().enumerate() {
        let before = h.clock.now();
        match h.fetch(0).await {
            FetchOutcome::Skipped(SkipReason::Failed(FetchError::Transient(_))) => {}
            other => panic!("expected Failed(Transient), got {other:?}"),
        }
        assert_eq!(h.fake.request_count(), i + 1);

        // Before the backoff has passed, nothing goes out.
        let until = match h.fetch(0).await {
            FetchOutcome::Skipped(SkipReason::BackingOffUntil(u)) => u,
            other => panic!("expected BackingOffUntil, got {other:?}"),
        };
        assert_eq!(until - before, secs(wait), "failure #{}", i + 1);
        h.clock.advance(Duration::from_secs(wait as u64 - 1));
        assert!(matches!(
            h.fetch(0).await,
            FetchOutcome::Skipped(SkipReason::BackingOffUntil(_))
        ));
        assert_eq!(h.fake.request_count(), i + 1);
        h.clock.advance(Duration::from_secs(1));
    }

    // A success resets the backoff.
    h.fake.set_default(Behavior::ok_fixture());
    assert_fresh(&h.fetch(0).await);
    h.fake.set_default(Behavior::Status(500));
    let before = h.clock.now();
    h.fetch(1).await;
    match h.fetch(1).await {
        FetchOutcome::Skipped(SkipReason::BackingOffUntil(u)) => assert_eq!(u - before, secs(30)),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn circuit_opens_after_5_consecutive_failures_and_closes_after_cooldown_probe() {
    let h = harness(cfg(100)).await;
    h.fake.set_default(Behavior::Status(500));

    // Five failures, each after its backoff has passed.
    for i in 1..=5 {
        assert!(matches!(
            h.fetch(0).await,
            FetchOutcome::Skipped(SkipReason::Failed(FetchError::Transient(_)))
        ));
        assert_eq!(h.fake.request_count(), i);
        let st = h.layer.provider_state(&gn()).await.unwrap();
        assert_eq!(st.consecutive_failures, i as u32);
        if i < 5 {
            assert_eq!(st.circuit, CircuitState::Closed);
            h.clock.advance(Duration::from_secs(3600));
        }
    }
    let opened_at = h.clock.now();
    let until = opened_at + TimeDelta::minutes(15);
    let st = h.layer.provider_state(&gn()).await.unwrap();
    assert_eq!(st.circuit, CircuitState::Open { until });
    assert!(st.last_error.is_some());

    // While open, nothing goes out, for any window.
    h.clock.advance(Duration::from_secs(15 * 60 - 1));
    for n in 0..3 {
        match h.fetch(n).await {
            FetchOutcome::Skipped(SkipReason::CircuitOpen { until: u }) => assert_eq!(u, until),
            other => panic!("expected CircuitOpen, got {other:?}"),
        }
    }
    assert_eq!(h.fake.request_count(), 5);

    // Cooldown over: half-open, exactly one probe. It fails, so the circuit
    // opens again. The new cooldown is max(15 min, backoff for failure #6 =
    // 30 s × 2^5 = 16 min), so repeated failed probes get rarer.
    h.clock.set(until);
    assert_eq!(
        h.layer.provider_state(&gn()).await.unwrap().circuit,
        CircuitState::HalfOpen
    );
    assert!(matches!(
        h.fetch(0).await,
        FetchOutcome::Skipped(SkipReason::Failed(_))
    ));
    assert_eq!(h.fake.request_count(), 6);
    let until2 = until + TimeDelta::minutes(16);
    assert!(matches!(
        h.fetch(0).await,
        FetchOutcome::Skipped(SkipReason::CircuitOpen { until: u }) if u == until2
    ));
    assert_eq!(h.fake.request_count(), 6);

    // Second cooldown over: the probe succeeds and the circuit closes.
    h.fake.set_default(Behavior::ok_fixture());
    h.clock.set(until2);
    assert_fresh(&h.fetch(0).await);
    let st = h.layer.provider_state(&gn()).await.unwrap();
    assert_eq!(st.circuit, CircuitState::Closed);
    assert_eq!(st.consecutive_failures, 0);
    assert_eq!(st.last_success, Some(until2));
    assert_fresh(&h.fetch(1).await);
    assert_eq!(h.fake.request_count(), 8);
}

#[tokio::test]
async fn circuit_state_survives_restart() {
    let fake = FakeProvider::start().await;
    let store = store().await;
    let clock = Arc::new(FakeClock::new(t0()));
    let new_layer = || {
        FetchLayer::new(
            store.clone(),
            vec![Arc::new(fake.gracenote())],
            cfg(100),
            clock.clone(),
        )
    };
    fake.set_default(Behavior::Status(500));
    // Five failures, each from a freshly started process, each after the
    // previous backoff has passed.
    for i in 0..5 {
        new_layer().fetch(&gn(), &lineup(), window(0)).await;
        if i < 4 {
            clock.advance(Duration::from_secs(3600));
        }
    }
    assert_eq!(fake.request_count(), 5);
    let layer = new_layer();
    let st = layer.provider_state(&gn()).await.unwrap();
    assert_eq!(st.consecutive_failures, 5, "failures persisted");
    assert!(matches!(st.circuit, CircuitState::Open { .. }));
    drop(layer);
    for n in 0..5 {
        assert!(matches!(
            new_layer().fetch(&gn(), &lineup(), window(n)).await,
            FetchOutcome::Skipped(SkipReason::CircuitOpen { .. })
        ));
    }
    assert_eq!(
        fake.request_count(),
        5,
        "a restarted process respects the open circuit"
    );
}

#[tokio::test]
async fn bad_window_400_is_remembered_and_not_retried_this_cycle() {
    let h = harness(cfg(100)).await;
    h.fake.script([Behavior::Status(400)]);

    assert!(matches!(
        h.fetch(0).await,
        FetchOutcome::Skipped(SkipReason::Failed(FetchError::BadWindow))
    ));
    assert_eq!(h.fake.request_count(), 1);

    // Same window again: remembered, no request.
    assert!(matches!(
        h.fetch(0).await,
        FetchOutcome::Skipped(SkipReason::KnownBadWindow)
    ));
    assert_eq!(h.fake.request_count(), 1);

    // Other windows are unaffected, and a 400 is not a provider failure.
    assert_fresh(&h.fetch(1).await);
    let st = h.layer.provider_state(&gn()).await.unwrap();
    assert_eq!(st.consecutive_failures, 0);
    assert_eq!(st.circuit, CircuitState::Closed);

    // Still remembered just before the 6 h memory expires...
    h.clock.advance(Duration::from_secs(6 * 3600 - 1));
    assert!(matches!(
        h.fetch(0).await,
        FetchOutcome::Skipped(SkipReason::KnownBadWindow)
    ));
    assert_eq!(h.fake.request_count(), 2);
    // ...and retried once it has.
    h.clock.advance(Duration::from_secs(1));
    assert_fresh(&h.fetch(0).await);
    assert_eq!(h.fake.request_count(), 3);
}

#[tokio::test]
async fn second_fetch_of_same_window_within_ttl_uses_raw_cache_without_http() {
    // Budget 1: the cache hit must work even with the budget used up.
    let h = harness(cfg(1)).await;
    let first = match h.fetch(0).await {
        FetchOutcome::Fresh(w) => w,
        other => panic!("{other:?}"),
    };
    assert_eq!(h.fake.request_count(), 1);

    h.clock.advance(Duration::from_secs(30 * 60));
    let second = match h.fetch(0).await {
        FetchOutcome::FromCache(w) => w,
        other => panic!("expected FromCache, got {other:?}"),
    };
    assert_eq!(h.fake.request_count(), 1, "no HTTP for a cache hit");
    assert_eq!(second.channels, first.channels);
    assert_eq!(second.observations.len(), first.observations.len());
    // Observations keep the original fetch time, not the cache read time.
    assert_eq!(
        second.observations[0].fetched_at,
        first.observations[0].fetched_at
    );
    let st = h.layer.provider_state(&gn()).await.unwrap();
    assert_eq!(st.budget_used, 1, "a cache hit costs no budget");

    // Once the cached copy is older than the cache max age, it is not used.
    h.clock.advance(Duration::from_secs(31 * 60));
    assert!(matches!(
        h.fetch(0).await,
        FetchOutcome::Skipped(SkipReason::BudgetExhausted)
    ));
}

#[tokio::test]
async fn parse_failure_counts_as_failure_and_never_replaces_cached_body() {
    let h = harness(cfg(100)).await;
    assert_fresh(&h.fetch(0).await);
    let good = h
        .store
        .raw_get_latest(
            &gn(),
            "ota",
            window(0),
            Duration::from_secs(86400),
            h.clock.now(),
        )
        .await
        .unwrap()
        .expect("good body cached");

    // Past the cache max age, the next fetch goes out and gets garbage.
    h.clock.advance(Duration::from_secs(2 * 3600));
    h.fake.script([Behavior::Truncated]);
    match h.fetch(0).await {
        FetchOutcome::Skipped(SkipReason::ParseFailed(msg)) => assert!(!msg.is_empty()),
        other => panic!("expected ParseFailed, got {other:?}"),
    }
    assert_eq!(h.fake.request_count(), 2);
    let st = h.layer.provider_state(&gn()).await.unwrap();
    assert_eq!(st.consecutive_failures, 1);

    // The cache still holds the good body, not the truncated one.
    let latest = h
        .store
        .raw_get_latest(
            &gn(),
            "ota",
            window(0),
            Duration::from_secs(86400),
            h.clock.now(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.fetched_at, good.fetched_at);
    assert_eq!(latest.body, good.body);

    // It is backed off like any failure.
    assert!(matches!(
        h.fetch(0).await,
        FetchOutcome::Skipped(SkipReason::BackingOffUntil(_))
    ));
    assert_eq!(h.fake.request_count(), 2);
}

#[tokio::test]
async fn slow_provider_times_out_as_transient_failure() {
    let h = harness(FetchCfg {
        request_timeout: Duration::from_millis(50),
        ..cfg(100)
    })
    .await;
    h.fake.script([Behavior::Delay(2000)]);
    assert!(matches!(
        h.fetch(0).await,
        FetchOutcome::Skipped(SkipReason::Failed(FetchError::Transient(_)))
    ));
    let st = h.layer.provider_state(&gn()).await.unwrap();
    assert_eq!(st.budget_used, 1, "a timed-out attempt still counts");
}

#[tokio::test]
async fn requests_carry_browser_like_user_agent() {
    let h = harness(cfg(100)).await;
    assert_fresh(&h.fetch(0).await);
    let reqs = h.fake.requests();
    let ua = reqs[0].user_agent.as_deref().expect("User-Agent header");
    assert!(ua.starts_with("Mozilla/5.0"), "{ua}");
    assert!(ua.contains("Chrome/") || ua.contains("Firefox/"), "{ua}");
    assert_eq!(reqs[0].query["time"], window(0).timestamp().to_string());
}

/// Wraps a provider and records the (fake) time of every HTTP attempt.
struct Recording {
    inner: Arc<dyn Provider>,
    clock: Arc<FakeClock>,
    times: Mutex<Vec<DateTime<Utc>>>,
}

#[async_trait]
impl Provider for Recording {
    fn id(&self) -> &ProviderId {
        self.inner.id()
    }
    async fn fetch_window(
        &self,
        lineup: &LineupCfg,
        window_start: DateTime<Utc>,
    ) -> Result<RawResponse, FetchError> {
        self.times.lock().unwrap().push(self.clock.now());
        self.inner.fetch_window(lineup, window_start).await
    }
    fn parse(&self, l: &LineupCfg, raw: &RawResponse, at: DateTime<Utc>) -> ParsedWindow {
        self.inner.parse(l, raw, at)
    }
}

#[tokio::test]
async fn concurrent_fetches_respect_min_interval_with_jitter() {
    let fake = FakeProvider::start().await;
    let clock = Arc::new(FakeClock::new(t0()));
    let recording = Arc::new(Recording {
        inner: Arc::new(fake.gracenote()),
        clock: clock.clone(),
        times: Mutex::new(Vec::new()),
    });
    // Jitter samples are in [-1, 1]; with 2 s and ±50% the gaps become
    // 2 s × (1 + 0.5 × sample): 1 s, 3 s, 2 s, 2.5 s, 1.5 s.
    let layer = Arc::new(FetchLayer::new(
        store().await,
        vec![recording.clone()],
        FetchCfg {
            default_daily_budget: 100,
            min_interval: Duration::from_secs(2),
            jitter_fraction: 0.5,
            jitter: Arc::new(SequenceJitter::new(vec![-1.0, 1.0, 0.0, 0.5, -0.5])),
            ..FetchCfg::default()
        },
        clock.clone(),
    ));

    let tasks: Vec<_> = (0..6)
        .map(|n| {
            let layer = layer.clone();
            tokio::spawn(async move { layer.fetch(&gn(), &lineup(), window(n)).await })
        })
        .collect();
    for t in tasks {
        assert_fresh(&t.await.unwrap());
    }

    let times = recording.times.lock().unwrap().clone();
    assert_eq!(times.len(), 6);
    assert_eq!(times[0], t0(), "the first request does not wait");
    let gaps: Vec<i64> = times
        .windows(2)
        .map(|w| (w[1] - w[0]).num_milliseconds())
        .collect();
    assert_eq!(gaps, [1000, 3000, 2000, 2500, 1500]);
    assert_eq!(
        fake.max_in_flight(),
        1,
        "one request in flight per provider"
    );
}

/// Open the circuit with one 403 (fatal) and move to the end of its cooldown,
/// so the next request is the half-open probe. Returns the harness.
async fn half_open_after_403(cfg: FetchCfg) -> Harness {
    let h = harness(cfg).await;
    h.fake.script([Behavior::Status(403)]);
    assert!(matches!(
        h.fetch(0).await,
        FetchOutcome::Skipped(SkipReason::Failed(FetchError::Fatal(_)))
    ));
    let st = h.layer.provider_state(&gn()).await.unwrap();
    assert_eq!(st.consecutive_failures, 1);
    let until = match st.circuit {
        CircuitState::Open { until } => until,
        other => panic!("403 should open the circuit, got {other:?}"),
    };
    h.clock.set(until);
    assert_eq!(
        h.layer.provider_state(&gn()).await.unwrap().circuit,
        CircuitState::HalfOpen
    );
    assert_eq!(h.fake.request_count(), 1);
    h
}

/// After a failed probe: open again, and nothing more goes out, for any
/// window, even long after the short backoff (60 s) would have passed.
async fn assert_reopened_and_silent(h: &Harness) {
    let probe_at = h.clock.now();
    let count = h.fake.request_count();
    let st = h.layer.provider_state(&gn()).await.unwrap();
    assert_eq!(
        st.circuit,
        CircuitState::Open {
            until: probe_at + TimeDelta::minutes(15)
        }
    );
    for step in [1, 60, 120, 240, 600] {
        h.clock.set(probe_at + secs(step));
        for n in 0..3 {
            assert!(
                matches!(
                    h.fetch(n).await,
                    FetchOutcome::Skipped(SkipReason::CircuitOpen { .. })
                ),
                "+{step}s window {n}"
            );
        }
    }
    assert_eq!(h.fake.request_count(), count, "no further requests");
}

#[tokio::test]
async fn half_open_probe_failure_with_500_reopens_circuit_and_sends_no_further_requests() {
    let h = half_open_after_403(cfg(100)).await;
    h.fake.script([Behavior::Status(500)]);
    assert!(matches!(
        h.fetch(0).await,
        FetchOutcome::Skipped(SkipReason::Failed(FetchError::Transient(_)))
    ));
    assert_eq!(h.fake.request_count(), 2);
    assert_reopened_and_silent(&h).await;
}

#[tokio::test]
async fn half_open_probe_timeout_reopens_circuit_and_sends_no_further_requests() {
    let h = half_open_after_403(FetchCfg {
        request_timeout: Duration::from_millis(50),
        ..cfg(100)
    })
    .await;
    h.fake.script([Behavior::Delay(2000)]);
    assert!(matches!(
        h.fetch(0).await,
        FetchOutcome::Skipped(SkipReason::Failed(FetchError::Transient(_)))
    ));
    assert_eq!(h.fake.request_count(), 2);
    assert_reopened_and_silent(&h).await;
}

#[tokio::test]
async fn half_open_probe_returning_400_closes_circuit_and_records_bad_window() {
    let h = half_open_after_403(cfg(100)).await;
    h.fake.script([Behavior::Status(400)]);
    assert!(matches!(
        h.fetch(0).await,
        FetchOutcome::Skipped(SkipReason::Failed(FetchError::BadWindow))
    ));
    let probe_at = h.clock.now();
    let st = h.layer.provider_state(&gn()).await.unwrap();
    assert_eq!(st.circuit, CircuitState::Closed);
    assert_eq!(st.consecutive_failures, 0);
    assert_eq!(st.last_success, Some(probe_at));
    assert!(
        h.store
            .bad_window_is_bad(&gn(), "ota", window(0), probe_at)
            .await
            .unwrap()
    );
    // The bad window is not retried; other windows work normally.
    assert!(matches!(
        h.fetch(0).await,
        FetchOutcome::Skipped(SkipReason::KnownBadWindow)
    ));
    assert_fresh(&h.fetch(1).await);
    assert_eq!(h.fake.request_count(), 3);
}

#[tokio::test]
async fn half_open_admits_only_one_probe() {
    // The probe is slow (it times out after 50 ms of real time), so the
    // second caller arrives while it is still in flight.
    let h = Arc::new(
        half_open_after_403(FetchCfg {
            request_timeout: Duration::from_millis(50),
            ..cfg(100)
        })
        .await,
    );
    h.fake.script([Behavior::Delay(2000)]);
    let a = {
        let h = h.clone();
        tokio::spawn(async move { h.fetch(0).await })
    };
    let b = {
        let h = h.clone();
        tokio::spawn(async move { h.fetch(1).await })
    };
    let outcomes = [a.await.unwrap(), b.await.unwrap()];
    assert_eq!(h.fake.request_count(), 2, "exactly one probe after the 403");
    let probes = outcomes
        .iter()
        .filter(|o| matches!(o, FetchOutcome::Skipped(SkipReason::Failed(_))))
        .count();
    let refused = outcomes
        .iter()
        .filter(|o| matches!(o, FetchOutcome::Skipped(SkipReason::CircuitOpen { .. })))
        .count();
    assert_eq!((probes, refused), (1, 1), "{outcomes:?}");
    assert_eq!(h.fake.max_in_flight(), 1);
}

#[tokio::test]
async fn retry_after_shorter_than_backoff_does_not_shorten_the_wait() {
    let h = harness(cfg(100)).await;
    // First failure: our backoff would be 30 s; the server says 1 s.
    h.fake.script([Behavior::RetryAfter(1)]);
    match h.fetch(0).await {
        FetchOutcome::Skipped(SkipReason::RateLimited { not_before }) => {
            assert_eq!(not_before, t0() + secs(30));
        }
        other => panic!("expected RateLimited, got {other:?}"),
    }
    h.clock.advance(Duration::from_secs(29));
    assert!(matches!(
        h.fetch(0).await,
        FetchOutcome::Skipped(SkipReason::BackingOffUntil(_))
    ));
    assert_eq!(h.fake.request_count(), 1);

    // Second failure: backoff 60 s beats `Retry-After: 0`.
    h.clock.advance(Duration::from_secs(1));
    h.fake.script([Behavior::RetryAfter(0)]);
    let before = h.clock.now();
    match h.fetch(0).await {
        FetchOutcome::Skipped(SkipReason::RateLimited { not_before }) => {
            assert_eq!(not_before - before, secs(60));
        }
        other => panic!("expected RateLimited, got {other:?}"),
    }
    assert_eq!(h.fake.request_count(), 2);
}

// ---- response size cap ----

#[tokio::test]
async fn oversized_provider_body_is_a_failed_fetch_and_not_cached() {
    let fake = FakeProvider::start().await;
    fake.set_default(Behavior::ok_fixture());
    let clock = Arc::new(FakeClock::new(t0()));
    // The fixture is about 400 KB; allow only 1000 bytes (production: 32 MiB).
    let layer = FetchLayer::new(
        store().await,
        vec![Arc::new(fake.gracenote().with_max_body_bytes(1000))],
        cfg(100),
        clock.clone(),
    );
    match layer.fetch(&gn(), &lineup(), window(0)).await {
        FetchOutcome::Skipped(SkipReason::Failed(FetchError::Transient(msg))) => {
            assert!(msg.contains("larger than 1000 bytes"), "{msg}");
        }
        other => panic!("expected a transient failure, got {other:?}"),
    }
    // Nothing was cached: once the backoff has passed, the window is asked
    // for again (and fails again).
    clock.advance(Duration::from_secs(120));
    assert!(matches!(
        layer.fetch(&gn(), &lineup(), window(0)).await,
        FetchOutcome::Skipped(SkipReason::Failed(FetchError::Transient(_)))
    ));
    assert_eq!(fake.request_count(), 2);

    // A limit the body fits in works normally.
    let len = support::fake_provider::fixture().len();
    let ok = fake.gracenote().with_max_body_bytes(len);
    let raw = ok.fetch_window(&lineup(), window(1)).await.unwrap();
    assert_eq!(raw.body.len(), len);
}

/// A one-shot HTTP server (plain std thread) that answers with a chunked
/// body of `total` bytes and NO Content-Length, so only the streaming check
/// can catch it.
fn chunked_server(total: usize) -> url::Url {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let mut buf = [0u8; 4096];
        let _ = s.read(&mut buf);
        let _ = s.write_all(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
        );
        let chunk = vec![b'x'; 1024];
        let mut sent = 0;
        while sent < total {
            let n = chunk.len().min(total - sent);
            let _ = write!(s, "{n:x}\r\n");
            let _ = s.write_all(&chunk[..n]);
            let _ = s.write_all(b"\r\n");
            sent += n;
        }
        let _ = s.write_all(b"0\r\n\r\n");
    });
    url::Url::parse(&format!("http://{addr}/")).unwrap()
}

#[tokio::test]
async fn body_without_content_length_is_cut_off_at_the_limit() {
    let base = chunked_server(64 * 1024);
    let provider = patina::provider::gracenote::GracenoteProvider::new(
        patina::provider::gracenote::http_client(Duration::from_secs(10)).unwrap(),
        base,
    )
    .with_max_body_bytes(10 * 1024);
    match provider.fetch_window(&lineup(), window(0)).await {
        Err(FetchError::Transient(msg)) => assert!(msg.contains("larger"), "{msg}"),
        other => panic!("expected a transient failure, got {other:?}"),
    }
}

// ---- fail closed on store errors ----

#[tokio::test]
async fn store_error_at_a_gate_sends_nothing_and_is_not_a_provider_failure() {
    let fake = FakeProvider::start().await;
    let failing = Arc::new(support::failing_store::FailingStore::new(store().await));
    let clock = Arc::new(FakeClock::new(t0()));
    let layer = FetchLayer::new(
        failing.clone(),
        vec![Arc::new(fake.gracenote())],
        cfg(100),
        clock.clone(),
    );
    failing
        .fail_bad_window_check
        .store(true, std::sync::atomic::Ordering::SeqCst);
    // Its own variant (not a `Failed(..)`), and nothing reached the server.
    assert_eq!(
        layer
            .fetch(&gn(), &lineup(), window(0))
            .await
            .skip_reason()
            .cloned(),
        Some(SkipReason::StoreUnavailable)
    );
    assert_eq!(fake.request_count(), 0);
    failing
        .fail_bad_window_check
        .store(false, std::sync::atomic::Ordering::SeqCst);
    // Neither budget nor backoff nor circuit moved: the next attempt goes
    // out at once.
    let st = layer.provider_state(&gn()).await.unwrap();
    assert_eq!((st.budget_used, st.consecutive_failures), (0, 0));
    assert_eq!(st.circuit, CircuitState::Closed);
    assert_fresh(&layer.fetch(&gn(), &lineup(), window(0)).await);
    assert_eq!(fake.request_count(), 1);
}
