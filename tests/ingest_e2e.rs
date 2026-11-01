//! End-to-end ingest: scheduler -> fetch layer -> fake Gracenote server ->
//! parse -> merge -> store. Everything runs offline against the fake
//! provider (tests/support/fake_provider.rs) and a `FakeClock`, so the tests
//! are deterministic and take no real time.
//!
//! Every test counts the HTTP requests that really reached the fake server:
//! being a good neighbor to the provider is the scheduler's first job.
//!
//! Time setup: "now" is 2026-10-31 20:00 UTC, the evening of the captured
//! fixture. The window containing now is 18:00-24:00 UTC, so a 7-day horizon
//! (until 2026-11-07 20:00) covers the windows starting 2026-10-31 18:00 up
//! to and including 2026-11-07 18:00: 4 per day for 7 days, plus one = 29.

mod common;
mod support;

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use patina::channels::hdhomerun::parse_lineup;
use patina::channels::matching::{Overrides, match_channels};
use patina::clock::FakeClock;
use patina::config::{LineupCfg, Weights};
use patina::fetch::{CircuitState, FetchCfg, FetchLayer, SequenceJitter};
use patina::merge::SlotWithObservations;
use patina::model::{ChannelId, ProviderId};
use patina::provider::Provider;
use patina::provider::gracenote::{GracenoteProvider, http_client};
use patina::scheduler::{ProviderRef, Scheduler, SchedulerDeps, TickReport};
use patina::store::{Store, WindowFailure, connect};
use support::failing_store::FailingStore;
use support::fake_provider::{Behavior, FakeProvider, fixture};
use tokio_util::sync::CancellationToken;

const TUNER_LINEUP: &[u8] = include_bytes!("fixtures/hdhomerun/lineup.json");

/// Windows in a 7-day horizon from `now0()` (see the module comment).
const HORIZON_WINDOWS: usize = 29;

fn now0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 31, 20, 0, 0).unwrap()
}

/// 2026-10-31 18:00 UTC: the first window, the one containing `now0()`.
fn first_window() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 31, 18, 0, 0).unwrap()
}

/// `n` consecutive 6-hour window starts from `from`, as unix seconds (the
/// form the fake server sees in the `time` query parameter).
fn grid(from: DateTime<Utc>, n: usize) -> Vec<i64> {
    (0..n as i64)
        .map(|i| (from + TimeDelta::hours(6 * i)).timestamp())
        .collect()
}

fn hours_mins(h: i64, m: i64) -> DateTime<Utc> {
    now0() + TimeDelta::hours(h) + TimeDelta::minutes(m)
}

fn gn() -> ProviderId {
    ProviderId("gracenote".into())
}

fn lineup() -> LineupCfg {
    LineupCfg {
        name: "ota".into(),
        lineup_id: "USA-OTA55401".into(),
        postal_code: "55401".into(),
        country: "USA".into(),
    }
}

/// Fetch settings for tests: a 1 s minimum interval without jitter (so the
/// fake clock moves 1 s per request, deterministically) and a given budget.
fn fetch_cfg(budget: u32) -> FetchCfg {
    FetchCfg {
        default_daily_budget: budget,
        min_interval: Duration::from_secs(1),
        jitter_fraction: 0.0,
        jitter: Arc::new(SequenceJitter::new(vec![0.0])),
        ..FetchCfg::default()
    }
}

struct Harness {
    fake: FakeProvider,
    store: Arc<dyn Store>,
    clock: Arc<FakeClock>,
    fetch: Arc<FetchLayer>,
    sched: Scheduler,
}

/// A scheduler (with its own fetch layer, as after a process restart) over
/// `store`, talking to `provider`.
fn scheduler_for(
    store: Arc<dyn Store>,
    provider: Arc<dyn Provider>,
    clock: Arc<FakeClock>,
    budget: u32,
) -> (Arc<FetchLayer>, Scheduler) {
    let fetch = Arc::new(FetchLayer::new(
        store.clone(),
        vec![provider],
        fetch_cfg(budget),
        clock.clone(),
    ));
    let deps = SchedulerDeps {
        store,
        fetch: fetch.clone(),
        providers: vec![ProviderRef {
            id: gn(),
            lineups: vec![lineup()],
        }],
        horizon_days: 7,
        weights: Weights::default(),
        tick_interval: Duration::from_secs(60),
        feed_generation: None,
    };
    (fetch, Scheduler::new(deps, clock))
}

/// A fake server answering every window with the shifted real fixture, an
/// empty in-memory SQLite store and a scheduler with the given daily budget.
async fn harness(budget: u32) -> Harness {
    harness_on(connect("sqlite::memory:").await.unwrap(), budget).await
}

async fn harness_on(store: Arc<dyn Store>, budget: u32) -> Harness {
    let fake = FakeProvider::start().await;
    fake.set_default(Behavior::ok_shifted());
    let clock = Arc::new(FakeClock::new(now0()));
    let (fetch, sched) = scheduler_for(
        store.clone(),
        Arc::new(fake.gracenote()),
        clock.clone(),
        budget,
    );
    Harness {
        fake,
        store,
        clock,
        fetch,
        sched,
    }
}

/// Window starts (unix seconds) of every request the fake server received.
fn requested(fake: &FakeProvider) -> Vec<i64> {
    fake.requests()
        .iter()
        .map(|r| r.query["time"].parse().unwrap())
        .collect()
}

/// Requests received after the first `skip`.
fn requested_after(fake: &FakeProvider, skip: usize) -> Vec<i64> {
    requested(fake).into_iter().skip(skip).collect()
}

/// The whole master guide: every channel, every slot with every
/// observation. Two dumps are equal only if nothing at all changed.
async fn dump(store: &Arc<dyn Store>) -> Vec<(ChannelId, Vec<SlotWithObservations>)> {
    let far_past = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
    let far_future = Utc.with_ymd_and_hms(2100, 1, 1, 0, 0, 0).unwrap();
    let mut out = Vec::new();
    for c in store.list_channels().await.unwrap() {
        let slots = store
            .slots_with_observations(&c.id, far_past, far_future)
            .await
            .unwrap();
        out.push((c.id, slots));
    }
    out
}

fn slot_total(d: &[(ChannelId, Vec<SlotWithObservations>)]) -> usize {
    d.iter().map(|(_, s)| s.len()).sum()
}

fn assert_no_slot_changes(r: &TickReport) {
    assert_eq!(
        (r.slots_inserted, r.slots_updated, r.slots_removed),
        (0, 0, 0),
        "{r:?}"
    );
}

// ---------------------------------------------------------------------------
// Test data sanity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ok_shifted_moves_the_fixture_to_the_requested_window() {
    // The fake server's shifting is what makes 29 windows look like 29
    // different responses, so check it does exactly that.
    let fake = FakeProvider::start().await;
    fake.set_default(Behavior::ok_shifted());
    let p = fake.gracenote();
    let w = first_window();
    let raw = p.fetch_window(&lineup(), w).await.unwrap();
    let parsed = p.parse(&lineup(), &raw, now0());
    let original = p.parse(
        &lineup(),
        &patina::provider::RawResponse {
            status: 200,
            body: fixture(),
        },
        now0(),
    );
    assert!(parsed.parse_error.is_none());
    assert_eq!(parsed.channels, original.channels);
    assert_eq!(parsed.observations.len(), original.observations.len());
    // Fixture window 22:00 moved to 18:00: every time is 4 hours earlier,
    // and nothing else changed.
    for (a, b) in parsed.observations.iter().zip(&original.observations) {
        assert_eq!(a.start, b.start - TimeDelta::hours(4));
        assert_eq!(a.end, b.end - TimeDelta::hours(4));
        assert_eq!((&a.title, &a.description), (&b.title, &b.description));
    }
}

// ---------------------------------------------------------------------------
// The plan's named tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn first_tick_fetches_all_windows_for_horizon() {
    let h = harness(150).await;
    let r = h.sched.tick().await;

    // Every window of the horizon exactly once, nearest first.
    assert_eq!(requested(&h.fake), grid(first_window(), HORIZON_WINDOWS));
    assert_eq!(r.windows_due, HORIZON_WINDOWS);
    assert_eq!(r.fetched, HORIZON_WINDOWS);
    assert_eq!(r.failed, 0);
    assert!(r.slots_inserted > 500, "{r:?}");
    assert_eq!(r.channels_seen, 89);
    let state = h.fetch.provider_state(&gn()).await.unwrap();
    assert_eq!(state.budget_used, HORIZON_WINDOWS as u32);

    // Each clean response counted once as a reliability success.
    let rel = h.store.provider_reliability_load_all().await.unwrap();
    assert_eq!((rel[0].success, rel[0].total), (29, 29));
    // The response-quality check ran (the fixture has no gaps or overlaps).
    let integrity = r.last_integrity.expect("integrity of the last response");
    assert_eq!(integrity.channels, 89);
    assert_eq!(
        (integrity.gap_fraction, integrity.overlap_fraction),
        (0.0, 0.0)
    );
    assert!(h.sched.response_quality().contains_key(&gn()));

    // One score-history row per slot the plans inserted or updated.
    let history = h
        .store
        .score_history_delete_older_than(
            Utc.with_ymd_and_hms(2100, 1, 1, 0, 0, 0).unwrap(),
            100_000,
        )
        .await
        .unwrap();
    assert_eq!(history as usize, r.slots_inserted + r.slots_updated);
}

/// How many score-history rows exist; empties the table, so each call
/// counts the rows added since the previous one.
async fn take_score_history(store: &Arc<dyn Store>) -> u64 {
    store
        .score_history_delete_older_than(
            Utc.with_ymd_and_hms(2100, 1, 1, 0, 0, 0).unwrap(),
            1_000_000,
        )
        .await
        .unwrap()
}

/// The fixture with one programme renamed: the first event of every
/// channel gets a new title.
fn renamed_fixture() -> bytes::Bytes {
    let mut grid: serde_json::Value = serde_json::from_slice(&fixture()).unwrap();
    for ch in grid["channels"].as_array_mut().unwrap() {
        if let Some(ev) = ch["events"].as_array_mut().unwrap().first_mut() {
            ev["program"]["title"] = serde_json::Value::String("Special Report".into());
        }
    }
    bytes::Bytes::from(serde_json::to_vec(&grid).unwrap())
}

#[tokio::test]
async fn identical_refresh_does_not_rewrite_slots_or_grow_score_history() {
    // Every window answers with the SAME unshifted fixture (programmes from
    // 2026-10-31 21:00 to 2026-11-01 05:00), and "now" starts a day before
    // them, so the 5 near windows re-send exactly the same programmes every
    // 3 hours and nothing airs during the test. (The shifted fixture is not
    // usable here: neighbouring windows overlap and disagree at the edges.)
    let h = harness(150).await;
    h.fake.set_default(Behavior::ok_fixture());
    let start = now0() - TimeDelta::hours(24);
    let at = |h: i64, m: i64| start + TimeDelta::hours(h) + TimeDelta::minutes(m);
    h.clock.set(start);
    let r = h.sched.tick().await;
    assert!(r.slots_inserted > 500, "{r:?}");

    // Two refreshes let the scores settle: the first scoring pass had no
    // reliability history (neutral 0.5) and the second no hindsight samples
    // yet; both jump once to their real values.
    for (hh, mm) in [(3, 5), (6, 10)] {
        h.clock.set(at(hh, mm));
        h.sched.tick().await;
    }
    take_score_history(&h.store).await;

    // The next refresh of identical data: reliability (Laplace counts) and
    // the other signals moved a little, far less than SCORE_TOLERANCE, so no
    // slot is rewritten and no score-history row is added. (With the old
    // 1e-9 comparison every touched slot was rewritten here.)
    h.clock.set(at(9, 15));
    let r = h.sched.tick().await;
    assert_eq!(r.fetched, 5, "{r:?}");
    assert_eq!((r.slots_updated, r.slots_inserted), (0, 0), "{r:?}");
    assert_eq!(take_score_history(&h.store).await, 0);

    // A real change still updates the slot and records history: the first
    // programme of every channel comes back renamed.
    h.fake.set_default(Behavior::Ok(renamed_fixture()));
    h.clock.set(at(12, 20));
    let r = h.sched.tick().await;
    assert!(r.slots_updated >= 80, "{r:?}");
    let rows = take_score_history(&h.store).await;
    assert_eq!(rows as usize, r.slots_updated + r.slots_inserted);
    let d = dump(&h.store).await;
    assert!(
        d.iter()
            .any(|(_, slots)| slots.iter().any(|s| s.slot.title == "Special Report")),
        "the renamed programme is in the guide"
    );
}

#[tokio::test]
async fn second_tick_within_interval_makes_no_requests() {
    let h = harness(150).await;
    h.sched.tick().await;
    assert_eq!(h.fake.request_count(), HORIZON_WINDOWS);

    // A minute later (the normal tick interval) and almost 3 hours later:
    // every window is still fresh, so nothing is sent.
    for at in [hours_mins(0, 1), hours_mins(1, 30), hours_mins(2, 50)] {
        h.clock.set(at);
        let r = h.sched.tick().await;
        assert_eq!(h.fake.request_count(), HORIZON_WINDOWS, "at {at}");
        assert_eq!((r.windows_due, r.fetched, r.from_cache), (0, 0, 0));
    }
}

#[tokio::test]
async fn near_windows_refresh_every_3h_far_windows_daily() {
    let h = harness(150).await;
    h.sched.tick().await;
    let mut seen = h.fake.request_count();
    let step = |at: DateTime<Utc>| {
        h.clock.set(at);
        let sched = &h.sched;
        async move { sched.tick().await }
    };

    // 2 h 50 min later: nothing is due yet.
    step(hours_mins(2, 50)).await;
    assert_eq!(requested_after(&h.fake, seen), Vec::<i64>::new());

    // 3 h 05 min later (23:05): only the windows starting within the next
    // 24 hours (2026-10-31 18:00 to 2026-11-01 18:00) are refreshed.
    step(hours_mins(3, 5)).await;
    assert_eq!(requested_after(&h.fake, seen), grid(first_window(), 5));
    seen = h.fake.request_count();

    // 23 h 50 min after the first pass (2026-11-01 19:50): the near windows
    // are due again, the far windows are not (fetched under 24 h ago); the
    // four windows that just entered the horizon (2026-11-08) are new.
    let nov1_18 = Utc.with_ymd_and_hms(2026, 11, 1, 18, 0, 0).unwrap();
    let nov8_00 = Utc.with_ymd_and_hms(2026, 11, 8, 0, 0, 0).unwrap();
    step(hours_mins(23, 50)).await;
    let mut expect = grid(nov1_18, 5);
    expect.extend(grid(nov8_00, 4));
    assert_eq!(requested_after(&h.fake, seen), expect);
    seen = h.fake.request_count();

    // 24 h 05 min: now the far windows (2026-11-03 to 2026-11-07) are a day
    // old and get their daily refresh; nothing else is due.
    step(hours_mins(24, 5)).await;
    let nov3_00 = Utc.with_ymd_and_hms(2026, 11, 3, 0, 0, 0).unwrap();
    assert_eq!(requested_after(&h.fake, seen), grid(nov3_00, 20));
}

#[tokio::test]
async fn tick_stops_cleanly_when_budget_exhausted_and_master_still_served() {
    let h = harness(10).await;
    let r = h.sched.tick().await;

    // The budget went to the 10 nearest windows; the rest were not sent.
    assert_eq!(requested(&h.fake), grid(first_window(), 10));
    assert_eq!(r.fetched, 10);
    assert_eq!(r.skipped.budget_exhausted, HORIZON_WINDOWS - 10);
    assert_eq!(r.failed, 0);
    // A budget stop is not the provider's fault.
    let rel = h.store.provider_reliability_load_all().await.unwrap();
    assert_eq!((rel[0].success, rel[0].total), (10, 10));

    // Later ticks the same day send nothing and change nothing, and the
    // master is still there to serve.
    let before = dump(&h.store).await;
    for m in [1, 2, 30, 120] {
        h.clock.set(hours_mins(0, m));
        let r = h.sched.tick().await;
        assert_eq!(h.fake.request_count(), 10);
        assert_no_slot_changes(&r);
        assert_eq!(r.skipped.budget_exhausted, HORIZON_WINDOWS - 10);
    }
    assert!(dump(&h.store).await == before, "master changed");
    let wcco = ChannelId("WCCODT.us".into());
    let tonight = h
        .store
        .slots(wcco, now0(), now0() + TimeDelta::hours(6))
        .await
        .unwrap();
    assert!(!tonight.is_empty(), "tonight's guide is still served");
}

#[tokio::test]
async fn provider_outage_leaves_master_unchanged() {
    let h = harness(150).await;
    h.sched.tick().await;
    let before = dump(&h.store).await;
    assert!(slot_total(&before) > 500);

    // From now on every request gets HTTP 500. Tick every 10 minutes for a
    // day and a half: every window becomes due, nothing can be fetched.
    h.fake.set_default(Behavior::Status(500));
    let mut failed = 0;
    for i in 1..=216 {
        h.clock.set(hours_mins(0, 10 * i));
        let r = h.sched.tick().await;
        assert_no_slot_changes(&r);
        assert_eq!(r.fetched + r.from_cache, 0);
        failed += r.failed;
    }
    assert!(failed > 0);
    assert!(
        dump(&h.store).await == before,
        "an outage changed the master"
    );

    // The fetch layer's backoff and circuit breaker kept the outage cheap:
    // nowhere near one request per due window per tick.
    let outage_requests = h.fake.request_count() - HORIZON_WINDOWS;
    assert!(outage_requests < 100, "{outage_requests} requests");
    assert_ne!(
        h.fetch.provider_state(&gn()).await.unwrap().circuit,
        CircuitState::Closed
    );
    // The failures are counted against the provider's reliability.
    let rel = h.store.provider_reliability_load_all().await.unwrap();
    assert_eq!(rel[0].success, 29);
    assert_eq!(rel[0].total as usize, 29 + outage_requests);
}

#[tokio::test]
async fn partial_failure_merges_successful_windows_only() {
    // Windows 1-3 answer, window 4 gets HTTP 500; the fetch layer then backs
    // off, so the rest of this tick sends nothing more.
    let h = harness(150).await;
    h.fake.script([
        Behavior::ok_shifted(),
        Behavior::ok_shifted(),
        Behavior::ok_shifted(),
        Behavior::Status(500),
    ]);
    let r = h.sched.tick().await;
    assert_eq!(requested(&h.fake), grid(first_window(), 4));
    assert_eq!((r.fetched, r.failed), (3, 1));
    assert_eq!(r.skipped.rate_limited, HORIZON_WINDOWS - 4);

    // The master is exactly what fetching only windows 1-3 gives: a control
    // run with a budget of 3 (same clock, same data) must be identical.
    let control = harness(3).await;
    control.sched.tick().await;
    assert_eq!(control.fake.request_count(), 3);
    assert!(
        dump(&h.store).await == dump(&control.store).await,
        "the failed window changed the master"
    );

    // Next tick (backoff over): the failed window and the rest are fetched;
    // the three good windows are not fetched again.
    h.clock.set(hours_mins(0, 1));
    let r = h.sched.tick().await;
    assert_eq!(
        requested_after(&h.fake, 4),
        grid(first_window() + TimeDelta::hours(18), HORIZON_WINDOWS - 3)
    );
    assert_eq!(r.fetched, HORIZON_WINDOWS - 3);
}

#[tokio::test]
async fn bad_window_400_in_middle_does_not_block_other_windows() {
    let h = harness(150).await;
    h.fake.script([
        Behavior::ok_shifted(),
        Behavior::ok_shifted(),
        Behavior::Status(400),
    ]);
    let r = h.sched.tick().await;

    // All 29 windows were tried once; only the third failed.
    assert_eq!(requested(&h.fake), grid(first_window(), HORIZON_WINDOWS));
    assert_eq!(r.fetched, HORIZON_WINDOWS - 1);
    assert_eq!(r.skipped.bad_window, 1);
    assert!(r.lineup_halted_bad_windows.is_empty());
    let w3 = first_window() + TimeDelta::hours(12);
    let st = h
        .store
        .window_state_get(&gn(), "ota", w3)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(st.last_success, None);
    assert_eq!(st.last_failure, Some(WindowFailure::BadWindow));
    // Later windows were merged: WCCO has programmes on 2026-11-05.
    let nov5 = Utc.with_ymd_and_hms(2026, 11, 5, 0, 0, 0).unwrap();
    let wcco = ChannelId("WCCODT.us".into());
    let later = h
        .store
        .slots(wcco, nov5, nov5 + TimeDelta::hours(6))
        .await
        .unwrap();
    assert!(!later.is_empty());

    // A minute later the bad window is remembered: no request.
    h.clock.set(hours_mins(0, 1));
    let r = h.sched.tick().await;
    assert_eq!(h.fake.request_count(), HORIZON_WINDOWS);
    assert_eq!(r.skipped.known_bad_window, 1);

    // After the fetch layer's 6-hour memory it is tried again.
    h.clock.set(hours_mins(6, 5));
    h.sched.tick().await;
    assert!(requested_after(&h.fake, HORIZON_WINDOWS).contains(&w3.timestamp()));
}

#[tokio::test]
async fn fixture_ingest_produces_slots_for_all_89_channels_matched_to_tuner_fixture() {
    let h = harness(150).await;
    h.sched.tick().await;

    // The provider's own channel list was persisted for matching.
    let provider_channels = h.store.provider_channels_list(&gn(), "ota").await.unwrap();
    assert_eq!(provider_channels.len(), 89);

    // Every one of the 89 channels has programmes in the master.
    let d = dump(&h.store).await;
    assert_eq!(d.len(), 89);
    for (id, slots) in &d {
        assert!(!slots.is_empty(), "{} has no programmes", id.0);
    }

    // Matching the real tuner lineup against the persisted provider channels
    // gives the pinned counts (tests/channels.rs, no overrides): 82 matched,
    // and every matched channel has guide data under its canonical id.
    let tuner = parse_lineup(TUNER_LINEUP).unwrap();
    let m = match_channels(
        &tuner,
        &provider_channels,
        &Overrides::from_toml("").unwrap(),
    );
    assert_eq!(
        (m.matched.len(), m.ambiguous.len(), m.unmatched.len()),
        (82, 1, 24)
    );
    for (t, c) in &m.matched {
        let has = d.iter().any(|(id, s)| *id == c.id && !s.is_empty());
        assert!(has, "tuner {} -> {} has no guide data", t.number, c.id.0);
    }
}

// ---------------------------------------------------------------------------
// Controller rulings
// ---------------------------------------------------------------------------

#[tokio::test]
async fn restart_does_not_refetch_fresh_windows() {
    let h = harness(150).await;
    h.sched.tick().await;
    assert_eq!(h.fake.request_count(), HORIZON_WINDOWS);
    drop(h.sched);

    // "Restart" 90 minutes later: a brand-new fetch layer and scheduler on
    // the same database. The raw cache is too old to help (1 h), so only the
    // persisted window state can prevent a full re-fetch.
    h.clock.set(hours_mins(1, 30));
    let (_fetch, sched) = scheduler_for(
        h.store.clone(),
        Arc::new(h.fake.gracenote()),
        h.clock.clone(),
        150,
    );
    let r = sched.tick().await;
    assert_eq!(h.fake.request_count(), HORIZON_WINDOWS);
    assert_eq!(r.windows_due, 0);

    // And once intervals elapse it does fetch (the near windows only).
    h.clock.set(hours_mins(3, 5));
    sched.tick().await;
    assert_eq!(
        requested_after(&h.fake, HORIZON_WINDOWS),
        grid(first_window(), 5)
    );
}

#[tokio::test]
async fn three_consecutive_bad_windows_put_the_lineup_on_a_six_hour_hold() {
    let h = harness(150).await;
    h.fake.script([
        Behavior::ok_shifted(),
        Behavior::Status(400),
        Behavior::Status(400),
        Behavior::Status(400),
    ]);
    let r = h.sched.tick().await;
    // Three 400s in a row: the sweep of this lineup stops and it goes on hold.
    assert_eq!(requested(&h.fake), grid(first_window(), 4));
    assert_eq!(r.lineup_halted_bad_windows, vec![(gn(), "ota".to_string())]);
    assert_eq!(r.skipped.bad_window, 3);
    assert_eq!(r.skipped.halted, HORIZON_WINDOWS - 4);

    // Next tick: the lineup is in its 6-hour hold, so NOTHING is asked for
    // (before the hold, the next tick swept on from window 5).
    h.clock.set(hours_mins(0, 1));
    let r = h.sched.tick().await;
    assert_eq!(h.fake.request_count(), 4, "{:?}", requested(&h.fake));
    assert!(r.lineup_halted_bad_windows.is_empty());
    assert_eq!(r.skipped.halted, r.windows_due);

    // Once the hold has ended the lineup is fetched again.
    h.clock.set(hours_mins(6, 1));
    let r = h.sched.tick().await;
    assert!(h.fake.request_count() > 4);
    assert!(r.fetched > 0, "{r:?}");
    assert_eq!(r.skipped.halted, 0);

    // Two in a row, then a good window, then two more: the streak resets,
    // nothing is halted.
    let h = harness(150).await;
    h.fake.script([
        Behavior::ok_shifted(),
        Behavior::Status(400),
        Behavior::Status(400),
        Behavior::ok_shifted(),
        Behavior::Status(400),
        Behavior::Status(400),
    ]);
    let r = h.sched.tick().await;
    assert_eq!(h.fake.request_count(), HORIZON_WINDOWS);
    assert!(r.lineup_halted_bad_windows.is_empty());
    assert_eq!(r.skipped.bad_window, 4);
}

#[tokio::test]
async fn all_400_provider_costs_at_most_three_requests_per_hold_period() {
    let h = harness(150).await;
    h.fake.set_default(Behavior::Status(400));
    // A tick every minute for 12 hours.
    for minute in 0..12 * 60 {
        h.clock.set(hours_mins(0, minute));
        h.sched.tick().await;
    }
    // Counted on the fake SERVER: what really reached the provider. Three
    // to set the first hold, three more when it ends after 6 hours.
    let n = h.fake.request_count();
    assert!(n <= 6, "{n} requests: {:?}", requested(&h.fake));
    assert!(n >= 3, "the provider was never asked ({n})");
}

#[tokio::test]
async fn hold_is_per_lineup() {
    let h = harness(150).await;
    // Lineup "ota" (USA-OTA55401) answers 400 to everything; "cable" is fine.
    h.fake.set_for_lineup("USA-OTA55401", Behavior::Status(400));
    let cable = LineupCfg {
        name: "cable".into(),
        lineup_id: "USA-CABLE1".into(),
        ..lineup()
    };
    let deps = SchedulerDeps {
        store: h.store.clone(),
        fetch: h.fetch.clone(),
        providers: vec![ProviderRef {
            id: gn(),
            lineups: vec![lineup(), cable],
        }],
        horizon_days: 7,
        weights: Weights::default(),
        tick_interval: Duration::from_secs(60),
        feed_generation: None,
    };
    let sched = Scheduler::new(deps, h.clock.clone());
    let per = |lineup: &str| {
        h.fake
            .requests()
            .iter()
            .filter(|r| r.query["lineupId"] == lineup)
            .count()
    };

    let r = sched.tick().await;
    assert_eq!(r.lineup_halted_bad_windows, vec![(gn(), "ota".to_string())]);
    assert_eq!(per("USA-OTA55401"), 3);
    assert_eq!(per("USA-CABLE1"), HORIZON_WINDOWS);

    // Three hours later the near cable windows are refreshed as usual, while
    // the held lineup is still left alone.
    h.clock.set(hours_mins(3, 5));
    let r = sched.tick().await;
    assert_eq!(per("USA-OTA55401"), 3);
    assert_eq!(per("USA-CABLE1"), HORIZON_WINDOWS + 5);
    assert!(r.fetched >= 5, "{r:?}");
}

#[tokio::test]
async fn parse_failure_changes_nothing() {
    let h = harness(150).await;
    h.sched.tick().await;
    let before = dump(&h.store).await;
    let w1_before = h
        .store
        .window_state_get(&gn(), "ota", first_window())
        .await
        .unwrap()
        .unwrap();

    // The near windows are due; the first answer is cut-off JSON.
    h.clock.set(hours_mins(3, 5));
    h.fake.set_default(Behavior::Truncated);
    let r = h.sched.tick().await;
    assert_eq!(h.fake.request_count(), HORIZON_WINDOWS + 1);
    assert_eq!((r.fetched, r.failed, r.parse_failed), (0, 1, 1));
    assert_no_slot_changes(&r);

    // After the backoff: valid JSON of the wrong shape.
    h.clock.set(hours_mins(3, 10));
    h.fake.set_default(Behavior::Ok(bytes::Bytes::from_static(
        b"{\"channels\":\"nope\"}",
    )));
    let r = h.sched.tick().await;
    assert_eq!(h.fake.request_count(), HORIZON_WINDOWS + 2);
    assert_eq!(r.parse_failed, 1);
    assert_no_slot_changes(&r);

    assert!(
        dump(&h.store).await == before,
        "a parse failure changed the master"
    );
    let w1 = h
        .store
        .window_state_get(&gn(), "ota", first_window())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(w1.last_success, w1_before.last_success);
    assert!(
        matches!(w1.last_failure, Some(WindowFailure::ParseFailed(_))),
        "{w1:?}"
    );
    let rel = h.store.provider_reliability_load_all().await.unwrap();
    assert_eq!((rel[0].success, rel[0].total), (29, 31));
}

#[tokio::test]
async fn budget_never_exceeded_across_many_ticks() {
    let budget = 30;
    let h = harness(budget).await;
    // Some failures on the way, which cost requests (retries).
    let mut script = vec![Behavior::ok_shifted(); 5];
    script.push(Behavior::Status(500));
    script.extend(vec![Behavior::ok_shifted(); 6]);
    script.push(Behavior::Status(503));
    h.fake.script(script);

    for i in 0..200 {
        h.clock.set(hours_mins(0, i));
        h.sched.tick().await;
        assert!(h.fake.request_count() <= budget as usize, "tick {i}");
    }
    // The budget really was the limit (29 windows + 2 retries + the 3-hour
    // refresh want more than 30), and it held exactly.
    assert_eq!(h.fake.request_count(), budget as usize);
    assert_eq!(
        h.fetch.provider_state(&gn()).await.unwrap().budget_used,
        budget
    );
}

#[tokio::test]
async fn redirects_are_not_followed() {
    let fake = FakeProvider::start().await;
    let again = format!("{}api/grid?time=1&again=1", fake.base_url());
    fake.set_default(Behavior::Redirect(again));

    // Control: a default reqwest client WOULD follow the redirect, sending
    // more requests than were charged to the budget.
    patina::tls::install_crypto_provider();
    let naive = GracenoteProvider::new(reqwest::Client::new(), fake.base_url());
    let _ = naive.fetch_window(&lineup(), first_window()).await;
    let followed = fake.request_count();
    assert!(followed > 1, "control: default client followed {followed}");

    // The production client: exactly one request per budget charge.
    let store = connect("sqlite::memory:").await.unwrap();
    let client = http_client(Duration::from_secs(5)).unwrap();
    let provider = Arc::new(GracenoteProvider::new(client, fake.base_url()));
    let clock = Arc::new(FakeClock::new(now0()));
    let (fetch, sched) = scheduler_for(store.clone(), provider, clock, 150);
    let r = sched.tick().await;
    assert_eq!(fake.request_count() - followed, 1);
    assert_eq!(fetch.provider_state(&gn()).await.unwrap().budget_used, 1);
    // A 302 is not a usable answer: one failure, then the circuit is open.
    assert_eq!(r.failed, 1);
    assert_eq!(r.skipped.circuit_open, HORIZON_WINDOWS - 1);
    assert_eq!(slot_total(&dump(&store).await), 0);
}

#[tokio::test]
async fn hindsight_updates_ema_when_slot_refetched_closer_to_air() {
    let h = harness(150).await;
    h.sched.tick().await;
    // A first fetch has nothing to compare with.
    assert!(
        h.store
            .provider_accuracy_load_all()
            .await
            .unwrap()
            .is_empty()
    );

    // 3 h 05 min later the near windows are re-fetched, and the provider has
    // moved every programme 5 minutes later (start and end: 600 s in total).
    h.clock.set(hours_mins(3, 5));
    h.fake.set_default(Behavior::OkShiftedBy(fixture(), 300));
    h.sched.tick().await;
    assert_eq!(h.fake.request_count(), HORIZON_WINDOWS + 5);

    let rows = h.store.provider_accuracy_load_all().await.unwrap();
    assert!(rows.len() > 50, "{} channels sampled", rows.len());
    // hindsight_sample for a 600 s shift, same title: 1 - (600 - 60) / 1740.
    // No sample can be better than that. Some are worse: the test data moves
    // one fixture to every window, so where two neighbouring windows overlap
    // they disagree, and a slot may hold the neighbour's (different)
    // programme; comparing with that gives a low or zero sample.
    let expected = 1.0 - 540.0 / 1740.0;
    for row in &rows {
        assert_eq!(row.provider, gn());
        assert!(row.samples >= 1);
        assert!((0.0..=expected + 1e-9).contains(&row.ema), "{row:?}");
        assert_eq!(row.updated_at.date_naive(), now0().date_naive());
    }
    assert!(
        rows.iter().any(|r| (r.ema - expected).abs() < 1e-9),
        "some channel saw only the plain 5-minute re-timing"
    );
}

#[tokio::test]
async fn gappy_response_records_reliability_failure_but_still_merges() {
    // Keep every other programme: lots of holes in each channel's timeline.
    let mut grid_json: serde_json::Value = serde_json::from_slice(&fixture()).unwrap();
    for ch in grid_json["channels"].as_array_mut().unwrap() {
        let events = ch["events"].as_array_mut().unwrap();
        let kept: Vec<_> = events.iter().step_by(2).cloned().collect();
        *events = kept;
    }
    let gappy = bytes::Bytes::from(serde_json::to_vec(&grid_json).unwrap());

    let h = harness(1).await;
    h.fake.set_default(Behavior::OkShifted(gappy));
    let r = h.sched.tick().await;
    assert_eq!(h.fake.request_count(), 1);
    assert!(r.last_integrity.unwrap().gap_fraction > 0.2, "{r:?}");
    let rel = h.store.provider_reliability_load_all().await.unwrap();
    assert_eq!((rel[0].success, rel[0].total), (0, 1));
    // Holes are not contradictions: what it did say is still merged.
    assert!(r.slots_inserted > 0);
}

#[tokio::test]
async fn run_stops_when_cancelled_and_never_ticks_after() {
    // Cancelled before it starts: not a single tick.
    let h = harness(150).await;
    let token = CancellationToken::new();
    token.cancel();
    h.sched.run(token).await;
    assert_eq!(h.fake.request_count(), 0);

    // Running: it ticks (the fake clock makes its 60 s sleeps instant), and
    // stops promptly once cancelled.
    let h = harness(150).await;
    let token = CancellationToken::new();
    let task = tokio::spawn(h.sched.run(token.clone()));
    // Real-time safety bound only (the tick itself takes a few seconds in a
    // debug build when all tests run at once); the test never sleeps that long.
    let waited = tokio::time::timeout(Duration::from_secs(120), async {
        while h.fake.request_count() < HORIZON_WINDOWS {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(waited.is_ok(), "run never completed a first tick");
    token.cancel();
    tokio::time::timeout(Duration::from_secs(120), task)
        .await
        .expect("run stops after cancel")
        .unwrap();
    let after = h.fake.request_count();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(h.fake.request_count(), after, "no request after stopping");
}

#[tokio::test]
async fn skipped_outcomes_leave_the_bad_window_streak_unchanged() {
    // First pass: window 2 answers 400 and is remembered as bad for 6 h.
    let h = harness(150).await;
    h.fake
        .script([Behavior::ok_shifted(), Behavior::Status(400)]);
    h.sched.tick().await;
    assert_eq!(h.fake.request_count(), HORIZON_WINDOWS);

    // 3 h 05 later the five near windows are due. Window 1 answers 400,
    // window 2 is skipped as known-bad (no request: neither a new 400 nor a
    // good answer, so the streak stays at 1), windows 3 and 4 answer 400:
    // that is the third real 400 in a row, so window 5 is not asked.
    h.clock.set(hours_mins(3, 5));
    h.fake.script([
        Behavior::Status(400),
        Behavior::Status(400),
        Behavior::Status(400),
    ]);
    let r = h.sched.tick().await;
    let w = |i: i64| first_window().timestamp() + i * 6 * 3600;
    assert_eq!(
        requested_after(&h.fake, HORIZON_WINDOWS),
        vec![w(0), w(2), w(3)]
    );
    assert_eq!(r.skipped.known_bad_window, 1);
    assert_eq!(r.skipped.bad_window, 3);
    assert_eq!(r.skipped.halted, 1);
    assert_eq!(r.lineup_halted_bad_windows, vec![(gn(), "ota".to_string())]);
}

/// The fixture plus one extra channel, ZZFAIL (a copy of the first
/// channel's programmes under another call sign).
fn fixture_with_extra_channel() -> bytes::Bytes {
    let mut grid: serde_json::Value = serde_json::from_slice(&fixture()).unwrap();
    let channels = grid["channels"].as_array_mut().unwrap();
    let mut extra = channels[0].clone();
    extra["callSign"] = "ZZFAIL".into();
    extra["channelNo"] = "99.1".into();
    extra["channelId"] = "999999".into();
    channels.push(extra);
    bytes::Bytes::from(serde_json::to_vec(&grid).unwrap())
}

#[tokio::test]
async fn failing_merge_is_retried_at_most_every_30_minutes() {
    // Window 2 (2026-11-01 00:00) also lists channel ZZFAIL; once the store
    // refuses every merge for ZZFAIL, that window keeps failing to store.
    let failing = Arc::new(FailingStore::new(connect("sqlite::memory:").await.unwrap()));
    let h = harness_on(failing.clone(), 150).await;
    let w_bad = first_window() + TimeDelta::hours(6);
    h.fake.set_for_window(
        w_bad.timestamp(),
        Behavior::OkShifted(fixture_with_extra_channel()),
    );
    let zz = ChannelId("ZZFAIL.us".into());

    // First pass: everything is stored, ZZFAIL included.
    h.sched.tick().await;
    assert_eq!(h.fake.request_count(), HORIZON_WINDOWS);
    *failing.fail_apply_channel.lock().unwrap() = Some(zz.clone());
    let rel_before = h.store.provider_reliability_load_all().await.unwrap()[0].clone();

    // Tick every minute for 3 hours, from 3 h 05 (near windows due) to
    // 6 h 04 (just before they are due again).
    for m in 185..365 {
        h.clock.set(hours_mins(0, m));
        h.sched.tick().await;
    }

    // Requests, counted on the fake server. Healthy windows only as the
    // normal tiers say: the four other near windows at 3 h 05, then at
    // 4 h 00 (00:00 UTC) 2026-11-02 00:00 becomes near (last fetched in the
    // first pass) and 2026-11-08 00:00 enters the horizon. The failing
    // window only when the raw cache (1 h) had expired: at most once per
    // hour.
    let after = requested_after(&h.fake, HORIZON_WINDOWS);
    let healthy: Vec<i64> = after
        .iter()
        .copied()
        .filter(|&t| t != w_bad.timestamp())
        .collect();
    let mut expect_healthy = grid(first_window(), 5);
    expect_healthy.retain(|&t| t != w_bad.timestamp());
    expect_healthy.push(
        Utc.with_ymd_and_hms(2026, 11, 2, 0, 0, 0)
            .unwrap()
            .timestamp(),
    );
    expect_healthy.push(
        Utc.with_ymd_and_hms(2026, 11, 8, 0, 0, 0)
            .unwrap()
            .timestamp(),
    );
    assert_eq!(healthy, expect_healthy, "healthy windows unaffected");
    let bad_requests = after.len() - healthy.len();
    assert!((1..=3).contains(&bad_requests), "{bad_requests} requests");

    // Re-ingested at most every 30 minutes over 3 hours (every ingest of
    // that window tries, and fails, to store ZZFAIL once).
    let attempts = failing
        .apply_failures
        .load(std::sync::atomic::Ordering::SeqCst);
    assert!((5..=7).contains(&attempts), "{attempts} attempts");
    let st = h
        .store
        .window_state_get(&gn(), "ota", w_bad)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(st.last_failure, Some(WindowFailure::MergeFailed));

    // Reliability counts real responses only, never cache replays.
    let rel = h.store.provider_reliability_load_all().await.unwrap()[0].clone();
    assert_eq!(rel.success - rel_before.success, after.len() as u32);
    assert_eq!(rel.total - rel_before.total, after.len() as u32);

    // ZZFAIL's merges never stored, so its hindsight samples never counted.
    let acc = h.store.provider_accuracy_load_all().await.unwrap();
    let zz_rows: Vec<_> = acc.iter().filter(|r| r.channel == zz).collect();
    assert!(zz_rows.is_empty(), "{zz_rows:?}");
}

#[tokio::test]
async fn unreadable_window_state_is_not_overwritten() {
    let failing = Arc::new(FailingStore::new(connect("sqlite::memory:").await.unwrap()));
    let h = harness_on(failing.clone(), 150).await;
    h.sched.tick().await;
    let before = h
        .store
        .window_state_get(&gn(), "ota", first_window())
        .await
        .unwrap();
    assert!(before.as_ref().unwrap().last_success.is_some());

    // A failed request while the window's state cannot be read: nothing is
    // written (not a row claiming the window never succeeded).
    h.fake.set_default(Behavior::Status(500));
    failing
        .fail_window_state_get
        .store(true, std::sync::atomic::Ordering::SeqCst);
    h.clock.set(hours_mins(3, 5));
    let r = h.sched.tick().await;
    assert_eq!(r.failed, 1);
    failing
        .fail_window_state_get
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let after = h
        .store
        .window_state_get(&gn(), "ota", first_window())
        .await
        .unwrap();
    assert_eq!(after, before);
}

#[tokio::test]
async fn window_state_in_the_future_is_treated_as_now() {
    let h = harness(150).await;
    h.sched.tick().await;
    // Window 2 (2026-11-01 00:00): still inside the horizon at 6 h 10.
    let w2 = first_window() + TimeDelta::hours(6);
    // Its last success lies 10 days ahead (the clock was
    // wrong, or jumped back).
    let mut st = h
        .store
        .window_state_get(&gn(), "ota", w2)
        .await
        .unwrap()
        .unwrap();
    st.last_success = Some(now0() + TimeDelta::days(10));
    st.last_attempt = now0() + TimeDelta::days(10);
    h.store.window_state_put(&gn(), "ota", &st).await.unwrap();

    // At 3 h 05 it counts as fetched just now (and is stored that way).
    h.clock.set(hours_mins(3, 5));
    h.sched.tick().await;
    let first = w2.timestamp();
    assert!(!requested_after(&h.fake, HORIZON_WINDOWS).contains(&first));
    let st = h
        .store
        .window_state_get(&gn(), "ota", w2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(st.last_success, Some(hours_mins(3, 5)));

    // One normal interval later it is due again instead of frozen.
    let seen = h.fake.request_count();
    h.clock.set(hours_mins(6, 10));
    h.sched.tick().await;
    assert!(requested_after(&h.fake, seen).contains(&first));
}

// ---------------------------------------------------------------------------
// The whole pipeline on both backends
// ---------------------------------------------------------------------------

/// First pass, quiet second pass, restart, outage: the core promises in one
/// scenario, so it can run unchanged on SQLite and on Postgres.
async fn full_ingest_scenario(store: Arc<dyn Store>) {
    let h = harness_on(store, 150).await;
    let r = h.sched.tick().await;
    assert_eq!(requested(&h.fake), grid(first_window(), HORIZON_WINDOWS));
    assert_eq!(r.channels_seen, 89);
    let before = dump(&h.store).await;
    assert_eq!(before.len(), 89);
    assert!(slot_total(&before) > 500);

    h.clock.set(hours_mins(0, 1));
    h.sched.tick().await;
    assert_eq!(h.fake.request_count(), HORIZON_WINDOWS);

    h.clock.set(hours_mins(1, 30));
    let (_f, restarted) = scheduler_for(
        h.store.clone(),
        Arc::new(h.fake.gracenote()),
        h.clock.clone(),
        150,
    );
    restarted.tick().await;
    assert_eq!(h.fake.request_count(), HORIZON_WINDOWS);

    h.fake.set_default(Behavior::Status(500));
    for i in 0..20 {
        h.clock.set(hours_mins(3, 5 + 10 * i));
        let r = restarted.tick().await;
        assert_no_slot_changes(&r);
    }
    assert!(h.fake.request_count() > HORIZON_WINDOWS);
    assert!(dump(&h.store).await == before);
}

#[tokio::test]
async fn full_ingest_on_sqlite_file() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("e2e.db").display());
    full_ingest_scenario(connect(&url).await.unwrap()).await;
}

#[tokio::test]
async fn full_ingest_on_postgres() {
    common::pg::with_fresh_schema(|url| async move {
        full_ingest_scenario(connect(&url).await.unwrap()).await;
    })
    .await;
}

// ---------------------------------------------------------------------------
// Window ledger: the two deliberate behaviour fixes
// ---------------------------------------------------------------------------

/// Every window state row of the horizon (and a little beyond).
async fn all_window_states(store: &Arc<dyn Store>) -> Vec<patina::store::WindowState> {
    store
        .window_state_range(
            &gn(),
            "ota",
            first_window(),
            first_window() + TimeDelta::days(9),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn store_outage_does_not_count_against_provider_reliability() {
    let failing = Arc::new(FailingStore::new(connect("sqlite::memory:").await.unwrap()));
    let h = harness_on(failing.clone(), 150).await;
    h.sched.tick().await;
    assert_eq!(h.fake.request_count(), HORIZON_WINDOWS);
    let rel_before = h.store.provider_reliability_load_all().await.unwrap();
    let windows_before = all_window_states(&h.store).await;
    let fetch_before = h.fetch.provider_state(&gn()).await.unwrap();

    // 3 h 05 later the five near windows are due, and for the whole tick the
    // database fails at one of the fetch layer's gates (after the budget and
    // circuit checks, before the request).
    h.clock.set(hours_mins(3, 5));
    failing
        .fail_bad_window_check
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let r = h.sched.tick().await;
    failing
        .fail_bad_window_check
        .store(false, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(r.windows_due, 5, "{r:?}");
    // Fail closed: not one request reached the provider.
    assert_eq!(h.fake.request_count(), HORIZON_WINDOWS);
    // A database hiccup is not the provider's fault: its reliability, the
    // windows' state, the budget and the backoff/circuit are all untouched.
    assert_eq!(r.failed, 0, "{r:?}");
    assert_eq!(r.skipped.store_unavailable, 5, "{r:?}");
    assert_eq!(
        h.store.provider_reliability_load_all().await.unwrap(),
        rel_before
    );
    assert_eq!(all_window_states(&h.store).await, windows_before);
    assert_eq!(h.fetch.provider_state(&gn()).await.unwrap(), fetch_before);

    // With the database back, the next tick fetches those windows normally.
    h.clock.set(hours_mins(3, 6));
    let r = h.sched.tick().await;
    assert_eq!(
        requested_after(&h.fake, HORIZON_WINDOWS),
        grid(first_window(), 5)
    );
    assert_eq!(r.fetched, 5);
}

#[tokio::test]
async fn real_429_counts_one_reliability_failure() {
    let h = harness(150).await;
    // The very first request gets HTTP 429 with `Retry-After: 600`.
    h.fake.script([Behavior::RetryAfter(600)]);
    let r = h.sched.tick().await;
    assert_eq!(h.fake.request_count(), 1);
    // The 429 itself and the 28 windows held back by the backoff that
    // followed it are all "rate limited" in the tick report.
    assert_eq!(r.skipped.rate_limited, HORIZON_WINDOWS, "{r:?}");
    assert_eq!(r.failed, 0);
    // A real 429 is a real (unwelcome) answer: one reliability failure.
    // The 28 windows that were only held back count nothing.
    let rel = h.store.provider_reliability_load_all().await.unwrap();
    assert_eq!((rel[0].success, rel[0].total), (0, 1));
    // No window failure is written for a 429 (as before).
    assert_eq!(all_window_states(&h.store).await, Vec::new());

    // While Retry-After holds, ticks send nothing and count nothing more.
    for m in 1..=9 {
        h.clock.set(hours_mins(0, m));
        h.sched.tick().await;
    }
    assert_eq!(h.fake.request_count(), 1);
    let rel = h.store.provider_reliability_load_all().await.unwrap();
    assert_eq!((rel[0].success, rel[0].total), (0, 1));

    // After it, the sweep proceeds normally.
    h.clock.set(hours_mins(0, 11));
    let r = h.sched.tick().await;
    assert_eq!(r.fetched, HORIZON_WINDOWS);
    let rel = h.store.provider_reliability_load_all().await.unwrap();
    assert_eq!((rel[0].success, rel[0].total), (29, 30));
}
