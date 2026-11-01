//! Tests of the master-guide ingest module (`patina::ingest`), driven
//! directly against a real SQLite store: no HTTP, no scheduler, no fake
//! provider. The ingest interface is the test surface: one provider's clean
//! response for one channel goes in, an outcome comes out, and the store
//! shows what happened.
//!
//! Times are minutes after 2026-11-01 00:00 UTC (`t`); `now` is minute 0
//! unless stated, so programmes at minute 600 are in the future.

mod common;
mod support;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use chrono::TimeDelta;
use common::merge_util::{StubScorer, ch, obs, pid, t};
use patina::channels::TunerChannel;
use patina::channels::matching::{Overrides, match_channels};
use patina::config::Weights;
use patina::ingest::{ChannelResponse, HINDSIGHT_MIN_AGE, Ingest, Ingested, store_channels};
use patina::merge::{MergePlan, NewSlot, ObservationWrite, Scorer, SlotFields, SlotRef};
use patina::merge::{MergeStats, SlotWithObservations};
use patina::model::{
    CanonicalChannel, ChannelId, MasterSlot, Observation, ProviderId, SlotId, canonical_channel,
};
use patina::provider::ProviderChannel;
use patina::score::{AccuracySnapshot, WeightedScorer, hindsight_sample};
use patina::store::{AccuracyRow, ReliabilityRow, Store, connect};
use proptest::prelude::*;
use proptest::test_runner::{Config, RngSeed, TestCaseError, TestRunner};
use support::failing_store::FailingStore;

const NOW: i64 = 0;

fn scorer() -> StubScorer {
    StubScorer::new(&[("a", 0.9), ("b", 0.5)])
}

/// The provider channel behind `ch()` (`WCCO-DT` -> `WCCODT.us`).
fn wcco() -> ProviderChannel {
    ProviderChannel {
        provider_channel_id: "10001".into(),
        number: "4.1".into(),
        call_sign: "WCCO-DT".into(),
    }
}

/// An empty SQLite store that knows the channel `ch()`.
async fn store() -> Arc<dyn Store> {
    let store = connect("sqlite::memory:").await.unwrap();
    store_channels(store.as_ref(), &pid("a"), "ota", &[wcco()], t(NOW))
        .await
        .unwrap();
    store
}

/// Every slot of `ch()` with its observations.
async fn dump(store: &dyn Store) -> Vec<SlotWithObservations> {
    store
        .slots_with_observations(&ch(), t(-20_000), t(20_000))
        .await
        .unwrap()
}

/// How many score-history rows are stored (deletes them, as a counter).
async fn take_history(store: &dyn Store) -> u64 {
    store
        .score_history_delete_older_than(t(1_000_000), 1_000_000)
        .await
        .unwrap()
}

/// Ingest one response of `provider` for `ch()` covering minutes
/// [from, to), as the scheduler would.
async fn ingest(
    store: &dyn Store,
    scorer: &(dyn Scorer + Sync),
    provider: &str,
    (from, to): (i64, i64),
    observations: &[Observation],
    now: i64,
) -> anyhow::Result<Ingested> {
    let provider = pid(provider);
    let channel = ch();
    Ingest {
        store,
        scorer,
        feed_generation: None,
    }
    .channel(
        ChannelResponse {
            provider: &provider,
            channel: &channel,
            window: t(from)..t(to),
            observations,
        },
        t(now),
    )
    .await
}

fn rich(mut o: Observation) -> Observation {
    o.description = Some(format!("About {}", o.title));
    o.image_url = Some(format!("https://img/{}.jpg", o.title));
    o.genres = vec!["Drama".into()];
    o.season = Some(1);
    o.episode = Some(2);
    o
}

fn slot_at(slots: &[SlotWithObservations], start: i64) -> &MasterSlot {
    &slots
        .iter()
        .find(|s| s.slot.start == t(start))
        .unwrap_or_else(|| panic!("no slot at {start}: {slots:#?}"))
        .slot
}

fn nothing_changed(out: &Ingested) -> bool {
    let MergeStats {
        inserted,
        updated,
        removed,
        ..
    } = out.stats;
    (inserted, updated, removed) == (0, 0, 0)
        && !out.applied
        && out.history.is_empty()
        && out.samples.is_empty()
}

// ---------------------------------------------------------------------------
// Channels
// ---------------------------------------------------------------------------

/// The canonical channels slots refer to are made from the provider's own
/// channel list in one place: id from the call sign, the provider's number,
/// and the call sign as the name.
#[tokio::test]
async fn store_channels_creates_canonical_channels_from_the_provider_list() {
    let store = connect("sqlite::memory:").await.unwrap();
    let kare = ProviderChannel {
        provider_channel_id: "10002".into(),
        number: "11.1".into(),
        call_sign: "KARE".into(),
    };
    let listed = store_channels(
        store.as_ref(),
        &pid("a"),
        "ota",
        &[wcco(), kare.clone()],
        t(0),
    )
    .await
    .unwrap();
    assert_eq!(
        listed.into_iter().collect::<Vec<_>>(),
        [ChannelId("KARE.us".into()), ch()]
    );
    assert_eq!(
        store.list_channels().await.unwrap(),
        [
            CanonicalChannel {
                id: ch(),
                number: "4.1".into(),
                name: "WCCO-DT".into(),
                call_sign: "WCCO-DT".into(),
            },
            CanonicalChannel {
                id: ChannelId("KARE.us".into()),
                number: "11.1".into(),
                name: "KARE".into(),
                call_sign: "KARE".into(),
            },
        ]
    );
    // The provider's own list is kept too (for matching and overrides).
    assert_eq!(
        store
            .provider_channels_list(&pid("a"), "ota")
            .await
            .unwrap(),
        [wcco(), kare.clone()]
    );

    // One construction site: what ingest stores is `canonical_channel`, and
    // tuner matching builds on the very same function (it only replaces the
    // number and name with the tuner's), so slots and feeds agree on ids.
    let stored = store.list_channels().await.unwrap();
    assert_eq!(
        stored,
        [canonical_channel(&wcco()), canonical_channel(&kare)]
    );
    let tuner = TunerChannel {
        number: " 4.1 ".into(),
        name: "WCCO".into(),
    };
    let matched = match_channels(&[tuner], &[wcco(), kare], &Overrides::default());
    assert_eq!(
        matched.matched[0].1,
        CanonicalChannel {
            number: "4.1".into(),
            name: "WCCO".into(),
            ..canonical_channel(&wcco())
        }
    );
}

// ---------------------------------------------------------------------------
// Never worse, idempotence
// ---------------------------------------------------------------------------

/// Replaying the same response changes nothing: an outcome with zero
/// changes, no history, no samples, and the store exactly as it was.
#[tokio::test]
async fn replaying_the_same_response_changes_nothing() {
    let store = store().await;
    let s = store.as_ref();
    let response = [
        rich(obs("a", 600, 660, "News", -60)),
        rich(obs("a", 660, 720, "Movie", -60)),
    ];
    let first = ingest(s, &scorer(), "a", (0, 1440), &response, NOW)
        .await
        .unwrap();
    assert_eq!(first.stats.inserted, 2);
    assert!(first.applied);
    let before = dump(s).await;
    assert_eq!(before.len(), 2);

    let again = ingest(s, &scorer(), "a", (0, 1440), &response, NOW)
        .await
        .unwrap();
    assert!(nothing_changed(&again), "{again:#?}");
    assert_eq!(dump(s).await, before);
}

/// A provider's thinner re-fetch of the same programmes keeps the image and
/// description it sent before.
#[tokio::test]
async fn thin_refetch_keeps_image_and_description() {
    let store = store().await;
    let s = store.as_ref();
    let full = rich(obs("a", 600, 660, "News", -120));
    ingest(
        s,
        &scorer(),
        "a",
        (0, 1440),
        std::slice::from_ref(&full),
        NOW,
    )
    .await
    .unwrap();
    let thin = obs("a", 600, 660, "News", -60); // no details at all
    ingest(s, &scorer(), "a", (0, 1440), &[thin], NOW)
        .await
        .unwrap();
    let slot = slot_at(&dump(s).await, 600).clone();
    assert_eq!(slot.image_url, full.image_url);
    assert_eq!(slot.description, full.description);
    assert_eq!(slot.genres, full.genres);
    assert_eq!((slot.season, slot.episode), (Some(1), Some(2)));
}

/// A response that lists the channel with no programmes removes nothing
/// (and reads nothing: there is nothing to say). A response that does not
/// list the channel at all is never handed to ingest.
#[tokio::test]
async fn a_listed_channel_with_zero_observations_removes_nothing() {
    let store = store().await;
    let s = store.as_ref();
    ingest(
        s,
        &scorer(),
        "a",
        (0, 1440),
        &[obs("a", 600, 660, "News", -120)],
        NOW,
    )
    .await
    .unwrap();
    let before = dump(s).await;
    let out = ingest(s, &scorer(), "a", (0, 1440), &[], NOW)
        .await
        .unwrap();
    assert_eq!(out, Ingested::default());
    assert_eq!(dump(s).await, before);

    // Absence of a programme from a later response removes nothing either.
    let out = ingest(
        s,
        &scorer(),
        "a",
        (0, 1440),
        &[obs("a", 900, 960, "Other", -60)],
        NOW,
    )
    .await
    .unwrap();
    assert_eq!(out.stats.removed, 0);
    assert_eq!(slot_at(&dump(s).await, 600).title, "News");
}

/// A stale replay (older than what is stored) changes nothing and yields no
/// hindsight sample.
#[tokio::test]
async fn a_stale_replay_changes_nothing() {
    let store = store().await;
    let s = store.as_ref();
    ingest(
        s,
        &scorer(),
        "a",
        (0, 1440),
        &[obs("a", 600, 660, "News", -60)],
        NOW,
    )
    .await
    .unwrap();
    let before = dump(s).await;
    let old = [obs("a", 605, 665, "Old News", -240)];
    let out = ingest(s, &scorer(), "a", (0, 1440), &old, NOW)
        .await
        .unwrap();
    assert!(nothing_changed(&out), "{out:#?}");
    assert_eq!(dump(s).await, before);
}

/// A second provider's observations go through the same interface: they
/// join the first provider's slot, the better-scored provider wins, and the
/// loser fills the fields the winner lacks.
#[tokio::test]
async fn a_second_provider_arbitrates_and_fills_missing_fields() {
    let store = store().await;
    let s = store.as_ref();
    ingest(
        s,
        &scorer(),
        "a",
        (0, 1440),
        &[obs("a", 600, 660, "News at Ten", -60)],
        NOW,
    )
    .await
    .unwrap();
    let mut b = obs("b", 602, 662, "News", -30);
    b.image_url = Some("https://img/b.jpg".into());
    b.description = Some("From b".into());
    let out = ingest(s, &scorer(), "b", (0, 1440), &[b], NOW)
        .await
        .unwrap();
    assert_eq!((out.stats.inserted, out.stats.updated), (0, 1), "{out:#?}");
    let slots = dump(s).await;
    assert_eq!(slots.len(), 1, "joined, not duplicated");
    let slot = &slots[0];
    assert_eq!(slot.observations.len(), 2);
    assert_eq!(slot.slot.winner, pid("a"));
    assert_eq!(slot.slot.title, "News at Ten");
    assert_eq!((slot.slot.start, slot.slot.end), (t(600), t(660)));
    assert_eq!(slot.slot.image_url.as_deref(), Some("https://img/b.jpg"));
    assert_eq!(slot.slot.description.as_deref(), Some("From b"));
    // b's sample is not a hindsight sample: hindsight compares a provider
    // with ITSELF.
    assert!(out.samples.is_empty());
}

// ---------------------------------------------------------------------------
// Score history
// ---------------------------------------------------------------------------

/// Every inserted and every updated slot gets one score-history row naming
/// that slot's id; unchanged slots get none.
#[tokio::test]
async fn score_history_rows_name_the_inserted_and_updated_slots() {
    let store = store().await;
    let s = store.as_ref();
    let out = ingest(
        s,
        &scorer(),
        "a",
        (0, 1440),
        &[
            obs("a", 660, 720, "Movie", -120),
            obs("a", 600, 660, "News", -120),
        ],
        NOW,
    )
    .await
    .unwrap();
    let slots = dump(s).await;
    let (news, movie) = (slot_at(&slots, 600).id, slot_at(&slots, 660).id);
    let mut ids: Vec<SlotId> = out.history.iter().map(|r| r.slot).collect();
    ids.sort();
    let mut want = vec![news, movie];
    want.sort();
    assert_eq!(ids, want);
    for r in &out.history {
        let slot = slots.iter().find(|s| s.slot.id == r.slot).unwrap();
        assert_eq!(r.provider, slot.slot.winner);
        assert_eq!(r.channel, ch());
        assert_eq!(r.scored_at, t(NOW));
        assert_eq!(r.score, slot.slot.score);
        assert_eq!(r.breakdown, slot.slot.breakdown);
    }
    assert_eq!(take_history(s).await, 2, "both rows were stored");

    // News renamed (an update), Movie unchanged, Late new (an insert).
    let out = ingest(
        s,
        &scorer(),
        "a",
        (0, 1440),
        &[
            obs("a", 600, 660, "Breaking News", -60),
            obs("a", 660, 720, "Movie", -60),
            obs("a", 720, 780, "Late", -60),
        ],
        NOW,
    )
    .await
    .unwrap();
    assert_eq!((out.stats.inserted, out.stats.updated), (1, 1), "{out:#?}");
    let slots = dump(s).await;
    let late = slot_at(&slots, 720).id;
    let mut ids: Vec<SlotId> = out.history.iter().map(|r| r.slot).collect();
    ids.sort();
    let mut want = vec![news, late];
    want.sort();
    assert_eq!(ids, want, "no row for the unchanged Movie slot");
    let news_row = out.history.iter().find(|r| r.slot == news).unwrap();
    assert_eq!(news_row.score, slot_at(&slots, 600).score);
    assert_eq!(take_history(s).await, 2);
}

// ---------------------------------------------------------------------------
// Hindsight (spec 8.5)
// ---------------------------------------------------------------------------

/// Hindsight samples come only from slots that have not started yet, and
/// only when the stored listing is at least `HINDSIGHT_MIN_AGE` older than
/// the new one. Each sample is `hindsight_sample(stored, new)`.
#[tokio::test]
async fn hindsight_samples_only_for_future_slots_listed_an_hour_earlier() {
    assert_eq!(HINDSIGHT_MIN_AGE, TimeDelta::hours(1));
    let store = store().await;
    let s = store.as_ref();
    let at = |fetched: i64, shift: i64| {
        vec![
            obs("a", -30, 30, "On Air", fetched), // started already
            obs("a", 600 + shift, 660 + shift, "News", fetched),
            obs("a", 700, 760, "Movie", fetched),
        ]
    };
    let first = ingest(s, &scorer(), "a", (-60, 1440), &at(-180, 0), NOW)
        .await
        .unwrap();
    assert!(first.samples.is_empty(), "nothing to compare with yet");

    // 59 minutes later: too soon to measure anything.
    let soon = ingest(s, &scorer(), "a", (-60, 1440), &at(-121, 0), NOW)
        .await
        .unwrap();
    assert!(soon.samples.is_empty(), "{soon:#?}");

    // Exactly an hour after the stored listing (-121): News moved by 10
    // minutes, Movie the same. The airing slot gives no sample.
    let stored = dump(s).await;
    let own = |start: i64| {
        stored
            .iter()
            .find(|x| x.slot.start == t(start))
            .unwrap()
            .observations[0]
            .observation
            .clone()
    };
    let later = at(-61, 10);
    let out = ingest(s, &scorer(), "a", (-60, 1440), &later, NOW)
        .await
        .unwrap();
    assert_eq!(
        out.samples,
        [
            hindsight_sample(&own(600), &later[1]),
            hindsight_sample(&own(700), &later[2]),
        ]
    );
    assert!(out.samples[0] < 1.0 && out.samples[1] == 1.0);
}

/// Hindsight compares the new listing with the stored observation of the
/// slot THE PLANNER matched it to, never a slot found by another rule. Here
/// provider a's own records were left oddly placed by provider b's times
/// (a's record of slot A covers only its first half; a's record of slot B
/// lies before B): the planner matches a's new 600-660 to slot A (it is
/// A's exact time range), while "the stored record that overlaps it most"
/// would be B's.
#[tokio::test]
async fn hindsight_uses_the_slot_the_planner_matched() {
    let store = store().await;
    let s = store.as_ref();
    let fields = |start: i64, end: i64, title: &str| SlotFields {
        start: t(start),
        end: t(end),
        title: title.into(),
        subtitle: None,
        description: None,
        image_url: None,
        season: None,
        episode: None,
        rating: None,
        genres: vec![],
        winner: pid("b"),
        score: 0.5,
        breakdown: Default::default(),
    };
    let write = |slot: usize, o: Observation| ObservationWrite {
        slot: SlotRef::New(slot),
        first_seen: o.fetched_at,
        observation: o,
        changes: 0,
    };
    let mut plan = MergePlan::empty(t(-180));
    plan.inserts = vec![
        NewSlot {
            channel: ch(),
            fields: fields(600, 660, "Show"),
        },
        NewSlot {
            channel: ch(),
            fields: fields(660, 720, "Next"),
        },
    ];
    let a_on_a = obs("a", 600, 630, "Show", -180);
    let a_on_b = obs("a", 610, 660, "Show", -180);
    plan.observations = vec![
        write(0, obs("b", 600, 660, "Show", -180)),
        write(0, a_on_a.clone()),
        write(1, obs("b", 660, 720, "Next", -180)),
        write(1, a_on_b.clone()),
    ];
    let ids = store.apply_merge(plan).await.unwrap().inserted_ids;

    let new = obs("a", 600, 660, "Show", -60);
    let out = ingest(
        s,
        &scorer(),
        "a",
        (0, 1440),
        std::slice::from_ref(&new),
        NOW,
    )
    .await
    .unwrap();
    let want = hindsight_sample(&a_on_a, &new);
    assert_ne!(want, hindsight_sample(&a_on_b, &new), "the test must tell");
    assert_eq!(out.samples, [want], "{out:#?}");
    // And it is indeed slot A that took the new listing.
    let slots = dump(s).await;
    let a = slots.iter().find(|x| x.slot.id == ids[0]).unwrap();
    let a_now = a
        .observations
        .iter()
        .find(|o| o.observation.provider == pid("a"))
        .unwrap();
    assert_eq!(a_now.observation, new);
}

/// When the store fails to apply the merge, ingest returns an error: no
/// samples (they would count a merge that never happened), no feed-cache
/// bump, the store unchanged. The same call on a working store does yield
/// a sample, so the test would notice samples leaking.
#[tokio::test]
async fn a_failed_apply_returns_an_error_and_no_samples() {
    let inner = store().await;
    ingest(
        inner.as_ref(),
        &scorer(),
        "a",
        (0, 1440),
        &[obs("a", 600, 660, "News", -180)],
        NOW,
    )
    .await
    .unwrap();
    let failing = FailingStore::new(inner.clone());
    *failing.fail_apply_channel.lock().unwrap() = Some(ch());
    let before = dump(inner.as_ref()).await;

    let later = [obs("a", 610, 670, "News", -60)];
    let generation = AtomicU64::new(0);
    let provider = pid("a");
    let channel = ch();
    let response = || ChannelResponse {
        provider: &provider,
        channel: &channel,
        window: t(0)..t(1440),
        observations: &later,
    };
    let sc = scorer();
    let result = Ingest {
        store: &failing,
        scorer: &sc,
        feed_generation: Some(&generation),
    }
    .channel(response(), t(NOW))
    .await;
    assert!(result.is_err(), "{result:?}");
    assert_eq!(failing.apply_failures.load(Ordering::SeqCst), 1);
    assert_eq!(generation.load(Ordering::SeqCst), 0, "no feed-cache bump");
    assert_eq!(dump(inner.as_ref()).await, before, "store unchanged");
    assert_eq!(take_history(inner.as_ref()).await, 1, "only the first row");

    // Liveness: the same call through a working store yields the sample
    // and bumps the feed cache.
    let ok = Ingest {
        store: inner.as_ref(),
        scorer: &sc,
        feed_generation: Some(&generation),
    }
    .channel(response(), t(NOW))
    .await
    .unwrap();
    assert_eq!(ok.samples.len(), 1);
    assert!(ok.applied);
    assert_eq!(generation.load(Ordering::SeqCst), 1);
}

/// The feed cache is told about every stored change, and only then.
#[tokio::test]
async fn the_feed_cache_is_bumped_after_every_stored_change_only() {
    let store = store().await;
    let generation = AtomicU64::new(0);
    let provider = pid("a");
    let channel = ch();
    let sc = scorer();
    let guide = Ingest {
        store: store.as_ref(),
        scorer: &sc,
        feed_generation: Some(&generation),
    };
    let response = [obs("a", 600, 660, "News", -60)];
    for expected in [1, 1] {
        guide
            .channel(
                ChannelResponse {
                    provider: &provider,
                    channel: &channel,
                    window: t(0)..t(1440),
                    observations: &response,
                },
                t(NOW),
            )
            .await
            .unwrap();
        assert_eq!(generation.load(Ordering::SeqCst), expected);
    }
}

// ---------------------------------------------------------------------------
// Properties at the ingest level, with the real scorer and SQLite
// ---------------------------------------------------------------------------

/// 48 cases on a fixed seed (each case builds a fresh SQLite database). For
/// a stress run set `PROPTEST_CASES` (for example 20000): then that many
/// cases run on a random seed.
fn config() -> Config {
    let stress = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse::<u32>().ok());
    Config {
        cases: stress.unwrap_or(48),
        failure_persistence: None,
        rng_seed: if stress.is_some() {
            RngSeed::Random
        } else {
            RngSeed::Fixed(0x696e_6765_7374)
        },
        ..Config::default()
    }
}

/// The real scorer (default weights) with a fixed history per provider.
fn weighted() -> WeightedScorer {
    let acc = |p: &str, ema: f64, samples: u32| AccuracyRow {
        provider: pid(p),
        channel: ch(),
        ema,
        samples,
        updated_at: t(-1440),
    };
    let rel = |p: &str, success: u32, total: u32| ReliabilityRow {
        provider: pid(p),
        success,
        total,
        window_start: t(-1440),
    };
    let snap = AccuracySnapshot::from_rows(
        &[acc("a", 0.9, 5), acc("b", 0.4, 2)],
        &[rel("a", 9, 10), rel("b", 3, 10)],
    );
    WeightedScorer::new(Weights::default(), Arc::new(snap))
}

const TITLES: [&str; 4] = ["News", "Movie", "Game", "Talk"];

/// One response: provider, covered window, observations, planning time.
#[derive(Debug, Clone)]
struct Step {
    provider: &'static str,
    window: (i64, i64),
    incoming: Vec<Observation>,
    now: i64,
}

/// Details depending on the start minute, so programmes differ in what
/// they have.
fn enrich(mut o: Observation) -> Observation {
    let m = o.start.timestamp() / 60;
    let has = |d: i64| m.rem_euclid(d) != 0;
    if has(2) {
        o.description = Some(format!("{} ({})", o.title, o.provider.0));
    }
    if has(3) {
        o.image_url = Some(format!("https://img/{}/{m}.jpg", o.provider.0));
    }
    if has(5) {
        o.genres = vec!["Drama".into()];
    }
    if has(7) {
        o.season = Some(1);
        o.episode = Some(3);
    }
    o
}

/// A base timeline and several responses about it (shifted, renamed,
/// dropped and rescheduled programmes, partial windows, two providers).
/// Later responses are fetched later, one hour per step.
fn history() -> impl Strategy<Value = Vec<Step>> {
    let base = (
        -200i64..600,
        prop::collection::vec((prop::bool::weighted(0.7), 1i64..8, 4i64..30), 1..8),
    )
        .prop_map(|(mut at, parts)| {
            let mut out = Vec::new();
            for (back_to_back, gap, dur) in parts {
                if !back_to_back {
                    at += gap * 5;
                }
                out.push((at, at + dur * 5));
                at += dur * 5;
            }
            out
        });
    base.prop_flat_map(|base| {
        let n = base.len();
        let step = (
            prop::sample::select(&["a", "b"][..]),
            prop::collection::vec((0u8..10, -10i64..10, -10i64..10, 0usize..4), n),
            (-400i64..800, 12i64..300),
            -180i64..0,
            -120i64..600,
        )
            .prop_map(move |(p, per, (ws, wl), fetched, now)| {
                let mut incoming = Vec::new();
                for (&(s, e), (mode, ds, de, ti)) in base.iter().zip(per) {
                    let (s, e) = match mode {
                        0..=6 => (s + ds, e + de),
                        7 => continue,
                        _ => {
                            let jump = (e - s) / 2 + 3;
                            (s + jump, e + jump)
                        }
                    };
                    if e > s {
                        incoming.push(enrich(obs(p, s, e, TITLES[ti], fetched)));
                    }
                }
                Step {
                    provider: p,
                    window: (ws, ws + wl * 5),
                    incoming,
                    now,
                }
            });
        prop::collection::vec(step, 1..5)
    })
    .prop_map(|mut steps| {
        for (n, s) in steps.iter_mut().enumerate() {
            for o in &mut s.incoming {
                o.fetched_at += TimeDelta::hours(n as i64);
            }
        }
        steps
    })
}

/// How many detail fields of a slot are filled.
fn completeness(s: &MasterSlot) -> usize {
    let text = |f: &Option<String>| f.as_deref().is_some_and(|v| !v.is_empty());
    [
        text(&s.subtitle),
        text(&s.description),
        text(&s.image_url),
        s.season.is_some(),
        s.episode.is_some(),
        text(&s.rating),
        !s.genres.is_empty(),
    ]
    .iter()
    .filter(|x| **x)
    .count()
}

fn own_title(slots: &[SlotWithObservations], id: SlotId, p: &ProviderId) -> Option<String> {
    slots
        .iter()
        .find(|s| s.slot.id == id)?
        .observations
        .iter()
        .find(|o| &o.observation.provider == p)
        .map(|o| o.observation.title.trim().to_lowercase())
}

/// Random sequences of ingest calls through a real SQLite store with the
/// real scorer:
/// * replaying any response right after it was ingested changes nothing
///   (outcome without changes, store byte-identical);
/// * a thinner re-send of the last provider's own programmes (same times
///   and titles, one hour later, no details) never lowers how complete a
///   slot that keeps that provider's programme is.
#[test]
fn ingest_is_idempotent_and_a_thin_resend_never_loses_completeness() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let scorer = weighted();
    let checked = AtomicUsize::new(0);
    let replays = AtomicUsize::new(0);
    let mut runner = TestRunner::new(config());
    runner
        .run(&history(), |steps| {
            rt.block_on(async {
                let store = store().await;
                let s = store.as_ref();
                for step in &steps {
                    let first = ingest(
                        s,
                        &scorer,
                        step.provider,
                        step.window,
                        &step.incoming,
                        step.now,
                    )
                    .await
                    .map_err(|e| TestCaseError::fail(format!("{e:#}")))?;
                    let before = dump(s).await;
                    let again = ingest(
                        s,
                        &scorer,
                        step.provider,
                        step.window,
                        &step.incoming,
                        step.now,
                    )
                    .await
                    .map_err(|e| TestCaseError::fail(format!("{e:#}")))?;
                    prop_assert!(
                        nothing_changed(&again),
                        "replay changed something: {again:#?}"
                    );
                    prop_assert_eq!(dump(s).await, before);
                    if first.applied {
                        // A replay of a response that really changed the guide.
                        replays.fetch_add(1, Ordering::Relaxed);
                    }
                }

                // The thin re-send of the last provider's records.
                let last = steps.last().unwrap();
                let p = pid(last.provider);
                let snapshot = dump(s).await;
                let thin: Vec<Observation> = snapshot
                    .iter()
                    .filter_map(|x| x.observations.iter().find(|o| o.observation.provider == p))
                    .map(|o| {
                        let mut o = o.observation.clone();
                        o.subtitle = None;
                        o.description = None;
                        o.image_url = None;
                        o.season = None;
                        o.episode = None;
                        o.rating = None;
                        o.genres = Vec::new();
                        o.fetched_at += TimeDelta::hours(1);
                        o
                    })
                    .collect();
                let before: HashMap<SlotId, usize> = snapshot
                    .iter()
                    .map(|x| (x.slot.id, completeness(&x.slot)))
                    .collect();
                ingest(s, &scorer, last.provider, last.window, &thin, last.now)
                    .await
                    .map_err(|e| TestCaseError::fail(format!("{e:#}")))?;
                let after = dump(s).await;
                for x in &after {
                    let id = x.slot.id;
                    let same_programme = own_title(&snapshot, id, &p).is_some()
                        && own_title(&snapshot, id, &p) == own_title(&after, id, &p);
                    if let (true, Some(&b)) = (same_programme, before.get(&id)) {
                        checked.fetch_add(1, Ordering::Relaxed);
                        prop_assert!(
                            completeness(&x.slot) >= b,
                            "slot {id:?} lost details: {b} -> {}\n{x:#?}",
                            completeness(&x.slot)
                        );
                    }
                }
                Ok(())
            })
        })
        .unwrap();
    let (n, r) = (
        checked.load(Ordering::Relaxed),
        replays.load(Ordering::Relaxed),
    );
    eprintln!("ingest property: replays of applied responses {r}, thin re-send slots checked {n}");
    // Liveness: the properties really looked at something.
    assert!(
        r > 40 && n > 20,
        "checked too little: replays {r}, slots {n}"
    );
}
