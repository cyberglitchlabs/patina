//! Property tests for the merge planner. Each case builds a random history of
//! provider responses (shifted, renamed, dropped and extra programmes over a
//! shared base timeline), applies them to an in-memory guide, and checks a
//! guarantee. The seed and case count are fixed so CI runs are repeatable, and
//! failure files are not written (no `proptest-regressions/` directory).

mod common;

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use common::merge_util::{Sim, StubScorer, ch, obs, pid, range, t};
use patina::merge::{Coverage, Scorer, SlotView, same_slot};
use patina::model::{Observation, ScoreBreakdown};
use proptest::prelude::*;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

use patina::config::Weights;
use patina::merge::SlotRef;
use patina::score::{AccuracySnapshot, WeightedScorer};
use patina::store::{AccuracyRow, ReliabilityRow};
use proptest::test_runner::{Config, RngSeed, TestRunner};
use std::sync::Arc;

/// 300 cases on a fixed seed, so CI runs are repeatable. For a stress run set
/// `PROPTEST_CASES` (for example 20000): then that many cases run on a random
/// seed (the liveness thresholds below only get easier with more cases).
fn config() -> Config {
    let stress = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse::<u32>().ok());
    Config {
        cases: stress.unwrap_or(300),
        failure_persistence: None,
        rng_seed: if stress.is_some() {
            RngSeed::Random
        } else {
            RngSeed::Fixed(0x7061_7469_6e61)
        },
        ..Config::default()
    }
}

const TITLES: [&str; 5] = ["News", "Movie", "Game", "Talk", "Show"];

/// One programme of the shared base timeline: (start, end) in minutes.
fn base_timeline() -> impl Strategy<Value = Vec<(i64, i64)>> {
    (
        -300i64..600,
        prop::collection::vec((prop::bool::weighted(0.7), 1i64..12, 3i64..36), 1..10),
    )
        .prop_map(|(mut at, parts)| {
            let mut out = Vec::new();
            for (back_to_back, gap, dur) in parts {
                if !back_to_back {
                    at += gap * 5;
                }
                let end = at + dur * 5;
                out.push((at, end));
                at = end;
            }
            out
        })
}

/// One provider response: who, what it covered, and what it said.
#[derive(Debug, Clone)]
struct Step {
    coverage: Coverage,
    incoming: Vec<Observation>,
    now: i64,
}

fn step(base: Vec<(i64, i64)>, providers: &'static [&'static str]) -> impl Strategy<Value = Step> {
    let n = base.len();
    (
        prop::sample::select(providers),
        // per base programme: mode, start shift, end shift, title index
        prop::collection::vec(
            (
                // 0-6 keep (shifted a little), 7 drop, 8-9 reschedule: move
                // by just over half its length, a different programme.
                0u8..10,
                -12i64..12,
                -12i64..12,
                0usize..5,
            ),
            n,
        ),
        // extra programmes at random places
        prop::collection::vec((-400i64..1500, 2i64..30, 0usize..5), 0..3),
        // coverage window start and length, channel present?
        (-600i64..900, 6i64..300, prop::bool::weighted(0.9)),
        -240i64..0,   // fetched_at
        -120i64..900, // now
        // sometimes a long placeholder block ("To Be Announced"): start, hours
        prop::option::weighted(0.1, (-300i64..600, 6i64..24)),
    )
        .prop_map(
            move |(p, per, extra, (ws, wl, present), fetched, now, block)| {
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
                        incoming.push(obs(p, s, e, TITLES[ti], fetched));
                    }
                }
                for (s, d, ti) in extra {
                    incoming.push(obs(p, s, s + d * 5, TITLES[ti], fetched));
                }
                if let Some((s, h)) = block {
                    incoming.push(obs(p, s, s + h * 60, "To Be Announced", fetched));
                }
                Step {
                    coverage: Coverage {
                        provider: pid(p),
                        channel: ch(),
                        window: t(ws)..t(ws + wl * 5),
                        channel_present: present,
                    },
                    incoming,
                    now,
                }
            },
        )
}

/// A base timeline and several responses about it.
/// Later responses are fetched later (one hour per step, on top of each
/// step's own random fetch time), as in real use, so fresher data can
/// contradict older data.
fn history(providers: &'static [&'static str]) -> impl Strategy<Value = Vec<Step>> {
    base_timeline()
        .prop_flat_map(move |base| prop::collection::vec(step(base, providers), 1..6))
        .prop_map(|mut steps| {
            for (n, s) in steps.iter_mut().enumerate() {
                for o in &mut s.incoming {
                    o.fetched_at += chrono::TimeDelta::hours(n as i64);
                }
            }
            steps
        })
}

fn scorer() -> StubScorer {
    StubScorer::new(&[("a", 0.9), ("b", 0.5), ("c", 0.7), ("z", 0.3)])
}

/// A scorer that, like the real one, also looks at the programme's length,
/// the change counter, first-seen time and fetch time. Used to check that the
/// plan is stable even when scores depend on those.
struct TimelineScorer;

impl Scorer for TimelineScorer {
    fn score(&self, v: &SlotView) -> (f64, ScoreBreakdown) {
        let c = &v.candidate;
        // Depends on the observation's own length only.
        let sanity = ((c.end - c.start).num_minutes().clamp(0, 180)) as f64 / 180.0;
        let stability = 1.0 / (1.0 + f64::from(v.changes));
        let age = ((v.now - c.fetched_at).num_minutes().max(0) as f64 / 1440.0).min(1.0);
        let seen = ((v.now - v.first_seen).num_minutes().max(0) as f64 / 1440.0).min(1.0);
        let base = if c.provider.0 == "a" { 0.3 } else { 0.2 };
        let mut b = ScoreBreakdown::default();
        b.0.insert("sanity".into(), sanity);
        b.0.insert("stability".into(), stability);
        let score = base + 0.3 * sanity + 0.2 * stability + 0.1 * (1.0 - age) + 0.05 * seen;
        (score, b)
    }
}

fn assert_no_half_overlaps(sim: &Sim) -> Result<(), TestCaseError> {
    for (i, x) in sim.slots.iter().enumerate() {
        for y in &sim.slots[i + 1..] {
            prop_assert!(
                !same_slot(&range(&x.slot), &range(&y.slot)),
                "slots overlap by half or more: {:?} and {:?}",
                range(&x.slot),
                range(&y.slot)
            );
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(config())]

    /// Planning again from the same response, after applying the first plan,
    /// changes nothing.
    #[test]
    fn plan_merge_is_idempotent(steps in history(&["a", "b", "c"])) {
        let mut sim = Sim::default();
        for (n, s) in steps.iter().enumerate() {
            let first = sim.merge(&s.incoming, &s.coverage, &scorer(), t(s.now));
            // Liveness, so "never do anything" cannot pass: on an empty guide
            // a listed channel with any recent programme must insert a slot.
            let recent = s.incoming.iter().any(|o| o.start >= t(s.now - 7 * 1440));
            if n == 0 && s.coverage.channel_present && recent {
                prop_assert!(!first.inserts.is_empty(), "nothing inserted: {s:#?}");
            }
            let again = sim.plan(&s.incoming, &s.coverage, &scorer(), t(s.now));
            prop_assert!(again.is_empty(), "second plan not empty: {again:#?}\nstate: {:#?}", sim.slots);
        }

        // The same with a scorer that, like the real one, depends only on
        // the observation itself (length, counters, fetch and first-seen
        // times), never on its neighbours.
        let mut sim = Sim::default();
        for s in &steps {
            sim.merge(&s.incoming, &s.coverage, &TimelineScorer, t(s.now));
            let again = sim.plan(&s.incoming, &s.coverage, &TimelineScorer, t(s.now));
            prop_assert!(again.is_empty(), "second plan not empty: {again:#?}\nstate: {:#?}", sim.slots);
            assert_no_half_overlaps(&sim)?;
        }
    }

    /// The master guide never holds two slots that are "the same slot", and
    /// aired slots never change.
    #[test]
    fn no_two_master_slots_on_one_channel_overlap_by_half(steps in history(&["a", "b", "c"])) {
        let mut sim = Sim::default();
        for s in &steps {
            let aired: Vec<_> = sim
                .slots
                .iter()
                .filter(|x| x.slot.end <= t(s.now))
                .cloned()
                .collect();
            sim.merge(&s.incoming, &s.coverage, &scorer(), t(s.now));
            assert_no_half_overlaps(&sim)?;
            for a in aired {
                prop_assert_eq!(sim.get(a.slot.id), Some(&a), "aired slot changed");
            }
        }
    }

    /// A response that does not list the channel, or lists it with no events,
    /// never removes, supersedes or updates anything (in fact writes nothing).
    #[test]
    fn a_plan_from_failed_or_empty_coverage_has_no_removals_or_updates(
        steps in history(&["a", "b", "c"]),
        last in 0usize..6,
    ) {
        let mut sim = Sim::default();
        for s in &steps {
            sim.merge(&s.incoming, &s.coverage, &scorer(), t(s.now));
        }
        let probe = &steps[last % steps.len()];

        let empty = sim.plan(&[], &probe.coverage, &scorer(), t(probe.now));
        prop_assert!(empty.removals.is_empty() && empty.updates.is_empty());
        prop_assert!(empty.is_empty(), "{empty:#?}");

        let absent = Coverage { channel_present: false, ..probe.coverage.clone() };
        let plan = sim.plan(&probe.incoming, &absent, &scorer(), t(probe.now));
        prop_assert!(plan.removals.is_empty() && plan.updates.is_empty());
        prop_assert!(plan.is_empty(), "{plan:#?}");
    }

    /// A new provider's data never removes a slot or lowers its score: the
    /// observations that made the score are all still there.
    #[test]
    fn adding_observations_never_lowers_any_slot_score_for_unchanged_data(
        steps in history(&["a", "b", "c"]),
        extra in history(&["z"]),
    ) {
        let mut sim = Sim::default();
        for s in &steps {
            sim.merge(&s.incoming, &s.coverage, &scorer(), t(s.now));
        }
        let before: HashMap<_, _> = sim.slots.iter().map(|s| (s.slot.id, s.slot.score)).collect();
        let snapshot = sim.slots.clone();
        // Plan at a fixed "now" so freshness-like effects cannot interfere.
        let now = t(steps.last().unwrap().now);
        for s in &extra {
            sim.merge(&s.incoming, &s.coverage, &scorer(), now);
        }
        for (id, score) in before {
            let after = sim.get(id);
            prop_assert!(after.is_some(), "slot {id:?} disappeared");
            prop_assert!(
                after.unwrap().slot.score >= score,
                "slot {id:?} score dropped\nbefore: {snapshot:#?}\nafter: {:#?}",
                sim.slots
            );
        }
    }
}

/// The real scorer (default weights) with a fixed history for each provider,
/// so the Task 8 guarantees are also checked against what production runs.
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
        &[acc("a", 0.9, 5), acc("c", 0.4, 2), acc("z", 0.7, 1)],
        &[rel("a", 9, 10), rel("b", 3, 10), rel("z", 5, 5)],
    );
    WeightedScorer::new(Weights::default(), Arc::new(snap))
}

proptest! {
    #![proptest_config(config())]

    /// `plan_merge_is_idempotent` with the real `WeightedScorer`, whose
    /// scores depend only on the observation itself (its fields, length,
    /// counters and fetch time) plus the provider's stats, never on
    /// neighbouring programmes; plus the no-overlap invariant.
    #[test]
    fn plan_merge_is_idempotent_with_weighted_scorer(steps in history(&["a", "b", "c"])) {
        let scorer = weighted();
        let mut sim = Sim::default();
        for (n, s) in steps.iter().enumerate() {
            let first = sim.merge(&s.incoming, &s.coverage, &scorer, t(s.now));
            let recent = s.incoming.iter().any(|o| o.start >= t(s.now - 7 * 1440));
            if n == 0 && s.coverage.channel_present && recent {
                prop_assert!(!first.inserts.is_empty(), "nothing inserted: {s:#?}");
            }
            let again = sim.plan(&s.incoming, &s.coverage, &scorer, t(s.now));
            prop_assert!(again.is_empty(), "second plan not empty: {again:#?}\nstate: {:#?}", sim.slots);
            assert_no_half_overlaps(&sim)?;
        }
    }

    /// `adding_observations_never_lowers_any_slot_score_for_unchanged_data`
    /// with the real scorer. The whole history is planned at ONE fixed "now":
    /// freshness legitimately falls as time passes, and that is not a loss of
    /// data (the original property fixes "now" for the same reason).
    #[test]
    fn adding_observations_never_lowers_any_slot_score_with_weighted_scorer(
        steps in history(&["a", "b", "c"]),
        extra in history(&["z"]),
    ) {
        let scorer = weighted();
        let now = t(steps.last().unwrap().now);
        let mut sim = Sim::default();
        for s in &steps {
            sim.merge(&s.incoming, &s.coverage, &scorer, now);
        }
        let before: HashMap<_, _> = sim.slots.iter().map(|s| (s.slot.id, s.slot.score)).collect();
        let snapshot = sim.slots.clone();
        for s in &extra {
            sim.merge(&s.incoming, &s.coverage, &scorer, now);
        }
        for (id, score) in before {
            let after = sim.get(id);
            prop_assert!(after.is_some(), "slot {id:?} disappeared");
            prop_assert!(
                after.unwrap().slot.score >= score,
                "slot {id:?} score dropped {score} -> {}\nbefore: {snapshot:#?}\nafter: {:#?}",
                after.unwrap().slot.score,
                sim.slots
            );
        }
    }
}

// ---- The never-worse rule for partial responses (spec 8.4). ----

/// Overlap test on minute ranges (true when they share any time).
fn overlaps(a: &std::ops::Range<DateTime<Utc>>, b: &std::ops::Range<DateTime<Utc>>) -> bool {
    a.start < b.end && b.start < a.end
}

/// Every future slot the response does not contradict comes through
/// untouched:
/// * no incoming observation overlaps it (or the provider's own record of
///   it), or the provider's stored record is at least as fresh as every
///   incoming observation overlapping it -> byte-identical: no update,
///   removal, supersede or observation write touches it;
/// * the window does not fully contain it -> never removed or superseded (it
///   may still be updated by an observation matched to it: that is an
///   upsert, not a loss).
#[test]
fn partial_response_leaves_untouched_every_future_slot_it_does_not_contradict() {
    let checked = AtomicUsize::new(0);
    let mut runner = TestRunner::new(config());
    runner
        .run(&history(&["a", "b", "c"]), |steps| {
            let mut sim = Sim::default();
            for s in &steps {
                let now = t(s.now);
                let plan = sim.plan(&s.incoming, &s.coverage, &scorer(), now);
                let p = &s.coverage.provider;
                for x in sim.slots.iter().filter(|x| x.slot.end > now) {
                    let id = x.slot.id;
                    let sr = range(&x.slot);
                    let own = x.observations.iter().find(|o| &o.observation.provider == p);
                    let own_r = own.map(|o| o.observation.start..o.observation.end);
                    let hits: Vec<_> = s
                        .incoming
                        .iter()
                        .filter(|j| {
                            let jr = j.start..j.end;
                            overlaps(&jr, &sr) || own_r.as_ref().is_some_and(|r| overlaps(&jr, r))
                        })
                        .collect();
                    let untouched_by_data = hits.is_empty()
                        || own.is_some_and(|o| {
                            hits.iter()
                                .all(|j| j.fetched_at <= o.observation.fetched_at)
                        });
                    let outside_window = !(s.coverage.window.start <= x.slot.start
                        && x.slot.end <= s.coverage.window.end);
                    if untouched_by_data || outside_window {
                        checked.fetch_add(1, Relaxed);
                        prop_assert!(!plan.removals.contains(&id), "removed {id:?}: {plan:#?}");
                        prop_assert!(
                            plan.superseded.iter().all(|(sid, _)| *sid != id),
                            "superseded {id:?}: {plan:#?}"
                        );
                    }
                    if untouched_by_data {
                        prop_assert!(plan.updates.iter().all(|(u, _)| *u != id), "updated {id:?}");
                        prop_assert!(
                            plan.observations
                                .iter()
                                .all(|w| w.slot != SlotRef::Existing(id)),
                            "observation written for {id:?}"
                        );
                    }
                }
                sim.apply(&plan);
            }
            Ok(())
        })
        .unwrap();
    // Liveness threshold tuned to the committed fixed seed and case count.
    assert!(
        checked.load(Relaxed) > 100,
        "the property checked too little"
    );
}

/// Every removal or supersede in a plan meets all the contradiction
/// conditions: listed channel, window contains the slot, the slot is in the
/// future, the provider had observed it, and a strictly fresher incoming
/// observation from that provider overlaps it and is either a different
/// programme (not the same slot) or the same slot as ANOTHER slot it was
/// merged into (many-to-one, only while it is the same slot as at most three
/// future slots in all: the MAX_ABSORBED_SLOTS cap). A removal also needs
/// that provider to have been the slot's only source.
#[test]
fn every_removal_or_supersede_satisfies_all_contradiction_conditions() {
    let contradictions = AtomicUsize::new(0);
    let merges = AtomicUsize::new(0);
    let over_cap = AtomicUsize::new(0);
    let mut runner = TestRunner::new(config());
    runner
        .run(&history(&["a", "b", "c"]), |steps| {
            let mut sim = Sim::default();
            for s in &steps {
                let now = t(s.now);
                let plan = sim.plan(&s.incoming, &s.coverage, &scorer(), now);
                let p = &s.coverage.provider;
                // Liveness for the cap: listed-channel responses holding an
                // observation that is the same slot as more than 3 future
                // slots (a placeholder), which must contradict none of them.
                if s.coverage.channel_present
                    && s.incoming.iter().any(|j| {
                        let jr = j.start..j.end;
                        sim.slots
                            .iter()
                            .filter(|y| y.slot.end > now && same_slot(&jr, &range(&y.slot)))
                            .count()
                            > 3
                    })
                {
                    over_cap.fetch_add(1, Relaxed);
                }
                let hit: Vec<_> = plan
                    .removals
                    .iter()
                    .map(|id| (*id, true))
                    .chain(plan.superseded.iter().map(|(id, q)| {
                        assert_eq!(q, p, "only the responding provider is superseded");
                        (*id, false)
                    }))
                    .collect();
                for (id, is_removal) in hit {
                    contradictions.fetch_add(1, Relaxed);
                    let x = sim.get(id).expect("contradicted slot exists");
                    let sr = range(&x.slot);
                    prop_assert!(s.coverage.channel_present);
                    prop_assert!(x.slot.end > now, "aired slot contradicted");
                    prop_assert!(
                        s.coverage.window.start <= x.slot.start
                            && x.slot.end <= s.coverage.window.end,
                        "window does not contain {sr:?}"
                    );
                    let own = x.observations.iter().find(|o| &o.observation.provider == p);
                    prop_assert!(own.is_some(), "provider never observed {id:?}");
                    let o = &own.unwrap().observation;
                    let or = o.start..o.end;
                    if is_removal {
                        prop_assert_eq!(x.observations.len(), 1, "removed a slot others back");
                    }
                    // Future slots j is the same slot as (by range, or by
                    // the provider's record of it), as the planner counts.
                    let same_as = |jr: &std::ops::Range<DateTime<Utc>>| {
                        sim.slots
                            .iter()
                            .filter(|y| y.slot.end > now)
                            .filter(|y| {
                                same_slot(jr, &range(&y.slot))
                                    || y.observations.iter().any(|q| {
                                        &q.observation.provider == p
                                            && same_slot(
                                                jr,
                                                &(q.observation.start..q.observation.end),
                                            )
                                    })
                            })
                            .count()
                    };
                    // Merged into another slot: j is the same slot as ours
                    // and at least one other, and at most 3 in all (the cap:
                    // survivor + MAX_ABSORBED_SLOTS = 2).
                    let merged_into_other = |jr: &std::ops::Range<DateTime<Utc>>| {
                        let n = same_as(jr);
                        (2..=3).contains(&n)
                    };
                    let mut merged = false;
                    let justified = s.incoming.iter().any(|j| {
                        let jr = j.start..j.end;
                        let different = !same_slot(&jr, &sr) && !same_slot(&jr, &or);
                        let m = !different && merged_into_other(&jr);
                        let ok =
                            j.fetched_at > o.fetched_at && overlaps(&jr, &sr) && (different || m);
                        merged |= ok && m;
                        ok
                    });
                    prop_assert!(justified, "no justifying observation for {id:?}: {plan:#?}");
                    if merged {
                        merges.fetch_add(1, Relaxed);
                    }
                }
                sim.apply(&plan);
            }
            Ok(())
        })
        .unwrap();
    let (c, m) = (contradictions.load(Relaxed), merges.load(Relaxed));
    let big = over_cap.load(Relaxed);
    eprintln!("contradictions checked: {c}, of which many-to-one: {m}; over-cap responses: {big}");
    // Liveness thresholds below are tuned to the committed fixed seed and
    // case count; re-check them if either changes.
    assert!(
        big >= 10,
        "generators produced too few over-cap placeholders ({big})"
    );
    assert!(c >= 30, "generators produced too few contradictions ({c})");
    assert!(
        m >= 3,
        "generators produced too few many-to-one contradictions ({m})"
    );
}

// ---- A provider's thinner re-send never erases its own details (C1). ----

/// Give an observation some details, which ones depending on its start
/// minute, so different programmes have different gaps.
fn enrich(mut o: Observation) -> Observation {
    let m = o.start.timestamp() / 60;
    let has = |d: i64| m.rem_euclid(d) != 0;
    if has(2) {
        o.description = Some(format!("{} ({} {m})", o.title, o.provider.0));
    }
    if has(3) {
        o.image_url = Some(format!("https://img/{}/{m}.jpg", o.provider.0));
    }
    if has(5) {
        o.genres = vec!["Drama".into()];
    }
    if has(7) {
        o.subtitle = Some("Part".into());
        o.season = Some(1);
        o.episode = Some((m.rem_euclid(30) + 1) as u32);
        o.rating = Some("TV-PG".into());
    }
    o
}

/// How many detail fields of a slot are filled.
fn completeness(s: &patina::model::MasterSlot) -> usize {
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

/// Build a guide from detailed responses, then let the last responding
/// provider re-send exactly what it said before (its stored records: same
/// times and titles) one hour later but with every detail missing. No slot
/// that keeps that provider's observation may lose a detail.
fn thin_resend_check(
    steps: &[Step],
    scorer: &dyn Scorer,
    checked: &AtomicUsize,
) -> Result<(), TestCaseError> {
    let mut sim = Sim::default();
    for s in steps {
        let rich: Vec<_> = s.incoming.iter().cloned().map(enrich).collect();
        sim.merge(&rich, &s.coverage, scorer, t(s.now));
    }
    let last = steps.last().unwrap();
    let p = &last.coverage.provider;
    let now = t(last.now);
    let mut thin = Vec::new();
    for x in &sim.slots {
        if let Some(o) = x.observations.iter().find(|o| &o.observation.provider == p) {
            // Same times and title, a later fetch, no details at all.
            let mut o = o.observation.clone();
            o.subtitle = None;
            o.description = None;
            o.image_url = None;
            o.season = None;
            o.episode = None;
            o.rating = None;
            o.genres = Vec::new();
            o.fetched_at += chrono::TimeDelta::hours(1);
            thin.push(o);
        }
    }
    let coverage = Coverage {
        channel_present: true,
        ..last.coverage.clone()
    };
    let before: HashMap<_, _> = sim
        .slots
        .iter()
        .map(|s| (s.slot.id, completeness(&s.slot)))
        .collect();
    let snapshot = sim.slots.clone();
    let plan = sim.plan(&thin, &coverage, scorer, now);
    sim.apply(&plan);
    let title_of = |slots: &[patina::merge::SlotWithObservations], id| {
        slots
            .iter()
            .find(|s| s.slot.id == id)
            .and_then(|s| s.observations.iter().find(|o| &o.observation.provider == p))
            .map(|o| o.observation.title.trim().to_lowercase())
    };
    for x in &sim.slots {
        let id = x.slot.id;
        // Only a re-send of the SAME programme is covered by the rule: the
        // provider's record of this slot has the same title before and after.
        // (A provider's own records can overlap each other in rare residual
        // cases; then the planner keeps one of them and another title may
        // land on this slot, which is a real title change.)
        let same_programme = title_of(&snapshot, id).is_some()
            && title_of(&snapshot, id) == title_of(&sim.slots, id);
        if !same_programme {
            continue;
        }
        if let Some(&b) = before.get(&id) {
            checked.fetch_add(1, Relaxed);
            prop_assert!(
                completeness(&x.slot) >= b,
                "slot {id:?} lost details: {b} -> {}\nafter: {:#?}\nbefore: {:#?}\nplan: {plan:#?}",
                completeness(&x.slot),
                x,
                snapshot.iter().find(|s| s.slot.id == id)
            );
        }
    }
    Ok(())
}

/// A thinner re-send of a provider's own programmes never lowers how
/// complete a slot is, with the stub scorer and with the real one.
#[test]
fn a_thin_resend_never_lowers_completeness_of_a_slot() {
    let checked = AtomicUsize::new(0);
    let weighted = weighted();
    let mut runner = TestRunner::new(config());
    runner
        .run(&history(&["a", "b", "c"]), |steps| {
            thin_resend_check(&steps, &scorer(), &checked)?;
            thin_resend_check(&steps, &weighted, &checked)
        })
        .unwrap();
    let n = checked.load(Relaxed);
    eprintln!("thin re-send: slots checked: {n}");
    // Liveness threshold tuned to the committed fixed seed and case count.
    assert!(
        n > 300,
        "the thin re-send property checked too little ({n})"
    );
}
