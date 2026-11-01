//! Tests for the weighted, explainable scorer (src/score.rs): each signal on
//! its own, then the scorer as `plan_merge` uses it.

mod common;

use std::collections::{BTreeSet, HashMap};
use std::ops::Range;
use std::sync::Arc;

use chrono::{DateTime, TimeDelta, Utc};
use common::merge_util::{ch, cover, obs, pid, t};
use patina::config::{Config, Weights};
use patina::merge::{SlotFields, SlotWithObservations, StoredObservation, plan_merge};
use patina::model::{MasterSlot, Observation, ScoreBreakdown, SlotId};
use patina::score::signals::{
    agreement, completeness, freshness, hindsight_accuracy, reliability, stability, timeline_sanity,
};
use patina::score::{
    AccuracySnapshot, ProviderChannelAccuracy, SIGNAL_NAMES, WeightedScorer, ema_update,
    hindsight_sample, next_accuracy_row, stability_observations,
};
use patina::store::{AccuracyRow, ReliabilityRow};

fn in_unit(v: f64) -> bool {
    v.is_finite() && (0.0..=1.0).contains(&v)
}

fn mins(m: i64) -> TimeDelta {
    TimeDelta::minutes(m)
}

fn r(start: i64, end: i64) -> Range<DateTime<Utc>> {
    t(start)..t(end)
}

/// A snapshot where provider "a" on the test channel has a known history.
fn snapshot() -> AccuracySnapshot {
    AccuracySnapshot::from_rows(
        &[AccuracyRow {
            provider: pid("a"),
            channel: ch(),
            ema: 0.8,
            samples: 4,
            updated_at: t(-600),
        }],
        &[ReliabilityRow {
            provider: pid("a"),
            success: 9,
            total: 10,
            window_start: t(-6000),
        }],
    )
}

fn scorer_with(weights: Weights, snap: AccuracySnapshot) -> WeightedScorer {
    WeightedScorer::new(weights, Arc::new(snap))
}

/// Score one observation by inserting it into an empty guide.
fn score_one(o: &Observation, scorer: &WeightedScorer, now: DateTime<Utc>) -> SlotFields {
    let plan = plan_merge(
        &[],
        std::slice::from_ref(o),
        &cover("a", -1440, 2880),
        scorer,
        now,
    );
    assert_eq!(plan.inserts.len(), 1, "{plan:#?}");
    plan.inserts[0].fields.clone()
}

/// A rich observation: every optional field present.
fn rich(start: i64, end: i64, fetched: i64) -> Observation {
    let mut o = obs("a", start, end, "News", fetched);
    o.description = Some("Local news".into());
    o.episode = Some(3);
    o.image_url = Some("https://img/x.jpg".into());
    o.rating = Some("TV-G".into());
    o
}

// ---------------------------------------------------------------------------
// Signals
// ---------------------------------------------------------------------------

#[test]
fn each_signal_is_clamped_to_0_1() {
    let deltas = [
        TimeDelta::MIN,
        TimeDelta::days(-10_000),
        mins(-60),
        TimeDelta::zero(),
        TimeDelta::seconds(1),
        mins(360),
        TimeDelta::days(100_000),
        TimeDelta::MAX,
    ];
    for a in deltas {
        for h in deltas {
            let v = freshness(a, h);
            assert!(in_unit(v), "freshness({a:?}, {h:?}) = {v}");
        }
    }

    // Observations with empty, blank and enormous text.
    let mut blank = obs("a", 0, 60, "   ", 0);
    blank.description = Some(String::new());
    let mut huge = rich(0, 60, 0);
    huge.title = "x".repeat(1_000_000);
    huge.description = Some("y".repeat(1_000_000));
    huge.season = Some(u32::MAX);
    huge.episode = Some(u32::MAX);
    for o in [&blank, &huge, &obs("a", 0, 60, "", 0)] {
        assert!(in_unit(completeness(o)), "completeness");
    }

    // Ranges: zero length, reversed, the whole representable time line.
    let min = DateTime::<Utc>::MIN_UTC;
    let max = DateTime::<Utc>::MAX_UTC;
    let ranges = [
        r(0, 60),
        r(0, 0),
        r(60, 0),
        r(-100_000, 100_000),
        min..max,
        max..min,
        min..min,
    ];
    for this in &ranges {
        let v = timeline_sanity(this);
        assert!(in_unit(v), "timeline_sanity({this:?}) = {v}");
    }

    let others = vec![
        obs("b", 0, 60, "News", 0),
        obs("c", -100_000, 100_000, "", 0),
        huge.clone(),
    ];
    for o in [&blank, &huge, &obs("a", 60, 0, "x", 0)] {
        assert!(in_unit(agreement(o, &[])));
        assert!(in_unit(agreement(o, &others)));
    }
    let mut far = obs("b", 0, 60, "x", 0);
    far.start = min;
    far.end = max;
    assert!(in_unit(agreement(&obs("a", 0, 60, "x", 0), &[far.clone()])));
    assert!(in_unit(hindsight_sample(&far, &obs("a", 0, 60, "x", 0))));
    assert!(in_unit(hindsight_sample(&obs("a", 0, 60, "x", 0), &far)));

    let counts = [0, 1, 2, 7, u32::MAX];
    for a in counts {
        for b in counts {
            assert!(in_unit(stability(a, b)), "stability({a}, {b})");
            assert!(in_unit(reliability(a, b)), "reliability({a}, {b})");
        }
    }

    let weird = [
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        -5.0,
        -0.0,
        0.0,
        0.5,
        1.0,
        7.0,
        f64::MAX,
        f64::MIN_POSITIVE,
    ];
    for ema in weird {
        for samples in counts {
            let v = hindsight_accuracy(&ProviderChannelAccuracy { ema, samples });
            assert!(in_unit(v), "hindsight_accuracy({ema}, {samples}) = {v}");
        }
        for sample in weird {
            assert!(
                in_unit(ema_update(ema, sample)),
                "ema_update({ema}, {sample})"
            );
        }
    }
}

#[test]
fn completeness_counts_title_description_episode_image_rating() {
    let nothing = obs("a", 0, 60, "", 0);
    assert_eq!(completeness(&nothing), 0.0);

    // Add fields one at a time: every one must raise the value.
    let mut o = obs("a", 0, 60, "News", 0);
    let mut last = completeness(&o);
    assert!(last > 0.0, "a title counts");
    let steps: [fn(&mut Observation); 4] = [
        |o| o.description = Some("Local news".into()),
        |o| o.episode = Some(3),
        |o| o.image_url = Some("https://img/x.jpg".into()),
        |o| o.rating = Some("TV-G".into()),
    ];
    for add in steps {
        add(&mut o);
        let now = completeness(&o);
        assert!(now > last, "{last} -> {now} for {o:?}");
        last = now;
    }
    assert_eq!(last, 1.0, "all five present is complete");

    // Each field alone, on top of the title, adds its own share.
    let title_only = completeness(&obs("a", 0, 60, "News", 0));
    for add in steps {
        let mut x = obs("a", 0, 60, "News", 0);
        add(&mut x);
        assert!(completeness(&x) > title_only);
    }
    // Episode information also counts when given as a season or subtitle.
    let mut s = obs("a", 0, 60, "News", 0);
    s.season = Some(2);
    assert!(completeness(&s) > title_only);
    let mut s = obs("a", 0, 60, "News", 0);
    s.subtitle = Some("Pilot".into());
    assert!(completeness(&s) > title_only);

    // Blank text does not count as present.
    let mut blank = obs("a", 0, 60, "  ", 0);
    blank.description = Some("   ".into());
    blank.rating = Some(String::new());
    assert_eq!(completeness(&blank), 0.0);

    // Genres and the like are not part of it.
    let mut g = obs("a", 0, 60, "News", 0);
    g.genres = vec!["News".into()];
    assert_eq!(completeness(&g), title_only);
}

#[test]
fn freshness_decreases_with_age() {
    let horizon = mins(60);
    let ages = [0, 30, 60, 360, 720, 1440, 4320, 43_200];
    let values: Vec<f64> = ages.iter().map(|&a| freshness(mins(a), horizon)).collect();
    assert_eq!(values[0], 1.0, "just fetched is fully fresh");
    for w in values.windows(2) {
        assert!(w[1] < w[0], "not decreasing: {values:?}");
    }
    // 6 hours old about a programme airing soon: half-life reached.
    assert!((freshness(mins(360), horizon) - 0.5).abs() < 1e-12);
    // Data about a programme days away goes stale more slowly.
    assert!(freshness(mins(360), TimeDelta::days(3)) > freshness(mins(360), horizon));
    // Clock skew (fetched "in the future") is treated as just fetched.
    assert_eq!(freshness(mins(-30), horizon), 1.0);
}

#[test]
fn timeline_sanity_penalizes_implausible_duration_and_odd_boundaries() {
    // Plausible lengths on whole minutes score fully.
    assert_eq!(timeline_sanity(&r(60, 120)), 1.0, "one hour");
    assert_eq!(timeline_sanity(&r(0, 1)), 1.0, "one minute");
    assert_eq!(
        timeline_sanity(&r(0, 480)),
        1.0,
        "8 hours is still plausible (a movie block)"
    );

    // Implausible durations.
    let nine_hours = timeline_sanity(&r(0, 540));
    assert!(nine_hours <= 0.3, "9-hour episode: {nine_hours}");
    assert_eq!(timeline_sanity(&r(60, 60)), 0.0, "0-minute episode");
    assert_eq!(timeline_sanity(&r(60, 0)), 0.0, "negative length");
    let thirty_seconds = timeline_sanity(&(t(0)..t(0) + TimeDelta::seconds(30)));
    assert!(thirty_seconds <= 0.3, "30-second episode: {thirty_seconds}");

    // Off-minute boundaries are penalised only mildly, start and end alike.
    let odd_start = timeline_sanity(&(t(60) + TimeDelta::seconds(30)..t(120)));
    let odd_end = timeline_sanity(&(t(60)..t(120) + TimeDelta::seconds(5)));
    let both = timeline_sanity(&(t(60) + TimeDelta::seconds(30)..t(120) + TimeDelta::seconds(5)));
    assert!((0.9..1.0).contains(&odd_start), "{odd_start}");
    assert!((0.9..1.0).contains(&odd_end), "{odd_end}");
    assert!(both < odd_start && both > 0.85, "{both}");
}

#[test]
fn timeline_integrity_reports_gaps_and_overlaps() {
    use patina::model::ChannelId;
    use patina::score::{TimelineReport, timeline_integrity};
    let on = |c: &str, s: i64, e: i64| {
        let mut o = obs("a", s, e, "Show", 0);
        o.channel = ChannelId(c.into());
        o
    };
    let close = |a: f64, b: f64| (a - b).abs() < 1e-9;

    // Empty: all zero, no NaN.
    assert_eq!(timeline_integrity(&[]), TimelineReport::default());

    // Clean back-to-back programmes, given out of order.
    let clean = timeline_integrity(&[on("A", 60, 120), on("A", 0, 60), on("A", 120, 150)]);
    assert_eq!(clean.channels, 1);
    assert!(close(clean.observed_minutes, 150.0));
    assert_eq!((clean.gap_minutes, clean.overlap_minutes), (0.0, 0.0));
    assert_eq!((clean.gap_fraction, clean.overlap_fraction), (0.0, 0.0));

    // A 30-minute gap in a 120-minute span.
    let gap = timeline_integrity(&[on("A", 0, 30), on("A", 60, 120)]);
    assert!(
        close(gap.gap_minutes, 30.0) && close(gap.gap_fraction, 0.25),
        "{gap:?}"
    );
    assert_eq!(gap.overlap_minutes, 0.0);

    // A 15-minute overlap in a 105-minute span.
    let overlap = timeline_integrity(&[on("A", 0, 60), on("A", 45, 105)]);
    assert!(close(overlap.overlap_minutes, 15.0), "{overlap:?}");
    assert!(close(overlap.overlap_fraction, 15.0 / 105.0));
    assert_eq!(overlap.gap_minutes, 0.0);

    // One programme per channel: no gaps between channels, spans add up.
    let per_channel = timeline_integrity(&[on("A", 0, 60), on("B", 600, 630), on("C", 0, 30)]);
    assert_eq!(per_channel.channels, 3);
    assert!(close(per_channel.observed_minutes, 120.0));
    assert_eq!(
        (per_channel.gap_minutes, per_channel.overlap_minutes),
        (0.0, 0.0)
    );

    // Degenerate programmes cover nothing and never produce NaN.
    // A channel whose only programmes are degenerate contributes nothing,
    // not even to the channel count.
    let odd = timeline_integrity(&[on("A", 60, 60), on("A", 90, 30)]);
    assert_eq!(odd, TimelineReport::default(), "{odd:?}");
    for v in [odd.gap_fraction, odd.overlap_fraction, odd.observed_minutes] {
        assert!(v.is_finite() && v >= 0.0, "{odd:?}");
    }

    // A zero-length programme does not set where the channel's span starts:
    // 00:00-00:00 then 10:00-11:00 is one clean hour, not a 600-minute gap.
    let junk_first = timeline_integrity(&[on("A", 0, 0), on("A", 600, 660)]);
    assert_eq!(junk_first.channels, 1);
    assert!(close(junk_first.observed_minutes, 60.0), "{junk_first:?}");
    assert_eq!(
        (junk_first.gap_minutes, junk_first.gap_fraction),
        (0.0, 0.0)
    );
    assert_eq!(junk_first.overlap_minutes, 0.0);

    // A reversed programme in the middle of a clean sequence is ignored: no
    // phantom gap or overlap, and the span is unchanged.
    let reversed_mid = timeline_integrity(&[
        on("A", 0, 60),
        on("A", 100, 10),
        on("A", 60, 120),
        on("A", 120, 180),
    ]);
    assert_eq!(reversed_mid.channels, 1);
    assert!(
        close(reversed_mid.observed_minutes, 180.0),
        "{reversed_mid:?}"
    );
    assert_eq!(
        (reversed_mid.gap_minutes, reversed_mid.overlap_minutes),
        (0.0, 0.0)
    );

    // Mixed: the degenerate-only channel B is not counted next to A.
    let mixed = timeline_integrity(&[on("A", 0, 60), on("B", 30, 30)]);
    assert_eq!(mixed.channels, 1);
    assert!(close(mixed.observed_minutes, 60.0));
    for v in [
        junk_first.gap_fraction,
        junk_first.overlap_fraction,
        reversed_mid.gap_fraction,
        reversed_mid.overlap_fraction,
        mixed.gap_fraction,
        mixed.overlap_fraction,
    ] {
        assert!(v.is_finite() && (0.0..=1.0).contains(&v));
    }
}

#[test]
fn agreement_is_neutral_with_no_other_providers() {
    let o = obs("a", 0, 60, "News", 0);
    assert_eq!(agreement(&o, &[]), 0.5);
    // The provider's own observations are not "others".
    assert_eq!(agreement(&o, &[obs("a", 0, 60, "Movie", 0)]), 0.5);
    // Another provider saying the same thing agrees fully...
    assert_eq!(agreement(&o, &[obs("b", 0, 60, "news", 0)]), 1.0);
    // ...and one saying something else disagrees.
    assert!(agreement(&o, &[obs("b", 30, 90, "Movie", 0)]) < 0.5);

    // In the scorer (one provider) the signal is computed, recorded as
    // neutral, and contributes nothing with the default weight 0.
    let f = score_one(
        &rich(60, 120, 0),
        &scorer_with(Weights::default(), snapshot()),
        t(0),
    );
    assert_eq!(f.breakdown.0["agreement"], 0.5);
}

#[test]
fn stability_drops_with_flip_flopping() {
    assert_eq!(stability(0, 0), 0.5, "no history is neutral");
    assert_eq!(stability(0, 5), 1.0);
    assert_eq!(stability(1, 4), 0.75);
    assert_eq!(stability(2, 4), 0.5);
    assert_eq!(stability(10, 2), 0.0);
    // Observations count 6-hour periods since first seen, at least one.
    assert_eq!(stability_observations(t(0), t(0)), 1);
    assert_eq!(stability_observations(t(0), t(359)), 1);
    assert_eq!(stability_observations(t(0), t(360)), 2);
    assert_eq!(stability_observations(t(0), t(-60)), 1, "clock skew");
    assert_eq!(stability_observations(DateTime::UNIX_EPOCH, t(0)), 0);
}

#[test]
fn reliability_is_laplace_smoothed() {
    assert_eq!(reliability(0, 0), 0.5, "no data is neutral");
    assert_eq!(reliability(9, 10), 10.0 / 12.0);
    assert_eq!(reliability(0, 10), 1.0 / 12.0);
    assert_eq!(reliability(10, 10), 11.0 / 12.0);
    assert_eq!(
        reliability(7, 3),
        reliability(3, 3),
        "success capped at total"
    );
}

#[test]
fn hindsight_accuracy_is_the_ema_or_neutral() {
    let none = ProviderChannelAccuracy {
        ema: 0.9,
        samples: 0,
    };
    assert_eq!(hindsight_accuracy(&none), 0.5);
    let some = ProviderChannelAccuracy {
        ema: 0.9,
        samples: 3,
    };
    assert_eq!(hindsight_accuracy(&some), 0.9);
    let bad = ProviderChannelAccuracy {
        ema: f64::NAN,
        samples: 3,
    };
    assert_eq!(hindsight_accuracy(&bad), 0.5);
}

#[test]
fn hindsight_sample_rewards_agreement_and_penalises_drift() {
    let a = obs("a", 0, 60, "The News!", 0);
    assert_eq!(hindsight_sample(&a, &a), 1.0);
    // Normalised titles: case, punctuation and spacing do not matter.
    assert_eq!(hindsight_sample(&a, &obs("a", 0, 60, "the  news", 60)), 1.0);
    // Within the 60-second tolerance.
    let mut near = a.clone();
    near.start += TimeDelta::seconds(30);
    near.end += TimeDelta::seconds(30);
    assert_eq!(hindsight_sample(&a, &near), 1.0);
    // Halfway: 15 minutes moved in total (start + end).
    let half = hindsight_sample(&a, &obs("a", 5, 70, "The News", 60));
    assert!(half > 0.0 && half < 1.0, "{half}");
    let more = hindsight_sample(&a, &obs("a", 10, 75, "The News", 60));
    assert!(more < half);
    // 30 minutes of combined start + end shift scores 0.
    assert_eq!(hindsight_sample(&a, &obs("a", 15, 75, "The News", 60)), 0.0);
    // A changed title scores at most 0.25, even with identical times.
    assert_eq!(hindsight_sample(&a, &obs("a", 0, 60, "Movie", 60)), 0.25);
    assert!(hindsight_sample(&a, &obs("a", 5, 70, "Movie", 60)) < 0.25);
    // Symmetric.
    let b = obs("a", 5, 70, "Movie", 60);
    assert_eq!(hindsight_sample(&a, &b), hindsight_sample(&b, &a));
}

// ---------------------------------------------------------------------------
// Accuracy bookkeeping
// ---------------------------------------------------------------------------

#[test]
fn ema_update_uses_0_2_alpha() {
    let close = |a: f64, b: f64| (a - b).abs() < 1e-12;
    assert!(close(ema_update(0.5, 1.0), 0.6));
    assert!(close(ema_update(1.0, 0.0), 0.8));
    assert!(close(ema_update(0.25, 0.75), 0.35));
    assert!(close(ema_update(0.0, 0.0), 0.0));
    // Inputs are clamped to 0..1 first.
    assert!(close(ema_update(2.0, -1.0), 0.8));
    assert!(close(ema_update(f64::NAN, 1.0), 0.2 + 0.8 * 0.5));
}

#[test]
fn first_sample_seeds_the_ema() {
    let first = ProviderChannelAccuracy::default().record(0.3);
    assert_eq!(
        first,
        ProviderChannelAccuracy {
            ema: 0.3,
            samples: 1
        }
    );
    let second = first.record(1.0);
    assert!((second.ema - (0.2 + 0.8 * 0.3)).abs() < 1e-12);
    assert_eq!(second.samples, 2);

    // The same through the stored row Task 10 will upsert.
    let row = next_accuracy_row(None, &pid("a"), &ch(), 0.3, t(0));
    assert_eq!((row.ema, row.samples, row.updated_at), (0.3, 1, t(0)));
    let row2 = next_accuracy_row(Some(&row), &pid("a"), &ch(), 1.0, t(60));
    assert!((row2.ema - 0.44).abs() < 1e-12);
    assert_eq!((row2.samples, row2.updated_at), (2, t(60)));
    assert_eq!(
        (row2.provider.clone(), row2.channel.clone()),
        (pid("a"), ch())
    );
}

#[test]
fn accuracy_snapshot_supplies_provider_stats() {
    let snap = snapshot();
    let stats = snap.get(&pid("a"), &ch()).expect("known provider");
    assert_eq!(stats.reliability, reliability(9, 10));
    assert_eq!(stats.hindsight, 0.8);
    assert_eq!(snap.get(&pid("nobody"), &ch()), None);

    // Reliability known, accuracy unknown on this channel: neutral hindsight.
    let other = patina::model::ChannelId("KARE.us".into());
    let stats = snap.get(&pid("a"), &other).expect("reliability known");
    assert_eq!(stats.hindsight, 0.5);

    // The scorer hands the same stats to plan_merge.
    let scorer = scorer_with(Weights::default(), snapshot());
    use patina::merge::Scorer;
    assert_eq!(
        scorer.provider_stats(&pid("a"), &ch()),
        snap.get(&pid("a"), &ch())
    );
    let f = score_one(&rich(60, 120, 0), &scorer, t(0));
    assert_eq!(f.breakdown.0["reliability"], reliability(9, 10));
    assert_eq!(f.breakdown.0["hindsight_accuracy"], 0.8);

    // Unknown provider: both neutral.
    let f = score_one(
        &rich(60, 120, 0),
        &scorer_with(Weights::default(), AccuracySnapshot::default()),
        t(0),
    );
    assert_eq!(f.breakdown.0["reliability"], 0.5);
    assert_eq!(f.breakdown.0["hindsight_accuracy"], 0.5);
}

// ---------------------------------------------------------------------------
// The weighted scorer
// ---------------------------------------------------------------------------

/// The signal values the scorer must produce for a lone, newly seen `o`.
fn expected_signals(o: &Observation, now: DateTime<Utc>) -> HashMap<&'static str, f64> {
    let acc = ProviderChannelAccuracy {
        ema: 0.8,
        samples: 4,
    };
    HashMap::from([
        ("freshness", freshness(now - o.fetched_at, o.start - now)),
        ("completeness", completeness(o)),
        ("timeline_sanity", timeline_sanity(&(o.start..o.end))),
        ("agreement", agreement(o, &[])),
        (
            "stability",
            stability(0, stability_observations(o.fetched_at, o.fetched_at)),
        ),
        ("hindsight_accuracy", hindsight_accuracy(&acc)),
        ("reliability", reliability(9, 10)),
    ])
}

#[test]
fn weighted_score_equals_sum_of_weight_times_signal() {
    let w = Weights::default();
    let now = t(0);
    // Not complete, fetched 3 hours ago: several signals strictly inside 0..1.
    let mut o = obs("a", 60, 120, "News", -180);
    o.description = Some("Local news".into());
    let f = score_one(&o, &scorer_with(w.clone(), snapshot()), now);
    let s = expected_signals(&o, now);
    let sum = w.freshness * s["freshness"]
        + w.completeness * s["completeness"]
        + w.timeline_sanity * s["timeline_sanity"]
        + w.agreement * s["agreement"]
        + w.stability * s["stability"]
        + w.hindsight_accuracy * s["hindsight_accuracy"]
        + w.reliability * s["reliability"];
    assert!(
        (f.score - sum).abs() < 1e-12,
        "score {} != sum {sum}",
        f.score
    );
    // Make sure the case is not trivial.
    assert!(s["freshness"] < 1.0 && s["completeness"] < 1.0);
    assert!(f.score > 0.0 && f.score < 1.0);
}

#[test]
fn score_is_normalised_when_weights_do_not_sum_to_one() {
    let now = t(0);
    let o = obs("a", 60, 120, "News", -180);
    let base = score_one(&o, &scorer_with(Weights::default(), snapshot()), now).score;

    // Every weight doubled: same score.
    let d = Weights::default();
    let doubled = Weights {
        freshness: d.freshness * 2.0,
        completeness: d.completeness * 2.0,
        timeline_sanity: d.timeline_sanity * 2.0,
        stability: d.stability * 2.0,
        reliability: d.reliability * 2.0,
        hindsight_accuracy: d.hindsight_accuracy * 2.0,
        agreement: d.agreement * 2.0,
    };
    let s = score_one(&o, &scorer_with(doubled, snapshot()), now).score;
    assert!((s - base).abs() < 1e-12, "{s} vs {base}");

    // One weight only: the score is exactly that signal.
    let only = Weights {
        freshness: 3.0,
        completeness: 0.0,
        timeline_sanity: 0.0,
        stability: 0.0,
        reliability: 0.0,
        hindsight_accuracy: 0.0,
        agreement: 0.0,
    };
    let f = score_one(&o, &scorer_with(only, snapshot()), now);
    assert!((f.score - f.breakdown.0["freshness"]).abs() < 1e-12);

    // Enormous weights (their sum overflows to infinity) still give the
    // plain mean of the signals, not NaN.
    let huge = Weights {
        freshness: f64::MAX,
        completeness: f64::MAX,
        timeline_sanity: f64::MAX,
        stability: f64::MAX,
        reliability: f64::MAX,
        hindsight_accuracy: f64::MAX,
        agreement: f64::MAX,
    };
    let f = score_one(&o, &scorer_with(huge, snapshot()), now);
    let mean = f.breakdown.0.values().sum::<f64>() / 7.0;
    assert!(
        in_unit(f.score) && (f.score - mean).abs() < 1e-12,
        "{}",
        f.score
    );
}

#[test]
fn breakdown_contains_every_signal_name() {
    let names: BTreeSet<&str> = [
        "freshness",
        "completeness",
        "timeline_sanity",
        "agreement",
        "stability",
        "hindsight_accuracy",
        "reliability",
    ]
    .into();
    assert_eq!(SIGNAL_NAMES.iter().copied().collect::<BTreeSet<_>>(), names);

    let now = t(0);
    let o = obs("a", 60, 120, "News", -180);
    let f = score_one(&o, &scorer_with(Weights::default(), snapshot()), now);
    let keys: BTreeSet<&str> = f.breakdown.0.keys().map(String::as_str).collect();
    assert_eq!(keys, names);
    // The values are the raw signals, not weighted.
    for (k, v) in expected_signals(&o, now) {
        assert_eq!(f.breakdown.0[k], v, "{k}");
        assert!(in_unit(v));
    }
}

#[test]
fn scorer_output_is_deterministic_for_same_inputs() {
    // A small timeline with gaps, an overlap and an implausible 9-hour block.
    let incoming = vec![
        rich(0, 60, -30),
        obs("a", 65, 120, "Movie", -30),
        obs("a", 110, 200, "Talk", -30),
        obs("a", 200, 230, "Show", -30),
        obs("a", 300, 840, "Marathon", -30),
    ];
    let plan = |snap: AccuracySnapshot| {
        let scorer = scorer_with(Weights::default(), snap);
        plan_merge(&[], &incoming, &cover("a", -60, 600), &scorer, t(10))
    };
    let p1 = plan(snapshot());
    let p2 = plan(snapshot());
    assert_eq!(p1, p2);
    let json = |p: &patina::merge::MergePlan| {
        p.inserts
            .iter()
            .map(|i| serde_json::to_string(&i.fields.breakdown).unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(json(&p1), json(&p2));
    // The sanity signal really varied (the 9-hour block scores lower).
    assert!(
        p1.inserts
            .iter()
            .any(|i| i.fields.breakdown.0["timeline_sanity"] < 1.0)
    );
}

/// The stored slot and its one observation by "a" with the given first_seen.
fn stored(o: &Observation, first_seen: DateTime<Utc>) -> SlotWithObservations {
    SlotWithObservations {
        slot: MasterSlot {
            id: SlotId(1),
            channel: ch(),
            start: o.start,
            end: o.end,
            title: o.title.clone(),
            subtitle: None,
            description: None,
            image_url: None,
            season: None,
            episode: None,
            rating: None,
            genres: vec![],
            winner: pid("a"),
            score: 0.0,
            breakdown: ScoreBreakdown::default(),
            updated_at: t(-600),
        },
        observations: vec![StoredObservation {
            observation: o.clone(),
            first_seen,
            changes: 0,
        }],
    }
}

#[test]
fn unknown_first_seen_is_neutral_not_maximal() {
    let old = obs("a", 60, 120, "News", -600);
    let new = obs("a", 60, 120, "News", -60);
    let scorer = scorer_with(Weights::default(), snapshot());
    let run = |first_seen: DateTime<Utc>| {
        let existing = [stored(&old, first_seen)];
        let plan = plan_merge(
            &existing,
            std::slice::from_ref(&new),
            &cover("a", -1440, 2880),
            &scorer,
            t(0),
        );
        assert_eq!(plan.updates.len(), 1, "{plan:#?}");
        plan.updates[0].1.clone()
    };
    // Rows migrated from before first_seen existed carry 0 (1970): unknown.
    let unknown = run(DateTime::UNIX_EPOCH);
    assert_eq!(unknown.breakdown.0["stability"], 0.5);
    // Freshness comes from fetched_at, not from the (unknown) first-seen age.
    assert_eq!(
        unknown.breakdown.0["freshness"],
        freshness(t(0) - new.fetched_at, new.start - t(0))
    );
    // Even earlier than 1970 is unknown, not "seen for a very long time".
    let before = run(DateTime::UNIX_EPOCH - TimeDelta::days(1));
    assert_eq!(before.breakdown.0["stability"], 0.5);
    // A known first-seen with no changes is fully stable, so it scores higher.
    let known = run(old.fetched_at);
    assert_eq!(known.breakdown.0["stability"], 1.0);
    assert!(known.score > unknown.score);
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[test]
fn zero_weights_total_is_rejected_at_config_validation() {
    let load = |toml: &str| Config::load(&HashMap::new(), Some(toml));
    let all_zero = "[scoring]\nfreshness=0.0\ncompleteness=0.0\ntimeline_sanity=0.0\n\
                    stability=0.0\nreliability=0.0\nhindsight_accuracy=0.0\nagreement=0.0\n";
    let err = load(all_zero).unwrap_err();
    assert!(err.to_string().contains("zero"), "got: {err}");

    // Each kind of bad single weight is rejected and named.
    for bad in ["nan", "inf", "-inf", "-0.5"] {
        let err = load(&format!("[scoring]\nstability = {bad}\n")).unwrap_err();
        assert!(err.to_string().contains("stability"), "{bad}: {err}");
    }
    // One positive weight is enough.
    let one = all_zero.replace("agreement=0.0", "agreement=1.0");
    assert!(load(&one).is_ok());
    // The defaults are valid and sum to 1.
    let c = Config::load(&HashMap::new(), None).unwrap();
    let w = c.scoring;
    let total = w.freshness
        + w.completeness
        + w.timeline_sanity
        + w.stability
        + w.reliability
        + w.hindsight_accuracy
        + w.agreement;
    assert!((total - 1.0).abs() < 1e-12);
}
