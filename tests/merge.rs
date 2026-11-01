//! Example tests for the merge planner (spec 4.11, 8.1, 8.4). Times are
//! minutes after 2026-11-01 00:00 UTC; `now` is minute 0 unless stated, so
//! programmes at minute 600 (10:00) are in the future.

mod common;

use chrono::TimeDelta;
use common::merge_util::{Sim, StubScorer, ch, cover, obs, pid, t};
use patina::merge::{Coverage, SlotRef, same_slot};

const NOW: i64 = 0;

fn scorer() -> StubScorer {
    StubScorer::new(&[("a", 0.9), ("b", 0.5)])
}

/// A guide with provider "a"'s programmes, fetched at minute -60.
fn seeded(programmes: &[(i64, i64, &str)]) -> Sim {
    let mut sim = Sim::default();
    let incoming: Vec<_> = programmes
        .iter()
        .map(|(s, e, title)| obs("a", *s, *e, title, -60))
        .collect();
    sim.merge(&incoming, &cover("a", 0, 1440), &scorer(), t(NOW));
    assert_eq!(sim.slots.len(), programmes.len(), "seeding failed");
    sim
}

#[test]
fn same_slot_when_overlap_at_least_half_of_shorter() {
    // Rule on bare ranges: 30 of 60 minutes is exactly half -> same slot.
    assert!(same_slot(&(t(600)..t(660)), &(t(630)..t(690))));
    // 29 of 60 minutes is less than half -> different.
    assert!(!same_slot(&(t(600)..t(660)), &(t(631)..t(691))));
    // A short item inside a long one overlaps 100% of the shorter.
    assert!(same_slot(&(t(600)..t(720)), &(t(645)..t(675))));

    // Through the planner: b's 10:30-11:30 attaches to a's 10:00-11:00 slot
    // instead of creating a second slot.
    let mut sim = seeded(&[(600, 660, "News")]);
    let id = sim.slots[0].slot.id;
    let plan = sim.merge(
        &[obs("b", 630, 690, "News", -30)],
        &cover("b", 0, 1440),
        &scorer(),
        t(NOW),
    );
    assert!(plan.inserts.is_empty(), "{plan:#?}");
    assert_eq!(plan.observations.len(), 1);
    assert_eq!(plan.observations[0].slot, SlotRef::Existing(id));
    assert_eq!(sim.slots.len(), 1);
    assert_eq!(sim.slots[0].observations.len(), 2);

    // ...while b's 10:31-11:31 (29 minutes of overlap) is a different slot.
    let mut sim = seeded(&[(600, 660, "News")]);
    let plan = sim.merge(
        &[obs("b", 631, 691, "Other", -30)],
        &cover("b", 0, 1440),
        &scorer(),
        t(NOW),
    );
    assert_eq!(plan.inserts.len(), 1, "{plan:#?}");
    assert_eq!(sim.titles(), ["News", "Other"]);
}

#[test]
fn shifted_start_by_3_minutes_is_same_slot() {
    let mut sim = seeded(&[(1200, 1230, "Wheel")]);
    let id = sim.slots[0].slot.id;
    let plan = sim.merge(
        &[obs("b", 1203, 1233, "Wheel of Fortune", -30)],
        &cover("b", 0, 1440),
        &scorer(),
        t(NOW),
    );
    assert!(plan.inserts.is_empty());
    assert_eq!(plan.observations[0].slot, SlotRef::Existing(id));
    assert_eq!(sim.slots.len(), 1);
    // a scores higher, so a's times and title stay.
    assert_eq!(sim.slots[0].slot.start, t(1200));
    assert_eq!(sim.slots[0].slot.title, "Wheel");
}

#[test]
fn adjacent_programmes_are_different_slots() {
    assert!(!same_slot(&(t(1200)..t(1230)), &(t(1230)..t(1260))));

    let mut sim = Sim::default();
    let plan = sim.merge(
        &[
            obs("a", 1200, 1230, "Wheel", -60),
            obs("a", 1230, 1260, "Jeopardy", -60),
        ],
        &cover("a", 0, 1440),
        &scorer(),
        t(NOW),
    );
    assert_eq!(plan.inserts.len(), 2);
    assert_eq!(sim.titles(), ["Wheel", "Jeopardy"]);

    // Another provider's programme right after a's last one is new too.
    sim.merge(
        &[obs("b", 1260, 1290, "News", -30)],
        &cover("b", 0, 1440),
        &scorer(),
        t(NOW),
    );
    assert_eq!(sim.titles(), ["Wheel", "Jeopardy", "News"]);
}

#[test]
fn new_observation_inserts_slot_with_provenance() {
    let mut o = obs("a", 600, 660, "News", -60);
    o.description = Some("Local news".into());
    o.image_url = Some("https://img/news.jpg".into());
    o.genres = vec!["News".into()];
    let plan = Sim::default().plan(&[o.clone()], &cover("a", 0, 1440), &scorer(), t(NOW));

    assert_eq!(plan.inserts.len(), 1);
    let ins = &plan.inserts[0];
    assert_eq!(ins.channel, ch());
    assert_eq!((ins.fields.start, ins.fields.end), (t(600), t(660)));
    assert_eq!(ins.fields.title, "News");
    assert_eq!(
        ins.fields.image_url.as_deref(),
        Some("https://img/news.jpg")
    );
    assert_eq!(ins.fields.winner, pid("a"));
    assert_eq!(ins.fields.score, 0.9);
    assert_eq!(ins.fields.breakdown.0["stub"], 0.9);

    // Provenance: a's own observation is written against the new slot.
    assert_eq!(plan.observations.len(), 1);
    let w = &plan.observations[0];
    assert_eq!(w.slot, SlotRef::New(0));
    assert_eq!(w.observation, o);
    assert_eq!(w.first_seen, o.fetched_at);
    assert_eq!(w.changes, 0);
    assert!(plan.removals.is_empty() && plan.updates.is_empty());
}

#[test]
fn higher_score_provider_wins_and_loser_fills_missing_image_and_description() {
    // b (lower score) arrives first with image and description.
    let mut sim = Sim::default();
    let mut b = obs("b", 602, 662, "The News", -90);
    b.image_url = Some("https://img/b.jpg".into());
    b.description = Some("From b".into());
    b.season = Some(3);
    sim.merge(&[b], &cover("b", 0, 1440), &scorer(), t(NOW));

    // a (higher score) has its own times and title but no image/description.
    let mut a = obs("a", 600, 660, "News at 10", -60);
    a.episode = Some(7);
    let plan = sim.merge(&[a], &cover("a", 0, 1440), &scorer(), t(NOW));
    assert_eq!(plan.updates.len(), 1, "{plan:#?}");
    assert_eq!(sim.slots.len(), 1);

    let s = &sim.slots[0].slot;
    assert_eq!(s.winner, pid("a"));
    assert_eq!(s.score, 0.9);
    assert_eq!((s.start, s.end), (t(600), t(660)));
    assert_eq!(s.title, "News at 10");
    assert_eq!(s.image_url.as_deref(), Some("https://img/b.jpg"));
    assert_eq!(s.description.as_deref(), Some("From b"));
    assert_eq!((s.season, s.episode), (Some(3), Some(7)));
    assert_eq!(sim.slots[0].observations.len(), 2);
}

#[test]
fn slot_only_one_provider_has_is_kept() {
    let mut sim = seeded(&[(600, 660, "News"), (660, 720, "Talk")]);
    let before = sim.slots.clone();
    // b covers the whole day but only lists News.
    let plan = sim.merge(
        &[obs("b", 600, 660, "News", -30)],
        &cover("b", 0, 1440),
        &scorer(),
        t(NOW),
    );
    assert!(plan.removals.is_empty() && plan.superseded.is_empty());
    assert!(plan.updates.is_empty(), "a still wins News");
    assert_eq!(sim.slots.len(), 2);
    assert_eq!(sim.slots[1], before[1], "Talk untouched");
}

#[test]
fn empty_response_for_present_channel_removes_nothing() {
    let sim = seeded(&[(600, 660, "News"), (660, 720, "Talk")]);
    // a's later successful fetch lists the channel, covers everything, but
    // has no events at all.
    let plan = sim.plan(&[], &cover("a", 0, 1440), &scorer(), t(NOW));
    assert!(plan.is_empty(), "{plan:#?}");
}

#[test]
fn response_without_the_channel_removes_nothing() {
    let sim = seeded(&[(600, 660, "News"), (660, 720, "Talk")]);
    let absent = Coverage {
        channel_present: false,
        ..cover("a", 0, 1440)
    };
    assert!(sim.plan(&[], &absent, &scorer(), t(NOW)).is_empty());
    // Even if a caller wrongly passed events with it, a response that does
    // not list the channel changes nothing.
    let contradicting = [obs("a", 590, 640, "Other", 0)];
    assert!(
        sim.plan(&contradicting, &absent, &scorer(), t(NOW))
            .is_empty()
    );
}

#[test]
fn channel_present_with_zero_events_removes_nothing_outside_explicit_contradiction() {
    let sim = seeded(&[(600, 660, "News"), (660, 720, "Talk"), (720, 780, "Late")]);
    // Zero events: nothing at all.
    assert!(
        sim.plan(&[], &cover("a", 0, 1440), &scorer(), t(NOW))
            .is_empty()
    );

    // Events for only part of the window: News is explicitly contradicted by a
    // fresher movie (overlap 20 of 60 minutes); Talk and Late are merely
    // absent and must stay, unshortened.
    let mut sim = sim;
    let talk = sim.slots[1].clone();
    let late = sim.slots[2].clone();
    let plan = sim.merge(
        &[obs("a", 500, 620, "Movie", 0)],
        &cover("a", 0, 1440),
        &scorer(),
        t(NOW),
    );
    assert_eq!(plan.removals.len(), 1, "{plan:#?}");
    assert_eq!(sim.titles(), ["Movie", "Talk", "Late"]);
    assert_eq!(sim.slots[1], talk);
    assert_eq!(sim.slots[2], late);
}

#[test]
fn explicit_contradiction_replaces_future_slot() {
    // a said 10:00-11:00 is "Old". A fresher successful a response covering
    // that time lists the channel with two programmes overlapping it, each by
    // only 20 of 60 minutes: neither is the same slot.
    let fresh = [
        obs("a", 510, 620, "Movie", 0),
        obs("a", 640, 760, "Game", 0),
    ];
    let mut sim = seeded(&[(600, 660, "Old")]);
    let old_id = sim.slots[0].slot.id;
    let plan = sim.merge(&fresh, &cover("a", 0, 1440), &scorer(), t(NOW));
    assert_eq!(plan.removals, [old_id]);
    assert_eq!(sim.titles(), ["Movie", "Game"]);

    // Not when the window does not fully contain the slot.
    let sim = seeded(&[(600, 660, "Old")]);
    let plan = sim.plan(&fresh, &cover("a", 0, 630), &scorer(), t(NOW));
    assert!(plan.removals.is_empty() && plan.superseded.is_empty());

    // Not when the "contradicting" data is older than what we hold.
    let stale = [obs("a", 510, 620, "Movie", -120)];
    let plan = sim.plan(&stale, &cover("a", 0, 1440), &scorer(), t(NOW));
    assert!(plan.removals.is_empty() && plan.superseded.is_empty());

    // Not by another provider: b cannot remove a's slot.
    let by_b = [obs("b", 510, 620, "Movie", 0)];
    let plan = sim.plan(&by_b, &cover("b", 0, 1440), &scorer(), t(NOW));
    assert!(plan.removals.is_empty() && plan.superseded.is_empty());

    // When another provider still has the slot, only a's observation goes and
    // the slot stays, re-scored from b's observation.
    let mut sim = seeded(&[(600, 660, "Old")]);
    sim.merge(
        &[obs("b", 600, 660, "Old (b)", -50)],
        &cover("b", 0, 1440),
        &scorer(),
        t(NOW),
    );
    let plan = sim.merge(&fresh, &cover("a", 0, 1440), &scorer(), t(NOW));
    assert!(plan.removals.is_empty());
    assert_eq!(plan.superseded, [(old_id, pid("a"))]);
    let kept = sim.get(old_id).expect("slot kept");
    assert_eq!(kept.observations.len(), 1);
    assert_eq!(kept.slot.winner, pid("b"));
    assert_eq!(kept.slot.title, "Old (b)");
}

#[test]
fn aired_slots_are_never_modified() {
    // Seeded at minute 0; now it is 12:00 and 10:00-11:00 has aired.
    let mut sim = seeded(&[(600, 660, "News"), (780, 840, "Future")]);
    let aired = sim.slots[0].clone();
    let now = t(720);

    // A fresher fetch with a changed title, and a contradiction pattern.
    let plan = sim.merge(
        &[
            obs("a", 600, 660, "Renamed", 700),
            obs("a", 640, 700, "Overlapping", 700),
        ],
        &cover("a", 0, 1440),
        &scorer(),
        now,
    );
    assert!(
        !plan.removals.contains(&aired.slot.id)
            && plan.updates.iter().all(|(id, _)| *id != aired.slot.id)
            && plan
                .observations
                .iter()
                .all(|w| w.slot != SlotRef::Existing(aired.slot.id)),
        "{plan:#?}"
    );
    assert_eq!(sim.get(aired.slot.id), Some(&aired));

    // Another provider cannot change it either.
    sim.merge(
        &[obs("b", 600, 660, "B title", 700)],
        &cover("b", 0, 1440),
        &StubScorer::new(&[("b", 1.0)]),
        now,
    );
    assert_eq!(sim.get(aired.slot.id), Some(&aired));

    // A brand-new aired programme within the last 7 days may fill history...
    let plan = sim.plan(
        &[obs("a", 400, 460, "Morning", 700)],
        &cover("a", 0, 1440),
        &scorer(),
        now,
    );
    assert_eq!(plan.inserts.len(), 1);
    // ...but not one older than that.
    let old = obs("a", 720 - 8 * 1440, 780 - 8 * 1440, "Ancient", 700);
    let mut wide = cover("a", -9 * 1440, 1440);
    wide.window.start = old.start;
    assert!(sim.plan(&[old], &wide, &scorer(), now).is_empty());
}

// ---- Many-to-one: a provider merges several programmes into one. ----

/// a alone says 10:00-10:30 "A" and 10:30-11:00 "B" (fetched at -60).
fn two_halves() -> Sim {
    seeded(&[(600, 630, "A"), (630, 660, "B")])
}

#[test]
fn provider_merging_two_programmes_into_one_replaces_both_with_one_consistent_slot() {
    let mut sim = two_halves();
    let (s1, s2) = (sim.slots[0].slot.id, sim.slots[1].slot.id);
    // A fresher a response covering both: one hour-long special.
    let special = [obs("a", 600, 660, "Special", 0)];
    let plan = sim.merge(&special, &cover("a", 0, 1440), &scorer(), t(NOW));
    assert_eq!(plan.removals, [s2], "{plan:#?}");
    assert_eq!(sim.slots.len(), 1);
    let s = &sim.slots[0].slot;
    // Same overlap with both halves: the earlier slot keeps its id, and title
    // and times both come from the special.
    assert_eq!(s.id, s1);
    assert_eq!(
        (s.start, s.end, s.title.as_str()),
        (t(600), t(660), "Special")
    );
    // And it is stable.
    assert!(
        sim.plan(&special, &cover("a", 0, 1440), &scorer(), t(NOW))
            .is_empty()
    );
}

#[test]
fn merged_programme_does_not_remove_a_slot_another_provider_still_supports() {
    let mut sim = two_halves();
    let (s1, s2) = (sim.slots[0].slot.id, sim.slots[1].slot.id);
    sim.merge(
        &[obs("b", 630, 660, "B (b)", -50)],
        &cover("b", 0, 1440),
        &scorer(),
        t(NOW),
    );
    let plan = sim.merge(
        &[obs("a", 600, 660, "Special", 0)],
        &cover("a", 0, 1440),
        &scorer(),
        t(NOW),
    );
    assert!(plan.removals.is_empty(), "{plan:#?}");
    assert_eq!(plan.superseded, [(s2, pid("a"))]);
    let kept = sim.get(s2).expect("b still backs it");
    assert_eq!(kept.observations.len(), 1);
    assert_eq!(kept.slot.title, "B (b)");
    // The residual case: the special cannot take 10:30-11:00 while b's slot
    // is there, so slot 1 keeps its times but shows the special's title.
    let first = &sim.get(s1).unwrap().slot;
    assert_eq!((first.start, first.end), (t(600), t(630)));
    assert_eq!(first.title, "Special");
    assert!(!same_slot(
        &(first.start..first.end),
        &(kept.slot.start..kept.slot.end)
    ));
}

#[test]
fn merged_programme_outside_coverage_window_removes_nothing() {
    let sim = two_halves();
    let s2 = sim.slots[1].clone();
    // The window ends at 10:45, so it does not contain 10:30-11:00.
    let plan = sim.plan(
        &[obs("a", 600, 660, "Special", 0)],
        &cover("a", 0, 645),
        &scorer(),
        t(NOW),
    );
    assert!(
        plan.removals.is_empty() && plan.superseded.is_empty(),
        "{plan:#?}"
    );
    assert!(plan.updates.iter().all(|(id, _)| *id != s2.slot.id));
    assert!(
        plan.observations
            .iter()
            .all(|w| w.slot != SlotRef::Existing(s2.slot.id))
    );
}

#[test]
fn merged_programme_not_fresher_than_stored_removes_nothing() {
    let sim = two_halves();
    // Fetched at -60, the same time as the stored halves: not fresher.
    let plan = sim.plan(
        &[obs("a", 600, 660, "Special", -60)],
        &cover("a", 0, 1440),
        &scorer(),
        t(NOW),
    );
    assert!(
        plan.removals.is_empty() && plan.superseded.is_empty(),
        "{plan:#?}"
    );
}

#[test]
fn incoming_subsecond_timestamps_do_not_cause_rewrites_or_false_contradictions() {
    let ns = TimeDelta::nanoseconds(123_456_789);
    let with_ns = |provider: &str, s: i64, e: i64, title: &str, f: i64| {
        let mut o = obs(provider, s, e, title, f);
        o.start += ns;
        o.end += ns;
        o.fetched_at += ns;
        o
    };
    let incoming = [
        with_ns("a", 600, 660, "News", 0),
        with_ns("a", 660, 720, "Talk", 0),
    ];
    let mut sim = Sim::default();
    let plan = sim.merge(&incoming, &cover("a", 0, 1440), &scorer(), t(NOW));
    // What gets written is in whole seconds, as the store keeps it.
    assert!(plan.observations.iter().all(|w| {
        let o = &w.observation;
        o.start.timestamp_subsec_nanos() == 0
            && o.end.timestamp_subsec_nanos() == 0
            && o.fetched_at.timestamp_subsec_nanos() == 0
    }));
    // The same response again: nothing to do.
    let again = sim.plan(&incoming, &cover("a", 0, 1440), &scorer(), t(NOW));
    assert!(again.is_empty(), "{again:#?}");

    // A "contradicting" programme from the same fetch second is not fresher.
    let same_second = [with_ns("a", 500, 620, "Movie", 0)];
    let plan = sim.plan(&same_second, &cover("a", 0, 1440), &scorer(), t(NOW));
    assert!(
        plan.removals.is_empty() && plan.superseded.is_empty(),
        "{plan:#?}"
    );
}

// ---- The many-to-one cap (MAX_ABSORBED_SLOTS = 2). ----

/// a alone holds `n` back-to-back half-hours from 10:00 (fetched at -60).
fn halves(n: i64) -> Sim {
    let programmes: Vec<(i64, i64, String)> = (0..n)
        .map(|i| (600 + 30 * i, 630 + 30 * i, format!("Show {i}")))
        .collect();
    let refs: Vec<(i64, i64, &str)> = programmes
        .iter()
        .map(|(s, e, t)| (*s, *e, t.as_str()))
        .collect();
    seeded(&refs)
}

#[test]
fn long_placeholder_does_not_remove_programmes_it_covers() {
    let mut sim = halves(12);
    let before = sim.slots.clone();
    // A fresher a response: one 24-hour block over a covering window.
    let tba = [obs("a", 0, 1440, "To Be Announced", 0)];
    let plan = sim.merge(&tba, &cover("a", 0, 1440), &scorer(), t(NOW));
    assert!(plan.removals.is_empty(), "{plan:#?}");
    assert!(plan.superseded.is_empty(), "{plan:#?}");
    assert!(plan.inserts.is_empty());
    // Only the ordinary attach to the greatest-overlap slot (all tie at 30
    // minutes, so the earliest).
    let first = before[0].slot.id;
    assert!(plan.updates.iter().all(|(id, _)| *id == first));
    assert!(
        plan.observations
            .iter()
            .all(|w| w.slot == SlotRef::Existing(first))
    );
    assert_eq!(sim.slots.len(), 12);
    assert_eq!(
        &sim.slots[1..],
        &before[1..],
        "other 11 slots byte-identical"
    );
    // And it is stable.
    assert!(
        sim.plan(&tba, &cover("a", 0, 1440), &scorer(), t(NOW))
            .is_empty()
    );
}

#[test]
fn merge_of_exactly_three_programmes_is_applied() {
    let mut sim = halves(3);
    let ids: Vec<_> = sim.slots.iter().map(|s| s.slot.id).collect();
    let plan = sim.merge(
        &[obs("a", 600, 690, "Special", 0)],
        &cover("a", 0, 1440),
        &scorer(),
        t(NOW),
    );
    assert_eq!(plan.removals, [ids[1], ids[2]], "{plan:#?}");
    assert_eq!(sim.slots.len(), 1);
    let s = &sim.slots[0].slot;
    assert_eq!((s.id, s.start, s.end), (ids[0], t(600), t(690)));
    assert_eq!(s.title, "Special");
}

#[test]
fn merge_of_four_programmes_is_not_applied() {
    let mut sim = halves(4);
    let before = sim.slots.clone();
    let plan = sim.merge(
        &[obs("a", 600, 720, "Special", 0)],
        &cover("a", 0, 1440),
        &scorer(),
        t(NOW),
    );
    assert!(
        plan.removals.is_empty() && plan.superseded.is_empty(),
        "{plan:#?}"
    );
    assert_eq!(sim.slots.len(), 4);
    assert_eq!(&sim.slots[1..], &before[1..]);
}

/// A slot's score depends only on its own observation (plus counters and
/// provider stats), never on the other programmes in the response, so a
/// stored score cannot go stale when a neighbour changes later.
#[test]
fn score_does_not_depend_on_other_observations_in_the_response() {
    use std::sync::Arc;

    use patina::config::Weights;
    use patina::score::{AccuracySnapshot, WeightedScorer};

    let scorer = WeightedScorer::new(Weights::default(), Arc::new(AccuracySnapshot::default()));
    let mut news = obs("a", 600, 660, "News", -30);
    news.description = Some("Local news".into());
    let alone = Sim::default().plan(&[news.clone()], &cover("a", 0, 1440), &scorer, t(NOW));

    // The same observation in a big response: back-to-back neighbours, a
    // gap, an overlap, and an absurd 9-hour block elsewhere.
    let crowd = [
        obs("a", 500, 600, "Before", -30),
        news.clone(),
        obs("a", 690, 760, "After a gap", -30),
        obs("a", 740, 800, "Overlapping", -30),
        obs("a", 900, 1440, "Nine hours", -30),
    ];
    let crowded = Sim::default().plan(&crowd, &cover("a", 0, 1440), &scorer, t(NOW));

    let fields_of = |plan: &patina::merge::MergePlan| {
        plan.inserts
            .iter()
            .find(|i| i.fields.title == "News")
            .expect("News inserted")
            .fields
            .clone()
    };
    let (a, b) = (fields_of(&alone), fields_of(&crowded));
    assert_eq!(a.score, b.score);
    assert_eq!(a.breakdown, b.breakdown);
}

// ---------------------------------------------------------------------------
// A provider's thinner re-send never erases its own earlier details (C1).
// ---------------------------------------------------------------------------

/// A slot seeded by provider a (fetched at minute -60) with every detail set.
fn rich_news() -> Sim {
    let mut o = obs("a", 600, 660, "News", -60);
    o.subtitle = Some("Late edition".into());
    o.description = Some("Local news".into());
    o.image_url = Some("https://img/news.jpg".into());
    o.season = Some(4);
    o.episode = Some(12);
    o.rating = Some("TV-G".into());
    o.genres = vec!["News".into()];
    let mut sim = Sim::default();
    sim.merge(&[o], &cover("a", 0, 1440), &scorer(), t(NOW));
    assert_eq!(sim.slots.len(), 1);
    sim
}

#[test]
fn thin_refetch_keeps_image_and_description() {
    let mut sim = rich_news();
    // A fresher response with the same times and title but no details.
    sim.merge(
        &[obs("a", 600, 660, "News", -30)],
        &cover("a", 0, 1440),
        &scorer(),
        t(NOW),
    );
    let s = &sim.slots[0].slot;
    assert_eq!(s.image_url.as_deref(), Some("https://img/news.jpg"));
    assert_eq!(s.description.as_deref(), Some("Local news"));
    assert_eq!(s.subtitle.as_deref(), Some("Late edition"));
    assert_eq!((s.season, s.episode), (Some(4), Some(12)));
    assert_eq!(s.rating.as_deref(), Some("TV-G"));
    assert_eq!(s.genres, ["News"]);
    // The stored observation keeps them too (and takes the new fetch time).
    let o = &sim.slots[0].observations[0].observation;
    assert_eq!(o.image_url.as_deref(), Some("https://img/news.jpg"));
    assert_eq!(o.description.as_deref(), Some("Local news"));
    assert_eq!(o.fetched_at, t(-30));
}

#[test]
fn refetch_with_a_different_title_does_not_carry_fields_over() {
    let mut sim = rich_news();
    sim.merge(
        &[obs("a", 600, 660, "Movie", -30)],
        &cover("a", 0, 1440),
        &scorer(),
        t(NOW),
    );
    let s = &sim.slots[0].slot;
    assert_eq!(s.title, "Movie");
    assert_eq!(s.image_url, None);
    assert_eq!(s.description, None);
    assert_eq!(s.genres, Vec::<String>::new());
}

#[test]
fn replaying_the_same_thin_response_gives_an_empty_plan() {
    let mut sim = rich_news();
    let thin = [obs("a", 600, 660, "News", -30)];
    let first = sim.merge(&thin, &cover("a", 0, 1440), &scorer(), t(NOW));
    assert!(!first.observations.is_empty(), "fresher fetch is recorded");
    let again = sim.plan(&thin, &cover("a", 0, 1440), &scorer(), t(NOW));
    assert!(again.is_empty(), "{again:#?}");
}

#[test]
fn title_match_is_trimmed_and_case_insensitive() {
    let mut sim = rich_news();
    sim.merge(
        &[obs("a", 600, 660, "  NEWS ", -30)],
        &cover("a", 0, 1440),
        &scorer(),
        t(NOW),
    );
    let s = &sim.slots[0].slot;
    assert_eq!(s.image_url.as_deref(), Some("https://img/news.jpg"));
    assert_eq!(s.description.as_deref(), Some("Local news"));
}

#[test]
fn genres_are_carried_only_when_the_new_list_is_empty() {
    let mut sim = rich_news();
    let mut o = obs("a", 600, 660, "News", -30);
    o.genres = vec!["Talk".into()];
    // An explicitly empty text field counts as missing too.
    o.description = Some(String::new());
    sim.merge(&[o], &cover("a", 0, 1440), &scorer(), t(NOW));
    let s = &sim.slots[0].slot;
    assert_eq!(s.genres, ["Talk"], "a non-empty new list replaces the old");
    assert_eq!(s.description.as_deref(), Some("Local news"));

    let mut sim = rich_news();
    sim.merge(
        &[obs("a", 600, 660, "News", -30)],
        &cover("a", 0, 1440),
        &scorer(),
        t(NOW),
    );
    assert_eq!(sim.slots[0].slot.genres, ["News"]);
}

/// The plan says which existing slot it matched each incoming observation
/// to (by the observation's position in the slice that was passed in), so a
/// caller that needs "the same slot" (hindsight) asks the planner instead of
/// matching again by its own rule. Observations that become new slots, are
/// dropped as a repeat of another one, or are the same slot as an aired
/// slot are matched to nothing. Matches write nothing: a plan with only
/// matches is empty.
#[test]
fn plan_reports_which_existing_slot_each_incoming_observation_matched() {
    let mut sim = seeded(&[(-120, -60, "Old"), (600, 660, "News"), (660, 720, "Movie")]);
    let id = |title: &str| {
        sim.slots
            .iter()
            .find(|s| s.slot.title == title)
            .unwrap()
            .slot
            .id
    };
    let (news, movie) = (id("News"), id("Movie"));
    let incoming = [
        obs("a", 662, 722, "Movie", -30),  // 0: the Movie slot
        obs("a", 900, 960, "New", -30),    // 1: a new slot
        obs("a", 601, 661, "News", -30),   // 2: the News slot
        obs("a", 603, 663, "News 2", -30), // 3: repeat of 2, dropped
        obs("a", -120, -60, "Old", -30),   // 4: aired, takes part in nothing
    ];
    let coverage = cover("a", -180, 1440);
    let plan = sim.merge(&incoming, &coverage, &scorer(), t(NOW));
    let pairs: Vec<_> = plan.matches.iter().map(|m| (m.incoming, m.slot)).collect();
    assert_eq!(pairs, [(0, movie), (2, news)], "{plan:#?}");
    assert_eq!(plan.inserts.len(), 1);

    // The same response again: nothing to write, the same matches.
    let again = sim.plan(&incoming, &coverage, &scorer(), t(NOW));
    assert!(again.is_empty(), "{again:#?}");
    let pairs: Vec<_> = again.matches.iter().map(|m| (m.incoming, m.slot)).collect();
    let new = sim
        .slots
        .iter()
        .find(|s| s.slot.title == "New")
        .unwrap()
        .slot
        .id;
    assert_eq!(pairs, [(0, movie), (1, new), (2, news)]);
}
