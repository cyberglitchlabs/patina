//! Merge planning: the pure core that decides how one provider response
//! changes the master guide.
//!
//! `plan_merge` reads the slots already stored for one channel, one provider's
//! fresh observations for that channel, and a statement of what that response
//! covered, and returns a `MergePlan`. It does no I/O, reads no clock and uses
//! no randomness, so the same inputs always give the same plan. A `Store`
//! applies the plan in one transaction (`Store::apply_merge`). The `ingest`
//! module is the one caller that loads, plans and applies.
//!
//! The rules, in plain words (spec 4.7, 4.11, 8.1, 8.4):
//!
//! * **Same slot.** Two records are the same programme airing when they are on
//!   the same channel and their time ranges overlap by at least half of the
//!   shorter range. Providers shifting a show by a few minutes still match.
//! * **Provenance.** Every slot keeps one observation per provider (that
//!   provider's own start, end, title, ...). The best-scored observation
//!   supplies the slot's start, end and title; empty fields are filled from
//!   lower-ranked observations.
//! * **Never worse.** Absence from a response never deletes or shortens a
//!   slot. A future slot is removed only when a fresher successful response
//!   from a provider that had observed it covers its whole time range, lists
//!   the channel, and has a different programme overlapping it (an explicit
//!   contradiction). Even then only that provider's own observation goes; the
//!   slot itself is removed only if no other provider still vouches for it.
//!   Extension of spec 8.4 (controller ruling): when one fresher programme
//!   from the provider is "the same slot" as several of its old ones (it
//!   merged them, e.g. two half-hour shows replaced by a one-hour special),
//!   it belongs to the one it overlaps most, and counts as a contradiction of
//!   the others, under the same conditions (listed channel, window contains
//!   the slot, strictly fresher, future slot). Such a merge may absorb at
//!   most `MAX_ABSORBED_SLOTS` (2) other slots: a fresher programme that is
//!   the same slot as more than three future slots (say a 24-hour "To Be
//!   Announced" block) is far more likely a placeholder or a thin response
//!   than a real schedule change, so it contradicts none of them; it is only
//!   recorded on the slot it overlaps most, and the others stay as they are.
//! * **Aired slots are history.** A slot whose end is at or before `now` is
//!   never updated or removed.
//! * **A provider's thinner re-send never erases its own earlier details.**
//!   When a provider sends a programme again for a slot it had already
//!   reported, with the same title (trimmed, ignoring case), every detail the
//!   new copy lacks (subtitle, description, image, season, episode, rating,
//!   and genres when the new list is empty) is taken from its own stored
//!   copy before anything else happens. A degraded or renamed provider field
//!   then cannot silently strip pictures and descriptions from the guide.
//!   Known trade-off: a provider can no longer remove a detail just by
//!   leaving it out (it must change the title, or send a new value). Keeping
//!   a slightly stale detail is the safer failure.

use std::cmp::Ordering;
use std::ops::Range;

use chrono::{DateTime, SubsecRound, TimeDelta, Utc};

use crate::model::{ChannelId, MasterSlot, Observation, ProviderId, ScoreBreakdown, SlotId};

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

/// What one SUCCESSFUL provider response (HTTP 200 and parsed without error)
/// covered for one channel. A failed or unparseable response has no coverage
/// at all: the caller must not call `plan_merge` for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Coverage {
    /// The provider that answered. Only this provider's observations are used,
    /// and only this provider's own old observations can be contradicted.
    pub provider: ProviderId,
    pub channel: ChannelId,
    /// The time window the request asked for and the response answered.
    pub window: Range<DateTime<Utc>>,
    /// Whether the response listed this channel at all. A response that does
    /// not list the channel says nothing about it, so nothing changes.
    pub channel_present: bool,
}

/// A stored observation plus the bookkeeping the stability signal needs.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredObservation {
    pub observation: Observation,
    /// When this provider first reported this slot.
    pub first_seen: DateTime<Utc>,
    /// How many times this provider's start, end or title for this slot
    /// changed between fetches (flip-flopping).
    pub changes: u32,
}

/// A master slot together with every provider's observation of it.
#[derive(Debug, Clone, PartialEq)]
pub struct SlotWithObservations {
    pub slot: MasterSlot,
    pub observations: Vec<StoredObservation>,
}

/// Per-provider running averages that later scoring signals use. Supplied by
/// the scorer itself (see `Scorer::provider_stats`), so `plan_merge` does not
/// need another parameter.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProviderStats {
    /// Share of recent fetches that succeeded, 0..1.
    pub reliability: f64,
    /// Running average of hindsight accuracy, 0..1.
    pub hindsight: f64,
}

/// Everything a scorer may look at for one candidate observation, and nothing
/// else. Built only inside this module (`#[non_exhaustive]` stops other
/// crates from constructing one), so scorers cannot depend on hidden state.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct SlotView {
    /// The observation being scored.
    pub candidate: Observation,
    /// How many times this provider's start/end/title for this slot changed.
    pub changes: u32,
    /// When this provider first reported this slot.
    pub first_seen: DateTime<Utc>,
    /// The planning time.
    pub now: DateTime<Utc>,
    /// Running per-provider averages, when the scorer has them.
    pub provider_stats: Option<ProviderStats>,
}

/// Turns a `SlotView` into a score. Task 9 implements the real weighted
/// scorer; tests use simple stubs.
pub trait Scorer {
    /// Score (higher is better) and how it was made up.
    fn score(&self, view: &SlotView) -> (f64, ScoreBreakdown);

    /// Running averages for a provider on a channel, if the scorer keeps them.
    /// They are copied into `SlotView::provider_stats`.
    fn provider_stats(
        &self,
        _provider: &ProviderId,
        _channel: &ChannelId,
    ) -> Option<ProviderStats> {
        None
    }
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

/// The content columns of a slot (everything except its id and channel).
#[derive(Debug, Clone, PartialEq)]
pub struct SlotFields {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub title: String,
    pub subtitle: Option<String>,
    pub description: Option<String>,
    pub image_url: Option<String>,
    pub season: Option<u32>,
    pub episode: Option<u32>,
    pub rating: Option<String>,
    pub genres: Vec<String>,
    pub winner: ProviderId,
    pub score: f64,
    pub breakdown: ScoreBreakdown,
}

/// A slot to create.
#[derive(Debug, Clone, PartialEq)]
pub struct NewSlot {
    pub channel: ChannelId,
    pub fields: SlotFields,
}

/// New content for an existing slot (the whole row is rewritten).
pub type SlotUpdate = SlotFields;

/// Which slot an observation write belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotRef {
    /// A slot that already exists.
    Existing(SlotId),
    /// The slot at this index of `MergePlan::inserts` (it has no id yet).
    New(usize),
}

/// Insert or replace one provider's observation of one slot. Stored with
/// "upsert on (slot_id, provider)".
#[derive(Debug, Clone, PartialEq)]
pub struct ObservationWrite {
    pub slot: SlotRef,
    pub observation: Observation,
    pub first_seen: DateTime<Utc>,
    pub changes: u32,
}

/// One incoming observation and the existing slot the planner assigned it
/// to: the planner's own answer to "which stored slot is this the same
/// programme as?" (spec 8.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanMatch {
    /// Position of the observation in the `incoming` slice given to
    /// `plan_merge` (the caller's own order, not the planner's sorted one).
    pub incoming: usize,
    /// The existing slot it belongs to.
    pub slot: SlotId,
}

/// What the merge decided. `Store::apply_merge` applies it in one transaction,
/// in this order: removals, superseded, updates, inserts, observations.
#[derive(Debug, Clone, PartialEq)]
pub struct MergePlan {
    /// Slots to delete (their observations go with them).
    pub removals: Vec<SlotId>,
    /// Single observations to delete: a provider explicitly contradicted its
    /// own earlier observation of a slot that other providers still vouch for.
    pub superseded: Vec<(SlotId, ProviderId)>,
    pub updates: Vec<(SlotId, SlotUpdate)>,
    pub inserts: Vec<NewSlot>,
    pub observations: Vec<ObservationWrite>,
    /// Existing slots the response touched that needed no change.
    pub unchanged: usize,
    /// Written to `slots.updated_at` for inserted and updated rows.
    pub planned_at: DateTime<Utc>,
    /// For every incoming observation the plan assigned to an EXISTING slot
    /// (matched in step 2 or attached in step 5), which slot that is, in
    /// incoming order. Also listed when the slot ends up unchanged, or is
    /// left as it is because the observation is a stale replay (not fresher
    /// than this provider's stored one there). Observations that become new
    /// slots, are dropped, or are the same slot as an aired slot are not
    /// listed. Information only: it writes nothing, so `is_empty` and
    /// `Store::apply_merge` ignore it. The ingest module uses it for
    /// hindsight, so nothing outside the planner has its own matching rule.
    pub matches: Vec<PlanMatch>,
}

impl MergePlan {
    /// A plan that changes nothing.
    pub fn empty(planned_at: DateTime<Utc>) -> Self {
        MergePlan {
            removals: Vec::new(),
            superseded: Vec::new(),
            updates: Vec::new(),
            inserts: Vec::new(),
            observations: Vec::new(),
            unchanged: 0,
            planned_at,
            matches: Vec::new(),
        }
    }

    /// True when applying the plan would write nothing (`matches` writes
    /// nothing, so it is not looked at).
    pub fn is_empty(&self) -> bool {
        self.removals.is_empty()
            && self.superseded.is_empty()
            && self.updates.is_empty()
            && self.inserts.is_empty()
            && self.observations.is_empty()
    }
}

/// Counts describing what applying a `MergePlan` changed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MergeStats {
    pub inserted: usize,
    pub updated: usize,
    pub removed: usize,
    pub unchanged: usize,
}

/// What `Store::apply_merge` did with a plan: the counts, and the id of
/// every slot row the plan's inserts ended up on. `inserted_ids[i]` belongs
/// to `plan.inserts[i]` (same order, same length). An insert whose channel
/// and start were already taken (the same plan applied twice) reuses that
/// row, so its id is reported too, although `stats.inserted` does not count
/// it. With these ids the caller never has to read the slots back.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MergeApplied {
    pub stats: MergeStats,
    pub inserted_ids: Vec<SlotId>,
}

// ---------------------------------------------------------------------------
// Slot identity (spec 8.1)
// ---------------------------------------------------------------------------

/// Seconds two ranges share (0 when they do not overlap).
pub fn overlap_secs(a: &Range<DateTime<Utc>>, b: &Range<DateTime<Utc>>) -> i64 {
    let start = a.start.max(b.start);
    let end = a.end.min(b.end);
    (end - start).num_seconds().max(0)
}

/// Overlap as a fraction of the shorter range, 0..1 (used only to rank
/// matches; the yes/no rule is `same_slot`).
fn overlap_fraction(a: &Range<DateTime<Utc>>, b: &Range<DateTime<Utc>>) -> f64 {
    let shorter = (a.end - a.start).min(b.end - b.start).num_seconds();
    if shorter <= 0 {
        return 0.0;
    }
    overlap_secs(a, b) as f64 / shorter as f64
}

/// The same-slot rule: overlap of at least 50% of the shorter range. (The
/// caller makes sure both are on the same channel.) Written with integers so
/// "exactly half" is not at the mercy of floating point.
pub fn same_slot(a: &Range<DateTime<Utc>>, b: &Range<DateTime<Utc>>) -> bool {
    let shorter = (a.end - a.start).min(b.end - b.start).num_seconds();
    shorter > 0 && 2 * overlap_secs(a, b) >= shorter
}

// ---------------------------------------------------------------------------
// Planning
// ---------------------------------------------------------------------------

/// How far back an observation that has already aired may still create a new
/// slot. Aired programmes are kept 7 days for history (spec 4.15), so filling
/// a gap in that recent history is harmless; anything older the janitor would
/// delete anyway, so it is ignored.
const AIRED_INSERT_LOOKBACK: TimeDelta = TimeDelta::days(7);

/// Safety bound for the "settle the slot ranges" loop below. Every round
/// strictly improves at least one slot's choice, so it ends long before this
/// on real data; the bound only guards against a bug looping forever.
const MAX_SETTLE_ROUNDS: usize = 64;

/// How many OTHER slots one fresher observation may absorb when a provider
/// merges several of its programmes into one (survivor + 2 = three
/// programmes, e.g. three half-hours replaced by a 90-minute special). A
/// single observation that is the same slot as more than three future slots
/// is overwhelmingly a placeholder ("To Be Announced" all day) or a thin
/// response, not a real schedule change; letting it contradict them would
/// collapse the guide to one block, the opposite of "never worse". Above the
/// limit it contradicts nothing and the stale data stays.
const MAX_ABSORBED_SLOTS: usize = 2;

/// Decide how one successful provider response changes the master guide of
/// one channel. Pure: same inputs, same plan.
///
/// * `existing`: every stored slot the response could touch, with each
///   provider's observation of it (the window plus every incoming
///   programme's span; `ingest` loads exactly that). Slots of other channels
///   are ignored.
/// * `incoming`: the response's observations for `coverage.channel`. Others
///   (another provider or channel, or `end <= start`) are ignored.
/// * `coverage`: what the response covered. Never pass a failed response.
///
/// How it works, step by step:
///
/// 1. Clean the incoming list: sort it, and drop any observation that is the
///    same slot as one already kept (a provider listing a programme twice).
///    Times are cut to whole seconds (what the store keeps).
/// 2. Match each incoming observation to at most one existing future slot
///    (and each slot to at most one incoming observation). A slot is a
///    candidate when the observation is the same slot as the slot's range OR
///    as this provider's own stored observation of that slot. Best pairs go
///    first: an exact repeat of the provider's own earlier observation, then
///    the greatest overlap, then the earliest slot. Matching the provider's
///    own record first is what makes a second plan from the same data find
///    the same slots again. A match whose observation is not fresher than the
///    provider's stored one there is a stale replay: that slot stays as it
///    is ("frozen").
/// 3. Explicit contradictions (spec 8.4): an unmatched future slot that this
///    provider had observed loses that observation only if the response lists
///    the channel, its window contains the whole slot, and a strictly fresher
///    incoming observation from this provider overlaps the slot and either is
///    not the same slot, or IS the same slot but was matched to (and taken by)
///    a different slot: the provider merged several programmes into one (see
///    the module docs; at most `MAX_ABSORBED_SLOTS` others, else none). The slot is removed only when no other provider
///    observed it. Absence alone never removes anything.
/// 4. Re-arbitrate every slot whose observations changed: score each
///    observation and rank them. The best one wins and supplies title,
///    fields and score, and also start/end unless those would make the slot
///    "the same slot" as a neighbouring slot, or end it at or before `now`;
///    then the slot keeps its current times. Repeated until no slot can
///    move.
/// 5. An observation that is unmatched, or only a stale replay that differs
///    from the provider's record, is attached to a slot this provider has no
///    record of (any more) if it is the same slot as that slot's final range
///    and fresher than this provider's own record there, if it still has
///    one; then steps 3 and 4 run again. The
///    rest become new slots, unless they would clash with a slot, or aired
///    more than 7 days ago.
/// 6. Aired slots (`end <= now`) are never changed and take part in no
///    match; an observation that is the same slot as one is dropped.
///
/// Steps 2-5 are built so that planning again from the same response after
/// applying the plan gives an empty plan (property-tested).
pub fn plan_merge(
    existing: &[SlotWithObservations],
    incoming: &[Observation],
    coverage: &Coverage,
    scorer: &dyn Scorer,
    now: DateTime<Utc>,
) -> MergePlan {
    let mut plan = MergePlan::empty(now);
    // A response that does not list the channel says nothing about it.
    if !coverage.channel_present {
        return plan;
    }
    let provider = &coverage.provider;

    // Slots of this channel, oldest first (ties by id, so order is total).
    let mut slots: Vec<&SlotWithObservations> = existing
        .iter()
        .filter(|s| s.slot.channel == coverage.channel)
        .collect();
    slots.sort_by_key(|s| (s.slot.start, s.slot.id));
    let n = slots.len();

    // Step 1. (`mut`: details are carried forward into it after step 2.)
    // `origin[i]` is where `fresh[i]` was in `incoming` (for `plan.matches`).
    let (mut fresh, origin) = clean_incoming(incoming, coverage);
    if fresh.is_empty() {
        // Nothing was said, so nothing changes (not even re-scoring).
        return plan;
    }

    let aired: Vec<bool> = slots.iter().map(|s| s.slot.end <= now).collect();
    // This provider's own stored observation of each slot, if any.
    let own: Vec<Option<&StoredObservation>> = slots
        .iter()
        .map(|s| {
            s.observations
                .iter()
                .find(|o| &o.observation.provider == provider)
        })
        .collect();

    // Step 2: `slot_of[i]` is the slot index incoming `i` belongs to;
    // `incoming_of[k]` is the reverse.
    let (mut slot_of, mut incoming_of) = match_incoming(&fresh, &slots, &own, &aired);

    // A thinner re-send never erases this provider's own earlier details
    // (see the module docs). Done once, here, so every later step (scoring,
    // settling and the observation writes) sees the filled copy, and a replay
    // of the same thin response compares equal to the stored copy.
    for (i, k) in slot_of.iter().enumerate() {
        if let Some(own) = k.and_then(|k| own[k]) {
            carry_forward(&mut fresh[i], &own.observation);
        }
    }
    let fresh = fresh; // read-only from here on

    // For each incoming observation: how many future slots it is the same
    // slot as (by the slot's range or by this provider's record of it), the
    // same test the matching uses. Fixed from the stored data.
    let same_as: Vec<usize> = fresh
        .iter()
        .map(|o| {
            let r = o.start..o.end;
            (0..n)
                .filter(|&k| {
                    !aired[k]
                        && (same_slot(&r, &(slots[k].slot.start..slots[k].slot.end))
                            || own[k].is_some_and(|own| {
                                same_slot(&r, &(own.observation.start..own.observation.end))
                            }))
                })
                .count()
        })
        .collect();

    // Steps 3 to 5 run in rounds, until no observation gets attached to a
    // different slot. Each observation is attached at most once (its target
    // really takes it, see below), so the rounds end.
    let mut settled;
    let mut frozen: Vec<bool>;
    let mut contradicted: Vec<bool>;
    let mut removed: Vec<bool>;
    let mut rounds = 0;
    let mut attached;
    loop {
        rounds += 1;
        // A matched observation that is not fresher than this provider's
        // stored one of that slot is a stale replay: it must not overwrite
        // newer data, so the slot is left exactly as it is (like an aired
        // one).
        frozen = (0..n)
            .map(|k| {
                aired[k]
                    || incoming_of[k].is_some_and(|i| {
                        own[k].is_some_and(|o| fresh[i].fetched_at <= o.observation.fetched_at)
                    })
            })
            .collect();

        // Step 3. Only unmatched slots can be contradicted. `merged[i]`:
        // incoming i is matched to a slot that really takes it (matches are
        // only ever with future slots, and frozen ones take nothing). Only
        // such a match counts as "merged into another slot"; a stale replay
        // must not contradict anything.
        // It must also be the same slot as at most 1 + MAX_ABSORBED_SLOTS
        // future slots (see that constant).
        let merged: Vec<bool> = slot_of
            .iter()
            .zip(&same_as)
            .map(|(k, &count)| k.is_some_and(|k| !frozen[k]) && count <= 1 + MAX_ABSORBED_SLOTS)
            .collect();
        contradicted = (0..n)
            .map(|k| {
                incoming_of[k].is_none()
                    && !aired[k]
                    && own[k].is_some_and(|o| {
                        is_contradicted(&slots[k].slot, o, &fresh, &merged, coverage)
                    })
            })
            .collect();
        // Removed: contradicted and nobody else vouches for the slot.
        removed = (0..n)
            .map(|k| contradicted[k] && slots[k].observations.len() == 1)
            .collect();

        // Step 4.
        settled = settle(
            &slots,
            &own,
            &fresh,
            &incoming_of,
            &contradicted,
            &removed,
            &frozen,
            coverage,
            scorer,
            now,
        );

        // Step 5, first half: an observation that is unmatched, or matched
        // only as a stale replay, is attached to a slot this provider has no
        // record of (any more) if it is the same slot as that slot's final
        // range, and is fresher than this provider's own record there (if it
        // still has one; other providers' records do not matter). A later plan would
        // otherwise match it there (this plan changed that slot's times), so
        // without this the plan would not be stable.
        attached = false;
        for (i, o) in fresh.iter().enumerate() {
            // Skip it if its match takes it, or if it is simply a repeat of
            // this provider's own record of that slot (then it is where it
            // belongs, just not newer).
            let settled_here = slot_of[i].is_some_and(|k| {
                !frozen[k]
                    || own[k].is_some_and(|own| {
                        own.observation.start == o.start && own.observation.end == o.end
                    })
            });
            if settled_here {
                continue;
            }
            let r = o.start..o.end;
            let target = (0..n)
                .filter(|&k| {
                    !aired[k]
                        && !removed[k]
                        && incoming_of[k].is_none()
                        && (own[k].is_none() || contradicted[k])
                        && own[k].is_none_or(|own| o.fetched_at > own.observation.fetched_at)
                        && same_slot(&r, &settled.ranges[k])
                })
                // Most overlap first, then earliest slot.
                .max_by(|&x, &y| {
                    overlap_secs(&r, &settled.ranges[x])
                        .cmp(&overlap_secs(&r, &settled.ranges[y]))
                        .then(y.cmp(&x))
                });
            if let Some(k) = target {
                // Release the slot it was a stale replay for.
                if let Some(old) = slot_of[i] {
                    incoming_of[old] = None;
                }
                slot_of[i] = Some(k);
                incoming_of[k] = Some(i);
                attached = true;
            }
        }
        // Safety bound only; see the loop comment.
        if !attached || rounds > fresh.len() + 1 {
            break;
        }
    }
    // Each observation is attached at most once, so the loop always ends
    // with a round that attached nothing; leaving through the safety bound
    // instead would mean `settled` is out of date (a bug).
    debug_assert!(!attached, "merge attach rounds did not settle");

    // Write out removals, superseded observations and updates.
    for k in 0..n {
        let id = slots[k].slot.id;
        if removed[k] {
            plan.removals.push(id);
            continue;
        }
        // Contradicted and not re-attached: drop just this provider's record.
        if contradicted[k] && incoming_of[k].is_none() {
            plan.superseded.push((id, provider.clone()));
        }
        if !settled.touched[k] {
            continue;
        }
        match &settled.fields[k] {
            Some(f) if !fields_match(f, &slots[k].slot) => plan.updates.push((id, f.clone())),
            _ => plan.unchanged += 1,
        }
    }

    // Observation writes for matched (or attached) slots.
    for (k, i) in incoming_of.iter().enumerate() {
        let Some(i) = *i else { continue };
        if frozen[k] || removed[k] {
            continue;
        }
        let (first_seen, changes) = counters(&fresh[i], own[k]);
        let unchanged = own[k].is_some_and(|o| {
            o.observation == fresh[i] && o.first_seen == first_seen && o.changes == changes
        });
        if !unchanged {
            plan.observations.push(ObservationWrite {
                slot: SlotRef::Existing(slots[k].slot.id),
                observation: fresh[i].clone(),
                first_seen,
                changes,
            });
        }
    }

    // Which existing slot every incoming observation ended up with, in the
    // caller's order. Aired slots never get a match (step 2 skips them and
    // step 5 only attaches to future slots), so every pair here is one the
    // planner really considers "the same slot".
    plan.matches = slot_of
        .iter()
        .enumerate()
        .filter_map(|(i, k)| {
            k.map(|k| PlanMatch {
                incoming: origin[i],
                slot: slots[k].slot.id,
            })
        })
        .collect();
    plan.matches.sort_by_key(|m| m.incoming);

    // Step 5, second half: new slots for what is left.
    let mut accepted: Vec<Range<DateTime<Utc>>> = Vec::new();
    for (i, o) in fresh.iter().enumerate() {
        if slot_of[i].is_some() {
            continue; // matched (aired slots never get a match)
        }
        let r = o.start..o.end;
        if o.end <= now && o.start < now - AIRED_INSERT_LOOKBACK {
            continue; // aired too long ago to be worth keeping
        }
        let clashes_slot = (0..n).any(|k| !removed[k] && same_slot(&r, &settled.ranges[k]));
        let clashes_new = accepted.iter().any(|a| same_slot(&r, a));
        if clashes_slot || clashes_new {
            // It is the same slot as something we hold but could not be
            // attached to it (that slot already has this provider's data, or
            // has aired). Dropping it keeps what we have.
            continue;
        }
        let cand = score_candidate(o.clone(), o.fetched_at, 0, coverage, scorer, now);
        plan.inserts.push(NewSlot {
            channel: coverage.channel.clone(),
            fields: build_fields(std::slice::from_ref(&cand), 0),
        });
        plan.observations.push(ObservationWrite {
            slot: SlotRef::New(plan.inserts.len() - 1),
            observation: o.clone(),
            first_seen: o.fetched_at,
            changes: 0,
        });
        accepted.push(r);
    }
    plan
}

/// Step 1: keep only this response's valid observations for the channel,
/// sorted, without repeats of the same slot. Times are cut to whole seconds,
/// because that is what the store keeps: otherwise a re-sent identical
/// response with a sub-second `fetched_at` would look fresher than its own
/// stored copy (a false contradiction) and never compare equal to it.
///
/// Also returns, for every kept observation, its position in `incoming`.
fn clean_incoming(incoming: &[Observation], coverage: &Coverage) -> (Vec<Observation>, Vec<usize>) {
    let mut list: Vec<(usize, Observation)> = incoming
        .iter()
        .enumerate()
        .map(|(n, o)| {
            let mut o = o.clone();
            o.start = o.start.trunc_subsecs(0);
            o.end = o.end.trunc_subsecs(0);
            o.fetched_at = o.fetched_at.trunc_subsecs(0);
            (n, o)
        })
        .filter(|(_, o)| {
            o.provider == coverage.provider && o.channel == coverage.channel && o.end > o.start
        })
        .collect();
    // A stable sort on the observation only: the positions ride along and
    // change nothing about the order or about which repeat is kept.
    list.sort_by(|(_, a), (_, b)| {
        (a.start, a.end, &a.title, a.fetched_at).cmp(&(b.start, b.end, &b.title, b.fetched_at))
    });
    let mut kept: Vec<Observation> = Vec::new();
    let mut origin: Vec<usize> = Vec::new();
    for (n, o) in list {
        if !kept
            .iter()
            .any(|k| same_slot(&(k.start..k.end), &(o.start..o.end)))
        {
            kept.push(o);
            origin.push(n);
        }
    }
    (kept, origin)
}

/// Fill the details `new` lacks from `old`, this same provider's stored copy
/// of the slot, when both have the same title (trimmed, case-insensitive).
/// A text field counts as missing when it is `None` or empty; genres when
/// the list is empty. Times, title and fetch time always stay `new`'s own.
fn carry_forward(new: &mut Observation, old: &Observation) {
    if new.title.trim().to_lowercase() != old.title.trim().to_lowercase() {
        return; // a different programme: nothing carries over
    }
    let fill = |new: &mut Option<String>, old: &Option<String>| {
        if new.as_deref().is_none_or(str::is_empty) && old.as_deref().is_some_and(|v| !v.is_empty())
        {
            *new = old.clone();
        }
    };
    fill(&mut new.subtitle, &old.subtitle);
    fill(&mut new.description, &old.description);
    fill(&mut new.image_url, &old.image_url);
    fill(&mut new.rating, &old.rating);
    if new.season.is_none() {
        new.season = old.season;
    }
    if new.episode.is_none() {
        new.episode = old.episode;
    }
    if new.genres.is_empty() {
        new.genres = old.genres.clone();
    }
}

/// Step 2: one-to-one matching of incoming observations to slots, best pairs
/// first. Returns (slot for each incoming, incoming for each slot).
fn match_incoming(
    fresh: &[Observation],
    slots: &[&SlotWithObservations],
    own: &[Option<&StoredObservation>],
    aired: &[bool],
) -> (Vec<Option<usize>>, Vec<Option<usize>>) {
    // (exact repeat?, overlap fraction, overlap seconds, incoming, slot)
    let mut pairs: Vec<(bool, f64, i64, usize, usize)> = Vec::new();
    for (i, o) in fresh.iter().enumerate() {
        let r = o.start..o.end;
        for (k, s) in slots.iter().enumerate() {
            // Aired slots take no data, so they take part in no match. (An
            // observation that is the same slot as one is still never
            // inserted as a duplicate: step 5 sees the clash and drops it.)
            if aired[k] {
                continue;
            }
            let sr = s.slot.start..s.slot.end;
            let mut fraction = overlap_fraction(&r, &sr);
            let mut secs = overlap_secs(&r, &sr);
            let mut matches = same_slot(&r, &sr);
            let mut exact = false;
            // Also compare with what this provider itself said last time.
            if let Some(own) = own[k] {
                let or = own.observation.start..own.observation.end;
                exact = or == r;
                matches |= same_slot(&r, &or);
                fraction = fraction.max(overlap_fraction(&r, &or));
                secs = secs.max(overlap_secs(&r, &or));
            }
            if matches {
                pairs.push((exact, fraction, secs, i, k));
            }
        }
    }
    // Exact repeats first, then the greatest overlap (seconds), then larger
    // fraction, then the earliest slot, so the order is total.
    pairs.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then(b.2.cmp(&a.2))
            .then(b.1.total_cmp(&a.1))
            .then(a.4.cmp(&b.4))
            .then(a.3.cmp(&b.3))
    });
    let mut slot_of = vec![None; fresh.len()];
    let mut incoming_of = vec![None; slots.len()];
    for (_, _, _, i, k) in pairs {
        if slot_of[i].is_none() && incoming_of[k].is_none() {
            slot_of[i] = Some(k);
            incoming_of[k] = Some(i);
        }
    }
    (slot_of, incoming_of)
}

/// Step 3: has this response explicitly contradicted the provider's own
/// stored observation `own` of `slot`? See `plan_merge` for the rule.
/// Only called for slots no incoming observation was matched to, so an
/// incoming observation with `merged[j]` set belongs to a different (future)
/// slot.
fn is_contradicted(
    slot: &MasterSlot,
    own: &StoredObservation,
    fresh: &[Observation],
    merged: &[bool],
    coverage: &Coverage,
) -> bool {
    let sr = slot.start..slot.end;
    let or = own.observation.start..own.observation.end;
    // The response must have covered the whole slot...
    let covered = coverage.window.start <= slot.start && slot.end <= coverage.window.end;
    covered
        // ...and say something different about part of that time, more
        // recently than what we hold.
        && fresh.iter().enumerate().any(|(i, j)| {
            let jr = j.start..j.end;
            // A different programme: not the same slot as ours...
            let different = !same_slot(&jr, &sr) && !same_slot(&jr, &or);
            // ...or the same slot, but matched to another slot: the provider
            // merged our programme into that one (many-to-one).
            let merged_elsewhere = merged[i];
            j.fetched_at > own.observation.fetched_at
                && overlap_secs(&jr, &sr) > 0
                && (different || merged_elsewhere)
        })
}

/// A scored observation competing to define a slot.
#[derive(Debug, Clone)]
struct Candidate {
    obs: Observation,
    score: f64,
    breakdown: ScoreBreakdown,
}

/// Result of step 4 for every slot (indexed like `slots`).
struct Settled {
    /// Slots whose observations changed in this plan.
    touched: Vec<bool>,
    /// Final time range of every slot (unchanged for untouched slots).
    ranges: Vec<Range<DateTime<Utc>>>,
    /// New content for touched slots; `None` = keep the slot as it is.
    fields: Vec<Option<SlotFields>>,
}

/// Step 4: score and rank the observations of every touched slot, and pick
/// each slot's winner so that no two slots end up "the same slot".
#[allow(clippy::too_many_arguments)]
fn settle(
    slots: &[&SlotWithObservations],
    own: &[Option<&StoredObservation>],
    fresh: &[Observation],
    incoming_of: &[Option<usize>],
    contradicted: &[bool],
    removed: &[bool],
    frozen: &[bool],
    coverage: &Coverage,
    scorer: &dyn Scorer,
    now: DateTime<Utc>,
) -> Settled {
    let n = slots.len();
    let provider = &coverage.provider;

    // A slot is touched when it gets this provider's new observation or loses
    // its old one. Frozen (aired, or matched only by stale data) and removed
    // slots are never touched.
    let touched: Vec<bool> = (0..n)
        .map(|k| !frozen[k] && !removed[k] && (incoming_of[k].is_some() || contradicted[k]))
        .collect();

    // Scores depend only on the observation itself (plus its counters and
    // provider stats), never on other observations: a stored score then
    // stays valid however neighbouring programmes change later. Gaps and
    // overlaps between a response's programmes are judged once per response
    // instead (`score::timeline_integrity`), outside the per-slot score.

    // Ranked candidates of each touched slot.
    let mut ranked: Vec<Vec<Candidate>> = vec![Vec::new(); n];
    for k in (0..n).filter(|&k| touched[k]) {
        let mut cands = Vec::new();
        for o in &slots[k].observations {
            if &o.observation.provider == provider {
                continue; // replaced below, or dropped by a contradiction
            }
            cands.push(score_candidate(
                o.observation.clone(),
                o.first_seen,
                o.changes,
                coverage,
                scorer,
                now,
            ));
        }
        if let Some(i) = incoming_of[k] {
            let (first_seen, changes) = counters(&fresh[i], own[k]);
            cands.push(score_candidate(
                fresh[i].clone(),
                first_seen,
                changes,
                coverage,
                scorer,
                now,
            ));
        }
        cands.sort_by(rank_order);
        ranked[k] = cands;
    }

    // The top-ranked candidate always wins: it supplies title, fields and
    // score. Its start/end are used too, unless that would make the slot "the
    // same slot" as another slot; then the slot keeps its current times, so
    // two master slots can never collide. A slot's current range is always
    // safe, because every other slot only moves to a range that avoids it.
    // Moving a slot can free room for another, so this repeats until a full
    // round moves nothing (each slot moves at most once, so it ends).
    //
    // Removed slots never block (they are skipped below). So when a provider
    // merges several of its programmes into one, the slots it alone backed
    // are removed and the survivor takes the winner's full times. The times
    // are held back only in the residual case: the collision is with a slot
    // that another provider still backs (or one that has aired), which this
    // provider cannot remove, or the winner's times would end the slot at or
    // before `now`. Then title and times may come from different
    // observations until that changes.
    let mut ranges: Vec<Range<DateTime<Utc>>> =
        slots.iter().map(|s| s.slot.start..s.slot.end).collect();
    for _ in 0..MAX_SETTLE_ROUNDS {
        let mut moved = false;
        for k in (0..n).filter(|&k| touched[k]) {
            let Some(winner) = ranked[k].first() else {
                continue;
            };
            let want = winner.obs.start..winner.obs.end;
            // Also never move a slot that has not aired into the past: it
            // would turn into history (which nothing may change) by our own
            // hand, and the next plan would then match differently.
            let free = want.end > now
                && (0..n).all(|m| m == k || removed[m] || !same_slot(&want, &ranges[m]));
            if ranges[k] != want && free {
                ranges[k] = want;
                moved = true;
            }
        }
        if !moved {
            break;
        }
    }

    let fields = (0..n)
        .map(|k| {
            (!ranked[k].is_empty()).then(|| {
                let mut f = build_fields(&ranked[k], 0);
                f.start = ranges[k].start;
                f.end = ranges[k].end;
                f
            })
        })
        .collect();
    Settled {
        touched,
        ranges,
        fields,
    }
}

/// Stability bookkeeping for a provider's new observation `o` of a slot it
/// had observed as `old`: keep the first-seen time, and count a change when
/// start, end or title differ.
fn counters(o: &Observation, old: Option<&StoredObservation>) -> (DateTime<Utc>, u32) {
    match old {
        Some(old) => {
            let p = &old.observation;
            let changed = p.start != o.start || p.end != o.end || p.title != o.title;
            (
                old.first_seen,
                old.changes.saturating_add(u32::from(changed)),
            )
        }
        None => (o.fetched_at, 0),
    }
}

/// Build the `SlotView` for one observation and score it.
fn score_candidate(
    obs: Observation,
    first_seen: DateTime<Utc>,
    changes: u32,
    coverage: &Coverage,
    scorer: &dyn Scorer,
    now: DateTime<Utc>,
) -> Candidate {
    let view = SlotView {
        provider_stats: scorer.provider_stats(&obs.provider, &coverage.channel),
        candidate: obs,
        changes,
        first_seen,
        now,
    };
    let (score, breakdown) = scorer.score(&view);
    Candidate {
        obs: view.candidate,
        // A broken scorer must not poison the stored score (NaN != NaN would
        // also make every later plan think the slot changed).
        score: if score.is_finite() { score } else { 0.0 },
        breakdown,
    }
}

/// Ranking: higher score first; ties go to the fresher fetch, then to the
/// lower provider id, so the result never depends on input order.
fn rank_order(a: &Candidate, b: &Candidate) -> Ordering {
    b.score
        .total_cmp(&a.score)
        .then(b.obs.fetched_at.cmp(&a.obs.fetched_at))
        .then(a.obs.provider.cmp(&b.obs.provider))
}

/// Slot content from the winning candidate, with empty fields filled from the
/// others in rank order (spec 4.11).
fn build_fields(ranked: &[Candidate], winner: usize) -> SlotFields {
    let w = &ranked[winner].obs;
    // The winner first, then everyone else in rank order.
    let order: Vec<&Observation> = std::iter::once(w)
        .chain(
            ranked
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != winner)
                .map(|(_, c)| &c.obs),
        )
        .collect();
    let first = |f: &dyn Fn(&Observation) -> Option<String>| order.iter().find_map(|o| f(o));
    SlotFields {
        start: w.start,
        end: w.end,
        title: w.title.clone(),
        subtitle: first(&|o| o.subtitle.clone()),
        description: first(&|o| o.description.clone()),
        image_url: first(&|o| o.image_url.clone()),
        season: order.iter().find_map(|o| o.season),
        episode: order.iter().find_map(|o| o.episode),
        rating: first(&|o| o.rating.clone()),
        genres: order
            .iter()
            .find(|o| !o.genres.is_empty())
            .map(|o| o.genres.clone())
            .unwrap_or_default(),
        winner: w.provider.clone(),
        score: ranked[winner].score,
        breakdown: ranked[winner].breakdown.clone(),
    }
}

/// Scores (and each breakdown value) that differ by less than this are
/// treated as unchanged: one percent on the 0..1 scale. Reliability counts,
/// the hindsight average and stability move a little on every fetch, so a
/// re-fetch of identical data always scores slightly differently. Without
/// this tolerance every refresh rewrote every slot it touched (thousands a
/// day) and appended a score-history row for each; a score that moved by
/// less than a percent is not worth a write. The stored score may then lag
/// the exact current one by up to this much, which ranking does not care
/// about (the merge still ranks with freshly computed scores).
pub const SCORE_TOLERANCE: f64 = 0.01;

/// Whether a stored slot already has this content: every field equal, and
/// the score and each breakdown value within `SCORE_TOLERANCE`.
fn fields_match(f: &SlotFields, s: &MasterSlot) -> bool {
    let close = |a: f64, b: f64| (a - b).abs() < SCORE_TOLERANCE;
    f.start == s.start
        && f.end == s.end
        && f.title == s.title
        && f.subtitle == s.subtitle
        && f.description == s.description
        && f.image_url == s.image_url
        && f.season == s.season
        && f.episode == s.episode
        && f.rating == s.rating
        && f.genres == s.genres
        && f.winner == s.winner
        && close(f.score, s.score)
        && f.breakdown.0.len() == s.breakdown.0.len()
        && f.breakdown
            .0
            .iter()
            .all(|(k, v)| s.breakdown.0.get(k).is_some_and(|w| close(*v, *w)))
}
