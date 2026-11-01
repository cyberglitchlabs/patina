//! Helpers for merge tests: a stub scorer, observation builders, and `Sim`, an
//! in-memory stand-in for the database that applies a `MergePlan` the same way
//! `Store::apply_merge` does (the store contract checks the real backends).

use std::collections::HashMap;
use std::ops::Range;

use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use patina::merge::{
    Coverage, MergePlan, NewSlot, Scorer, SlotFields, SlotRef, SlotView, SlotWithObservations,
    StoredObservation, plan_merge,
};
use patina::model::{ChannelId, MasterSlot, Observation, ProviderId, ScoreBreakdown, SlotId};

/// Scores by provider name only (unknown providers get 0.5). The breakdown
/// records the score under "stub" so tests can see it was stored.
#[derive(Debug, Clone, Default)]
pub struct StubScorer(pub HashMap<String, f64>);

impl StubScorer {
    pub fn new(scores: &[(&str, f64)]) -> Self {
        StubScorer(scores.iter().map(|(p, s)| (p.to_string(), *s)).collect())
    }
}

impl Scorer for StubScorer {
    fn score(&self, view: &SlotView) -> (f64, ScoreBreakdown) {
        let s = *self.0.get(&view.candidate.provider.0).unwrap_or(&0.5);
        let mut b = ScoreBreakdown::default();
        b.0.insert("stub".into(), s);
        (s, b)
    }
}

/// 2026-11-01 00:00 UTC plus `min` minutes.
pub fn t(min: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 1, 0, 0, 0).unwrap() + TimeDelta::minutes(min)
}

pub fn ch() -> ChannelId {
    ChannelId("WCCODT.us".into())
}

pub fn pid(p: &str) -> ProviderId {
    ProviderId(p.into())
}

/// A bare observation: provider, minutes from base, title, fetch minute.
pub fn obs(provider: &str, start: i64, end: i64, title: &str, fetched: i64) -> Observation {
    Observation {
        provider: pid(provider),
        channel: ch(),
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
        fetched_at: t(fetched),
    }
}

/// A successful response from `provider` that listed the channel and covered
/// minutes [from, to).
pub fn cover(provider: &str, from: i64, to: i64) -> Coverage {
    Coverage {
        provider: pid(provider),
        channel: ch(),
        window: t(from)..t(to),
        channel_present: true,
    }
}

pub fn range(s: &MasterSlot) -> Range<DateTime<Utc>> {
    s.start..s.end
}

fn master_from(id: SlotId, channel: ChannelId, f: &SlotFields, at: DateTime<Utc>) -> MasterSlot {
    MasterSlot {
        id,
        channel,
        start: f.start,
        end: f.end,
        title: f.title.clone(),
        subtitle: f.subtitle.clone(),
        description: f.description.clone(),
        image_url: f.image_url.clone(),
        season: f.season,
        episode: f.episode,
        rating: f.rating.clone(),
        genres: f.genres.clone(),
        winner: f.winner.clone(),
        score: f.score,
        breakdown: f.breakdown.clone(),
        updated_at: at,
    }
}

/// In-memory master guide.
#[derive(Debug, Clone, Default)]
pub struct Sim {
    pub slots: Vec<SlotWithObservations>,
    next_id: i64,
}

impl Sim {
    pub fn plan(
        &self,
        incoming: &[Observation],
        coverage: &Coverage,
        scorer: &dyn Scorer,
        now: DateTime<Utc>,
    ) -> MergePlan {
        plan_merge(&self.slots, incoming, coverage, scorer, now)
    }

    /// Plan and apply in one go; returns the plan.
    pub fn merge(
        &mut self,
        incoming: &[Observation],
        coverage: &Coverage,
        scorer: &dyn Scorer,
        now: DateTime<Utc>,
    ) -> MergePlan {
        let plan = self.plan(incoming, coverage, scorer, now);
        self.apply(&plan);
        plan
    }

    pub fn get(&self, id: SlotId) -> Option<&SlotWithObservations> {
        self.slots.iter().find(|s| s.slot.id == id)
    }

    pub fn titles(&self) -> Vec<String> {
        self.slots.iter().map(|s| s.slot.title.clone()).collect()
    }

    /// Mirrors `Store::apply_merge`: removals, superseded, updates, inserts
    /// (an insert whose channel+start already exists reuses that slot), then
    /// observation upserts on (slot, provider).
    pub fn apply(&mut self, plan: &MergePlan) {
        self.slots.retain(|s| !plan.removals.contains(&s.slot.id));
        for (id, provider) in &plan.superseded {
            if let Some(s) = self.slots.iter_mut().find(|s| s.slot.id == *id) {
                s.observations
                    .retain(|o| &o.observation.provider != provider);
            }
        }
        for (id, f) in &plan.updates {
            if let Some(s) = self.slots.iter_mut().find(|s| s.slot.id == *id) {
                s.slot = master_from(*id, s.slot.channel.clone(), f, plan.planned_at);
            }
        }
        let mut new_ids = Vec::new();
        for NewSlot { channel, fields } in &plan.inserts {
            let clash = self
                .slots
                .iter()
                .find(|s| &s.slot.channel == channel && s.slot.start == fields.start);
            let id = match clash {
                Some(s) => s.slot.id,
                None => {
                    self.next_id += 1;
                    let id = SlotId(self.next_id);
                    self.slots.push(SlotWithObservations {
                        slot: master_from(id, channel.clone(), fields, plan.planned_at),
                        observations: vec![],
                    });
                    id
                }
            };
            new_ids.push(id);
        }
        for w in &plan.observations {
            let id = match w.slot {
                SlotRef::Existing(id) => id,
                SlotRef::New(i) => new_ids[i],
            };
            let s = self
                .slots
                .iter_mut()
                .find(|s| s.slot.id == id)
                .expect("observation for a missing slot");
            let mut o = w.observation.clone();
            o.channel = s.slot.channel.clone();
            let rec = StoredObservation {
                observation: o,
                first_seen: w.first_seen,
                changes: w.changes,
            };
            match s
                .observations
                .iter_mut()
                .find(|x| x.observation.provider == w.observation.provider)
            {
                Some(x) => *x = rec,
                None => s.observations.push(rec),
            }
        }
        for s in &mut self.slots {
            s.observations
                .sort_by(|a, b| a.observation.provider.cmp(&b.observation.provider));
        }
        self.slots.sort_by_key(|s| s.slot.start);
    }
}
