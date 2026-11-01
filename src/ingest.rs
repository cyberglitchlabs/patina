//! Master-guide ingest: how one provider's clean response gets into the
//! master guide.
//!
//! Two entry points, in the order the scheduler uses them for a response:
//!
//! 1. [`store_channels`], once per response: remember the provider's own
//!    channel list and create or refresh the canonical channels that slots
//!    refer to. Returns the channels the response listed.
//! 2. [`Ingest::channel`], once per listed channel: given ONE provider's
//!    successful response for ONE channel, it loads every stored slot the
//!    response could touch, plans the merge (`merge::plan_merge`), applies
//!    it in one transaction, writes score history for the slots it inserted
//!    or updated, and returns an [`Ingested`] outcome: the counts, whether
//!    anything was stored, the score-history rows written and the hindsight
//!    samples this response yields.
//!
//! What the caller does NOT need to know (it all lives here):
//!
//! * **The range rule.** The planner is only correct if it sees every slot
//!   the response could touch: the window plus every incoming programme's
//!   span (`existing_range`).
//! * **Slot ids.** Score history needs the id of every inserted slot;
//!   `Store::apply_merge` returns them (`MergeApplied`), so nothing is read
//!   back.
//! * **Matching.** Hindsight (spec 8.5) compares a provider's earlier
//!   listing of a slot with its later one. Which stored slot a new listing
//!   is "the same slot" as is the planner's answer (`MergePlan::matches`);
//!   there is no second matching rule here.
//!
//! **Fail closed.** Any store error returns `Err` and the outcome is lost
//! with it: no samples (a merge that was not stored must not count), no
//! feed-cache bump. The transaction in `apply_merge` means the guide is
//! unchanged. The caller treats the channel as a failed merge.
//!
//! **One at a time.** `Store::apply_merge` does not notice that the data
//! changed after the plan was computed. Callers must therefore never run
//! two `Ingest::channel` calls for the same channel at the same time; the
//! scheduler is a single task and ingests channels one after the other.

use std::collections::BTreeSet;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use chrono::{DateTime, TimeDelta, Utc};

use crate::merge::{
    Coverage, MergeStats, PlanMatch, Scorer, SlotFields, SlotWithObservations, plan_merge,
};
use crate::model::{
    CanonicalChannel, ChannelId, Observation, ProviderId, SlotId, canonical_channel,
};
use crate::provider::ProviderChannel;
use crate::score::hindsight_sample;
use crate::store::{ScoreHistoryRow, Store};

/// A hindsight sample is only taken when the stored observation is at least
/// this much older than the new one: comparing two fetches minutes apart
/// measures nothing (and the same data merged twice in one tick, from two
/// overlapping windows, must not count twice).
pub const HINDSIGHT_MIN_AGE: TimeDelta = TimeDelta::hours(1);

/// Store the provider's channel list of one response (keyed by its own
/// channel ids, for matching and overrides) and the canonical channels its
/// slots will refer to. Returns the ids of the canonical channels the
/// response listed: exactly the channels that may then be ingested.
pub async fn store_channels(
    store: &dyn Store,
    provider: &ProviderId,
    lineup: &str,
    channels: &[ProviderChannel],
    now: DateTime<Utc>,
) -> Result<BTreeSet<ChannelId>> {
    store
        .provider_channels_upsert(provider, lineup, channels, now)
        .await
        .context("cannot store provider channels")?;
    let canonical: Vec<CanonicalChannel> = channels.iter().map(canonical_channel).collect();
    store
        .upsert_channels(&canonical)
        .await
        .context("cannot store canonical channels")?;
    Ok(canonical.into_iter().map(|c| c.id).collect())
}

/// One provider's SUCCESSFUL response (HTTP 200, parsed without error) for
/// one channel the response listed. A failed, skipped or unparseable
/// response, or a channel the response did not list, is never ingested:
/// it says nothing, so nothing may change.
#[derive(Debug, Clone)]
pub struct ChannelResponse<'a> {
    pub provider: &'a ProviderId,
    pub channel: &'a ChannelId,
    /// The time window the request asked for and the response answered.
    pub window: Range<DateTime<Utc>>,
    /// The response's programmes for this channel, each with its own
    /// `fetched_at`. May be empty (a listed channel with no programmes):
    /// then nothing changes.
    pub observations: &'a [Observation],
}

/// What ingesting one channel response did. Only returned when everything
/// was stored (or there was nothing to store).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Ingested {
    /// What the merge changed (all zero when nothing changed).
    pub stats: MergeStats,
    /// True when a plan was written to the store (and the feed cache was
    /// told). False when the response changed nothing.
    pub applied: bool,
    /// Hindsight samples (spec 8.5), in the order of the response's
    /// observations. The caller folds them into the running averages; they
    /// are only handed out once the merge is stored, so a merge that fails
    /// and is retried can never count the same sample twice.
    pub samples: Vec<f64>,
    /// The score-history rows written: one per inserted or updated slot,
    /// none for unchanged ones. Empty when writing them failed (history is
    /// for later analysis only, so that is logged, not an error).
    pub history: Vec<ScoreHistoryRow>,
}

/// Everything ingest needs besides the response itself.
pub struct Ingest<'a> {
    pub store: &'a dyn Store,
    /// Ranks the observations. In production a `score::WeightedScorer` over
    /// the accuracy snapshot loaded once per tick.
    pub scorer: &'a (dyn Scorer + Sync),
    /// Shared with the XMLTV feed cache: bumped right after every
    /// successful apply, so a fresh merge is visible in the feeds at once
    /// instead of after the cache's TTL. `None` when no feeds are served.
    pub feed_generation: Option<&'a AtomicU64>,
}

impl Ingest<'_> {
    /// Merge one channel response into the master guide: load -> plan ->
    /// apply -> history, and the hindsight samples. See the module docs.
    /// `now` is the planning time (aired slots, `end <= now`, never change).
    pub async fn channel(
        &self,
        response: ChannelResponse<'_>,
        now: DateTime<Utc>,
    ) -> Result<Ingested> {
        let incoming = response.observations;
        if incoming.is_empty() {
            // Listed with no programmes: the plan would be empty (absence
            // changes nothing), so skip the reads.
            return Ok(Ingested::default());
        }
        let coverage = Coverage {
            provider: response.provider.clone(),
            channel: response.channel.clone(),
            window: response.window,
            // Only listed channels are ever ingested.
            channel_present: true,
        };

        let (from, to) = existing_range(&coverage, incoming);
        let existing = self
            .store
            .slots_with_observations(&coverage.channel, from, to)
            .await?;
        let plan = plan_merge(&existing, incoming, &coverage, self.scorer, now);

        // Computed now, but only handed out below, after the plan is stored.
        let samples = hindsight(&existing, &plan.matches, incoming, &coverage.provider, now);
        if plan.is_empty() {
            return Ok(Ingested {
                samples,
                ..Ingested::default()
            });
        }

        // Score history for every slot this plan updates, then (once the
        // store has given them ids) every slot it inserts.
        let row = |slot: SlotId, f: &SlotFields| ScoreHistoryRow {
            slot,
            provider: f.winner.clone(),
            channel: coverage.channel.clone(),
            scored_at: now,
            score: f.score,
            breakdown: f.breakdown.clone(),
        };
        let mut history: Vec<ScoreHistoryRow> =
            plan.updates.iter().map(|(id, f)| row(*id, f)).collect();
        let inserts: Vec<SlotFields> = plan.inserts.iter().map(|n| n.fields.clone()).collect();

        let applied = self.store.apply_merge(plan).await?;
        // Tell the feed cache its rendered bytes are out of date.
        if let Some(generation) = self.feed_generation {
            generation.fetch_add(1, Ordering::SeqCst);
        }
        // `inserted_ids[i]` is the id of `inserts[i]` (same order).
        history.extend(
            inserts
                .iter()
                .zip(&applied.inserted_ids)
                .map(|(f, id)| row(*id, f)),
        );
        if let Err(e) = self.store.score_history_append(&history).await {
            // History is for later analysis; losing a batch is not worth
            // failing the window (the merge itself is stored).
            tracing::warn!(channel = %coverage.channel.0, error = %format!("{e:#}"), "cannot store score history");
            history.clear();
        }
        Ok(Ingested {
            stats: applied.stats,
            applied: true,
            samples,
            history,
        })
    }
}

/// The time range `slots_with_observations` must be asked for before
/// planning: everything the window or any incoming observation touches. The
/// plan is only correct if every slot that could match is passed in.
fn existing_range(coverage: &Coverage, incoming: &[Observation]) -> (DateTime<Utc>, DateTime<Utc>) {
    let mut from = coverage.window.start;
    let mut to = coverage.window.end;
    for o in incoming {
        from = from.min(o.start);
        to = to.max(o.end);
    }
    (from, to)
}

/// Hindsight samples (spec 8.5): the same provider's earlier listing of a
/// slot compared with this later one, closer to air time. For every
/// incoming observation the planner matched to an existing slot (each slot
/// takes at most one, so each is used once): only when the slot has not
/// started yet (`start > now`), the provider has a stored observation of
/// it, and that one is at least `HINDSIGHT_MIN_AGE` older. In incoming
/// order. The new listing is the one exactly as the provider sent it.
fn hindsight(
    existing: &[SlotWithObservations],
    matches: &[PlanMatch],
    incoming: &[Observation],
    provider: &ProviderId,
    now: DateTime<Utc>,
) -> Vec<f64> {
    matches
        .iter()
        .filter_map(|m| {
            let slot = existing.iter().find(|s| s.slot.id == m.slot)?;
            if slot.slot.start <= now {
                return None; // on air or over: nothing left to forecast
            }
            let stored = slot
                .observations
                .iter()
                .find(|o| &o.observation.provider == provider)?;
            let new = &incoming[m.incoming];
            (new.fetched_at - stored.observation.fetched_at >= HINDSIGHT_MIN_AGE)
                .then(|| hindsight_sample(&stored.observation, new))
        })
        .collect()
}
