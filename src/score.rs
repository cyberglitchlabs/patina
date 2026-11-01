//! Scoring: how much to trust one provider's observation of one programme
//! (spec 4.10, 4.11, 8.5).
//!
//! The score is a transparent weighted average of seven *signals*. Each
//! signal is a small pure function that returns a number from 0.0 (bad) to
//! 1.0 (good), never NaN. The merge planner (`merge::plan_merge`) calls the
//! scorer through the `Scorer` trait; the highest score wins a slot. Every
//! signal value (unweighted) is kept in the slot's `ScoreBreakdown`, so the
//! status page can show *why* a source won.
//!
//! Everything here is deterministic: no clock reads (the planning time comes
//! in `SlotView::now`), no randomness, and breakdowns use a sorted
//! `BTreeMap`. Per-provider history (hindsight accuracy, fetch reliability)
//! comes from an `AccuracySource`, normally an `AccuracySnapshot` that the
//! scheduler loads from the store before each merge pass, so the scorer
//! itself never does I/O.
//!
//! "Unknown" data scores the neutral 0.5, never the maximum: a provider we
//! know nothing about should neither win nor lose because of that.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, TimeDelta, Utc};

use crate::config::Weights;
use crate::merge::{ProviderStats, Scorer, SlotView};
use crate::model::{ChannelId, Observation, ProviderId, ScoreBreakdown};
use crate::store::{AccuracyRow, ReliabilityRow, Store};

/// The name of every signal, as stored in `ScoreBreakdown` (sorted).
pub const SIGNAL_NAMES: [&str; 7] = [
    "agreement",
    "completeness",
    "freshness",
    "hindsight_accuracy",
    "reliability",
    "stability",
    "timeline_sanity",
];

/// The value used when we simply do not know.
const NEUTRAL: f64 = 0.5;

/// Smoothing factor of the hindsight running average: each new sample
/// counts 20%, the history 80%.
const EMA_ALPHA: f64 = 0.2;

/// Force a value into 0..1. NaN (which `clamp` would pass through) becomes
/// the neutral 0.5, so a bad input can never poison a score.
fn unit(x: f64) -> f64 {
    if x.is_nan() {
        NEUTRAL
    } else {
        x.clamp(0.0, 1.0)
    }
}

/// One provider's hindsight accuracy on one channel: a running average of
/// samples (see `hindsight_sample`), and how many samples it holds.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct ProviderChannelAccuracy {
    pub ema: f64,
    pub samples: u32,
}

impl ProviderChannelAccuracy {
    /// The average after one more sample. The very first sample seeds the
    /// average with itself (otherwise the starting value 0 would drag it
    /// down for many fetches); later ones use `ema_update`.
    pub fn record(&self, sample: f64) -> Self {
        if self.samples == 0 {
            ProviderChannelAccuracy {
                ema: unit(sample),
                samples: 1,
            }
        } else {
            ProviderChannelAccuracy {
                ema: ema_update(self.ema, sample),
                samples: self.samples.saturating_add(1),
            }
        }
    }
}

/// Exponential moving average step: `0.2 * sample + 0.8 * old`. Both inputs
/// are forced into 0..1 first (NaN counts as 0.5).
pub fn ema_update(old: f64, sample: f64) -> f64 {
    unit(EMA_ALPHA * unit(sample) + (1.0 - EMA_ALPHA) * unit(old))
}

/// The stored row after one more hindsight sample for (provider, channel).
/// `old` is the current row, if there is one. Task 10 calls this when a slot
/// is re-fetched closer to air time and upserts the result.
pub fn next_accuracy_row(
    old: Option<&AccuracyRow>,
    provider: &ProviderId,
    channel: &ChannelId,
    sample: f64,
    at: DateTime<Utc>,
) -> AccuracyRow {
    let current = old
        .map(|r| ProviderChannelAccuracy {
            ema: r.ema,
            samples: r.samples,
        })
        .unwrap_or_default();
    let next = current.record(sample);
    AccuracyRow {
        provider: provider.clone(),
        channel: channel.clone(),
        ema: next.ema,
        samples: next.samples,
        updated_at: at,
    }
}

/// Up to this much total start+end difference (seconds) counts as "the same
/// times" when comparing two observations.
const SAME_TIME_TOLERANCE_SECS: f64 = 60.0;
/// At this much total start+end difference (30 minutes) the times no longer
/// agree at all.
const NO_AGREEMENT_SECS: f64 = 1800.0;
/// The most a pair with different titles can score.
const TITLE_CHANGE_CAP: f64 = 0.25;

/// Unix time in seconds as a wide integer, so differences of extreme dates
/// cannot overflow.
fn secs(t: DateTime<Utc>) -> i128 {
    i128::from(t.timestamp())
}

/// A title reduced to lowercase words of letters and digits, so "The News!"
/// and "the  news" compare equal.
fn normalize_title(title: &str) -> String {
    title
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
        .collect::<Vec<_>>()
        .join(" ")
}

/// How well two observations of the same airing agree, 0..1.
///
/// In plain words: add up how far the start moved and how far the end moved.
/// Up to 60 seconds in total is a perfect 1.0; from there the value falls in
/// a straight line to 0.0 at 30 minutes in total. If the (normalized) titles
/// differ, the result is additionally multiplied by 0.25, so a changed title
/// scores at most 0.25. Symmetric: swapping the two gives the same value.
///
/// Used as the hindsight sample (an earlier fetch compared with a later one
/// closer to air time; spec 8.5) and by the agreement signal.
pub fn hindsight_sample(earlier: &Observation, later: &Observation) -> f64 {
    let shift = (secs(earlier.start) - secs(later.start)).abs()
        + (secs(earlier.end) - secs(later.end)).abs();
    let shift = shift as f64;
    let times = if shift <= SAME_TIME_TOLERANCE_SECS {
        1.0
    } else {
        1.0 - (shift - SAME_TIME_TOLERANCE_SECS) / (NO_AGREEMENT_SECS - SAME_TIME_TOLERANCE_SECS)
    };
    let titles = if normalize_title(&earlier.title) == normalize_title(&later.title) {
        1.0
    } else {
        TITLE_CHANGE_CAP
    };
    unit(unit(times) * titles)
}

/// Length of one "observation period" for the stability signal.
const STABILITY_PERIOD_SECS: i64 = 6 * 3600;

/// How many times, roughly, a provider has reported a slot: one plus the
/// number of whole 6-hour periods between `first_seen` and this fetch. We do
/// not store a fetch count per observation, so this estimates it from time
/// (6 hours is the fetch window length; near-term slots are re-fetched more
/// often, which makes this an underestimate, i.e. stricter).
///
/// Returns 0 (meaning "unknown") when `first_seen` is at or before 1970:
/// rows written before migration 0004 have `first_seen = 0`.
pub fn stability_observations(first_seen: DateTime<Utc>, fetched_at: DateTime<Utc>) -> u32 {
    if first_seen.timestamp() <= 0 {
        return 0;
    }
    let span = (fetched_at.timestamp() - first_seen.timestamp()).max(0);
    let periods = span / STABILITY_PERIOD_SECS;
    u32::try_from(periods).unwrap_or(u32::MAX).saturating_add(1)
}

/// The signals, each a pure function returning 0.0..=1.0 (never NaN).
pub mod signals {
    use super::*;

    /// Half-life of freshness for programmes airing soon: 6 hours.
    const FRESHNESS_HALF_LIFE_SECS: f64 = 6.0 * 3600.0;

    /// **Freshness**: how recent the data is, relative to how far away the
    /// programme is.
    ///
    /// `age` is how long ago the observation was fetched (`now - fetched_at`)
    /// and `horizon` how long until the programme starts (`start - now`).
    /// The value halves every "half-life": 6 hours, or a quarter of the time
    /// until air if that is longer (a listing for next week does not go
    /// stale as fast as one for tonight). Formula: `0.5 ^ (age / half_life)`.
    /// A negative age (clock skew) counts as just fetched (1.0).
    pub fn freshness(age: TimeDelta, horizon: TimeDelta) -> f64 {
        let age = age.num_seconds().max(0) as f64;
        let horizon = horizon.num_seconds().max(0) as f64;
        let half_life = FRESHNESS_HALF_LIFE_SECS.max(horizon / 4.0);
        unit(0.5f64.powf(age / half_life))
    }

    /// Text that is present and not just spaces.
    fn present(s: &Option<String>) -> bool {
        s.as_deref().is_some_and(|x| !x.trim().is_empty())
    }

    /// **Completeness**: the share of useful fields the observation fills,
    /// with these points out of 100: title 30, description 25, episode
    /// information (season or episode number, or an episode subtitle) 20,
    /// image 15, rating 10. Blank text counts as missing.
    pub fn completeness(o: &Observation) -> f64 {
        let mut points = 0u32;
        if !o.title.trim().is_empty() {
            points += 30;
        }
        if present(&o.description) {
            points += 25;
        }
        if o.season.is_some() || o.episode.is_some() || present(&o.subtitle) {
            points += 20;
        }
        if present(&o.image_url) {
            points += 15;
        }
        if present(&o.rating) {
            points += 10;
        }
        unit(f64::from(points) / 100.0)
    }

    /// Shortest and longest plausible programme, in seconds.
    const MIN_PLAUSIBLE_SECS: i128 = 60;
    const MAX_PLAUSIBLE_SECS: i128 = 8 * 3600;
    /// Whether a time sits exactly on a whole minute.
    fn on_minute(t: DateTime<Utc>) -> bool {
        t.timestamp().rem_euclid(60) == 0 && t.timestamp_subsec_nanos() == 0
    }

    /// **Timeline sanity**: is this programme's own time range plausible?
    /// It looks at the observation alone, never at other programmes, so a
    /// stored score stays valid however neighbouring programmes change later
    /// (gaps and overlaps in a whole response are reported separately by
    /// `timeline_integrity`). Starts at 1.0 and multiplies in penalties:
    /// * zero or negative length: 0.0 straight away;
    /// * shorter than 1 minute or longer than 8 hours: x0.3;
    /// * a start or end not on a whole minute: x0.95 each (a mild hint of
    ///   bad data; listings are minute-aligned).
    pub fn timeline_sanity(this: &Range<DateTime<Utc>>) -> f64 {
        let duration = secs(this.end) - secs(this.start);
        if duration <= 0 {
            return 0.0;
        }
        let mut v = 1.0;
        if !(MIN_PLAUSIBLE_SECS..=MAX_PLAUSIBLE_SECS).contains(&duration) {
            v *= 0.3;
        }
        if !on_minute(this.start) {
            v *= 0.95;
        }
        if !on_minute(this.end) {
            v *= 0.95;
        }
        unit(v)
    }

    /// **Agreement**: how well other providers' observations of the same
    /// slot agree with this one: the average `hindsight_sample` against each
    /// of them. Observations from the same provider are ignored. With no
    /// other provider it is the neutral 0.5 (in v1, with one provider, it is
    /// always 0.5 and its default weight is 0).
    pub fn agreement(this: &Observation, others: &[Observation]) -> f64 {
        let values: Vec<f64> = others
            .iter()
            .filter(|o| o.provider != this.provider)
            .map(|o| hindsight_sample(this, o))
            .collect();
        if values.is_empty() {
            return NEUTRAL;
        }
        unit(values.iter().sum::<f64>() / values.len() as f64)
    }

    /// **Stability**: how rarely the provider changed its story (start, end
    /// or title) for this slot: `1 - min(1, changes / observations)`. With
    /// no known history (`observations == 0`) it is the neutral 0.5.
    pub fn stability(changes: u32, observations: u32) -> f64 {
        if observations == 0 {
            return NEUTRAL;
        }
        unit(1.0 - (f64::from(changes) / f64::from(observations)).min(1.0))
    }

    /// **Hindsight accuracy**: the provider's running average on this
    /// channel (how well its earlier listings matched its later ones), or the
    /// neutral 0.5 before the first sample.
    pub fn hindsight_accuracy(acc: &ProviderChannelAccuracy) -> f64 {
        if acc.samples == 0 {
            return NEUTRAL;
        }
        unit(acc.ema)
    }

    /// **Reliability**: share of recent fetches that succeeded, smoothed as
    /// if we had seen one extra success and one extra failure (Laplace):
    /// `(success + 1) / (total + 2)`. No data gives 0.5; a few failures do
    /// not crash the value to 0. `success` is capped at `total`.
    pub fn reliability(success: u32, total: u32) -> f64 {
        let success = success.min(total);
        unit((f64::from(success) + 1.0) / (f64::from(total) + 2.0))
    }
}

/// Where the scorer gets per-provider history from. Synchronous on purpose:
/// the scorer stays pure; the caller loads a fresh `AccuracySnapshot` before
/// each merge pass.
pub trait AccuracySource: Send + Sync {
    fn stats(&self, provider: &ProviderId, channel: &ChannelId) -> Option<ProviderStats>;
}

/// An in-memory copy of the stored accuracy and reliability tables.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AccuracySnapshot {
    accuracy: BTreeMap<(ProviderId, ChannelId), ProviderChannelAccuracy>,
    /// provider -> (success, total)
    reliability: BTreeMap<ProviderId, (u32, u32)>,
}

impl AccuracySnapshot {
    /// Build from stored rows (as returned by the `Store` methods).
    pub fn from_rows(accuracy: &[AccuracyRow], reliability: &[ReliabilityRow]) -> Self {
        AccuracySnapshot {
            accuracy: accuracy
                .iter()
                .map(|r| {
                    let acc = ProviderChannelAccuracy {
                        ema: r.ema,
                        samples: r.samples,
                    };
                    ((r.provider.clone(), r.channel.clone()), acc)
                })
                .collect(),
            reliability: reliability
                .iter()
                .map(|r| (r.provider.clone(), (r.success, r.total)))
                .collect(),
        }
    }

    /// Read both tables from the store.
    pub async fn load(store: &dyn Store) -> Result<Self> {
        let accuracy = store.provider_accuracy_load_all().await?;
        let reliability = store.provider_reliability_load_all().await?;
        Ok(Self::from_rows(&accuracy, &reliability))
    }

    /// The raw running average for (provider, channel); zero samples if none.
    pub fn accuracy(&self, provider: &ProviderId, channel: &ChannelId) -> ProviderChannelAccuracy {
        self.accuracy
            .get(&(provider.clone(), channel.clone()))
            .copied()
            .unwrap_or_default()
    }

    /// The stats the scorer uses, as 0..1 signal values. `None` when nothing
    /// at all is known about the provider on this channel; a missing half
    /// (say reliability known, accuracy not) is filled with neutral 0.5.
    pub fn get(&self, provider: &ProviderId, channel: &ChannelId) -> Option<ProviderStats> {
        let acc = self.accuracy.get(&(provider.clone(), channel.clone()));
        let rel = self.reliability.get(provider);
        if acc.is_none() && rel.is_none() {
            return None;
        }
        Some(ProviderStats {
            reliability: rel.map_or(NEUTRAL, |&(s, t)| signals::reliability(s, t)),
            hindsight: signals::hindsight_accuracy(&acc.copied().unwrap_or_default()),
        })
    }
}

impl AccuracySource for AccuracySnapshot {
    fn stats(&self, provider: &ProviderId, channel: &ChannelId) -> Option<ProviderStats> {
        self.get(provider, channel)
    }
}

/// The real scorer: the weighted average of all seven signals.
pub struct WeightedScorer {
    /// Weights in `SIGNAL_NAMES` order, already scaled so the largest is 1.
    weights: [f64; 7],
    accuracy: Arc<dyn AccuracySource>,
}

impl WeightedScorer {
    /// `weights` normally come from validated config. As a safety net, a
    /// negative or non-finite weight is treated as 0, and an all-zero set
    /// falls back to the defaults.
    pub fn new(weights: Weights, accuracy: Arc<dyn AccuracySource>) -> Self {
        let in_order = |w: &Weights| {
            [
                w.agreement,
                w.completeness,
                w.freshness,
                w.hindsight_accuracy,
                w.reliability,
                w.stability,
                w.timeline_sanity,
            ]
        };
        let clean = |ws: [f64; 7]| ws.map(|w| if w.is_finite() && w > 0.0 { w } else { 0.0 });
        let mut ws = clean(in_order(&weights));
        let mut max = ws.iter().copied().fold(0.0, f64::max);
        if max <= 0.0 {
            ws = clean(in_order(&Weights::default()));
            max = ws.iter().copied().fold(0.0, f64::max);
        }
        // Dividing by the largest weight first keeps the later sum finite
        // even for absurd weights like 1e308 (whose plain sum is infinity).
        WeightedScorer {
            weights: ws.map(|w| w / max),
            accuracy,
        }
    }
}

impl Scorer for WeightedScorer {
    /// score = sum(weight * signal) / sum(weight), so it stays in 0..1 for
    /// any weights; with the defaults (which add up to 1) it is simply the
    /// weighted sum. The breakdown holds the unweighted signal values.
    fn score(&self, view: &SlotView) -> (f64, ScoreBreakdown) {
        let c = &view.candidate;
        let stats = view.provider_stats;
        // Same order as SIGNAL_NAMES.
        let values = [
            // The SlotView carries no other providers' observations, so in v1
            // agreement is the neutral value (weight 0 by default).
            signals::agreement(c, &[]),
            signals::completeness(c),
            // Age from the fetch time, never from first_seen (which may be
            // unknown).
            signals::freshness(view.now - c.fetched_at, c.start - view.now),
            stats.map_or(NEUTRAL, |s| unit(s.hindsight)),
            stats.map_or(NEUTRAL, |s| unit(s.reliability)),
            signals::stability(
                view.changes,
                stability_observations(view.first_seen, c.fetched_at),
            ),
            signals::timeline_sanity(&(c.start..c.end)),
        ];
        let total: f64 = self.weights.iter().sum();
        let weighted: f64 = self.weights.iter().zip(values).map(|(w, v)| w * v).sum();
        let breakdown = SIGNAL_NAMES
            .iter()
            .zip(values)
            .map(|(name, v)| (name.to_string(), v))
            .collect();
        (unit(weighted / total), ScoreBreakdown(breakdown))
    }

    fn provider_stats(&self, provider: &ProviderId, channel: &ChannelId) -> Option<ProviderStats> {
        self.accuracy.stats(provider, channel)
    }
}

/// Gaps and overlaps in ONE provider response, per channel. This is a check
/// of the response as a whole and is deliberately NOT part of any slot's
/// score: a per-slot score must depend only on that observation, or it would
/// go stale whenever a neighbouring programme changes. The scheduler logs and
/// exposes it, and may fold a bad result into the provider's reliability.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct TimelineReport {
    /// Distinct channels in the response.
    pub channels: usize,
    /// Sum over channels of the response's own span (first start to last
    /// end), in minutes.
    pub observed_minutes: f64,
    /// Time inside those spans that no programme covers, in minutes.
    pub gap_minutes: f64,
    /// Time covered by more than one programme, in minutes (counted once
    /// per extra programme covering it).
    pub overlap_minutes: f64,
    /// `gap_minutes / observed_minutes`, 0..1 (0 when nothing was observed).
    pub gap_fraction: f64,
    /// `overlap_minutes / observed_minutes`, 0..1 (0 when nothing observed).
    pub overlap_fraction: f64,
}

/// Build the `TimelineReport` for one response's observations (any order).
/// Pure; never NaN.
///
/// Programmes with `end <= start` (zero-length or reversed) are junk and are
/// ignored entirely, before anything else: they do not start or end a
/// channel's span, and they make no gap or overlap. A channel whose
/// programmes are all junk contributes nothing at all, and is not counted in
/// `channels` either (`channels` counts channels with at least one real
/// programme).
pub fn timeline_integrity(observations: &[Observation]) -> TimelineReport {
    let mut sorted: Vec<&Observation> = observations.iter().filter(|o| o.end > o.start).collect();
    sorted.sort_by(|a, b| (&a.channel, a.start, a.end).cmp(&(&b.channel, b.start, b.end)));

    let mut report = TimelineReport::default();
    let (mut observed, mut gap, mut overlap) = (0i128, 0i128, 0i128);
    let mut i = 0;
    while i < sorted.len() {
        // One channel: walk its programmes in start order, keeping the end of
        // everything covered so far.
        let channel = &sorted[i].channel;
        report.channels += 1;
        let first = secs(sorted[i].start);
        let mut covered_until = first;
        let mut last_end = first;
        while i < sorted.len() && &sorted[i].channel == channel {
            // Every programme here has end > start (junk was filtered out).
            let (s, e) = (secs(sorted[i].start), secs(sorted[i].end));
            if s > covered_until {
                gap += s - covered_until;
            } else {
                overlap += e.min(covered_until) - s;
            }
            covered_until = covered_until.max(e);
            last_end = last_end.max(e);
            i += 1;
        }
        observed += (last_end - first).max(0);
    }
    let minutes = |x: i128| x as f64 / 60.0;
    report.observed_minutes = minutes(observed);
    report.gap_minutes = minutes(gap);
    report.overlap_minutes = minutes(overlap);
    if observed > 0 {
        report.gap_fraction = unit(gap as f64 / observed as f64);
        report.overlap_fraction = unit(overlap as f64 / observed as f64);
    }
    report
}
