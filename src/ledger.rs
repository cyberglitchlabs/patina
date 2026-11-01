//! The window ledger: what ONE attempt at one guide window MEANS.
//!
//! The scheduler asks the fetch layer for a window and gets a
//! [`FetchOutcome`]. Several pieces of bookkeeping depend on that outcome:
//!
//! * the provider's **reliability** score (feeds the scorer): record a
//!   success, a failure, or nothing;
//! * the window's stored **state** (`window_state`), which decides when the
//!   window is due again;
//! * the lineup's **400-streak** and **bad-window hold** (a provider that
//!   rejects window after window is left alone for 6 hours);
//! * the **tick report** counters (`/status`).
//!
//! This module holds all of those rules in one place and nothing else. It
//! is pure: no store, no clock, no locks, no HTTP. The scheduler classifies
//! what happened ([`classify`], then [`Attempt::Answered`] after ingesting a
//! usable answer), asks [`decide`], and applies the returned [`Decision`]
//! to the store, metrics and logs. Because the rules are pure, every one of
//! them is tested as a table in `tests/ledger.rs`.
//!
//! It also owns the freshness rule: [`is_due`] (which windows to ask for)
//! and [`clamp_future`] (times that lie in the future after a clock jump).
//!
//! # The rules, in plain words
//!
//! | What happened | Reliability | Window state | 400-streak |
//! | --- | --- | --- | --- |
//! | No request sent (budget, circuit, our own backoff, known bad window, store unavailable) | nothing | unchanged | unchanged |
//! | HTTP 429 from the provider | one failure | unchanged | unchanged |
//! | HTTP 400 for this window | nothing (the provider answered normally) | failure `BadWindow` | +1; at 3 a 6 h hold starts |
//! | Body did not parse | failure | failure `ParseFailed` | reset |
//! | Any other failed request | failure | failure `Failed` | reset |
//! | Fresh answer | success, or failure if the timeline has too many gaps/overlaps | success, or `MergeFailed` if storing failed | reset |
//! | Answer from the raw cache | nothing (counted when it was fetched) | as for a fresh answer | reset |

use chrono::{DateTime, TimeDelta, Utc};

use crate::fetch::{FetchOutcome, SkipReason};
use crate::provider::{FetchError, ParsedWindow};
use crate::score::TimelineReport;
pub use crate::store::WindowFailure;
use crate::store::WindowState;

/// Windows starting before `now + NEAR` are "near" and refresh more often.
pub const NEAR: TimeDelta = TimeDelta::hours(24);
/// Refresh interval of near windows.
pub const NEAR_REFRESH: TimeDelta = TimeDelta::hours(3);
/// Refresh interval of every later window.
pub const FAR_REFRESH: TimeDelta = TimeDelta::hours(24);
/// When a window's response arrived but storing the merge failed, wait at
/// least this long (after the last attempt) before handling that window
/// again. Without it the window would stay due and be re-ingested every
/// tick (60 times an hour from the raw cache, then a real request whenever
/// the cache expires), each time repeating the same failing work.
pub const MERGE_RETRY_FLOOR: TimeDelta = TimeDelta::minutes(30);
/// After this many HTTP 400 answers in a row for one lineup (within one
/// tick), stop asking for that lineup's windows for `BAD_WINDOW_HOLD`. Each
/// 400 costs a request from the budget; a provider rejecting window after
/// window (a block page, a WAF, an API change) is better left alone than
/// swept to the end.
pub const BAD_WINDOW_STREAK_LIMIT: u32 = 3;
/// How long a lineup is left alone after `BAD_WINDOW_STREAK_LIMIT` 400s in
/// a row. Ending the halt with the tick was not enough: every new tick
/// would ask for the next three windows not yet remembered as bad, which
/// spreads a full sweep over a few minutes and repeats it whenever the
/// 6-hour bad-window memory expires (about 112 requests a day for nothing).
/// With the hold a provider that rejects everything costs at most 3
/// requests per hold. The circuit breaker cannot do this job: a 400 counts
/// as an answer, not a failure.
pub const BAD_WINDOW_HOLD: TimeDelta = TimeDelta::hours(6);
/// A response is counted as a reliability failure when more than this share
/// of its timeline is gaps ...
pub const MAX_GAP_FRACTION: f64 = 0.20;
/// ... or more than this share is overlapping programmes.
pub const MAX_OVERLAP_FRACTION: f64 = 0.10;

// ---------------------------------------------------------------------------
// Freshness
// ---------------------------------------------------------------------------

/// Whether a window should be asked for now.
///
/// * Never attempted, or never succeeded: due.
/// * The last attempt got an answer but storing its merge failed
///   (`WindowFailure::MergeFailed`): not due until `MERGE_RETRY_FLOOR` after
///   that attempt; after that the tiers below apply.
/// * Tiers: a window STARTING before `now + NEAR` is due when its last
///   success is `NEAR_REFRESH` (3 h) old or older; a later window when it is
///   `FAR_REFRESH` (24 h) old or older.
/// * A time in the future (the clock jumped back) counts as `now`, so the
///   window is not stale forever and not due at once either (the scheduler
///   also stores the corrected time, see [`clamp_future`]).
pub fn is_due(
    window_start: DateTime<Utc>,
    now: DateTime<Utc>,
    state: Option<&WindowState>,
) -> bool {
    let Some(state) = state else {
        return true; // never attempted
    };
    // `.min(now)` turns a future time into "now": the age is never negative.
    if state.last_failure == Some(WindowFailure::MergeFailed)
        && now - state.last_attempt.min(now) < MERGE_RETRY_FLOOR
    {
        return false;
    }
    let interval = if window_start < now + NEAR {
        NEAR_REFRESH
    } else {
        FAR_REFRESH
    };
    match state.last_success {
        None => true,
        Some(t) => now - t.min(now) >= interval,
    }
}

/// A copy of `state` with every time that lies after `now` set to `now`,
/// or `None` when nothing lies in the future. The scheduler stores the
/// corrected row: left alone, such a window would not be refreshed until
/// the clock caught up, possibly days later.
pub fn clamp_future(state: &WindowState, now: DateTime<Utc>) -> Option<WindowState> {
    let future_success = state.last_success.is_some_and(|t| t > now);
    if !future_success && state.last_attempt <= now {
        return None;
    }
    Some(WindowState {
        last_success: state.last_success.map(|t| t.min(now)),
        last_attempt: state.last_attempt.min(now),
        ..state.clone()
    })
}

// ---------------------------------------------------------------------------
// What happened
// ---------------------------------------------------------------------------

/// Why no request was sent for a window. None of these says anything about
/// the provider, so none changes its reliability, the window's state or the
/// 400-streak.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotSent {
    BudgetExhausted,
    CircuitOpen,
    /// Our own backoff (or an earlier `Retry-After`) still holds.
    BackingOff,
    /// The window answered HTTP 400 recently.
    KnownBadWindow,
    /// The store failed at one of the fetch layer's gates (fail closed).
    StoreUnavailable,
}

/// What happened for ONE window attempt, in the ledger's terms.
#[derive(Debug, Clone, PartialEq)]
pub enum Attempt {
    /// No request was sent.
    NotSent(NotSent),
    /// The provider answered HTTP 429.
    RateLimited,
    /// The provider answered HTTP 400 for this window.
    BadWindow,
    /// HTTP 200 but the body could not be understood.
    ParseFailed(String),
    /// Any other failed request (5xx, 403, timeout, redirect, ...).
    Failed(String),
    /// A usable answer, after the scheduler tried to store it.
    Answered {
        /// True for a response fetched just now, false for a raw-cache hit.
        fresh: bool,
        /// The response's gap/overlap check, or `None` when storing failed
        /// before the check was reached (the channel lists could not be
        /// stored); then no reliability is recorded.
        integrity: Option<TimelineReport>,
        /// True when everything the answer contained was stored.
        stored: bool,
    },
}

/// The fetch outcome, sorted into "needs ingesting" and "already decided".
#[derive(Debug)]
pub enum Classified {
    /// A clean answer: the scheduler ingests it, then builds
    /// `Attempt::Answered`.
    Usable { parsed: ParsedWindow, fresh: bool },
    /// Nothing to ingest; this is the whole story.
    Settled(Attempt),
}

/// Sort one fetch outcome.
pub fn classify(outcome: FetchOutcome) -> Classified {
    use Attempt as A;
    let settled = |a| Classified::Settled(a);
    match outcome {
        // The fetch layer never returns a parse error inside Fresh or
        // FromCache (it returns `Skipped(ParseFailed)`, or treats a cached
        // body that no longer parses as a cache miss; tests/fetch.rs pins
        // both). This check is defence in depth: a broken body must never
        // reach the merge, whatever a future change upstream does. It costs
        // one comparison and is tested in tests/ledger.rs.
        FetchOutcome::Fresh(p) | FetchOutcome::FromCache(p) if p.parse_error.is_some() => {
            settled(A::ParseFailed(p.parse_error.unwrap_or_default()))
        }
        FetchOutcome::Fresh(parsed) => Classified::Usable {
            parsed,
            fresh: true,
        },
        FetchOutcome::FromCache(parsed) => Classified::Usable {
            parsed,
            fresh: false,
        },
        FetchOutcome::Skipped(reason) => settled(match reason {
            SkipReason::BudgetExhausted => A::NotSent(NotSent::BudgetExhausted),
            SkipReason::CircuitOpen { .. } => A::NotSent(NotSent::CircuitOpen),
            SkipReason::BackingOffUntil(_) => A::NotSent(NotSent::BackingOff),
            SkipReason::KnownBadWindow => A::NotSent(NotSent::KnownBadWindow),
            SkipReason::StoreUnavailable => A::NotSent(NotSent::StoreUnavailable),
            // The fetch layer reports a 429 as `RateLimited`, but should a
            // `Failed(RateLimited)` ever appear it means the same thing.
            SkipReason::RateLimited { .. } | SkipReason::Failed(FetchError::RateLimited { .. }) => {
                A::RateLimited
            }
            SkipReason::Failed(FetchError::BadWindow) => A::BadWindow,
            SkipReason::Failed(e) => A::Failed(e.to_string()),
            SkipReason::ParseFailed(msg) => A::ParseFailed(msg),
        }),
    }
}

/// Whether a response's timeline is good enough to count as a reliability
/// success: gaps at most 20 %, overlaps at most 10 %. (A worse response is
/// still merged: holes are not contradictions.)
pub fn acceptable(integrity: &TimelineReport) -> bool {
    integrity.gap_fraction <= MAX_GAP_FRACTION && integrity.overlap_fraction <= MAX_OVERLAP_FRACTION
}

// ---------------------------------------------------------------------------
// The lineup's streak and hold
// ---------------------------------------------------------------------------

/// The 400-streak and bad-window hold of one lineup.
///
/// The scheduler keeps one per lineup in memory only: after a restart the
/// hold is gone, which costs at most the `BAD_WINDOW_STREAK_LIMIT` (3)
/// requests that set it again.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LineupRun {
    /// HTTP 400 answers in a row in the current tick.
    pub streak: u32,
    /// The lineup is left alone until this time (exclusive: at `until` it
    /// may be asked again).
    pub hold_until: Option<DateTime<Utc>>,
}

impl LineupRun {
    /// The end of the active hold at `now`, or `None` when the lineup may
    /// be asked. A hold is never longer than `BAD_WINDOW_HOLD` from `now`:
    /// if the clock jumped backwards after the hold started, the stored end
    /// would otherwise lie further away than intended, so it is clamped.
    pub fn held_until(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let until = self.hold_until.filter(|until| *until > now)?;
        let longest = now.checked_add_signed(BAD_WINDOW_HOLD).unwrap_or(until);
        Some(until.min(longest))
    }

    /// The state to begin a new tick with: the streak counts 400s in a row
    /// within one tick only, so it starts at 0; the hold carries over until
    /// it ends (clamped as in [`held_until`](Self::held_until)).
    pub fn start_tick(self, now: DateTime<Utc>) -> LineupRun {
        LineupRun {
            streak: 0,
            hold_until: self.held_until(now),
        }
    }
}

// ---------------------------------------------------------------------------
// Decisions
// ---------------------------------------------------------------------------

/// What to record for the provider's reliability score.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reliability {
    Nothing,
    Success,
    Failure,
}

/// What to write to the window's stored state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowWrite {
    /// Leave the stored row as it is.
    Nothing,
    /// The window is fresh: last success and last attempt are now.
    Success,
    /// The attempt failed: keep the previous last success, set the last
    /// attempt to now and remember the failure.
    Failure(WindowFailure),
}

/// The window's stored state as the scheduler could read it.
#[derive(Debug, Clone, Copy)]
pub enum SavedWindow<'a> {
    /// Reading it failed.
    Unreadable,
    /// There is no row (never attempted).
    Absent,
    Present(&'a WindowState),
}

impl WindowWrite {
    /// Whether [`row`](Self::row) needs the saved state. The scheduler
    /// reads the store only then (a success needs no read).
    pub fn needs_saved_state(&self) -> bool {
        matches!(self, WindowWrite::Failure(_))
    }

    /// The row to store, or `None` for "write nothing". A failure whose
    /// previous state could not be read writes nothing: never overwrite a
    /// real last success with a guess ("never succeeded" would make the
    /// window look stale and cost a request).
    pub fn row(
        &self,
        window_start: DateTime<Utc>,
        saved: SavedWindow<'_>,
        now: DateTime<Utc>,
    ) -> Option<WindowState> {
        match self {
            WindowWrite::Nothing => None,
            WindowWrite::Success => Some(WindowState {
                window_start,
                last_success: Some(now),
                last_attempt: now,
                last_failure: None,
            }),
            WindowWrite::Failure(failure) => {
                let last_success = match saved {
                    SavedWindow::Unreadable => return None,
                    SavedWindow::Absent => None,
                    SavedWindow::Present(st) => st.last_success,
                };
                Some(WindowState {
                    window_start,
                    last_success,
                    last_attempt: now,
                    last_failure: Some(failure.clone()),
                })
            }
        }
    }
}

/// Which tick-report counter one attempt adds to (exactly one each).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tally {
    /// A fresh response from the provider.
    Fetched,
    /// An answer served from the raw cache.
    FromCache,
    BudgetExhausted,
    CircuitOpen,
    /// Our own backoff held the request back, or the provider answered 429.
    RateLimited,
    KnownBadWindow,
    /// The provider answered 400 just now.
    BadWindow,
    /// No request: the store failed at a fetch-layer gate.
    StoreUnavailable,
    /// Not asked: the lineup is in a bad-window hold.
    Halted,
    /// A failed request (5xx, 403, timeout, ...).
    Failed,
    /// A body that did not parse (counted in `failed` too).
    ParseFailed,
}

/// Everything one attempt changes. The scheduler applies it; the ledger
/// itself changes nothing.
#[derive(Debug, Clone, PartialEq)]
pub struct Decision {
    pub reliability: Reliability,
    pub window: WindowWrite,
    pub tally: Tally,
    /// The lineup's streak and hold after this attempt.
    pub lineup: LineupRun,
    /// `Some(until)` when this attempt started a bad-window hold (log it
    /// once and report it). While the hold lasts the scheduler skips the
    /// lineup's remaining windows (`LineupRun::held_until`), counting each
    /// as `Tally::Halted`.
    pub hold_started: Option<DateTime<Utc>>,
}

/// Decide what one window attempt means. `lineup` is the lineup's state
/// before the attempt; `now` is when the attempt finished.
pub fn decide(lineup: LineupRun, attempt: &Attempt, now: DateTime<Utc>) -> Decision {
    // Most outcomes end a run of 400s; set otherwise below.
    let reset = LineupRun {
        streak: 0,
        ..lineup
    };
    let mut hold_started = None;
    let (reliability, window, tally, next) = match attempt {
        // No request: nothing new is known about the provider or the
        // window. In particular a store outage is OUR problem: it must not
        // lower the provider's score (which would change how its data is
        // ranked) nor mark the window as failed.
        Attempt::NotSent(why) => {
            let tally = match why {
                NotSent::BudgetExhausted => Tally::BudgetExhausted,
                NotSent::CircuitOpen => Tally::CircuitOpen,
                NotSent::BackingOff => Tally::RateLimited,
                NotSent::KnownBadWindow => Tally::KnownBadWindow,
                NotSent::StoreUnavailable => Tally::StoreUnavailable,
            };
            (Reliability::Nothing, WindowWrite::Nothing, tally, lineup)
        }
        // A real 429: the provider is telling us to slow down, which is a
        // failure for its reliability (once per answer). The fetch layer
        // has already counted it for backoff and circuit. No window failure
        // is written and the 400-streak is left alone: a 429 is about the
        // provider, not about this window or this lineup's windows.
        Attempt::RateLimited => (
            Reliability::Failure,
            WindowWrite::Nothing,
            Tally::RateLimited,
            lineup,
        ),
        // A 400: the provider answered normally, only not for this window,
        // so it is no reliability failure. Three in a row put the lineup on
        // hold (see `BAD_WINDOW_HOLD`).
        Attempt::BadWindow => {
            let streak = lineup.streak.saturating_add(1);
            let mut hold_until = lineup.hold_until;
            if streak >= BAD_WINDOW_STREAK_LIMIT {
                // Saturate instead of panicking at the end of time.
                let until = now.checked_add_signed(BAD_WINDOW_HOLD).unwrap_or(now);
                hold_until = Some(until);
                hold_started = Some(until);
            }
            (
                Reliability::Nothing,
                WindowWrite::Failure(WindowFailure::BadWindow),
                Tally::BadWindow,
                LineupRun { streak, hold_until },
            )
        }
        Attempt::ParseFailed(msg) => (
            Reliability::Failure,
            WindowWrite::Failure(WindowFailure::ParseFailed(msg.clone())),
            Tally::ParseFailed,
            reset,
        ),
        Attempt::Failed(msg) => (
            Reliability::Failure,
            WindowWrite::Failure(WindowFailure::Failed(msg.clone())),
            Tally::Failed,
            reset,
        ),
        Attempt::Answered {
            fresh,
            integrity,
            stored,
        } => {
            // Only fresh responses count: a cached answer was counted when
            // it was fetched, and would be counted again here. And only when
            // the response was checked at all.
            let reliability = match integrity {
                Some(i) if *fresh && acceptable(i) => Reliability::Success,
                Some(_) if *fresh => Reliability::Failure,
                _ => Reliability::Nothing,
            };
            let window = if *stored {
                WindowWrite::Success
            } else {
                // Not marked fresh; `is_due` waits MERGE_RETRY_FLOOR.
                WindowWrite::Failure(WindowFailure::MergeFailed)
            };
            let tally = if *fresh {
                Tally::Fetched
            } else {
                Tally::FromCache
            };
            (reliability, window, tally, reset)
        }
    };
    Decision {
        reliability,
        window,
        tally,
        lineup: next,
        hold_started,
    }
}
