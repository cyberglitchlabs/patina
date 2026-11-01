//! The window ledger (`patina::ledger`): what ONE window attempt means for
//! the provider's reliability, the window's stored state, the lineup's
//! 400-streak and hold, and the tick report. Plus the freshness rule that
//! decides which windows are due.
//!
//! The ledger is pure (no store, no clock, no HTTP), so every rule is tested
//! here as a table: (saved state, lineup streak/hold, outcome, now) ->
//! expected decisions. The scheduler e2e tests (tests/ingest_e2e.rs) keep
//! the request-count guarantees that only a real tick can show.

use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use patina::fetch::{FetchOutcome, SkipReason};
use patina::ledger::{
    Attempt, BAD_WINDOW_HOLD, BAD_WINDOW_STREAK_LIMIT, Classified, Decision, LineupRun,
    MERGE_RETRY_FLOOR, NotSent, Reliability, SavedWindow, Tally, WindowWrite, acceptable,
    clamp_future, classify, decide, is_due,
};
use patina::provider::{FetchError, ParsedWindow};
use patina::score::TimelineReport;
use patina::store::{WindowFailure, WindowState};

/// 2026-10-31 20:00 UTC (as in the e2e tests).
fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 31, 20, 0, 0).unwrap()
}

fn h(n: i64) -> TimeDelta {
    TimeDelta::hours(n)
}

fn m(n: i64) -> TimeDelta {
    TimeDelta::minutes(n)
}

fn s(n: i64) -> TimeDelta {
    TimeDelta::seconds(n)
}

/// A clean timeline (no gaps, no overlaps).
fn clean() -> TimelineReport {
    TimelineReport {
        channels: 89,
        observed_minutes: 1000.0,
        ..TimelineReport::default()
    }
}

fn timeline(gap_fraction: f64, overlap_fraction: f64) -> TimelineReport {
    TimelineReport {
        gap_fraction,
        overlap_fraction,
        ..clean()
    }
}

fn answered(fresh: bool, integrity: Option<TimelineReport>, stored: bool) -> Attempt {
    Attempt::Answered {
        fresh,
        integrity,
        stored,
    }
}

fn run(streak: u32, hold_until: Option<DateTime<Utc>>) -> LineupRun {
    LineupRun { streak, hold_until }
}

// ---------------------------------------------------------------------------
// decide(): one row per outcome
// ---------------------------------------------------------------------------

/// What one row expects from `decide`.
struct Expect {
    reliability: Reliability,
    window: WindowWrite,
    tally: Tally,
    /// The lineup's streak/hold afterwards.
    lineup: LineupRun,
    hold_started: Option<DateTime<Utc>>,
}

#[test]
fn every_outcome_decides_reliability_window_streak_and_tally() {
    use Reliability::{Failure, Nothing, Success};
    let n = now();
    let streak1 = run(1, None);
    let rows: Vec<(&str, LineupRun, Attempt, Expect)> = vec![
        // --- no request was sent: count only the tally, change nothing ---
        (
            "budget exhausted",
            streak1,
            Attempt::NotSent(NotSent::BudgetExhausted),
            Expect {
                reliability: Nothing,
                window: WindowWrite::Nothing,
                tally: Tally::BudgetExhausted,
                lineup: streak1,
                hold_started: None,
            },
        ),
        (
            "circuit open",
            streak1,
            Attempt::NotSent(NotSent::CircuitOpen),
            Expect {
                reliability: Nothing,
                window: WindowWrite::Nothing,
                tally: Tally::CircuitOpen,
                lineup: streak1,
                hold_started: None,
            },
        ),
        (
            "our own backoff",
            streak1,
            Attempt::NotSent(NotSent::BackingOff),
            Expect {
                reliability: Nothing,
                window: WindowWrite::Nothing,
                tally: Tally::RateLimited,
                lineup: streak1,
                hold_started: None,
            },
        ),
        (
            "known bad window",
            streak1,
            Attempt::NotSent(NotSent::KnownBadWindow),
            Expect {
                reliability: Nothing,
                window: WindowWrite::Nothing,
                tally: Tally::KnownBadWindow,
                lineup: streak1,
                hold_started: None,
            },
        ),
        // FIX (i): a store outage is our problem, never the provider's.
        (
            "store unavailable",
            streak1,
            Attempt::NotSent(NotSent::StoreUnavailable),
            Expect {
                reliability: Nothing,
                window: WindowWrite::Nothing,
                tally: Tally::StoreUnavailable,
                lineup: streak1,
                hold_started: None,
            },
        ),
        // --- a request was sent ---
        // FIX (ii): a real 429 is one reliability failure; streak unchanged
        // and no window failure written (as before).
        (
            "real 429",
            streak1,
            Attempt::RateLimited,
            Expect {
                reliability: Failure,
                window: WindowWrite::Nothing,
                tally: Tally::RateLimited,
                lineup: streak1,
                hold_started: None,
            },
        ),
        // A 400 is a normal answer about one window: no reliability failure.
        (
            "first 400",
            run(0, None),
            Attempt::BadWindow,
            Expect {
                reliability: Nothing,
                window: WindowWrite::Failure(WindowFailure::BadWindow),
                tally: Tally::BadWindow,
                lineup: run(1, None),
                hold_started: None,
            },
        ),
        (
            "second 400",
            run(1, None),
            Attempt::BadWindow,
            Expect {
                reliability: Nothing,
                window: WindowWrite::Failure(WindowFailure::BadWindow),
                tally: Tally::BadWindow,
                lineup: run(2, None),
                hold_started: None,
            },
        ),
        (
            "third 400 starts a 6 h hold",
            run(2, None),
            Attempt::BadWindow,
            Expect {
                reliability: Nothing,
                window: WindowWrite::Failure(WindowFailure::BadWindow),
                tally: Tally::BadWindow,
                lineup: run(3, Some(n + h(6))),
                hold_started: Some(n + h(6)),
            },
        ),
        (
            "parse failure",
            run(2, None),
            Attempt::ParseFailed("EOF while parsing".into()),
            Expect {
                reliability: Failure,
                window: WindowWrite::Failure(WindowFailure::ParseFailed(
                    "EOF while parsing".into(),
                )),
                tally: Tally::ParseFailed,
                lineup: run(0, None),
                hold_started: None,
            },
        ),
        (
            "other failure (5xx, 403, timeout)",
            run(2, None),
            Attempt::Failed("HTTP 500".into()),
            Expect {
                reliability: Failure,
                window: WindowWrite::Failure(WindowFailure::Failed("HTTP 500".into())),
                tally: Tally::Failed,
                lineup: run(0, None),
                hold_started: None,
            },
        ),
        // --- usable answers ---
        (
            "fresh, clean, stored",
            run(2, None),
            answered(true, Some(clean()), true),
            Expect {
                reliability: Success,
                window: WindowWrite::Success,
                tally: Tally::Fetched,
                lineup: run(0, None),
                hold_started: None,
            },
        ),
        (
            "fresh, gappy, stored: merged, but a reliability failure",
            run(0, None),
            answered(true, Some(timeline(0.5, 0.0)), true),
            Expect {
                reliability: Failure,
                window: WindowWrite::Success,
                tally: Tally::Fetched,
                lineup: run(0, None),
                hold_started: None,
            },
        ),
        (
            "fresh, clean, merge failed",
            run(0, None),
            answered(true, Some(clean()), false),
            Expect {
                reliability: Success,
                window: WindowWrite::Failure(WindowFailure::MergeFailed),
                tally: Tally::Fetched,
                lineup: run(0, None),
                hold_started: None,
            },
        ),
        (
            "fresh, channels could not be stored (never checked): no reliability",
            run(0, None),
            answered(true, None, false),
            Expect {
                reliability: Nothing,
                window: WindowWrite::Failure(WindowFailure::MergeFailed),
                tally: Tally::Fetched,
                lineup: run(0, None),
                hold_started: None,
            },
        ),
        // A cache hit was counted when it was fetched: never again.
        (
            "cache hit, stored",
            run(2, None),
            answered(false, Some(clean()), true),
            Expect {
                reliability: Nothing,
                window: WindowWrite::Success,
                tally: Tally::FromCache,
                lineup: run(0, None),
                hold_started: None,
            },
        ),
        (
            "cache hit, gappy: still nothing",
            run(0, None),
            answered(false, Some(timeline(0.9, 0.9)), true),
            Expect {
                reliability: Nothing,
                window: WindowWrite::Success,
                tally: Tally::FromCache,
                lineup: run(0, None),
                hold_started: None,
            },
        ),
        (
            "cache hit, merge failed",
            run(0, None),
            answered(false, Some(clean()), false),
            Expect {
                reliability: Nothing,
                window: WindowWrite::Failure(WindowFailure::MergeFailed),
                tally: Tally::FromCache,
                lineup: run(0, None),
                hold_started: None,
            },
        ),
    ];
    for (name, before, attempt, want) in rows {
        let d: Decision = decide(before, &attempt, n);
        assert_eq!(d.reliability, want.reliability, "{name}: reliability");
        assert_eq!(d.window, want.window, "{name}: window write");
        assert_eq!(d.tally, want.tally, "{name}: tally");
        assert_eq!(d.lineup, want.lineup, "{name}: lineup streak/hold");
        assert_eq!(d.hold_started, want.hold_started, "{name}: hold started");
    }
}

#[test]
fn integrity_thresholds_are_strictly_greater_than() {
    // gap_fraction > 0.20 or overlap_fraction > 0.10 is a failure; exactly
    // at the limit is fine.
    for (gap, overlap, ok) in [
        (0.0, 0.0, true),
        (0.20, 0.0, true),
        (0.2000001, 0.0, false),
        (0.0, 0.10, true),
        (0.0, 0.1000001, false),
        (0.20, 0.10, true),
        (1.0, 1.0, false),
    ] {
        assert_eq!(acceptable(&timeline(gap, overlap)), ok, "{gap} {overlap}");
        let d = decide(
            LineupRun::default(),
            &answered(true, Some(timeline(gap, overlap)), true),
            now(),
        );
        let want = if ok {
            Reliability::Success
        } else {
            Reliability::Failure
        };
        assert_eq!(d.reliability, want, "{gap} {overlap}");
    }
}

// ---------------------------------------------------------------------------
// The 400 streak and the lineup hold
// ---------------------------------------------------------------------------

/// Feed a sequence of attempts to one lineup; return the final run.
fn feed(attempts: &[Attempt]) -> (LineupRun, Vec<Option<DateTime<Utc>>>) {
    let mut r = LineupRun::default();
    let mut holds = Vec::new();
    for a in attempts {
        let d = decide(r, a, now());
        holds.push(d.hold_started);
        r = d.lineup;
    }
    (r, holds)
}

#[test]
fn three_400s_in_a_row_start_the_hold_and_skips_do_not_reset_the_streak() {
    assert_eq!(BAD_WINDOW_STREAK_LIMIT, 3);
    assert_eq!(BAD_WINDOW_HOLD, h(6));
    let bad = || Attempt::BadWindow;
    // Every no-request outcome (and a real 429) between the 400s leaves the
    // streak where it was, so the third 400 still starts the hold.
    for skip in [
        Attempt::NotSent(NotSent::BudgetExhausted),
        Attempt::NotSent(NotSent::CircuitOpen),
        Attempt::NotSent(NotSent::BackingOff),
        Attempt::NotSent(NotSent::KnownBadWindow),
        Attempt::NotSent(NotSent::StoreUnavailable),
        Attempt::RateLimited,
    ] {
        let (r, holds) = feed(&[bad(), skip.clone(), bad(), skip.clone(), bad()]);
        assert_eq!(r.hold_until, Some(now() + h(6)), "{skip:?}");
        assert_eq!(holds.iter().flatten().count(), 1, "logged once: {skip:?}");
    }
    // A real answer in between resets it: no hold.
    for reset in [
        answered(true, Some(clean()), true),
        answered(false, Some(clean()), true),
        answered(true, None, false),
        Attempt::Failed("HTTP 500".into()),
        Attempt::ParseFailed("bad".into()),
    ] {
        let (r, _) = feed(&[bad(), bad(), reset.clone(), bad(), bad()]);
        assert_eq!((r.streak, r.hold_until), (2, None), "{reset:?}");
    }
}

#[test]
fn hold_lasts_exactly_six_hours_and_a_new_tick_resets_only_the_streak() {
    let n = now();
    let held = run(3, Some(n + h(6)));
    // Held right up to (not including) the end.
    assert_eq!(held.held_until(n), Some(n + h(6)));
    assert_eq!(held.held_until(n + h(6) - s(1)), Some(n + h(6)));
    assert_eq!(held.held_until(n + h(6)), None);
    assert_eq!(held.held_until(n + h(7)), None);

    // A new tick: the streak is per tick, the hold carries over until it
    // ends, then it is forgotten.
    assert_eq!(held.start_tick(n + m(1)), run(0, Some(n + h(6))));
    assert_eq!(held.start_tick(n + h(6)), run(0, None));
    assert_eq!(run(2, None).start_tick(n), run(0, None));
}

#[test]
fn a_hold_never_reaches_more_than_six_hours_past_now_when_the_clock_jumps_back() {
    let n = now();
    // The hold was set at n, then the clock jumped back a day: left alone it
    // would last 30 hours. It is clamped to now + 6 h.
    let held = run(3, Some(n + h(6)));
    let earlier = n - h(24);
    assert_eq!(held.held_until(earlier), Some(earlier + h(6)));
    assert_eq!(held.start_tick(earlier), run(0, Some(earlier + h(6))));
    // A clock jump forward simply ends it early.
    assert_eq!(held.held_until(n + h(48)), None);
}

// ---------------------------------------------------------------------------
// The window row to write
// ---------------------------------------------------------------------------

fn state(last_success: Option<DateTime<Utc>>, failure: Option<WindowFailure>) -> WindowState {
    WindowState {
        window_start: now() - h(2),
        last_success,
        last_attempt: now() - h(5),
        last_failure: failure,
    }
}

#[test]
fn window_rows_keep_the_last_success_and_never_guess_it() {
    let n = now();
    let w = n - h(2);
    let prev = state(Some(n - h(5)), None);
    let fail = WindowWrite::Failure(WindowFailure::Failed("HTTP 500".into()));

    // A success needs no saved state and marks the window fresh.
    assert!(!WindowWrite::Success.needs_saved_state());
    for saved in [
        SavedWindow::Unreadable,
        SavedWindow::Absent,
        SavedWindow::Present(&prev),
    ] {
        assert_eq!(
            WindowWrite::Success.row(w, saved, n),
            Some(WindowState {
                window_start: w,
                last_success: Some(n),
                last_attempt: n,
                last_failure: None,
            })
        );
    }

    // A failure keeps the previous last success ...
    assert!(fail.needs_saved_state());
    assert_eq!(
        fail.row(w, SavedWindow::Present(&prev), n),
        Some(WindowState {
            window_start: w,
            last_success: Some(n - h(5)),
            last_attempt: n,
            last_failure: Some(WindowFailure::Failed("HTTP 500".into())),
        })
    );
    // ... or "never" if there was no row ...
    assert_eq!(
        fail.row(w, SavedWindow::Absent, n).unwrap().last_success,
        None
    );
    // ... and writes NOTHING when the previous state could not be read
    // (never overwrite a real last success with a guess).
    assert_eq!(fail.row(w, SavedWindow::Unreadable, n), None);
    let merge = WindowWrite::Failure(WindowFailure::MergeFailed);
    assert_eq!(merge.row(w, SavedWindow::Unreadable, n), None);

    // Nothing is nothing.
    assert!(!WindowWrite::Nothing.needs_saved_state());
    assert_eq!(
        WindowWrite::Nothing.row(w, SavedWindow::Present(&prev), n),
        None
    );
}

// ---------------------------------------------------------------------------
// Freshness: is_due and the future clamp
// ---------------------------------------------------------------------------

#[test]
fn freshness_tiers_and_merge_retry_floor() {
    let n = now();
    let near = n + h(4); // starts within the next 24 h
    let far = n + h(30);
    let ok = |age: TimeDelta| state(Some(n - age), None);
    let merge_failed = |success_age: TimeDelta, attempt_age: TimeDelta| WindowState {
        last_attempt: n - attempt_age,
        ..state(Some(n - success_age), Some(WindowFailure::MergeFailed))
    };
    assert_eq!(MERGE_RETRY_FLOOR, m(30));
    let rows: Vec<(&str, DateTime<Utc>, Option<WindowState>, bool)> = vec![
        ("never attempted", far, None, true),
        (
            "attempted, never succeeded",
            far,
            Some(state(None, None)),
            true,
        ),
        // Near windows: due once 3 h old (exactly 3 h counts).
        ("near, 2h59m59s", near, Some(ok(h(3) - s(1))), false),
        ("near, exactly 3 h", near, Some(ok(h(3))), true),
        // Far windows: due once 24 h old.
        ("far, 3 h", far, Some(ok(h(3))), false),
        ("far, 23h59m59s", far, Some(ok(h(24) - s(1))), false),
        ("far, exactly 24 h", far, Some(ok(h(24))), true),
        // The near/far edge is the window START against now + 24 h: a
        // window starting 1 s before it is near even though it ends a
        // day later; one starting exactly at it is far.
        (
            "starts 1 s before now+24h, 3 h old",
            n + h(24) - s(1),
            Some(ok(h(3))),
            true,
        ),
        (
            "starts exactly at now+24h, 3 h old",
            n + h(24),
            Some(ok(h(3))),
            false,
        ),
        // The window containing now is near.
        ("current window", n - h(2), Some(ok(h(3))), true),
        // Merge failed: wait 30 min after the last attempt, then the
        // normal tiers apply (on the last SUCCESS, not the attempt).
        (
            "merge failed 29m59s ago",
            near,
            Some(merge_failed(h(10), m(30) - s(1))),
            false,
        ),
        (
            "merge failed exactly 30 min ago",
            near,
            Some(merge_failed(h(10), m(30))),
            true,
        ),
        (
            "merge failed 31 min ago, but the data is 1 h old",
            near,
            Some(merge_failed(h(1), m(31))),
            false,
        ),
        (
            "merge failed, never succeeded, 10 min ago",
            near,
            Some(WindowState {
                last_attempt: n - m(10),
                ..state(None, Some(WindowFailure::MergeFailed))
            }),
            false,
        ),
        // Only MergeFailed has a floor: any other failure is due by the tiers.
        (
            "other failure 1 min ago, never succeeded",
            near,
            Some(WindowState {
                last_attempt: n - m(1),
                ..state(None, Some(WindowFailure::Failed("HTTP 500".into())))
            }),
            true,
        ),
        (
            "bad window 1 min ago, never succeeded",
            near,
            Some(WindowState {
                last_attempt: n - m(1),
                ..state(None, Some(WindowFailure::BadWindow))
            }),
            true,
        ),
        // A future last attempt (clock jumped back) counts as "now": the
        // floor still holds, never a negative age.
        (
            "merge failed, attempt in the future",
            near,
            Some(merge_failed(h(10), -h(5))),
            false,
        ),
        // A future last success counts as "now": fresh, not stale forever
        // and not due at once either.
        (
            "near, success 10 days ahead",
            near,
            Some(ok(-h(240))),
            false,
        ),
    ];
    for (name, window, st, want) in rows {
        assert_eq!(is_due(window, n, st.as_ref()), want, "{name}");
    }
}

#[test]
fn future_times_are_clamped_to_now_and_reported_for_storing() {
    let n = now();
    let good = state(Some(n - h(1)), None);
    assert_eq!(clamp_future(&good, n), None, "nothing to correct");

    let future_success = state(Some(n + h(240)), None);
    let fixed = clamp_future(&future_success, n).expect("corrected");
    assert_eq!(fixed.last_success, Some(n));
    assert_eq!(fixed.last_attempt, future_success.last_attempt);

    let future_attempt = WindowState {
        last_attempt: n + h(1),
        ..good.clone()
    };
    let fixed = clamp_future(&future_attempt, n).expect("corrected");
    assert_eq!(
        (fixed.last_success, fixed.last_attempt),
        (Some(n - h(1)), n)
    );
    assert_eq!(fixed.last_failure, good.last_failure);
}

// ---------------------------------------------------------------------------
// classify(): fetch outcome -> ledger attempt
// ---------------------------------------------------------------------------

#[test]
fn every_fetch_outcome_is_classified() {
    let t = now();
    let settled = |o: FetchOutcome| match classify(o) {
        Classified::Settled(a) => a,
        Classified::Usable { .. } => panic!("expected settled"),
    };
    let skip = |r: SkipReason| settled(FetchOutcome::Skipped(r));
    assert_eq!(
        skip(SkipReason::BudgetExhausted),
        Attempt::NotSent(NotSent::BudgetExhausted)
    );
    assert_eq!(
        skip(SkipReason::CircuitOpen { until: t }),
        Attempt::NotSent(NotSent::CircuitOpen)
    );
    assert_eq!(
        skip(SkipReason::BackingOffUntil(t)),
        Attempt::NotSent(NotSent::BackingOff)
    );
    assert_eq!(
        skip(SkipReason::KnownBadWindow),
        Attempt::NotSent(NotSent::KnownBadWindow)
    );
    assert_eq!(
        skip(SkipReason::StoreUnavailable),
        Attempt::NotSent(NotSent::StoreUnavailable)
    );
    assert_eq!(
        skip(SkipReason::RateLimited { not_before: t }),
        Attempt::RateLimited
    );
    assert_eq!(
        skip(SkipReason::Failed(FetchError::BadWindow)),
        Attempt::BadWindow
    );
    assert_eq!(
        skip(SkipReason::Failed(FetchError::Fatal("HTTP 403".into()))),
        Attempt::Failed("fatal fetch error: HTTP 403".into())
    );
    assert_eq!(
        skip(SkipReason::ParseFailed("EOF".into())),
        Attempt::ParseFailed("EOF".into())
    );

    let clean_window = ParsedWindow::default();
    for (outcome, fresh) in [
        (FetchOutcome::Fresh(clean_window.clone()), true),
        (FetchOutcome::FromCache(clean_window.clone()), false),
    ] {
        match classify(outcome) {
            Classified::Usable { fresh: f, .. } => assert_eq!(f, fresh),
            other => panic!("expected usable, got {other:?}"),
        }
    }

    // Defence in depth: the fetch layer never returns a Fresh/FromCache
    // window with `parse_error` set (tests/fetch.rs pins that), but if a
    // future change ever did, it must be a parse failure and never reach
    // the merge.
    let broken = ParsedWindow {
        parse_error: Some("bad body".into()),
        ..ParsedWindow::default()
    };
    assert_eq!(
        settled(FetchOutcome::Fresh(broken.clone())),
        Attempt::ParseFailed("bad body".into())
    );
    assert_eq!(
        settled(FetchOutcome::FromCache(broken)),
        Attempt::ParseFailed("bad body".into())
    );
}

// ---------------------------------------------------------------------------
// WindowFailure's stored codes
// ---------------------------------------------------------------------------

#[test]
fn window_failure_codes_are_stable_and_unknown_text_is_generic() {
    // The stored codes are part of the database format: pinned here.
    assert_eq!(WindowFailure::MergeFailed.to_stored(), "merge_failed");
    assert_eq!(WindowFailure::BadWindow.to_stored(), "bad_window");
    assert_eq!(
        WindowFailure::ParseFailed("x: y".into()).to_stored(),
        "parse_failed: x: y"
    );
    assert_eq!(
        WindowFailure::Failed("HTTP 500".into()).to_stored(),
        "failed: HTTP 500"
    );
    for f in [
        WindowFailure::MergeFailed,
        WindowFailure::BadWindow,
        WindowFailure::ParseFailed("x: y".into()),
        WindowFailure::Failed("merge_failed".into()),
        WindowFailure::Failed(String::new()),
    ] {
        assert_eq!(WindowFailure::from_stored(&f.to_stored()), f);
    }
    for legacy in [
        "storing the merge failed",
        "HTTP 400 (bad window)",
        "merge_failed: x",
        "bad_window: x",
        "parse_failed",
        "",
    ] {
        assert_eq!(
            WindowFailure::from_stored(legacy),
            WindowFailure::Failed(legacy.into()),
            "{legacy:?}"
        );
    }
}
