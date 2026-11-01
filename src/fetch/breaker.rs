//! Backoff and circuit breaker, as pure functions over the stored
//! [`ProviderFetchState`]. No I/O here, so the rules are easy to test.
//!
//! The rules, in plain words:
//! * Every failure (5xx, network error, timeout, 429, 403, unparseable body)
//!   adds one to `consecutive_failures` and holds the provider off for a
//!   backoff time: `base × 2^(failures − 1)`, capped at `max`
//!   (30 s, 60 s, 120 s, … 1 h by default). A 429 with `Retry-After` uses the
//!   server's time instead.
//! * After `threshold` failures in a row (or at once for a "fatal" error such
//!   as 403, which usually means we are blocked) the circuit *opens*: no
//!   requests for a cooldown (`max(cooldown, current backoff)`).
//! * When the cooldown has passed the circuit is *half-open*: the next request
//!   is a probe. The fetch layer sends one request at a time per provider and
//!   records the probe's outcome before releasing its turn, so exactly one
//!   probe goes out. Success closes the circuit; *any* failure opens it again
//!   for another (possibly longer) cooldown. An HTTP 400 for the probe's
//!   window counts as success here: the provider answered normally, only
//!   about that one window (the window itself is still remembered as bad).
//! * A 429's `Retry-After` is honored, but never shortens our own backoff.
//! * A success resets everything.

use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};

use crate::store::ProviderFetchState;

/// Never wait longer than a day, whatever a server's `Retry-After` says (a
/// broken header must not silence a provider for years).
const MAX_RETRY_AFTER: Duration = Duration::from_secs(24 * 3600);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation.
    Closed,
    /// Too many failures: no requests until `until`.
    Open { until: DateTime<Utc> },
    /// Cooldown over: the next request is a single probe.
    HalfOpen,
}

/// What kind of failure happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Failure {
    Transient,
    RateLimited(Option<Duration>),
    Fatal,
    ParseFailed,
}

/// Why a request may not go out right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Hold {
    CircuitOpen(DateTime<Utc>),
    NotBefore(DateTime<Utc>),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Policy {
    pub backoff_base: Duration,
    pub backoff_max: Duration,
    pub threshold: u32,
    pub cooldown: Duration,
}

/// What a failure changed, for logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FailureEffect {
    /// The circuit went from closed (or half-open) to open.
    pub opened: bool,
    pub not_before: DateTime<Utc>,
}

/// Backoff after the `failures`-th failure in a row (1-based).
pub(crate) fn backoff(failures: u32, base: Duration, max: Duration) -> Duration {
    // 2^(failures-1), saturating at u32::MAX for absurdly long failure runs.
    let factor = 1u32
        .checked_shl(failures.saturating_sub(1))
        .unwrap_or(u32::MAX);
    base.saturating_mul(factor).min(max)
}

pub(crate) fn circuit_state(st: &ProviderFetchState, now: DateTime<Utc>) -> CircuitState {
    match st.circuit_open_until {
        None => CircuitState::Closed,
        Some(until) if now < until => CircuitState::Open { until },
        Some(_) => CircuitState::HalfOpen,
    }
}

/// `Some` if a request must not be sent at `now`. The circuit is checked
/// first so callers see the more serious reason.
pub(crate) fn hold(st: &ProviderFetchState, now: DateTime<Utc>) -> Option<Hold> {
    if let CircuitState::Open { until } = circuit_state(st, now) {
        return Some(Hold::CircuitOpen(until));
    }
    match st.not_before {
        Some(t) if now < t => Some(Hold::NotBefore(t)),
        _ => None,
    }
}

/// Record a success. Returns true if this closed an open/half-open circuit.
pub(crate) fn on_success(st: &mut ProviderFetchState, now: DateTime<Utc>) -> bool {
    let was_open = st.circuit_open_until.is_some();
    st.consecutive_failures = 0;
    st.not_before = None;
    st.circuit_open_until = None;
    st.last_success = Some(now);
    was_open
}

pub(crate) fn on_failure(
    st: &mut ProviderFetchState,
    now: DateTime<Utc>,
    failure: Failure,
    message: String,
    policy: &Policy,
) -> FailureEffect {
    st.consecutive_failures = st.consecutive_failures.saturating_add(1);
    st.last_error = Some(message);

    let backoff = backoff(
        st.consecutive_failures,
        policy.backoff_base,
        policy.backoff_max,
    );
    let wait = match failure {
        // Honor the server's number, but never retry sooner than our own
        // backoff would (a `Retry-After: 0` must not make us faster).
        Failure::RateLimited(Some(retry_after)) => retry_after.max(backoff).min(MAX_RETRY_AFTER),
        _ => backoff,
    };
    let not_before = add(now, wait);
    st.not_before = Some(not_before);

    // `circuit_open_until` is only ever set while the circuit is open or
    // half-open, and no request is sent while it is open. So if it is set
    // here, this failure was the half-open probe, and the circuit must open
    // again, whatever the failure count (it may be below the threshold, e.g.
    // after a 403 opened it at count 1).
    let was_half_open = st.circuit_open_until.is_some();
    let should_open = was_half_open
        || st.consecutive_failures >= policy.threshold
        || matches!(failure, Failure::Fatal);
    let mut opened = false;
    if should_open {
        // Closed -> open is the transition worth reporting as "opened".
        opened = !was_half_open;
        let cooldown = policy.cooldown.max(wait);
        st.circuit_open_until = Some(add(now, cooldown));
    }
    FailureEffect { opened, not_before }
}

/// `t + d`, saturating at the far future instead of overflowing.
pub(crate) fn add(t: DateTime<Utc>, d: Duration) -> DateTime<Utc> {
    TimeDelta::from_std(d)
        .ok()
        .and_then(|d| t.checked_add_signed(d))
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn policy() -> Policy {
        Policy {
            backoff_base: Duration::from_secs(30),
            backoff_max: Duration::from_secs(3600),
            threshold: 5,
            cooldown: Duration::from_secs(900),
        }
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let b = |n| backoff(n, Duration::from_secs(30), Duration::from_secs(3600)).as_secs();
        assert_eq!(
            [b(1), b(2), b(3), b(4), b(7), b(8), b(40), b(u32::MAX)],
            [30, 60, 120, 240, 1920, 3600, 3600, 3600]
        );
    }

    #[test]
    fn fatal_opens_at_once_and_success_closes() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let mut st = ProviderFetchState::default();
        let eff = on_failure(&mut st, now, Failure::Fatal, "HTTP 403".into(), &policy());
        assert!(eff.opened);
        assert_eq!(
            circuit_state(&st, now),
            CircuitState::Open {
                until: now + TimeDelta::seconds(900)
            }
        );
        assert_eq!(
            circuit_state(&st, now + TimeDelta::seconds(900)),
            CircuitState::HalfOpen
        );
        assert!(on_success(&mut st, now));
        assert_eq!(circuit_state(&st, now), CircuitState::Closed);
        assert_eq!(hold(&st, now), None);
    }

    #[test]
    fn any_failure_while_half_open_reopens() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let mut st = ProviderFetchState::default();
        on_failure(&mut st, now, Failure::Fatal, "HTTP 403".into(), &policy());
        let later = now + TimeDelta::seconds(900);
        assert_eq!(circuit_state(&st, later), CircuitState::HalfOpen);
        // Count is now 2, below the threshold of 5, yet it must reopen.
        let eff = on_failure(
            &mut st,
            later,
            Failure::Transient,
            "HTTP 500".into(),
            &policy(),
        );
        assert!(!eff.opened, "re-open, not a fresh open");
        assert_eq!(st.consecutive_failures, 2);
        assert_eq!(
            circuit_state(&st, later),
            CircuitState::Open {
                until: later + TimeDelta::seconds(900)
            }
        );
    }

    #[test]
    fn short_retry_after_does_not_beat_backoff() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let mut st = ProviderFetchState::default();
        let f = Failure::RateLimited(Some(Duration::from_secs(0)));
        let eff = on_failure(&mut st, now, f, "429".into(), &policy());
        assert_eq!(eff.not_before, now + TimeDelta::seconds(30));
    }

    #[test]
    fn retry_after_overrides_backoff_but_is_capped() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let mut st = ProviderFetchState::default();
        let f = Failure::RateLimited(Some(Duration::from_secs(10 * 86400)));
        let eff = on_failure(&mut st, now, f, "429".into(), &policy());
        assert_eq!(eff.not_before, now + TimeDelta::days(1));
    }
}
