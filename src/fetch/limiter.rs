//! Minimum spacing between requests to one provider, with random jitter so
//! our traffic does not look like a metronome.
//!
//! The limiter only computes *when* the next request may go out; the fetch
//! layer holds a per-provider lock while waiting and sending, so requests to
//! one provider never overlap and each waiter sees the previous request's
//! time.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};

/// A source of jitter samples in `[-1.0, 1.0]`. Swappable so tests can be
/// deterministic.
pub trait Jitter: Send + Sync {
    fn sample(&self) -> f64;
}

/// Uniformly random jitter (the default).
#[derive(Debug, Default)]
pub struct RandomJitter;

impl Jitter for RandomJitter {
    fn sample(&self) -> f64 {
        // fastrand::f64() is in [0, 1); stretch it to [-1, 1).
        fastrand::f64() * 2.0 - 1.0
    }
}

/// Test support: returns the given samples in order, then repeats them.
#[derive(Debug)]
pub struct SequenceJitter {
    samples: Mutex<VecDeque<f64>>,
}

impl SequenceJitter {
    pub fn new(samples: Vec<f64>) -> Self {
        assert!(
            !samples.is_empty(),
            "SequenceJitter needs at least one sample"
        );
        SequenceJitter {
            samples: Mutex::new(samples.into()),
        }
    }
}

impl Jitter for SequenceJitter {
    fn sample(&self) -> f64 {
        let mut q = self.samples.lock().unwrap();
        let s = q.pop_front().expect("never empty");
        q.push_back(s);
        s
    }
}

pub(crate) struct Limiter {
    min_interval: Duration,
    /// How far jitter may move the interval, as a fraction (0.5 = ±50%).
    jitter_fraction: f64,
    jitter: Arc<dyn Jitter>,
    /// When the previous request to this provider was sent (in this process).
    last_request: Option<DateTime<Utc>>,
}

impl Limiter {
    pub(crate) fn new(
        min_interval: Duration,
        jitter_fraction: f64,
        jitter: Arc<dyn Jitter>,
    ) -> Self {
        Limiter {
            min_interval,
            // Clamp so the interval can never become negative.
            jitter_fraction: jitter_fraction.clamp(0.0, 1.0),
            jitter,
            last_request: None,
        }
    }

    /// The earliest time the next request may be sent, or `None` if it may
    /// go at once. Draws a new jitter sample each time it is called with a
    /// previous request on record.
    pub(crate) fn next_allowed(&self) -> Option<DateTime<Utc>> {
        let last = self.last_request?;
        if self.min_interval.is_zero() {
            return None;
        }
        let sample = self.jitter.sample().clamp(-1.0, 1.0);
        let factor = 1.0 + self.jitter_fraction * sample;
        let interval = self.min_interval.mul_f64(factor);
        let interval = TimeDelta::from_std(interval).unwrap_or(TimeDelta::MAX);
        last.checked_add_signed(interval)
    }

    /// Record that a request is being sent now.
    pub(crate) fn mark_sent(&mut self, at: DateTime<Utc>) {
        self.last_request = Some(at);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn interval_follows_jitter_samples() {
        let t = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let mut l = Limiter::new(
            Duration::from_secs(2),
            0.5,
            Arc::new(SequenceJitter::new(vec![-1.0, 1.0, 0.0, 7.0])),
        );
        assert_eq!(l.next_allowed(), None, "no previous request");
        l.mark_sent(t);
        let gaps: Vec<i64> = (0..4)
            .map(|_| (l.next_allowed().unwrap() - t).num_milliseconds())
            .collect();
        // The out-of-range sample 7.0 is clamped to 1.0.
        assert_eq!(gaps, [1000, 3000, 2000, 3000]);
    }

    #[test]
    fn random_jitter_stays_in_range() {
        for _ in 0..1000 {
            let s = RandomJitter.sample();
            assert!((-1.0..=1.0).contains(&s));
        }
    }
}
