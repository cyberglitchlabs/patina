//! Time source. Code that makes decisions based on time (budgets, backoff,
//! circuit breaker, rate limiting) asks a `Clock` instead of calling
//! `Timestamp::now()` directly, so tests can control time with [`FakeClock`].

use jiff::{SignedDuration, Timestamp};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;

#[async_trait]
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;

    /// Wait until `deadline` (returns at once if it has passed).
    async fn sleep_until(&self, deadline: Timestamp);
}

/// The real wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

#[async_trait]
impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        Timestamp::now()
    }

    async fn sleep_until(&self, deadline: Timestamp) {
        // A negative difference (deadline already passed) becomes an error
        // from the conversion, which we treat as "no wait".
        if let Ok(wait) = Duration::try_from(deadline - Timestamp::now()) {
            tokio::time::sleep(wait).await;
        }
    }
}

/// Test support: a clock that only moves when told to.
///
/// Always compiled (it is tiny) so integration tests in `tests/` can use it;
/// production code never constructs one. `sleep_until` does not wait at all,
/// it jumps the clock forward to the deadline, so code that "sleeps" for
/// seconds runs instantly and deterministically in tests.
#[derive(Debug)]
pub struct FakeClock {
    now: Mutex<Timestamp>,
}

impl FakeClock {
    pub fn new(start: Timestamp) -> Self {
        FakeClock {
            now: Mutex::new(start),
        }
    }

    /// Move time forward.
    pub fn advance(&self, by: Duration) {
        let mut now = self.now.lock().unwrap();
        *now += SignedDuration::try_from(by).expect("duration out of range");
    }

    /// Jump to a given time (forwards or backwards).
    pub fn set(&self, to: Timestamp) {
        *self.now.lock().unwrap() = to;
    }
}

/// Whole Unix seconds of `t`, rounded down. (`Timestamp::as_second` rounds
/// towards zero, which is the wrong way for a time just before 1970.)
pub(crate) fn floor_second(t: Timestamp) -> i64 {
    t.as_second() - i64::from(t.subsec_nanosecond() < 0)
}

#[async_trait]
impl Clock for FakeClock {
    fn now(&self) -> Timestamp {
        *self.now.lock().unwrap()
    }

    async fn sleep_until(&self, deadline: Timestamp) {
        {
            let mut now = self.now.lock().unwrap();
            if deadline > *now {
                *now = deadline;
            }
        }
        // Let other tasks run, as a real sleep would.
        tokio::task::yield_now().await;
    }
}
