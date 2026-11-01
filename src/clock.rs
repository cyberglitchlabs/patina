//! Time source. Code that makes decisions based on time (budgets, backoff,
//! circuit breaker, rate limiting) asks a `Clock` instead of calling
//! `Utc::now()` directly, so tests can control time with [`FakeClock`].

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};

#[async_trait]
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;

    /// Wait until `deadline` (returns at once if it has passed).
    async fn sleep_until(&self, deadline: DateTime<Utc>);
}

/// The real wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

#[async_trait]
impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    async fn sleep_until(&self, deadline: DateTime<Utc>) {
        // A negative difference (deadline already passed) becomes an error
        // from `to_std`, which we treat as "no wait".
        if let Ok(wait) = (deadline - Utc::now()).to_std() {
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
    now: Mutex<DateTime<Utc>>,
}

impl FakeClock {
    pub fn new(start: DateTime<Utc>) -> Self {
        FakeClock {
            now: Mutex::new(start),
        }
    }

    /// Move time forward.
    pub fn advance(&self, by: Duration) {
        let mut now = self.now.lock().unwrap();
        *now += TimeDelta::from_std(by).expect("duration out of range");
    }

    /// Jump to a given time (forwards or backwards).
    pub fn set(&self, to: DateTime<Utc>) {
        *self.now.lock().unwrap() = to;
    }
}

#[async_trait]
impl Clock for FakeClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap()
    }

    async fn sleep_until(&self, deadline: DateTime<Utc>) {
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
