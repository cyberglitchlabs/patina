//! Daily request budget, per provider (shared by all its lineups).
//!
//! The count lives in the store, keyed by UTC calendar day, so restarting the
//! process cannot reset it. A request is charged *before* it is sent, so a
//! crash in the middle of a request still counts.

use anyhow::Result;
use chrono::{DateTime, NaiveDate, Utc};

use crate::model::ProviderId;
use crate::store::Store;

/// One provider's daily request budget: the limit, and the rule that a day is
/// a UTC calendar date. The counting itself is in the store.
#[derive(Clone, Copy)]
pub(crate) struct Budget {
    limit: u32,
}

impl Budget {
    pub(crate) fn new(limit: u32) -> Self {
        Budget { limit }
    }

    pub(crate) fn limit(self) -> u32 {
        self.limit
    }

    fn day(now: DateTime<Utc>) -> NaiveDate {
        now.date_naive()
    }

    /// Requests already charged today.
    pub(crate) async fn used(
        self,
        store: &dyn Store,
        provider: &ProviderId,
        now: DateTime<Utc>,
    ) -> Result<u32> {
        store.budget_used(provider, Self::day(now)).await
    }

    /// Cheap early check: true if the day's budget is already used up.
    pub(crate) async fn exhausted(
        self,
        store: &dyn Store,
        provider: &ProviderId,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        Ok(self.used(store, provider, now).await? >= self.limit)
    }

    /// Charge one request. Returns true if it fits in the budget and may be
    /// sent. The increment is atomic in the store, so this is the real gate:
    /// even if the early check raced with something, at most `limit` requests
    /// are ever approved per day. (A refused charge stays counted; that only
    /// makes us more conservative.)
    pub(crate) async fn charge(
        self,
        store: &dyn Store,
        provider: &ProviderId,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        Ok(store.budget_add(provider, Self::day(now), 1).await? <= self.limit)
    }
}
