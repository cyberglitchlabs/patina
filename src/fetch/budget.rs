//! Daily request budget, per provider (shared by all its lineups).
//!
//! The count lives in the store, keyed by UTC calendar day, so restarting the
//! process cannot reset it. A request is charged *before* it is sent, so a
//! crash in the middle of a request still counts.

use anyhow::Result;
use chrono::{DateTime, NaiveDate, Utc};

use crate::model::ProviderId;
use crate::store::Store;

/// The budget day a moment belongs to: its UTC calendar date.
pub(crate) fn budget_day(now: DateTime<Utc>) -> NaiveDate {
    now.date_naive()
}

/// Cheap early check: true if the day's budget is already used up.
pub(crate) async fn exhausted(
    store: &dyn Store,
    provider: &ProviderId,
    now: DateTime<Utc>,
    limit: u32,
) -> Result<bool> {
    Ok(store.budget_used(provider, budget_day(now)).await? >= limit)
}

/// Charge one request. Returns true if it fits in the budget and may be
/// sent. The increment is atomic in the store, so this is the real gate: even
/// if the early check raced with something, at most `limit` requests are ever
/// approved per day. (A refused charge stays counted; that only makes us
/// more conservative.)
pub(crate) async fn charge(
    store: &dyn Store,
    provider: &ProviderId,
    now: DateTime<Utc>,
    limit: u32,
) -> Result<bool> {
    Ok(store.budget_add(provider, budget_day(now), 1).await? <= limit)
}
