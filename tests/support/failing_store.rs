//! A `Store` wrapper for tests that delegates to a real store but can be
//! told to fail particular calls, to test how callers handle store errors.
#![allow(dead_code)]

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use patina::channels::TunerChannel;
use patina::merge::{MergeApplied, MergePlan, SlotWithObservations};
use patina::model::{CanonicalChannel, ChannelId, MasterSlot, ProviderId};
use patina::provider::ProviderChannel;
use patina::store::{
    AccuracyRow, DeletedImages, GuideStats, ProviderFetchState, RawEntry, ReliabilityRow,
    ScoreHistoryRow, StateTable, Store, WindowState,
};

pub struct FailingStore {
    pub inner: Arc<dyn Store>,
    /// `apply_merge` fails for any plan that writes an observation of this
    /// channel (None: never).
    pub fail_apply_channel: Mutex<Option<ChannelId>>,
    /// How many `apply_merge` calls were made to fail.
    pub apply_failures: AtomicUsize,
    /// `window_state_get` fails while this is set.
    pub fail_window_state_get: AtomicBool,
    /// `bad_window_is_bad` (one of the fetch layer's gates before a request)
    /// fails while this is set: a database hiccup in the middle of a tick.
    pub fail_bad_window_check: AtomicBool,
    /// `ping` and `guide_stats` fail while this is set (a database that has
    /// gone away, for the status/metrics tests).
    pub fail_stats: AtomicBool,
    /// The calls behind /status and /metrics (ping, guide_stats, reliability,
    /// budget_used, fetch_state_load, provider_channels_list) never finish
    /// while this is set: a database that accepts connections but never
    /// answers.
    pub hang_stats: AtomicBool,
    /// How many times `guide_stats` was called.
    pub guide_stats_calls: AtomicUsize,
    /// `delete_raw_older_than` fails while this is set (a janitor category
    /// that breaks while the others work).
    pub fail_raw_delete: AtomicBool,
    /// Every janitor delete call that succeeded: (method name, rows deleted).
    pub delete_log: Mutex<Vec<(&'static str, u64)>>,
    /// Cancelled right after the next `delete_aired_slots_older_than`
    /// returns, to stop a janitor run between two chunks.
    pub cancel_after_slot_delete: Mutex<Option<tokio_util::sync::CancellationToken>>,
}

impl FailingStore {
    pub fn new(inner: Arc<dyn Store>) -> Self {
        FailingStore {
            inner,
            fail_apply_channel: Mutex::new(None),
            apply_failures: AtomicUsize::new(0),
            fail_window_state_get: AtomicBool::new(false),
            fail_bad_window_check: AtomicBool::new(false),
            fail_stats: AtomicBool::new(false),
            hang_stats: AtomicBool::new(false),
            guide_stats_calls: AtomicUsize::new(0),
            fail_raw_delete: AtomicBool::new(false),
            delete_log: Mutex::new(Vec::new()),
            cancel_after_slot_delete: Mutex::new(None),
        }
    }

    fn log_delete(&self, name: &'static str, result: &Result<u64>) {
        if let Ok(n) = result {
            self.delete_log.lock().unwrap().push((name, *n));
        }
    }

    /// Sleeps (practically) forever when `hang_stats` is set.
    async fn maybe_hang(&self) {
        if self.hang_stats.load(Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        }
    }

    fn fail_apply_for(&self, plan: &MergePlan) -> bool {
        let target = self.fail_apply_channel.lock().unwrap().clone();
        target.is_some_and(|c| plan.observations.iter().any(|w| w.observation.channel == c))
    }
}

#[async_trait]
impl Store for FailingStore {
    async fn migrate(&self) -> Result<()> {
        self.inner.migrate().await
    }

    async fn ping(&self) -> Result<()> {
        self.maybe_hang().await;
        if self.fail_stats.load(Ordering::SeqCst) {
            // The text carries a fake password on purpose: tests check that
            // no endpoint ever repeats a database error verbatim.
            anyhow::bail!("injected ping failure (postgres://patina:s3cretpw@db.internal/patina)");
        }
        self.inner.ping().await
    }

    async fn guide_stats(&self, now: DateTime<Utc>) -> Result<GuideStats> {
        self.guide_stats_calls.fetch_add(1, Ordering::SeqCst);
        self.maybe_hang().await;
        if self.fail_stats.load(Ordering::SeqCst) {
            anyhow::bail!(
                "injected guide_stats failure (postgres://patina:s3cretpw@db.internal/patina)"
            );
        }
        self.inner.guide_stats(now).await
    }

    async fn upsert_channels(&self, channels: &[CanonicalChannel]) -> Result<()> {
        self.inner.upsert_channels(channels).await
    }

    async fn list_channels(&self) -> Result<Vec<CanonicalChannel>> {
        self.inner.list_channels().await
    }

    async fn slots(
        &self,
        channel: ChannelId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<MasterSlot>> {
        self.inner.slots(channel, from, to).await
    }

    async fn slots_with_observations(
        &self,
        channel: &ChannelId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<SlotWithObservations>> {
        self.inner.slots_with_observations(channel, from, to).await
    }

    async fn apply_merge(&self, plan: MergePlan) -> Result<MergeApplied> {
        if self.fail_apply_for(&plan) {
            self.apply_failures.fetch_add(1, Ordering::SeqCst);
            anyhow::bail!("injected apply_merge failure");
        }
        self.inner.apply_merge(plan).await
    }

    async fn budget_used(&self, provider: &ProviderId, day: NaiveDate) -> Result<u32> {
        self.maybe_hang().await;
        self.inner.budget_used(provider, day).await
    }

    async fn budget_add(&self, provider: &ProviderId, day: NaiveDate, n: u32) -> Result<u32> {
        self.inner.budget_add(provider, day, n).await
    }

    async fn bad_window_mark(
        &self,
        provider: &ProviderId,
        lineup: &str,
        window_start: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<()> {
        self.inner
            .bad_window_mark(provider, lineup, window_start, until)
            .await
    }

    async fn bad_window_is_bad(
        &self,
        provider: &ProviderId,
        lineup: &str,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        if self.fail_bad_window_check.load(Ordering::SeqCst) {
            anyhow::bail!("injected bad_window_is_bad failure");
        }
        self.inner
            .bad_window_is_bad(provider, lineup, window_start, now)
            .await
    }

    async fn raw_put(
        &self,
        provider: &ProviderId,
        lineup: &str,
        window_start: DateTime<Utc>,
        fetched_at: DateTime<Utc>,
        body: &[u8],
    ) -> Result<()> {
        self.inner
            .raw_put(provider, lineup, window_start, fetched_at, body)
            .await
    }

    async fn raw_get_latest(
        &self,
        provider: &ProviderId,
        lineup: &str,
        window_start: DateTime<Utc>,
        max_age: Duration,
        now: DateTime<Utc>,
    ) -> Result<Option<RawEntry>> {
        self.inner
            .raw_get_latest(provider, lineup, window_start, max_age, now)
            .await
    }

    async fn fetch_state_load(&self, provider: &ProviderId) -> Result<Option<ProviderFetchState>> {
        self.maybe_hang().await;
        self.inner.fetch_state_load(provider).await
    }

    async fn fetch_state_save(
        &self,
        provider: &ProviderId,
        state: &ProviderFetchState,
    ) -> Result<()> {
        self.inner.fetch_state_save(provider, state).await
    }

    async fn save_tuner_channels(
        &self,
        source_key: &str,
        channels: &[TunerChannel],
        now: DateTime<Utc>,
    ) -> Result<()> {
        self.inner
            .save_tuner_channels(source_key, channels, now)
            .await
    }

    async fn load_tuner_channels(
        &self,
        source_key: &str,
    ) -> Result<Option<(Vec<TunerChannel>, DateTime<Utc>)>> {
        self.inner.load_tuner_channels(source_key).await
    }

    async fn provider_accuracy_load_all(&self) -> Result<Vec<AccuracyRow>> {
        self.inner.provider_accuracy_load_all().await
    }

    async fn provider_accuracy_upsert(&self, row: &AccuracyRow) -> Result<()> {
        self.inner.provider_accuracy_upsert(row).await
    }

    async fn provider_reliability_record(
        &self,
        provider: &ProviderId,
        success: bool,
        at: DateTime<Utc>,
    ) -> Result<()> {
        self.inner
            .provider_reliability_record(provider, success, at)
            .await
    }

    async fn provider_reliability_load_all(&self) -> Result<Vec<ReliabilityRow>> {
        self.maybe_hang().await;
        self.inner.provider_reliability_load_all().await
    }

    async fn score_history_append(&self, rows: &[ScoreHistoryRow]) -> Result<()> {
        self.inner.score_history_append(rows).await
    }

    async fn score_history_delete_older_than(
        &self,
        cutoff: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64> {
        let r = self
            .inner
            .score_history_delete_older_than(cutoff, limit)
            .await;
        self.log_delete("score_history_delete_older_than", &r);
        r
    }

    async fn delete_raw_older_than(&self, cutoff: DateTime<Utc>, limit: u32) -> Result<u64> {
        if self.fail_raw_delete.load(Ordering::SeqCst) {
            // Looks like a driver error, so tests can check it never reaches
            // a report.
            anyhow::bail!("injected raw delete failure (postgres://patina:s3cretpw@db/patina)");
        }
        let r = self.inner.delete_raw_older_than(cutoff, limit).await;
        self.log_delete("delete_raw_older_than", &r);
        r
    }

    async fn delete_aired_slots_older_than(
        &self,
        cutoff: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64> {
        let r = self
            .inner
            .delete_aired_slots_older_than(cutoff, limit)
            .await;
        self.log_delete("delete_aired_slots_older_than", &r);
        if let Some(token) = self.cancel_after_slot_delete.lock().unwrap().take() {
            token.cancel();
        }
        r
    }

    async fn images_clear_for_slots_older_than(
        &self,
        cutoff: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64> {
        let r = self
            .inner
            .images_clear_for_slots_older_than(cutoff, limit)
            .await;
        self.log_delete("images_clear_for_slots_older_than", &r);
        r
    }

    async fn images_delete_unreferenced(&self, limit: u32) -> Result<DeletedImages> {
        let r = self.inner.images_delete_unreferenced(limit).await;
        if let Ok(d) = &r {
            self.delete_log
                .lock()
                .unwrap()
                .push(("images_delete_unreferenced", d.rows));
        }
        r
    }

    async fn delete_state_older_than(
        &self,
        table: StateTable,
        cutoff: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64> {
        let r = self
            .inner
            .delete_state_older_than(table, cutoff, limit)
            .await;
        self.log_delete("delete_state_older_than", &r);
        r
    }

    async fn approx_size_bytes(&self) -> Result<u64> {
        self.inner.approx_size_bytes().await
    }

    async fn provider_channels_upsert(
        &self,
        provider: &ProviderId,
        lineup: &str,
        channels: &[ProviderChannel],
        now: DateTime<Utc>,
    ) -> Result<()> {
        self.inner
            .provider_channels_upsert(provider, lineup, channels, now)
            .await
    }

    async fn provider_channels_list(
        &self,
        provider: &ProviderId,
        lineup: &str,
    ) -> Result<Vec<ProviderChannel>> {
        self.maybe_hang().await;
        self.inner.provider_channels_list(provider, lineup).await
    }

    async fn window_state_get(
        &self,
        provider: &ProviderId,
        lineup: &str,
        window_start: DateTime<Utc>,
    ) -> Result<Option<WindowState>> {
        if self.fail_window_state_get.load(Ordering::SeqCst) {
            anyhow::bail!("injected window_state_get failure");
        }
        self.inner
            .window_state_get(provider, lineup, window_start)
            .await
    }

    async fn window_state_put(
        &self,
        provider: &ProviderId,
        lineup: &str,
        state: &WindowState,
    ) -> Result<()> {
        self.inner.window_state_put(provider, lineup, state).await
    }

    async fn window_state_range(
        &self,
        provider: &ProviderId,
        lineup: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<WindowState>> {
        self.inner
            .window_state_range(provider, lineup, from, to)
            .await
    }
}
