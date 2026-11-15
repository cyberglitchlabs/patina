//! Tests of window ingest (`patina::ingest::WindowIngest`): one tick's
//! provider windows going into the master guide, driven against a real SQLite
//! store. No HTTP, no scheduler, no fake provider. A parsed window goes in,
//! a [`WindowIngested`] comes out, and the store shows what happened.
//!
//! Times are minutes after 2026-11-01 00:00 UTC (`t`); `now` is minute 0, so
//! programmes at minute 600 are in the future.

mod common;
mod support;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use common::merge_util::{ch, obs, pid, t};
use patina::config::Weights;
use patina::ingest::WindowIngest;
use patina::model::{ChannelId, Observation};
use patina::provider::{ParsedWindow, ProviderChannel};
use patina::store::{Store, connect};
use support::failing_store::FailingStore;

const NOW: i64 = 0;
const WINDOW: (i64, i64) = (0, 360);

fn wcco() -> ProviderChannel {
    ProviderChannel {
        provider_channel_id: "10001".into(),
        number: "4.1".into(),
        call_sign: "WCCO-DT".into(),
    }
}

fn kare() -> ProviderChannel {
    ProviderChannel {
        provider_channel_id: "10002".into(),
        number: "11.1".into(),
        call_sign: "KARE-DT".into(),
    }
}

fn kare_id() -> ChannelId {
    ChannelId("KAREDT.us".into())
}

fn on_kare(mut o: Observation) -> Observation {
    o.channel = kare_id();
    o
}

fn window(channels: Vec<ProviderChannel>, observations: Vec<Observation>) -> ParsedWindow {
    ParsedWindow {
        channels,
        observations,
        ..ParsedWindow::default()
    }
}

async fn store() -> Arc<dyn Store> {
    connect("sqlite::memory:").await.unwrap()
}

async fn slots(store: &dyn Store, channel: &ChannelId) -> usize {
    store
        .slots_with_observations(channel, t(-20_000), t(20_000))
        .await
        .unwrap()
        .len()
}

/// Ingest one window of provider `a` as the scheduler would.
async fn ingest_window(
    ingest: &mut WindowIngest,
    parsed: &ParsedWindow,
    now: i64,
) -> patina::ingest::WindowIngested {
    ingest
        .window(&pid("a"), "ota", t(WINDOW.0)..t(WINDOW.1), parsed, t(now))
        .await
}

#[tokio::test]
async fn a_window_stores_its_channels_and_merges_each_listed_one() {
    let store = store().await;
    let parsed = window(
        vec![wcco(), kare()],
        vec![
            obs("a", 100, 160, "News", NOW),
            on_kare(obs("a", 100, 160, "Weather", NOW)),
        ],
    );

    let mut ingest = WindowIngest::begin(store.clone(), &Weights::default(), None).await;
    let done = ingest_window(&mut ingest, &parsed, NOW).await;
    let seen = ingest.finish().await;

    assert!(done.integrity.is_some());
    assert!(done.stored);
    assert_eq!(done.merge_errors, 0);
    assert_eq!(done.stats.inserted, 2);
    assert_eq!(seen, 2);
    assert_eq!(slots(store.as_ref(), &ch()).await, 1);
    assert_eq!(slots(store.as_ref(), &kare_id()).await, 1);
}

/// The order is: store channels, then the timeline check, then the merges.
/// When the channels cannot be stored the check is never reached and nothing
/// is merged.
#[tokio::test]
async fn when_channels_cannot_be_stored_nothing_is_checked_or_merged() {
    let inner = store().await;
    let failing = Arc::new(FailingStore::new(inner.clone()));
    failing.fail_channel_storage.store(true, Ordering::SeqCst);
    let parsed = window(vec![wcco()], vec![obs("a", 100, 160, "News", NOW)]);

    let mut ingest = WindowIngest::begin(failing.clone(), &Weights::default(), None).await;
    let done = ingest_window(&mut ingest, &parsed, NOW).await;
    let seen = ingest.finish().await;

    assert_eq!(done.integrity, None, "the check was not reached");
    assert!(!done.stored);
    assert_eq!(done.stats.inserted, 0);
    assert_eq!(done.merge_errors, 0);
    assert_eq!(seen, 0);
    assert_eq!(slots(inner.as_ref(), &ch()).await, 0);
}

/// A merge that fails leaves that channel unchanged and counts as an error;
/// the check was already done, the other channels carry on, and the failed
/// channel still counts as seen.
#[tokio::test]
async fn a_failed_merge_leaves_its_channel_and_the_others_carry_on() {
    let inner = store().await;
    let failing = Arc::new(FailingStore::new(inner.clone()));
    *failing.fail_apply_channel.lock().unwrap() = Some(ch());
    let parsed = window(
        vec![wcco(), kare()],
        vec![
            obs("a", 100, 160, "News", NOW),
            on_kare(obs("a", 100, 160, "Weather", NOW)),
        ],
    );

    let mut ingest = WindowIngest::begin(failing.clone(), &Weights::default(), None).await;
    let done = ingest_window(&mut ingest, &parsed, NOW).await;
    let seen = ingest.finish().await;

    assert!(
        done.integrity.is_some(),
        "the check comes before the merges"
    );
    assert!(!done.stored);
    assert_eq!(done.merge_errors, 1);
    assert_eq!(done.stats.inserted, 1, "only the other channel was stored");
    assert_eq!(seen, 2, "a channel counts as seen before its merge");
    assert_eq!(slots(inner.as_ref(), &ch()).await, 0);
    assert_eq!(slots(inner.as_ref(), &kare_id()).await, 1);
}

/// Hindsight accuracy is kept in memory during a tick and written by
/// `finish`: a later listing of a future slot, an hour or more after the
/// first, produces one stored accuracy row.
#[tokio::test]
async fn finishing_a_tick_writes_the_hindsight_accuracy_it_collected() {
    let store = store().await;
    let first = window(vec![wcco()], vec![obs("a", 600, 660, "News", -180)]);
    let later = window(vec![wcco()], vec![obs("a", 610, 670, "News", -60)]);

    let mut tick = WindowIngest::begin(store.clone(), &Weights::default(), None).await;
    ingest_window(&mut tick, &first, NOW).await;
    tick.finish().await;
    assert!(store.provider_accuracy_load_all().await.unwrap().is_empty());

    let mut tick = WindowIngest::begin(store.clone(), &Weights::default(), None).await;
    let done = ingest_window(&mut tick, &later, NOW).await;
    assert!(done.stored);
    assert!(
        store.provider_accuracy_load_all().await.unwrap().is_empty(),
        "nothing is written before the tick finishes"
    );
    tick.finish().await;

    let rows = store.provider_accuracy_load_all().await.unwrap();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(
        (rows[0].provider.clone(), rows[0].channel.clone()),
        (pid("a"), ch())
    );
    assert_eq!(rows[0].samples, 1);
}

/// A merge that fails must not feed hindsight, even when the response would
/// have produced a sample.
#[tokio::test]
async fn a_failed_merge_feeds_no_hindsight() {
    let inner = store().await;
    let first = window(vec![wcco()], vec![obs("a", 600, 660, "News", -180)]);
    let mut tick = WindowIngest::begin(inner.clone(), &Weights::default(), None).await;
    ingest_window(&mut tick, &first, NOW).await;
    tick.finish().await;

    let failing = Arc::new(FailingStore::new(inner.clone()));
    *failing.fail_apply_channel.lock().unwrap() = Some(ch());
    let later = window(vec![wcco()], vec![obs("a", 610, 670, "News", -60)]);
    let mut tick = WindowIngest::begin(failing.clone(), &Weights::default(), None).await;
    let done = ingest_window(&mut tick, &later, NOW).await;
    tick.finish().await;

    assert_eq!(done.merge_errors, 1);
    assert!(inner.provider_accuracy_load_all().await.unwrap().is_empty());
}
