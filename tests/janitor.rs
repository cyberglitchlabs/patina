//! Janitor tests: retention per category, the disk cap, chunking,
//! cancellation, and the safety rule that no slot with `end > now` is ever
//! deleted. Run against a real SQLite file (the Store methods themselves are
//! covered on both backends by the contract in tests/common/mod.rs).
//!
//! Times: `now()` is fixed by a `FakeClock`; "aged N days" means the row's
//! timestamp is `now() - N days`. The rule everywhere is "delete when
//! strictly older than the cutoff" (`cutoff = now - retention`), so a row
//! exactly at the cutoff is kept (see `retention_boundaries_are_exact`).

mod common;
mod support;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, TimeDelta, TimeZone, Utc};
use patina::clock::FakeClock;
use patina::config::Retention;
use patina::janitor::{Janitor, JanitorReport, ReportSink};
use patina::merge::{MergePlan, NewSlot, ObservationWrite, SlotFields, SlotRef};
use patina::metrics::Metrics;
use patina::model::{CanonicalChannel, ChannelId, Observation, ProviderId, ScoreBreakdown, SlotId};
use patina::provider::ProviderChannel;
use patina::store::{ScoreHistoryRow, Store, WindowState, connect};
use proptest::prelude::*;
use sqlx::{Connection, SqliteConnection};
use support::failing_store::FailingStore;
use tokio_util::sync::CancellationToken;

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 1, 12, 0, 0).unwrap()
}

fn ago(d: TimeDelta) -> DateTime<Utc> {
    now() - d
}

fn days(n: i64) -> TimeDelta {
    TimeDelta::days(n)
}

fn pid(name: &str) -> ProviderId {
    ProviderId(name.to_string())
}

/// One slot to seed: times relative to `now()`.
#[derive(Clone)]
struct Seed {
    channel: &'static str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    image: Option<String>,
}

/// A slot that ended `ended_ago` ago, one hour long, on channel C1.
fn aired(ended_ago: TimeDelta, image: Option<&str>) -> Seed {
    Seed {
        channel: "C1",
        start: ago(ended_ago) - TimeDelta::hours(1),
        end: ago(ended_ago),
        image: image.map(str::to_string),
    }
}

struct Env {
    _dir: tempfile::TempDir,
    url: String,
    store: Arc<dyn Store>,
    clock: Arc<FakeClock>,
}

impl Env {
    async fn new() -> Env {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("j.db").display());
        let store = connect(&url).await.unwrap();
        for id in ["C1", "C2"] {
            store
                .upsert_channels(&[CanonicalChannel {
                    id: ChannelId(id.into()),
                    number: "4.1".into(),
                    name: id.into(),
                    call_sign: id.into(),
                }])
                .await
                .unwrap();
        }
        Env {
            _dir: dir,
            url,
            store,
            clock: Arc::new(FakeClock::new(now())),
        }
    }

    fn janitor(&self, retention: Retention, cap: Option<u64>) -> Janitor {
        Janitor::new(self.store.clone(), retention, cap, self.clock.clone()).with_chunk_size(500)
    }

    /// Insert slots (each with one observation, as the merge does).
    async fn add_slots(&self, seeds: &[Seed]) {
        add_slots_to(&self.store, seeds).await;
    }

    /// Run a counting query directly against the database file.
    async fn count(&self, sql: &'static str) -> i64 {
        let mut c = SqliteConnection::connect(&self.url).await.unwrap();
        sqlx::query_scalar(sql).fetch_one(&mut c).await.unwrap()
    }

    async fn exec(&self, sql: &'static str) {
        let mut c = SqliteConnection::connect(&self.url).await.unwrap();
        sqlx::query(sql).execute(&mut c).await.unwrap();
    }

    async fn slot_count(&self) -> i64 {
        self.count("SELECT COUNT(*) FROM slots").await
    }
}

async fn add_slots_to(store: &Arc<dyn Store>, seeds: &[Seed]) {
    let mut plan = MergePlan::empty(now());
    for (i, s) in seeds.iter().enumerate() {
        plan.inserts.push(NewSlot {
            channel: ChannelId(s.channel.into()),
            fields: SlotFields {
                start: s.start,
                end: s.end,
                title: format!("show {i}"),
                subtitle: None,
                description: None,
                image_url: s.image.clone(),
                season: None,
                episode: None,
                rating: None,
                genres: vec![],
                winner: pid("gracenote"),
                score: 0.5,
                breakdown: ScoreBreakdown::default(),
            },
        });
        plan.observations.push(ObservationWrite {
            slot: SlotRef::New(i),
            observation: Observation {
                provider: pid("gracenote"),
                channel: ChannelId(s.channel.into()),
                start: s.start,
                end: s.end,
                title: format!("show {i}"),
                subtitle: None,
                description: None,
                image_url: s.image.clone(),
                season: None,
                episode: None,
                rating: None,
                genres: vec![],
                fetched_at: now(),
            },
            first_seen: now(),
            changes: 0,
        });
    }
    store.apply_merge(plan).await.unwrap();
}

async fn raw_at(store: &Arc<dyn Store>, fetched_at: DateTime<Utc>, body: &[u8]) {
    store
        .raw_put(&pid("gracenote"), "ota", fetched_at, fetched_at, body)
        .await
        .unwrap();
}

async fn history_at(store: &Arc<dyn Store>, scored_at: DateTime<Utc>) {
    store
        .score_history_append(&[ScoreHistoryRow {
            slot: SlotId(1),
            provider: pid("gracenote"),
            channel: ChannelId("C1".into()),
            scored_at,
            score: 0.5,
            breakdown: ScoreBreakdown::default(),
        }])
        .await
        .unwrap();
}

/// Bytes that gzip cannot shrink, so a stored raw response really takes up
/// roughly `n` bytes of the database (a simple xorshift generator, no crate).
fn noise(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x & 0xff) as u8
        })
        .collect()
}

// ---------------------------------------------------------------- retention

#[tokio::test]
async fn raw_responses_deleted_after_7_days_kept_before() {
    let env = Env::new().await;
    raw_at(&env.store, ago(days(8)), b"old").await;
    raw_at(&env.store, ago(days(6)), b"recent").await;
    let report = env.janitor(Retention::default(), None).run_once().await;
    assert_eq!(report.raw_deleted, 1);
    assert_eq!(env.count("SELECT COUNT(*) FROM raw_responses").await, 1);
    // The survivor is the recent one.
    let left = env
        .store
        .raw_get_latest(
            &pid("gracenote"),
            "ota",
            ago(days(6)),
            Duration::from_secs(10 * 86400),
            now(),
        )
        .await
        .unwrap();
    assert!(left.is_some());
}

#[tokio::test]
async fn aired_programmes_deleted_7_days_after_end() {
    let env = Env::new().await;
    env.add_slots(&[aired(days(8), None), aired(days(6), None)])
        .await;
    let report = env.janitor(Retention::default(), None).run_once().await;
    assert_eq!(report.programmes_deleted, 1);
    assert_eq!(env.slot_count().await, 1);
    let left_end: i64 = env.count("SELECT end_utc FROM slots").await;
    assert_eq!(left_end, ago(days(6)).timestamp());
}

#[tokio::test]
async fn future_and_recent_programmes_are_kept() {
    let env = Env::new().await;
    let n = now();
    env.add_slots(&[
        // On air right now.
        Seed {
            channel: "C1",
            start: n - TimeDelta::minutes(30),
            end: n + TimeDelta::minutes(30),
            image: None,
        },
        // Tomorrow, and next week.
        Seed {
            channel: "C1",
            start: n + days(1),
            end: n + days(1) + TimeDelta::hours(1),
            image: None,
        },
        Seed {
            channel: "C1",
            start: n + days(7),
            end: n + days(7) + TimeDelta::hours(1),
            image: None,
        },
        // Ended an hour ago, and two days ago.
        aired(TimeDelta::hours(1), None),
        aired(days(2), None),
    ])
    .await;
    let report = env.janitor(Retention::default(), None).run_once().await;
    assert_eq!(report.programmes_deleted, 0);
    assert_eq!(env.slot_count().await, 5);
}

#[tokio::test]
async fn history_deleted_after_90_days() {
    let env = Env::new().await;
    history_at(&env.store, ago(days(91))).await;
    history_at(&env.store, ago(days(89))).await;
    let report = env.janitor(Retention::default(), None).run_once().await;
    assert_eq!(report.history_deleted, 1);
    assert_eq!(env.count("SELECT COUNT(*) FROM score_history").await, 1);
}

#[tokio::test]
async fn image_records_removed_3_days_after_last_referencing_programme_airs() {
    let env = Env::new().await;
    let img = Some("https://img/shared.jpg");
    // Two slots share one image: one ended 4 days ago, one 2 days ago.
    env.add_slots(&[aired(days(4), img), aired(days(2), img)])
        .await;
    assert_eq!(env.count("SELECT COUNT(*) FROM images").await, 1);

    let j = env.janitor(Retention::default(), None);
    let report = j.run_once().await;
    // The 4-day-old slot lost its link, but the 2-day-old one still uses it.
    assert_eq!(report.images_deleted, 0);
    assert_eq!(env.count("SELECT COUNT(*) FROM images").await, 1);
    assert_eq!(
        env.count("SELECT COUNT(*) FROM slots WHERE image_id IS NOT NULL")
            .await,
        1
    );
    // Both slots themselves are still there (aired slots live 7 days).
    assert_eq!(env.slot_count().await, 2);

    // Two days later the last referencing programme is 4 days past its end.
    env.clock.advance(Duration::from_secs(2 * 86400));
    let report = j.run_once().await;
    assert_eq!(report.images_deleted, 1);
    assert_eq!(env.count("SELECT COUNT(*) FROM images").await, 0);
    assert_eq!(env.slot_count().await, 2, "the slots stay, only links go");
}

#[tokio::test]
async fn unreferenced_image_records_removed_immediately() {
    let env = Env::new().await;
    env.exec("INSERT INTO images (source_url) VALUES ('https://img/orphan.jpg')")
        .await;
    env.exec(
        "INSERT INTO images (source_url, local_path) VALUES ('https://img/cached.jpg', '/cache/ab/cd.jpg')",
    )
    .await;
    // One referenced by a future slot: must stay.
    let n = now();
    env.add_slots(&[Seed {
        channel: "C1",
        start: n + days(1),
        end: n + days(1) + TimeDelta::hours(1),
        image: Some("https://img/live.jpg".into()),
    }])
    .await;
    let report = env.janitor(Retention::default(), None).run_once().await;
    assert_eq!(report.images_deleted, 2);
    // The file a future image store would have to remove is reported.
    assert_eq!(report.files_to_delete, vec!["/cache/ab/cd.jpg".to_string()]);
    assert_eq!(env.count("SELECT COUNT(*) FROM images").await, 1);
}

// ---------------------------------------------------------------- disk cap

#[tokio::test]
async fn disk_cap_evicts_oldest_first_until_under_cap() {
    let env = Env::new().await;
    // Ten 50 KB incompressible raw responses, one per hour, all within the
    // 7-day retention so only the cap can remove them.
    for i in 0..10 {
        raw_at(
            &env.store,
            ago(TimeDelta::hours(100 - i)),
            &noise(50_000, i as u64 + 1),
        )
        .await;
    }
    // Two aired slots and a future slot that must survive while raw suffices.
    let n = now();
    env.add_slots(&[
        aired(days(2), None),
        Seed {
            channel: "C1",
            start: n + days(1),
            end: n + days(1) + TimeDelta::hours(1),
            image: None,
        },
    ])
    .await;

    let size = env.store.approx_size_bytes().await.unwrap();
    // Ask for a cap that needs about half of the raw data gone.
    let cap = size - 230_000;
    let report = env
        .janitor(Retention::default(), Some(cap))
        .with_chunk_size(10) // eviction chunks are 1/5 of this: 2 rows
        .run_once()
        .await;

    assert!(report.disk_evicted >= 4, "{report:?}");
    assert!(
        report.disk_evicted < 10,
        "stops once under the cap: {report:?}"
    );
    let after = report.size_after.expect("size measured");
    assert!(after <= cap, "after {after} > cap {cap}");
    assert!(report.size_before.unwrap() > cap);
    // Oldest first: the survivors are the newest raw responses.
    let oldest_left: i64 = env.count("SELECT MIN(fetched_at) FROM raw_responses").await;
    let evicted = report.disk_evicted as i64;
    assert_eq!(
        oldest_left,
        ago(TimeDelta::hours(100 - evicted)).timestamp(),
        "exactly the {evicted} oldest were evicted"
    );
    // Slots were not needed.
    assert_eq!(env.slot_count().await, 2);
}

#[tokio::test]
async fn disk_cap_falls_back_to_history_then_aired_slots_and_warns_when_unreachable() {
    let env = Env::new().await;
    let n = now();
    env.add_slots(&[
        aired(days(5), None),
        aired(days(1), None),
        Seed {
            channel: "C1",
            start: n - TimeDelta::minutes(10),
            end: n + TimeDelta::minutes(50),
            image: None,
        },
    ])
    .await;
    history_at(&env.store, ago(days(10))).await;
    // A cap of 1 byte can never be met: everything evictable goes, the
    // slot on air stays.
    let report = env.janitor(Retention::default(), Some(1)).run_once().await;
    assert_eq!(report.disk_evicted, 3, "{report:?}"); // 1 history + 2 aired slots
    assert_eq!(env.count("SELECT COUNT(*) FROM score_history").await, 0);
    assert_eq!(env.slot_count().await, 1);
    let left_end: i64 = env.count("SELECT end_utc FROM slots").await;
    assert_eq!(left_end, (n + TimeDelta::minutes(50)).timestamp());
}

#[tokio::test]
async fn disk_eviction_keeps_aired_slots_the_feed_still_serves() {
    // The feed shows programmes from 3 hours before now (feeds::LOOKBACK).
    // Disk eviction must never remove one of those, however full the disk.
    let env = Env::new().await;
    let lookback = patina::feeds::LOOKBACK;
    env.add_slots(&[
        aired(days(1), None),                          // evictable
        aired(lookback + TimeDelta::minutes(1), None), // evictable
        aired(TimeDelta::hours(1), None),              // in the feed
        aired(lookback - TimeDelta::minutes(1), None), // in the feed
    ])
    .await;
    let report = env.janitor(Retention::default(), Some(1)).run_once().await;
    assert_eq!(report.disk_evicted, 2, "{report:?}");
    assert_eq!(env.slot_count().await, 2);
    let oldest_end: i64 = env.count("SELECT MIN(end_utc) FROM slots").await;
    assert_eq!(
        oldest_end,
        ago(lookback - TimeDelta::minutes(1)).timestamp()
    );
}

// ---------------------------------------------------------------- reports

#[tokio::test]
async fn report_counts_match_rows_deleted() {
    let env = Env::new().await;
    // raw: 3 old, 2 kept.
    for i in 0..3 {
        raw_at(&env.store, ago(days(8) + TimeDelta::hours(i)), b"x").await;
    }
    for i in 0..2 {
        raw_at(&env.store, ago(days(1) + TimeDelta::hours(i)), b"x").await;
    }
    // programmes: 4 old (2 with images), 1 recent with an image.
    env.add_slots(&[
        aired(days(9), Some("https://img/a.jpg")),
        aired(days(10), Some("https://img/b.jpg")),
        aired(days(11), None),
        aired(days(12), None),
        aired(days(4), Some("https://img/c.jpg")),
    ])
    .await;
    // history: 2 old, 1 recent.
    history_at(&env.store, ago(days(100))).await;
    history_at(&env.store, ago(days(95))).await;
    history_at(&env.store, ago(days(1))).await;
    // state: 2 old window_state rows, 1 current.
    for (h, label) in [(-72i64, "old"), (-48, "old"), (0, "now")] {
        let _ = label;
        let w = ago(days(1)) + TimeDelta::hours(h);
        env.store
            .window_state_put(
                &pid("gracenote"),
                "ota",
                &WindowState {
                    window_start: w,
                    last_success: Some(w),
                    last_attempt: w,
                    last_failure: None,
                },
            )
            .await
            .unwrap();
    }

    let before = (
        env.count("SELECT COUNT(*) FROM raw_responses").await,
        env.slot_count().await,
        env.count("SELECT COUNT(*) FROM images").await,
        env.count("SELECT COUNT(*) FROM score_history").await,
        env.count("SELECT COUNT(*) FROM window_state").await,
    );
    let report = env.janitor(Retention::default(), None).run_once().await;
    let after = (
        env.count("SELECT COUNT(*) FROM raw_responses").await,
        env.slot_count().await,
        env.count("SELECT COUNT(*) FROM images").await,
        env.count("SELECT COUNT(*) FROM score_history").await,
        env.count("SELECT COUNT(*) FROM window_state").await,
    );

    assert_eq!(report.raw_deleted as i64, before.0 - after.0);
    assert_eq!(report.programmes_deleted as i64, before.1 - after.1);
    assert_eq!(report.images_deleted as i64, before.2 - after.2);
    assert_eq!(report.history_deleted as i64, before.3 - after.3);
    assert_eq!(report.state_deleted as i64, before.4 - after.4);
    assert_eq!(
        (
            report.raw_deleted,
            report.programmes_deleted,
            report.images_deleted,
            report.history_deleted,
            report.state_deleted,
            report.disk_evicted,
        ),
        (3, 4, 2 + 1, 2, 2, 0)
    );
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.finished_at, now());
    assert_eq!(report.started_at, now());
}

#[tokio::test]
async fn run_once_is_idempotent() {
    let env = Env::new().await;
    raw_at(&env.store, ago(days(8)), b"old").await;
    env.add_slots(&[
        aired(days(9), Some("https://img/a.jpg")),
        aired(days(1), Some("https://img/b.jpg")),
    ])
    .await;
    history_at(&env.store, ago(days(100))).await;
    let j = env.janitor(Retention::default(), None);
    let first = j.run_once().await;
    assert_eq!(
        first.raw_deleted + first.programmes_deleted + first.history_deleted,
        3
    );

    let snapshot = |env: &Env| {
        let url = env.url.clone();
        async move {
            let mut c = SqliteConnection::connect(&url).await.unwrap();
            let slots: Vec<(i64, Option<i64>)> =
                sqlx::query_as("SELECT id, image_id FROM slots ORDER BY id")
                    .fetch_all(&mut c)
                    .await
                    .unwrap();
            let images: Vec<i64> = sqlx::query_scalar("SELECT id FROM images ORDER BY id")
                .fetch_all(&mut c)
                .await
                .unwrap();
            (slots, images)
        }
    };
    let state = snapshot(&env).await;
    let second = j.run_once().await;
    assert_eq!(
        (
            second.raw_deleted,
            second.programmes_deleted,
            second.images_deleted,
            second.history_deleted,
            second.state_deleted,
            second.disk_evicted
        ),
        (0, 0, 0, 0, 0, 0)
    );
    assert_eq!(snapshot(&env).await, state, "second run changed nothing");
}

#[tokio::test]
async fn retention_boundaries_are_exact() {
    // Rule: delete when STRICTLY older than the cutoff (now - retention).
    // A row exactly at the cutoff stays; one second older goes.
    let env = Env::new().await;
    let one_s = TimeDelta::seconds(1);
    // raw
    raw_at(&env.store, ago(days(7)), b"edge").await;
    raw_at(&env.store, ago(days(7) + one_s), b"over").await;
    // history
    history_at(&env.store, ago(days(90))).await;
    history_at(&env.store, ago(days(90) + one_s)).await;
    // programmes (by end)
    env.add_slots(&[
        aired(days(7), None),
        aired(days(7) + one_s, None),
        // a slot whose END is exactly now has ended but is not older than
        // anything; with 0-day retention it must stay too (cutoff == now).
    ])
    .await;
    let report = env.janitor(Retention::default(), None).run_once().await;
    assert_eq!(report.raw_deleted, 1);
    assert_eq!(report.history_deleted, 1);
    assert_eq!(report.programmes_deleted, 1);
    let left: i64 = env.count("SELECT end_utc FROM slots").await;
    assert_eq!(left, ago(days(7)).timestamp());
    let left_raw: i64 = env.count("SELECT fetched_at FROM raw_responses").await;
    assert_eq!(left_raw, ago(days(7)).timestamp());

    // Image links: a slot that ended exactly 3 days ago keeps its link.
    let env = Env::new().await;
    env.add_slots(&[
        aired(days(3), Some("https://img/edge.jpg")),
        aired(days(3) + one_s, Some("https://img/over.jpg")),
    ])
    .await;
    let report = env.janitor(Retention::default(), None).run_once().await;
    assert_eq!(report.images_deleted, 1);
    let kept: String = {
        let mut c = SqliteConnection::connect(&env.url).await.unwrap();
        sqlx::query_scalar("SELECT source_url FROM images")
            .fetch_one(&mut c)
            .await
            .unwrap()
    };
    assert_eq!(kept, "https://img/edge.jpg");

    // Zero-day retention: a slot ending exactly now stays; one that ended
    // a second ago goes; one still on air stays.
    let env = Env::new().await;
    let n = now();
    let zero = Retention {
        images_days: 0,
        raw_days: 0,
        programmes_days: 0,
        history_days: 0,
    };
    env.add_slots(&[
        aired(TimeDelta::zero(), None),
        aired(one_s, None),
        Seed {
            channel: "C1",
            start: n - TimeDelta::minutes(5),
            end: n + one_s,
            image: None,
        },
    ])
    .await;
    let report = env.janitor(zero, None).run_once().await;
    assert_eq!(report.programmes_deleted, 1);
    assert_eq!(env.slot_count().await, 2);
}

#[tokio::test]
async fn observations_of_deleted_slots_are_gone_and_score_history_outlives_them() {
    // Observations are removed with their slot (ON DELETE CASCADE, and the
    // SQLite connections run with foreign keys on). score_history has no
    // foreign key BY DESIGN (migration 0005: history outlives the slots it
    // describes and is pruned by its own 90-day age), so it stays.
    let env = Env::new().await;
    env.add_slots(&[aired(days(9), None), aired(days(1), None)])
        .await;
    assert_eq!(env.count("SELECT COUNT(*) FROM observations").await, 2);
    let old_id: i64 = env
        .count("SELECT id FROM slots ORDER BY end_utc LIMIT 1")
        .await;
    env.store
        .score_history_append(&[ScoreHistoryRow {
            slot: SlotId(old_id),
            provider: pid("gracenote"),
            channel: ChannelId("C1".into()),
            scored_at: ago(days(9)),
            score: 0.9,
            breakdown: ScoreBreakdown::default(),
        }])
        .await
        .unwrap();
    env.janitor(Retention::default(), None).run_once().await;
    assert_eq!(env.slot_count().await, 1);
    assert_eq!(
        env.count("SELECT COUNT(*) FROM observations").await,
        1,
        "the deleted slot's observation is gone"
    );
    assert_eq!(
        env.count("SELECT COUNT(*) FROM observations WHERE slot_id NOT IN (SELECT id FROM slots)")
            .await,
        0
    );
    assert_eq!(
        env.count("SELECT COUNT(*) FROM score_history").await,
        1,
        "history is pruned by age only"
    );
}

// ---------------------------------------------------------------- state

#[tokio::test]
async fn window_state_and_provider_channels_are_pruned() {
    let env = Env::new().await;
    let ws = |start: DateTime<Utc>| WindowState {
        window_start: start,
        last_success: Some(start),
        last_attempt: start,
        last_failure: None,
    };
    // Windows: 2 days ago (pruned), exactly 1 day ago (kept: not strictly
    // older), the current one.
    for w in [ago(days(2)), ago(days(1)), now()] {
        env.store
            .window_state_put(&pid("gracenote"), "ota", &ws(w))
            .await
            .unwrap();
    }
    let chan = |id: &str| ProviderChannel {
        provider_channel_id: id.into(),
        number: "4.1".into(),
        call_sign: id.into(),
    };
    env.store
        .provider_channels_upsert(&pid("gracenote"), "ota", &[chan("old")], ago(days(31)))
        .await
        .unwrap();
    env.store
        .provider_channels_upsert(&pid("gracenote"), "ota", &[chan("edge")], ago(days(30)))
        .await
        .unwrap();
    env.store
        .provider_channels_upsert(&pid("gracenote"), "ota", &[chan("new")], now())
        .await
        .unwrap();
    // Things that must never be touched by the state pass.
    env.exec(
        "INSERT INTO fetch_provider_state (provider, consecutive_failures) VALUES ('gracenote', 3)",
    )
    .await;
    env.exec("INSERT INTO provider_accuracy VALUES ('gracenote', 'C1', 0.5, 3, 0)")
        .await;
    env.exec("INSERT INTO provider_reliability VALUES ('gracenote', 1, 2, 0)")
        .await;
    env.exec("INSERT INTO tuner_channel_lists VALUES ('hd', 0)")
        .await;
    env.exec("INSERT INTO tuner_channels VALUES ('hd', 0, '4.1', 'WCCO')")
        .await;

    let report = env.janitor(Retention::default(), None).run_once().await;
    assert_eq!(report.state_deleted, 2, "1 window + 1 channel: {report:?}");
    assert_eq!(env.count("SELECT COUNT(*) FROM window_state").await, 2);
    let ids = env
        .store
        .provider_channels_list(&pid("gracenote"), "ota")
        .await
        .unwrap();
    let mut ids: Vec<String> = ids.into_iter().map(|c| c.provider_channel_id).collect();
    ids.sort();
    assert_eq!(ids, ["edge", "new"]);
    for table in [
        "fetch_provider_state",
        "provider_accuracy",
        "provider_reliability",
        "tuner_channel_lists",
        "tuner_channels",
        "channels",
    ] {
        let sql: &'static str = match table {
            "fetch_provider_state" => "SELECT COUNT(*) FROM fetch_provider_state",
            "provider_accuracy" => "SELECT COUNT(*) FROM provider_accuracy",
            "provider_reliability" => "SELECT COUNT(*) FROM provider_reliability",
            "tuner_channel_lists" => "SELECT COUNT(*) FROM tuner_channel_lists",
            "tuner_channels" => "SELECT COUNT(*) FROM tuner_channels",
            _ => "SELECT COUNT(*) FROM channels",
        };
        assert!(env.count(sql).await >= 1, "{table} must be untouched");
    }
}

#[tokio::test]
async fn expired_bad_windows_are_pruned_but_budget_of_today_is_kept() {
    let env = Env::new().await;
    let gn = pid("gracenote");
    let w = ago(days(1));
    // One bad-window memory expired an hour ago, one still in force.
    env.store
        .bad_window_mark(&gn, "ota", w, now() - TimeDelta::hours(1))
        .await
        .unwrap();
    env.store
        .bad_window_mark(
            &gn,
            "ota",
            w + TimeDelta::hours(6),
            now() + TimeDelta::hours(1),
        )
        .await
        .unwrap();
    // Budget rows: today, yesterday, 3 days ago (kept), 4 and 10 days ago (gone).
    let day = |back: i64| -> NaiveDate { (now() - days(back)).date_naive() };
    for back in [0, 1, 3, 4, 10] {
        env.store.budget_add(&gn, day(back), 7).await.unwrap();
    }
    let report = env.janitor(Retention::default(), None).run_once().await;
    assert_eq!(report.state_deleted, 1 + 2, "{report:?}");
    // The live bad window is still remembered, the expired one is gone.
    assert_eq!(env.count("SELECT COUNT(*) FROM fetch_bad_windows").await, 1);
    assert!(
        env.store
            .bad_window_is_bad(&gn, "ota", w + TimeDelta::hours(6), now())
            .await
            .unwrap()
    );
    // Today's and yesterday's budget counters are intact.
    assert_eq!(env.store.budget_used(&gn, day(0)).await.unwrap(), 7);
    assert_eq!(env.store.budget_used(&gn, day(1)).await.unwrap(), 7);
    assert_eq!(env.store.budget_used(&gn, day(3)).await.unwrap(), 7);
    assert_eq!(env.store.budget_used(&gn, day(4)).await.unwrap(), 0);
    assert_eq!(env.store.budget_used(&gn, day(10)).await.unwrap(), 0);
}

// ---------------------------------------------------------------- chunks

#[tokio::test]
async fn deletes_happen_in_chunks() {
    let env = Env::new().await;
    let base = ago(days(30));
    let seeds: Vec<Seed> = (0..5000)
        .map(|i| Seed {
            channel: "C1",
            start: base + TimeDelta::minutes(i),
            end: base + TimeDelta::minutes(i + 1),
            image: None,
        })
        .collect();
    env.add_slots(&seeds).await;
    assert_eq!(env.slot_count().await, 5000);

    let failing = Arc::new(FailingStore::new(env.store.clone()));
    let j = Janitor::new(
        failing.clone(),
        Retention::default(),
        None,
        env.clock.clone(),
    )
    .with_chunk_size(500);
    let report = j.run_once().await;
    assert_eq!(report.programmes_deleted, 5000);
    assert_eq!(env.slot_count().await, 0);
    assert_eq!(env.count("SELECT COUNT(*) FROM observations").await, 0);

    let log = failing.delete_log.lock().unwrap().clone();
    let slot_calls: Vec<u64> = log
        .iter()
        .filter(|(name, _)| *name == "delete_aired_slots_older_than")
        .map(|(_, n)| *n)
        .collect();
    assert!(slot_calls.len() > 1, "{slot_calls:?}");
    assert!(
        slot_calls.iter().all(|n| *n <= 500),
        "no statement may delete more than the chunk size: {slot_calls:?}"
    );
    assert_eq!(slot_calls.iter().sum::<u64>(), 5000);
}

#[tokio::test]
async fn cancellation_stops_between_chunks() {
    let env = Env::new().await;
    let base = ago(days(30));
    let seeds: Vec<Seed> = (0..60)
        .map(|i| Seed {
            channel: "C1",
            start: base + TimeDelta::minutes(i),
            end: base + TimeDelta::minutes(i + 1),
            image: None,
        })
        .collect();
    env.add_slots(&seeds).await;

    let token = CancellationToken::new();
    let failing = Arc::new(FailingStore::new(env.store.clone()));
    // Cancel right after the first slot chunk has been deleted.
    *failing.cancel_after_slot_delete.lock().unwrap() = Some(token.clone());
    let j = Janitor::new(
        failing.clone(),
        Retention::default(),
        None,
        env.clock.clone(),
    )
    .with_chunk_size(10);
    let report = j.run_once_until(&token).await;

    assert_eq!(
        report.programmes_deleted, 10,
        "exactly one chunk: {report:?}"
    );
    assert_eq!(env.slot_count().await, 50, "the rest is untouched");
    // Whole statements only: the rows that were deleted took their
    // observations along, none are orphaned.
    assert_eq!(env.count("SELECT COUNT(*) FROM observations").await, 50);
    assert!(report.interrupted);

    // A token that is already cancelled does nothing at all.
    let before = env.slot_count().await;
    let report = j.run_once_until(&token).await;
    assert_eq!(report.programmes_deleted, 0);
    assert_eq!(env.slot_count().await, before);
}

#[tokio::test]
async fn failing_category_does_not_stop_the_run() {
    let env = Env::new().await;
    raw_at(&env.store, ago(days(8)), b"old").await;
    env.add_slots(&[aired(days(9), None)]).await;
    history_at(&env.store, ago(days(100))).await;

    let failing = Arc::new(FailingStore::new(env.store.clone()));
    failing.fail_raw_delete.store(true, Ordering::SeqCst);
    let j = Janitor::new(failing, Retention::default(), None, env.clock.clone());
    let report = j.run_once().await;

    assert_eq!(report.raw_deleted, 0);
    assert_eq!(report.programmes_deleted, 1, "later categories still run");
    assert_eq!(report.history_deleted, 1);
    assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
    let text = &report.errors[0];
    assert!(text.contains("raw"), "names the category: {text}");
    assert!(
        !text.contains("injected") && !text.contains("s3cretpw"),
        "no driver text in the report: {text}"
    );
    assert_eq!(env.count("SELECT COUNT(*) FROM raw_responses").await, 1);
}

// ---------------------------------------------------------------- safety

/// What to seed in one random case.
#[derive(Debug, Clone)]
struct Case {
    /// (start offset minutes, length minutes, with image) around now.
    slots: Vec<(i64, i64, bool)>,
    raw_ages_hours: Vec<i64>,
    retention: [u32; 4],
    cap: Option<u64>,
}

fn case_strategy() -> impl Strategy<Value = Case> {
    (
        prop::collection::vec((-14_400i64..5_000, 1i64..600, any::<bool>()), 0..40),
        prop::collection::vec(0i64..400, 0..8),
        [0u32..10, 0u32..10, 0u32..10, 0u32..120],
        prop::option::of(prop_oneof![Just(1u64), 1u64..2_000_000]),
    )
        .prop_map(|(slots, raw_ages_hours, retention, cap)| Case {
            slots,
            raw_ages_hours,
            retention,
            cap,
        })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn janitor_never_deletes_future_slots_even_when_disk_cap_is_exceeded(case in case_strategy()) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let store = connect("sqlite::memory:").await.unwrap();
            store
                .upsert_channels(&[CanonicalChannel {
                    id: ChannelId("C1".into()),
                    number: "4.1".into(),
                    name: "C1".into(),
                    call_sign: "C1".into(),
                }])
                .await
                .unwrap();
            // Unique starts keep (channel, start) from colliding.
            let mut seen = std::collections::BTreeSet::new();
            let mut seeds = Vec::new();
            for (i, (start_min, len, with_image)) in case.slots.iter().enumerate() {
                if !seen.insert(*start_min) {
                    continue;
                }
                seeds.push(Seed {
                    channel: "C1",
                    start: now() + TimeDelta::minutes(*start_min),
                    end: now() + TimeDelta::minutes(start_min + len),
                    image: with_image.then(|| format!("https://img/{i}.jpg")),
                });
            }
            add_slots_to(&store, &seeds).await;
            for (i, h) in case.raw_ages_hours.iter().enumerate() {
                raw_at(&store, now() - TimeDelta::hours(*h), &noise(2000, i as u64 + 1)).await;
            }
            let future_before: Vec<i64> = {
                let mut v: Vec<i64> = seeds
                    .iter()
                    .filter(|s| s.end > now())
                    .map(|s| s.start.timestamp())
                    .collect();
                v.sort();
                v
            };

            let retention = Retention {
                images_days: case.retention[0],
                raw_days: case.retention[1],
                programmes_days: case.retention[2],
                history_days: case.retention[3],
            };
            let clock = Arc::new(FakeClock::new(now()));
            let j = Janitor::new(store.clone(), retention, case.cap, clock).with_chunk_size(5);
            j.run_once().await;

            let left = store
                .slots(ChannelId("C1".into()), now() - days(1000), now() + days(1000))
                .await
                .unwrap();
            let mut future_after: Vec<i64> = left
                .iter()
                .filter(|s| s.end > now())
                .map(|s| s.start.timestamp())
                .collect();
            future_after.sort();
            assert_eq!(future_before, future_after, "a slot with end > now was deleted");
        });
    }
}

#[tokio::test]
async fn tiny_cap_and_zero_retention_keep_current_and_future_slots() {
    let env = Env::new().await;
    let n = now();
    env.add_slots(&[
        aired(days(1), Some("https://img/a.jpg")),
        Seed {
            channel: "C1",
            start: n - TimeDelta::minutes(1),
            end: n + TimeDelta::minutes(1),
            image: Some("https://img/b.jpg".into()),
        },
        Seed {
            channel: "C1",
            start: n + days(2),
            end: n + days(2) + TimeDelta::hours(1),
            image: Some("https://img/c.jpg".into()),
        },
    ])
    .await;
    raw_at(&env.store, ago(days(1)), &noise(10_000, 3)).await;
    let zero = Retention {
        images_days: 0,
        raw_days: 0,
        programmes_days: 0,
        history_days: 0,
    };
    let report = env.janitor(zero, Some(1)).run_once().await;
    assert_eq!(report.programmes_deleted, 1);
    assert_eq!(env.slot_count().await, 2);
    // The links of the two non-aired slots survive, so their images do too.
    assert_eq!(env.count("SELECT COUNT(*) FROM images").await, 2);
}

// ---------------------------------------------------------------- run loop

#[tokio::test]
async fn run_waits_then_runs_repeatedly_and_stops_on_cancel() {
    let env = Env::new().await;
    raw_at(&env.store, ago(days(8)), b"old").await;
    let sink: ReportSink = Arc::new(std::sync::RwLock::new(None));
    let j = env
        .janitor(Retention::default(), None)
        .with_report_sink(sink.clone())
        .with_timing(Duration::from_millis(150), Duration::from_millis(100));
    let token = CancellationToken::new();
    let task = tokio::spawn(j.run(token.clone()));

    // Nothing happens before the start delay.
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(sink.read().unwrap().is_none(), "ran too early");
    assert_eq!(env.count("SELECT COUNT(*) FROM raw_responses").await, 1);

    // After the delay the first run has happened (the sink holds the
    // latest report, so look at the data, not at the counts).
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(sink.read().unwrap().is_some(), "first run reported");
    assert_eq!(env.count("SELECT COUNT(*) FROM raw_responses").await, 0);

    // It keeps running on the interval: old data added later is also removed.
    raw_at(&env.store, ago(days(9)), b"later").await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(env.count("SELECT COUNT(*) FROM raw_responses").await, 0);

    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("stops promptly")
        .unwrap();
}

#[tokio::test]
async fn run_never_starts_a_run_after_cancellation() {
    let env = Env::new().await;
    raw_at(&env.store, ago(days(8)), b"old").await;
    let sink: ReportSink = Arc::new(std::sync::RwLock::new(None));
    let j = env
        .janitor(Retention::default(), None)
        .with_report_sink(sink.clone())
        .with_timing(Duration::ZERO, Duration::from_millis(10));
    let token = CancellationToken::new();
    token.cancel();
    j.run(token).await;
    assert!(sink.read().unwrap().is_none());
    assert_eq!(env.count("SELECT COUNT(*) FROM raw_responses").await, 1);
}

#[tokio::test]
async fn run_once_fills_sink_and_metrics_with_exact_counts() {
    let env = Env::new().await;
    raw_at(&env.store, ago(days(8)), b"old").await;
    raw_at(&env.store, ago(days(9)), b"old").await;
    history_at(&env.store, ago(days(100))).await;
    env.add_slots(&[aired(days(9), None)]).await;
    let metrics = Arc::new(Metrics::new("t", &[pid("gracenote")], &[]));
    let sink: ReportSink = Arc::new(std::sync::RwLock::new(None));
    let report = env
        .janitor(Retention::default(), None)
        .with_metrics(metrics.clone())
        .with_report_sink(sink.clone())
        .run_once()
        .await;
    assert_eq!(sink.read().unwrap().as_ref(), Some(&report));
    let text = metrics.render();
    let value = |cat: &str| -> f64 {
        text.lines()
            .find(|l| {
                l.starts_with(&format!(
                    "patina_janitor_deleted_total{{category=\"{cat}\"}}"
                ))
            })
            .and_then(|l| l.rsplit(' ').next())
            .and_then(|v| v.parse().ok())
            .unwrap_or(-1.0)
    };
    assert_eq!(value("raw"), 2.0);
    assert_eq!(value("programmes"), 1.0);
    assert_eq!(value("history"), 1.0);
    assert_eq!(value("images"), 0.0);
    assert_eq!(value("state"), 0.0);
    assert_eq!(value("disk"), 0.0);
}

// A helper type check: the report is a plain struct the status code can read.
#[allow(dead_code)]
fn _report_is_clone(r: &JanitorReport) -> JanitorReport {
    r.clone()
}
