//! Backend-independent storage tests shared by every store test file
//! (`mod common;`). `run_contract` takes any `Store`, so each backend calls it
//! with its own store.

// Each test file compiles this module separately and may not use all of it.
#![allow(dead_code)]

pub mod merge_util;
pub mod pg;

use std::sync::Arc;
use std::time::Duration;

use chrono::{NaiveDate, TimeDelta, TimeZone, Utc};
use patina::model::{CanonicalChannel, ChannelId, ProviderId};
use patina::store::{ProviderFetchState, Store};

fn channel(id: &str, number: &str, name: &str) -> CanonicalChannel {
    CanonicalChannel {
        id: ChannelId(id.to_string()),
        number: number.to_string(),
        name: name.to_string(),
        call_sign: id.to_string(),
    }
}

pub async fn run_contract(store: Arc<dyn Store>) {
    migrate_is_idempotent(&store).await;
    slots_empty_for_unknown_channel(&store).await;
    channels_roundtrip(&store).await;
    upsert_channel_updates_name_not_id(&store).await;
    budget_add_is_atomic_under_concurrency(&store).await;
    raw_cache_roundtrip_is_gzip_transparent(&store).await;
    bad_window_roundtrip(&store).await;
    fetch_state_roundtrip(&store).await;
    tuner_channels_roundtrip_replace_and_empty(&store).await;
    apply_merge_is_atomic_and_idempotent(&store).await;
    accuracy_ema_roundtrip(&store).await;
    reliability_counter_rolls_over(&store).await;
    score_history_append_and_prune(&store).await;
    provider_channels_roundtrip(&store).await;
    window_state_roundtrip(&store).await;
    store_ping_and_guide_stats(&store).await;
    janitor_deletes_are_bounded_ordered_and_strict(&store).await;
    janitor_image_cleanup_in_two_steps(&store).await;
    janitor_state_tables_are_pruned_by_rule(&store).await;
    approx_size_grows_with_data(&store).await;
    apply_merge_reports_inserted_ids(&store).await;
}

async fn migrate_is_idempotent(store: &Arc<dyn Store>) {
    store.migrate().await.expect("first migrate");
    store.migrate().await.expect("second migrate");
}

async fn channels_roundtrip(store: &Arc<dyn Store>) {
    // Inserted out of order; "11.1" sorts before "4.1" as plain text, so this
    // also checks the numeric ordering.
    store
        .upsert_channels(&[
            channel("KARE", "11.1", "KARE"),
            channel("WCCO", "4.1", "WCCO"),
        ])
        .await
        .unwrap();
    let list = store.list_channels().await.unwrap();
    let ids: Vec<&str> = list.iter().map(|c| c.id.0.as_str()).collect();
    assert_eq!(ids, ["WCCO", "KARE"]);
}

async fn upsert_channel_updates_name_not_id(store: &Arc<dyn Store>) {
    store
        .upsert_channels(&[channel("WCCO", "4.1", "WCCO-HD")])
        .await
        .unwrap();
    let list = store.list_channels().await.unwrap();
    assert_eq!(list.len(), 2, "same id must not create a new channel");
    let wcco = list.iter().find(|c| c.id.0 == "WCCO").unwrap();
    assert_eq!(wcco.name, "WCCO-HD");
}

async fn slots_empty_for_unknown_channel(store: &Arc<dyn Store>) {
    let from = Utc.with_ymd_and_hms(2026, 11, 1, 0, 0, 0).unwrap();
    let to = Utc.with_ymd_and_hms(2026, 11, 2, 0, 0, 0).unwrap();
    let slots = store
        .slots(ChannelId("nope".into()), from, to)
        .await
        .unwrap();
    assert!(slots.is_empty());
}

fn pid(name: &str) -> ProviderId {
    ProviderId(name.to_string())
}

async fn budget_add_is_atomic_under_concurrency(store: &Arc<dyn Store>) {
    let day = NaiveDate::from_ymd_opt(2026, 11, 1).unwrap();
    assert_eq!(store.budget_used(&pid("gn"), day).await.unwrap(), 0);

    // 25 tasks each add 1 at the same time. If the add were "read, then
    // write", some would overwrite each other and the total would be short.
    let tasks: Vec<_> = (0..25)
        .map(|_| {
            let store = store.clone();
            tokio::spawn(async move { store.budget_add(&pid("gn"), day, 1).await.unwrap() })
        })
        .collect();
    let mut seen = Vec::new();
    for t in tasks {
        seen.push(t.await.unwrap());
    }
    seen.sort();
    // Every caller got a distinct running total: 1, 2, ..., 25.
    assert_eq!(seen, (1..=25).collect::<Vec<u32>>());
    assert_eq!(store.budget_used(&pid("gn"), day).await.unwrap(), 25);

    // n > 1, and counts are separate per provider and per day.
    assert_eq!(store.budget_add(&pid("gn"), day, 5).await.unwrap(), 30);
    let next = day.succ_opt().unwrap();
    assert_eq!(store.budget_used(&pid("gn"), next).await.unwrap(), 0);
    assert_eq!(store.budget_used(&pid("other"), day).await.unwrap(), 0);
}

async fn raw_cache_roundtrip_is_gzip_transparent(store: &Arc<dyn Store>) {
    let w = Utc.with_ymd_and_hms(2026, 11, 1, 12, 0, 0).unwrap();
    let t1 = w + TimeDelta::minutes(1);
    let t2 = w + TimeDelta::minutes(30);
    let day = Duration::from_secs(86400);
    // Bytes that are not valid UTF-8, and a body large enough to compress.
    let mut big = b"{\"channels\":[".repeat(20_000);
    big.extend_from_slice(&[0, 0xff, 0xfe, 0x80]);

    assert!(
        store
            .raw_get_latest(&pid("gn"), "ota", w, day, t2)
            .await
            .unwrap()
            .is_none()
    );
    store
        .raw_put(&pid("gn"), "ota", w, t1, b"older")
        .await
        .unwrap();
    store.raw_put(&pid("gn"), "ota", w, t2, &big).await.unwrap();

    // Newest wins, and comes back byte for byte.
    let got = store
        .raw_get_latest(&pid("gn"), "ota", w, day, t2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.fetched_at, t2);
    assert_eq!(&got.body[..], &big[..]);

    // max_age: 10 minutes after t2, a 5 minute limit excludes everything,
    // and a 15 minute limit finds only the t2 copy.
    let later = t2 + TimeDelta::minutes(10);
    let five = Duration::from_secs(300);
    assert!(
        store
            .raw_get_latest(&pid("gn"), "ota", w, five, later)
            .await
            .unwrap()
            .is_none()
    );
    let fifteen = Duration::from_secs(900);
    let got = store
        .raw_get_latest(&pid("gn"), "ota", w, fifteen, later)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.fetched_at, t2);

    // Keyed by provider, lineup and window.
    let other = w + TimeDelta::hours(6);
    assert!(
        store
            .raw_get_latest(&pid("gn"), "ota", other, day, t2)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .raw_get_latest(&pid("gn"), "cable", w, day, t2)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .raw_get_latest(&pid("x"), "ota", w, day, t2)
            .await
            .unwrap()
            .is_none()
    );

    // An empty body survives too.
    store
        .raw_put(&pid("gn"), "ota", other, t1, b"")
        .await
        .unwrap();
    let got = store
        .raw_get_latest(&pid("gn"), "ota", other, day, t1)
        .await
        .unwrap()
        .unwrap();
    assert!(got.body.is_empty());
}

async fn bad_window_roundtrip(store: &Arc<dyn Store>) {
    let w = Utc.with_ymd_and_hms(2026, 11, 1, 18, 0, 0).unwrap();
    let now = w - TimeDelta::hours(1);
    let until = now + TimeDelta::hours(6);
    assert!(
        !store
            .bad_window_is_bad(&pid("gn"), "ota", w, now)
            .await
            .unwrap()
    );

    store
        .bad_window_mark(&pid("gn"), "ota", w, until)
        .await
        .unwrap();
    assert!(
        store
            .bad_window_is_bad(&pid("gn"), "ota", w, now)
            .await
            .unwrap()
    );
    let just_before = until - TimeDelta::seconds(1);
    assert!(
        store
            .bad_window_is_bad(&pid("gn"), "ota", w, just_before)
            .await
            .unwrap()
    );
    // Expires exactly at `until`.
    assert!(
        !store
            .bad_window_is_bad(&pid("gn"), "ota", w, until)
            .await
            .unwrap()
    );
    // Other windows, lineups and providers are unaffected.
    let next = w + TimeDelta::hours(6);
    assert!(
        !store
            .bad_window_is_bad(&pid("gn"), "ota", next, now)
            .await
            .unwrap()
    );
    assert!(
        !store
            .bad_window_is_bad(&pid("gn"), "cable", w, now)
            .await
            .unwrap()
    );
    assert!(
        !store
            .bad_window_is_bad(&pid("x"), "ota", w, now)
            .await
            .unwrap()
    );

    // Marking again replaces the expiry (here: extends it).
    let later = until + TimeDelta::hours(6);
    store
        .bad_window_mark(&pid("gn"), "ota", w, later)
        .await
        .unwrap();
    assert!(
        store
            .bad_window_is_bad(&pid("gn"), "ota", w, until)
            .await
            .unwrap()
    );
}

async fn fetch_state_roundtrip(store: &Arc<dyn Store>) {
    assert_eq!(store.fetch_state_load(&pid("gn")).await.unwrap(), None);
    let t = Utc.with_ymd_and_hms(2026, 11, 1, 12, 0, 0).unwrap();
    let full = ProviderFetchState {
        consecutive_failures: 7,
        not_before: Some(t + TimeDelta::minutes(5)),
        circuit_open_until: Some(t + TimeDelta::minutes(15)),
        last_success: Some(t - TimeDelta::hours(2)),
        last_error: Some("HTTP 503".into()),
    };
    store.fetch_state_save(&pid("gn"), &full).await.unwrap();
    assert_eq!(
        store.fetch_state_load(&pid("gn")).await.unwrap(),
        Some(full)
    );

    // Saving again replaces every field, including back to "none".
    let cleared = ProviderFetchState {
        last_success: Some(t),
        ..ProviderFetchState::default()
    };
    store.fetch_state_save(&pid("gn"), &cleared).await.unwrap();
    assert_eq!(
        store.fetch_state_load(&pid("gn")).await.unwrap(),
        Some(cleared)
    );
    assert_eq!(store.fetch_state_load(&pid("other")).await.unwrap(), None);
}

async fn tuner_channels_roundtrip_replace_and_empty(store: &Arc<dyn Store>) {
    use patina::channels::TunerChannel;
    let tc = |n: &str, name: &str| TunerChannel {
        number: n.into(),
        name: name.into(),
    };
    let t = Utc.with_ymd_and_hms(2026, 11, 1, 12, 0, 0).unwrap();

    // Never saved is None.
    assert_eq!(store.load_tuner_channels("tuner").await.unwrap(), None);

    // Roundtrip keeps order (11.1 before 4.1 on purpose: order is the source's).
    let first = vec![tc("11.1", "KARE"), tc("4.1", "WCCO-DT"), tc("4.1", "dup")];
    store.save_tuner_channels("tuner", &first, t).await.unwrap();
    assert_eq!(
        store.load_tuner_channels("tuner").await.unwrap(),
        Some((first, t))
    );

    // Saving again replaces the list rather than appending to it.
    let t2 = t + TimeDelta::hours(1);
    let second = vec![tc("5.1", "KSTP")];
    store
        .save_tuner_channels("tuner", &second, t2)
        .await
        .unwrap();
    assert_eq!(
        store.load_tuner_channels("tuner").await.unwrap(),
        Some((second.clone(), t2))
    );

    // Another source key is independent.
    assert_eq!(store.load_tuner_channels("other").await.unwrap(), None);

    // An empty list is valid and distinct from never saved.
    let t3 = t2 + TimeDelta::hours(1);
    store.save_tuner_channels("tuner", &[], t3).await.unwrap();
    assert_eq!(
        store.load_tuner_channels("tuner").await.unwrap(),
        Some((vec![], t3))
    );
    assert_eq!(
        store.load_tuner_channels("other").await.unwrap(),
        None,
        "saving one source must not touch another"
    );
}

/// Writes slots through `apply_merge` and reads them back. Covers what used to
/// be backend-specific read checks (range filter, half-open boundary, image
/// LEFT JOIN, integer/float column mapping), plus: idempotence (same plan
/// twice, and planning again from the same data), updates that move a slot's
/// start into a time another slot is leaving, superseded observations,
/// removals, and that a failing plan changes nothing.
async fn apply_merge_is_atomic_and_idempotent(store: &Arc<dyn Store>) {
    use merge_util::{StubScorer, ch, cover, obs, pid, t};
    use patina::merge::{MergePlan, NewSlot, SlotFields, plan_merge};

    store
        .upsert_channels(&[channel(&ch().0, "7.1", "MRG")])
        .await
        .unwrap();
    let scorer = StubScorer::new(&[("a", 0.75), ("b", 0.25)]);
    let now = t(0);
    let read = |from: i64, to: i64| {
        let store = store.clone();
        async move { store.slots(ch(), t(from), t(to)).await.unwrap() }
    };
    let read_full = || {
        let store = store.clone();
        async move {
            store
                .slots_with_observations(&ch(), t(-1440), t(2880))
                .await
                .unwrap()
        }
    };

    // ---- Insert two slots from a fresh plan. ----
    let mut first = obs("a", 600, 660, "Early", -60);
    first.image_url = Some("https://img/early.jpg".into());
    first.season = Some(2);
    first.episode = Some(5);
    first.genres = vec!["Drama".into(), "News".into()];
    first.subtitle = Some("Part 1".into());
    first.rating = Some("TV-PG".into());
    let second = obs("a", 660, 720, "Late", -60);
    let incoming = vec![first.clone(), second.clone()];
    let coverage = cover("a", 0, 1440);

    assert!(read_full().await.is_empty());
    let plan = plan_merge(&read_full().await, &incoming, &coverage, &scorer, now);
    let stats = store.apply_merge(plan.clone()).await.unwrap().stats;
    assert_eq!((stats.inserted, stats.updated, stats.removed), (2, 0, 0));

    // Range filter: [630, 690) overlaps both; [660, 690) touches Early only
    // at its end (half-open), so only Late.
    let both = read(630, 690).await;
    assert_eq!(both.len(), 2);
    let late = read(660, 690).await;
    assert_eq!(late.len(), 1);
    assert_eq!(late[0].title, "Late");
    assert!(read(720, 800).await.is_empty(), "ends exactly at 720");

    // Column mapping and the image LEFT JOIN.
    let early = &both[0];
    assert_eq!(early.title, "Early");
    assert_eq!((early.start, early.end), (t(600), t(660)));
    assert_eq!(early.image_url.as_deref(), Some("https://img/early.jpg"));
    assert_eq!((early.season, early.episode), (Some(2), Some(5)));
    assert_eq!(early.genres, ["Drama", "News"]);
    assert_eq!(early.subtitle.as_deref(), Some("Part 1"));
    assert_eq!(early.rating.as_deref(), Some("TV-PG"));
    assert_eq!(early.score, 0.75);
    assert_eq!(early.breakdown.0["stub"], 0.75);
    assert_eq!(early.winner, pid("a"));
    assert_eq!(early.updated_at, now);
    assert_eq!(both[1].image_url, None);
    assert_eq!(both[1].season, None);
    assert!(both[1].genres.is_empty());

    // Provenance round trip.
    let full = read_full().await;
    assert_eq!(full.len(), 2);
    assert_eq!(full[0].observations.len(), 1);
    assert_eq!(full[0].observations[0].observation, first);
    assert_eq!(full[0].observations[0].first_seen, first.fetched_at);
    assert_eq!(full[0].observations[0].changes, 0);
    assert_eq!(full[1].observations[0].observation, second);

    // ---- Idempotence. ----
    // Applying the same plan again changes nothing (inserts find their row).
    let stats = store.apply_merge(plan).await.unwrap().stats;
    assert_eq!((stats.inserted, stats.removed), (0, 0));
    assert_eq!(read_full().await, full);
    // Planning again from the same data gives an empty plan.
    let again = plan_merge(&full, &incoming, &coverage, &scorer, now);
    assert!(again.is_empty(), "{again:#?}");

    // ---- A changed observation updates slot and observation counters. ----
    let mut renamed = first.clone();
    renamed.title = "Early (new)".into();
    renamed.fetched_at = t(-30);
    renamed.image_url = Some("https://img/early2.jpg".into());
    let plan = plan_merge(&full, &[renamed.clone()], &coverage, &scorer, t(1));
    let stats = store.apply_merge(plan).await.unwrap().stats;
    assert_eq!(stats.updated, 1);
    let full = read_full().await;
    assert_eq!(full[0].slot.title, "Early (new)");
    assert_eq!(
        full[0].slot.image_url.as_deref(),
        Some("https://img/early2.jpg")
    );
    assert_eq!(full[0].slot.updated_at, t(1));
    assert_eq!(full[0].observations[0].observation, renamed);
    assert_eq!(full[0].observations[0].first_seen, first.fetched_at);
    assert_eq!(full[0].observations[0].changes, 1);

    // ---- Updates that move starts: Early moves to 660, where Late starts,
    // while Late moves on to 690. Needs care with UNIQUE (channel, start). ----
    let (e_id, l_id) = (full[0].slot.id, full[1].slot.id);
    let fields = |s: &patina::model::MasterSlot, start: i64, end: i64| SlotFields {
        start: t(start),
        end: t(end),
        title: s.title.clone(),
        subtitle: s.subtitle.clone(),
        description: s.description.clone(),
        image_url: None,
        season: s.season,
        episode: s.episode,
        rating: s.rating.clone(),
        genres: s.genres.clone(),
        winner: s.winner.clone(),
        score: s.score,
        breakdown: s.breakdown.clone(),
    };
    let mut plan = MergePlan::empty(t(2));
    plan.updates = vec![
        (e_id, fields(&full[0].slot, 660, 690)),
        (l_id, fields(&full[1].slot, 690, 720)),
    ];
    let stats = store.apply_merge(plan).await.unwrap().stats;
    assert_eq!(stats.updated, 2);
    let moved = read(0, 1440).await;
    let spans: Vec<_> = moved.iter().map(|s| (s.id, s.start, s.end)).collect();
    assert_eq!(
        spans,
        [(e_id, t(660), t(690)), (l_id, t(690), t(720))],
        "updated in place, ids kept"
    );
    assert_eq!(moved[0].image_url, None, "image link cleared");

    // ---- Superseded observation and removal. ----
    // b observes the Early slot too.
    let full = read_full().await;
    let b_obs = obs("b", 660, 690, "Early (b)", -20);
    let plan = plan_merge(&full, &[b_obs], &cover("b", 0, 1440), &scorer, t(2));
    store.apply_merge(plan).await.unwrap();
    assert_eq!(read_full().await[0].observations.len(), 2);

    let mut plan = MergePlan::empty(t(3));
    plan.superseded = vec![(e_id, pid("a"))];
    plan.removals = vec![l_id];
    let stats = store.apply_merge(plan).await.unwrap().stats;
    assert_eq!(stats.removed, 1);
    let full = read_full().await;
    assert_eq!(full.len(), 1);
    assert_eq!(full[0].slot.id, e_id);
    let providers: Vec<_> = full[0]
        .observations
        .iter()
        .map(|o| o.observation.provider.0.as_str())
        .collect();
    assert_eq!(providers, ["b"]);

    // ---- Atomic: a plan that fails part-way changes nothing. ----
    let mut bad = MergePlan::empty(t(4));
    bad.removals = vec![e_id];
    bad.inserts = vec![NewSlot {
        channel: ChannelId("no-such-channel".into()),
        fields: fields(&full[0].slot, 800, 860),
    }];
    assert!(
        store.apply_merge(bad).await.is_err(),
        "unknown channel must fail"
    );
    assert_eq!(read_full().await, full, "removal was rolled back");

    // Other channels are unaffected and empty.
    assert!(
        store
            .slots_with_observations(&ChannelId("WCCO".into()), t(-1440), t(2880))
            .await
            .unwrap()
            .is_empty()
    );
}

async fn accuracy_ema_roundtrip(store: &Arc<dyn Store>) {
    use patina::score::{AccuracySnapshot, next_accuracy_row};
    use patina::store::AccuracyRow;

    let at = Utc.with_ymd_and_hms(2026, 11, 1, 12, 0, 0).unwrap();
    let wcco = ChannelId("WCCODT.us".into());
    let kare = ChannelId("KARE.us".into());
    assert!(store.provider_accuracy_load_all().await.unwrap().is_empty());

    // First sample seeds the average, the second moves it by 20%.
    let first = next_accuracy_row(None, &pid("gn"), &wcco, 0.3, at);
    store.provider_accuracy_upsert(&first).await.unwrap();
    let other = AccuracyRow {
        provider: pid("gn"),
        channel: kare.clone(),
        ema: 0.123_456_789_012_345_67,
        samples: 7,
        updated_at: at,
    };
    store.provider_accuracy_upsert(&other).await.unwrap();
    let loaded = store.provider_accuracy_load_all().await.unwrap();
    // Ordered by provider then channel: KARE before WCCO.
    assert_eq!(loaded, vec![other.clone(), first.clone()]);

    let later = at + TimeDelta::hours(6);
    let second = next_accuracy_row(Some(&loaded[1]), &pid("gn"), &wcco, 1.0, later);
    store.provider_accuracy_upsert(&second).await.unwrap();
    let loaded = store.provider_accuracy_load_all().await.unwrap();
    assert_eq!(loaded.len(), 2, "upsert replaces, never duplicates");
    let w = &loaded[1];
    assert_eq!(w.channel, wcco);
    assert!((w.ema - 0.44).abs() < 1e-12, "ema {}", w.ema);
    assert_eq!((w.samples, w.updated_at), (2, later));
    // f64 survives exactly (DOUBLE PRECISION in Postgres, not 4-byte REAL).
    assert_eq!(loaded[0].ema, other.ema);

    // The snapshot the scheduler builds reads the same data.
    let snap = AccuracySnapshot::load(store.as_ref()).await.unwrap();
    assert_eq!(snap.accuracy(&pid("gn"), &wcco).samples, 2);
    let stats = snap.get(&pid("gn"), &kare).unwrap();
    assert_eq!(stats.hindsight, other.ema);
}

async fn reliability_counter_rolls_over(store: &Arc<dyn Store>) {
    use patina::score::AccuracySnapshot;
    use patina::score::signals::reliability;
    use patina::store::{RELIABILITY_WINDOW, ReliabilityRow};

    let t0 = Utc.with_ymd_and_hms(2026, 11, 1, 0, 0, 0).unwrap();
    let p = pid("rel");
    for (i, ok) in [true, true, true, false].into_iter().enumerate() {
        store
            .provider_reliability_record(&p, ok, t0 + TimeDelta::hours(i as i64))
            .await
            .unwrap();
    }
    let row = |rows: Vec<ReliabilityRow>| rows.into_iter().find(|r| r.provider == p).unwrap();
    let r = row(store.provider_reliability_load_all().await.unwrap());
    assert_eq!((r.success, r.total, r.window_start), (3, 4, t0));

    // Just inside the window: still counting into the same window.
    let window = TimeDelta::from_std(RELIABILITY_WINDOW).unwrap();
    let inside = t0 + window - TimeDelta::seconds(1);
    store
        .provider_reliability_record(&p, true, inside)
        .await
        .unwrap();
    let r = row(store.provider_reliability_load_all().await.unwrap());
    assert_eq!((r.success, r.total, r.window_start), (4, 5, t0));

    // A week on: old counts halved (4/2, 5/2), then this failure added.
    let rolled = t0 + window;
    store
        .provider_reliability_record(&p, false, rolled)
        .await
        .unwrap();
    let r = row(store.provider_reliability_load_all().await.unwrap());
    assert_eq!((r.success, r.total, r.window_start), (2, 3, rolled));

    // Concurrent records never lose a count (one atomic statement each).
    let q = pid("rel-concurrent");
    let at = t0 + TimeDelta::hours(1);
    let tasks: Vec<_> = (0..10)
        .map(|i| {
            let store = store.clone();
            let q = q.clone();
            tokio::spawn(async move { store.provider_reliability_record(&q, i % 2 == 0, at).await })
        })
        .collect();
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    let rows = store.provider_reliability_load_all().await.unwrap();
    let c = rows.iter().find(|r| r.provider == q).unwrap();
    assert_eq!((c.success, c.total), (5, 10));
    // Ordered by provider.
    let names: Vec<&str> = rows.iter().map(|r| r.provider.0.as_str()).collect();
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted);

    // The snapshot turns it into the smoothed reliability signal.
    let snap = AccuracySnapshot::load(store.as_ref()).await.unwrap();
    let stats = snap.get(&p, &ChannelId("any".into())).unwrap();
    assert_eq!(stats.reliability, reliability(2, 3));
}

async fn score_history_append_and_prune(store: &Arc<dyn Store>) {
    use patina::model::{ScoreBreakdown, SlotId};
    use patina::store::ScoreHistoryRow;

    let t0 = Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap();
    let row = |days: i64, score: f64| {
        let mut b = ScoreBreakdown::default();
        b.0.insert("freshness".into(), score);
        b.0.insert("agreement".into(), 0.5);
        ScoreHistoryRow {
            slot: SlotId(42),
            provider: pid("gn"),
            channel: ChannelId("WCCODT.us".into()),
            scored_at: t0 + TimeDelta::days(days),
            score,
            breakdown: b,
        }
    };
    store.score_history_append(&[]).await.unwrap();
    store
        .score_history_append(&[row(0, 0.1), row(1, 0.2), row(2, 0.3)])
        .await
        .unwrap();
    store.score_history_append(&[row(3, 0.4)]).await.unwrap();

    // Strictly older than the cutoff: day 1 itself stays.
    let deleted = store
        .score_history_delete_older_than(t0 + TimeDelta::days(1), 1000)
        .await
        .unwrap();
    assert_eq!(deleted, 1);
    assert_eq!(
        store
            .score_history_delete_older_than(t0 + TimeDelta::days(1), 1000)
            .await
            .unwrap(),
        0,
        "pruning again deletes nothing"
    );
    let deleted = store
        .score_history_delete_older_than(t0 + TimeDelta::days(365), 1000)
        .await
        .unwrap();
    assert_eq!(deleted, 3, "the rest, appended in two batches");
}

fn pch(id: &str, number: &str, call_sign: &str) -> patina::provider::ProviderChannel {
    patina::provider::ProviderChannel {
        provider_channel_id: id.into(),
        number: number.into(),
        call_sign: call_sign.into(),
    }
}

async fn provider_channels_roundtrip(store: &Arc<dyn Store>) {
    let t = Utc.with_ymd_and_hms(2026, 11, 1, 12, 0, 0).unwrap();
    let list = |p: &'static str, l: &'static str| {
        let store = store.clone();
        async move { store.provider_channels_list(&pid(p), l).await.unwrap() }
    };
    assert!(list("gn", "ota").await.is_empty());

    store
        .provider_channels_upsert(&pid("gn"), "ota", &[], t)
        .await
        .unwrap();
    store
        .provider_channels_upsert(
            &pid("gn"),
            "ota",
            &[pch("200", "11.1", "KAREDT"), pch("100", "4.1", "WCCO-DT")],
            t,
        )
        .await
        .unwrap();
    // Ordered by provider channel id; kept per provider and per lineup.
    assert_eq!(
        list("gn", "ota").await,
        vec![pch("100", "4.1", "WCCO-DT"), pch("200", "11.1", "KAREDT")]
    );
    assert!(list("gn", "other").await.is_empty());
    assert!(list("other", "ota").await.is_empty());

    // A later response updates number/call sign of a known id and adds new
    // ids; ids absent from it are kept (absence never deletes).
    store
        .provider_channels_upsert(
            &pid("gn"),
            "ota",
            &[pch("100", "4.2", "WCCO"), pch("300", "5.6", "KSTCDT6")],
            t + TimeDelta::hours(1),
        )
        .await
        .unwrap();
    assert_eq!(
        list("gn", "ota").await,
        vec![
            pch("100", "4.2", "WCCO"),
            pch("200", "11.1", "KAREDT"),
            pch("300", "5.6", "KSTCDT6"),
        ]
    );
}

async fn window_state_roundtrip(store: &Arc<dyn Store>) {
    use patina::store::{WindowFailure, WindowState};
    let w0 = Utc.with_ymd_and_hms(2026, 11, 1, 0, 0, 0).unwrap();
    let w = |h: i64| w0 + TimeDelta::hours(h);
    assert_eq!(
        store.window_state_get(&pid("gn"), "ota", w0).await.unwrap(),
        None
    );
    let failed = WindowState {
        window_start: w0,
        last_success: None,
        last_attempt: w(-1),
        last_failure: Some(WindowFailure::Failed("HTTP 500".into())),
    };
    store
        .window_state_put(&pid("gn"), "ota", &failed)
        .await
        .unwrap();
    assert_eq!(
        store.window_state_get(&pid("gn"), "ota", w0).await.unwrap(),
        Some(failed)
    );
    // Put replaces every field, including back to "no error".
    let ok = WindowState {
        window_start: w0,
        last_success: Some(w(-1)),
        last_attempt: w(-1),
        last_failure: None,
    };
    store
        .window_state_put(&pid("gn"), "ota", &ok)
        .await
        .unwrap();
    assert_eq!(
        store.window_state_get(&pid("gn"), "ota", w0).await.unwrap(),
        Some(ok.clone())
    );
    // Every failure kind reads back as itself (typed, stored as a stable
    // code in the text column), details with ": " in them included.
    for failure in [
        WindowFailure::MergeFailed,
        WindowFailure::BadWindow,
        WindowFailure::ParseFailed("expected value at line 1: column 7".into()),
        WindowFailure::Failed("transient fetch error: HTTP 503".into()),
        WindowFailure::Failed("merge_failed".into()),
        WindowFailure::Failed(String::new()),
    ] {
        let st = WindowState {
            last_failure: Some(failure),
            ..ok.clone()
        };
        store
            .window_state_put(&pid("gn"), "ota", &st)
            .await
            .unwrap();
        assert_eq!(
            store.window_state_get(&pid("gn"), "ota", w0).await.unwrap(),
            Some(st)
        );
    }
    store
        .window_state_put(&pid("gn"), "ota", &ok)
        .await
        .unwrap();
    // Keyed by provider and lineup too.
    assert_eq!(
        store
            .window_state_get(&pid("gn"), "other", w0)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store.window_state_get(&pid("x"), "ota", w0).await.unwrap(),
        None
    );

    // Range read: half-open [from, to), ordered by window start.
    let later = |h: i64| WindowState {
        window_start: w(h),
        last_success: Some(w(h - 1)),
        last_attempt: w(h - 1),
        last_failure: None,
    };
    store
        .window_state_put(&pid("gn"), "ota", &later(12))
        .await
        .unwrap();
    store
        .window_state_put(&pid("gn"), "ota", &later(6))
        .await
        .unwrap();
    store
        .window_state_put(&pid("gn"), "other", &later(6))
        .await
        .unwrap();
    let got = store
        .window_state_range(&pid("gn"), "ota", w0, w(12))
        .await
        .unwrap();
    assert_eq!(got, vec![ok, later(6)]);
    assert!(
        store
            .window_state_range(&pid("gn"), "ota", w(13), w(24))
            .await
            .unwrap()
            .is_empty()
    );
}

/// `ping` succeeds on a healthy store; `guide_stats` counts slots, future
/// slots (end > now), the newest end and the channels with future slots.
async fn store_ping_and_guide_stats(store: &Arc<dyn Store>) {
    use patina::merge::{MergePlan, NewSlot, SlotFields};
    use patina::model::ScoreBreakdown;

    store.ping().await.expect("ping on a healthy store");

    // Use channels no earlier contract step has put slots on, and measure
    // against the stats from before, so the test does not depend on what
    // the other steps left in the store.
    let now = Utc.with_ymd_and_hms(2031, 5, 1, 12, 0, 0).unwrap();
    let before = store.guide_stats(now).await.unwrap();
    store
        .upsert_channels(&[
            channel("STATS-A", "901.1", "A"),
            channel("STATS-B", "901.2", "B"),
            channel("STATS-C", "901.3", "C"),
        ])
        .await
        .unwrap();
    let slot = |ch: &str, start_min: i64, len_min: i64| NewSlot {
        channel: ChannelId(ch.into()),
        fields: SlotFields {
            start: now + TimeDelta::minutes(start_min),
            end: now + TimeDelta::minutes(start_min + len_min),
            title: "t".into(),
            subtitle: None,
            description: None,
            image_url: None,
            season: None,
            episode: None,
            rating: None,
            genres: vec![],
            winner: ProviderId("gracenote".into()),
            score: 1.0,
            breakdown: ScoreBreakdown::default(),
        },
    };
    let mut plan = MergePlan::empty(now);
    // A: one past slot, one on air now (ends 12:30), one upcoming.
    plan.inserts.push(slot("STATS-A", -120, 60));
    plan.inserts.push(slot("STATS-A", -30, 60));
    plan.inserts.push(slot("STATS-A", 60, 30));
    // B: only a slot that ended exactly at `now` (not future: end > now).
    plan.inserts.push(slot("STATS-B", -60, 60));
    // C: one far-future slot, the newest in the guide.
    plan.inserts.push(slot("STATS-C", 60 * 24 * 365 * 40, 30));
    store.apply_merge(plan).await.unwrap();

    let after = store.guide_stats(now).await.unwrap();
    assert_eq!(after.slots_total, before.slots_total + 5);
    assert_eq!(after.future_slots_total, before.future_slots_total + 3);
    assert_eq!(
        after.newest_slot_end,
        Some(now + TimeDelta::minutes(60 * 24 * 365 * 40 + 30))
    );
    let ours: Vec<&ChannelId> = after
        .future_channels
        .iter()
        .filter(|c| c.0.starts_with("STATS-"))
        .collect();
    assert_eq!(
        ours,
        [&ChannelId("STATS-A".into()), &ChannelId("STATS-C".into())]
    );
    assert_eq!(
        after.channels_with_future_slots as usize,
        after.future_channels.len()
    );
    // `now` later than everything: nothing is future any more.
    let late = now + TimeDelta::minutes(60 * 24 * 365 * 41);
    let end = store.guide_stats(late).await.unwrap();
    assert_eq!(end.future_slots_total, 0);
    assert_eq!(end.channels_with_future_slots, 0);
    assert!(end.future_channels.is_empty());
    assert_eq!(end.slots_total, after.slots_total);
}

// ---------------------------------------------------------------------------
// Janitor store methods. All data below is from January 2020, earlier than
// anything the other contract steps store, and every cutoff lies inside that
// month, so these deletes can only ever match the rows created here.

fn jan(day: u32, hour: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2020, 1, day, hour, 0, 0).unwrap()
}

/// Insert one-hour slots on `channel` ending at the given instants; each has
/// one observation and optionally an image.
async fn jan_slots(
    store: &Arc<dyn Store>,
    channel: &str,
    ends: &[(chrono::DateTime<Utc>, Option<&str>)],
) {
    use patina::merge::{MergePlan, NewSlot, ObservationWrite, SlotFields, SlotRef};
    use patina::model::{Observation, ScoreBreakdown};

    store
        .upsert_channels(&[CanonicalChannel {
            id: ChannelId(channel.into()),
            number: "902.1".into(),
            name: channel.into(),
            call_sign: channel.into(),
        }])
        .await
        .unwrap();
    let mut plan = MergePlan::empty(jan(1, 0));
    for (i, (end, image)) in ends.iter().enumerate() {
        let start = *end - TimeDelta::hours(1);
        plan.inserts.push(NewSlot {
            channel: ChannelId(channel.into()),
            fields: SlotFields {
                start,
                end: *end,
                title: "t".into(),
                subtitle: None,
                description: None,
                image_url: image.map(str::to_string),
                season: None,
                episode: None,
                rating: None,
                genres: vec![],
                winner: pid("jan"),
                score: 1.0,
                breakdown: ScoreBreakdown::default(),
            },
        });
        plan.observations.push(ObservationWrite {
            slot: SlotRef::New(i),
            observation: Observation {
                provider: pid("jan"),
                channel: ChannelId(channel.into()),
                start,
                end: *end,
                title: "t".into(),
                subtitle: None,
                description: None,
                image_url: image.map(str::to_string),
                season: None,
                episode: None,
                rating: None,
                genres: vec![],
                fetched_at: jan(1, 0),
            },
            first_seen: jan(1, 0),
            changes: 0,
        });
    }
    store.apply_merge(plan).await.unwrap();
}

async fn jan_channel_slots(
    store: &Arc<dyn Store>,
    channel: &str,
) -> Vec<patina::merge::SlotWithObservations> {
    store
        .slots_with_observations(
            &ChannelId(channel.into()),
            jan(1, 0) - TimeDelta::days(30),
            jan(31, 0),
        )
        .await
        .unwrap()
}

/// Each delete removes at most `limit` rows, the OLDEST first, and only rows
/// strictly older than the cutoff. Slots take their observations along;
/// score history is left alone.
async fn janitor_deletes_are_bounded_ordered_and_strict(store: &Arc<dyn Store>) {
    // ---- raw responses: Jan 1..5, window_start = fetched_at = that day. ----
    for d in 1..=5 {
        store
            .raw_put(&pid("jan-raw"), "ota", jan(d, 0), jan(d, 0), b"x")
            .await
            .unwrap();
    }
    let present = |d: u32| {
        let store = store.clone();
        async move {
            store
                .raw_get_latest(
                    &pid("jan-raw"),
                    "ota",
                    jan(d, 0),
                    Duration::from_secs(3600),
                    jan(d, 0),
                )
                .await
                .unwrap()
                .is_some()
        }
    };
    // Cutoff Jan 4: days 1, 2, 3 qualify; limit 2 takes the two oldest.
    assert_eq!(store.delete_raw_older_than(jan(4, 0), 2).await.unwrap(), 2);
    assert!(!present(1).await && !present(2).await);
    assert!(present(3).await, "the third-oldest is still there");
    assert_eq!(store.delete_raw_older_than(jan(4, 0), 2).await.unwrap(), 1);
    assert_eq!(store.delete_raw_older_than(jan(4, 0), 2).await.unwrap(), 0);
    assert!(present(4).await, "a row exactly at the cutoff is kept");
    assert!(present(5).await);

    // ---- score history ----
    let row = |d: u32| patina::store::ScoreHistoryRow {
        slot: patina::model::SlotId(1),
        provider: pid("jan-h"),
        channel: ChannelId("JAN-H".into()),
        scored_at: Utc.with_ymd_and_hms(2019, 12, d, 0, 0, 0).unwrap(),
        score: 0.5,
        breakdown: patina::model::ScoreBreakdown::default(),
    };
    store
        .score_history_append(&[row(1), row(2), row(3)])
        .await
        .unwrap();
    let cut = Utc.with_ymd_and_hms(2019, 12, 31, 0, 0, 0).unwrap();
    assert_eq!(
        store.score_history_delete_older_than(cut, 2).await.unwrap(),
        2
    );
    assert_eq!(
        store.score_history_delete_older_than(cut, 2).await.unwrap(),
        1
    );
    assert_eq!(
        store.score_history_delete_older_than(cut, 2).await.unwrap(),
        0
    );

    // ---- aired slots: ends Jan 1 10:00, Jan 2 10:00, Jan 3 10:00, Jan 4 10:00 ----
    let ends: Vec<_> = (1..=4).map(|d| (jan(d, 10), None)).collect();
    jan_slots(store, "JAN-S", &ends).await;
    assert_eq!(jan_channel_slots(store, "JAN-S").await.len(), 4);
    // Cutoff Jan 3 10:00: only the Jan 1 and Jan 2 slots are strictly older.
    assert_eq!(
        store
            .delete_aired_slots_older_than(jan(3, 10), 1)
            .await
            .unwrap(),
        1
    );
    let left = jan_channel_slots(store, "JAN-S").await;
    assert_eq!(left.len(), 3, "limit 1 removed exactly one, the oldest");
    assert_eq!(left[0].slot.end, jan(2, 10));
    assert_eq!(
        store
            .delete_aired_slots_older_than(jan(3, 10), 10)
            .await
            .unwrap(),
        1
    );
    let left = jan_channel_slots(store, "JAN-S").await;
    let ends_left: Vec<_> = left.iter().map(|s| s.slot.end).collect();
    assert_eq!(ends_left, [jan(3, 10), jan(4, 10)], "end == cutoff stays");
    // Observations went with their slots: the survivors still have theirs.
    assert!(left.iter().all(|s| s.observations.len() == 1));
    assert_eq!(
        store
            .delete_aired_slots_older_than(jan(3, 10), 10)
            .await
            .unwrap(),
        0
    );
}

/// Image cleanup: unlink by slot age, then delete what no slot uses.
async fn janitor_image_cleanup_in_two_steps(store: &Arc<dyn Store>) {
    // Purge image rows earlier steps left unreferenced, so the counts below
    // are exactly ours.
    let _ = store.images_delete_unreferenced(100_000).await.unwrap();

    jan_slots(
        store,
        "JAN-I",
        &[
            (jan(1, 10), Some("https://jan/shared.jpg")),
            (jan(2, 10), Some("https://jan/solo.jpg")),
            (jan(5, 10), Some("https://jan/shared.jpg")),
        ],
    )
    .await;
    // Nothing is unreferenced yet.
    let d = store.images_delete_unreferenced(10).await.unwrap();
    assert_eq!((d.rows, d.local_paths.len()), (0, 0));

    // Cutoff Jan 3: slots ending Jan 1 and Jan 2 lose their link; one per call.
    assert_eq!(
        store
            .images_clear_for_slots_older_than(jan(3, 0), 1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .images_clear_for_slots_older_than(jan(3, 0), 1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .images_clear_for_slots_older_than(jan(3, 0), 1)
            .await
            .unwrap(),
        0
    );
    let slots = jan_channel_slots(store, "JAN-I").await;
    assert_eq!(slots.len(), 3, "the slots stay");
    assert_eq!(slots[0].slot.image_url, None);
    assert_eq!(slots[1].slot.image_url, None);
    assert_eq!(
        slots[2].slot.image_url.as_deref(),
        Some("https://jan/shared.jpg")
    );

    // solo.jpg is now unreferenced; shared.jpg is still used by the Jan 5 slot.
    let d = store.images_delete_unreferenced(10).await.unwrap();
    assert_eq!(d.rows, 1);
    assert!(d.local_paths.is_empty(), "v1 never sets local_path");
    assert_eq!(store.images_delete_unreferenced(10).await.unwrap().rows, 0);
    let slots = jan_channel_slots(store, "JAN-I").await;
    assert_eq!(
        slots[2].slot.image_url.as_deref(),
        Some("https://jan/shared.jpg")
    );
}

/// Each bookkeeping table is pruned by its own column, strictly older than
/// the cutoff, and never beyond the limit.
async fn janitor_state_tables_are_pruned_by_rule(store: &Arc<dyn Store>) {
    use patina::store::{StateTable, WindowState};

    let p = pid("jan-st");

    // window_state.window_start
    for d in 1..=3 {
        store
            .window_state_put(
                &p,
                "ota",
                &WindowState {
                    window_start: jan(d, 0),
                    last_success: Some(jan(d, 0)),
                    last_attempt: jan(d, 0),
                    last_failure: None,
                },
            )
            .await
            .unwrap();
    }
    let t = StateTable::WindowState;
    assert_eq!(
        store
            .delete_state_older_than(t, jan(3, 0), 1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .delete_state_older_than(t, jan(3, 0), 1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .delete_state_older_than(t, jan(3, 0), 1)
            .await
            .unwrap(),
        0
    );
    assert!(
        store
            .window_state_get(&p, "ota", jan(3, 0))
            .await
            .unwrap()
            .is_some()
    );

    // provider_channels.last_seen
    store
        .provider_channels_upsert(&p, "ota", &[pch("old", "1.1", "OLD")], jan(1, 0))
        .await
        .unwrap();
    store
        .provider_channels_upsert(&p, "ota", &[pch("new", "1.2", "NEW")], jan(9, 0))
        .await
        .unwrap();
    let t = StateTable::ProviderChannels;
    assert_eq!(
        store
            .delete_state_older_than(t, jan(5, 0), 10)
            .await
            .unwrap(),
        1
    );
    let left = store.provider_channels_list(&p, "ota").await.unwrap();
    assert_eq!(left, vec![pch("new", "1.2", "NEW")]);

    // fetch_bad_windows.bad_until: pass "now"; expired rows go, live ones stay.
    store
        .bad_window_mark(&p, "ota", jan(1, 0), jan(2, 0))
        .await
        .unwrap();
    store
        .bad_window_mark(&p, "ota", jan(1, 6), jan(20, 0))
        .await
        .unwrap();
    let t = StateTable::BadWindows;
    assert_eq!(
        store
            .delete_state_older_than(t, jan(10, 0), 10)
            .await
            .unwrap(),
        1
    );
    assert!(
        store
            .bad_window_is_bad(&p, "ota", jan(1, 6), jan(10, 0))
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .delete_state_older_than(t, jan(10, 0), 10)
            .await
            .unwrap(),
        0
    );

    // fetch_budget.day: the cutoff's UTC day; strictly earlier days go.
    let day = |d: u32| NaiveDate::from_ymd_opt(2020, 1, d).unwrap();
    for d in [8, 9, 10] {
        store.budget_add(&p, day(d), 4).await.unwrap();
    }
    let t = StateTable::FetchBudget;
    // 2020-01-10 12:00 -> day 2020-01-10 itself stays.
    assert_eq!(
        store
            .delete_state_older_than(t, jan(10, 12), 1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .delete_state_older_than(t, jan(10, 12), 10)
            .await
            .unwrap(),
        1
    );
    assert_eq!(store.budget_used(&p, day(8)).await.unwrap(), 0);
    assert_eq!(store.budget_used(&p, day(9)).await.unwrap(), 0);
    assert_eq!(store.budget_used(&p, day(10)).await.unwrap(), 4);
}

/// `approx_size_bytes` is positive and grows when incompressible data is
/// stored (it is the measure behind the disk cap).
async fn approx_size_grows_with_data(store: &Arc<dyn Store>) {
    let before = store.approx_size_bytes().await.unwrap();
    assert!(before > 0);
    let mut x: u64 = 88172645463325252;
    let noise: Vec<u8> = (0..400_000)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x & 0xff) as u8
        })
        .collect();
    store
        .raw_put(&pid("jan-size"), "ota", jan(20, 0), jan(20, 0), &noise)
        .await
        .unwrap();
    let after = store.approx_size_bytes().await.unwrap();
    assert!(after > before + 100_000, "before {before}, after {after}");
    store.delete_raw_older_than(jan(21, 0), 100).await.unwrap();
}

/// `apply_merge` tells the caller the id of every slot it inserted, in the
/// order of `plan.inserts` (`inserted_ids[i]` belongs to `plan.inserts[i]`),
/// so nobody has to read the slots back and guess which row is which. An
/// insert that found its row already there (the same plan applied twice)
/// reports that row's id. A plan that fails reports nothing and leaves no
/// row behind. Cleans up after itself.
async fn apply_merge_reports_inserted_ids(store: &Arc<dyn Store>) {
    use merge_util::{StubScorer, cover, obs, pid, t};
    use patina::merge::{MergePlan, NewSlot, ObservationWrite, SlotFields, SlotRef, plan_merge};
    use patina::model::SlotId;

    let chan = ChannelId("IDS.us".into());
    store
        .upsert_channels(&[channel(&chan.0, "70.1", "IDS")])
        .await
        .unwrap();
    let on_chan = |mut o: patina::model::Observation| {
        o.channel = chan.clone();
        o
    };
    let coverage = patina::merge::Coverage {
        channel: chan.clone(),
        ..cover("a", 0, 1440)
    };
    let scorer = StubScorer::new(&[("a", 0.75)]);
    let full = || {
        let store = store.clone();
        let chan = chan.clone();
        async move {
            store
                .slots_with_observations(&chan, t(-1440), t(2880))
                .await
                .unwrap()
        }
    };
    let id_at = |slots: &[patina::merge::SlotWithObservations], start: i64| {
        slots
            .iter()
            .find(|s| s.slot.start == t(start))
            .map(|s| s.slot.id)
            .expect("slot at start")
    };

    // ---- Insert only. The plan lists the later programme first, so a
    // mapping by position (not by time) is what is checked. ----
    let incoming = vec![
        on_chan(obs("a", 700, 760, "Second", -60)),
        on_chan(obs("a", 600, 660, "First", -60)),
    ];
    let mut plan = plan_merge(&full().await, &incoming, &coverage, &scorer, t(0));
    plan.inserts.reverse();
    for w in &mut plan.observations {
        if let SlotRef::New(i) = w.slot {
            w.slot = SlotRef::New(1 - i);
        }
    }
    let starts: Vec<_> = plan.inserts.iter().map(|n| n.fields.start).collect();
    let applied = store.apply_merge(plan.clone()).await.unwrap();
    assert_eq!(applied.stats.inserted, 2);
    let stored = full().await;
    assert_eq!(applied.inserted_ids.len(), 2);
    for (i, start) in starts.iter().enumerate() {
        let s = stored.iter().find(|s| s.slot.start == *start).unwrap();
        assert_eq!(applied.inserted_ids[i], s.slot.id, "insert #{i}");
        // The plan's observation for that insert landed on the same row.
        assert_eq!(s.observations.len(), 1);
    }
    let (first, second) = (id_at(&stored, 600), id_at(&stored, 700));
    assert_ne!(first, second);

    // The same plan again: nothing new, and the ids of the rows it found.
    let again = store.apply_merge(plan).await.unwrap();
    assert_eq!(again.stats.inserted, 0);
    assert_eq!(again.inserted_ids, applied.inserted_ids);
    assert_eq!(full().await, stored);

    // ---- Update only: no inserted ids. ----
    let mut renamed = on_chan(obs("a", 600, 660, "First (new)", -30));
    renamed.description = Some("now with a description".into());
    let plan = plan_merge(&stored, &[renamed], &coverage, &scorer, t(1));
    assert!(
        plan.inserts.is_empty() && plan.updates.len() == 1,
        "{plan:#?}"
    );
    let applied = store.apply_merge(plan).await.unwrap();
    assert_eq!((applied.stats.inserted, applied.stats.updated), (0, 1));
    assert!(applied.inserted_ids.is_empty());

    // ---- Mixed: one update and one insert. ----
    let stored = full().await;
    let incoming = vec![
        on_chan(obs("a", 600, 660, "First (newer)", -20)),
        on_chan(obs("a", 800, 860, "Third", -20)),
    ];
    let plan = plan_merge(&stored, &incoming, &coverage, &scorer, t(2));
    assert_eq!(
        (plan.updates.len(), plan.inserts.len()),
        (1, 1),
        "{plan:#?}"
    );
    let applied = store.apply_merge(plan).await.unwrap();
    assert_eq!((applied.stats.inserted, applied.stats.updated), (1, 1));
    let stored = full().await;
    assert_eq!(applied.inserted_ids, [id_at(&stored, 800)]);
    assert!(![first, second].contains(&applied.inserted_ids[0]));

    // ---- Rollback: a plan that fails after a good insert reports no id
    // (it is an error) and leaves no row behind. ----
    let fields = |start: i64| SlotFields {
        start: t(start),
        end: t(start + 30),
        title: "Never".into(),
        subtitle: None,
        description: None,
        image_url: None,
        season: None,
        episode: None,
        rating: None,
        genres: vec![],
        winner: pid("a"),
        score: 0.5,
        breakdown: Default::default(),
    };
    let mut bad = MergePlan::empty(t(3));
    bad.inserts = vec![
        NewSlot {
            channel: chan.clone(),
            fields: fields(900),
        },
        NewSlot {
            channel: ChannelId("no-such-channel".into()),
            fields: fields(1000),
        },
    ];
    bad.observations = vec![ObservationWrite {
        slot: SlotRef::New(0),
        observation: on_chan(obs("a", 900, 930, "Never", -10)),
        first_seen: t(-10),
        changes: 0,
    }];
    assert!(store.apply_merge(bad).await.is_err());
    assert_eq!(full().await, stored, "the good insert was rolled back");

    // Clean up, so later checks on this store see no extra slots.
    let mut cleanup = MergePlan::empty(t(4));
    cleanup.removals = full()
        .await
        .iter()
        .map(|s| s.slot.id)
        .collect::<Vec<SlotId>>();
    let applied = store.apply_merge(cleanup).await.unwrap();
    assert_eq!(applied.stats.removed, 3);
    assert!(applied.inserted_ids.is_empty());
    assert!(full().await.is_empty());
}
