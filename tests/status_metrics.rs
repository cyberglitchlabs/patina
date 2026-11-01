//! `/status` and `/metrics`: contents, admin-token rules, and that the
//! counters and gauges move when the fetch layer and scheduler do things.
//! Data comes from the real scheduler talking to the fake Gracenote server.

mod support;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use chrono_tz::America::Chicago;
use http_body_util::BodyExt;
use patina::auth::ct_eq;
use patina::channels::matching::Overrides;
use patina::channels::{StaticSource, TunerChannel};
use patina::clock::FakeClock;
use patina::config::{LineupCfg, Weights};
use patina::feeds::{FeedSourceSpec, Feeds, FeedsConfig};
use patina::fetch::{FetchCfg, FetchLayer, FetchOutcome, SequenceJitter};
use patina::http::{AppState, router};
use patina::janitor::{Janitor, ReportSink};
use patina::metrics::{JanitorCategory, Metrics};
use patina::model::{CanonicalChannel, ProviderId, canonical_id};
use patina::provider::ProviderChannel;
use patina::provider::gracenote::GracenoteProvider;
use patina::scheduler::{ProviderRef, Scheduler, SchedulerDeps};
use patina::status::{Admin, AdminDeps};
use patina::store::{Store, connect};
use support::failing_store::FailingStore;
use support::fake_provider::{Behavior, FakeProvider, fixture};
use tower::ServiceExt;

const ADMIN: &str = "admin-secret-xyz";
const FEED: &str = "feed-secret-abc";

fn now0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 31, 20, 0, 0).unwrap()
}

fn gn() -> ProviderId {
    ProviderId("gracenote".into())
}

fn backup() -> ProviderId {
    ProviderId("backup".into())
}

fn lineup() -> LineupCfg {
    LineupCfg {
        name: "ota".into(),
        lineup_id: "USA-OTA55401".into(),
        postal_code: "55401".into(),
        country: "USA".into(),
    }
}

/// The start of the n-th 6-hour window after 18:00 UTC (the window that
/// contains `now0`). Distinct n never share a raw-cache entry.
fn window(n: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 31, 18, 0, 0).unwrap() + TimeDelta::hours(6 * n)
}

#[derive(Default)]
struct Opts {
    admin_token: Option<&'static str>,
    feed_token: Option<&'static str>,
    /// Use this store instead of a fresh in-memory SQLite.
    store: Option<Arc<dyn Store>>,
    /// Daily budget of the `gracenote` provider (default 1000).
    budget: Option<u32>,
    /// Add a second provider "backup" with budget 7, on its own fake server.
    with_backup: bool,
    sources: Vec<(&'static str, Vec<TunerChannel>)>,
}

struct Rig {
    store: Arc<dyn Store>,
    clock: Arc<FakeClock>,
    fake: FakeProvider,
    fake_backup: Option<FakeProvider>,
    fetch: Arc<FetchLayer>,
    metrics: Arc<Metrics>,
    feeds: Arc<Feeds>,
    sched: Scheduler,
    app: Router,
    janitor_sink: ReportSink,
}

async fn rig(opts: Opts) -> Rig {
    let store = match opts.store {
        Some(s) => s,
        None => connect("sqlite::memory:").await.unwrap(),
    };
    let clock = Arc::new(FakeClock::new(now0()));
    let fake = FakeProvider::start().await;
    fake.set_default(Behavior::ok_shifted());
    let mut providers: Vec<Arc<dyn patina::provider::Provider>> = vec![Arc::new(fake.gracenote())];
    let mut ids = vec![gn()];
    let mut daily_budgets = HashMap::new();
    daily_budgets.insert(gn(), opts.budget.unwrap_or(1000));
    let mut fake_backup = None;
    if opts.with_backup {
        let f = FakeProvider::start().await;
        f.set_default(Behavior::ok_shifted());
        providers.push(Arc::new(GracenoteProvider::new_with_id(
            backup(),
            patina::provider::gracenote::http_client(std::time::Duration::from_secs(10)).unwrap(),
            f.base_url(),
        )));
        ids.push(backup());
        daily_budgets.insert(backup(), 7);
        fake_backup = Some(f);
    }
    let source_specs: Vec<FeedSourceSpec> = opts
        .sources
        .iter()
        .map(|(name, tuner)| FeedSourceSpec {
            name: (*name).into(),
            include_unmatched: false,
            overrides: Overrides::default(),
            lineups: vec![(gn(), "ota".into())],
            source: Arc::new(StaticSource::new(tuner.clone())),
        })
        .collect();
    let source_names: Vec<String> = source_specs.iter().map(|s| s.name.clone()).collect();
    let generation = Arc::new(AtomicU64::new(0));
    let feeds = Arc::new(Feeds::new(FeedsConfig {
        store: store.clone(),
        clock: clock.clone(),
        tz: Chicago,
        horizon_days: 1,
        token: opts.feed_token.map(str::to_string),
        sources: source_specs,
        generation: generation.clone(),
    }));
    let metrics = Arc::new(Metrics::new(env!("CARGO_PKG_VERSION"), &ids, &source_names));
    let fetch = Arc::new(
        FetchLayer::new(
            store.clone(),
            providers,
            FetchCfg {
                daily_budgets,
                min_interval: Duration::ZERO,
                jitter_fraction: 0.0,
                jitter: Arc::new(SequenceJitter::new(vec![0.0])),
                ..FetchCfg::default()
            },
            clock.clone(),
        )
        .with_metrics(metrics.clone()),
    );
    let sched = Scheduler::new(
        SchedulerDeps {
            store: store.clone(),
            fetch: fetch.clone(),
            providers: vec![ProviderRef {
                id: gn(),
                lineups: vec![lineup()],
            }],
            horizon_days: 1,
            weights: Weights::default(),
            tick_interval: Duration::from_secs(60),
            feed_generation: Some(generation),
        },
        clock.clone(),
    )
    .with_metrics(metrics.clone());
    let janitor_sink: ReportSink = Arc::new(std::sync::RwLock::new(None));
    let admin = Arc::new(
        Admin::new(AdminDeps {
            token: opts.admin_token.map(str::to_string),
            store: store.clone(),
            backend: "sqlite",
            fetch: fetch.clone(),
            feeds: feeds.clone(),
            scheduler: sched.status_handle(),
            metrics: metrics.clone(),
            clock: clock.clone(),
            providers: ids,
            version: env!("CARGO_PKG_VERSION"),
            store_timeout: Duration::from_millis(300),
        })
        .with_janitor(janitor_sink.clone()),
    );
    let app = router(AppState::new(feeds.clone()).with_admin(admin));
    Rig {
        store,
        clock,
        fake,
        fake_backup,
        fetch,
        metrics,
        feeds,
        sched,
        app,
        janitor_sink,
    }
}

struct Resp {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: Vec<u8>,
}

impl Resp {
    fn text(&self) -> String {
        String::from_utf8(self.body.clone()).unwrap()
    }
    fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).expect("JSON body")
    }
}

async fn send(app: &Router, uri: &str, headers: &[(&str, &str)]) -> Resp {
    let mut req = Request::get(uri);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let r = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = r.status();
    let headers = r.headers().clone();
    let body = r.into_body().collect().await.unwrap().to_bytes().to_vec();
    Resp {
        status,
        headers,
        body,
    }
}

fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

/// GET with the right admin token.
async fn authed(app: &Router, uri: &str) -> Resp {
    send(app, uri, &[("authorization", &bearer(ADMIN))]).await
}

async fn status_json(r: &Rig) -> serde_json::Value {
    let resp = authed(&r.app, "/status").await;
    assert_eq!(resp.status, StatusCode::OK);
    resp.json()
}

async fn scrape(r: &Rig) -> String {
    let resp = authed(&r.app, "/metrics").await;
    assert_eq!(resp.status, StatusCode::OK);
    resp.text()
}

/// The value of the series `name` whose labels include every `k="v"` pair
/// in `labels`. Panics when not exactly one such series exists.
fn series(text: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
    let found: Vec<f64> = text
        .lines()
        .filter(|l| !l.starts_with('#'))
        .filter_map(|l| {
            let rest = l.strip_prefix(name)?;
            // The name must end here: either labels follow or the value.
            let (lab, value) = if let Some(r) = rest.strip_prefix('{') {
                let (lab, v) = r.split_once("} ")?;
                (lab, v)
            } else {
                let v = rest.strip_prefix(' ')?;
                ("", v)
            };
            labels
                .iter()
                .all(|(k, v)| lab.contains(&format!("{k}=\"{v}\"")))
                .then(|| value.trim().parse::<f64>().unwrap())
        })
        .collect();
    assert_eq!(found.len(), 1, "series {name} {labels:?} in:\n{text}");
    found[0]
}

fn provider_json<'a>(status: &'a serde_json::Value, name: &str) -> &'a serde_json::Value {
    status["providers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == name)
        .unwrap_or_else(|| panic!("no provider {name} in {status}"))
}

// ------------------------------------------------------------------ status

#[tokio::test]
async fn status_reports_budget_and_circuit_for_each_provider() {
    let r = rig(Opts {
        admin_token: Some(ADMIN),
        with_backup: true,
        ..Opts::default()
    })
    .await;
    // gracenote: one good fetch. backup: its server says 403, which opens
    // the circuit at once.
    r.fake_backup
        .as_ref()
        .unwrap()
        .set_default(Behavior::Status(403));
    assert!(matches!(
        r.fetch.fetch(&gn(), &lineup(), window(0)).await,
        FetchOutcome::Fresh(_)
    ));
    assert!(matches!(
        r.fetch.fetch(&backup(), &lineup(), window(0)).await,
        FetchOutcome::Skipped(_)
    ));

    let s = status_json(&r).await;
    let g = provider_json(&s, "gracenote");
    assert_eq!(g["circuit"], "closed");
    assert_eq!(g["budget"]["used"], 1);
    assert_eq!(g["budget"]["limit"], 1000);
    assert_eq!(g["budget"]["day"], "2026-10-31");
    assert_eq!(g["consecutive_failures"], 0);
    assert!(g["last_success"].is_string(), "{g}");
    assert!(g["last_error"].is_null());

    let b = provider_json(&s, "backup");
    assert_eq!(b["circuit"], "open");
    assert_eq!(b["budget"]["used"], 1);
    assert_eq!(b["budget"]["limit"], 7);
    assert_eq!(b["consecutive_failures"], 1);
    assert!(b["last_error"].as_str().unwrap().contains("403"), "{b}");
    assert!(b["last_success"].is_null());
    assert_eq!(s["providers"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn status_shows_guide_horizon_and_coverage_after_ingest() {
    // Tuner channels: three the provider has (matched by number), one it
    // does not know, and one provider channel ("NOSLOTS") that matches but
    // never gets programmes.
    let probe = rig(Opts::default()).await;
    let report = probe.sched.tick().await;
    assert!(report.slots_inserted > 100, "{report:?}");
    let provider = probe
        .store
        .provider_channels_list(&gn(), "ota")
        .await
        .unwrap();
    assert!(provider.len() >= 3);
    drop(probe);

    // The real rig: same data, sources defined from the real channel list.
    let mut tuner: Vec<TunerChannel> = provider
        .iter()
        .take(3)
        .map(|c| TunerChannel {
            number: c.number.clone(),
            name: c.call_sign.clone(),
        })
        .collect();
    tuner.push(TunerChannel {
        number: "998.1".into(),
        name: "NOSLOTS".into(),
    });
    tuner.push(TunerChannel {
        number: "999.9".into(),
        name: "NOPE".into(),
    });
    let r = rig(Opts {
        admin_token: Some(ADMIN),
        sources: vec![("static-1", tuner)],
        ..Opts::default()
    })
    .await;
    let report = r.sched.tick().await;
    assert!(
        report.fetched > 0 && report.slots_inserted > 100,
        "{report:?}"
    );
    // A matched channel with no programmes at all.
    let noslots = ProviderChannel {
        provider_channel_id: "noslots-id".into(),
        number: "998.1".into(),
        call_sign: "NOSLOTS".into(),
    };
    r.store
        .provider_channels_upsert(&gn(), "ota", std::slice::from_ref(&noslots), now0())
        .await
        .unwrap();
    r.store
        .upsert_channels(&[CanonicalChannel {
            id: canonical_id("NOSLOTS"),
            number: "998.1".into(),
            name: "NOSLOTS".into(),
            call_sign: "NOSLOTS".into(),
        }])
        .await
        .unwrap();
    r.feeds.refresh_channels().await;
    r.clock.advance(Duration::from_secs(90));

    let s = status_json(&r).await;
    // Independent expectations, straight from the store.
    let mut newest: Option<DateTime<Utc>> = None;
    let mut total = 0;
    let mut with_data = 0;
    for c in r.store.list_channels().await.unwrap() {
        let slots = r
            .store
            .slots(
                c.id,
                now0() - TimeDelta::days(1),
                now0() + TimeDelta::days(30),
            )
            .await
            .unwrap();
        total += slots.len();
        if slots.iter().any(|sl| sl.end > r.clock_now()) {
            with_data += 1;
        }
        newest = newest.max(slots.iter().map(|sl| sl.end).max());
    }
    let newest = newest.expect("ingest stored slots");

    let g = &s["guide"];
    assert_eq!(g["slots_total"], total);
    assert_eq!(g["channels_with_data"], with_data);
    let expected_hours = (newest - r.clock_now()).num_seconds() as f64 / 3600.0;
    assert!(expected_hours > 20.0, "a day of horizon: {expected_hours}");
    let hours = g["horizon_hours"].as_f64().unwrap();
    assert!(
        (hours - expected_hours).abs() < 0.01,
        "{hours} vs {expected_hours}"
    );
    assert_eq!(
        g["newest_slot_end"].as_str().unwrap(),
        newest.format("%Y-%m-%dT%H:%M:%SZ").to_string()
    );
    // 5 matched (3 + NOSLOTS... the unknown NOPE is not), 3 have slots.
    let src = &s["sources"][0];
    assert_eq!(src["name"], "static-1");
    assert_eq!(src["kind"], "static");
    assert_eq!(src["channels_total"], 5);
    assert_eq!(src["channels_matched"], 4);
    assert_eq!(src["stale"], false);
    assert!(src["loaded_at"].is_string());
    assert_eq!(g["coverage_pct"].as_f64().unwrap(), 75.0);

    // The rest of the document.
    assert_eq!(s["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(s["uptime_seconds"], 90);
    assert_eq!(s["store"]["backend"], "sqlite");
    assert_eq!(s["store"]["ok"], true);
    assert!(s["store"]["error"].is_null());
    assert!(s["janitor"].is_null());
    assert!(s["scheduler"]["last_tick_at"].is_string());
    assert_eq!(s["scheduler"]["last_tick"]["fetched"], report.fetched);
    assert_eq!(
        s["scheduler"]["last_tick"]["slots_inserted"],
        report.slots_inserted
    );
    let p = provider_json(&s, "gracenote");
    assert_eq!(p["success_rate"].as_f64().unwrap(), 1.0);
    let q = &p["response_quality"];
    assert!(q["observed_minutes"].as_f64().unwrap() > 0.0, "{q}");
    assert!(q["gap_fraction"].is_number() && q["overlap_fraction"].is_number());
    assert!(q["at"].is_string());

    // The same coverage reaches /metrics.
    let m = scrape(&r).await;
    assert_eq!(series(&m, "patina_guide_coverage_ratio", &[]), 0.75);
    assert_eq!(series(&m, "patina_slots_total", &[]), total as f64);
    let h = series(&m, "patina_guide_horizon_seconds", &[]);
    assert!((h / 3600.0 - expected_hours).abs() < 0.01);
    assert_eq!(
        series(
            &m,
            "patina_source_channels",
            &[("source", "static-1"), ("state", "matched")]
        ),
        4.0
    );
    assert_eq!(
        series(
            &m,
            "patina_source_channels",
            &[("source", "static-1"), ("state", "unmatched")]
        ),
        1.0
    );
}

impl Rig {
    fn clock_now(&self) -> DateTime<Utc> {
        use patina::clock::Clock;
        self.clock.now()
    }
}

#[tokio::test]
async fn status_before_any_data_is_sane() {
    let r = rig(Opts::default()).await;
    let s = status_json(&r).await;
    assert_eq!(s["guide"]["slots_total"], 0);
    assert_eq!(s["guide"]["horizon_hours"], 0.0);
    assert_eq!(s["guide"]["coverage_pct"], 0.0);
    assert!(s["guide"]["newest_slot_end"].is_null());
    assert!(s["scheduler"]["last_tick"].is_null());
    assert!(provider_json(&s, "gracenote")["success_rate"].is_null());
    assert!(provider_json(&s, "gracenote")["response_quality"].is_null());
    assert_eq!(s["sources"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn status_shows_active_lineup_holds() {
    let r = rig(Opts {
        admin_token: Some(ADMIN),
        ..Opts::default()
    })
    .await;
    // No hold yet: an empty list, not null.
    let s = status_json(&r).await;
    assert_eq!(
        provider_json(&s, "gracenote")["lineup_holds"],
        serde_json::json!([])
    );

    // The provider rejects every window: three 400s in a row put lineup
    // "ota" on a 6-hour hold.
    r.fake.set_default(Behavior::Status(400));
    let report = r.sched.tick().await;
    assert_eq!(report.lineup_halted_bad_windows.len(), 1, "{report:?}");
    assert_eq!(r.fake.request_count(), 3);
    let until = patina::clock::Clock::now(r.clock.as_ref()) + TimeDelta::hours(6);
    let s = status_json(&r).await;
    let holds = &provider_json(&s, "gracenote")["lineup_holds"];
    assert_eq!(holds.as_array().unwrap().len(), 1, "{holds}");
    assert_eq!(holds[0]["lineup"], "ota");
    let shown: DateTime<Utc> = holds[0]["until"].as_str().unwrap().parse().unwrap();
    assert_eq!(shown, until);
    assert!(
        holds[0]["until"].as_str().unwrap().ends_with('Z'),
        "{holds}"
    );

    // Still there one second before the end, gone at the end (even with no
    // tick in between).
    r.clock.set(until - TimeDelta::seconds(1));
    let s = status_json(&r).await;
    assert_eq!(
        provider_json(&s, "gracenote")["lineup_holds"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    r.clock.set(until);
    let s = status_json(&r).await;
    assert_eq!(
        provider_json(&s, "gracenote")["lineup_holds"],
        serde_json::json!([])
    );
}

#[tokio::test]
async fn status_reports_store_error_without_echoing_it() {
    let inner = connect("sqlite::memory:").await.unwrap();
    let failing = Arc::new(FailingStore::new(inner));
    let r = rig(Opts {
        store: Some(failing.clone()),
        ..Opts::default()
    })
    .await;
    failing.fail_stats.store(true, Ordering::SeqCst);
    let resp = r_status_open(&r).await;
    assert_eq!(resp.status, StatusCode::OK);
    let s = resp.json();
    assert_eq!(s["store"]["ok"], false);
    assert!(s["store"]["error"].is_string());
    assert!(!resp.text().contains("s3cretpw"), "{}", resp.text());
    assert!(!resp.text().contains("db.internal"));
    assert_eq!(s["guide"]["slots_total"], 0);
}

async fn r_status_open(r: &Rig) -> Resp {
    send(&r.app, "/status", &[]).await
}

// ----------------------------------------------------------------- metrics

#[tokio::test]
async fn metrics_include_all_required_names() {
    let r = rig(Opts {
        with_backup: true,
        sources: vec![("static-1", vec![]), ("hdhomerun-1", vec![])],
        ..Opts::default()
    })
    .await;
    let resp = send(&r.app, "/metrics", &[]).await;
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(
        resp.headers.get(header::CONTENT_TYPE).unwrap(),
        "text/plain; version=0.0.4; charset=utf-8"
    );
    let m = resp.text();
    for name in [
        "patina_provider_requests_total",
        "patina_provider_budget_used",
        "patina_provider_budget_limit",
        "patina_circuit_open",
        "patina_guide_horizon_seconds",
        "patina_guide_coverage_ratio",
        "patina_slots_total",
        "patina_janitor_deleted_total",
        "patina_merge_dropped_total",
        "patina_build_info",
        "patina_source_channels",
    ] {
        assert!(
            m.lines().any(|l| l.starts_with(&format!("# TYPE {name} "))),
            "missing {name} in:\n{m}"
        );
        assert!(
            m.lines()
                .any(|l| !l.starts_with('#') && l.starts_with(name)),
            "{name} has no sample"
        );
    }
    // Every provider x outcome series exists from the first scrape.
    for p in ["gracenote", "backup"] {
        for o in [
            "ok",
            "cache",
            "bad_window",
            "rate_limited",
            "transient",
            "fatal",
            "parse_failed",
            "skipped_budget",
            "skipped_circuit",
            "skipped_bad_window",
            "skipped_backoff",
        ] {
            assert_eq!(
                series(
                    &m,
                    "patina_provider_requests_total",
                    &[("provider", p), ("outcome", o)]
                ),
                0.0
            );
        }
        assert_eq!(
            series(&m, "patina_provider_budget_used", &[("provider", p)]),
            0.0
        );
        assert_eq!(series(&m, "patina_circuit_open", &[("provider", p)]), 0.0);
    }
    assert_eq!(
        series(
            &m,
            "patina_provider_budget_limit",
            &[("provider", "gracenote")]
        ),
        1000.0
    );
    assert_eq!(
        series(
            &m,
            "patina_provider_budget_limit",
            &[("provider", "backup")]
        ),
        7.0
    );
    for reason in ["invalid_time", "missing_title", "duplicate"] {
        assert_eq!(
            series(&m, "patina_merge_dropped_total", &[("reason", reason)]),
            0.0
        );
    }
    assert_eq!(
        series(
            &m,
            "patina_build_info",
            &[("version", env!("CARGO_PKG_VERSION"))]
        ),
        1.0
    );
    for src in ["static-1", "hdhomerun-1"] {
        for st in ["matched", "unmatched"] {
            series(
                &m,
                "patina_source_channels",
                &[("source", src), ("state", st)],
            );
        }
    }
    // No label values other than configured names.
    assert!(!m.contains("provider=\"unknown\""));
}

#[tokio::test]
async fn janitor_series_preregistered_at_zero() {
    let r = rig(Opts::default()).await;
    let m = scrape_open(&r).await;
    for c in ["raw", "programmes", "images", "history", "state", "disk"] {
        assert_eq!(
            series(&m, "patina_janitor_deleted_total", &[("category", c)]),
            0.0
        );
    }
    // And Task 13's handle really increments them.
    r.metrics.janitor_deleted(JanitorCategory::Raw, 3);
    r.metrics.janitor_deleted(JanitorCategory::Disk, 1);
    let m = scrape_open(&r).await;
    assert_eq!(
        series(&m, "patina_janitor_deleted_total", &[("category", "raw")]),
        3.0
    );
    assert_eq!(
        series(&m, "patina_janitor_deleted_total", &[("category", "disk")]),
        1.0
    );
    assert_eq!(
        series(
            &m,
            "patina_janitor_deleted_total",
            &[("category", "images")]
        ),
        0.0
    );
}

async fn scrape_open(r: &Rig) -> String {
    let resp = send(&r.app, "/metrics", &[]).await;
    assert_eq!(resp.status, StatusCode::OK);
    resp.text()
}

#[tokio::test]
async fn budget_metric_updates_after_a_fetch() {
    let r = rig(Opts {
        admin_token: Some(ADMIN),
        ..Opts::default()
    })
    .await;
    let m = scrape(&r).await;
    assert_eq!(
        series(
            &m,
            "patina_provider_budget_used",
            &[("provider", "gracenote")]
        ),
        0.0
    );
    assert_eq!(
        series(
            &m,
            "patina_provider_budget_limit",
            &[("provider", "gracenote")]
        ),
        1000.0
    );

    r.fetch.fetch(&gn(), &lineup(), window(0)).await;
    r.fetch.fetch(&gn(), &lineup(), window(1)).await;
    let m = scrape(&r).await;
    assert_eq!(
        series(
            &m,
            "patina_provider_budget_used",
            &[("provider", "gracenote")]
        ),
        2.0
    );
}

#[tokio::test]
async fn circuit_open_metric_reflects_open_circuit() {
    let r = rig(Opts {
        admin_token: Some(ADMIN),
        with_backup: true,
        ..Opts::default()
    })
    .await;
    let fb = r.fake_backup.as_ref().unwrap();
    fb.set_default(Behavior::Status(403));
    let m = scrape(&r).await;
    assert_eq!(
        series(&m, "patina_circuit_open", &[("provider", "backup")]),
        0.0
    );

    // One 403 is fatal and opens the breaker.
    r.fetch.fetch(&backup(), &lineup(), window(0)).await;
    let m = scrape(&r).await;
    assert_eq!(
        series(&m, "patina_circuit_open", &[("provider", "backup")]),
        1.0
    );
    assert_eq!(
        series(&m, "patina_circuit_open", &[("provider", "gracenote")]),
        0.0
    );

    // After the cooldown the circuit is half-open: still reported as 1.
    r.clock.advance(Duration::from_secs(16 * 60));
    let m = scrape(&r).await;
    assert_eq!(
        series(&m, "patina_circuit_open", &[("provider", "backup")]),
        1.0
    );
    let s = status_json(&r).await;
    assert_eq!(provider_json(&s, "backup")["circuit"], "half_open");

    // A good probe closes it.
    fb.set_default(Behavior::ok_shifted());
    assert!(matches!(
        r.fetch.fetch(&backup(), &lineup(), window(1)).await,
        FetchOutcome::Fresh(_)
    ));
    let m = scrape(&r).await;
    assert_eq!(
        series(&m, "patina_circuit_open", &[("provider", "backup")]),
        0.0
    );
}

#[tokio::test]
async fn requests_total_counts_outcomes() {
    let r = rig(Opts {
        admin_token: Some(ADMIN),
        ..Opts::default()
    })
    .await;
    let count = |m: &str, o: &str| {
        series(
            m,
            "patina_provider_requests_total",
            &[("provider", "gracenote"), ("outcome", o)],
        )
    };

    // ok, then the same window again: cache (no second request).
    r.fetch.fetch(&gn(), &lineup(), window(0)).await;
    r.fetch.fetch(&gn(), &lineup(), window(0)).await;
    assert_eq!(r.fake.request_count(), 1);
    // HTTP 400 for another window: bad_window; again: skipped_bad_window.
    r.fake
        .set_for_window(window(1).timestamp(), Behavior::Status(400));
    r.fetch.fetch(&gn(), &lineup(), window(1)).await;
    r.fetch.fetch(&gn(), &lineup(), window(1)).await;
    // 429: rate_limited; the next window is held back: skipped_backoff.
    r.fake
        .set_for_window(window(2).timestamp(), Behavior::RetryAfter(120));
    r.fetch.fetch(&gn(), &lineup(), window(2)).await;
    r.fetch.fetch(&gn(), &lineup(), window(3)).await;

    let m = scrape(&r).await;
    assert_eq!(count(&m, "ok"), 1.0);
    assert_eq!(count(&m, "cache"), 1.0);
    assert_eq!(count(&m, "bad_window"), 1.0);
    assert_eq!(count(&m, "skipped_bad_window"), 1.0);
    assert_eq!(count(&m, "rate_limited"), 1.0);
    assert_eq!(count(&m, "skipped_backoff"), 1.0);
    for o in [
        "transient",
        "fatal",
        "parse_failed",
        "skipped_budget",
        "skipped_circuit",
    ] {
        assert_eq!(count(&m, o), 0.0, "{o}");
    }
    // Three requests reached the provider: ok, 400, 429.
    assert_eq!(r.fake.request_count(), 3);
}

#[tokio::test]
async fn requests_total_counts_failures_and_skips() {
    let r = rig(Opts {
        admin_token: Some(ADMIN),
        budget: Some(2),
        ..Opts::default()
    })
    .await;
    let count = |m: &str, o: &str| {
        series(
            m,
            "patina_provider_requests_total",
            &[("provider", "gracenote"), ("outcome", o)],
        )
    };
    // 500 (transient), then after the backoff a truncated body (parse_failed).
    r.fake.script([Behavior::Status(500), Behavior::Truncated]);
    r.fetch.fetch(&gn(), &lineup(), window(0)).await;
    r.clock.advance(Duration::from_secs(3600));
    r.fetch.fetch(&gn(), &lineup(), window(1)).await;
    // Budget of 2 is now used up.
    r.fetch.fetch(&gn(), &lineup(), window(2)).await;
    let m = scrape(&r).await;
    assert_eq!(count(&m, "transient"), 1.0);
    assert_eq!(count(&m, "parse_failed"), 1.0);
    assert_eq!(count(&m, "skipped_budget"), 1.0);

    // A 403 opens the circuit; the next attempt is skipped_circuit.
    let r = rig(Opts {
        admin_token: Some(ADMIN),
        ..Opts::default()
    })
    .await;
    r.fake.script([Behavior::Status(403)]);
    r.fetch.fetch(&gn(), &lineup(), window(0)).await;
    r.fetch.fetch(&gn(), &lineup(), window(1)).await;
    let m = scrape(&r).await;
    assert_eq!(count(&m, "fatal"), 1.0);
    assert_eq!(count(&m, "skipped_circuit"), 1.0);
}

#[tokio::test]
async fn merge_dropped_counted_from_fresh_responses_only() {
    // A response with one duplicate, one blank title and one reversed time.
    let mut grid: serde_json::Value = serde_json::from_slice(&fixture()).unwrap();
    let ch = grid["channels"][0].clone();
    grid["channels"] = serde_json::json!([ch]);
    let events = grid["channels"][0]["events"].as_array_mut().unwrap();
    let dup = events[0].clone();
    let mut blank = events[1].clone();
    blank["program"]["title"] = "  ".into();
    let mut reversed = events[2].clone();
    reversed["endTime"] = reversed["startTime"].clone();
    events.push(dup);
    events.push(blank);
    events.push(reversed);
    let body = Bytes::from(serde_json::to_vec(&grid).unwrap());

    let r = rig(Opts {
        admin_token: Some(ADMIN),
        ..Opts::default()
    })
    .await;
    r.fake.set_default(Behavior::Ok(body));
    let report = r.sched.tick().await;
    assert!(report.fetched >= 1, "{report:?}");
    let n = report.fetched as f64;
    let m = scrape(&r).await;
    assert_eq!(
        series(&m, "patina_merge_dropped_total", &[("reason", "duplicate")]),
        n
    );
    assert_eq!(
        series(
            &m,
            "patina_merge_dropped_total",
            &[("reason", "missing_title")]
        ),
        n
    );
    assert_eq!(
        series(
            &m,
            "patina_merge_dropped_total",
            &[("reason", "invalid_time")]
        ),
        n
    );
}

#[tokio::test]
async fn metrics_scrape_survives_store_error() {
    let inner = connect("sqlite::memory:").await.unwrap();
    let failing = Arc::new(FailingStore::new(inner));
    let r = rig(Opts {
        admin_token: Some(ADMIN),
        store: Some(failing.clone()),
        ..Opts::default()
    })
    .await;
    r.sched.tick().await;
    let m1 = scrape(&r).await;
    let slots = series(&m1, "patina_slots_total", &[]);
    assert!(slots > 100.0);
    let requests_ok = series(
        &m1,
        "patina_provider_requests_total",
        &[("provider", "gracenote"), ("outcome", "ok")],
    );
    assert!(requests_ok > 0.0);

    // The database goes away: the scrape still answers 200 with the
    // counters, and the gauges keep their last values.
    failing.fail_stats.store(true, Ordering::SeqCst);
    let resp = authed(&r.app, "/metrics").await;
    assert_eq!(resp.status, StatusCode::OK);
    let m2 = resp.text();
    assert_eq!(series(&m2, "patina_slots_total", &[]), slots);
    assert_eq!(
        series(
            &m2,
            "patina_provider_requests_total",
            &[("provider", "gracenote"), ("outcome", "ok")]
        ),
        requests_ok
    );
    assert!(!m2.contains("s3cretpw"));
}

// -------------------------------------------------------------------- auth

#[tokio::test]
async fn admin_token_required_when_configured() {
    let r = rig(Opts {
        admin_token: Some(ADMIN),
        ..Opts::default()
    })
    .await;
    for path in ["/status", "/metrics"] {
        let none = send(&r.app, path, &[]).await;
        assert_eq!(none.status, StatusCode::UNAUTHORIZED, "{path}");
        assert!(none.body.is_empty());
        assert_eq!(
            none.headers.get(header::WWW_AUTHENTICATE).unwrap(),
            "Bearer"
        );

        for bad in [
            bearer("wrong"),
            bearer(&ADMIN.to_uppercase()),
            bearer(&format!("{ADMIN} ")),
            bearer(&ADMIN[..ADMIN.len() - 1]),
            ADMIN.to_string(),          // no scheme
            "Bearer".to_string(),       // no token
            "Bearer ".to_string(),      // empty token
            format!("Basic {ADMIN}"),   // wrong scheme
            format!("Bearer  {ADMIN}"), // two spaces
        ] {
            let resp = send(&r.app, path, &[("authorization", &bad)]).await;
            assert_eq!(resp.status, StatusCode::UNAUTHORIZED, "{path} with {bad:?}");
            assert!(resp.body.is_empty());
        }

        let ok = send(&r.app, path, &[("authorization", &bearer(ADMIN))]).await;
        assert_eq!(ok.status, StatusCode::OK, "{path}");
        // The scheme name is case-insensitive.
        let ok = send(
            &r.app,
            path,
            &[("authorization", &format!("bearer {ADMIN}"))],
        )
        .await;
        assert_eq!(ok.status, StatusCode::OK, "{path}");
    }

    // No token configured: open.
    let open = rig(Opts::default()).await;
    assert_eq!(send(&open.app, "/status", &[]).await.status, StatusCode::OK);
    assert_eq!(
        send(&open.app, "/metrics", &[]).await.status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn healthz_stays_open_even_with_admin_token() {
    // A store that is down: /healthz must not care (it never touches it).
    let inner = connect("sqlite::memory:").await.unwrap();
    let failing = Arc::new(FailingStore::new(inner));
    failing.fail_stats.store(true, Ordering::SeqCst);
    let r = rig(Opts {
        admin_token: Some(ADMIN),
        feed_token: Some(FEED),
        store: Some(failing),
        ..Opts::default()
    })
    .await;
    let resp = send(&r.app, "/healthz", &[]).await;
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(resp.text(), "ok");
    // A wrong token on /healthz changes nothing either.
    let resp = send(&r.app, "/healthz", &[("authorization", "Bearer nope")]).await;
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn admin_endpoints_reject_query_token_and_multiple_auth_headers() {
    let r = rig(Opts {
        admin_token: Some(ADMIN),
        ..Opts::default()
    })
    .await;
    for uri in [
        format!("/status?token={ADMIN}"),
        format!("/metrics?token={ADMIN}"),
        format!("/status?access_token={ADMIN}&admin_token={ADMIN}"),
        format!("/metrics?bearer={ADMIN}"),
    ] {
        let resp = send(&r.app, &uri, &[]).await;
        assert_eq!(resp.status, StatusCode::UNAUTHORIZED, "{uri}");
        assert!(resp.body.is_empty());
    }
    // A token in the path is not a route at all.
    for uri in [format!("/status/{ADMIN}"), format!("/t/{ADMIN}/status")] {
        let resp = send(&r.app, &uri, &[]).await;
        assert_ne!(resp.status, StatusCode::OK, "{uri}");
    }
    // Two Authorization headers are refused even when one is right, in
    // either order.
    let good = bearer(ADMIN);
    for path in ["/status", "/metrics"] {
        for pair in [
            [
                ("authorization", good.as_str()),
                ("authorization", "Bearer wrong"),
            ],
            [
                ("authorization", "Bearer wrong"),
                ("authorization", good.as_str()),
            ],
            [
                ("authorization", good.as_str()),
                ("authorization", good.as_str()),
            ],
        ] {
            let resp = send(&r.app, path, &pair).await;
            assert_eq!(resp.status, StatusCode::UNAUTHORIZED, "{path} {pair:?}");
            assert!(resp.body.is_empty());
        }
    }
}

/// (case description, status, sorted headers, body)
type Refusal = (String, StatusCode, Vec<(String, Vec<u8>)>, Vec<u8>);

#[tokio::test]
async fn unauthorized_responses_are_identical_for_status_and_metrics() {
    let r = rig(Opts {
        admin_token: Some(ADMIN),
        ..Opts::default()
    })
    .await;
    let good = bearer(ADMIN);
    let mut seen: Vec<Refusal> = Vec::new();
    for path in ["/status", "/metrics"] {
        let cases: Vec<Vec<(&str, &str)>> = vec![
            vec![],
            vec![("authorization", "Bearer wrong")],
            vec![("authorization", "Bearer")],
            vec![("authorization", "Basic abc")],
            vec![
                ("authorization", good.as_str()),
                ("authorization", good.as_str()),
            ],
        ];
        for headers in cases {
            let resp = send(&r.app, path, &headers).await;
            let mut hs: Vec<(String, Vec<u8>)> = resp
                .headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
                .collect();
            hs.sort();
            seen.push((format!("{path} {headers:?}"), resp.status, hs, resp.body));
        }
    }
    let (first_name, status, headers, body) = &seen[0];
    assert_eq!(*status, StatusCode::UNAUTHORIZED);
    assert!(body.is_empty());
    assert_eq!(
        headers,
        &vec![
            ("content-length".to_string(), b"0".to_vec()),
            ("www-authenticate".to_string(), b"Bearer".to_vec()),
        ]
    );
    for (name, s, h, b) in &seen {
        assert_eq!(
            (s, h, b),
            (status, headers, body),
            "{name} differs from {first_name}"
        );
    }
}

#[tokio::test]
async fn status_never_contains_tokens_or_db_password() {
    // Both a healthy store and one whose errors carry a password.
    for broken in [false, true] {
        let inner = connect("sqlite::memory:").await.unwrap();
        let failing = Arc::new(FailingStore::new(inner));
        let r = rig(Opts {
            admin_token: Some(ADMIN),
            feed_token: Some(FEED),
            store: Some(failing.clone()),
            with_backup: true,
            sources: vec![("static-1", vec![])],
            ..Opts::default()
        })
        .await;
        r.fake_backup
            .as_ref()
            .unwrap()
            .set_default(Behavior::Status(403));
        r.sched.tick().await;
        r.fetch.fetch(&backup(), &lineup(), window(0)).await;
        r.feeds.refresh_channels().await;
        failing.fail_stats.store(broken, Ordering::SeqCst);

        let status = authed(&r.app, "/status").await.text();
        let metrics = scrape(&r).await;
        for text in [&status, &metrics] {
            for secret in [
                ADMIN,
                FEED,
                "s3cretpw",
                "db.internal",
                "authorization",
                "Bearer",
            ] {
                assert!(
                    !text.contains(secret),
                    "{secret} leaked (broken={broken}) in {text}"
                );
            }
        }
    }
}

#[tokio::test]
async fn feed_auth_behaviour_unchanged_after_ct_eq_move() {
    // The comparison itself.
    assert!(ct_eq(b"", b""));
    assert!(ct_eq(b"abc", b"abc"));
    assert!(!ct_eq(b"abc", b"abd"));
    assert!(!ct_eq(b"abc", b"abcd"));
    assert!(!ct_eq(b"abcd", b"abc"));
    assert!(!ct_eq(b"abc", b""));
    assert!(!ct_eq(b"abc\0", b"abc")); // zero padding must not equal absence

    // The feed routes: query and path tokens, bare empty 401, and no
    // admin-style WWW-Authenticate header on feed refusals.
    let r = rig(Opts {
        feed_token: Some(FEED),
        admin_token: Some(ADMIN),
        sources: vec![("static-1", vec![])],
        ..Opts::default()
    })
    .await;
    for uri in [
        "/xmltv/all.xml".to_string(),
        "/xmltv/all.xml?token=wrong".to_string(),
        "/t/wrong/xmltv/all.xml".to_string(),
        format!("/xmltv/all.xml?token={ADMIN}"), // the admin token is not the feed token
    ] {
        let resp = send(&r.app, &uri, &[]).await;
        assert_eq!(resp.status, StatusCode::UNAUTHORIZED, "{uri}");
        assert!(resp.body.is_empty());
        assert!(resp.headers.get(header::WWW_AUTHENTICATE).is_none());
    }
    // The feed token is not an admin token, and the header is not a feed token.
    let resp = send(&r.app, "/status", &[("authorization", &bearer(FEED))]).await;
    assert_eq!(resp.status, StatusCode::UNAUTHORIZED);
    let resp = send(
        &r.app,
        "/xmltv/all.xml",
        &[("authorization", &bearer(FEED))],
    )
    .await;
    assert_eq!(resp.status, StatusCode::UNAUTHORIZED);
    // Right token (path and query) gets past auth: an unknown feed is 404.
    for uri in [
        format!("/xmltv/nope.xml?token={FEED}"),
        format!("/t/{FEED}/xmltv/nope.xml"),
    ] {
        assert_eq!(
            send(&r.app, &uri, &[]).await.status,
            StatusCode::NOT_FOUND,
            "{uri}"
        );
    }
    // A known feed that is not ready yet is 503 once authorized.
    let resp = send(&r.app, &format!("/xmltv/static-1.xml?token={FEED}"), &[]).await;
    assert_eq!(resp.status, StatusCode::SERVICE_UNAVAILABLE);
}

// ---------------------------------------------------- deadline and caching

/// A rig over a store that can hang, with one source so the match query
/// also runs, and an ingested guide.
async fn hanging_rig() -> (Rig, Arc<FailingStore>) {
    let inner = connect("sqlite::memory:").await.unwrap();
    let failing = Arc::new(FailingStore::new(inner));
    let r = rig(Opts {
        admin_token: Some(ADMIN),
        store: Some(failing.clone()),
        sources: vec![("static-1", vec![])],
        ..Opts::default()
    })
    .await;
    r.sched.tick().await;
    r.feeds.refresh_channels().await;
    (r, failing)
}

#[tokio::test]
async fn status_answers_within_deadline_when_store_hangs() {
    let (r, failing) = hanging_rig().await;
    failing.hang_stats.store(true, Ordering::SeqCst);
    let started = std::time::Instant::now();
    let resp = authed(&r.app, "/status").await;
    // One shared 300 ms deadline, not one wait per store call.
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(resp.status, StatusCode::OK);
    let s = resp.json();
    assert_eq!(s["store"]["ok"], false);
    assert!(s["store"]["error"].is_string());
    assert_eq!(provider_json(&s, "gracenote")["circuit"], "unknown");
    assert_eq!(s["guide"]["slots_total"], 0);
    // /healthz does not depend on the store.
    assert_eq!(send(&r.app, "/healthz", &[]).await.status, StatusCode::OK);
}

#[tokio::test]
async fn metrics_scrape_answers_within_deadline_when_store_hangs() {
    let (r, failing) = hanging_rig().await;
    let m1 = scrape(&r).await;
    let slots = series(&m1, "patina_slots_total", &[]);
    assert!(slots > 100.0);
    // Past the stats TTL so the hang is actually hit.
    r.clock.advance(Duration::from_secs(60));
    failing.hang_stats.store(true, Ordering::SeqCst);
    let started = std::time::Instant::now();
    let resp = authed(&r.app, "/metrics").await;
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(resp.status, StatusCode::OK);
    let m2 = resp.text();
    // Gauges keep their last values; counters are still there.
    assert_eq!(series(&m2, "patina_slots_total", &[]), slots);
    assert!(
        series(
            &m2,
            "patina_provider_requests_total",
            &[("provider", "gracenote"), ("outcome", "ok")]
        ) > 0.0
    );
    assert_eq!(send(&r.app, "/healthz", &[]).await.status, StatusCode::OK);
}

#[tokio::test]
async fn guide_stats_are_cached_between_scrapes_within_ttl() {
    let (r, failing) = hanging_rig().await;
    let calls = || failing.guide_stats_calls.load(Ordering::SeqCst);
    let before = calls();
    scrape(&r).await;
    r.clock.advance(Duration::from_secs(10));
    scrape(&r).await;
    status_json(&r).await;
    assert_eq!(
        calls() - before,
        1,
        "scrapes within the TTL share one computation"
    );
    // New data inside the TTL is not visible yet; after the TTL it is.
    r.clock.advance(Duration::from_secs(6));
    scrape(&r).await;
    assert_eq!(calls() - before, 2, "recomputed after the TTL");
}

#[tokio::test]
async fn concurrent_scrapes_share_one_computation() {
    let (r, failing) = hanging_rig().await;
    let before = failing.guide_stats_calls.load(Ordering::SeqCst);
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let app = r.app.clone();
        tasks.push(tokio::spawn(async move {
            authed(&app, "/metrics").await.status
        }));
    }
    for t in tasks {
        assert_eq!(t.await.unwrap(), StatusCode::OK);
    }
    assert_eq!(failing.guide_stats_calls.load(Ordering::SeqCst) - before, 1);
}

/// A real janitor run, publishing to the same sink `/status` reads and the
/// same `Metrics` `/metrics` renders.
async fn run_janitor(r: &Rig) -> patina::janitor::JanitorReport {
    let pid = ProviderId("gracenote".into());
    let old = patina::clock::Clock::now(r.clock.as_ref()) - chrono::TimeDelta::days(10);
    r.store
        .raw_put(&pid, "ota", old, old, b"old")
        .await
        .unwrap();
    r.store
        .raw_put(&pid, "ota", old, old, b"older")
        .await
        .unwrap();
    Janitor::new(
        r.store.clone(),
        patina::config::Retention::default(),
        None,
        r.clock.clone(),
    )
    .with_metrics(r.metrics.clone())
    .with_report_sink(r.janitor_sink.clone())
    .run_once()
    .await
}

#[tokio::test]
async fn status_shows_janitor_report_after_run() {
    let r = rig(Opts::default()).await;
    // Before the first run: null.
    assert!(status_json(&r).await["janitor"].is_null());

    let report = run_janitor(&r).await;
    assert_eq!(report.raw_deleted, 2);
    let s = status_json(&r).await;
    let j = &s["janitor"];
    assert!(j.is_object(), "{s}");
    assert_eq!(
        j["last_run"],
        report.finished_at.format("%Y-%m-%dT%H:%M:%SZ").to_string()
    );
    assert_eq!(j["deleted"]["raw"], 2);
    for k in ["programmes", "images", "history", "state", "disk"] {
        assert_eq!(j["deleted"][k], 0, "{k}");
    }
    assert!(j["size_before"].as_u64().unwrap() > 0);
    assert!(j["size_after"].is_u64());
    assert_eq!(j["errors"], serde_json::json!([]));
}

#[tokio::test]
async fn metrics_janitor_counter_matches_report() {
    let r = rig(Opts::default()).await;
    let report = run_janitor(&r).await;
    let m = scrape_open(&r).await;
    for (cat, n) in [
        ("raw", report.raw_deleted),
        ("programmes", report.programmes_deleted),
        ("images", report.images_deleted),
        ("history", report.history_deleted),
        ("state", report.state_deleted),
        ("disk", report.disk_evicted),
    ] {
        assert_eq!(
            series(&m, "patina_janitor_deleted_total", &[("category", cat)]),
            n as f64,
            "{cat}"
        );
    }
    // A second run adds to the counters (it is a counter, not a gauge).
    run_janitor(&r).await;
    let m = scrape_open(&r).await;
    assert_eq!(
        series(&m, "patina_janitor_deleted_total", &[("category", "raw")]),
        (report.raw_deleted * 2) as f64
    );
}
