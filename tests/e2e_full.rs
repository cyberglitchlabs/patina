//! End-to-end verification through the REAL startup wiring (`patina::app`).
//!
//! Nothing here assembles the pieces by hand: each test writes a real TOML
//! config and overrides file into a temp dir, loads it with `Config::load`,
//! and builds the application with `app::build`, exactly as `main` does. Only
//! two things are replaced: the provider's `base_url` and the tuner URL point
//! at local fake servers, and the clock is a `FakeClock`. No test contacts a
//! real provider.
//!
//! Every assertion about request counts uses the number the fake SERVER
//! counted, never the client's own bookkeeping.
//!
//! Time: "now" is 2026-10-31 20:00 UTC, the evening of the captured fixture
//! (see tests/ingest_e2e.rs). After the first tick the tests move the clock
//! to a round `FEED_TIME` (20:10:00). The scheduler's request pacing moves
//! the fake clock by a random few seconds, and jumping to a fixed time makes
//! every later feed render depend only on the stored data, not on that noise.

mod common;
mod support;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::routing::get;
use bytes::Bytes;
use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use http_body_util::BodyExt;
use patina::app::{App, AppDeps, build};
use patina::clock::{Clock, FakeClock};
use patina::config::{Config, LineupCfg};
use patina::merge::SlotWithObservations;
use patina::provider::gracenote::{GracenoteProvider, http_client};
use patina::provider::{Provider, RawResponse};
use patina::store::Store;
use patina::supervise::Exit;
use support::fake_provider::{Behavior, FakeProvider, fixture};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use url::Url;

const TUNER_LINEUP: &[u8] = include_bytes!("fixtures/hdhomerun/lineup.json");
const FEED_TOKEN: &str = "feed-token-for-e2e";
const ADMIN_TOKEN: &str = "admin-token-for-e2e";

fn now0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 31, 20, 0, 0).unwrap()
}

/// A round time shortly after the first tick; see the module comment.
fn feed_time() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 31, 20, 10, 0).unwrap()
}

// ---------------------------------------------------------------------------
// The fake tuner and the config files
// ---------------------------------------------------------------------------

/// A tiny local HDHomeRun: `GET /lineup.json` returns the captured lineup.
struct FakeTuner {
    url: String,
    hits: Arc<AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
}

impl FakeTuner {
    async fn start() -> Self {
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        let app = Router::new().route(
            "/lineup.json",
            get(move || {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    ([(header::CONTENT_TYPE, "application/json")], TUNER_LINEUP)
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        FakeTuner { url, hits, server }
    }
}

impl Drop for FakeTuner {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// The provider channel id of a call sign in the captured Gracenote grid,
/// found with the real parser (the overrides below need real ids).
fn provider_channel_id(call_sign: &str) -> String {
    let p = GracenoteProvider::new(
        http_client(Duration::from_secs(10)).unwrap(),
        Url::parse("http://127.0.0.1:1/").unwrap(), // never contacted: parse only
    );
    let lineup = LineupCfg {
        name: "ota".into(),
        lineup_id: "USA-OTA55401".into(),
        postal_code: "55401".into(),
        country: "USA".into(),
    };
    let raw = RawResponse {
        status: 200,
        body: fixture(),
    };
    p.parse(&lineup, &raw, now0())
        .channels
        .into_iter()
        .find(|c| c.call_sign == call_sign)
        .unwrap_or_else(|| panic!("no channel {call_sign} in the fixture"))
        .provider_channel_id
}

/// The realistic overrides pinned by tests/channels.rs
/// (`pinned_counts_with_realistic_overrides`): fix Gracenote's 9.x numbering,
/// ignore the junk 5000 entry, and choose one of the two 5.6 channels. With
/// them 83 tuner channels match.
fn overrides_toml() -> String {
    format!(
        "[[override]]\nnumber = \"9.1\"\nprovider_channel_id = \"{}\"\n\
         [[override]]\nnumber = \"9.2\"\nprovider_channel_id = \"{}\"\n\
         [[override]]\nnumber = \"9.9\"\nprovider_channel_id = \"{}\"\n\
         [[override]]\nnumber = \"5000\"\nignore = true\n\
         [[override]]\nnumber = \"5.6\"\nprovider_channel_id = \"{}\"\n",
        provider_channel_id("KMSPDT"),
        provider_channel_id("WFTCDT"),
        provider_channel_id("WFTCDT2"),
        provider_channel_id("KSTCDT6"),
    )
}

/// Write the overrides file and a config file into `dir` and load the config
/// the way `main` does (file text through `Config::load`). Both tokens are
/// set. `database_url` is a SQLite file in `dir` or a Postgres URL.
fn write_config(
    dir: &Path,
    database_url: &str,
    provider_base: &Url,
    tuner_url: &str,
) -> (Config, String) {
    let overrides = dir.join("overrides.toml");
    std::fs::write(&overrides, overrides_toml()).unwrap();
    let text = format!(
        r#"
timezone = "America/Chicago"
database_url = "{database_url}"
feed_token = "{FEED_TOKEN}"
admin_token = "{ADMIN_TOKEN}"

[[providers]]
kind = "gracenote"
name = "gracenote"
daily_budget = 150
base_url = "{provider_base}"

[[providers.lineups]]
name = "ota"
lineup_id = "USA-OTA55401"
postal_code = "55401"
country = "USA"

[[sources]]
kind = "hdhomerun"
url = "{tuner_url}"
lineups = ["ota"]
overrides_path = "{overrides}"
"#,
        overrides = overrides.display(),
    );
    std::fs::write(dir.join("patina.toml"), &text).unwrap();
    let config = Config::load(&std::collections::HashMap::new(), Some(&text)).unwrap();
    (config, text)
}

fn sqlite_url(dir: &Path) -> String {
    format!("sqlite://{}?mode=rwc", dir.join("patina.db").display())
}

// ---------------------------------------------------------------------------
// The running stack
// ---------------------------------------------------------------------------

struct Stack {
    app: App,
    fake: FakeProvider,
    tuner: FakeTuner,
    clock: Arc<FakeClock>,
    _dir: tempfile::TempDir,
}

/// Fake provider (shifted fixture for every window), fake tuner, config,
/// and the app built through `app::build`, with the channel lists loaded.
async fn start(database_url: Option<String>) -> Stack {
    let dir = tempfile::tempdir().unwrap();
    let fake = FakeProvider::start().await;
    fake.set_default(Behavior::ok_shifted());
    let tuner = FakeTuner::start().await;
    let clock = Arc::new(FakeClock::new(now0()));
    let url = database_url.unwrap_or_else(|| sqlite_url(dir.path()));
    let (config, _) = write_config(dir.path(), &url, &fake.base_url(), &tuner.url);
    let app = build(config, AppDeps::with_clock(clock.clone()))
        .await
        .expect("app builds");
    // Deterministic: load the tuner lists now instead of waiting for the
    // background refresher.
    app.feeds.refresh_channels().await;
    Stack {
        app,
        fake,
        tuner,
        clock,
        _dir: dir,
    }
}

/// Rebuild the app over the same config text and database (a restart).
async fn rebuild(
    dir: &Path,
    fake: &FakeProvider,
    tuner: &FakeTuner,
    clock: &Arc<FakeClock>,
    database_url: &str,
) -> App {
    let (config, _) = write_config(dir, database_url, &fake.base_url(), &tuner.url);
    let app = build(config, AppDeps::with_clock(clock.clone()))
        .await
        .expect("app rebuilds");
    app.feeds.refresh_channels().await;
    app
}

struct Resp {
    status: StatusCode,
    body: Bytes,
}

impl Resp {
    fn text(&self) -> String {
        String::from_utf8(self.body.to_vec()).unwrap()
    }
    fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).expect("JSON body")
    }
}

/// One request through the real router (no socket needed).
async fn get_with(router: &Router, uri: &str, bearer: Option<&str>) -> Resp {
    let mut req = Request::builder().uri(uri);
    if let Some(t) = bearer {
        req = req.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    let resp = router
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    Resp {
        status: resp.status(),
        body: resp.into_body().collect().await.unwrap().to_bytes(),
    }
}

async fn feed(router: &Router, name: &str) -> Resp {
    get_with(
        router,
        &format!("/xmltv/{name}.xml?token={FEED_TOKEN}"),
        None,
    )
    .await
}

async fn status(router: &Router) -> serde_json::Value {
    let r = get_with(router, "/status", Some(ADMIN_TOKEN)).await;
    assert_eq!(r.status, StatusCode::OK);
    r.json()
}

/// Parse with a strict XML parser and summarise.
struct FeedSummary {
    channels: Vec<String>,
    display_names: Vec<String>,
    programmes: usize,
    programmes_with_icon: usize,
}

fn summarize(xml: &str) -> FeedSummary {
    let doc = roxmltree::Document::parse(xml).expect("feed must be well-formed XML");
    let mut s = FeedSummary {
        channels: vec![],
        display_names: vec![],
        programmes: 0,
        programmes_with_icon: 0,
    };
    for n in doc.root_element().children().filter(|n| n.is_element()) {
        match n.tag_name().name() {
            "channel" => {
                s.channels.push(n.attribute("id").unwrap().to_string());
                for d in n.children().filter(|c| c.has_tag_name("display-name")) {
                    s.display_names.push(d.text().unwrap_or("").to_string());
                }
            }
            "programme" => {
                s.programmes += 1;
                if n.children()
                    .any(|c| c.has_tag_name("icon") && c.attribute("src").is_some())
                {
                    s.programmes_with_icon += 1;
                }
            }
            _ => {}
        }
    }
    s
}

/// The value of the series line `name{...labels...} value`, where every
/// `label="value"` in `labels` appears in the line.
fn series(metrics: &str, name: &str, labels: &[&str]) -> Option<f64> {
    metrics
        .lines()
        .filter(|l| !l.starts_with('#') && l.starts_with(name))
        .find(|l| labels.iter().all(|lab| l.contains(lab)))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
}

/// Every channel with every slot and its observations.
async fn dump(store: &Arc<dyn Store>) -> Vec<(String, Vec<SlotWithObservations>)> {
    let far_past = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
    let far_future = Utc.with_ymd_and_hms(2100, 1, 1, 0, 0, 0).unwrap();
    let mut out = Vec::new();
    for c in store.list_channels().await.unwrap() {
        let slots = store
            .slots_with_observations(&c.id, far_past, far_future)
            .await
            .unwrap();
        out.push((c.id.0, slots));
    }
    out
}

/// Slots that have not started yet at `now`, as comparable tuples.
fn future_slots(
    d: &[(String, Vec<SlotWithObservations>)],
    now: DateTime<Utc>,
) -> Vec<(String, DateTime<Utc>, DateTime<Utc>, String)> {
    let mut v = Vec::new();
    for (ch, slots) in d {
        for s in slots {
            if s.slot.start >= now {
                v.push((ch.clone(), s.slot.start, s.slot.end, s.slot.title.clone()));
            }
        }
    }
    v.sort();
    v
}

/// Every slot has at least one observation that explains it.
fn assert_every_slot_has_observations(d: &[(String, Vec<SlotWithObservations>)]) {
    for (ch, slots) in d {
        for s in slots {
            assert!(
                !s.observations.is_empty(),
                "slot without observations on {ch}: {:?}",
                s.slot
            );
        }
    }
}

/// The atomicity invariant of a merge for CONSISTENT input: within a channel
/// slots never overlap, and every slot has observations.
fn assert_internally_consistent(d: &[(String, Vec<SlotWithObservations>)]) {
    assert_every_slot_has_observations(d);
    for (ch, slots) in d {
        let mut sorted: Vec<_> = slots.iter().collect();
        sorted.sort_by_key(|s| s.slot.start);
        for pair in sorted.windows(2) {
            assert!(
                pair[0].slot.end <= pair[1].slot.start,
                "overlapping slots on {ch}: {:?} / {:?}",
                pair[0].slot,
                pair[1].slot
            );
        }
    }
}

const REQUIRED_SERIES: [&str; 11] = [
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
];

// ---------------------------------------------------------------------------
// Scenario 1: the whole stack, on a given database
// ---------------------------------------------------------------------------

async fn full_stack_scenario(database_url: Option<String>) {
    let s = start(database_url).await;
    let router = s.app.router.clone();

    // /healthz is open; nothing is served before the first tick except that.
    let h = get_with(&router, "/healthz", None).await;
    assert_eq!((h.status, h.text()), (StatusCode::OK, "ok".to_string()));
    let before_tick = feed(&router, "hdhomerun-1").await;
    assert_eq!(before_tick.status, StatusCode::SERVICE_UNAVAILABLE);

    // One scheduler tick: 29 windows for a 7-day horizon from 20:00.
    let report = s.app.scheduler.tick().await;
    assert_eq!(report.failed, 0, "{report:?}");
    let requests = s.fake.request_count();
    assert_eq!(
        requests, 29,
        "7 days of 6-hour windows plus the current one"
    );
    assert_eq!(report.fetched, requests);
    s.clock.set(feed_time());

    // Feeds: token required.
    let source = feed(&router, "hdhomerun-1").await;
    assert_eq!(source.status, StatusCode::OK);
    let no_token = get_with(&router, "/xmltv/hdhomerun-1.xml", None).await;
    assert_eq!(
        (no_token.status, no_token.body.len()),
        (StatusCode::UNAUTHORIZED, 0)
    );
    let wrong = get_with(&router, "/xmltv/all.xml?token=nope", None).await;
    assert_eq!(
        (wrong.status, wrong.body.len()),
        (StatusCode::UNAUTHORIZED, 0)
    );
    let by_path = get_with(
        &router,
        &format!("/t/{FEED_TOKEN}/xmltv/hdhomerun-1.xml"),
        None,
    )
    .await;
    assert_eq!(by_path.status, StatusCode::OK);

    let sum = summarize(&source.text());
    assert_eq!(
        sum.channels.len(),
        83,
        "83 matched with the realistic overrides"
    );
    assert!(
        sum.display_names.iter().any(|n| n == "4.1 WCCO-DT"),
        "WCCO is in the feed"
    );
    assert!(sum.programmes > 500, "{} programmes", sum.programmes);
    assert!(
        sum.programmes_with_icon > 0,
        "programme pictures are passed through as <icon src=...>"
    );

    // The combined feed has the same channels (one source).
    let all = feed(&router, "all").await;
    assert_eq!(all.status, StatusCode::OK);
    let all_sum = summarize(&all.text());
    assert_eq!(all_sum.channels, sum.channels);
    assert_eq!(all_sum.programmes, sum.programmes);

    // /status: admin token required; numbers agree with the fake SERVER.
    let denied = get_with(&router, "/status", None).await;
    assert_eq!(denied.status, StatusCode::UNAUTHORIZED);
    let st = status(&router).await;
    let p = &st["providers"][0];
    assert_eq!(p["name"], "gracenote");
    assert_eq!(
        p["budget"]["used"], requests as u64,
        "budget used == requests the server saw"
    );
    assert_eq!(p["budget"]["limit"], 150);
    assert_eq!(p["circuit"], "closed");
    assert_eq!(p["consecutive_failures"], 0);
    assert!(st["guide"]["coverage_pct"].as_f64().unwrap() > 0.0);
    assert!(st["guide"]["slots_total"].as_u64().unwrap() > 500);
    assert_eq!(st["sources"][0]["name"], "hdhomerun-1");
    assert_eq!(st["sources"][0]["channels_matched"], 83);
    assert_eq!(st["store"]["ok"], true);
    assert!(st["janitor"].is_null(), "no janitor run yet");

    // /metrics: every required series, and the ok counter equals the server's.
    let m = get_with(&router, "/metrics", Some(ADMIN_TOKEN)).await;
    assert_eq!(m.status, StatusCode::OK);
    let metrics = m.text();
    for name in REQUIRED_SERIES {
        assert!(metrics.contains(name), "missing series {name}");
    }
    assert_eq!(
        series(
            &metrics,
            "patina_provider_requests_total",
            &["provider=\"gracenote\"", "outcome=\"ok\""]
        ),
        Some(requests as f64)
    );
    assert_eq!(
        series(
            &metrics,
            "patina_provider_budget_used",
            &["provider=\"gracenote\""]
        ),
        Some(requests as f64)
    );
    assert_eq!(
        get_with(&router, "/metrics", None).await.status,
        StatusCode::UNAUTHORIZED
    );

    // Janitor, once: fresh data loses nothing; future slots and the feed are
    // unchanged; /status now shows a report with zero deletions.
    let store = s.app.store.clone();
    let before = dump(&store).await;
    let future_before = future_slots(&before, s.clock.now());
    assert!(!future_before.is_empty());
    let jr = s.app.janitor.run_once().await;
    assert!(jr.errors.is_empty(), "{:?}", jr.errors);
    let after = dump(&store).await;
    assert_eq!(future_slots(&after, s.clock.now()), future_before);
    assert_every_slot_has_observations(&after);
    // Past the 30 s feed cache, so this really re-renders from the store.
    s.clock.set(feed_time() + TimeDelta::seconds(31));
    let source2 = feed(&router, "hdhomerun-1").await;
    assert_eq!(source2.body, source.body, "feed unchanged by the janitor");
    s.clock.set(feed_time() + TimeDelta::seconds(120));
    let st = status(&router).await;
    let j = &st["janitor"];
    assert!(j.is_object(), "{j}");
    for k in ["raw", "programmes", "images", "history", "state", "disk"] {
        assert_eq!(j["deleted"][k], 0, "janitor deleted {k} from fresh data");
    }
    assert_eq!(j["errors"].as_array().unwrap().len(), 0);

    // And the provider heard nothing further, and the tuner list was read.
    assert_eq!(s.fake.request_count(), requests);
    assert!(s.tuner.hits.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
async fn e2e_full_stack_with_fake_provider_and_both_backends() {
    full_stack_scenario(None).await;
    // Same scenario on Postgres when a server is configured (skipped
    // otherwise; CI sets PATINA_REQUIRE_POSTGRES=1 so it cannot vanish).
    common::pg::with_fresh_schema(|url| async move {
        full_stack_scenario(Some(url)).await;
    })
    .await;
}

// ---------------------------------------------------------------------------
// Scenario 2: the provider goes away midway
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e2e_provider_outage_midway_keeps_serving_previous_guide() {
    let s = start(None).await;
    let router = s.app.router.clone();
    s.app.scheduler.tick().await;
    let good_requests = s.fake.request_count();
    assert_eq!(good_requests, 29);

    // Move past the 3-hour refresh interval of the near windows. The feed
    // lookback moves with the clock, so the comparison below renders at the
    // same instant `t` both times (and bumps the cache generation to force
    // a re-render from the store).
    let t = now0() + TimeDelta::minutes(3 * 60 + 5); // 23:05 UTC, same UTC day
    s.clock.set(t);
    let before = feed(&router, "hdhomerun-1").await;
    assert_eq!(before.status, StatusCode::OK);
    let slots_before = dump(&s.app.store).await;

    // Every request now gets HTTP 500. Tick until the circuit opens (each
    // failure backs off longer, 30 s doubling; 9 minutes between ticks
    // outlasts every backoff before the fifth failure). The five ticks end
    // at 23:50, so the whole outage stays inside one UTC budget day and the
    // budget can be compared with the server's count directly.
    s.fake.set_default(Behavior::Status(500));
    let mut outage_requests = 0;
    let mut opened = false;
    for i in 0..10 {
        s.clock.advance(Duration::from_secs(9 * 60));
        let seen = s.fake.request_count();
        let r = s.app.scheduler.tick().await;
        let sent = s.fake.request_count() - seen;
        assert!(
            sent <= r.windows_due,
            "tick {i} sent {sent} requests for {} due windows",
            r.windows_due
        );
        outage_requests += sent;
        let st = status(&router).await;
        let p = &st["providers"][0];
        assert!(
            p["budget"]["used"].as_u64().unwrap() <= p["budget"]["limit"].as_u64().unwrap(),
            "budget never exceeds the limit"
        );
        assert_eq!(p["budget"]["used"], s.fake.request_count() as u64);
        assert!(p["consecutive_failures"].as_u64().unwrap() > 0);
        assert!(p["last_error"].is_string());
        assert!(
            st["guide"]["slots_total"].as_u64().unwrap() > 500,
            "guide data remains"
        );
        assert!(st["guide"]["coverage_pct"].as_f64().unwrap() > 0.0);
        if p["circuit"] == "open" {
            opened = true;
            break;
        }
    }
    assert!(opened, "5 consecutive failures open the circuit");
    // Default circuit threshold is 5: never more requests than that.
    assert!(
        outage_requests <= 5,
        "{outage_requests} requests in the outage"
    );

    // Circuit open: further ticks within the cooldown send nothing.
    let seen = s.fake.request_count();
    for _ in 0..3 {
        s.clock.advance(Duration::from_secs(2 * 60));
        s.app.scheduler.tick().await;
    }
    assert_eq!(
        s.fake.request_count(),
        seen,
        "no requests while the circuit is open"
    );
    let m = get_with(&router, "/metrics", Some(ADMIN_TOKEN))
        .await
        .text();
    assert_eq!(
        series(&m, "patina_circuit_open", &["provider=\"gracenote\""]),
        Some(1.0)
    );

    // The stored guide did not change, and the feed is byte-identical.
    assert!(
        dump(&s.app.store).await == slots_before,
        "slots changed in the outage"
    );
    s.clock.set(t);
    s.app.feeds.generation().fetch_add(1, Ordering::SeqCst);
    let during = feed(&router, "hdhomerun-1").await;
    assert_eq!(during.status, StatusCode::OK);
    assert_eq!(
        during.body, before.body,
        "feed is byte-identical during the outage"
    );

    // Recovery: provider healthy again, cooldown (15 min) long past.
    s.fake.set_default(Behavior::ok_shifted());
    // 01:05 UTC is a new budget day, so `used` now counts only the requests
    // sent from here on.
    let sent_before_recovery = s.fake.request_count();
    s.clock.set(t + TimeDelta::hours(2));
    let r = s.app.scheduler.tick().await;
    assert!(r.fetched > 0, "{r:?}");
    let st = status(&router).await;
    let p = &st["providers"][0];
    assert_eq!(p["circuit"], "closed");
    assert_eq!(p["consecutive_failures"], 0);
    // The last success is the recovery tick, after the outage began.
    let last_success: DateTime<Utc> = p["last_success"].as_str().unwrap().parse().unwrap();
    assert!(last_success >= t + TimeDelta::hours(2), "{last_success}");
    assert_eq!(
        p["budget"]["used"],
        (s.fake.request_count() - sent_before_recovery) as u64
    );
    assert!(p["budget"]["used"].as_u64().unwrap() <= 150);
    let after = feed(&router, "hdhomerun-1").await;
    assert_eq!(after.status, StatusCode::OK);
    assert_eq!(summarize(&after.text()).channels.len(), 83);
}

// ---------------------------------------------------------------------------
// Scenario 3: restart
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e2e_restart_keeps_state() {
    let Stack {
        app,
        fake,
        tuner,
        clock,
        _dir: dir,
    } = start(None).await;
    let url = sqlite_url(dir.path());
    app.scheduler.tick().await;
    let requests = fake.request_count();
    assert_eq!(requests, 29);
    clock.set(feed_time());
    let first = feed(&app.router, "hdhomerun-1").await;
    assert_eq!(first.status, StatusCode::OK);

    // "Restart": drop everything the first process held, build again over
    // the same database file.
    drop(app);
    let app = rebuild(dir.path(), &fake, &tuner, &clock, &url).await;

    let r = app.scheduler.tick().await;
    assert_eq!(
        fake.request_count(),
        requests,
        "the second process makes ZERO provider requests (window state is persisted)"
    );
    assert_eq!((r.windows_due, r.fetched), (0, 0), "{r:?}");

    // Budget survived the restart, and the feed is the same.
    let st = status(&app.router).await;
    assert_eq!(st["providers"][0]["budget"]["used"], requests as u64);
    clock.set(feed_time() + TimeDelta::seconds(31));
    let second = feed(&app.router, "hdhomerun-1").await;
    assert_eq!(second.status, StatusCode::OK);
    assert_eq!(second.body, first.body);
}

// ---------------------------------------------------------------------------
// Scenario 4: SIGTERM-style shutdown during a tick
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_graceful_shutdown_finishes_inflight_merge() {
    let s = start(None).await;
    // Every answer is the (unshifted) fixture after 100 ms of real time, so
    // the first tick is certainly still running when shutdown is requested.
    // Unshifted on purpose: every window then reports the same programmes,
    // so the master must come out free of overlaps. The shifted fixture
    // makes consecutive windows disagree (an 8-hour programme sits at a
    // different time in each answer), which no real provider does.
    s.fake.set_default(Behavior::Delay(100));
    let store = s.app.store.clone();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let Stack { app, fake, .. } = s;
    let run = tokio::spawn(app.run(listener, shutdown.clone()));

    // Wait (briefly polling, no fixed sleep) until requests are flowing.
    let started = std::time::Instant::now();
    while fake.request_count() < 2 {
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "tick never started"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // The server answers while the tick runs.
    let live = reqwest::get(format!("http://{addr}/healthz"))
        .await
        .unwrap();
    assert_eq!(live.text().await.unwrap(), "ok");

    shutdown.cancel();
    let exit = tokio::time::timeout(Duration::from_secs(30), run)
        .await
        .expect("run returns after shutdown")
        .expect("run task did not panic");
    assert_eq!(exit, Exit::Clean);

    // It stopped early (not all 29 windows), and whatever was merged is
    // whole: no overlaps, every slot has observations.
    let sent = fake.request_count();
    assert!((2..29).contains(&sent), "{sent} requests");
    let d = dump(&store).await;
    assert!(
        !d.is_empty(),
        "the in-flight window was merged, not dropped"
    );
    assert_internally_consistent(&d);
    // Nothing more arrives after run() returned.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(fake.request_count(), sent);
}
