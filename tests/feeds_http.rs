//! XMLTV feeds over HTTP: routing, token rules, content selection, caching.
//! Small tests build their data by hand; the last ones use the real captured
//! HDHomeRun lineup and Gracenote grid, ingested by the real scheduler.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use chrono_tz::America::Chicago;
use http_body_util::BodyExt;
use patina::channels::hdhomerun::parse_lineup;
use patina::channels::matching::Overrides;
use patina::channels::{ChannelSource, StaticSource, TunerChannel};
use patina::clock::FakeClock;
use patina::config::{LineupCfg, SourceCfg, SourceKind, Weights};
use patina::feeds::{FeedSourceSpec, Feeds, FeedsConfig, source_names};
use patina::fetch::{FetchCfg, FetchLayer, SequenceJitter};
use patina::http::{AppState, router};
use patina::merge::{MergePlan, NewSlot, SlotFields};
use patina::model::{CanonicalChannel, ChannelId, ProviderId, ScoreBreakdown, canonical_id};
use patina::provider::ProviderChannel;
use patina::scheduler::{ProviderRef, Scheduler, SchedulerDeps};
use patina::store::{Store, connect};
use support::fake_provider::{Behavior, FakeProvider};
use tower::ServiceExt;

const TUNER_LINEUP: &[u8] = include_bytes!("fixtures/hdhomerun/lineup.json");

fn now0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 31, 20, 0, 0).unwrap()
}

fn gn() -> ProviderId {
    ProviderId("gracenote".into())
}

fn tc(number: &str, name: &str) -> TunerChannel {
    TunerChannel {
        number: number.into(),
        name: name.into(),
    }
}

fn pc(id: &str, number: &str, call: &str) -> ProviderChannel {
    ProviderChannel {
        provider_channel_id: id.into(),
        number: number.into(),
        call_sign: call.into(),
    }
}

/// Everything a small test needs.
struct Env {
    store: Arc<dyn Store>,
    clock: Arc<FakeClock>,
    generation: Arc<AtomicU64>,
    feeds: Arc<Feeds>,
}

impl Env {
    fn app(&self) -> Router {
        router(AppState::new(self.feeds.clone()))
    }
}

fn spec(name: &str, tuner: Vec<TunerChannel>, include_unmatched: bool) -> FeedSourceSpec {
    FeedSourceSpec {
        name: name.into(),
        include_unmatched,
        overrides: Overrides::default(),
        lineups: vec![(gn(), "ota".into())],
        source: Arc::new(StaticSource::new(tuner)),
    }
}

/// A store with the given provider channels (lineup "ota") and canonical
/// channels, feeds over the given sources, channel lists already loaded.
async fn env_with(
    provider: &[ProviderChannel],
    sources: Vec<FeedSourceSpec>,
    token: Option<&str>,
) -> Env {
    let store = connect("sqlite::memory:").await.unwrap();
    store
        .provider_channels_upsert(&gn(), "ota", provider, now0())
        .await
        .unwrap();
    let canon: Vec<CanonicalChannel> = provider
        .iter()
        .map(|p| CanonicalChannel {
            id: canonical_id(&p.call_sign),
            number: p.number.clone(),
            name: p.call_sign.clone(),
            call_sign: p.call_sign.clone(),
        })
        .collect();
    store.upsert_channels(&canon).await.unwrap();
    let clock = Arc::new(FakeClock::new(now0()));
    let generation = Arc::new(AtomicU64::new(0));
    let feeds = Arc::new(Feeds::new(FeedsConfig {
        store: store.clone(),
        clock: clock.clone(),
        tz: Chicago,
        horizon_days: 7,
        token: token.map(str::to_string),
        sources,
        generation: generation.clone(),
    }));
    feeds.refresh_channels().await;
    Env {
        store,
        clock,
        generation,
        feeds,
    }
}

/// Insert one slot straight into the master guide.
async fn add_slot(store: &Arc<dyn Store>, channel: &str, start: DateTime<Utc>, title: &str) {
    let mut plan = MergePlan::empty(now0());
    plan.inserts.push(NewSlot {
        channel: ChannelId(channel.into()),
        fields: SlotFields {
            start,
            end: start + TimeDelta::minutes(30),
            title: title.into(),
            subtitle: None,
            description: None,
            image_url: None,
            season: None,
            episode: None,
            rating: None,
            genres: vec![],
            winner: gn(),
            score: 1.0,
            breakdown: ScoreBreakdown::default(),
        },
    });
    store.apply_merge(plan).await.unwrap();
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
}

async fn get_with(app: &Router, uri: &str, headers: &[(&str, &str)]) -> Resp {
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

async fn get(app: &Router, uri: &str) -> Resp {
    get_with(app, uri, &[]).await
}

fn doc(xml: &str) -> roxmltree::Document<'_> {
    roxmltree::Document::parse(xml).expect("feed must be well-formed XML")
}

fn channel_ids(xml: &str) -> Vec<String> {
    doc(xml)
        .descendants()
        .filter(|n| n.has_tag_name("channel") && n.parent().unwrap().has_tag_name("tv"))
        .map(|n| n.attribute("id").unwrap().to_string())
        .collect()
}

fn programme_titles(xml: &str) -> Vec<String> {
    doc(xml)
        .descendants()
        .filter(|n| n.has_tag_name("programme"))
        .map(|p| {
            p.children()
                .find(|n| n.has_tag_name("title"))
                .unwrap()
                .text()
                .unwrap()
                .to_string()
        })
        .collect()
}

fn small_provider() -> Vec<ProviderChannel> {
    vec![
        pc("p1", "4.1", "WCCO-DT"),
        pc("p2", "11.1", "KARE-DT"),
        pc("p3", "5.1", "KSTP-DT"),
    ]
}

// ------------------------------------------------------------------ content

#[tokio::test]
async fn feed_returns_matched_channels_even_without_programmes() {
    let env = env_with(
        &small_provider(),
        vec![spec(
            "static-1",
            vec![tc("4.1", "WCCO-DT"), tc("11.1", "KARE 11")],
            false,
        )],
        None,
    )
    .await;
    let r = get(&env.app(), "/xmltv/static-1.xml").await;
    assert_eq!(r.status, StatusCode::OK);
    let xml = r.text();
    // No slots at all, yet both channels are present with the TUNER's number
    // and name as display name, in tuner order.
    assert_eq!(channel_ids(&xml), ["WCCODT.us", "KAREDT.us"]);
    let names: Vec<_> = doc(&xml)
        .descendants()
        .filter(|n| n.has_tag_name("display-name"))
        .map(|n| n.text().unwrap().to_string())
        .collect();
    assert_eq!(names, ["4.1 WCCO-DT", "4.1", "11.1 KARE 11", "11.1"]);
    assert!(programme_titles(&xml).is_empty());
}

#[tokio::test]
async fn unmatched_tuner_entries_omitted_by_default() {
    // 99.1 has no provider channel; 5.1 matches. 8.8 is ambiguous (two
    // provider channels share its number).
    let mut provider = small_provider();
    provider.push(pc("p8a", "8.8", "AAAA-DT"));
    provider.push(pc("p8b", "8.8", "BBBB-DT"));
    let env = env_with(
        &provider,
        vec![spec(
            "static-1",
            vec![
                tc("4.1", "WCCO-DT"),
                tc("99.1", "Nowhere"),
                tc("8.8", "Mystery"),
            ],
            false,
        )],
        None,
    )
    .await;
    let xml = get(&env.app(), "/xmltv/static-1.xml").await.text();
    assert_eq!(channel_ids(&xml), ["WCCODT.us"]);
}

#[tokio::test]
async fn include_unmatched_adds_empty_channels() {
    let mut provider = small_provider();
    provider.push(pc("p8a", "8.8", "AAAA-DT"));
    provider.push(pc("p8b", "8.8", "BBBB-DT"));
    let env = env_with(
        &provider,
        vec![spec(
            "static-1",
            vec![
                tc("99.1", "Nowhere"),
                tc("4.1", "WCCO-DT"),
                tc("8.8", "Mystery"),
            ],
            true,
        )],
        None,
    )
    .await;
    add_slot(&env.store, "WCCODT.us", now0(), "News").await;
    let xml = get(&env.app(), "/xmltv/static-1.xml").await.text();
    // Tuner order is kept; ambiguous counts as unmatched.
    assert_eq!(
        channel_ids(&xml),
        ["UNMATCHED-99.1.us", "WCCODT.us", "UNMATCHED-8.8.us"]
    );
    let names: Vec<_> = doc(&xml)
        .descendants()
        .filter(|n| n.has_tag_name("display-name"))
        .map(|n| n.text().unwrap().to_string())
        .collect();
    assert_eq!(
        names,
        [
            "99.1 Nowhere",
            "99.1",
            "4.1 WCCO-DT",
            "4.1",
            "8.8 Mystery",
            "8.8"
        ]
    );
    // Only the matched channel has a programme.
    assert_eq!(programme_titles(&xml), ["News"]);
    let p = doc(&xml)
        .descendants()
        .find(|n| n.has_tag_name("programme"))
        .unwrap()
        .attribute("channel")
        .unwrap()
        .to_string();
    assert_eq!(p, "WCCODT.us");
}

#[tokio::test]
async fn combined_feed_dedupes_station_carried_by_two_sources() {
    let env = env_with(
        &small_provider(),
        vec![
            spec(
                "hdhomerun-1",
                vec![tc("4.1", "WCCO-DT"), tc("11.1", "KARE 11")],
                false,
            ),
            // Same stations under other numbers/names, plus one of its own.
            spec(
                "static-1",
                vec![tc("4.1", "Channel 4"), tc("5.1", "KSTP")],
                false,
            ),
        ],
        None,
    )
    .await;
    add_slot(&env.store, "WCCODT.us", now0(), "Once").await;
    let xml = get(&env.app(), "/xmltv/all.xml").await.text();
    assert_eq!(channel_ids(&xml), ["WCCODT.us", "KAREDT.us", "KSTPDT.us"]);
    // First source wins the display name.
    let first = doc(&xml)
        .descendants()
        .find(|n| n.has_tag_name("display-name"))
        .unwrap()
        .text()
        .unwrap()
        .to_string();
    assert_eq!(first, "4.1 WCCO-DT");
    // The programme of the shared station appears exactly once.
    assert_eq!(programme_titles(&xml), ["Once"]);
}

#[tokio::test]
async fn slots_window_starts_three_hours_before_now_and_ends_at_horizon() {
    let env = env_with(
        &small_provider(),
        vec![spec("static-1", vec![tc("4.1", "WCCO-DT")], false)],
        None,
    )
    .await;
    let now = now0();
    // A slot overlapping the window start is kept, one ending before it is
    // not; one starting just before the horizon is kept, one at it is not.
    add_slot(
        &env.store,
        "WCCODT.us",
        now - TimeDelta::hours(3) - TimeDelta::minutes(31),
        "ended-before",
    )
    .await;
    add_slot(
        &env.store,
        "WCCODT.us",
        now - TimeDelta::hours(3) - TimeDelta::minutes(20),
        "overlaps-start",
    )
    .await;
    add_slot(&env.store, "WCCODT.us", now - TimeDelta::hours(1), "airing").await;
    add_slot(
        &env.store,
        "WCCODT.us",
        now + TimeDelta::days(7) - TimeDelta::minutes(1),
        "last",
    )
    .await;
    add_slot(
        &env.store,
        "WCCODT.us",
        now + TimeDelta::days(7),
        "past-horizon",
    )
    .await;
    let xml = get(&env.app(), "/xmltv/static-1.xml").await.text();
    assert_eq!(programme_titles(&xml), ["overlaps-start", "airing", "last"]);
}

// -------------------------------------------------------------------- token

async fn token_env() -> Env {
    env_with(
        &small_provider(),
        vec![spec("static-1", vec![tc("4.1", "WCCO-DT")], false)],
        Some("s3cret"),
    )
    .await
}

#[tokio::test]
async fn token_required_when_configured_and_wrong_token_is_401() {
    let env = token_env().await;
    let app = env.app();
    for uri in [
        "/xmltv/static-1.xml",
        "/xmltv/all.xml",
        "/xmltv/static-1.xml?token=wrong",
        "/xmltv/static-1.xml?token=",
        "/xmltv/static-1.xml?other=s3cret",
        "/t/wrong/xmltv/static-1.xml",
        "/t/s3cre/xmltv/static-1.xml",
        "/t/s3cret2/xmltv/static-1.xml",
    ] {
        let r = get(&app, uri).await;
        assert_eq!(r.status, StatusCode::UNAUTHORIZED, "{uri}");
        assert!(r.body.is_empty(), "{uri}");
    }
    // The right token works, and healthz never needs one.
    assert_eq!(
        get(&app, "/xmltv/static-1.xml?token=s3cret").await.status,
        StatusCode::OK
    );
    assert_eq!(get(&app, "/healthz").await.status, StatusCode::OK);
}

#[tokio::test]
async fn no_token_configured_means_open() {
    let env = env_with(
        &small_provider(),
        vec![spec("static-1", vec![tc("4.1", "WCCO-DT")], false)],
        None,
    )
    .await;
    let app = env.app();
    assert_eq!(
        get(&app, "/xmltv/static-1.xml").await.status,
        StatusCode::OK
    );
    assert_eq!(get(&app, "/xmltv/all.xml").await.status, StatusCode::OK);
    // The extension is optional.
    assert_eq!(get(&app, "/xmltv/all").await.status, StatusCode::OK);
    // A token that nobody asked for is simply ignored.
    assert_eq!(
        get(&app, "/xmltv/all.xml?token=whatever").await.status,
        StatusCode::OK
    );
    assert_eq!(
        get(&app, "/t/whatever/xmltv/all.xml").await.status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn unknown_source_is_404_when_open() {
    let env = env_with(
        &small_provider(),
        vec![spec("static-1", vec![tc("4.1", "WCCO-DT")], false)],
        None,
    )
    .await;
    let app = env.app();
    for uri in [
        "/xmltv/nope.xml",
        "/xmltv/static-1.xml.xml",
        "/xmltv/a/b.xml",
        "/xmltv/.xml",
    ] {
        assert_eq!(get(&app, uri).await.status, StatusCode::NOT_FOUND, "{uri}");
    }
}

#[tokio::test]
async fn unknown_source_with_valid_token_is_404() {
    let env = token_env().await;
    let app = env.app();
    assert_eq!(
        get(&app, "/xmltv/nope.xml?token=s3cret").await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&app, "/t/s3cret/xmltv/nope.xml").await.status,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn token_check_does_not_reveal_unknown_source_existence() {
    let env = token_env().await;
    let app = env.app();
    // Without (or with a wrong) token, a real source, the combined feed and
    // an invented name all answer byte-for-byte the same thing.
    let mut seen = Vec::new();
    for uri in [
        "/xmltv/static-1.xml",
        "/xmltv/all.xml",
        "/xmltv/does-not-exist.xml",
        "/xmltv/static-1.xml?token=bad",
        "/xmltv/does-not-exist.xml?token=bad",
        "/t/bad/xmltv/static-1.xml",
        "/t/bad/xmltv/does-not-exist.xml",
        "/t/bad/elsewhere",
        "/t/bad",
        "/xmltv/a/b/c",
    ] {
        let r = get(&app, uri).await;
        let mut headers: Vec<_> = r
            .headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
            .collect();
        headers.sort();
        seen.push((uri, r.status, headers, r.body));
    }
    let (_, status0, headers0, body0) = &seen[0];
    assert_eq!(*status0, StatusCode::UNAUTHORIZED);
    for (uri, status, headers, body) in &seen {
        assert_eq!(status, status0, "{uri}");
        assert_eq!(headers, headers0, "{uri}");
        assert_eq!(body, body0, "{uri}");
    }
}

#[tokio::test]
async fn token_in_path_and_query_both_work() {
    let env = token_env().await;
    let app = env.app();
    let by_query = get(&app, "/xmltv/static-1.xml?token=s3cret").await;
    let by_path = get(&app, "/t/s3cret/xmltv/static-1.xml").await;
    for r in [&by_query, &by_path] {
        assert_eq!(r.status, StatusCode::OK);
    }
    assert_eq!(by_query.body, by_path.body);
    // A token containing characters that need URL escaping.
    let env = env_with(
        &small_provider(),
        vec![spec("static-1", vec![tc("4.1", "WCCO-DT")], false)],
        Some("a b&c"),
    )
    .await;
    let app = env.app();
    assert_eq!(
        get(&app, "/xmltv/all.xml?token=a%20b%26c").await.status,
        StatusCode::OK
    );
    assert_eq!(
        get(&app, "/t/a%20b%26c/xmltv/all.xml").await.status,
        StatusCode::OK
    );
}

/// The bare 401 every unauthorized feed request gets, for comparison.
async fn plain_401(app: &Router) -> Resp {
    let r = get(app, "/xmltv/static-1.xml").await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    r
}

fn same_response(a: &Resp, b: &Resp) {
    let sorted = |r: &Resp| {
        let mut h: Vec<_> = r
            .headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
            .collect();
        h.sort();
        h
    };
    assert_eq!(a.status, b.status);
    assert_eq!(sorted(a), sorted(b));
    assert_eq!(a.body, b.body);
}

#[tokio::test]
async fn two_query_tokens_are_rejected_even_if_one_is_valid() {
    // One request may carry only one guess: otherwise a single request could
    // try hundreds of tokens.
    let env = token_env().await;
    let app = env.app();
    let base = plain_401(&app).await;
    for uri in [
        "/xmltv/static-1.xml?token=wrong&token=s3cret",
        "/xmltv/static-1.xml?token=s3cret&token=wrong",
        "/xmltv/static-1.xml?token=s3cret&token=s3cret",
        "/xmltv/all.xml?token=s3cret&token=",
    ] {
        let r = get(&app, uri).await;
        same_response(&r, &base);
    }
}

#[tokio::test]
async fn path_token_plus_query_token_is_rejected() {
    let env = token_env().await;
    let app = env.app();
    let base = plain_401(&app).await;
    for uri in [
        "/t/s3cret/xmltv/static-1.xml?token=s3cret",
        "/t/s3cret/xmltv/static-1.xml?token=wrong",
        "/t/wrong/xmltv/static-1.xml?token=s3cret",
    ] {
        let r = get(&app, uri).await;
        same_response(&r, &base);
    }
}

#[tokio::test]
async fn cache_control_is_private_when_a_token_is_configured() {
    let env = token_env().await;
    let app = env.app();
    let r = get(&app, "/xmltv/static-1.xml?token=s3cret").await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.headers[header::CACHE_CONTROL], "private, max-age=60");
    let etag = r.headers[header::ETAG].to_str().unwrap().to_string();
    let n = get_with(
        &app,
        "/t/s3cret/xmltv/static-1.xml",
        &[("if-none-match", &etag)],
    )
    .await;
    assert_eq!(n.status, StatusCode::NOT_MODIFIED);
    assert_eq!(n.headers[header::CACHE_CONTROL], "private, max-age=60");
}

#[tokio::test]
async fn cache_control_unchanged_when_no_token() {
    let env = env_with(
        &small_provider(),
        vec![spec("static-1", vec![tc("4.1", "WCCO-DT")], false)],
        None,
    )
    .await;
    let app = env.app();
    let r = get(&app, "/xmltv/static-1.xml").await;
    assert_eq!(r.headers[header::CACHE_CONTROL], "max-age=60");
    // With no token configured, extra token parameters are still ignored.
    let r = get(&app, "/t/x/xmltv/static-1.xml?token=y&token=z").await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.headers[header::CACHE_CONTROL], "max-age=60");
}

// ------------------------------------------------------------------ caching

#[tokio::test]
async fn headers_and_etag_and_if_none_match_return_304() {
    let env = env_with(
        &small_provider(),
        vec![spec("static-1", vec![tc("4.1", "WCCO-DT")], false)],
        None,
    )
    .await;
    let app = env.app();
    let r = get(&app, "/xmltv/static-1.xml").await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(
        r.headers[header::CONTENT_TYPE],
        "application/xml; charset=utf-8"
    );
    assert_eq!(r.headers[header::CACHE_CONTROL], "max-age=60");
    let etag = r.headers[header::ETAG].to_str().unwrap().to_string();
    assert!(etag.starts_with('"') && etag.ends_with('"'), "{etag}");

    // Same tag => 304 with no body, and the tag and cache headers repeated.
    for inm in [
        etag.clone(),
        format!("W/{etag}"),
        format!("\"x\", {etag}"),
        "*".into(),
    ] {
        let n = get_with(&app, "/xmltv/static-1.xml", &[("if-none-match", &inm)]).await;
        assert_eq!(n.status, StatusCode::NOT_MODIFIED, "{inm}");
        assert!(n.body.is_empty());
        assert_eq!(n.headers[header::ETAG].to_str().unwrap(), etag);
    }
    // A different tag gets the full body.
    let n = get_with(
        &app,
        "/xmltv/static-1.xml",
        &[("if-none-match", "\"nope\"")],
    )
    .await;
    assert_eq!(n.status, StatusCode::OK);
    assert_eq!(n.body, r.body);

    // New content => new tag.
    add_slot(&env.store, "WCCODT.us", now0(), "New").await;
    env.generation.fetch_add(1, Ordering::SeqCst);
    let n = get_with(&app, "/xmltv/static-1.xml", &[("if-none-match", &etag)]).await;
    assert_eq!(n.status, StatusCode::OK);
    assert_ne!(n.headers[header::ETAG].to_str().unwrap(), etag);
}

#[tokio::test]
async fn not_modified_still_requires_the_token() {
    let env = token_env().await;
    let app = env.app();
    let ok = get(&app, "/xmltv/static-1.xml?token=s3cret").await;
    let etag = ok.headers[header::ETAG].to_str().unwrap().to_string();
    let r = get_with(&app, "/xmltv/static-1.xml", &[("if-none-match", &etag)]).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn rendered_feed_is_cached_until_ttl_or_generation_change() {
    let env = env_with(
        &small_provider(),
        vec![spec("static-1", vec![tc("4.1", "WCCO-DT")], false)],
        None,
    )
    .await;
    let app = env.app();
    let uri = "/xmltv/static-1.xml";
    assert!(programme_titles(&get(&app, uri).await.text()).is_empty());

    // Data changes behind the cache's back: still the cached answer...
    add_slot(&env.store, "WCCODT.us", now0(), "Fresh").await;
    assert!(programme_titles(&get(&app, uri).await.text()).is_empty());
    // ...until the TTL (30 s) has passed...
    env.clock.advance(Duration::from_secs(29));
    assert!(programme_titles(&get(&app, uri).await.text()).is_empty());
    env.clock.advance(Duration::from_secs(2));
    assert_eq!(programme_titles(&get(&app, uri).await.text()), ["Fresh"]);

    // ...or the generation moves, which shows the change at once.
    add_slot(
        &env.store,
        "WCCODT.us",
        now0() + TimeDelta::hours(1),
        "Later",
    )
    .await;
    assert_eq!(programme_titles(&get(&app, uri).await.text()), ["Fresh"]);
    env.generation.fetch_add(1, Ordering::SeqCst);
    assert_eq!(
        programme_titles(&get(&app, uri).await.text()),
        ["Fresh", "Later"]
    );
}

fn lineup_cfg() -> LineupCfg {
    LineupCfg {
        name: "ota".into(),
        lineup_id: "USA-OTA55401".into(),
        postal_code: "55401".into(),
        country: "USA".into(),
    }
}

/// A scheduler over `store` talking to a fake Gracenote that answers every
/// window with the (shifted) real fixture. Returns the scheduler's own clock
/// too, which the fetch layer advances while it works.
async fn real_scheduler(
    store: Arc<dyn Store>,
    generation: Option<Arc<AtomicU64>>,
) -> (FakeProvider, Scheduler) {
    let fake = FakeProvider::start().await;
    fake.set_default(Behavior::ok_shifted());
    let clock = Arc::new(FakeClock::new(now0()));
    let fetch = Arc::new(FetchLayer::new(
        store.clone(),
        vec![Arc::new(fake.gracenote())],
        FetchCfg {
            default_daily_budget: 1000,
            min_interval: Duration::from_secs(1),
            jitter_fraction: 0.0,
            jitter: Arc::new(SequenceJitter::new(vec![0.0])),
            ..FetchCfg::default()
        },
        clock.clone(),
    ));
    let sched = Scheduler::new(
        SchedulerDeps {
            store,
            fetch,
            providers: vec![ProviderRef {
                id: gn(),
                lineups: vec![lineup_cfg()],
            }],
            horizon_days: 7,
            weights: Weights::default(),
            tick_interval: Duration::from_secs(60),
            feed_generation: generation,
        },
        clock,
    );
    (fake, sched)
}

#[tokio::test]
async fn feed_cache_is_invalidated_when_scheduler_merges() {
    let store = connect("sqlite::memory:").await.unwrap();
    // The feeds use a clock that never moves, so the 30 s TTL cannot be what
    // refreshes the feed: only the generation bump can.
    let feed_clock = Arc::new(FakeClock::new(now0()));
    let generation = Arc::new(AtomicU64::new(0));
    let feeds = Arc::new(Feeds::new(FeedsConfig {
        store: store.clone(),
        clock: feed_clock,
        tz: Chicago,
        horizon_days: 7,
        token: None,
        sources: vec![FeedSourceSpec {
            name: "static-1".into(),
            include_unmatched: false,
            overrides: Overrides::default(),
            lineups: vec![(gn(), "ota".into())],
            source: Arc::new(StaticSource::new(vec![tc("4.1", "WCCO-DT")])),
        }],
        generation: generation.clone(),
    }));
    feeds.refresh_channels().await;
    let app = router(AppState::new(feeds));

    // Before any merge the provider's channels are unknown, so the feed is
    // not ready (and 503 is never cached).
    let before = get(&app, "/xmltv/static-1.xml").await;
    assert_eq!(before.status, StatusCode::SERVICE_UNAVAILABLE);
    let g0 = generation.load(Ordering::SeqCst);

    let (_fake, sched) = real_scheduler(store.clone(), Some(generation.clone())).await;
    let report = sched.tick().await;
    assert!(report.slots_inserted > 0, "{report:?}");
    assert!(generation.load(Ordering::SeqCst) > g0, "merge must bump");

    // Same (never advancing) feed clock, yet the new data is visible.
    let after = get(&app, "/xmltv/static-1.xml").await.text();
    assert_eq!(channel_ids(&after), ["WCCODT.us"]);
    assert!(!programme_titles(&after).is_empty());

    // A tick with nothing left to merge leaves the generation alone.
    let g = generation.load(Ordering::SeqCst);
    let again = sched.tick().await;
    assert_eq!(again.slots_inserted, 0);
    assert_eq!(generation.load(Ordering::SeqCst), g);
}

// ---------------------------------------------------------------- refresher

struct FailingSource;

#[async_trait]
impl ChannelSource for FailingSource {
    async fn load(&self) -> Result<Vec<TunerChannel>> {
        Err(anyhow!("tuner unreachable"))
    }
}

#[tokio::test]
async fn feed_503_when_source_never_loaded() {
    let mut s = spec("hdhomerun-1", vec![], false);
    s.source = Arc::new(FailingSource);
    let env = env_with(&small_provider(), vec![s], None).await;
    let app = env.app();
    let r = get(&app, "/xmltv/hdhomerun-1.xml").await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(r.headers[header::RETRY_AFTER], "60");
    let text = r.text();
    assert!(!text.is_empty() && !text.contains("<tv"), "{text}");
    // The combined feed has nothing to offer either, and is not an empty guide.
    let r = get(&app, "/xmltv/all.xml").await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(r.headers[header::RETRY_AFTER], "60");
    let st = env.feeds.source_status();
    assert_eq!(st[0].0, "hdhomerun-1");
    assert!(st[0].1.as_ref().unwrap().loaded_at.is_none());
}

#[tokio::test]
async fn feed_503_until_provider_channels_are_known() {
    // The tuner list is loaded but the scheduler has not stored the guide
    // provider's channels yet: nothing could match, and an empty guide would
    // be cached by Jellyfin as "no channels".
    let env = env_with(
        &[],
        vec![spec("static-1", vec![tc("4.1", "WCCO-DT")], false)],
        None,
    )
    .await;
    let r = get(&env.app(), "/xmltv/static-1.xml").await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(r.headers[header::RETRY_AFTER], "60");
}

#[tokio::test]
async fn refresher_keeps_last_known_list_when_tuner_goes_down() {
    // A source that works once, then fails.
    struct Flaky(std::sync::atomic::AtomicBool);
    #[async_trait]
    impl ChannelSource for Flaky {
        async fn load(&self) -> Result<Vec<TunerChannel>> {
            if self.0.swap(false, Ordering::SeqCst) {
                Ok(vec![TunerChannel {
                    number: "4.1".into(),
                    name: "WCCO-DT".into(),
                }])
            } else {
                Err(anyhow!("down"))
            }
        }
    }
    let mut s = spec("hdhomerun-1", vec![], false);
    s.source = Arc::new(Flaky(std::sync::atomic::AtomicBool::new(true)));
    let env = env_with(&small_provider(), vec![s], None).await;
    let app = env.app();
    assert_eq!(
        channel_ids(&get(&app, "/xmltv/hdhomerun-1.xml").await.text()),
        ["WCCODT.us"]
    );
    // The second refresh fails; the feed keeps serving the last-known list
    // and the status says it is stale.
    env.feeds.refresh_channels().await;
    env.clock.advance(Duration::from_secs(60));
    let r = get(&app, "/xmltv/hdhomerun-1.xml").await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(channel_ids(&r.text()), ["WCCODT.us"]);
    let st = env.feeds.source_status();
    let status = st[0].1.as_ref().unwrap();
    assert!(status.stale);
    assert!(status.error.as_deref().unwrap().contains("down"));
}

/// A source that answers with whatever list it holds (possibly empty).
struct Settable(std::sync::Mutex<Vec<TunerChannel>>);

#[async_trait]
impl ChannelSource for Settable {
    async fn load(&self) -> Result<Vec<TunerChannel>> {
        Ok(self.0.lock().unwrap().clone())
    }
}

#[tokio::test]
async fn empty_tuner_answer_keeps_serving_the_old_list() {
    let source = Arc::new(Settable(std::sync::Mutex::new(vec![tc("4.1", "WCCO-DT")])));
    let mut s = spec("hdhomerun-1", vec![], false);
    s.source = source.clone();
    let env = env_with(&small_provider(), vec![s], None).await;
    let app = env.app();
    assert_eq!(
        channel_ids(&get(&app, "/xmltv/hdhomerun-1.xml").await.text()),
        ["WCCODT.us"]
    );

    // Mid channel-scan the tuner answers `[]`.
    source.0.lock().unwrap().clear();
    env.feeds.refresh_channels().await;
    env.clock.advance(Duration::from_secs(60));
    let r = get(&app, "/xmltv/hdhomerun-1.xml").await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(channel_ids(&r.text()), ["WCCODT.us"]);
    let st = env.feeds.source_status();
    let status = st[0].1.as_ref().unwrap();
    assert!(status.stale);
    assert_eq!(status.channels_total, 1);
    assert!(status.error.as_deref().unwrap().contains("no channels"));
    // The saved fallback is still the good list.
    let (saved, _) = env
        .store
        .load_tuner_channels("hdhomerun-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.len(), 1);
}

#[tokio::test]
async fn first_ever_empty_tuner_answer_is_503() {
    let mut s = spec("hdhomerun-1", vec![], false);
    s.source = Arc::new(Settable(std::sync::Mutex::new(Vec::new())));
    let env = env_with(&small_provider(), vec![s], None).await;
    let app = env.app();
    let r = get(&app, "/xmltv/hdhomerun-1.xml").await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE);
    let r = get(&app, "/xmltv/all.xml").await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE);
}

#[test]
fn source_names_derive_from_kind_and_index() {
    let cfg = |kind: SourceKind| SourceCfg {
        kind,
        lineups: vec![],
        overrides_path: None,
        include_unmatched: false,
    };
    let hd = || SourceKind::HdHomeRun {
        url: "http://x".into(),
    };
    let st = || SourceKind::Static { channels: vec![] };
    let names = source_names(&[cfg(hd()), cfg(st()), cfg(hd()), cfg(st())]);
    assert_eq!(
        names,
        ["hdhomerun-1", "static-1", "hdhomerun-2", "static-2"]
    );
}

// -------------------------------------------------------- real fixture data

#[tokio::test]
async fn real_fixture_feed_has_83_matched_channels_and_programmes() {
    let store = connect("sqlite::memory:").await.unwrap();
    // Real scheduler, real Gracenote grid (via the fake server), 7 days.
    let (_fake, sched) = real_scheduler(store.clone(), None).await;
    let report = sched.tick().await;
    assert!(report.slots_inserted > 1000, "{report:?}");

    // The realistic overrides from the channels tests: fix Gracenote's 9.x
    // numbering, ignore the junk 5000 entry, pick one of the two 5.6s.
    let provider = store.provider_channels_list(&gn(), "ota").await.unwrap();
    let pid = |call: &str| {
        provider
            .iter()
            .find(|c| c.call_sign == call)
            .unwrap()
            .provider_channel_id
            .clone()
    };
    let overrides = Overrides::from_toml(&format!(
        "[[override]]\nnumber = \"9.1\"\nprovider_channel_id = \"{}\"\n\
         [[override]]\nnumber = \"9.2\"\nprovider_channel_id = \"{}\"\n\
         [[override]]\nnumber = \"9.9\"\nprovider_channel_id = \"{}\"\n\
         [[override]]\nnumber = \"5000\"\nignore = true\n\
         [[override]]\nnumber = \"5.6\"\nprovider_channel_id = \"{}\"\n",
        pid("KMSPDT"),
        pid("WFTCDT"),
        pid("WFTCDT2"),
        pid("KSTCDT6"),
    ))
    .unwrap();
    let tuner = parse_lineup(TUNER_LINEUP).unwrap();
    let generation = Arc::new(AtomicU64::new(0));
    let mk = |include_unmatched: bool| {
        Arc::new(Feeds::new(FeedsConfig {
            store: store.clone(),
            clock: Arc::new(FakeClock::new(now0())),
            tz: Chicago,
            horizon_days: 7,
            token: None,
            sources: vec![FeedSourceSpec {
                name: "hdhomerun-1".into(),
                include_unmatched,
                overrides: overrides.clone(),
                lineups: vec![(gn(), "ota".into())],
                source: Arc::new(StaticSource::new(tuner.clone())),
            }],
            generation: generation.clone(),
        }))
    };

    let feeds = mk(false);
    feeds.refresh_channels().await;
    let app = router(AppState::new(feeds));
    let r = get(&app, "/xmltv/hdhomerun-1.xml").await;
    assert_eq!(r.status, StatusCode::OK);
    let xml = r.text();
    let d = doc(&xml);
    let ids = channel_ids(&xml);
    assert_eq!(ids.len(), 83, "83 matched channels (see tests/channels.rs)");
    let mut unique = ids.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 83, "channel ids must be unique");
    assert!(ids.contains(&"WCCODT.us".to_string()));
    assert!(
        d.descendants()
            .any(|n| n.has_tag_name("display-name") && n.text() == Some("4.1 WCCO-DT"))
    );
    // Tuner order: 4.1 comes before 11.1.
    let pos = |s: &str| ids.iter().position(|i| i == s).unwrap();
    assert!(pos("WCCODT.us") < pos("KAREDT.us"));
    // Real programmes, in a CDT or CST offset, inside the window.
    let progs: Vec<_> = d
        .descendants()
        .filter(|n| n.has_tag_name("programme"))
        .collect();
    assert!(progs.len() > 1000, "{}", progs.len());
    assert!(progs.iter().all(|p| {
        let s = p.attribute("start").unwrap();
        s.ends_with(" -0500") || s.ends_with(" -0600")
    }));
    // The DST change on 2026-11-01 shows up within this very feed.
    assert!(
        progs
            .iter()
            .any(|p| p.attribute("start").unwrap().ends_with(" -0500"))
    );
    assert!(
        progs
            .iter()
            .any(|p| p.attribute("start").unwrap().ends_with(" -0600"))
    );

    // With include_unmatched the other 23 tuner entries appear as empty
    // channels (83 matched + 23 unmatched + 1 ignored = 107).
    let feeds = mk(true);
    feeds.refresh_channels().await;
    let app = router(AppState::new(feeds));
    let xml = get(&app, "/xmltv/hdhomerun-1.xml").await.text();
    let ids = channel_ids(&xml);
    assert_eq!(ids.len(), 83 + 23);
    assert_eq!(
        ids.iter().filter(|i| i.starts_with("UNMATCHED-")).count(),
        23
    );
}
