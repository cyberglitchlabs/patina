//! Offline tests for the Gracenote provider: parsing a real captured
//! response, and mapping HTTP statuses to `FetchError`. No test here
//! contacts the real Gracenote service.

use std::time::Duration;

use axum::{Router, http::StatusCode, routing::get};
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use patina::config::LineupCfg;
use patina::model::canonical_id;
use patina::provider::gracenote::{GracenoteProvider, map_status};
use patina::provider::{FetchError, Provider, RawResponse};
use url::Url;

const FIXTURE: &[u8] = include_bytes!("fixtures/gracenote/grid-USA-OTA55401-6h.json");

fn lineup() -> LineupCfg {
    LineupCfg {
        name: "home".into(),
        lineup_id: "USA-OTA55401".into(),
        postal_code: "55401".into(),
        country: "USA".into(),
    }
}

fn provider() -> GracenoteProvider {
    GracenoteProvider::new(
        patina::provider::gracenote::http_client(std::time::Duration::from_secs(10)).unwrap(),
        Url::parse("https://tvlistings.gracenote.com").unwrap(),
    )
}

fn fetched_at() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 31, 20, 0, 0).unwrap()
}

fn raw(json: &str) -> RawResponse {
    RawResponse {
        status: 200,
        body: Bytes::from(json.to_owned()),
    }
}

/// One channel with the given events, as Gracenote JSON.
fn one_channel(events: &str) -> String {
    format!(
        r#"{{"channels":[{{"callSign":"WCCO-DT","channelNo":"4.1","channelId":"1","events":[{events}]}}]}}"#
    )
}

fn event(start: &str, end: &str, title: &str, desc: &str) -> String {
    format!(
        r#"{{"startTime":"{start}","endTime":"{end}","thumbnail":"p1_aa","program":{{"title":"{title}","shortDesc":"{desc}"}}}}"#
    )
}

#[test]
fn canonical_id_normalizes_case_and_punctuation() {
    assert_eq!(canonical_id("WCCO-DT").0, "WCCODT.us");
    assert_eq!(canonical_id("WCCODT").0, "WCCODT.us");
    assert_eq!(canonical_id("wcco dt").0, "WCCODT.us");
}

#[test]
fn parses_89_channels_from_fixture() {
    let w = provider().parse(&lineup(), &raw_fixture(), fetched_at());
    assert_eq!(w.channels.len(), 89);
    assert!(!w.observations.is_empty());
    assert_eq!(w.observations[0].provider.0, "gracenote");
    assert_eq!(w.observations[0].fetched_at, fetched_at());
}

fn raw_fixture() -> RawResponse {
    RawResponse {
        status: 200,
        body: Bytes::from_static(FIXTURE),
    }
}

#[test]
fn every_observation_has_image_url_with_tmsimg_prefix() {
    let w = provider().parse(&lineup(), &raw_fixture(), fetched_at());
    for o in &w.observations {
        let url = o.image_url.as_deref().expect("image_url present");
        assert!(
            url.starts_with("https://zap2it.tmsimg.com/assets/"),
            "{url}"
        );
        assert!(url.ends_with(".jpg"), "{url}");
    }
}

#[test]
fn thumbnail_is_used_as_is_only_with_an_http_scheme() {
    let thumb = |t: &str| {
        let ev = format!(
            r#"{{"startTime":"2026-10-31T22:00:00Z","endTime":"2026-10-31T23:00:00Z","thumbnail":"{t}","program":{{"title":"X"}}}}"#
        );
        let w = provider().parse(&lineup(), &raw(&one_channel(&ev)), fetched_at());
        w.observations[0].image_url.clone().unwrap()
    };
    assert_eq!(
        thumb("https://img.example/p.jpg"),
        "https://img.example/p.jpg"
    );
    assert_eq!(
        thumb("http://img.example/p.jpg"),
        "http://img.example/p.jpg"
    );
    // Anything else is a Gracenote image id, put under the image host.
    for t in ["httpx://evil/p", "javascript:alert(1)", "httpfoo"] {
        let url = thumb(t);
        assert!(
            url.starts_with("https://zap2it.tmsimg.com/assets/"),
            "{t}: {url}"
        );
    }
}

#[test]
fn channel_id_is_normalized_call_sign() {
    let w = provider().parse(&lineup(), &raw_fixture(), fetched_at());
    assert!(w.channels.iter().any(|c| c.call_sign == "WCCODT"));
    assert!(w.observations.iter().any(|o| o.channel.0 == "WCCODT.us"));
}

#[test]
fn duplicate_channel_number_5_6_keeps_both_channels() {
    let w = provider().parse(&lineup(), &raw_fixture(), fetched_at());
    let on_5_6: Vec<&str> = w
        .channels
        .iter()
        .filter(|c| c.number == "5.6")
        .map(|c| c.call_sign.as_str())
        .collect();
    assert!(on_5_6.contains(&"KSTCDT6"), "{on_5_6:?}");
    assert!(on_5_6.contains(&"KSTCDT4"), "{on_5_6:?}");
}

#[test]
fn drops_event_with_end_before_or_equal_start() {
    let json = one_channel(&format!(
        "{},{},{}",
        event("2026-10-31T21:00:00Z", "2026-10-31T21:00:00Z", "Equal", "d"),
        event(
            "2026-10-31T21:00:00Z",
            "2026-10-31T20:00:00Z",
            "Before",
            "d"
        ),
        event("2026-10-31T21:00:00Z", "2026-10-31T22:00:00Z", "Good", "d"),
    ));
    let w = provider().parse(&lineup(), &raw(&json), fetched_at());
    assert_eq!(w.observations.len(), 1);
    assert_eq!(w.observations[0].title, "Good");
    assert_eq!(w.dropped.invalid_time, 2);
}

#[test]
fn drops_event_with_missing_or_blank_title() {
    let blank = event("2026-10-31T21:00:00Z", "2026-10-31T22:00:00Z", "   ", "d");
    let missing =
        r#"{"startTime":"2026-10-31T22:00:00Z","endTime":"2026-10-31T23:00:00Z","program":{}}"#;
    let json = one_channel(&format!("{blank},{missing}"));
    let w = provider().parse(&lineup(), &raw(&json), fetched_at());
    assert!(w.observations.is_empty());
    assert_eq!(w.dropped.missing_title, 2);
}

#[test]
fn truncates_description_over_4000_chars_and_counts_nothing_dropped() {
    let long = "é".repeat(5000); // multi-byte on purpose: must cut on a char boundary
    let json = one_channel(&event(
        "2026-10-31T21:00:00Z",
        "2026-10-31T22:00:00Z",
        "T",
        &long,
    ));
    let w = provider().parse(&lineup(), &raw(&json), fetched_at());
    assert_eq!(w.observations.len(), 1);
    assert_eq!(
        w.observations[0]
            .description
            .as_ref()
            .unwrap()
            .chars()
            .count(),
        4000
    );
    assert_eq!(w.dropped, Default::default());
}

#[test]
fn duplicate_events_are_collapsed_and_counted() {
    let e = event("2026-10-31T21:00:00Z", "2026-10-31T22:00:00Z", "Same", "d");
    let json = one_channel(&format!("{e},{e}"));
    let w = provider().parse(&lineup(), &raw(&json), fetched_at());
    assert_eq!(w.observations.len(), 1);
    assert_eq!(w.dropped.duplicate, 1);
}

#[test]
fn unknown_json_shape_returns_parse_error_not_panic() {
    for body in [
        "not json",
        r#"{"unexpected":true}"#,
        "[]",
        "",
        r#"{"channels":"nope"}"#,
    ] {
        let w = provider().parse(&lineup(), &raw(body), fetched_at());
        assert!(
            w.parse_error.is_some(),
            "body {body:?} should be a parse error"
        );
        assert!(w.channels.is_empty());
        assert!(w.observations.is_empty());
    }
}

#[test]
fn valid_empty_channels_list_is_not_a_parse_error() {
    let w = provider().parse(&lineup(), &raw(r#"{"channels":[]}"#), fetched_at());
    assert!(w.parse_error.is_none());
    assert!(w.channels.is_empty());
}

#[test]
fn http_400_maps_to_bad_window() {
    assert!(matches!(map_status(400, None), Err(FetchError::BadWindow)));
}

#[test]
fn http_429_with_retry_after_header_maps_to_rate_limited() {
    match map_status(429, Some("120")) {
        Err(FetchError::RateLimited { retry_after }) => {
            assert_eq!(retry_after, Some(Duration::from_secs(120)));
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        map_status(429, None),
        Err(FetchError::RateLimited { retry_after: None })
    ));
}

#[test]
fn other_statuses_map_as_documented() {
    assert!(map_status(200, None).is_ok());
    assert!(matches!(
        map_status(503, None),
        Err(FetchError::Transient(_))
    ));
    assert!(matches!(map_status(403, None), Err(FetchError::Fatal(_))));
    assert!(matches!(map_status(404, None), Err(FetchError::Fatal(_))));
}

/// End to end through a real (local) HTTP server: the request carries the
/// expected query, and a 429 with `Retry-After` is mapped from the wire.
#[tokio::test]
async fn fetch_window_sends_query_and_maps_429_from_the_wire() {
    use axum::extract::{Query, State};
    use axum::response::IntoResponse;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    // The handler only records the query; assertions run in the test body so
    // a failure shows a clear message instead of a 500 from the server task.
    type Seen = Arc<Mutex<Option<HashMap<String, String>>>>;
    let seen: Seen = Arc::default();
    let app = Router::new()
        .route(
            "/api/grid",
            get(
                |State(seen): State<Seen>, Query(q): Query<HashMap<String, String>>| async move {
                    *seen.lock().unwrap() = Some(q);
                    (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "7")]).into_response()
                },
            ),
        )
        .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let p = GracenoteProvider::new(
        patina::provider::gracenote::http_client(std::time::Duration::from_secs(10)).unwrap(),
        base,
    );
    let start = Utc.timestamp_opt(1793480400, 0).unwrap();
    match p.fetch_window(&lineup(), start).await {
        Err(FetchError::RateLimited { retry_after }) => {
            assert_eq!(retry_after, Some(Duration::from_secs(7)));
        }
        other => panic!("{other:?}"),
    }
    let q = seen.lock().unwrap().clone().expect("server saw a request");
    assert_eq!(q["lineupId"], "USA-OTA55401");
    assert_eq!(q["headendId"], "lineupId");
    assert_eq!(q["timespan"], "6");
    assert_eq!(q["time"], "1793480400");
    assert_eq!(q["aid"], "orbebb");
}
