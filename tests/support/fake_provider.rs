//! A fake Gracenote-like provider server for tests.
//!
//! It is a real HTTP server on `127.0.0.1` (random port) that answers
//! `GET /api/grid` according to a script: each request takes the next
//! [`Behavior`] from the queue, and once the queue is empty it uses the
//! default behaviour (serve the real fixture unless changed). It records
//! every request so tests can count calls and inspect headers.
//!
//! Usage:
//! ```ignore
//! let fake = FakeProvider::start().await;
//! fake.script([Behavior::Status(500), Behavior::ok_fixture()]);
//! let provider = fake.gracenote();          // GracenoteProvider pointed here
//! assert_eq!(fake.request_count(), 0);
//! ```
//!
//! Shared by several test files; each uses only part of it.
#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use patina::provider::gracenote::GracenoteProvider;
use url::Url;

/// The captured 89-channel Gracenote response used for successful answers.
pub fn fixture() -> Bytes {
    Bytes::from_static(include_bytes!(
        "../fixtures/gracenote/grid-USA-OTA55401-6h.json"
    ))
}

/// The 6-hour window the captured fixture answers, as unix seconds
/// (2026-10-31T22:00:00Z). The capture was not made on the 6-hour grid: every
/// one of its 89 channels lists programmes covering at least 21:30 to 04:00
/// UTC, so 22:00-04:00 is the 6-hour block all of them fully cover.
pub const FIXTURE_WINDOW_START: i64 = 1_793_484_000;

/// The fixture with every event's `startTime` and `endTime` moved by
/// `shift_secs` seconds. Nothing else changes (same channels, titles,
/// images, order), so the structure stays exactly that of a real response.
pub fn shifted_fixture(body: &[u8], shift_secs: i64) -> Bytes {
    let mut grid: serde_json::Value = serde_json::from_slice(body).expect("fixture is JSON");
    let delta = chrono::TimeDelta::seconds(shift_secs);
    let shift = |v: &mut serde_json::Value| {
        if let Some(text) = v.as_str() {
            let t = chrono::DateTime::parse_from_rfc3339(text).expect("fixture time") + delta;
            *v = serde_json::Value::String(t.format("%Y-%m-%dT%H:%M:%SZ").to_string());
        }
    };
    for ch in grid["channels"].as_array_mut().expect("channels") {
        for ev in ch["events"].as_array_mut().expect("events") {
            shift(&mut ev["startTime"]);
            shift(&mut ev["endTime"]);
        }
    }
    Bytes::from(serde_json::to_vec(&grid).unwrap())
}

/// What the server does for one request.
#[derive(Debug, Clone)]
pub enum Behavior {
    /// HTTP 200 with this body.
    Ok(Bytes),
    /// An empty response with this status code (400, 403, 500, ...).
    Status(u16),
    /// HTTP 429 with a `Retry-After: <secs>` header.
    RetryAfter(u64),
    /// Wait this many milliseconds (real time), then serve the fixture.
    Delay(u64),
    /// HTTP 200 with only the first half of the fixture (invalid JSON).
    Truncated,
    /// HTTP 200 with this body (a fixture) moved in time so that its 6-hour
    /// window (`FIXTURE_WINDOW_START`) lands on the window the request asked
    /// for (`time` query parameter, unix seconds). Each window then gets its
    /// own distinct, realistic answer. Only the event times change.
    OkShifted(Bytes),
    /// Like `OkShifted`, then moved `extra_secs` further (a provider that
    /// re-timed every programme, for hindsight-accuracy tests).
    OkShiftedBy(Bytes, i64),
    /// HTTP 302 pointing at `Location` (a client following it would send a
    /// second request).
    Redirect(String),
}

impl Behavior {
    /// HTTP 200 with the real fixture body.
    pub fn ok_fixture() -> Self {
        Behavior::Ok(fixture())
    }

    /// The real fixture, moved to whichever window is requested.
    pub fn ok_shifted() -> Self {
        Behavior::OkShifted(fixture())
    }
}

/// One request as the server saw it.
#[derive(Debug, Clone)]
pub struct SeenRequest {
    pub query: HashMap<String, String>,
    pub user_agent: Option<String>,
}

#[derive(Default)]
struct Inner {
    script: VecDeque<Behavior>,
    /// Fixed behaviour for particular windows (by `time` parameter); wins
    /// over the script and the default, and is never used up.
    per_window: HashMap<i64, Behavior>,
    /// Fixed behaviour for one lineup (by `lineupId` parameter); wins over
    /// the script and the default (but not `per_window`), never used up.
    per_lineup: HashMap<String, Behavior>,
    default: Option<Behavior>,
    seen: Vec<SeenRequest>,
    in_flight: usize,
    max_in_flight: usize,
}

type Shared = Arc<Mutex<Inner>>;

pub struct FakeProvider {
    base_url: Url,
    inner: Shared,
    server: tokio::task::JoinHandle<()>,
}

impl FakeProvider {
    /// Start a server. Its default behaviour is `Behavior::ok_fixture()`.
    pub async fn start() -> Self {
        let inner: Shared = Arc::default();
        let app = Router::new()
            .route("/api/grid", get(handle))
            .with_state(inner.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        // Trailing slash matters: the provider joins "api/grid" onto it.
        let base_url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        FakeProvider {
            base_url,
            inner,
            server,
        }
    }

    /// Queue behaviours for the next requests, in order.
    pub fn script(&self, behaviors: impl IntoIterator<Item = Behavior>) {
        self.inner.lock().unwrap().script.extend(behaviors);
    }

    /// Always answer requests for the window starting at `time` (unix
    /// seconds) like this, whatever the script or default say.
    pub fn set_for_window(&self, time: i64, behavior: Behavior) {
        self.inner.lock().unwrap().per_window.insert(time, behavior);
    }

    /// Always answer requests for this lineup (`lineupId` parameter) like
    /// this, whatever the script or default say.
    pub fn set_for_lineup(&self, lineup_id: &str, behavior: Behavior) {
        self.inner
            .lock()
            .unwrap()
            .per_lineup
            .insert(lineup_id.to_string(), behavior);
    }

    /// What to do once the script queue is empty.
    pub fn set_default(&self, behavior: Behavior) {
        self.inner.lock().unwrap().default = Some(behavior);
    }

    /// How many requests the server has received.
    pub fn request_count(&self) -> usize {
        self.inner.lock().unwrap().seen.len()
    }

    /// Every request received so far, oldest first.
    pub fn requests(&self) -> Vec<SeenRequest> {
        self.inner.lock().unwrap().seen.clone()
    }

    /// The largest number of requests that were being answered at once.
    pub fn max_in_flight(&self) -> usize {
        self.inner.lock().unwrap().max_in_flight
    }

    /// Base URL to hand to a provider, e.g. `http://127.0.0.1:41234/`.
    pub fn base_url(&self) -> Url {
        self.base_url.clone()
    }

    /// A real `GracenoteProvider` (id `gracenote`) pointed at this server.
    pub fn gracenote(&self) -> GracenoteProvider {
        GracenoteProvider::new(
            patina::provider::gracenote::http_client(std::time::Duration::from_secs(10)).unwrap(),
            self.base_url(),
        )
    }
}

impl Drop for FakeProvider {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn handle(
    State(inner): State<Shared>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    // Requested window start (unix seconds), used by the shifting behaviours.
    let requested: Option<i64> = query.get("time").and_then(|t| t.parse().ok());
    let lineup_id = query.get("lineupId").cloned();
    let behavior = {
        let mut st = inner.lock().unwrap();
        st.seen.push(SeenRequest {
            query,
            user_agent: headers
                .get(header::USER_AGENT)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned),
        });
        st.in_flight += 1;
        st.max_in_flight = st.max_in_flight.max(st.in_flight);
        let fixed = requested
            .and_then(|t| st.per_window.get(&t).cloned())
            .or_else(|| lineup_id.and_then(|l| st.per_lineup.get(&l).cloned()));
        fixed
            .or_else(|| st.script.pop_front())
            .or_else(|| st.default.clone())
            .unwrap_or_else(Behavior::ok_fixture)
    };

    let response = match behavior {
        Behavior::Ok(body) => (StatusCode::OK, body).into_response(),
        Behavior::Status(code) => StatusCode::from_u16(code).unwrap().into_response(),
        Behavior::RetryAfter(secs) => (
            StatusCode::TOO_MANY_REQUESTS,
            [(header::RETRY_AFTER, secs.to_string())],
        )
            .into_response(),
        Behavior::Delay(ms) => {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            (StatusCode::OK, fixture()).into_response()
        }
        Behavior::OkShifted(body) => {
            let shift = requested.expect("time parameter") - FIXTURE_WINDOW_START;
            (StatusCode::OK, shifted_fixture(&body, shift)).into_response()
        }
        Behavior::OkShiftedBy(body, extra) => {
            let shift = requested.expect("time parameter") - FIXTURE_WINDOW_START + extra;
            (StatusCode::OK, shifted_fixture(&body, shift)).into_response()
        }
        Behavior::Redirect(location) => {
            (StatusCode::FOUND, [(header::LOCATION, location)]).into_response()
        }
        Behavior::Truncated => {
            let body = fixture();
            (StatusCode::OK, body.slice(..body.len() / 2)).into_response()
        }
    };
    inner.lock().unwrap().in_flight -= 1;
    response
}
