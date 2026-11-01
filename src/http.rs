//! HTTP routing: health, XMLTV feeds, and the admin endpoints `/status` and
//! `/metrics`.
//!
//! Logging note: nothing in this module logs a request URL, path, query or
//! header, and no request-logging middleware is installed, because feed
//! tokens travel in the URL and the admin token in a header. Keep it that way.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::{Router, routing::get};

use crate::auth::{bearer_ok, unauthorized};
use crate::feeds::{FeedOutcome, Feeds, Rendered};
use crate::metrics::CONTENT_TYPE as METRICS_CONTENT_TYPE;
use crate::status::Admin;

/// Shared state handed to every request handler. `Clone` is required by axum
/// because each request gets its own handle to the state.
#[derive(Clone, Default)]
pub struct AppState {
    /// `None` only in `AppState::default()` (the health check test); every
    /// feed route then answers 404.
    feeds: Option<Arc<Feeds>>,
    /// `None` only when no admin endpoints are wired (tests of the feeds
    /// alone); `/status` and `/metrics` then answer 404.
    admin: Option<Arc<Admin>>,
}

impl AppState {
    pub fn new(feeds: Arc<Feeds>) -> Self {
        AppState {
            feeds: Some(feeds),
            admin: None,
        }
    }

    /// Also serve `/status` and `/metrics`.
    pub fn with_admin(mut self, admin: Arc<Admin>) -> Self {
        self.admin = Some(admin);
        self
    }
}

/// Builds the application's routes.
pub fn router(state: AppState) -> Router {
    Router::new()
        // Health check: always open, never requires a token.
        .route("/healthz", get(healthz))
        // Admin endpoints: bearer token (header only) when one is configured.
        .route("/status", get(status))
        .route("/metrics", get(metrics))
        // Everything under /xmltv and /t goes through one handler each, so
        // the token check covers every path there, including nonsense ones.
        .route("/xmltv", get(xmltv_root))
        .route("/xmltv/{*file}", get(xmltv_plain))
        .route("/t", get(xmltv_root))
        .route("/t/{*rest}", get(xmltv_with_path_token))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}

/// `GET /status`: JSON for humans and the future web UI.
async fn status(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let Some(admin) = &state.admin else {
        return StatusCode::NOT_FOUND.into_response();
    };
    // The token is only read from the Authorization header, never the URL.
    if !bearer_ok(admin.token(), &headers) {
        return unauthorized();
    }
    axum::Json(admin.status().await).into_response()
}

/// `GET /metrics`: Prometheus text format.
async fn metrics(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let Some(admin) = &state.admin else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if !bearer_ok(admin.token(), &headers) {
        return unauthorized();
    }
    (
        [(header::CONTENT_TYPE, METRICS_CONTENT_TYPE)],
        admin.metrics_text().await,
    )
        .into_response()
}

/// `GET /xmltv` and `GET /t` themselves: not feeds, but guarded like the rest.
async fn xmltv_root(State(state): State<AppState>, uri: Uri, headers: HeaderMap) -> Response {
    serve(&state, query_tokens(&uri), None, &headers).await
}

/// `GET /xmltv/{file}`, with the token (if any) in `?token=`.
async fn xmltv_plain(
    State(state): State<AppState>,
    Path(file): Path<String>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    serve(&state, query_tokens(&uri), Some(&file), &headers).await
}

/// `GET /t/{token}/xmltv/{file}`. Anything else under `/t` is not a feed,
/// but it still gets the 401 a missing token deserves.
async fn xmltv_with_path_token(
    State(state): State<AppState>,
    Path(rest): Path<String>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let (path_token, tail) = rest.split_once('/').unwrap_or((rest.as_str(), ""));
    // `None` for the file means "not a feed path": 404 once authorized.
    let file = tail.strip_prefix("xmltv/");
    // The path token and any query tokens are all candidates; `token_ok`
    // refuses a request that carries more than one.
    let mut tokens = vec![path_token.to_string()];
    tokens.extend(query_tokens(&uri));
    serve(&state, tokens, file, &headers).await
}

/// The `token` query parameters, percent-decoded.
fn query_tokens(uri: &Uri) -> Vec<String> {
    uri.query()
        .map(|q| {
            url::form_urlencoded::parse(q.as_bytes())
                .filter(|(k, _)| k == "token")
                .map(|(_, v)| v.into_owned())
                .collect()
        })
        .unwrap_or_default()
}

/// Token check first, then the feed. Order matters: an unauthorized request
/// gets the same bare 401 whether or not the feed exists.
async fn serve(
    state: &AppState,
    tokens: Vec<String>,
    file: Option<&str>,
    headers: &HeaderMap,
) -> Response {
    let Some(feeds) = &state.feeds else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let supplied: Vec<&str> = tokens.iter().map(String::as_str).collect();
    if !feeds.token_ok(&supplied) {
        // Empty body and no extra headers: identical for every path.
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(file) = file else {
        return StatusCode::NOT_FOUND.into_response();
    };
    // `all.xml` and `<source>.xml`; the extension is optional.
    let name = file.strip_suffix(".xml").unwrap_or(file);
    match feeds.feed(name).await {
        FeedOutcome::NotFound => StatusCode::NOT_FOUND.into_response(),
        FeedOutcome::Unavailable(why) => (
            StatusCode::SERVICE_UNAVAILABLE,
            [
                (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
                (header::RETRY_AFTER, "60"),
            ],
            format!("{why}\n"),
        )
            .into_response(),
        FeedOutcome::Ok(rendered) => ok_response(&rendered, headers, feeds.token_required()),
    }
}

/// A feed body (or 304). `private`: a feed token is configured, so shared
/// caches (proxies) must not store the body; only the client may.
fn ok_response(r: &Rendered, request_headers: &HeaderMap, private: bool) -> Response {
    let etag = HeaderValue::from_str(&r.etag).expect("etag is ASCII");
    let cache_control = if private {
        "private, max-age=60"
    } else {
        "max-age=60"
    };
    let common = [
        (header::ETAG, etag),
        (
            header::CACHE_CONTROL,
            HeaderValue::from_static(cache_control),
        ),
    ];
    if request_headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|inm| etag_matches(inm, &r.etag))
    {
        return (StatusCode::NOT_MODIFIED, common).into_response();
    }
    (
        StatusCode::OK,
        common,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/xml; charset=utf-8"),
        )],
        r.body.clone(),
    )
        .into_response()
}

/// Does an `If-None-Match` value (a list of tags, or `*`) include `etag`?
/// Weak tags (`W/"x"`) compare equal to strong ones, as the spec says for
/// this header.
fn etag_matches(header_value: &str, etag: &str) -> bool {
    header_value.split(',').map(str::trim).any(|candidate| {
        candidate == "*" || candidate.strip_prefix("W/").unwrap_or(candidate) == etag
    })
}
