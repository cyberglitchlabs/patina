//! Gracenote (tvlistings.gracenote.com) provider.

use std::collections::HashSet;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use url::Url;

use super::{
    FetchError, MAX_BODY_BYTES, ParsedWindow, Provider, ProviderChannel, RawResponse,
    read_body_limited,
};
use crate::config::LineupCfg;
use crate::model::{Observation, ProviderId, canonical_id};

/// The site rejects obviously non-browser clients, so look like one.
pub const USER_AGENT: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36";
/// The real Gracenote listings site (used unless the config overrides it).
pub const DEFAULT_BASE_URL: &str = "https://tvlistings.gracenote.com/";
const IMAGE_PREFIX: &str = "https://zpmc.tmsimg.com/assets/";
const MAX_TITLE_CHARS: usize = 500;
const MAX_DESC_CHARS: usize = 4000;

/// The production HTTP client for Gracenote. Built once in `main` and shared.
///
/// * Redirects are NOT followed. The fetch layer charges the daily budget
///   once per `fetch_window` call; a client that silently follows redirects
///   would turn one charged request into several real ones (up to 10 with
///   reqwest's default), breaking the "hard budget" promise. A 3xx is
///   instead reported as an error (see `map_status`).
/// * Every request has a timeout, so a hung connection cannot hold the
///   provider's turn forever (the fetch layer has its own, too).
/// * The browser-like User-Agent is set here as well as on each request.
pub fn http_client(timeout: Duration) -> reqwest::Result<reqwest::Client> {
    // reqwest is built without a bundled crypto provider (see `crate::tls`).
    crate::tls::install_crypto_provider();
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(timeout)
        .user_agent(USER_AGENT)
        .build()
}

pub struct GracenoteProvider {
    id: ProviderId,
    client: reqwest::Client,
    base_url: Url,
    /// Largest body accepted (`MAX_BODY_BYTES` unless a test lowers it).
    max_body_bytes: usize,
}

impl GracenoteProvider {
    /// Provider named `gracenote`.
    pub fn new(client: reqwest::Client, base_url: Url) -> Self {
        Self::new_with_id(ProviderId("gracenote".into()), client, base_url)
    }

    /// Provider with a custom id (the name from the config file).
    pub fn new_with_id(id: ProviderId, client: reqwest::Client, base_url: Url) -> Self {
        Self {
            id,
            client,
            base_url,
            max_body_bytes: MAX_BODY_BYTES,
        }
    }

    /// Accept bodies of at most `limit` bytes instead of `MAX_BODY_BYTES`
    /// (tests use a small limit instead of serving 32 MiB).
    pub fn with_max_body_bytes(mut self, limit: usize) -> Self {
        self.max_body_bytes = limit;
        self
    }
}

/// Map an HTTP status to success or the matching `FetchError`. Pure, so the
/// policy is unit-testable without a server.
///
/// 403 is `Fatal` rather than `Transient` on purpose: it usually means our
/// client has been blocked, and hammering a server that blocked us makes it
/// worse. Other 4xx (404, ...) mean the request itself is wrong.
pub fn map_status(status: u16, retry_after: Option<&str>) -> Result<(), FetchError> {
    match status {
        200..=299 => Ok(()),
        400 => Err(FetchError::BadWindow),
        429 => Err(FetchError::RateLimited {
            // Retry-After can also be an HTTP date; we only honor seconds.
            retry_after: retry_after
                .and_then(|v| v.trim().parse::<u64>().ok())
                .map(Duration::from_secs),
        }),
        500..=599 => Err(FetchError::Transient(format!("HTTP {status}"))),
        _ => Err(FetchError::Fatal(format!("HTTP {status}"))),
    }
}

#[async_trait]
impl Provider for GracenoteProvider {
    fn id(&self) -> &ProviderId {
        &self.id
    }

    async fn fetch_window(
        &self,
        lineup: &LineupCfg,
        window_start: DateTime<Utc>,
    ) -> Result<RawResponse, FetchError> {
        let url = self
            .base_url
            .join("api/grid")
            .map_err(|e| FetchError::Fatal(format!("bad base url: {e}")))?;
        let time = window_start.timestamp().to_string();
        // Errors are logged upstream; `without_url` keeps the request URL
        // (with the lineup's query string) out of the logs.
        let resp = self
            .client
            .get(url)
            .header(reqwest::header::USER_AGENT, USER_AGENT)
            .query(&[
                ("lineupId", lineup.lineup_id.as_str()),
                ("timespan", "6"),
                ("headendId", "lineupId"),
                ("country", lineup.country.as_str()),
                ("timezone", ""),
                ("device", "-"),
                ("postalCode", lineup.postal_code.as_str()),
                ("isOverride", "true"),
                ("time", time.as_str()),
                ("pref", "-"),
                ("userId", "-"),
                ("aid", "orbebb"),
                ("languagecode", "en-us"),
            ])
            .send()
            .await
            .map_err(|e| FetchError::Transient(e.without_url().to_string()))?;

        let status = resp.status().as_u16();
        let retry_after = resp
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        map_status(status, retry_after.as_deref())?;
        // An oversized body is a failed fetch (counted like any other
        // transient failure); nothing of it is parsed or cached.
        let body = read_body_limited(resp, self.max_body_bytes)
            .await
            .map_err(FetchError::Transient)?;
        Ok(RawResponse { status, body })
    }

    fn parse(
        &self,
        _lineup: &LineupCfg,
        raw: &RawResponse,
        fetched_at: DateTime<Utc>,
    ) -> ParsedWindow {
        let grid: Grid = match serde_json::from_slice(&raw.body) {
            Ok(g) => g,
            Err(e) => {
                tracing::warn!(error = %e, "gracenote: unrecognised response shape");
                return ParsedWindow {
                    parse_error: Some(e.to_string()),
                    ..ParsedWindow::default()
                };
            }
        };

        let mut out = ParsedWindow::default();
        // Identity of an event within this response; see `DropCounts::duplicate`.
        let mut seen = HashSet::new();

        for ch in grid.channels {
            let Some(call_sign) = ch.call_sign.filter(|s| !s.trim().is_empty()) else {
                continue; // cannot build a canonical id without it
            };
            let channel = canonical_id(&call_sign);
            out.channels.push(ProviderChannel {
                provider_channel_id: ch.channel_id.unwrap_or_default(),
                number: ch.channel_no.unwrap_or_default(),
                call_sign: call_sign.clone(),
            });

            for ev in ch.events {
                let (Some(start), Some(end)) =
                    (parse_time(&ev.start_time), parse_time(&ev.end_time))
                else {
                    out.dropped.invalid_time += 1;
                    continue;
                };
                if end <= start {
                    out.dropped.invalid_time += 1;
                    continue;
                }
                let program = ev.program.unwrap_or_default();
                let Some(title) = non_blank(program.title) else {
                    out.dropped.missing_title += 1;
                    continue;
                };
                let title = truncate_chars(&title, MAX_TITLE_CHARS);
                if !seen.insert((channel.clone(), start, end, title.clone())) {
                    out.dropped.duplicate += 1;
                    continue;
                }
                out.observations.push(Observation {
                    provider: self.id.clone(),
                    channel: channel.clone(),
                    start,
                    end,
                    title,
                    subtitle: non_blank(program.episode_title),
                    description: non_blank(program.short_desc)
                        .map(|d| truncate_chars(&d, MAX_DESC_CHARS)),
                    image_url: non_blank(ev.thumbnail).map(|t| image_url(&t)),
                    season: program.season.as_ref().and_then(lenient_u32),
                    episode: program.episode.as_ref().and_then(lenient_u32),
                    rating: non_blank(ev.rating),
                    genres: Vec::new(),
                    fetched_at,
                });
            }
        }
        out
    }
}

fn parse_time(s: &Option<String>) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s.as_deref()?)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// `Some(trimmed)` unless the value is absent or only whitespace.
fn non_blank(s: Option<String>) -> Option<String> {
    s.map(|s| s.trim().to_owned()).filter(|s| !s.is_empty())
}

/// Cut to at most `max` characters. Counting chars (not bytes) guarantees
/// the cut never lands inside a multi-byte character.
fn truncate_chars(s: &str, max: usize) -> String {
    s.chars().take(max).collect()
}

/// A full `http(s)://` link is used as is; anything else is an image id
/// under the image host (so `httpx:` or `javascript:` can never pass).
fn image_url(thumbnail: &str) -> String {
    if crate::xmltv::is_http_url(thumbnail) {
        thumbnail.to_owned()
    } else {
        format!("{IMAGE_PREFIX}{thumbnail}.jpg")
    }
}

/// Season/episode arrive as strings ("1"), sometimes numbers; anything else
/// (null, "", "Special") becomes `None`.
fn lenient_u32(v: &Value) -> Option<u32> {
    match v {
        Value::Number(n) => n.as_u64().and_then(|n| u32::try_from(n).ok()),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

// --- Wire format. Every field is optional and unknown fields are ignored
// (serde's default), so a provider-side change degrades gracefully.

/// Accept a string, a number or a bool as `Option<String>`; null and other
/// shapes become `None` instead of failing the whole response.
fn lenient_string<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    Ok(match Value::deserialize(d)? {
        Value::String(s) => Some(s),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    })
}

#[derive(Deserialize)]
struct Grid {
    channels: Vec<WireChannel>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireChannel {
    #[serde(default, deserialize_with = "lenient_string")]
    call_sign: Option<String>,
    #[serde(default, deserialize_with = "lenient_string")]
    channel_no: Option<String>,
    #[serde(default, deserialize_with = "lenient_string")]
    channel_id: Option<String>,
    #[serde(default)]
    events: Vec<WireEvent>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireEvent {
    #[serde(default, deserialize_with = "lenient_string")]
    start_time: Option<String>,
    #[serde(default, deserialize_with = "lenient_string")]
    end_time: Option<String>,
    #[serde(default, deserialize_with = "lenient_string")]
    thumbnail: Option<String>,
    #[serde(default, deserialize_with = "lenient_string")]
    rating: Option<String>,
    program: Option<WireProgram>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct WireProgram {
    #[serde(default, deserialize_with = "lenient_string")]
    title: Option<String>,
    #[serde(default, deserialize_with = "lenient_string")]
    short_desc: Option<String>,
    #[serde(default, deserialize_with = "lenient_string")]
    episode_title: Option<String>,
    season: Option<Value>,
    episode: Option<Value>,
}
