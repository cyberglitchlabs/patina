//! The `Provider` abstraction: something that can fetch a window of guide
//! data for a lineup and parse it into provider-neutral observations.
//!
//! Fetching and parsing are deliberately separate: `fetch_window` does one
//! HTTP request and nothing else (retry, backoff and rate limiting live in
//! the shared fetch layer), while `parse` is pure so it can be tested
//! offline against captured responses.

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::config::LineupCfg;
use crate::model::{Observation, ProviderId};

pub mod gracenote;

/// The largest response body read from a provider or a tuner: 32 MiB. A
/// real 6-hour Gracenote window is about 400 KB and a tuner lineup a few
/// KB; anything far larger is broken or hostile and is refused (a failed
/// fetch, never partially parsed) instead of being held in memory.
pub const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;

/// Read a response body, refusing it once it is larger than `limit` bytes:
/// at once when the `Content-Length` header says so, otherwise as soon as
/// the received bytes pass the limit (a server may send no length, or a
/// wrong one). The error text says what happened, for logs and `/status`.
pub async fn read_body_limited(mut resp: reqwest::Response, limit: usize) -> Result<Bytes, String> {
    let too_big = || format!("response body larger than {limit} bytes; refused");
    if resp
        .content_length()
        .is_some_and(|n| n > u64::try_from(limit).unwrap_or(u64::MAX))
    {
        return Err(too_big());
    }
    let mut body: Vec<u8> = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| e.without_url().to_string())?
    {
        if body.len() + chunk.len() > limit {
            return Err(too_big());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(body))
}

/// A successful HTTP response, body not yet interpreted.
#[derive(Debug, Clone)]
pub struct RawResponse {
    pub status: u16,
    pub body: Bytes,
}

/// A channel as the provider names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderChannel {
    pub provider_channel_id: String,
    /// Channel number as shown to viewers, for example `5.6`.
    pub number: String,
    pub call_sign: String,
}

/// How many events `parse` discarded, by reason.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DropCounts {
    /// End not after start, or a time that would not parse.
    pub invalid_time: u32,
    /// Title absent or only whitespace.
    pub missing_title: u32,
    /// Same channel, start, end and title seen earlier in the response.
    pub duplicate: u32,
}

#[derive(Debug, Clone, Default)]
pub struct ParsedWindow {
    pub channels: Vec<ProviderChannel>,
    pub observations: Vec<Observation>,
    pub dropped: DropCounts,
    /// Set when the body could not be understood at all. A valid response
    /// with no programmes leaves this `None`, so callers can tell the two
    /// apart (never treat a parse failure as "the guide is empty").
    pub parse_error: Option<String>,
}

/// Why a fetch failed. The variant tells the fetch layer what to do next.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FetchError {
    /// Worth retrying later (5xx, network error, timeout).
    #[error("transient fetch error: {0}")]
    Transient(String),
    /// The provider asked us to slow down.
    #[error("rate limited (retry after {retry_after:?})")]
    RateLimited { retry_after: Option<Duration> },
    /// The provider rejected the requested time window (HTTP 400).
    #[error("provider rejected the requested window")]
    BadWindow,
    /// Retrying will not help (for example 403 because we are blocked).
    #[error("fatal fetch error: {0}")]
    Fatal(String),
}

#[async_trait]
pub trait Provider: Send + Sync {
    fn id(&self) -> &ProviderId;

    /// Perform exactly one request for the window starting at `window_start`.
    async fn fetch_window(
        &self,
        lineup: &LineupCfg,
        window_start: DateTime<Utc>,
    ) -> Result<RawResponse, FetchError>;

    /// Turn a raw response into observations. Pure: no I/O, never panics on
    /// malformed input (returns an empty window with `parse_error` set).
    fn parse(
        &self,
        lineup: &LineupCfg,
        raw: &RawResponse,
        fetched_at: DateTime<Utc>,
    ) -> ParsedWindow;
}
