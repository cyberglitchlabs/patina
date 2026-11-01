//! HDHomeRun tuner source: reads `{base}/lineup.json`.

use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use url::Url;

use super::{ChannelSource, TunerChannel};
use crate::provider::{MAX_BODY_BYTES, read_body_limited};

const TIMEOUT: Duration = Duration::from_secs(10);

pub struct HdHomeRunSource {
    client: reqwest::Client,
    base_url: Url,
    /// Largest body accepted (`MAX_BODY_BYTES` unless a test lowers it).
    max_body_bytes: usize,
}

impl HdHomeRunSource {
    /// `url` is the device's base URL, for example `http://192.168.1.50`.
    pub fn new(client: reqwest::Client, url: Url) -> Self {
        Self {
            client,
            base_url: url,
            max_body_bytes: MAX_BODY_BYTES,
        }
    }

    /// Accept bodies of at most `limit` bytes instead of `MAX_BODY_BYTES`
    /// (for tests).
    pub fn with_max_body_bytes(mut self, limit: usize) -> Self {
        self.max_body_bytes = limit;
        self
    }

    fn lineup_url(&self) -> Result<Url> {
        // `Url::join` replaces the last path segment unless the base ends in
        // '/', so make sure it does.
        let mut base = self.base_url.clone();
        if !base.path().ends_with('/') {
            let p = format!("{}/", base.path());
            base.set_path(&p);
        }
        base.join("lineup.json").context("bad HDHomeRun URL")
    }
}

#[async_trait]
impl ChannelSource for HdHomeRunSource {
    async fn load(&self) -> Result<Vec<TunerChannel>> {
        let url = self.lineup_url()?;
        let resp = self
            .client
            .get(url.clone())
            .timeout(TIMEOUT)
            .send()
            .await
            .with_context(|| format!("request to {url} failed"))?
            .error_for_status()
            .with_context(|| format!("{url} returned an error status"))?;
        // An oversized body is a failed load (the last-known list is kept).
        let body = read_body_limited(resp, self.max_body_bytes)
            .await
            .map_err(|e| anyhow::anyhow!("reading response from {url}: {e}"))?;
        parse_lineup(&body)
    }
}

/// One lineup entry; the device sends many more fields, which we ignore.
#[derive(Deserialize)]
struct LineupEntry {
    #[serde(rename = "GuideNumber", default)]
    guide_number: String,
    #[serde(rename = "GuideName", default)]
    guide_name: String,
}

/// Parse a `lineup.json` body. Entries without a guide number are skipped.
pub fn parse_lineup(body: &[u8]) -> Result<Vec<TunerChannel>> {
    let entries: Vec<LineupEntry> =
        serde_json::from_slice(body).context("lineup.json is not a JSON list of channels")?;
    Ok(entries
        .into_iter()
        .filter_map(|e| {
            let number = e.guide_number.trim().to_string();
            (!number.is_empty()).then(|| TunerChannel {
                number,
                name: e.guide_name.trim().to_string(),
            })
        })
        .collect())
}
