//! LIVE smoke test against the real Gracenote service. Never run by default:
//! the test is `#[ignore]`, so plain `cargo test` skips it. It is started
//! by hand from the "Live smoke" GitHub workflow (workflow_dispatch only), or
//! locally with:
//!
//!     cargo test --test live_smoke -- --ignored --nocapture
//!
//! It makes exactly two requests (two consecutive 6-hour windows of one
//! lineup, 5 seconds apart) to keep the load on the provider tiny.
//! Settings come from environment variables so the workflow can pass inputs:
//!   PATINA_SMOKE_POSTAL_CODE (default 55401)
//!   PATINA_SMOKE_LINEUP_ID   (default USA-OTA55401)

use std::time::Duration;

use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use patina::config::LineupCfg;
use patina::provider::Provider;
use patina::provider::gracenote::{DEFAULT_BASE_URL, GracenoteProvider, http_client};

fn setting(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

/// Start of the current 6-hour block (UTC), a valid window start.
fn current_window_start(now: DateTime<Utc>) -> DateTime<Utc> {
    let hours = (now.timestamp() / 3600 / 6) * 6;
    Utc.timestamp_opt(hours * 3600, 0).unwrap()
}

#[tokio::test]
#[ignore = "contacts the real Gracenote service; run manually with --ignored"]
async fn live_gracenote_lineup_is_parseable() {
    let lineup = LineupCfg {
        name: "smoke".into(),
        lineup_id: setting("PATINA_SMOKE_LINEUP_ID", "USA-OTA55401"),
        postal_code: setting("PATINA_SMOKE_POSTAL_CODE", "55401"),
        country: "USA".into(),
    };
    let client = http_client(Duration::from_secs(60)).expect("http client");
    let base = url::Url::parse(DEFAULT_BASE_URL).unwrap();
    let provider = GracenoteProvider::new(client, base);

    let first = current_window_start(Utc::now());
    for (i, start) in [first, first + TimeDelta::hours(6)].into_iter().enumerate() {
        if i > 0 {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        let raw = provider
            .fetch_window(&lineup, start)
            .await
            .unwrap_or_else(|e| panic!("window {i}: fetch failed: {e}"));
        assert_eq!(raw.status, 200, "window {i}: HTTP status");
        let parsed = provider.parse(&lineup, &raw, Utc::now());
        assert_eq!(parsed.parse_error, None, "window {i}: response must parse");
        assert!(
            parsed.channels.len() >= 10,
            "window {i}: only {} channels",
            parsed.channels.len()
        );
        assert!(
            parsed.observations.iter().any(|o| o.image_url.is_some()),
            "window {i}: no observation has an image URL"
        );
        println!(
            "window {i}: {} channels, {} observations, dropped {:?}",
            parsed.channels.len(),
            parsed.observations.len(),
            parsed.dropped
        );
    }
}
