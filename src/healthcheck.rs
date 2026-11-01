//! `patina healthcheck`: a tiny client for the container `HEALTHCHECK`.
//!
//! The runtime image has no shell, curl or wget, so the binary checks itself:
//! it asks the running server (on the configured listen address; a wildcard
//! address is probed on loopback) for `/healthz` and succeeds only on HTTP 200
//! with the body `ok`. Nothing here prints URLs, headers or configuration.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

/// Where to probe for a server that listens on `listen`. A wildcard address
/// (`0.0.0.0` or `::`) cannot be connected to, so it becomes loopback.
pub fn target_url(listen: SocketAddr) -> String {
    let ip = match listen.ip() {
        IpAddr::V4(v4) if v4.is_unspecified() => IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        IpAddr::V6(v6) if v6.is_unspecified() => IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
        other => other,
    };
    format!("http://{}/healthz", SocketAddr::new(ip, listen.port()))
}

/// Probe the server. `Ok` only for HTTP 200 with the body `ok`; the whole
/// request (connect, response, body) must finish within `timeout`.
pub async fn check(listen: SocketAddr, timeout: Duration) -> Result<(), String> {
    crate::tls::install_crypto_provider();
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| format!("cannot build HTTP client: {e}"))?;
    let response = client
        .get(target_url(listen))
        .send()
        .await
        .map_err(|e| format!("health request failed: {}", e.without_url()))?;
    let status = response.status();
    if status != reqwest::StatusCode::OK {
        return Err(format!("unhealthy: HTTP {status}"));
    }
    let body = response
        .text()
        .await
        .map_err(|e| format!("cannot read health response: {}", e.without_url()))?;
    if body.trim() != "ok" {
        return Err("unhealthy: unexpected response body".into());
    }
    Ok(())
}
