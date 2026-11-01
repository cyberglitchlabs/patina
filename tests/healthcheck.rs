//! The `patina healthcheck` subcommand's probe, run against real servers on
//! 127.0.0.1:0 (a free port chosen by the OS).

use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;
use patina::healthcheck::{check, target_url};
use patina::http::{AppState, router};

const TIMEOUT: Duration = Duration::from_secs(3);

/// Serve `app` on a free local port and return its address.
async fn serve(app: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

#[tokio::test]
async fn healthy_with_the_real_router() {
    let addr = serve(router(AppState::default())).await;
    check(addr, TIMEOUT).await.expect("real router is healthy");
}

#[tokio::test]
async fn unhealthy_on_non_200() {
    let app = Router::new().route(
        "/healthz",
        get(|| async { (StatusCode::INTERNAL_SERVER_ERROR, "ok") }),
    );
    let addr = serve(app).await;
    assert!(check(addr, TIMEOUT).await.is_err());
}

#[tokio::test]
async fn unhealthy_on_wrong_body() {
    let app = Router::new().route("/healthz", get(|| async { "not ok" }));
    let addr = serve(app).await;
    assert!(check(addr, TIMEOUT).await.is_err());
}

#[tokio::test]
async fn unhealthy_when_nothing_listens() {
    // Bind then drop to get a port that is (almost certainly) closed.
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    assert!(check(addr, TIMEOUT).await.is_err());
}

#[tokio::test]
async fn unhealthy_when_the_server_hangs() {
    let app = Router::new().route(
        "/healthz",
        get(|| async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            "ok"
        }),
    );
    let addr = serve(app).await;
    let started = std::time::Instant::now();
    assert!(check(addr, Duration::from_millis(300)).await.is_err());
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "timeout applies"
    );
}

#[test]
fn wildcard_listen_addresses_probe_loopback() {
    let v4: SocketAddr = "0.0.0.0:8080".parse().unwrap();
    assert_eq!(target_url(v4), "http://127.0.0.1:8080/healthz");
    let v6: SocketAddr = "[::]:9000".parse().unwrap();
    assert_eq!(target_url(v6), "http://[::1]:9000/healthz");
    let specific: SocketAddr = "192.168.1.5:80".parse().unwrap();
    assert_eq!(target_url(specific), "http://192.168.1.5:80/healthz");
}
