//! The production HTTP client must build with its TLS stack. In reqwest the
//! TLS configuration (crypto provider, certificate verifier) is created at
//! `build()` time, so a broken provider setup panics or errors here, with no
//! network access needed.

use std::time::Duration;

use patina::provider::gracenote::http_client;

#[test]
fn production_https_client_builds() {
    http_client(Duration::from_secs(60)).expect("client with rustls builds");
}

#[tokio::test]
async fn production_client_rejects_a_non_tls_peer_over_https() {
    // Proves the client really speaks TLS: an https request to a plain-text
    // local listener fails at the handshake rather than succeeding or panicking.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        if let Ok((mut s, _)) = listener.accept() {
            use std::io::Write;
            let _ = s.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nhi");
        }
    });
    let client = http_client(Duration::from_secs(5)).unwrap();
    let result = client
        .get(format!("https://127.0.0.1:{port}/"))
        .send()
        .await;
    assert!(
        result.is_err(),
        "plain-text peer must fail the TLS handshake"
    );
}
