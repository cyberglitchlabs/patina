//! Postgres connections that demand TLS must really try TLS.
//!
//! Without a rustls feature in sqlx, `sslmode=require` fails before touching
//! the network ("built without TLS support"). These tests use a fake server
//! that refuses the TLS request, so no real Postgres is needed: the error must
//! come from the SERVER REFUSING TLS, which proves the client got as far as
//! sending the TLS request.

use std::io::{Read, Write};
use std::net::TcpListener;

/// A fake Postgres that answers the first message (the 8-byte SSLRequest)
/// with 'N' ("no TLS here"), then hangs up.
fn refuses_tls() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        if let Ok((mut sock, _)) = listener.accept() {
            let mut request = [0u8; 8];
            let _ = sock.read_exact(&mut request);
            let _ = sock.write_all(b"N");
        }
    });
    port
}

#[tokio::test]
async fn sslmode_require_attempts_tls() {
    let port = refuses_tls();
    let url = format!("postgres://u:p@127.0.0.1:{port}/db?sslmode=require");
    let err = match patina::store::connect(&url).await {
        Ok(_) => panic!("a server that refuses TLS must make sslmode=require fail"),
        Err(e) => format!("{e:#}"),
    };
    assert!(
        !err.to_lowercase().contains("without tls support"),
        "sqlx has no TLS feature compiled in: {err}"
    );
    assert!(
        err.to_lowercase().contains("does not support tls"),
        "expected the server's refusal of TLS, got: {err}"
    );
}

/// With a real server, `prefer` still works whether or not it offers TLS.
#[tokio::test]
async fn sslmode_prefer_connects_to_a_real_server() {
    // Same rule as the other Postgres tests: skip with a notice, or fail
    // when PATINA_REQUIRE_POSTGRES=1.
    let Some(base) = std::env::var("PATINA_TEST_POSTGRES_URL")
        .ok()
        .filter(|u| !u.is_empty())
    else {
        if std::env::var("PATINA_REQUIRE_POSTGRES").as_deref() == Ok("1") {
            panic!("PATINA_REQUIRE_POSTGRES=1 but PATINA_TEST_POSTGRES_URL is not set");
        }
        eprintln!("SKIPPED: PATINA_TEST_POSTGRES_URL not set");
        return;
    };
    let sep = if base.contains('?') { '&' } else { '?' };
    let url = format!("{base}{sep}sslmode=prefer");
    // Connect only (no migrations: other tests own the schema).
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(&url).await.expect("connect");
    conn.ping().await.expect("ping");
}
