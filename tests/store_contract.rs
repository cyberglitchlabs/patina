//! SQLite runs the shared store contract; see tests/store_postgres.rs for Postgres.

mod common;

use chrono::{TimeZone, Utc};
use common::run_contract;
use patina::model::ProviderId;
use patina::store::connect;
use sqlx::{Connection, SqliteConnection};

#[tokio::test]
async fn sqlite_satisfies_contract() {
    // A temp file (not :memory:) so the test also exercises WAL and the pool.
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
    run_contract(connect(&url).await.unwrap()).await;
}

#[tokio::test]
async fn in_memory_sqlite_works() {
    run_contract(connect("sqlite::memory:").await.unwrap()).await;
}

#[tokio::test]
async fn unsupported_scheme_is_rejected() {
    for url in ["mysql://localhost/x", "http://localhost/x", "nonsense"] {
        let err = connect(url).await.err().expect("error");
        assert!(err.to_string().contains("unsupported"), "{url}: {err}");
    }
}

/// The contract shows bodies come back unchanged; this checks they are
/// really stored compressed (gzip files start with the bytes 1f 8b).
#[tokio::test]
async fn sqlite_raw_bodies_are_stored_gzipped() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
    let store = connect(&url).await.unwrap();
    let body = b"{\"channels\":[]}".repeat(1000);
    let t = Utc.with_ymd_and_hms(2026, 11, 1, 0, 0, 0).unwrap();
    store
        .raw_put(&ProviderId("gn".into()), "ota", t, t, &body)
        .await
        .unwrap();
    let mut conn = SqliteConnection::connect(&url).await.unwrap();
    let stored: Vec<u8> = sqlx::query_scalar("SELECT body_gzip FROM raw_responses")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(&stored[..2], &[0x1f, 0x8b]);
    assert!(
        stored.len() < body.len() / 10,
        "stored {} bytes",
        stored.len()
    );
}
