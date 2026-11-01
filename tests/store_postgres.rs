//! Runs the shared store contract against PostgreSQL.
//!
//! Needs a server: set PATINA_TEST_POSTGRES_URL (e.g.
//! postgres://postgres:postgres@localhost:55432/postgres). Without it the test
//! is skipped with a notice, unless PATINA_REQUIRE_POSTGRES=1, which makes a
//! missing URL a failure (CI sets it so the test can never silently vanish).

mod common;

use chrono::{TimeZone, Utc};
use common::pg::with_fresh_schema;
use common::run_contract;
use patina::store::connect;
use sqlx::{Connection, PgConnection};

#[tokio::test]
async fn postgres_satisfies_contract() {
    with_fresh_schema(|url| async move { run_contract(connect(&url).await.unwrap()).await }).await;
}

/// `apply_merge` upserts observations on (slot_id, provider), so that pair
/// must be unique. (Slot read-back is in the shared contract.)
#[tokio::test]
async fn postgres_observations_are_unique_per_slot_and_provider() {
    with_fresh_schema(|url| async move {
        connect(&url).await.unwrap();
        let mut conn = PgConnection::connect(&url).await.unwrap();
        sqlx::query(
            "INSERT INTO channels (id, number, name, call_sign) VALUES ('C', '4.1', 'C', 'C')",
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO slots (channel_id, start_utc, end_utc, title, winner_provider, score, updated_at) \
             VALUES ('C', 100, 200, 'Show', 'a', 0.5, 0)",
        )
        .execute(&mut conn)
        .await
        .unwrap();

        let insert = |provider: &'static str| {
            sqlx::query(
                "INSERT INTO observations (slot_id, provider, start_utc, end_utc, title, fetched_at) \
                 VALUES ((SELECT id FROM slots), $1, 100, 200, 'Show', 0)",
            )
            .bind(provider)
        };
        insert("a").execute(&mut conn).await.unwrap();
        assert!(insert("a").execute(&mut conn).await.is_err());
        insert("b").execute(&mut conn).await.unwrap();
    })
    .await;
}

/// Raw bodies are stored gzip-compressed (the contract only shows they come
/// back unchanged).
#[tokio::test]
async fn postgres_raw_bodies_are_stored_gzipped() {
    with_fresh_schema(|url| async move {
        let store = connect(&url).await.unwrap();
        let body = b"{\"channels\":[]}".repeat(1000);
        let t = Utc.with_ymd_and_hms(2026, 11, 1, 0, 0, 0).unwrap();
        store
            .raw_put(&patina::model::ProviderId("gn".into()), "ota", t, t, &body)
            .await
            .unwrap();
        let mut conn = PgConnection::connect(&url).await.unwrap();
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
    })
    .await;
}
