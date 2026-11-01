//! SQLite schema checks with raw SQL on a second connection to the database
//! file. Slot read-back (range filter, mapping, image join) is in the shared
//! store contract (tests/common/mod.rs), which writes through `apply_merge`.

use patina::store::connect;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, SqliteConnection};

/// `apply_merge` upserts observations with (slot_id, provider) as the
/// conflict target, so that pair must be unique; checked here with raw SQL.
#[tokio::test]
async fn observation_rows_are_unique_per_slot_and_provider() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("obs.db");
    connect(&format!("sqlite://{}?mode=rwc", path.display()))
        .await
        .unwrap();
    let mut conn = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(&path)
            .foreign_keys(true),
    )
    .await
    .unwrap();

    sqlx::query("INSERT INTO channels (id, number, name, call_sign) VALUES ('C', '4.1', 'C', 'C')")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO slots (id, channel_id, start_utc, end_utc, title, winner_provider, score, updated_at) \
         VALUES (1, 'C', 100, 200, 'Show', 'a', 0.5, 0)",
    )
    .execute(&mut conn)
    .await
    .unwrap();

    let insert = |provider: &'static str| {
        sqlx::query(
            "INSERT INTO observations (slot_id, provider, start_utc, end_utc, title, fetched_at) \
             VALUES (1, ?, 100, 200, 'Show', 0)",
        )
        .bind(provider)
    };
    insert("a").execute(&mut conn).await.unwrap();
    // Same (slot, provider) again: the constraint must reject it.
    assert!(insert("a").execute(&mut conn).await.is_err());
    // A different provider on the same slot is fine.
    insert("b").execute(&mut conn).await.unwrap();
}

/// `window_state.last_error` holds a typed `WindowFailure` as a stable code.
/// A row whose text is not a known code (written before the codes existed,
/// or by hand) must read back as a generic failure: no panic, no error, and
/// above all not as `MergeFailed` (which would delay the window's refresh).
#[tokio::test]
async fn window_state_with_unknown_error_text_reads_as_a_generic_failure() {
    use chrono::{TimeZone, Utc};
    use patina::model::ProviderId;
    use patina::store::WindowFailure;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ws.db");
    let store = connect(&format!("sqlite://{}?mode=rwc", path.display()))
        .await
        .unwrap();
    let mut conn = SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(&path))
        .await
        .unwrap();
    let w = Utc.with_ymd_and_hms(2026, 11, 1, 0, 0, 0).unwrap();
    for (i, text) in [
        "storing the merge failed", // the old English sentinel
        "HTTP 400 (bad window)",
        "merge_failed: with a detail it never has",
        "",
    ]
    .into_iter()
    .enumerate()
    {
        let start = w + chrono::TimeDelta::hours(6 * i as i64);
        sqlx::query(
            "INSERT INTO window_state (provider, lineup, window_start, last_success, last_attempt, last_error) \
             VALUES ('gn', 'ota', ?, NULL, ?, ?)",
        )
        .bind(start.timestamp())
        .bind(start.timestamp())
        .bind(text)
        .execute(&mut conn)
        .await
        .unwrap();
        let st = store
            .window_state_get(&ProviderId("gn".into()), "ota", start)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            st.last_failure,
            Some(WindowFailure::Failed(text.to_string())),
            "{text:?}"
        );
    }
}
