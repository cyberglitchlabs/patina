//! PostgreSQL test helpers: run a test against a brand-new schema.
//!
//! Needs a server: set PATINA_TEST_POSTGRES_URL (e.g.
//! postgres://postgres:postgres@localhost:55432/postgres). Without it the test
//! is skipped with a notice, unless PATINA_REQUIRE_POSTGRES=1, which makes a
//! missing URL a failure (CI sets it so the test can never silently vanish).

use sqlx::{AssertSqlSafe, Connection, PgConnection};

/// Returns the test server URL, or None when the test should be skipped.
pub fn test_url() -> Option<String> {
    match std::env::var("PATINA_TEST_POSTGRES_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            if std::env::var("PATINA_REQUIRE_POSTGRES").as_deref() == Ok("1") {
                panic!("PATINA_REQUIRE_POSTGRES=1 but PATINA_TEST_POSTGRES_URL is not set");
            }
            eprintln!("SKIPPED: PATINA_TEST_POSTGRES_URL not set; Postgres tests did not run");
            None
        }
    }
}

/// A schema is a namespace inside one database. A random one per run keeps
/// repeated runs and parallel test files from seeing each other's tables.
fn random_schema() -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    // Tests in this file run in parallel and the clock may tick coarsely, so a
    // counter guarantees two of them never get the same name.
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "patina_test_{}_{nanos}_{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// Runs `test` against a store in a brand-new schema, which is dropped
/// afterwards even if the test fails. `test` gets the schema's connection URL.
pub async fn with_fresh_schema<F, Fut>(test: F)
where
    F: FnOnce(String) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let Some(base) = test_url() else { return };
    let schema = random_schema();

    let mut admin = PgConnection::connect(&base).await.unwrap();
    // The schema name is generated above (letters, digits, underscores), so
    // building SQL text from it is safe; sqlx makes us say so explicitly.
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&mut admin)
        .await
        .unwrap();

    // `options=-c search_path=...` makes every connection in the store's pool
    // use our schema, so the store code itself needs no test-only changes.
    let sep = if base.contains('?') { '&' } else { '?' };
    let url = format!("{base}{sep}options=-c%20search_path%3D{schema}");

    // Spawned so a panic inside the test is caught here and cleanup still runs.
    let result = tokio::spawn(test(url)).await;

    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&mut admin)
        .await
        .unwrap();
    result.unwrap();
}
