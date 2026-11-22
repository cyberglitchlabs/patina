//! SQLite as a [`Backend`] for the generic [`SqlStore`](super::sql::SqlStore).

use std::borrow::Cow;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteQueryResult};
use sqlx::{Pool, Sqlite, Transaction};

use super::sql::{Backend, SqlStore};
use crate::config::redact_url;

/// The store on SQLite (the default).
pub type SqliteStore = SqlStore<Sqlite>;

#[async_trait]
impl Backend for Sqlite {
    async fn connect(database_url: &str) -> Result<Pool<Sqlite>> {
        let options = SqliteConnectOptions::from_str(database_url)
            // Redacted: an error message may end up in logs or on screen.
            .with_context(|| format!("invalid sqlite URL {:?}", redact_url(database_url)))?
            // WAL lets readers work while a write is in progress.
            .journal_mode(SqliteJournalMode::Wal)
            // SQLite ignores foreign keys unless asked, per connection.
            .foreign_keys(true)
            // Wait up to 5s for a lock instead of failing at once.
            .busy_timeout(Duration::from_secs(5));
        // Each connection to ":memory:" would get its own empty database, so
        // in-memory use is limited to a single shared connection.
        let max_connections = if database_url.contains(":memory:") {
            1
        } else {
            5
        };
        SqlitePoolOptions::new()
            .max_connections(max_connections)
            .connect_with(options)
            .await
            .with_context(|| format!("cannot open database {:?}", redact_url(database_url)))
    }

    async fn migrate(pool: &Pool<Sqlite>) -> Result<()> {
        // The migration files are compiled into the binary. sqlx records which
        // ones ran, so calling this again does nothing.
        sqlx::migrate!("migrations/sqlite").run(pool).await?;
        Ok(())
    }

    fn sql(sql: &'static str) -> Cow<'static, str> {
        Cow::Borrowed(sql)
    }

    async fn snapshot(_tx: &mut Transaction<'_, Sqlite>) -> Result<()> {
        // A SQLite transaction already reads one snapshot.
        Ok(())
    }

    fn rows_affected(result: &SqliteQueryResult) -> u64 {
        result.rows_affected()
    }

    async fn size_bytes(pool: &Pool<Sqlite>) -> Result<u64> {
        // One connection for all three pragmas so they describe the same
        // moment. Pages on the freelist hold no data (deleted rows), so they
        // do not count as used.
        let mut conn = pool.acquire().await?;
        let pages: i64 = sqlx::query_scalar("PRAGMA page_count")
            .fetch_one(&mut *conn)
            .await?;
        let free: i64 = sqlx::query_scalar("PRAGMA freelist_count")
            .fetch_one(&mut *conn)
            .await?;
        let page_size: i64 = sqlx::query_scalar("PRAGMA page_size")
            .fetch_one(&mut *conn)
            .await?;
        Ok(u64::try_from((pages - free).max(0) * page_size).unwrap_or(0))
    }
}
