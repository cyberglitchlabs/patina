//! PostgreSQL as a [`Backend`] for the generic [`SqlStore`](super::sql::SqlStore).

use std::borrow::Cow;

use anyhow::{Context, Result};
use async_trait::async_trait;
use sqlx::postgres::{PgPoolOptions, PgQueryResult};
use sqlx::{Pool, Postgres, Transaction};

use super::sql::{Backend, SqlStore};

/// The store on PostgreSQL.
pub type PostgresStore = SqlStore<Postgres>;

#[async_trait]
impl Backend for Postgres {
    async fn connect(database_url: &str) -> Result<Pool<Postgres>> {
        PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .context("cannot connect to the PostgreSQL database")
    }

    async fn migrate(pool: &Pool<Postgres>) -> Result<()> {
        // Migration files are compiled into the binary; sqlx records which ones
        // ran (in its _sqlx_migrations table), so calling this again is a no-op.
        sqlx::migrate!("migrations/postgres").run(pool).await?;
        Ok(())
    }

    /// PostgreSQL numbers its placeholders: `$1`, `$2`, ...
    fn sql(sql: &'static str) -> Cow<'static, str> {
        Cow::Owned(super::dialect::numbered(sql))
    }

    async fn snapshot(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
        // The default (READ COMMITTED) gives each statement its own snapshot;
        // REPEATABLE READ makes the reads in this transaction share one.
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    fn rows_affected(result: &PgQueryResult) -> u64 {
        result.rows_affected()
    }

    async fn size_bytes(pool: &Pool<Postgres>) -> Result<u64> {
        // Includes dead space: PostgreSQL reuses space freed by deletes for
        // new rows but does not hand it back to the OS, so this number does
        // not fall right after a delete. The janitor's disk eviction notices
        // that (see `Janitor`).
        let bytes: i64 = sqlx::query_scalar("SELECT pg_database_size(current_database())")
            .fetch_one(pool)
            .await?;
        Ok(u64::try_from(bytes).unwrap_or(0))
    }
}
