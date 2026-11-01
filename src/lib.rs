//! Patina library crate. Integration tests (in `tests/`) can only import a
//! library, so the real code lives here and `main.rs` stays a thin wrapper.
pub mod app;
pub mod auth;
pub mod channels;
pub mod clock;
pub mod config;
pub mod feeds;
pub mod fetch;
pub mod healthcheck;
pub mod http;
pub mod ingest;
pub mod janitor;
pub mod ledger;
pub mod merge;
pub mod metrics;
pub mod model;
pub mod provider;
pub mod scheduler;
pub mod score;
pub mod status;
pub mod store;
pub mod supervise;
pub mod tls;
pub mod xmltv;
