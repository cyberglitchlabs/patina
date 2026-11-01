//! TLS crypto provider setup.
//!
//! Patina uses the `ring` provider for every TLS connection (HTTPS to guide
//! providers via reqwest, Postgres via sqlx). `ring` builds with just a C
//! compiler, unlike `aws-lc-rs`, which can also need cmake. reqwest is built
//! with its `rustls-no-provider` feature, which means we must say which
//! provider to use before building any client; this does that once.

use std::sync::Once;

/// Install `ring` as the process-wide rustls provider. Safe to call many
/// times, from any thread; only the first call does anything.
pub fn install_crypto_provider() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        // Err means another provider is already installed, which is fine.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}
