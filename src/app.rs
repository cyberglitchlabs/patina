//! Startup wiring: turns a loaded [`Config`] into a ready-to-run [`App`].
//!
//! This used to live inline in `main`. It is a library function so the
//! end-to-end tests (tests/e2e_full.rs) can start the REAL wiring with a
//! fake clock and local fake servers, instead of re-assembling the parts by
//! hand and risking that the test wires things differently from production.
//!
//! `build` opens the database, creates the one shared HTTP client, fetch
//! layer, scheduler, janitor, feeds and admin endpoints, and the router, but
//! starts NO background task and binds NO socket. [`App::run`] does that.
//! `main` stays a thin wrapper: config loading, logging, signals, exit codes.

use std::collections::HashMap;
use std::fmt;
use std::future::IntoFuture;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use axum::Router;
use tokio_util::sync::CancellationToken;

use crate::clock::{Clock, SystemClock};
use crate::config::{Config, ProviderKind};
use crate::feeds::{Feeds, FeedsConfig, feed_sources_from_config, source_names};
use crate::fetch::{FetchCfg, FetchLayer};
use crate::http::{AppState, router};
use crate::janitor::Janitor;
use crate::metrics::Metrics;
use crate::model::ProviderId;
use crate::provider::Provider;
use crate::provider::gracenote::{DEFAULT_BASE_URL, GracenoteProvider, http_client};
use crate::scheduler::{ProviderRef, Scheduler, SchedulerDeps};
use crate::status::{ADMIN_STORE_TIMEOUT, Admin, AdminDeps};
use crate::store::Store;
use crate::supervise::{Exit, supervise};

/// Per-request timeout of the provider HTTP client.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// Pause between scheduler passes. Each pass only fetches windows that are
/// due, so a short pause costs no requests; it just notices due windows soon.
const TICK_INTERVAL: Duration = Duration::from_secs(60);

/// What tests may replace. Production uses [`AppDeps::production`].
///
/// The provider base URL and the tuner URL are already plain configuration
/// (`providers.base_url`, `sources.url`), so they need no hook here; the
/// shutdown token is passed to [`App::run`].
pub struct AppDeps {
    /// The time source for budgets, backoff, scheduling and feeds.
    pub clock: Arc<dyn Clock>,
}

impl AppDeps {
    /// The real wall clock.
    pub fn production() -> Self {
        AppDeps {
            clock: Arc::new(SystemClock),
        }
    }

    /// A given clock (tests pass a `FakeClock`).
    pub fn with_clock(clock: Arc<dyn Clock>) -> Self {
        AppDeps { clock }
    }
}

/// Why the application could not be built. Each variant names what `main`
/// logs before exiting with status 1.
#[derive(Debug)]
pub enum BuildError {
    /// A source's overrides file is invalid (`source` is its 0-based index).
    Overrides { source: usize, error: String },
    /// The database could not be opened or migrated.
    Database(String),
    /// The provider HTTP client could not be created.
    HttpClient(String),
    /// A provider's `base_url` is not a URL (config validation prevents it).
    BaseUrl { provider: String, error: String },
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BuildError::Overrides { source, error } => {
                write!(f, "invalid channel overrides (source {source}): {error}")
            }
            BuildError::Database(e) => write!(f, "cannot open database: {e}"),
            BuildError::HttpClient(e) => write!(f, "cannot build HTTP client: {e}"),
            BuildError::BaseUrl { provider, error } => {
                write!(f, "invalid base_url for provider {provider}: {error}")
            }
        }
    }
}

impl std::error::Error for BuildError {}

/// The assembled application. The fields are public so tests can drive the
/// pieces directly (for example `app.scheduler.tick()`); `run` consumes the
/// whole thing.
pub struct App {
    /// All HTTP routes (`/healthz`, `/xmltv`, `/t`, `/status`, `/metrics`).
    pub router: Router,
    /// Fetch, parse and merge; `tick()` runs one pass.
    pub scheduler: Scheduler,
    /// Retention and disk cap; `run_once()` runs one pass.
    pub janitor: Janitor,
    /// The XMLTV feeds. `refresh_channels()` loads the tuner lists now.
    pub feeds: Arc<Feeds>,
    pub store: Arc<dyn Store>,
    pub metrics: Arc<Metrics>,
}

/// Wire everything together. Starts nothing; binds nothing.
pub async fn build(config: Config, deps: AppDeps) -> Result<App, BuildError> {
    // Validate every source's overrides file now, so a typo is a startup
    // failure that names the bad entry rather than a surprise at refresh time.
    // The loaded overrides are kept: the XMLTV feeds use them to match
    // tuner channels.
    let mut source_overrides = Vec::new();
    for (i, source) in config.sources.iter().enumerate() {
        match crate::channels::load_overrides(source.overrides_path.as_deref()) {
            Ok(o) => source_overrides.push(o),
            Err(e) => {
                return Err(BuildError::Overrides {
                    source: i,
                    error: e.to_string(),
                });
            }
        }
    }

    // Open the database and bring the schema up to date before serving
    // anything. A failure here (bad URL, unwritable file, failed migration)
    // is fatal: the caller exits non-zero so the supervisor notices.
    let store = crate::store::connect(&config.database_url)
        .await
        .map_err(|e| BuildError::Database(format!("{e:#}")))?;
    tracing::info!("database ready");

    // Providers, the ONE shared fetch layer, and the scheduler. Exactly one
    // HTTP client and one fetch layer exist per process, so every request to
    // a provider goes through the same budget, backoff and circuit breaker.
    let client = http_client(REQUEST_TIMEOUT).map_err(|e| BuildError::HttpClient(e.to_string()))?;
    let mut providers: Vec<Arc<dyn Provider>> = Vec::new();
    let mut refs = Vec::new();
    let mut daily_budgets = HashMap::new();
    for p in &config.providers {
        let id = ProviderId(p.name.clone());
        let provider: Arc<dyn Provider> = match p.kind {
            ProviderKind::Gracenote => {
                let base = p.base_url.as_deref().unwrap_or(DEFAULT_BASE_URL);
                // Validated by Config::load, so this only fails on a bug.
                let base = url::Url::parse(base).map_err(|e| BuildError::BaseUrl {
                    provider: p.name.clone(),
                    error: e.to_string(),
                })?;
                Arc::new(GracenoteProvider::new_with_id(
                    id.clone(),
                    client.clone(),
                    base,
                ))
            }
        };
        providers.push(provider);
        daily_budgets.insert(id.clone(), p.daily_budget);
        refs.push(ProviderRef {
            id,
            lineups: p.lineups.clone(),
        });
    }
    let clock = deps.clock;
    // Bumped by the scheduler after each merge; the feed cache watches it.
    let feed_generation = Arc::new(AtomicU64::new(0));
    let feeds = Arc::new(Feeds::new(FeedsConfig {
        store: store.clone(),
        clock: clock.clone(),
        tz: config.timezone,
        horizon_days: config.horizon_days,
        token: config.feed_token.clone(),
        sources: feed_sources_from_config(&config, source_overrides, client.clone()),
        generation: feed_generation.clone(),
    }));
    let fetch_cfg = FetchCfg {
        daily_budgets,
        request_timeout: REQUEST_TIMEOUT,
        ..FetchCfg::default()
    };
    // Metrics only ever carry the configured provider and source names.
    let provider_ids: Vec<ProviderId> = refs.iter().map(|r| r.id.clone()).collect();
    let metrics = Arc::new(Metrics::new(
        env!("CARGO_PKG_VERSION"),
        &provider_ids,
        &source_names(&config.sources),
    ));
    let fetch = Arc::new(
        FetchLayer::new(store.clone(), providers, fetch_cfg, clock.clone())
            .with_metrics(metrics.clone()),
    );
    let scheduler = Scheduler::new(
        SchedulerDeps {
            store: store.clone(),
            fetch: fetch.clone(),
            providers: refs,
            horizon_days: config.horizon_days,
            weights: config.scoring.clone(),
            tick_interval: TICK_INTERVAL,
            feed_generation: Some(feed_generation.clone()),
        },
        clock.clone(),
    )
    .with_metrics(metrics.clone());
    // The janitor (retention and disk cap). Its latest report is shared with
    // `/status` through `janitor_report`.
    let janitor_report: crate::janitor::ReportSink = Arc::new(std::sync::RwLock::new(None));
    let janitor = Janitor::new(
        store.clone(),
        config.retention.clone(),
        config.disk_cap_bytes,
        clock.clone(),
    )
    .with_metrics(metrics.clone())
    .with_report_sink(janitor_report.clone());
    let admin = Arc::new(
        Admin::new(AdminDeps {
            token: config.admin_token.clone(),
            store: store.clone(),
            backend: crate::store::backend_of(&config.database_url),
            fetch,
            feeds: feeds.clone(),
            scheduler: scheduler.status_handle(),
            metrics: metrics.clone(),
            clock,
            providers: provider_ids,
            version: env!("CARGO_PKG_VERSION"),
            store_timeout: ADMIN_STORE_TIMEOUT,
        })
        .with_janitor(janitor_report),
    );

    let router = router(AppState::new(feeds.clone()).with_admin(admin));
    Ok(App {
        router,
        scheduler,
        janitor,
        feeds,
        store,
        metrics,
    })
}

impl App {
    /// Start the background tasks and serve on `listener` until `shutdown`
    /// is cancelled or either the scheduler or the server dies.
    ///
    /// One token tells everything to stop. The scheduler finishes the window
    /// it is merging (it never stops half-way through applying a plan).
    pub async fn run(self, listener: tokio::net::TcpListener, shutdown: CancellationToken) -> Exit {
        let scheduler_task = tokio::spawn(self.scheduler.run(shutdown.clone()));
        // The janitor is NOT supervised like the scheduler: the guide stays
        // correct without it (it only trims old data), so if its task ever
        // panics we log one ERROR and keep serving instead of taking the whole
        // process down. Nothing restarts it; the next process start does.
        let janitor_task = tokio::spawn(self.janitor.run(shutdown.clone()));
        tokio::spawn(async move {
            if let Err(e) = janitor_task.await
                && e.is_panic()
            {
                tracing::error!(
                    "janitor task panicked; old data will not be cleaned up until restart"
                );
            }
        });
        // Tuner channel lists are reloaded in the background, never in a request.
        tokio::spawn(self.feeds.clone().run_refresher(shutdown.clone()));

        let server = axum::serve(listener, self.router)
            .with_graceful_shutdown(shutdown.clone().cancelled_owned())
            .into_future();
        // Runs until shutdown, or until either half dies. If the scheduler dies
        // on its own the server is stopped too and the result is a failure, so
        // the supervisor restarts us instead of serving a guide nobody refreshes.
        supervise(scheduler_task, server, shutdown).await
    }
}
