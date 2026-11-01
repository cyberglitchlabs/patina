use std::collections::HashMap;
use std::time::Duration;

use patina::app::{AppDeps, BuildError};
use patina::config::{Config, EmptyTokenNotice};
use patina::supervise::Exit;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

const USAGE: &str = "Usage: patina [healthcheck | --help | --version]

With no arguments, patina runs the server (configured by PATINA_* environment
variables and the optional TOML file named by PATINA_CONFIG).

  healthcheck   probe the running server's /healthz (3 s timeout); exit 0 if
                it answers 200 \"ok\", else 1. Used by the container HEALTHCHECK.
  --help        show this help
  --version     show the version";

/// How long `patina healthcheck` waits for the server.
const HEALTHCHECK_TIMEOUT: Duration = Duration::from_secs(3);

#[tokio::main]
async fn main() {
    // Gather settings. Config loading happens before logging starts, so
    // problems are reported on stderr.
    let env: HashMap<String, String> = std::env::vars().collect();
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        None => {}
        Some("healthcheck") if args.len() == 1 => {
            std::process::exit(run_healthcheck(&env).await);
        }
        Some("--help" | "-h") => {
            println!("{USAGE}");
            return;
        }
        Some("--version" | "-V") => {
            println!("patina {}", env!("CARGO_PKG_VERSION"));
            return;
        }
        Some(_) => {
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    }
    // PATINA_CONFIG names an optional TOML file; we read it here and hand the
    // text to `Config::load`, which keeps the loader free of file access.
    let toml_text = match env.get("PATINA_CONFIG").filter(|p| !p.is_empty()) {
        Some(path) => match std::fs::read_to_string(path) {
            Ok(t) => Some(t),
            Err(e) => {
                eprintln!("cannot read config file {path}: {e}");
                std::process::exit(1);
            }
        },
        None => None,
    };
    let config = match Config::load(&env, toml_text.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("configuration error: {e}");
            std::process::exit(1);
        }
    };

    // The log setting is a filter such as "debug" or "patina=debug,info";
    // falls back to "info" if invalid.
    let filter = EnvFilter::try_new(&config.log).unwrap_or_else(|_| EnvFilter::new("info"));
    // One JSON object per log line, easy for log collectors to parse.
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .init();
    tracing::info!(config = %config.redacted(), "configuration loaded");
    // Say plainly whether the endpoints are protected (values never logged).
    let enabled = |t: &Option<String>| if t.is_some() { "enabled" } else { "disabled" };
    tracing::info!(
        admin_auth = enabled(&config.admin_token),
        feed_auth = enabled(&config.feed_token),
        "endpoint authentication"
    );
    for notice in patina::config::empty_token_notices(&env, &config) {
        match notice {
            EmptyTokenNotice::ConfigFileTokenUsed(var) => tracing::info!(
                variable = var,
                "token variable is set but empty, so it is ignored; the token from the config file is used"
            ),
            EmptyTokenNotice::Open(var) => tracing::warn!(
                variable = var,
                "token variable is set but empty, so it is ignored; no token from the config file is set either, so these endpoints are OPEN"
            ),
        }
    }

    // Wire everything (overrides, database, providers, scheduler, feeds,
    // janitor, routes). A failure here is fatal: log it and exit non-zero so
    // the supervisor notices. Nothing is running yet.
    let app = match patina::app::build(config.clone(), AppDeps::production()).await {
        Ok(app) => app,
        Err(e) => {
            match &e {
                BuildError::Overrides { source, error } => {
                    tracing::error!(source = *source, error = %error, "invalid channel overrides");
                }
                BuildError::Database(error) => {
                    tracing::error!(error = %error, "cannot open database");
                }
                BuildError::HttpClient(error) => {
                    tracing::error!(error = %error, "cannot build HTTP client");
                }
                BuildError::BaseUrl { provider, error } => {
                    tracing::error!(provider = %provider, error = %error, "invalid base_url");
                }
            }
            std::process::exit(1);
        }
    };

    // One token tells everything to stop (see `App::run`).
    let shutdown = CancellationToken::new();
    tokio::spawn(cancel_on_signal(shutdown.clone()));

    let listen = config.listen;
    let listener = match tokio::net::TcpListener::bind(listen).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(%listen, error = %e, "cannot bind listen address");
            std::process::exit(1);
        }
    };
    tracing::info!(%listen, "patina listening");

    match app.run(listener, shutdown).await {
        Exit::Clean => tracing::info!("patina stopped"),
        Exit::Failed(why) => {
            tracing::error!(error = %why, "patina stopping after a failure");
            std::process::exit(1);
        }
    }
}

/// Cancel `shutdown` on Ctrl-C or (on Unix) SIGTERM, which is what container
/// runtimes send to stop a process.
async fn cancel_on_signal(shutdown: CancellationToken) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    tracing::info!("shutdown signal received");
    shutdown.cancel();
}

/// `patina healthcheck`: returns the process exit code. Reads the listen
/// address the same way the server does (so `PATINA_LISTEN` or the TOML file
/// both work) and prints only a short reason on failure, never configuration.
async fn run_healthcheck(env: &HashMap<String, String>) -> i32 {
    let toml_text = match env.get("PATINA_CONFIG").filter(|p| !p.is_empty()) {
        Some(path) => match std::fs::read_to_string(path) {
            Ok(t) => Some(t),
            Err(e) => {
                eprintln!("cannot read config file: {e}");
                return 1;
            }
        },
        None => None,
    };
    let config = match Config::load(env, toml_text.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("configuration error: {e}");
            return 1;
        }
    };
    match patina::healthcheck::check(config.listen, HEALTHCHECK_TIMEOUT).await {
        Ok(()) => 0,
        Err(why) => {
            eprintln!("{why}");
            1
        }
    }
}
