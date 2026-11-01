//! Configuration: environment variables for simple settings plus one optional
//! TOML file for structured ones (providers, lineups, sources, scoring).
//! Environment wins over the file. Anything invalid is an error, so the
//! service refuses to start instead of running half-configured.
//!
//! `Config::load` takes the environment as a plain map instead of reading the
//! process environment itself. That keeps tests free of global state; only
//! `main` builds the map from `std::env::vars()`.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use chrono_tz::Tz;
use serde::Deserialize;

/// Everything that can go wrong while loading configuration. Each message is
/// meant to be read by a human, so it names the offending field or variable.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// TOML syntax or structure problem. The toml crate's message already
    /// includes the line number and a snippet of the offending text.
    #[error("invalid config file: {0}")]
    Toml(#[from] toml::de::Error),
    /// An environment variable has a value we cannot use.
    #[error("invalid environment variable {var}: {reason}")]
    Env { var: String, reason: String },
    /// A value (from env or TOML) is out of range or inconsistent.
    #[error("invalid config: {0}")]
    Invalid(String),
}

/// How long to keep each kind of data, in days. All must be at least 1.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Retention {
    pub images_days: u32,
    pub raw_days: u32,
    pub programmes_days: u32,
    pub history_days: u32,
}

impl Default for Retention {
    fn default() -> Self {
        Retention {
            images_days: 3,
            raw_days: 7,
            programmes_days: 7,
            history_days: 90,
        }
    }
}

/// Weights for the quality score signals. Higher weight, more influence.
/// Each must be >= 0 and at least one must be > 0; the score divides by the
/// sum, so the weights do not need to add up to 1.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Weights {
    pub freshness: f64,
    pub completeness: f64,
    pub timeline_sanity: f64,
    pub stability: f64,
    pub reliability: f64,
    pub hindsight_accuracy: f64,
    pub agreement: f64,
}

impl Default for Weights {
    fn default() -> Self {
        Weights {
            freshness: 0.25,
            completeness: 0.2,
            timeline_sanity: 0.2,
            stability: 0.15,
            reliability: 0.15,
            hindsight_accuracy: 0.05,
            agreement: 0.0,
        }
    }
}

impl Weights {
    fn validate(&self) -> Result<(), ConfigError> {
        let all = [
            ("freshness", self.freshness),
            ("completeness", self.completeness),
            ("timeline_sanity", self.timeline_sanity),
            ("stability", self.stability),
            ("reliability", self.reliability),
            ("hindsight_accuracy", self.hindsight_accuracy),
            ("agreement", self.agreement),
        ];
        for (name, w) in all {
            // `is_finite` rejects NaN and infinity; then check the sign.
            if !w.is_finite() || w < 0.0 {
                return Err(ConfigError::Invalid(format!(
                    "scoring.{name} must be a number >= 0 (got {w})"
                )));
            }
        }
        if all.iter().map(|(_, w)| w).sum::<f64>() <= 0.0 {
            return Err(ConfigError::Invalid(
                "scoring weights must not all be zero".into(),
            ));
        }
        Ok(())
    }
}

/// One lineup (a channel list for a place) under a provider.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineupCfg {
    /// Local nickname; sources refer to lineups by this name.
    pub name: String,
    pub lineup_id: String,
    pub postal_code: String,
    pub country: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderKind {
    Gracenote,
}

fn default_budget() -> u32 {
    150
}

/// A guide provider and the lineups to fetch from it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCfg {
    pub kind: ProviderKind,
    pub name: String,
    /// Hard cap on requests per day; must be at least 1.
    #[serde(default = "default_budget")]
    pub daily_budget: u32,
    /// Where the provider's API lives (http or https). Unset means the
    /// provider's real address (Gracenote: https://tvlistings.gracenote.com).
    /// Mainly for pointing at a local fake in testing.
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub lineups: Vec<LineupCfg>,
}

/// A channel declared by hand in a `static` source.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticChannel {
    pub number: String,
    pub name: String,
    pub callsign: Option<String>,
}

/// What kind of source this is, with the settings only that kind has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceKind {
    /// An HDHomeRun tuner, queried at `url` for its channel list.
    HdHomeRun { url: String },
    /// A fixed channel list written in the config.
    Static { channels: Vec<StaticChannel> },
}

/// A channel source: says which channels exist and which lineups to match
/// them against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceCfg {
    pub kind: SourceKind,
    /// Names of lineups (from `providers.lineups`) to match against.
    pub lineups: Vec<String>,
    pub overrides_path: Option<PathBuf>,
    /// Keep channels that match no lineup instead of dropping them.
    pub include_unmatched: bool,
}

/// The file's shape for a source. All kinds share one flat table in TOML, so
/// we read it loosely and then check which fields the chosen kind needs.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum RawSourceKind {
    Hdhomerun,
    Static,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSource {
    kind: RawSourceKind,
    url: Option<String>,
    channels: Option<Vec<StaticChannel>>,
    #[serde(default)]
    lineups: Vec<String>,
    overrides_path: Option<PathBuf>,
    #[serde(default)]
    include_unmatched: bool,
}

impl RawSourceKind {
    fn label(&self) -> &'static str {
        match self {
            RawSourceKind::Hdhomerun => "hdhomerun",
            RawSourceKind::Static => "static",
        }
    }
}

impl RawSource {
    fn into_cfg(self) -> Result<SourceCfg, ConfigError> {
        let kind = match self.kind {
            RawSourceKind::Hdhomerun => {
                if self.channels.is_some() {
                    return Err(ConfigError::Invalid(
                        "must not set 'channels' (that is for 'static')".into(),
                    ));
                }
                let url = self
                    .url
                    .filter(|u| !u.is_empty())
                    .ok_or_else(|| ConfigError::Invalid("requires 'url'".into()))?;
                SourceKind::HdHomeRun { url }
            }
            RawSourceKind::Static => {
                if self.url.is_some() {
                    return Err(ConfigError::Invalid(
                        "must not set 'url' (that is for 'hdhomerun')".into(),
                    ));
                }
                let channels = self
                    .channels
                    .ok_or_else(|| ConfigError::Invalid("requires 'channels'".into()))?;
                if channels.is_empty() {
                    return Err(ConfigError::Invalid(
                        "'channels' must list at least one channel".into(),
                    ));
                }
                SourceKind::Static { channels }
            }
        };
        Ok(SourceCfg {
            kind,
            lineups: self.lineups,
            overrides_path: self.overrides_path,
            include_unmatched: self.include_unmatched,
        })
    }
}

/// The TOML file's top level. Every key is optional; env may fill them too.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawConfig {
    listen: Option<String>,
    log: Option<String>,
    database_url: Option<String>,
    timezone: Option<String>,
    feed_token: Option<String>,
    admin_token: Option<String>,
    horizon_days: Option<u32>,
    disk_cap_bytes: Option<u64>,
    retention: Retention,
    providers: Vec<ProviderCfg>,
    sources: Vec<RawSource>,
    scoring: Weights,
}

/// Fully validated configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub listen: SocketAddr,
    /// A tracing filter such as "info" or "patina=debug,info".
    pub log: String,
    pub database_url: String,
    pub timezone: Tz,
    /// If set, XMLTV feed URLs require this token.
    pub feed_token: Option<String>,
    /// If set, /status and /metrics require this as a bearer token.
    pub admin_token: Option<String>,
    pub retention: Retention,
    /// If set, the janitor evicts oldest data first to stay under this size.
    pub disk_cap_bytes: Option<u64>,
    pub providers: Vec<ProviderCfg>,
    pub sources: Vec<SourceCfg>,
    pub scoring: Weights,
    /// How many days ahead to keep the guide filled.
    pub horizon_days: u32,
}

/// The longest guide horizon `horizon_days` may ask for, in days.
pub const MAX_HORIZON_DAYS: u32 = 14;

/// Parse an env var with `FromStr`, naming the variable if it fails.
fn env_parse<T: FromStr>(env: &HashMap<String, String>, var: &str) -> Result<Option<T>, ConfigError>
where
    T::Err: std::fmt::Display,
{
    match env.get(var) {
        None => Ok(None),
        Some(s) => s
            .trim()
            .parse::<T>()
            .map(Some)
            .map_err(|e| ConfigError::Env {
                var: var.to_string(),
                reason: format!("{e} (got '{s}')"),
            }),
    }
}

/// A string env var; empty counts as unset (common with templated deployments).
fn env_str(env: &HashMap<String, String>, var: &str) -> Option<String> {
    env.get(var).filter(|s| !s.is_empty()).cloned()
}

/// Token variables that are present but empty. An empty value silently
/// means "no token" (open endpoints), which is rarely what someone who set
/// the variable intended, so startup warns about each one.
pub fn empty_token_vars(env: &HashMap<String, String>) -> Vec<&'static str> {
    ["PATINA_ADMIN_TOKEN", "PATINA_FEED_TOKEN"]
        .into_iter()
        .filter(|v| env.get(*v).is_some_and(|s| s.is_empty()))
        .collect()
}

/// What startup says about a token variable that is set but empty (an
/// empty variable is ignored, see `empty_token_vars`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyTokenNotice {
    /// The config file sets this token, so it still applies (logged at info).
    ConfigFileTokenUsed(&'static str),
    /// No token from the config file either: these endpoints are OPEN
    /// (logged as a warning).
    Open(&'static str),
}

/// One notice per token variable that is set but empty, decided from the
/// EFFECTIVE token in `config` (env or TOML). Pure, so it is tested without
/// a logger.
pub fn empty_token_notices(
    env: &HashMap<String, String>,
    config: &Config,
) -> Vec<EmptyTokenNotice> {
    empty_token_vars(env)
        .into_iter()
        .map(|var| {
            let effective = match var {
                "PATINA_ADMIN_TOKEN" => &config.admin_token,
                _ => &config.feed_token,
            };
            if effective.is_some() {
                EmptyTokenNotice::ConfigFileTokenUsed(var)
            } else {
                EmptyTokenNotice::Open(var)
            }
        })
        .collect()
}

fn parse_tz(s: &str) -> Result<Tz, String> {
    s.parse::<Tz>()
        .map_err(|_| format!("unknown timezone '{s}' (use an IANA name like America/Chicago)"))
}

/// The text of an `Invalid` error without its "invalid config:" prefix, so it
/// can be re-wrapped with extra context.
fn inner_msg(e: ConfigError) -> String {
    match e {
        ConfigError::Invalid(m) => m,
        other => other.to_string(),
    }
}

fn at_least_one(name: &str, v: u32) -> Result<u32, ConfigError> {
    if v == 0 {
        Err(ConfigError::Invalid(format!("{name} must be at least 1")))
    } else {
        Ok(v)
    }
}

impl Config {
    /// Build the configuration from env vars and optional TOML text.
    /// Precedence: env, then TOML, then built-in default.
    pub fn load(
        env: &HashMap<String, String>,
        toml_text: Option<&str>,
    ) -> Result<Config, ConfigError> {
        let raw: RawConfig = match toml_text {
            Some(t) => toml::from_str(t)?,
            None => RawConfig::default(),
        };

        // listen
        let listen_text = env_str(env, "PATINA_LISTEN");
        let (listen_src, listen_val) = match (&listen_text, &raw.listen) {
            (Some(s), _) => ("environment variable PATINA_LISTEN", s.as_str()),
            (None, Some(s)) => ("listen", s.as_str()),
            (None, None) => ("listen", "0.0.0.0:8080"),
        };
        let listen: SocketAddr = listen_val.parse().map_err(|e| {
            ConfigError::Invalid(format!(
                "{listen_src} is not a valid host:port ({e}); got '{listen_val}'"
            ))
        })?;

        // timezone
        let tz_text = env_str(env, "PATINA_TIMEZONE");
        let timezone = match (&tz_text, &raw.timezone) {
            (Some(s), _) => parse_tz(s).map_err(|reason| ConfigError::Env {
                var: "PATINA_TIMEZONE".into(),
                reason,
            })?,
            (None, Some(s)) => {
                parse_tz(s).map_err(|r| ConfigError::Invalid(format!("timezone: {r}")))?
            }
            (None, None) => chrono_tz::America::Chicago,
        };

        // Retention: TOML table first, then per-field env overrides on top.
        let mut retention = raw.retention.clone();
        if let Some(v) = env_parse::<u32>(env, "PATINA_RETENTION_IMAGES_DAYS")? {
            retention.images_days = v;
        }
        if let Some(v) = env_parse::<u32>(env, "PATINA_RETENTION_RAW_DAYS")? {
            retention.raw_days = v;
        }
        if let Some(v) = env_parse::<u32>(env, "PATINA_RETENTION_PROGRAMMES_DAYS")? {
            retention.programmes_days = v;
        }
        if let Some(v) = env_parse::<u32>(env, "PATINA_RETENTION_HISTORY_DAYS")? {
            retention.history_days = v;
        }
        at_least_one("retention.images_days", retention.images_days)?;
        at_least_one("retention.raw_days", retention.raw_days)?;
        at_least_one("retention.programmes_days", retention.programmes_days)?;
        at_least_one("retention.history_days", retention.history_days)?;

        // horizon_days: 1 to MAX_HORIZON_DAYS. A large value would overflow
        // the date arithmetic, and even a moderate one (say 30) spends the
        // daily request budget on far-away days instead of near-term
        // refreshes.
        let (horizon_src, horizon_days) = match env_parse::<u32>(env, "PATINA_HORIZON_DAYS")? {
            Some(v) => ("environment variable PATINA_HORIZON_DAYS", v),
            None => ("horizon_days", raw.horizon_days.unwrap_or(7)),
        };
        if !(1..=MAX_HORIZON_DAYS).contains(&horizon_days) {
            return Err(ConfigError::Invalid(format!(
                "{horizon_src} (horizon_days) must be between 1 and {MAX_HORIZON_DAYS} days (got {horizon_days})"
            )));
        }

        let disk_cap_bytes = env_parse::<u64>(env, "PATINA_DISK_CAP_BYTES")?.or(raw.disk_cap_bytes);
        if disk_cap_bytes == Some(0) {
            return Err(ConfigError::Invalid(
                "disk_cap_bytes must be at least 1 if set".into(),
            ));
        }

        // Providers: check budgets and that lineup names are unique, since
        // sources refer to lineups by name.
        let mut lineup_names: HashSet<&str> = HashSet::new();
        let mut provider_names: HashSet<&str> = HashSet::new();
        for p in &raw.providers {
            if p.name.is_empty() {
                return Err(ConfigError::Invalid("a provider has an empty name".into()));
            }
            // Budgets are tracked per provider name, so names must be unique.
            if !provider_names.insert(p.name.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "provider name '{}' is used more than once",
                    p.name
                )));
            }
            if p.daily_budget == 0 {
                return Err(ConfigError::Invalid(format!(
                    "provider '{}': daily_budget must be at least 1",
                    p.name
                )));
            }
            if let Some(u) = &p.base_url {
                let ok = url::Url::parse(u)
                    .map(|u| u.scheme() == "http" || u.scheme() == "https")
                    .unwrap_or(false);
                if !ok {
                    return Err(ConfigError::Invalid(format!(
                        "provider '{}': base_url must be an http(s) URL (got '{u}')",
                        p.name
                    )));
                }
            }
            for l in &p.lineups {
                if l.name.is_empty() || l.lineup_id.is_empty() {
                    return Err(ConfigError::Invalid(format!(
                        "provider '{}': lineup 'name' and 'lineup_id' must not be empty",
                        p.name
                    )));
                }
                if !lineup_names.insert(l.name.as_str()) {
                    return Err(ConfigError::Invalid(format!(
                        "lineup name '{}' is used more than once",
                        l.name
                    )));
                }
            }
        }

        // Sources: convert, then check every lineup reference exists.
        let mut sources = Vec::new();
        for (i, s) in raw.sources.into_iter().enumerate() {
            // Errors name the source by its 1-based position in the file.
            let prefix = format!("source #{} ({})", i + 1, s.kind.label());
            let cfg = s
                .into_cfg()
                .map_err(|e| ConfigError::Invalid(format!("{prefix}: {}", inner_msg(e))))?;
            for name in &cfg.lineups {
                if !lineup_names.contains(name.as_str()) {
                    return Err(ConfigError::Invalid(format!(
                        "{prefix}: refers to unknown lineup '{name}'"
                    )));
                }
            }
            sources.push(cfg);
        }

        raw.scoring.validate()?;

        // Check the log filter's syntax; `try_new` needs no initialised subscriber.
        let log = env_str(env, "PATINA_LOG")
            .or(raw.log)
            .unwrap_or_else(|| "info".to_string());
        if let Err(e) = tracing_subscriber::EnvFilter::try_new(&log) {
            return Err(ConfigError::Invalid(format!(
                "log filter '{log}' is invalid ({e})"
            )));
        }

        Ok(Config {
            listen,
            log,
            database_url: env_str(env, "PATINA_DATABASE_URL")
                .or(raw.database_url)
                .unwrap_or_else(|| "sqlite://patina.db?mode=rwc".to_string()),
            timezone,
            feed_token: env_str(env, "PATINA_FEED_TOKEN")
                .or(raw.feed_token)
                .filter(|s| !s.is_empty()),
            admin_token: env_str(env, "PATINA_ADMIN_TOKEN")
                .or(raw.admin_token)
                .filter(|s| !s.is_empty()),
            retention,
            disk_cap_bytes,
            providers: raw.providers,
            sources,
            scoring: raw.scoring,
            horizon_days,
        })
    }

    /// A one-line summary that is safe to log: tokens are masked and any
    /// password in the database URL is replaced.
    pub fn redacted(&self) -> String {
        let mask = |t: &Option<String>| if t.is_some() { "***" } else { "none" };
        format!(
            "listen={} log={} database_url={} timezone={} feed_token={} admin_token={} \
             horizon_days={} retention={:?} disk_cap_bytes={:?} providers={} sources={} scoring={:?}",
            self.listen,
            self.log,
            redact_url(&self.database_url),
            self.timezone,
            mask(&self.feed_token),
            mask(&self.admin_token),
            self.horizon_days,
            self.retention,
            self.disk_cap_bytes,
            self.providers.len(),
            self.sources.len(),
            self.scoring,
        )
    }
}

/// What `redact_url` returns for a URL it cannot parse: never the raw text,
/// which might hold a password in a form we did not recognise.
pub const UNPARSEABLE_DATABASE_URL: &str = "<unparseable database URL>";

/// Query parameters whose values are secrets (libpq/sqlx spellings).
const SECRET_QUERY_KEYS: [&str; 2] = ["password", "sslpassword"];

/// A database URL that is safe to log or show: the password of
/// `scheme://user:password@host/...` and the values of the `password` and
/// `sslpassword` query parameters are replaced by `***`. A URL with nothing
/// secret in it (SQLite paths, Postgres without a password) is returned
/// unchanged. Text that does not parse as a URL becomes
/// `UNPARSEABLE_DATABASE_URL`, never the raw string.
pub fn redact_url(raw: &str) -> String {
    let Ok(mut url) = url::Url::parse(raw) else {
        return UNPARSEABLE_DATABASE_URL.to_string();
    };
    let mut changed = false;
    if url.password().is_some() {
        // Only fails for URLs that cannot have credentials at all, and those
        // have no password to begin with.
        if url.set_password(Some("***")).is_err() {
            return UNPARSEABLE_DATABASE_URL.to_string();
        }
        changed = true;
    }
    let is_secret = |k: &str| SECRET_QUERY_KEYS.iter().any(|s| k.eq_ignore_ascii_case(s));
    if url.query_pairs().any(|(k, _)| is_secret(&k)) {
        let pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(k, v)| {
                let v = if is_secret(&k) {
                    "***".into()
                } else {
                    v.into_owned()
                };
                (k.into_owned(), v)
            })
            .collect();
        url.query_pairs_mut().clear().extend_pairs(pairs);
        changed = true;
    }
    if changed {
        url.to_string()
    } else {
        raw.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{UNPARSEABLE_DATABASE_URL, redact_url};

    /// Redact `raw` and check that `secret` is gone and `keep` is still there.
    fn hides(raw: &str, secret: &str, keep: &str) {
        let out = redact_url(raw);
        assert!(!out.contains(secret), "{secret} leaked: {out}");
        assert!(out.contains(keep), "{keep} missing: {out}");
    }

    #[test]
    fn user_password_is_masked() {
        let out = redact_url("postgres://patina:dbsecret@db.local:5432/patina");
        assert_eq!(out, "postgres://patina:***@db.local:5432/patina");
    }

    #[test]
    fn password_query_parameters_are_masked() {
        hides(
            "postgres://patina@db.local/patina?password=qsecret",
            "qsecret",
            "db.local",
        );
        hides(
            "postgres://db.local/patina?sslmode=require&sslpassword=keysecret",
            "keysecret",
            "sslmode=require",
        );
        hides(
            "postgres://db.local/patina?PASSWORD=upper",
            "upper",
            "db.local",
        );
        // Both forms at once.
        let out = redact_url("postgres://u:one@h/db?password=two");
        assert!(!out.contains("one") && !out.contains("two"), "{out}");
    }

    #[test]
    fn password_with_special_characters_is_masked() {
        // Percent-encoded '/', '?', '@'.
        hides(
            "postgres://u:a%2Fb%3Fc%40d@db.local/patina",
            "a%2Fb",
            "db.local",
        );
        hides(
            "postgres://u:a%2Fb%3Fc%40d@db.local/patina",
            "c%40d",
            "db.local",
        );
        // A raw '@' inside the password: the last '@' ends the credentials.
        hides("postgres://u:p@ss@db.local/patina", "ss@db", "db.local");
        let out = redact_url("postgres://u:p@ss@db.local/patina");
        assert!(!out.contains("p@ss") && !out.contains("p%40ss"), "{out}");
        // Raw '/' or '?' in the password do not parse as a URL at all: the
        // fixed text comes back, never the input.
        assert_eq!(
            redact_url("postgres://u:a/b@db.local/patina"),
            UNPARSEABLE_DATABASE_URL
        );
        assert_eq!(
            redact_url("postgres://u:a?b@db.local/patina"),
            UNPARSEABLE_DATABASE_URL
        );
        // Encoded secrets in a query value.
        hides(
            "postgres://h/db?password=a%2Fb%3Fc%40d",
            "a%2Fb",
            "postgres://h/db",
        );
    }

    #[test]
    fn urls_without_credentials_are_unchanged() {
        for url in [
            "postgres://patina@db.local:5432/patina?sslmode=verify-full",
            "postgresql://db.local/patina",
            "sqlite://patina.db?mode=rwc",
            "sqlite:///data/patina.db?mode=rwc",
            "sqlite::memory:",
        ] {
            assert_eq!(redact_url(url), url);
        }
    }

    #[test]
    fn garbage_is_never_echoed() {
        for raw in [
            "not a url at all password=hunter2",
            "host=db.local user=patina password=hunter2",
            "",
            "://:hunter2@",
        ] {
            assert_eq!(redact_url(raw), UNPARSEABLE_DATABASE_URL, "{raw}");
        }
    }
}
