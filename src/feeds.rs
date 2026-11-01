//! The XMLTV feeds: which channels and programmes each feed contains, the
//! background refresh of the tuner channel lists, and the in-memory cache of
//! rendered feeds. HTTP details (routes, token, headers) live in `http.rs`.
//!
//! Feed names: `all` is the combined feed. Every configured channel source
//! also has a feed named after it. Sources have no name in the config file,
//! so [`source_names`] derives one from the kind and the 1-based position
//! among sources of that kind: `hdhomerun-1`, `hdhomerun-2`, `static-1`, ...

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, TimeDelta, Utc};
use chrono_tz::Tz;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::auth::ct_eq;
use crate::channels::matching::{Overrides, match_channels};
use crate::channels::{
    ChannelSource, ChannelSourceLoader, ChannelSourceStatus, HdHomeRunSource, LoadedChannels,
    StaticSource, TunerChannel,
};
use crate::clock::Clock;
use crate::config::{Config, SourceCfg, SourceKind};
use crate::model::{CanonicalChannel, ChannelId, MasterSlot, ProviderId};
use crate::provider::ProviderChannel;
use crate::store::Store;
use crate::xmltv;

/// A rendered feed is reused for this long (unless a merge bumps the
/// generation first).
pub const CACHE_TTL: Duration = Duration::from_secs(30);
/// Programmes start this long before "now", so shows on air are present.
pub const LOOKBACK: TimeDelta = TimeDelta::hours(3);
/// How often the tuner channel lists are reloaded.
pub const REFRESH_INTERVAL: Duration = Duration::from_secs(10 * 60);

/// Stable feed names for the configured sources (see the module comment).
pub fn source_names(sources: &[SourceCfg]) -> Vec<String> {
    let (mut hd, mut st) = (0, 0);
    sources
        .iter()
        .map(|s| match s.kind {
            SourceKind::HdHomeRun { .. } => {
                hd += 1;
                format!("hdhomerun-{hd}")
            }
            SourceKind::Static { .. } => {
                st += 1;
                format!("static-{st}")
            }
        })
        .collect()
}

/// Everything the feeds need to know about one configured channel source.
pub struct FeedSourceSpec {
    /// Feed name, from [`source_names`].
    pub name: String,
    pub include_unmatched: bool,
    pub overrides: Overrides,
    /// (provider, lineup name) pairs whose channels this source matches
    /// against; the lineup name is the key `Store::provider_channels_list`
    /// and the scheduler use.
    pub lineups: Vec<(ProviderId, String)>,
    /// Where the tuner channel list comes from (read by the refresher only).
    pub source: Arc<dyn ChannelSource>,
}

/// Build the specs for every source in `config`. `overrides` must hold one
/// entry per source, in order (they were validated at startup).
pub fn feed_sources_from_config(
    config: &Config,
    overrides: Vec<Overrides>,
    client: reqwest::Client,
) -> Vec<FeedSourceSpec> {
    let names = source_names(&config.sources);
    config
        .sources
        .iter()
        .zip(overrides)
        .zip(names)
        .map(|((cfg, overrides), name)| {
            let source: Arc<dyn ChannelSource> = match &cfg.kind {
                SourceKind::HdHomeRun { url } => match url::Url::parse(url) {
                    Ok(u) => Arc::new(HdHomeRunSource::new(client.clone(), u)),
                    // Config validation rejects bad URLs; fail the refresh
                    // (visible in status) rather than panic if one slips by.
                    Err(e) => Arc::new(BrokenSource(format!("invalid tuner URL: {e}"))),
                },
                SourceKind::Static { channels } => Arc::new(StaticSource::from_config(channels)),
            };
            let lineups = cfg
                .lineups
                .iter()
                .filter_map(|l| {
                    config
                        .providers
                        .iter()
                        .find(|p| p.lineups.iter().any(|pl| pl.name == *l))
                        .map(|p| (ProviderId(p.name.clone()), l.clone()))
                })
                .collect();
            FeedSourceSpec {
                name,
                include_unmatched: cfg.include_unmatched,
                overrides,
                lineups,
                source,
            }
        })
        .collect()
}

struct BrokenSource(String);

#[async_trait::async_trait]
impl ChannelSource for BrokenSource {
    async fn load(&self) -> anyhow::Result<Vec<TunerChannel>> {
        Err(anyhow::anyhow!("{}", self.0))
    }
}

pub struct FeedsConfig {
    pub store: Arc<dyn Store>,
    pub clock: Arc<dyn Clock>,
    pub tz: Tz,
    pub horizon_days: u32,
    /// `Config.feed_token`; `None` means the feeds are open.
    pub token: Option<String>,
    pub sources: Vec<FeedSourceSpec>,
    /// Shared with the scheduler, which bumps it after every merge.
    pub generation: Arc<AtomicU64>,
}

/// A rendered feed ready to send.
#[derive(Debug, Clone)]
pub struct Rendered {
    pub body: Bytes,
    /// Quoted entity tag, a hash of `body`.
    pub etag: String,
}

#[derive(Debug, Clone)]
pub enum FeedOutcome {
    Ok(Rendered),
    /// No such feed.
    NotFound,
    /// The feed cannot be built yet; the text says why. Not cached.
    Unavailable(String),
}

/// Channel matching result of one source (see [`Feeds::source_matches`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceMatch {
    pub name: String,
    /// Canonical ids of the matched channels, one per station.
    pub matched: Vec<ChannelId>,
    /// Tuner entries that matched nothing or were ambiguous (`ignore`d
    /// entries are in neither list).
    pub unmatched: usize,
}

struct CacheEntry {
    generation: u64,
    at: DateTime<Utc>,
    rendered: Rendered,
}

pub struct Feeds {
    store: Arc<dyn Store>,
    clock: Arc<dyn Clock>,
    tz: Tz,
    horizon_days: u32,
    token: Option<String>,
    sources: Vec<FeedSourceSpec>,
    generation: Arc<AtomicU64>,
    /// Latest tuner channel list per source name. Written by the refresher,
    /// only read by requests. A plain `RwLock`: never held across `.await`.
    loaded: RwLock<HashMap<String, LoadedChannels>>,
    /// Rendered feeds by name. An async mutex held while rendering, so a
    /// burst of requests for a stale feed renders it once, not N times.
    cache: Mutex<HashMap<String, CacheEntry>>,
}

impl Feeds {
    pub fn new(cfg: FeedsConfig) -> Self {
        Feeds {
            store: cfg.store,
            clock: cfg.clock,
            tz: cfg.tz,
            horizon_days: cfg.horizon_days,
            token: cfg.token,
            sources: cfg.sources,
            generation: cfg.generation,
            loaded: RwLock::new(HashMap::new()),
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// The cache generation (the same counter the scheduler bumps).
    pub fn generation(&self) -> Arc<AtomicU64> {
        self.generation.clone()
    }

    /// Does the supplied token pass? Always true when no token is configured.
    /// `supplied` holds every token the request carried (path and query).
    /// When a token is configured, exactly ONE must be supplied: a request
    /// with several (two `?token=` values, or a path token plus a query one)
    /// is refused even if one of them is right, so one request can never
    /// try more than one guess. The comparison runs in constant time, so
    /// timing reveals nothing about how much of a guess was right.
    pub fn token_ok(&self, supplied: &[&str]) -> bool {
        let Some(expected) = &self.token else {
            return true;
        };
        match supplied {
            [one] => ct_eq(one.as_bytes(), expected.as_bytes()),
            _ => false,
        }
    }

    /// Whether a feed token is configured. Responses then carry
    /// `Cache-Control: private`, so a shared cache never keeps a copy of a
    /// token-guarded feed.
    pub fn token_required(&self) -> bool {
        self.token.is_some()
    }

    /// What `/status` shows per source: name and the state of its channel
    /// list (`None` before the first refresh finished).
    pub fn source_status(&self) -> Vec<(String, Option<ChannelSourceStatus>)> {
        let loaded = self.loaded.read().unwrap();
        self.sources
            .iter()
            .map(|s| (s.name.clone(), loaded.get(&s.name).map(Into::into)))
            .collect()
    }

    /// How each source's loaded tuner channels match the guide provider's
    /// channels right now, for `/status` and `/metrics`. Sources whose
    /// tuner list has never loaded, or whose provider channels cannot be
    /// read, are left out (callers show those as zero).
    pub async fn source_matches(&self) -> Vec<SourceMatch> {
        let mut out = Vec::new();
        for spec in &self.sources {
            let loaded = self.loaded.read().unwrap().get(&spec.name).cloned();
            let Some(loaded) = loaded else { continue };
            // `provider_channels` logs its own failure.
            let Ok(provider) = self.provider_channels(spec).await else {
                continue;
            };
            let result = match_channels(&loaded.channels, &provider, &spec.overrides);
            // De-duplicated: two tuner entries can map to one station.
            let mut seen = HashSet::new();
            let matched: Vec<ChannelId> = result
                .matched
                .iter()
                .map(|(_, c)| c.id.clone())
                .filter(|id| seen.insert(id.clone()))
                .collect();
            out.push(SourceMatch {
                name: spec.name.clone(),
                matched,
                // Ambiguous entries count as unmatched, as in the feeds.
                unmatched: result.unmatched.len() + result.ambiguous.len(),
            });
        }
        out
    }

    // ------------------------------------------------------------ refresh

    /// Reload every source's tuner channel list once. A failed load keeps
    /// serving the last-known list (from the store, or from memory) marked
    /// stale. Bumps the cache generation if anything changed.
    pub async fn refresh_channels(&self) {
        let now = self.clock.now();
        for spec in &self.sources {
            let fresh = ChannelSourceLoader
                .load_with_fallback(&spec.name, spec.source.as_ref(), self.store.as_ref(), now)
                .await;
            if let Some(e) = &fresh.error {
                tracing::warn!(source = %spec.name, error = %e, "channel source refresh problem");
            }
            let changed = {
                let mut map = self.loaded.write().unwrap();
                let next = match map.get(&spec.name) {
                    // Nothing usable came back, but we already hold a list
                    // from earlier: keep it, marked stale, with the new error.
                    Some(old) if fresh.loaded_at.is_none() && !old.channels.is_empty() => {
                        LoadedChannels {
                            channels: old.channels.clone(),
                            stale: true,
                            loaded_at: old.loaded_at,
                            error: fresh.error.clone(),
                        }
                    }
                    _ => fresh,
                };
                let changed = map
                    .get(&spec.name)
                    .is_none_or(|old| old.channels != next.channels);
                map.insert(spec.name.clone(), next);
                changed
            };
            if changed {
                self.generation.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    /// Background task: refresh now, then every [`REFRESH_INTERVAL`] (plus a
    /// little random delay so several instances do not hit a tuner in step)
    /// until `shutdown` is cancelled.
    pub async fn run_refresher(self: Arc<Self>, shutdown: CancellationToken) {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = self.refresh_channels() => {}
            }
            let jitter = Duration::from_millis(fastrand::u64(0..30_000));
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(REFRESH_INTERVAL + jitter) => {}
            }
        }
    }

    // -------------------------------------------------------------- serve

    /// The feed called `name` (`all` or a source name), from cache when the
    /// cached copy is current.
    pub async fn feed(&self, name: &str) -> FeedOutcome {
        if name != "all" && !self.sources.iter().any(|s| s.name == name) {
            return FeedOutcome::NotFound;
        }
        let mut cache = self.cache.lock().await;
        // Read the generation BEFORE rendering: a merge that lands while we
        // render then makes this entry already stale, never wrongly fresh.
        let generation = self.generation.load(Ordering::SeqCst);
        let now = self.clock.now();
        if let Some(e) = cache.get(name)
            && e.generation == generation
            && now >= e.at
            && (now - e.at).to_std().is_ok_and(|age| age < CACHE_TTL)
        {
            return FeedOutcome::Ok(e.rendered.clone());
        }
        match self.build(name, now).await {
            Ok(xml) => {
                let body = Bytes::from(xml);
                let rendered = Rendered {
                    etag: etag_of(&body),
                    body,
                };
                cache.insert(
                    name.to_string(),
                    CacheEntry {
                        generation,
                        at: now,
                        rendered: rendered.clone(),
                    },
                );
                FeedOutcome::Ok(rendered)
            }
            Err(why) => FeedOutcome::Unavailable(why),
        }
    }

    /// Render one feed from scratch. `Err` carries the reason it cannot be
    /// built yet.
    async fn build(&self, name: &str, now: DateTime<Utc>) -> Result<String, String> {
        let selected: Vec<&FeedSourceSpec> = self
            .sources
            .iter()
            .filter(|s| name == "all" || s.name == name)
            .collect();

        // Channels, in source order then tuner order, one per canonical id
        // (the first source to carry a station names it).
        let mut channels: Vec<CanonicalChannel> = Vec::new();
        let mut seen: HashSet<ChannelId> = HashSet::new();
        let mut any_ready = false;
        for spec in selected {
            let loaded = self.loaded.read().unwrap().get(&spec.name).cloned();
            // "Never loaded": no list in memory, or an empty one that never
            // came from a successful load. Serving that as an empty guide
            // would make Jellyfin remember "no channels".
            let Some(loaded) = loaded.filter(|l| l.loaded_at.is_some() || !l.channels.is_empty())
            else {
                continue;
            };
            let provider = self.provider_channels(spec).await?;
            if provider.is_empty() {
                // Same reasoning: the scheduler has not stored the
                // provider's channels yet, so nothing could match.
                continue;
            }
            any_ready = true;
            for c in feed_channels(spec, &loaded.channels, &provider) {
                if seen.insert(c.id.clone()) {
                    channels.push(c);
                }
            }
        }
        if !any_ready {
            return Err(if name == "all" {
                "no channel source is ready yet (tuner channel list or guide channels not loaded)"
                    .to_string()
            } else {
                format!(
                    "feed {name} is not ready yet (tuner channel list or guide channels not loaded)"
                )
            });
        }

        // Programmes for the real (not UNMATCHED-) channels.
        let from = now - LOOKBACK;
        let to = crate::scheduler::horizon_end(now, self.horizon_days);
        let mut slots: Vec<MasterSlot> = Vec::new();
        for c in channels.iter().filter(|c| !is_unmatched(&c.id)) {
            match self.store.slots(c.id.clone(), from, to).await {
                Ok(mut s) => slots.append(&mut s),
                Err(e) => {
                    tracing::error!(feed = name, error = %format!("{e:#}"), "cannot read slots");
                    return Err("the guide database could not be read".to_string());
                }
            }
        }
        Ok(xmltv::render(&channels, &slots, self.tz))
    }

    /// Provider channels of all the source's lineups, one per provider
    /// channel id (a station listed in two lineups must not look ambiguous).
    async fn provider_channels(
        &self,
        spec: &FeedSourceSpec,
    ) -> Result<Vec<ProviderChannel>, String> {
        let mut out: Vec<ProviderChannel> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for (provider, lineup) in &spec.lineups {
            match self.store.provider_channels_list(provider, lineup).await {
                Ok(list) => {
                    for c in list {
                        if seen.insert(c.provider_channel_id.clone()) {
                            out.push(c);
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(source = %spec.name, error = %format!("{e:#}"), "cannot read provider channels");
                    return Err("the guide database could not be read".to_string());
                }
            }
        }
        Ok(out)
    }
}

/// The channels of one source's feed, in tuner order: matched entries
/// (always), plus unmatched and ambiguous entries as empty channels when
/// `include_unmatched` is set. Entries removed by an `ignore` override are
/// never shown.
fn feed_channels(
    spec: &FeedSourceSpec,
    tuner: &[TunerChannel],
    provider: &[ProviderChannel],
) -> Vec<CanonicalChannel> {
    let result = match_channels(tuner, provider, &spec.overrides);
    // `match_channels` returns each category in tuner order but separately;
    // walk the tuner list and take from whichever category holds the entry.
    let mut matched = result.matched.iter().peekable();
    let mut unmatched = result.unmatched.iter().peekable();
    let mut ambiguous = result.ambiguous.iter().map(|(t, _)| t).peekable();
    let mut out = Vec::new();
    let mut ids: HashSet<ChannelId> = HashSet::new();
    for t in tuner {
        if let Some((_, c)) = matched.next_if(|(mt, _)| mt == t) {
            if ids.insert(c.id.clone()) {
                out.push(c.clone());
            }
        } else if unmatched.next_if(|u| *u == t).is_some()
            || ambiguous.next_if(|a| *a == t).is_some()
        {
            // Ambiguous entries count as unmatched.
            if spec.include_unmatched {
                let c = unmatched_channel(t);
                if ids.insert(c.id.clone()) {
                    out.push(c);
                }
            }
        }
        // Otherwise the entry was ignored: nothing to show.
    }
    out
}

/// An empty placeholder channel for a tuner entry that matched nothing:
/// `UNMATCHED-<number>.us`, so Jellyfin can still show the channel.
fn unmatched_channel(t: &TunerChannel) -> CanonicalChannel {
    let number: String = t
        .number
        .trim()
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();
    CanonicalChannel {
        id: ChannelId(format!("UNMATCHED-{number}.us")),
        number: t.number.trim().to_string(),
        name: t.name.clone(),
        call_sign: t.name.clone(),
    }
}

fn is_unmatched(id: &ChannelId) -> bool {
    id.0.starts_with("UNMATCHED-")
}

/// A strong-enough content hash for an ETag: 64-bit FNV-1a, written out so
/// the value is stable across Rust versions and runs (std's hashers do not
/// promise that). It only has to change when the body changes.
fn etag_of(body: &[u8]) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in body {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("\"{h:016x}\"")
}
