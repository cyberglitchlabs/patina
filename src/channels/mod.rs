//! Channel sources: where the list of channels the user actually has comes
//! from (an HDHomeRun tuner or a list in the config), how that list is
//! remembered when the source is down, and how it is matched to a guide
//! provider's channels.

pub mod hdhomerun;
pub mod matching;

use std::path::Path;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::config::StaticChannel;
use crate::store::Store;

pub use hdhomerun::HdHomeRunSource;
pub use matching::{OverrideError, Overrides};

/// One channel as the tuner (or the config) names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TunerChannel {
    /// Channel number as text, for example `4.1`.
    pub number: String,
    pub name: String,
}

#[async_trait]
pub trait ChannelSource: Send + Sync {
    async fn load(&self) -> Result<Vec<TunerChannel>>;
}

/// A fixed list, built from the `static` source kind in the config.
pub struct StaticSource {
    channels: Vec<TunerChannel>,
}

impl StaticSource {
    pub fn new(channels: Vec<TunerChannel>) -> Self {
        Self { channels }
    }

    pub fn from_config(channels: &[StaticChannel]) -> Self {
        Self::new(
            channels
                .iter()
                .map(|c| TunerChannel {
                    number: c.number.trim().to_string(),
                    name: c.name.clone(),
                })
                .collect(),
        )
    }
}

#[async_trait]
impl ChannelSource for StaticSource {
    async fn load(&self) -> Result<Vec<TunerChannel>> {
        Ok(self.channels.clone())
    }
}

/// Read the overrides file named by a source's `overrides_path`.
/// `None` means no file is configured, which is an empty override set.
/// A missing or invalid file is an error (called at startup, which exits).
pub fn load_overrides(path: Option<&Path>) -> Result<Overrides, OverrideError> {
    let Some(path) = path else {
        return Ok(Overrides::default());
    };
    let text = std::fs::read_to_string(path).map_err(|e| OverrideError::Read {
        path: path.display().to_string(),
        message: e.to_string(),
    })?;
    Overrides::from_toml(&text)
}

/// Result of loading a source through [`ChannelSourceLoader`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedChannels {
    pub channels: Vec<TunerChannel>,
    /// True when the live source failed and `channels` is the last-known
    /// list (or empty if there never was one).
    pub stale: bool,
    /// When `channels` was last successfully loaded from the source.
    pub loaded_at: Option<DateTime<Utc>>,
    /// Why the live load failed, if it did.
    pub error: Option<String>,
}

/// What `/status` shows about a channel source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelSourceStatus {
    pub channels_total: usize,
    pub stale: bool,
    pub loaded_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

impl From<&LoadedChannels> for ChannelSourceStatus {
    fn from(l: &LoadedChannels) -> Self {
        Self {
            channels_total: l.channels.len(),
            stale: l.stale,
            loaded_at: l.loaded_at,
            error: l.error.clone(),
        }
    }
}

/// Combines a source with the store so a down tuner does not empty the guide.
pub struct ChannelSourceLoader;

impl ChannelSourceLoader {
    pub async fn load_with_fallback(
        &self,
        source_key: &str,
        source: &dyn ChannelSource,
        store: &dyn Store,
        now: DateTime<Utc>,
    ) -> LoadedChannels {
        // An empty list is treated as a failed load: a tuner in the middle
        // of a channel scan answers `[]`, and saving that would replace the
        // good list (and serve an empty guide, which Jellyfin then caches as
        // "no channels"). A `static` source cannot be empty (the config
        // rejects it), so an empty answer is never a real channel list.
        let loaded = source.load().await.and_then(|channels| {
            if channels.is_empty() {
                Err(anyhow!("tuner returned no channels"))
            } else {
                Ok(channels)
            }
        });
        match loaded {
            Ok(channels) => {
                // A failed save must not hide a good list: report it as the
                // error but still hand back the fresh channels.
                let error = store
                    .save_tuner_channels(source_key, &channels, now)
                    .await
                    .err()
                    .map(|e| format!("could not save channel list: {e:#}"));
                LoadedChannels {
                    channels,
                    stale: false,
                    loaded_at: Some(now),
                    error,
                }
            }
            Err(e) => {
                let error = Some(format!("{e:#}"));
                match store.load_tuner_channels(source_key).await {
                    Ok(Some((channels, saved_at))) => LoadedChannels {
                        channels,
                        stale: true,
                        loaded_at: Some(saved_at),
                        error,
                    },
                    // Never loaded, or the store failed too: nothing to show.
                    Ok(None) | Err(_) => LoadedChannels {
                        channels: Vec::new(),
                        stale: true,
                        loaded_at: None,
                        error,
                    },
                }
            }
        }
    }
}
