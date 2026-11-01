//! Matching tuner channels to provider channels, and the overrides file.

use std::collections::HashSet;
use std::fmt;

use serde::Deserialize;

use super::TunerChannel;
use crate::model::{CanonicalChannel, ChannelId, canonical_channel, canonical_id};
use crate::provider::ProviderChannel;

/// Uppercase letters and digits only, no `.us` suffix, so `WCCO-DT`,
/// `WCCODT` and `WCCO DT` all compare equal. Built on `canonical_id` so
/// there is only one normalizer in the program.
pub fn normalize_call_sign(s: &str) -> String {
    let id = canonical_id(s).0;
    // `canonical_id` always appends ".us"; the normalized part has no dots,
    // so stripping the suffix is exact.
    id.strip_suffix(".us").unwrap_or(&id).to_string()
}

// ---------------------------------------------------------------- overrides

/// Which tuner entry an override applies to.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Selector {
    Number(String),
    /// Trimmed and lowercased tuner `GuideName`; compared for exact equality
    /// (not call-sign normalization, which would merge "FOX 9" and "FOX 9+").
    /// May match several tuner entries; the override applies to all of them.
    Name(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Action {
    /// Force this provider channel.
    ProviderChannel(String),
    /// Create a channel with exactly this canonical id.
    Canonical(String),
    Ignore,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OverrideEntry {
    selector: Selector,
    action: Action,
}

/// A validated set of overrides. Build with [`Overrides::from_toml`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Overrides {
    entries: Vec<OverrideEntry>,
}

impl Overrides {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The override for a tuner entry, if any. Duplicate selectors were
    /// rejected at parse time, but a `name` entry and a `number` entry can
    /// both match the same tuner channel; then the entry that appears
    /// earlier in the file wins (`find` returns the first in file order).
    fn find(&self, t: &TunerChannel) -> Option<&Action> {
        let name = name_key(&t.name);
        self.entries
            .iter()
            .find(|e| match &e.selector {
                Selector::Number(n) => n == t.number.trim(),
                Selector::Name(n) => *n == name,
            })
            .map(|e| &e.action)
    }

    /// Parse and validate the overrides file text.
    pub fn from_toml(text: &str) -> Result<Overrides, OverrideError> {
        // Read generically first so each entry can be validated on its own
        // and errors can name the entry (serde's own error would not).
        let table: toml::Table =
            text.parse()
                .map_err(|e: toml::de::Error| OverrideError::Syntax {
                    message: e.message().to_string(),
                })?;
        if let Some(k) = table.keys().find(|k| *k != "override") {
            return Err(OverrideError::Syntax {
                message: format!("unknown top-level key {k:?} (expected [[override]] tables)"),
            });
        }
        let Some(list) = table.get("override") else {
            return Ok(Overrides::default());
        };
        let Some(list) = list.as_array() else {
            return Err(OverrideError::Syntax {
                message: "'override' must be an array of tables ([[override]])".into(),
            });
        };

        let mut entries = Vec::new();
        let mut seen: HashSet<(bool, String)> = HashSet::new();
        for (i, value) in list.iter().enumerate() {
            let index = i + 1; // humans count from 1
            let bad = |label: String, problem: String| OverrideError::Entry {
                index,
                label,
                problem,
            };
            let raw: RawOverride = value.clone().try_into().map_err(|e: toml::de::Error| {
                bad(describe_value(value), e.message().to_string())
            })?;
            let label = raw.label();
            let selector = match (&raw.number, &raw.name) {
                (Some(_), Some(_)) => {
                    return Err(bad(
                        label,
                        "sets both 'number' and 'name'; use exactly one".into(),
                    ));
                }
                (None, None) => {
                    return Err(bad(
                        label,
                        "needs one of 'number' or 'name' to select a channel".into(),
                    ));
                }
                (Some(n), None) => Selector::Number(n.trim().to_string()),
                (None, Some(n)) => {
                    let key = name_key(n);
                    if key.is_empty() {
                        return Err(bad(label, "has an empty 'name' selector".into()));
                    }
                    Selector::Name(key)
                }
            };
            // `ignore = false` is the same as not setting it.
            let ignore = raw.ignore == Some(true);
            let action = match (raw.provider_channel_id, raw.canonical, ignore) {
                (Some(id), None, false) => Action::ProviderChannel(id),
                (None, Some(c), false) => Action::Canonical(c),
                (None, None, true) => Action::Ignore,
                (None, None, false) => {
                    return Err(bad(
                        label,
                        "has no action; set one of 'provider_channel_id', 'canonical' or 'ignore = true'".into(),
                    ));
                }
                _ => {
                    return Err(bad(
                        label,
                        "sets more than one action; use only one of 'provider_channel_id', 'canonical', 'ignore'".into(),
                    ));
                }
            };
            let key = match &selector {
                Selector::Number(n) => (true, n.clone()),
                Selector::Name(n) => (false, n.clone()),
            };
            if !seen.insert(key) {
                return Err(bad(
                    label,
                    "is a duplicate of an earlier entry with the same selector".into(),
                ));
            }
            entries.push(OverrideEntry { selector, action });
        }
        Ok(Overrides { entries })
    }
}

/// One `[[override]]` table as written. Unknown keys are rejected.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOverride {
    number: Option<String>,
    name: Option<String>,
    provider_channel_id: Option<String>,
    canonical: Option<String>,
    ignore: Option<bool>,
}

impl RawOverride {
    fn label(&self) -> String {
        match (&self.number, &self.name) {
            (Some(n), None) => format!("number {n:?}"),
            (None, Some(n)) => format!("name {n:?}"),
            (Some(n), Some(m)) => format!("number {n:?} / name {m:?}"),
            (None, None) => "(no number or name)".into(),
        }
    }
}

/// Best-effort label for an entry that did not even deserialize.
fn describe_value(v: &toml::Value) -> String {
    let get = |k: &str| v.get(k).and_then(|x| x.as_str()).map(str::to_string);
    match (get("number"), get("name")) {
        (Some(n), _) => format!("number {n:?}"),
        (None, Some(n)) => format!("name {n:?}"),
        _ => "(no number or name)".into(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OverrideError {
    Read {
        path: String,
        message: String,
    },
    Syntax {
        message: String,
    },
    Entry {
        index: usize,
        label: String,
        problem: String,
    },
}

impl fmt::Display for OverrideError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OverrideError::Read { path, message } => {
                write!(f, "cannot read overrides file {path}: {message}")
            }
            OverrideError::Syntax { message } => write!(f, "invalid overrides file: {message}"),
            OverrideError::Entry {
                index,
                label,
                problem,
            } => {
                write!(
                    f,
                    "invalid overrides file: entry {index} ({label}): {problem}"
                )
            }
        }
    }
}

impl std::error::Error for OverrideError {}

/// Key for comparing `name` selectors with tuner names: trimmed, Unicode
/// lowercased, otherwise exact.
fn name_key(s: &str) -> String {
    s.trim().to_lowercase()
}

// ----------------------------------------------------------------- matching

/// Where every tuner entry ended up. Each entry is in exactly one list.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MatchResult {
    pub matched: Vec<(TunerChannel, CanonicalChannel)>,
    /// Nothing in the provider's lineup fits (or an override pointed at a
    /// provider channel that does not exist).
    pub unmatched: Vec<TunerChannel>,
    /// Several provider channels fit equally well; an override must decide.
    pub ambiguous: Vec<(TunerChannel, Vec<ProviderChannel>)>,
    /// Removed by an `ignore = true` override.
    pub ignored: Vec<TunerChannel>,
}

/// The channel Jellyfin sees when a tuner entry is matched to a provider one:
/// the provider channel's canonical channel (`model::canonical_channel`: id
/// and call sign from the provider, so guide data lines up), with number and
/// name from the tuner (what the viewer knows).
fn canonical_from(t: &TunerChannel, p: &ProviderChannel) -> CanonicalChannel {
    CanonicalChannel {
        number: t.number.trim().to_string(),
        name: t.name.clone(),
        ..canonical_channel(p)
    }
}

/// Outcome for one tuner entry while matching is in progress.
enum Outcome {
    Matched(CanonicalChannel),
    Unmatched,
    Ambiguous(Vec<ProviderChannel>),
    Ignored,
    /// No provider channel has this entry's number; the call-sign fallback
    /// decides in a second pass (it needs to know what the first pass claimed).
    NeedsFallback,
}

pub fn match_channels(
    tuner: &[TunerChannel],
    provider: &[ProviderChannel],
    overrides: &Overrides,
) -> MatchResult {
    // Pass 1: overrides and number matching. `claimed` collects provider
    // channels taken by an automatic number match.
    let mut claimed: HashSet<&str> = HashSet::new();
    let mut outcomes: Vec<Outcome> = Vec::with_capacity(tuner.len());
    for t in tuner {
        // Overrides win over automatic matching.
        if let Some(action) = overrides.find(t) {
            outcomes.push(match action {
                Action::Ignore => Outcome::Ignored,
                Action::Canonical(id) => Outcome::Matched(CanonicalChannel {
                    id: ChannelId(id.clone()),
                    number: t.number.trim().to_string(),
                    name: t.name.clone(),
                    call_sign: t.name.clone(),
                }),
                Action::ProviderChannel(pid) => {
                    match provider.iter().find(|p| p.provider_channel_id == *pid) {
                        Some(p) => Outcome::Matched(canonical_from(t, p)),
                        None => Outcome::Unmatched,
                    }
                }
            });
            continue;
        }

        let number = t.number.trim();
        let name = normalize_call_sign(&t.name);
        let same_number: Vec<&ProviderChannel> = provider
            .iter()
            .filter(|p| p.number.trim() == number)
            .collect();
        if same_number.is_empty() {
            outcomes.push(Outcome::NeedsFallback);
            continue;
        }
        // Several share the number: the call sign may tell them apart.
        let candidates = if same_number.len() > 1 {
            let by_name: Vec<_> = same_number
                .iter()
                .copied()
                .filter(|p| normalize_call_sign(&p.call_sign) == name)
                .collect();
            if by_name.is_empty() {
                same_number
            } else {
                by_name
            }
        } else {
            same_number
        };
        outcomes.push(match candidates.as_slice() {
            [one] => {
                claimed.insert(one.provider_channel_id.as_str());
                Outcome::Matched(canonical_from(t, one))
            }
            many => Outcome::Ambiguous(many.iter().map(|p| (*p).clone()).collect()),
        });
    }

    // Pass 2: call-sign fallback, only for entries whose number the provider
    // does not have. To keep canonical ids unique among automatic matches it
    // may only take a provider channel that (1) no number match claimed and
    // (2) is named by exactly one tuner entry. Sub-channels sharing a base
    // call sign (several "KWJM-LD" entries) therefore stay unmatched.
    let norm_names: Vec<String> = tuner.iter().map(|t| normalize_call_sign(&t.name)).collect();
    for (i, t) in tuner.iter().enumerate() {
        if !matches!(outcomes[i], Outcome::NeedsFallback) {
            continue;
        }
        let name = &norm_names[i];
        let hits: Vec<&ProviderChannel> = provider
            .iter()
            .filter(|p| !name.is_empty() && normalize_call_sign(&p.call_sign) == *name)
            .collect();
        outcomes[i] = match hits.as_slice() {
            [p] if !claimed.contains(p.provider_channel_id.as_str())
                && norm_names.iter().filter(|n| *n == name).count() == 1 =>
            {
                Outcome::Matched(canonical_from(t, p))
            }
            _ => Outcome::Unmatched,
        };
    }

    let mut r = MatchResult::default();
    for (t, o) in tuner.iter().zip(outcomes) {
        match o {
            Outcome::Matched(c) => r.matched.push((t.clone(), c)),
            Outcome::Unmatched | Outcome::NeedsFallback => r.unmatched.push(t.clone()),
            Outcome::Ambiguous(c) => r.ambiguous.push((t.clone(), c)),
            Outcome::Ignored => r.ignored.push(t.clone()),
        }
    }
    r
}
