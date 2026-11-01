//! Domain types shared by the whole program. Plain data, plus the two rules
//! that make canonical channels (`canonical_id`, `canonical_channel`).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::provider::ProviderChannel;

/// Stable id of a canonical channel (for example `WCCODT.us`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ChannelId(pub String);

/// Name of a guide provider (for example `gracenote`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ProviderId(pub String);

/// Internal id of one slot (a programme airing) in the master guide.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SlotId(pub i64);

/// A channel as Jellyfin will see it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalChannel {
    pub id: ChannelId,
    /// Channel number as text, such as `4.1` or `11.1`.
    pub number: String,
    pub name: String,
    pub call_sign: String,
}

/// One provider's claim about one programme, with that provider's own times.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observation {
    pub provider: ProviderId,
    pub channel: ChannelId,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub title: String,
    pub subtitle: Option<String>,
    pub description: Option<String>,
    pub image_url: Option<String>,
    pub season: Option<u32>,
    pub episode: Option<u32>,
    pub rating: Option<String>,
    pub genres: Vec<String>,
    pub fetched_at: DateTime<Utc>,
}

/// How a slot's score was made up: signal name -> value in 0..1.
/// A `BTreeMap` keeps the names sorted, so the stored JSON is stable.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScoreBreakdown(pub BTreeMap<String, f64>);

/// One programme in the master guide, after providers were merged.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MasterSlot {
    pub id: SlotId,
    pub channel: ChannelId,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub title: String,
    pub subtitle: Option<String>,
    pub description: Option<String>,
    pub image_url: Option<String>,
    pub season: Option<u32>,
    pub episode: Option<u32>,
    pub rating: Option<String>,
    pub genres: Vec<String>,
    /// Provider whose data won arbitration for this slot.
    pub winner: ProviderId,
    pub score: f64,
    pub breakdown: ScoreBreakdown,
    pub updated_at: DateTime<Utc>,
}

/// The one place canonical channel ids are made: uppercase the call sign,
/// drop everything that is not a letter or digit, append `.us`.
/// `WCCO-DT`, `WCCODT` and `WCCO DT` all become `WCCODT.us`.
pub fn canonical_id(call_sign: &str) -> ChannelId {
    let mut s: String = call_sign
        .chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(char::to_uppercase)
        .collect();
    s.push_str(".us");
    ChannelId(s)
}

/// The one place a canonical channel is made from a provider's channel: id
/// from the call sign (`canonical_id`, so guide data lines up), the
/// provider's number, and the call sign as the name. Ingest stores exactly
/// this for every channel a response lists; tuner matching starts from it
/// and replaces number and name with what the tuner shows.
pub fn canonical_channel(p: &ProviderChannel) -> CanonicalChannel {
    CanonicalChannel {
        id: canonical_id(&p.call_sign),
        number: p.number.clone(),
        name: p.call_sign.clone(),
        call_sign: p.call_sign.clone(),
    }
}
