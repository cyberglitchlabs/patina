//! XMLTV rendering: turns channels and master slots into the XML document
//! Jellyfin reads. Pure functions only (no store, no clock), so everything
//! here is tested against plain values.
//!
//! The XML is written by hand rather than with an XML library: the shape is
//! tiny and fixed, and writing it ourselves keeps every escaping decision in
//! one visible place ([`esc`]).

use std::collections::HashMap;
use std::fmt::Write;

use chrono::{DateTime, Datelike, Utc};
use chrono_tz::Tz;

use crate::model::{CanonicalChannel, MasterSlot};

/// Render a complete XMLTV document.
///
/// * `channels` are written first, in the given order; the XMLTV `id` is the
///   canonical id and the display name is `"<number> <name>"`.
/// * programmes follow, grouped in the order of `channels` and sorted by
///   start time within a channel. Slots whose channel is not in `channels`
///   are skipped, as are slots whose times XMLTV cannot express (a year
///   outside 0..=9999).
/// * times carry the UTC offset that applies at that very instant in `tz`,
///   so a feed crossing a daylight-saving change switches offset mid-feed.
pub fn render(channels: &[CanonicalChannel], slots: &[MasterSlot], tz: Tz) -> String {
    let mut out = String::with_capacity(256 + channels.len() * 160 + slots.len() * 400);
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    out.push_str("<tv generator-info-name=\"patina\">\n");

    for c in channels {
        let _ = writeln!(
            out,
            "  <channel id=\"{}\">\n    <display-name>{}</display-name>\n  </channel>",
            esc(&c.id.0, true),
            esc(&format!("{} {}", c.number, c.name), false),
        );
    }

    // Group slots by channel position in the feed (first occurrence wins if
    // a channel id is repeated), then sort each group by start (and end, so
    // the order is total and the output deterministic).
    let mut position: HashMap<&str, usize> = HashMap::new();
    for (i, c) in channels.iter().enumerate() {
        position.entry(c.id.0.as_str()).or_insert(i);
    }
    let mut ordered: Vec<(usize, &MasterSlot)> = slots
        .iter()
        .filter_map(|s| position.get(s.channel.0.as_str()).map(|p| (*p, s)))
        .collect();
    ordered.sort_by(|(pa, a), (pb, b)| (pa, a.start, a.end, a.id).cmp(&(pb, b.start, b.end, b.id)));

    for (_, s) in ordered {
        let (Some(start), Some(stop)) = (xmltv_time(s.start, tz), xmltv_time(s.end, tz)) else {
            continue;
        };
        write_programme(&mut out, s, &start, &stop);
    }

    out.push_str("</tv>\n");
    out
}

fn write_programme(out: &mut String, s: &MasterSlot, start: &str, stop: &str) {
    let _ = writeln!(
        out,
        "  <programme start=\"{start}\" stop=\"{stop}\" channel=\"{}\">",
        esc(&s.channel.0, true)
    );
    // Child order follows the XMLTV DTD: title, sub-title, desc, category,
    // icon, episode-num, rating.
    let _ = writeln!(
        out,
        "    <title lang=\"en\">{}</title>",
        esc(&s.title, false)
    );
    if let Some(t) = non_blank(&s.subtitle) {
        let _ = writeln!(
            out,
            "    <sub-title lang=\"en\">{}</sub-title>",
            esc(t, false)
        );
    }
    if let Some(d) = non_blank(&s.description) {
        let _ = writeln!(out, "    <desc lang=\"en\">{}</desc>", esc(d, false));
    }
    for g in s.genres.iter().filter(|g| !g.trim().is_empty()) {
        let _ = writeln!(
            out,
            "    <category lang=\"en\">{}</category>",
            esc(g, false)
        );
    }
    // Only plain web links: a `javascript:`, `data:` or `file:` URL from a
    // provider must never reach a client as an image source.
    if let Some(url) = non_blank(&s.image_url).filter(|u| is_http_url(u)) {
        let _ = writeln!(out, "    <icon src=\"{}\"/>", esc(url, true));
    }
    // Season/episode 0 are treated as "unknown": providers use 0 for "no
    // value", and a zero-based number cannot be made from it anyway.
    let season = s.season.filter(|n| *n > 0);
    let episode = s.episode.filter(|n| *n > 0);
    if season.is_some() || episode.is_some() {
        // xmltv_ns is "season.episode.part", all zero-based; unknown parts
        // stay empty but keep their dots (episode 5 only => ".4.").
        let part = |n: Option<u32>| n.map(|n| (n - 1).to_string()).unwrap_or_default();
        let _ = writeln!(
            out,
            "    <episode-num system=\"xmltv_ns\">{}.{}.</episode-num>",
            part(season),
            part(episode)
        );
    }
    if let (Some(se), Some(ep)) = (season, episode) {
        let _ = writeln!(
            out,
            "    <episode-num system=\"onscreen\">S{se:02}E{ep:02}</episode-num>"
        );
    }
    if let Some(r) = non_blank(&s.rating) {
        let _ = writeln!(out, "    <rating><value>{}</value></rating>", esc(r, false));
    }
    out.push_str("  </programme>\n");
}

fn non_blank(s: &Option<String>) -> Option<&str> {
    s.as_deref().filter(|s| !s.trim().is_empty())
}

/// `YYYYMMDDHHMMSS +ZZZZ` in `tz`, or `None` if the local year does not fit
/// in four digits. The offset is looked up for this exact instant, which is
/// what makes the two 01:30s of a fall-back night come out differently.
/// Sub-second parts are dropped by the formatter.
fn xmltv_time(t: DateTime<Utc>, tz: Tz) -> Option<String> {
    let local = t.with_timezone(&tz);
    if !(0..=9999).contains(&local.year()) {
        return None;
    }
    Some(local.format("%Y%m%d%H%M%S %z").to_string())
}

/// Is `c` allowed in an XML 1.0 document? Tab, newline and carriage return
/// are; the other C0 controls are not. The C1 controls (U+007F to U+009F)
/// are technically legal but "discouraged" and trip up some parsers, so they
/// are dropped too, as are the non-characters U+FFFE and U+FFFF.
fn xml_ok(c: char) -> bool {
    matches!(c, '\t' | '\n' | '\r')
        || matches!(c, '\u{20}'..='\u{7E}')
        || matches!(c, '\u{A0}'..='\u{D7FF}')
        || matches!(c, '\u{E000}'..='\u{FFFD}')
        || matches!(c, '\u{10000}'..='\u{10FFFF}')
}

/// Escape text for XML. Invalid characters are removed; `&`, `<`, `>`, `"`
/// and `'` become entities. A carriage return becomes `&#13;` (a raw one
/// would be normalized to a newline by parsers); in attributes tab and
/// newline are also written as references for the same reason. Everything
/// else, including emoji and non-Latin text, passes through unchanged.
pub fn esc(s: &str, attribute: bool) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for c in s.chars().filter(|c| xml_ok(*c)) {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            '\r' => out.push_str("&#13;"),
            '\n' if attribute => out.push_str("&#10;"),
            '\t' if attribute => out.push_str("&#9;"),
            c => out.push(c),
        }
    }
    out
}

/// True when `url` starts with exactly `http://` or `https://` (the scheme
/// compared without regard to case) and has something after it. Everything
/// else (`javascript:`, `data:`, `file:`, `ftp:`, `httpx:`, relative links)
/// is not an image link we hand to clients.
pub fn is_http_url(url: &str) -> bool {
    let lower = url.get(..8).unwrap_or(url).to_ascii_lowercase();
    let rest = if lower.starts_with("https://") {
        &url[8..]
    } else if lower.starts_with("http://") {
        &url[7..]
    } else {
        return false;
    };
    !rest.is_empty()
}
