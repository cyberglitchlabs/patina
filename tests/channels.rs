//! Channel sources, matching and overrides. Uses the real captured HDHomeRun
//! lineup and the real Gracenote grid; nothing here touches a network
//! except an in-test server on 127.0.0.1.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use axum::{Router, routing::get};
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use patina::channels::hdhomerun::{HdHomeRunSource, parse_lineup};
use patina::channels::matching::{Overrides, match_channels, normalize_call_sign};
use patina::channels::{
    ChannelSource, ChannelSourceLoader, ChannelSourceStatus, StaticSource, TunerChannel,
    load_overrides,
};
use patina::config::{LineupCfg, StaticChannel};
use patina::provider::gracenote::GracenoteProvider;
use patina::provider::{Provider, ProviderChannel, RawResponse};
use patina::store::connect;
use url::Url;

const LINEUP: &[u8] = include_bytes!("fixtures/hdhomerun/lineup.json");
const GRID: &[u8] = include_bytes!("fixtures/gracenote/grid-USA-OTA55401-6h.json");

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 31, 20, 0, 0).unwrap()
}

fn tuner() -> Vec<TunerChannel> {
    parse_lineup(LINEUP).unwrap()
}

/// The 89 real Gracenote channels, parsed by the real parser.
fn provider_channels() -> Vec<ProviderChannel> {
    let p = GracenoteProvider::new(
        patina::provider::gracenote::http_client(std::time::Duration::from_secs(10)).unwrap(),
        Url::parse("https://tvlistings.gracenote.com").unwrap(),
    );
    let lineup = LineupCfg {
        name: "home".into(),
        lineup_id: "USA-OTA55401".into(),
        postal_code: "55401".into(),
        country: "USA".into(),
    };
    let raw = RawResponse {
        status: 200,
        body: Bytes::from_static(GRID),
    };
    let w = p.parse(&lineup, &raw, now());
    assert_eq!(w.channels.len(), 89);
    w.channels
}

fn tc(number: &str, name: &str) -> TunerChannel {
    TunerChannel {
        number: number.into(),
        name: name.into(),
    }
}

fn overrides(toml: &str) -> Overrides {
    Overrides::from_toml(toml).unwrap()
}

// ---- sources ----

#[test]
fn hdhomerun_lineup_fixture_parses_107_entries() {
    let t = tuner();
    assert_eq!(t.len(), 107);
    assert!(t.contains(&tc("4.1", "WCCO-DT")));
    assert!(t.contains(&tc("5.6", "MeTOONS")));
    assert!(t.contains(&tc("5000", "Unknown")));
}

#[test]
fn lineup_skips_empty_guide_number_and_rejects_garbage() {
    let t = parse_lineup(
        br#"[{"GuideNumber":"","GuideName":"x"},{"GuideNumber":" 7.1 ","GuideName":"A"}]"#,
    )
    .unwrap();
    assert_eq!(t, vec![tc("7.1", "A")]);
    assert!(parse_lineup(b"<html>nope</html>").is_err());
}

#[tokio::test]
async fn hdhomerun_http_path_fetches_lineup_json() {
    let app = Router::new().route("/lineup.json", get(|| async { LINEUP }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let src = HdHomeRunSource::new(
        patina::provider::gracenote::http_client(std::time::Duration::from_secs(10)).unwrap(),
        Url::parse(&format!("http://{addr}")).unwrap(),
    );
    assert_eq!(src.load().await.unwrap().len(), 107);
}

#[tokio::test]
async fn oversized_tuner_lineup_is_a_failed_load() {
    let app = Router::new().route("/lineup.json", get(|| async { LINEUP }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client =
        patina::provider::gracenote::http_client(std::time::Duration::from_secs(10)).unwrap();
    let url = Url::parse(&format!("http://{addr}")).unwrap();
    // A limit below the fixture's size (production: 32 MiB).
    let small = HdHomeRunSource::new(client.clone(), url.clone()).with_max_body_bytes(100);
    let err = small.load().await.unwrap_err();
    assert!(
        format!("{err:#}").contains("larger than 100 bytes"),
        "{err:#}"
    );
    // The same body under a limit it fits in loads normally.
    let fits = HdHomeRunSource::new(client, url).with_max_body_bytes(LINEUP.len());
    assert_eq!(fits.load().await.unwrap().len(), 107);
}

#[tokio::test]
async fn static_source_roundtrip() {
    let cfg = vec![
        StaticChannel {
            number: "4.1".into(),
            name: "WCCO".into(),
            callsign: Some("WCCO-DT".into()),
        },
        StaticChannel {
            number: "5.1".into(),
            name: "KSTP".into(),
            callsign: None,
        },
    ];
    let src = StaticSource::from_config(&cfg);
    assert_eq!(
        src.load().await.unwrap(),
        vec![tc("4.1", "WCCO"), tc("5.1", "KSTP")]
    );
    let src = StaticSource::new(vec![tc("9.1", "X")]);
    assert_eq!(src.load().await.unwrap(), vec![tc("9.1", "X")]);
}

// ---- matching ----

#[test]
fn normalization_variants_agree() {
    for s in ["WCCO-DT", "WCCODT", "WCCO DT", "wcco-dt"] {
        assert_eq!(normalize_call_sign(s), "WCCODT");
    }
}

#[test]
fn matches_by_number_then_call_sign() {
    let r = match_channels(
        &[tc("4.1", "WCCO-DT")],
        &provider_channels(),
        &overrides(""),
    );
    assert_eq!(r.matched.len(), 1);
    let (t, c) = &r.matched[0];
    assert_eq!(t.number, "4.1");
    assert_eq!(c.id.0, "WCCODT.us");
    assert_eq!(c.number, "4.1");
    assert_eq!(c.name, "WCCO-DT");
    assert_eq!(c.call_sign, "WCCODT");
}

#[test]
fn call_sign_match_when_number_absent_from_provider() {
    // Number 99.9 exists nowhere, but the name equals a provider call sign.
    let r = match_channels(
        &[tc("99.9", "WCCO DT")],
        &provider_channels(),
        &overrides(""),
    );
    assert_eq!(r.matched.len(), 1);
    assert_eq!(r.matched[0].1.id.0, "WCCODT.us");
}

#[test]
fn duplicate_provider_number_5_6_is_ambiguous_until_overridden() {
    let provider = provider_channels();
    let entry = tc("5.6", "MeTOONS");
    let r = match_channels(std::slice::from_ref(&entry), &provider, &overrides(""));
    assert!(r.matched.is_empty() && r.unmatched.is_empty());
    assert_eq!(r.ambiguous.len(), 1);
    let cands: Vec<&str> = r.ambiguous[0]
        .1
        .iter()
        .map(|c| c.call_sign.as_str())
        .collect();
    assert!(
        cands.contains(&"KSTCDT6") && cands.contains(&"KSTCDT4"),
        "{cands:?}"
    );

    let id = provider
        .iter()
        .find(|c| c.call_sign == "KSTCDT6")
        .unwrap()
        .provider_channel_id
        .clone();
    let o = overrides(&format!(
        "[[override]]\nnumber = \"5.6\"\nprovider_channel_id = \"{id}\"\n"
    ));
    let r = match_channels(&[entry], &provider, &o);
    assert!(r.ambiguous.is_empty());
    assert_eq!(r.matched.len(), 1);
    assert_eq!(r.matched[0].1.id.0, "KSTCDT6.us");
    assert_eq!(r.matched[0].1.number, "5.6");
}

#[test]
fn duplicate_number_is_disambiguated_by_call_sign() {
    let r = match_channels(
        &[tc("5.6", "KSTC-DT4")],
        &provider_channels(),
        &overrides(""),
    );
    assert_eq!(r.matched.len(), 1);
    assert_eq!(r.matched[0].1.id.0, "KSTCDT4.us");
}

#[test]
fn override_ignore_removes_unknown_5000() {
    let t = tuner();
    let plain = match_channels(&t, &provider_channels(), &overrides(""));
    assert!(plain.unmatched.contains(&tc("5000", "Unknown")));
    assert!(plain.ignored.is_empty());

    let o = overrides("[[override]]\nnumber = \"5000\"\nignore = true\n");
    let r = match_channels(&t, &provider_channels(), &o);
    assert_eq!(r.ignored, vec![tc("5000", "Unknown")]);
    assert!(!r.unmatched.iter().any(|c| c.number == "5000"));
}

#[test]
fn override_maps_tuner_name_to_provider_channel_id() {
    let provider = provider_channels();
    let target = provider.iter().find(|c| c.call_sign == "WCCODT").unwrap();
    let o = overrides(&format!(
        "[[override]]\nname = \"MeTOONS\"\nprovider_channel_id = \"{}\"\n",
        target.provider_channel_id
    ));
    let r = match_channels(&[tc("5.6", "MeTOONS")], &provider, &o);
    assert_eq!(r.matched.len(), 1);
    assert_eq!(r.matched[0].1.id.0, "WCCODT.us");
    assert_eq!(r.matched[0].1.number, "5.6");
}

#[test]
fn override_with_unknown_provider_id_is_unmatched() {
    let o = overrides("[[override]]\nnumber = \"4.1\"\nprovider_channel_id = \"nope\"\n");
    let r = match_channels(&[tc("4.1", "WCCO-DT")], &provider_channels(), &o);
    assert!(r.matched.is_empty());
    assert_eq!(r.unmatched.len(), 1);
}

#[test]
fn override_canonical_creates_channel() {
    let o = overrides("[[override]]\nnumber = \"5000\"\ncanonical = \"MYCHAN.us\"\n");
    let r = match_channels(&[tc("5000", "Unknown")], &provider_channels(), &o);
    assert_eq!(r.matched.len(), 1);
    let c = &r.matched[0].1;
    assert_eq!(
        (
            c.id.0.as_str(),
            c.number.as_str(),
            c.name.as_str(),
            c.call_sign.as_str()
        ),
        ("MYCHAN.us", "5000", "Unknown", "Unknown")
    );
}

#[test]
fn unmatched_entries_are_omitted_unless_include_unmatched() {
    // The omission itself is applied when rendering (Task 11); here the
    // result must list them so the renderer can decide.
    let r = match_channels(
        &[tc("4.1", "WCCO-DT"), tc("5000", "Unknown")],
        &provider_channels(),
        &overrides(""),
    );
    assert_eq!(r.matched.len(), 1);
    assert_eq!(r.unmatched, vec![tc("5000", "Unknown")]);
}

#[test]
fn every_fixture_entry_lands_in_exactly_one_list() {
    let t = tuner();
    let o = overrides("[[override]]\nnumber = \"5000\"\nignore = true\n");
    for ov in [overrides(""), o] {
        let r = match_channels(&t, &provider_channels(), &ov);
        assert_eq!(
            r.matched.len() + r.unmatched.len() + r.ambiguous.len() + r.ignored.len(),
            107
        );
        assert!(!r.matched.is_empty());
    }
}

// ---- override file errors ----

fn err(toml: &str) -> String {
    Overrides::from_toml(toml).unwrap_err().to_string()
}

#[test]
fn override_errors_name_the_entry_and_the_problem() {
    let e = err(
        "[[override]]\nnumber = \"1.1\"\nignore = true\n[[override]]\nnumber = \"5.6\"\nname = \"X\"\nignore = true\n",
    );
    assert!(
        e.contains("entry 2") && e.contains("5.6") && e.contains("both"),
        "{e}"
    );
    let e = err("[[override]]\nignore = true\n");
    assert!(
        e.contains("entry 1") && e.contains("number") && e.contains("name"),
        "{e}"
    );
    let e = err("[[override]]\nnumber = \"7.1\"\n");
    assert!(
        e.contains("entry 1") && e.contains("7.1") && e.contains("action"),
        "{e}"
    );
    let e = err("[[override]]\nnumber = \"7.1\"\nignore = true\ncanonical = \"A.us\"\n");
    assert!(e.contains("7.1") && e.contains("only one"), "{e}");
    let e = err("[[override]]\nnumber = \"7.1\"\nignore = true\nbogus = 1\n");
    assert!(
        e.contains("entry 1") && e.contains("7.1") && e.contains("bogus"),
        "{e}"
    );
    let e = err(
        "[[override]]\nnumber = \"7.1\"\nignore = true\n[[override]]\nnumber = \"7.1\"\ncanonical = \"A.us\"\n",
    );
    assert!(e.contains("entry 2") && e.contains("duplicate"), "{e}");
    let e = err("[[overide]]\nnumber = \"7.1\"\n");
    assert!(e.contains("overide"), "{e}");
}

#[test]
fn load_overrides_reads_file_or_empty() {
    assert!(load_overrides(None).unwrap().is_empty());
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("o.toml");
    std::fs::write(&p, "[[override]]\nnumber = \"1.1\"\nignore = true\n").unwrap();
    assert_eq!(load_overrides(Some(&p)).unwrap().len(), 1);
    std::fs::write(&p, "[[override]]\nnumber = \"1.1\"\n").unwrap();
    assert!(
        load_overrides(Some(&p))
            .unwrap_err()
            .to_string()
            .contains("entry 1")
    );
    assert!(load_overrides(Some(&dir.path().join("missing.toml"))).is_err());
}

// ---- last-known fallback ----

/// A source that works or fails on demand.
struct Flaky(std::sync::Mutex<Option<Vec<TunerChannel>>>);

#[async_trait]
impl ChannelSource for Flaky {
    async fn load(&self) -> Result<Vec<TunerChannel>> {
        self.0
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow!("connection refused"))
    }
}

#[tokio::test]
async fn hdhomerun_unreachable_falls_back_to_last_known_list_and_marks_stale() {
    let store = connect("sqlite::memory:").await.unwrap();
    let store: Arc<dyn patina::store::Store> = store;
    let loader = ChannelSourceLoader;
    let flaky = Flaky(std::sync::Mutex::new(None));

    // Never loaded and unreachable: empty, stale, error recorded.
    let r = loader
        .load_with_fallback("tuner", &flaky, store.as_ref(), now())
        .await;
    assert!(r.channels.is_empty() && r.stale && r.loaded_at.is_none());
    assert!(r.error.unwrap().contains("connection refused"));

    // Reachable: fresh, and persisted.
    *flaky.0.lock().unwrap() = Some(tuner());
    let r = loader
        .load_with_fallback("tuner", &flaky, store.as_ref(), now())
        .await;
    assert!(!r.stale && r.error.is_none());
    assert_eq!((r.channels.len(), r.loaded_at), (107, Some(now())));

    // Unreachable again, an hour later: last-known list, marked stale.
    *flaky.0.lock().unwrap() = None;
    let later = now() + chrono::TimeDelta::hours(1);
    let r = loader
        .load_with_fallback("tuner", &flaky, store.as_ref(), later)
        .await;
    assert!(r.stale);
    assert_eq!(r.channels.len(), 107);
    assert_eq!(
        r.loaded_at,
        Some(now()),
        "loaded_at is when the list was last good"
    );
    assert!(r.error.as_deref().unwrap().contains("connection refused"));

    let status = ChannelSourceStatus::from(&r);
    assert_eq!(status.channels_total, 107);
    assert!(status.stale);
    assert_eq!(status.loaded_at, Some(now()));
    assert!(status.error.is_some());
}

#[tokio::test]
async fn empty_tuner_answer_keeps_the_last_known_list() {
    // A tuner in the middle of a channel scan answers `[]`. That must not
    // replace a good list: it is handled like a failed load.
    let store = connect("sqlite::memory:").await.unwrap();
    let store: Arc<dyn patina::store::Store> = store;
    let loader = ChannelSourceLoader;
    let flaky = Flaky(std::sync::Mutex::new(Some(tuner())));
    let r = loader
        .load_with_fallback("tuner", &flaky, store.as_ref(), now())
        .await;
    assert_eq!((r.channels.len(), r.stale), (107, false));

    *flaky.0.lock().unwrap() = Some(Vec::new());
    let later = now() + chrono::TimeDelta::hours(1);
    let r = loader
        .load_with_fallback("tuner", &flaky, store.as_ref(), later)
        .await;
    assert!(r.stale, "{r:?}");
    assert_eq!(r.channels.len(), 107, "the old list is still served");
    assert_eq!(
        r.loaded_at,
        Some(now()),
        "loaded_at is when it was last good"
    );
    assert!(
        r.error.as_deref().unwrap().contains("no channels"),
        "{:?}",
        r.error
    );
    // The store still holds the old list.
    let (saved, saved_at) = store.load_tuner_channels("tuner").await.unwrap().unwrap();
    assert_eq!((saved.len(), saved_at), (107, now()));
}

#[tokio::test]
async fn first_ever_empty_tuner_answer_is_not_a_loaded_list() {
    let store = connect("sqlite::memory:").await.unwrap();
    let store: Arc<dyn patina::store::Store> = store;
    let flaky = Flaky(std::sync::Mutex::new(Some(Vec::new())));
    let r = ChannelSourceLoader
        .load_with_fallback("tuner", &flaky, store.as_ref(), now())
        .await;
    assert!(
        r.channels.is_empty() && r.stale && r.loaded_at.is_none(),
        "{r:?}"
    );
    assert!(r.error.as_deref().unwrap().contains("no channels"));
    assert!(store.load_tuner_channels("tuner").await.unwrap().is_none());
}

// ---- name selector semantics ----

fn names_hit(o: &Overrides, tuner: &[TunerChannel]) -> Vec<String> {
    // `ignore` actions put hit entries in `ignored`; report their numbers.
    match_channels(tuner, &[], o)
        .ignored
        .iter()
        .map(|t| t.number.clone())
        .collect()
}

#[test]
fn name_selector_distinguishes_fox_9_from_fox_9_plus() {
    let t = tuner();
    let o = overrides(
        "[[override]]\nname = \"FOX 9\"\nignore = true\n\
         [[override]]\nname = \"FOX 9+\"\ncanonical = \"PLUS.us\"\n",
    );
    let r = match_channels(&t, &provider_channels(), &o);
    // Case-insensitive exact: "FOX 9" hits 9.1 and 9.9 ("Fox 9"), not "FOX9 +".
    let mut ign: Vec<&str> = r.ignored.iter().map(|c| c.number.as_str()).collect();
    ign.sort();
    assert_eq!(ign, vec!["9.1", "9.9"]);
    let plus: Vec<&str> = r
        .matched
        .iter()
        .filter(|(_, c)| c.id.0 == "PLUS.us")
        .map(|(t, _)| t.number.as_str())
        .collect();
    assert_eq!(plus, vec!["9.10"]);
}

#[test]
fn name_selector_is_case_insensitive_and_trimmed() {
    let t = [tc("9.1", "FOX 9"), tc("9.2", " fox 9 ")];
    let o = overrides("[[override]]\nname = \"  Fox 9\\t\"\nignore = true\n");
    assert_eq!(names_hit(&o, &t), vec!["9.1", "9.2"]);
    // Same selector in different case/spacing is a duplicate.
    let e = err(
        "[[override]]\nname = \"FOX 9\"\nignore = true\n[[override]]\nname = \" fox 9 \"\nignore = true\n",
    );
    assert!(e.contains("entry 2") && e.contains("duplicate"), "{e}");
}

#[test]
fn empty_name_selector_is_rejected() {
    for sel in ["name = \"\"", "name = \"   \""] {
        let e = err(&format!("[[override]]\n{sel}\nignore = true\n"));
        assert!(e.contains("entry 1") && e.contains("empty"), "{e}");
    }
}

#[test]
fn name_selector_matching_several_entries_applies_to_all() {
    // The real lineup has 3 "SMB" and 7 "KWJM-LD" entries.
    let t = tuner();
    let o = overrides("[[override]]\nname = \"SMB\"\nignore = true\n");
    let r = match_channels(&t, &provider_channels(), &o);
    assert_eq!(r.ignored.len(), 3);
    assert!(r.ignored.iter().all(|c| c.name == "SMB"));
    let o = overrides("[[override]]\nname = \"KWJM-LD\"\nignore = true\n");
    assert_eq!(
        match_channels(&t, &provider_channels(), &o).ignored.len(),
        7
    );
}

#[test]
fn earlier_override_wins_when_name_and_number_both_match() {
    let t = [tc("5000", "Unknown")];
    let a = overrides(
        "[[override]]\nname = \"Unknown\"\nignore = true\n\
         [[override]]\nnumber = \"5000\"\ncanonical = \"X.us\"\n",
    );
    let r = match_channels(&t, &[], &a);
    assert_eq!((r.ignored.len(), r.matched.len()), (1, 0));
    let b = overrides(
        "[[override]]\nnumber = \"5000\"\ncanonical = \"X.us\"\n\
         [[override]]\nname = \"Unknown\"\nignore = true\n",
    );
    let r = match_channels(&t, &[], &b);
    assert_eq!((r.ignored.len(), r.matched.len()), (0, 1));
}

// ---- call-sign fallback safety and pinned real-data results ----

fn numbers(list: &[TunerChannel]) -> Vec<String> {
    list.iter().map(|t| t.number.clone()).collect()
}

#[test]
fn callsign_fallback_does_not_claim_a_provider_channel_already_matched_by_number() {
    let r = match_channels(&tuner(), &provider_channels(), &overrides(""));
    let un = numbers(&r.unmatched);
    for n in [
        "15.5", "15.6", "15.7", "33.8", "43.7", "49.5", "49.6", "49.7",
    ] {
        assert!(un.contains(&n.to_string()), "{n} should be unmatched");
    }
    let id_of = |num: &str| {
        r.matched
            .iter()
            .find(|(t, _)| t.number == num)
            .map(|(_, c)| c.id.0.clone())
    };
    assert_eq!(id_of("15.1").as_deref(), Some("KWJMLD.us"));
    assert_eq!(id_of("33.1").as_deref(), Some("K33LND.us"));
    assert_eq!(id_of("43.1").as_deref(), Some("KMBDLD.us"));
    assert_eq!(id_of("49.1").as_deref(), Some("KMQVLD.us"));
}

/// Overrides that deliberately point two entries at one provider channel are
/// outside this invariant; these two sets do not do that.
#[test]
fn automatic_matches_have_unique_canonical_ids() {
    for ov in [overrides(""), realistic_overrides()] {
        let r = match_channels(&tuner(), &provider_channels(), &ov);
        let mut ids: Vec<&str> = r.matched.iter().map(|(_, c)| c.id.0.as_str()).collect();
        let before = ids.len();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), before, "duplicate canonical ids");
    }
}

fn pid(call_sign: &str) -> String {
    provider_channels()
        .iter()
        .find(|c| c.call_sign == call_sign)
        .unwrap()
        .provider_channel_id
        .clone()
}

/// What a user would write: fix Gracenote's 9.x numbering by hand, ignore the
/// junk 5000 entry, and pick one of the two Gracenote 5.6 channels.
fn realistic_overrides() -> Overrides {
    overrides(&format!(
        "[[override]]\nnumber = \"9.1\"\nprovider_channel_id = \"{}\"\n\
         [[override]]\nnumber = \"9.2\"\nprovider_channel_id = \"{}\"\n\
         [[override]]\nnumber = \"9.9\"\nprovider_channel_id = \"{}\"\n\
         [[override]]\nnumber = \"5000\"\nignore = true\n\
         [[override]]\nnumber = \"5.6\"\nprovider_channel_id = \"{}\"\n",
        pid("KMSPDT"),
        pid("WFTCDT"),
        // Gracenote's 9.1/9.9 are swapped relative to the tuner, so 9.9 must
        // move off KMSPDT too or two entries would share one channel.
        pid("WFTCDT2"),
        pid("KSTCDT6"),
    ))
}

// Counts below come from the 2026-10-31 fixtures (107 tuner entries from the
// real HDHomeRun lineup, 89 channels from the real Gracenote grid).
//
// Known Gracenote-number mismatches, resolved via overrides: Gracenote numbers
// 9.1 as WFTCDT2, 9.9 as KMSPDT and 14.1 as K16HYD, which the matcher cannot
// tell is wrong because tuner names are not call signs.
#[test]
fn pinned_counts_without_overrides() {
    let r = match_channels(&tuner(), &provider_channels(), &overrides(""));
    assert_eq!(
        (
            r.matched.len(),
            r.ambiguous.len(),
            r.unmatched.len(),
            r.ignored.len()
        ),
        (82, 1, 24, 0)
    );
    assert_eq!(r.ambiguous[0].0.number, "5.6");
    let mut un = numbers(&r.unmatched);
    un.sort();
    let mut expect: Vec<&str> = vec![
        "2.6", "3.1", "3.2", "3.3", "3.4", "17.1", "17.2", "17.3", "17.4", "17.5", "23.10", "24.1",
        "24.2", "24.3", "24.4", "5000", "15.5", "15.6", "15.7", "33.8", "43.7", "49.5", "49.6",
        "49.7",
    ];
    expect.sort();
    assert_eq!(un, expect);
}

#[test]
fn pinned_counts_with_realistic_overrides() {
    let r = match_channels(&tuner(), &provider_channels(), &realistic_overrides());
    // 5000 ignored; 5.6 resolved (ambiguous -> matched); the rest unchanged.
    assert_eq!(
        (
            r.matched.len(),
            r.ambiguous.len(),
            r.unmatched.len(),
            r.ignored.len()
        ),
        (83, 0, 23, 1)
    );
    let id_of = |num: &str| {
        r.matched
            .iter()
            .find(|(t, _)| t.number == num)
            .map(|(_, c)| c.id.0.clone())
    };
    assert_eq!(id_of("9.1").as_deref(), Some("KMSPDT.us"));
    assert_eq!(id_of("9.2").as_deref(), Some("WFTCDT.us"));
    assert_eq!(id_of("5.6").as_deref(), Some("KSTCDT6.us"));
    assert_eq!(r.ignored, vec![tc("5000", "Unknown")]);
}
