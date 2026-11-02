//! XMLTV rendering: pure function tests (no store, no HTTP).

use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use chrono_tz::{America::Chicago, Tz};
use patina::model::{CanonicalChannel, ChannelId, MasterSlot, ProviderId, ScoreBreakdown, SlotId};
use patina::xmltv::render;

fn chan(id: &str, number: &str, name: &str) -> CanonicalChannel {
    CanonicalChannel {
        id: ChannelId(id.into()),
        number: number.into(),
        name: name.into(),
        call_sign: name.into(),
    }
}

fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, mo, d, h, mi, s).unwrap()
}

fn slot(channel: &str, start: DateTime<Utc>, end: DateTime<Utc>, title: &str) -> MasterSlot {
    MasterSlot {
        id: SlotId(1),
        channel: ChannelId(channel.into()),
        start,
        end,
        title: title.into(),
        subtitle: None,
        description: None,
        image_url: None,
        season: None,
        episode: None,
        rating: None,
        genres: vec![],
        winner: ProviderId("gracenote".into()),
        score: 1.0,
        breakdown: ScoreBreakdown::default(),
        updated_at: utc(2026, 10, 31, 0, 0, 0),
    }
}

fn one_slot_xml(s: MasterSlot) -> String {
    let id = s.channel.0.clone();
    render(&[chan(&id, "4.1", "WCCO-DT")], &[s], Chicago)
}

fn parse(xml: &str) -> roxmltree::Document<'_> {
    roxmltree::Document::parse(xml).expect("output must be well-formed XML")
}

fn programme_attrs(xml: &str) -> Vec<(String, String)> {
    parse(xml)
        .descendants()
        .filter(|n| n.has_tag_name("programme"))
        .map(|n| {
            (
                n.attribute("start").unwrap().to_string(),
                n.attribute("stop").unwrap().to_string(),
            )
        })
        .collect()
}

#[test]
fn golden_output_for_fixture_slots() {
    let channels = vec![
        chan("WCCODT.us", "4.1", "WCCO-DT"),
        chan("KAREDT.us", "11.1", "KARE-HD"),
        chan("EMPTY.us", "99.1", "No Data"),
    ];
    let mut a = slot(
        "WCCODT.us",
        utc(2026, 10, 31, 22, 0, 0),
        utc(2026, 10, 31, 23, 0, 0),
        "CBS Evening News",
    );
    a.subtitle = Some("Saturday".into());
    a.description = Some("The day's news & weather.".into());
    a.genres = vec!["News".into(), "Weather".into()];
    a.image_url = Some("https://img.example/a.jpg?w=1&h=2".into());
    a.season = Some(2);
    a.episode = Some(5);
    a.rating = Some("TV-G".into());
    let b = slot(
        "WCCODT.us",
        utc(2026, 10, 31, 23, 0, 0),
        utc(2026, 11, 1, 0, 30, 0),
        "Late Show",
    );
    let c = slot(
        "KAREDT.us",
        utc(2026, 10, 31, 22, 0, 0),
        utc(2026, 10, 31, 22, 30, 0),
        "Local News",
    );
    // Deliberately out of order: render sorts by channel (feed order), start.
    let xml = render(&channels, &[c, b, a], Chicago);

    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/golden/fixture_slots.xml"
    );
    // Regenerate only on request, never silently.
    if std::env::var("UPDATE_GOLDEN").is_ok() {
        std::fs::write(path, &xml).unwrap();
        eprintln!("UPDATE_GOLDEN set: rewrote {path}");
    }
    let expected = std::fs::read_to_string(path)
        .expect("golden file missing; run with UPDATE_GOLDEN=1 to create it");
    assert_eq!(xml, expected, "output differs from {path}");
}

#[test]
fn channel_display_names_use_number_and_name() {
    let xml = render(&[chan("WCCODT.us", "4.1", "WCCO-DT")], &[], Chicago);
    let doc = parse(&xml);
    let ch = doc
        .descendants()
        .find(|n| n.has_tag_name("channel"))
        .unwrap();
    assert_eq!(ch.attribute("id"), Some("WCCODT.us"));
    let dn = ch
        .children()
        .find(|n| n.has_tag_name("display-name"))
        .unwrap();
    assert_eq!(dn.text(), Some("4.1 WCCO-DT"));
    assert_eq!(
        doc.root_element().attribute("generator-info-name"),
        Some("patina")
    );
}

#[test]
fn icon_tag_present_when_image_url_exists() {
    let mut s = slot(
        "WCCODT.us",
        utc(2026, 10, 31, 22, 0, 0),
        utc(2026, 10, 31, 23, 0, 0),
        "X",
    );
    let without = one_slot_xml(s.clone());
    assert!(
        !parse(&without)
            .descendants()
            .any(|n| n.has_tag_name("icon"))
    );
    s.image_url = Some("https://img.example/p.png".into());
    let xml = one_slot_xml(s);
    let doc = parse(&xml);
    let icon = doc.descendants().find(|n| n.has_tag_name("icon")).unwrap();
    assert_eq!(icon.attribute("src"), Some("https://img.example/p.png"));
}

#[test]
fn icon_with_non_http_scheme_is_dropped() {
    let base = slot(
        "WCCODT.us",
        utc(2026, 10, 31, 22, 0, 0),
        utc(2026, 10, 31, 23, 0, 0),
        "X",
    );
    for bad in [
        "javascript:alert(1)",
        "data:image/png;base64,AAAA",
        "file:///etc/passwd",
        "ftp://img.example/p.png",
        "httpx://img.example/p.png",
        "http:/img.example/p.png",
        "//img.example/p.png",
        "p1_aa",
    ] {
        let mut s = base.clone();
        s.image_url = Some(bad.into());
        let xml = one_slot_xml(s);
        assert!(
            !parse(&xml).descendants().any(|n| n.has_tag_name("icon")),
            "{bad}: {xml}"
        );
    }
    for good in ["http://img.example/p.png", "https://img.example/p.png"] {
        let mut s = base.clone();
        s.image_url = Some(good.into());
        let xml = one_slot_xml(s);
        let doc = parse(&xml);
        let icon = doc.descendants().find(|n| n.has_tag_name("icon"));
        assert_eq!(icon.and_then(|i| i.attribute("src")), Some(good));
    }
}

#[test]
fn offsets_correct_across_us_fall_back_2026_11_01() {
    // America/Chicago falls back when 02:00 CDT (-0500) becomes 01:00 CST
    // (-0600). 02:00 CDT is 07:00 UTC, so 07:00Z is the first -0600 instant
    // and the local hour 01:00-01:59 happens twice: first as CDT (06:00Z
    // to 06:59:59Z), again as CST (07:00Z to 07:59:59Z).
    let cases = [
        (utc(2026, 11, 1, 5, 30, 0), "20261101003000 -0500"), // 00:30 CDT
        (utc(2026, 11, 1, 6, 30, 0), "20261101013000 -0500"), // 01:30 CDT, first
        (utc(2026, 11, 1, 6, 59, 59), "20261101015959 -0500"), // last CDT second
        (utc(2026, 11, 1, 7, 0, 0), "20261101010000 -0600"),  // 01:00 CST
        (utc(2026, 11, 1, 7, 30, 0), "20261101013000 -0600"), // 01:30 CST, second
        (utc(2026, 11, 1, 8, 0, 0), "20261101020000 -0600"),  // 02:00 CST
    ];
    for (instant, expected) in cases {
        let xml = one_slot_xml(slot(
            "WCCODT.us",
            instant,
            instant + TimeDelta::minutes(30),
            "T",
        ));
        let (start, _) = &programme_attrs(&xml)[0];
        assert_eq!(start, expected, "instant {instant}");
        // Round trip: parsing the rendered text gives back the same instant.
        let parsed = DateTime::parse_from_str(start, "%Y%m%d%H%M%S %z").unwrap();
        assert_eq!(parsed.with_timezone(&Utc), instant);
    }
    // The two 01:30s render identically except for the offset.
    // A slot spanning the change has different offsets for start and stop.
    let xml = one_slot_xml(slot(
        "WCCODT.us",
        utc(2026, 11, 1, 6, 30, 0),
        utc(2026, 11, 1, 7, 30, 0),
        "Span",
    ));
    assert_eq!(
        programme_attrs(&xml)[0],
        (
            "20261101013000 -0500".to_string(),
            "20261101013000 -0600".to_string()
        )
    );
    // Before the day: still CDT. After: CST.
    let before = one_slot_xml(slot(
        "WCCODT.us",
        utc(2026, 10, 31, 22, 0, 0),
        utc(2026, 10, 31, 23, 0, 0),
        "B",
    ));
    assert_eq!(programme_attrs(&before)[0].0, "20261031170000 -0500");
    let after = one_slot_xml(slot(
        "WCCODT.us",
        utc(2026, 11, 1, 23, 0, 0),
        utc(2026, 11, 2, 0, 0, 0),
        "A",
    ));
    assert_eq!(programme_attrs(&after)[0].0, "20261101170000 -0600");
}

#[test]
fn spring_forward_2027_03_14_skips_the_02_00_hour() {
    // 07:59:59Z is 01:59:59 CST (-0600); 08:00Z is 03:00:00 CDT (-0500).
    let xml = one_slot_xml(slot(
        "WCCODT.us",
        utc(2027, 3, 14, 7, 59, 59),
        utc(2027, 3, 14, 8, 0, 0),
        "Gap",
    ));
    assert_eq!(
        programme_attrs(&xml)[0],
        (
            "20270314015959 -0600".to_string(),
            "20270314030000 -0500".to_string()
        )
    );
}

#[test]
fn escapes_amp_lt_gt_quotes_and_strips_xml_invalid_control_chars() {
    let mut s = slot(
        "WCCODT.us",
        utc(2026, 10, 31, 22, 0, 0),
        utc(2026, 10, 31, 23, 0, 0),
        "Tom & Jerry <\"live\"> it's \u{1}\u{8}\u{b}\u{c}\u{e}\u{1f}\u{7f}\u{85}\u{9f}\u{fffe}\u{ffff}end",
    );
    s.description = Some("a\tb\nc\rd & e".into());
    s.image_url = Some("https://x.example/?a=1&b=\"2\"<3>".into());
    let xml = one_slot_xml(s);
    // Raw output: markup characters are escaped, invalid chars are gone.
    assert!(xml.contains("Tom &amp; Jerry &lt;&quot;live&quot;&gt; it&apos;s end"));
    assert!(xml.contains("src=\"https://x.example/?a=1&amp;b=&quot;2&quot;&lt;3&gt;\""));
    for bad in [
        '\u{1}', '\u{8}', '\u{b}', '\u{c}', '\u{e}', '\u{1f}', '\u{7f}', '\u{85}', '\u{9f}',
        '\u{fffe}', '\u{ffff}',
    ] {
        assert!(!xml.contains(bad), "{bad:?} must be stripped");
    }
    // A strict parser accepts it and decodes back to the clean text.
    let doc = parse(&xml);
    let title = doc.descendants().find(|n| n.has_tag_name("title")).unwrap();
    assert_eq!(title.text(), Some("Tom & Jerry <\"live\"> it's end"));
    let icon = doc.descendants().find(|n| n.has_tag_name("icon")).unwrap();
    assert_eq!(
        icon.attribute("src"),
        Some("https://x.example/?a=1&b=\"2\"<3>")
    );
    let desc = doc.descendants().find(|n| n.has_tag_name("desc")).unwrap();
    // Tab and newline survive; the carriage return is written as a character
    // reference so the parser does not normalize it away.
    assert_eq!(desc.text(), Some("a\tb\nc\rd & e"));
}

#[test]
fn emoji_and_non_latin_text_roundtrip() {
    let title = "Привет мир 日本語 \u{1F600} café Ελληνικά العربية \u{1F468}\u{200D}\u{1F469}";
    let mut s = slot(
        "WCCODT.us",
        utc(2026, 10, 31, 22, 0, 0),
        utc(2026, 10, 31, 23, 0, 0),
        title,
    );
    s.genres = vec!["Новости".into()];
    let xml = render(
        &[chan("WCCODT.us", "4.1", "日本 TV \u{1F4FA}")],
        &[s],
        Chicago,
    );
    assert!(xml.contains(title), "text must pass through untouched");
    let doc = parse(&xml);
    let t = doc.descendants().find(|n| n.has_tag_name("title")).unwrap();
    assert_eq!(t.text(), Some(title));
    let dn = doc
        .descendants()
        .find(|n| n.has_tag_name("display-name"))
        .unwrap();
    assert_eq!(dn.text(), Some("4.1 日本 TV \u{1F4FA}"));
}

#[test]
fn output_parses_as_xml() {
    let channels = vec![
        chan("A.us", "1.1", "A"),
        chan("B.us", "2.1", "B & <C>"),
        chan("NONE.us", "3.1", "No programmes"),
    ];
    let mut slots = Vec::new();
    for i in 0..10 {
        let mut s = slot(
            if i % 2 == 0 { "A.us" } else { "B.us" },
            utc(2026, 10, 31, 12 + i, 0, 0),
            utc(2026, 10, 31, 13 + i, 0, 0),
            &format!("Show {i} & friends"),
        );
        s.season = Some(1);
        s.episode = Some(i);
        s.genres = vec!["Drama".into()];
        slots.push(s);
    }
    let xml = render(&channels, &slots, Chicago);
    assert!(xml.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
    let doc = parse(&xml);
    assert_eq!(doc.root_element().tag_name().name(), "tv");
    assert_eq!(
        doc.descendants()
            .filter(|n| n.has_tag_name("channel"))
            .count(),
        3
    );
    assert_eq!(
        doc.descendants()
            .filter(|n| n.has_tag_name("programme"))
            .count(),
        10
    );
    // DTD order: all channels precede all programmes.
    let names: Vec<_> = doc
        .root_element()
        .children()
        .filter(|n| n.is_element())
        .map(|n| n.tag_name().name().to_string())
        .collect();
    let first_prog = names.iter().position(|n| n == "programme").unwrap();
    assert!(names[..first_prog].iter().all(|n| n == "channel"));
    assert!(names[first_prog..].iter().all(|n| n == "programme"));
}

#[test]
fn episode_num_formats_for_season_episode_combinations() {
    // (season, episode, expected xmltv_ns, expected onscreen)
    type Case = (
        Option<u32>,
        Option<u32>,
        Option<&'static str>,
        Option<&'static str>,
    );
    let cases: [Case; 7] = [
        (Some(2), Some(5), Some("1.4."), Some("S02E05")),
        (None, Some(5), Some(".4."), None),
        (Some(2), None, Some("1.."), None),
        (None, None, None, None),
        (Some(1), Some(1), Some("0.0."), Some("S01E01")),
        (Some(12), Some(105), Some("11.104."), Some("S12E105")),
        // Zero means "unknown" from providers (it cannot be made zero-based).
        (Some(0), Some(0), None, None),
    ];
    for (season, episode, ns, onscreen) in cases {
        let mut s = slot(
            "WCCODT.us",
            utc(2026, 10, 31, 22, 0, 0),
            utc(2026, 10, 31, 23, 0, 0),
            "E",
        );
        s.season = season;
        s.episode = episode;
        let xml = one_slot_xml(s);
        let doc = parse(&xml);
        let get = |system: &str| {
            doc.descendants()
                .find(|n| n.has_tag_name("episode-num") && n.attribute("system") == Some(system))
                .map(|n| n.text().unwrap().to_string())
        };
        assert_eq!(get("xmltv_ns").as_deref(), ns, "{season:?} {episode:?}");
        assert_eq!(
            get("onscreen").as_deref(),
            onscreen,
            "{season:?} {episode:?}"
        );
    }
}

#[test]
fn programme_children_follow_dtd_order() {
    let mut s = slot(
        "WCCODT.us",
        utc(2026, 10, 31, 22, 0, 0),
        utc(2026, 10, 31, 23, 0, 0),
        "T",
    );
    s.subtitle = Some("S".into());
    s.description = Some("D".into());
    s.genres = vec!["G1".into(), "G2".into()];
    s.image_url = Some("https://i/x".into());
    s.season = Some(1);
    s.episode = Some(2);
    s.rating = Some("TV-14".into());
    let xml = one_slot_xml(s);
    let doc = parse(&xml);
    let p = doc
        .descendants()
        .find(|n| n.has_tag_name("programme"))
        .unwrap();
    let order: Vec<_> = p
        .children()
        .filter(|n| n.is_element())
        .map(|n| n.tag_name().name())
        .collect();
    assert_eq!(
        order,
        [
            "title",
            "sub-title",
            "desc",
            "category",
            "category",
            "icon",
            "episode-num",
            "episode-num",
            "rating"
        ]
    );
    let title = p.children().find(|n| n.has_tag_name("title")).unwrap();
    assert_eq!(title.attribute("lang"), Some("en"));
    let rating_value = p.descendants().find(|n| n.has_tag_name("value")).unwrap();
    assert_eq!(rating_value.text(), Some("TV-14"));
}

#[test]
fn sub_second_and_far_future_slots_render_valid_times() {
    let sub = Utc.timestamp_opt(1_793_484_000, 999_000_000).unwrap();
    let xml = one_slot_xml(slot(
        "WCCODT.us",
        sub,
        sub + TimeDelta::milliseconds(1500),
        "Sub",
    ));
    // Fractions are dropped, never rendered as part of the time.
    let (start, stop) = programme_attrs(&xml).remove(0);
    for t in [&start, &stop] {
        assert_eq!(t.len(), 20, "{t}");
        DateTime::parse_from_str(t, "%Y%m%d%H%M%S %z").unwrap();
    }
    // Far future: still a 4-digit year, and a slot beyond year 9999 (which
    // XMLTV cannot express) is skipped instead of producing a bad time.
    let far = utc(2099, 12, 31, 23, 0, 0);
    let ok = slot("WCCODT.us", far, far + TimeDelta::hours(1), "Far");
    let beyond = Utc.with_ymd_and_hms(10000, 1, 1, 0, 0, 0).unwrap();
    let bad = slot("WCCODT.us", beyond, beyond + TimeDelta::hours(1), "Beyond");
    let xml = render(&[chan("WCCODT.us", "4.1", "W")], &[ok, bad], Tz::UTC);
    let attrs = programme_attrs(&xml);
    assert_eq!(attrs.len(), 1);
    assert_eq!(attrs[0].0, "20991231230000 +0000");
}

#[test]
fn programmes_are_grouped_by_channel_then_start() {
    let channels = vec![chan("B.us", "2.1", "B"), chan("A.us", "1.1", "A")];
    let s = |c: &str, h: u32, t: &str| {
        let start = utc(2026, 10, 31, h, 0, 0);
        slot(c, start, start + TimeDelta::hours(1), t)
    };
    let slots = vec![
        s("A.us", 23, "a2"),
        s("B.us", 23, "b2"),
        s("A.us", 22, "a1"),
        s("B.us", 22, "b1"),
        s("GONE.us", 22, "x"),
    ];
    let xml = render(&channels, &slots, Chicago);
    let doc = parse(&xml);
    let titles: Vec<_> = doc
        .descendants()
        .filter(|n| n.has_tag_name("title"))
        .map(|n| n.text().unwrap().to_string())
        .collect();
    // Slots for a channel that is not in the feed are not rendered.
    assert_eq!(titles, ["b1", "b2", "a1", "a2"]);
}

/// Jellyfin learns a channel's number only from a purely numeric
/// `<display-name>` and matches tuner channels to the guide by it, so every
/// channel must carry its bare number as an extra display name.
#[test]
fn every_channel_has_a_bare_numeric_display_name() {
    let xml = render(
        &[
            chan("A.us", "4.1", "WCCO-DT"),
            chan("B.us", "11.1", "KARE 11"),
        ],
        &[],
        Chicago,
    );
    let doc = parse(&xml);
    for ch in doc.descendants().filter(|n| n.has_tag_name("channel")) {
        let names: Vec<_> = ch
            .children()
            .filter(|n| n.has_tag_name("display-name"))
            .map(|n| n.text().unwrap().to_string())
            .collect();
        assert!(names.len() >= 2, "{names:?}");
        assert!(
            names.iter().any(|n| n.parse::<f64>().is_ok()),
            "no numeric display name in {names:?}"
        );
    }
}
