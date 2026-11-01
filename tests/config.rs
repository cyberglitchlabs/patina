use std::collections::HashMap;

use patina::config::{Config, ConfigError, SourceKind};

fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn defaults_apply_with_empty_env_and_no_toml() {
    let c = Config::load(&HashMap::new(), None).unwrap();
    assert_eq!(c.listen.to_string(), "0.0.0.0:8080");
    assert_eq!(c.log, "info");
    assert_eq!(c.database_url, "sqlite://patina.db?mode=rwc");
    assert_eq!(c.timezone, chrono_tz::America::Chicago);
    assert_eq!(c.feed_token, None);
    assert_eq!(c.admin_token, None);
    assert_eq!(c.retention.images_days, 3);
    assert_eq!(c.retention.raw_days, 7);
    assert_eq!(c.retention.programmes_days, 7);
    assert_eq!(c.retention.history_days, 90);
    assert_eq!(c.disk_cap_bytes, None);
    assert_eq!(c.horizon_days, 7);
    assert!(c.providers.is_empty());
    assert!(c.sources.is_empty());
    assert_eq!(c.scoring.freshness, 0.25);
    assert_eq!(c.scoring.agreement, 0.0);
}

#[test]
fn env_overrides_toml() {
    let toml = r#"
listen = "127.0.0.1:1111"
timezone = "UTC"
horizon_days = 3
"#;
    let e = env(&[
        ("PATINA_LISTEN", "127.0.0.1:2222"),
        ("PATINA_HORIZON_DAYS", "5"),
    ]);
    let c = Config::load(&e, Some(toml)).unwrap();
    assert_eq!(c.listen.to_string(), "127.0.0.1:2222");
    assert_eq!(c.horizon_days, 5);
    // Not set in env, so the TOML value stands.
    assert_eq!(c.timezone, chrono_tz::UTC);
}

#[test]
fn full_toml_parses() {
    let toml = r#"
[retention]
images_days = 2

[[providers]]
kind = "gracenote"
name = "gn"
daily_budget = 100
[[providers.lineups]]
name = "home"
lineup_id = "USA-X"
postal_code = "60601"
country = "USA"

[[sources]]
kind = "hdhomerun"
url = "http://192.168.1.5"
lineups = ["home"]
include_unmatched = true

[[sources]]
kind = "static"
lineups = ["home"]
overrides_path = "overrides.toml"
[[sources.channels]]
number = "4.1"
name = "Channel A"

[scoring]
freshness = 0.5
"#;
    let c = Config::load(&HashMap::new(), Some(toml)).unwrap();
    assert_eq!(c.retention.images_days, 2);
    assert_eq!(c.retention.raw_days, 7);
    assert_eq!(c.providers[0].daily_budget, 100);
    assert_eq!(c.providers[0].lineups[0].lineup_id, "USA-X");
    assert!(c.sources[0].include_unmatched);
    assert!(matches!(c.sources[0].kind, SourceKind::HdHomeRun { .. }));
    assert!(matches!(c.sources[1].kind, SourceKind::Static { .. }));
    assert!(c.sources[1].overrides_path.is_some());
    assert_eq!(c.scoring.freshness, 0.5);
    assert_eq!(c.scoring.completeness, 0.2);
}

#[test]
fn invalid_toml_is_rejected_with_line_info() {
    let err = Config::load(&HashMap::new(), Some("listen = \"x\"\nbad line here\n")).unwrap_err();
    assert!(matches!(err, ConfigError::Toml(_)));
    assert!(err.to_string().contains("line 2"), "got: {err}");
}

#[test]
fn unknown_keys_are_rejected() {
    let err = Config::load(&HashMap::new(), Some("lissten = \"x\"\n")).unwrap_err();
    assert!(err.to_string().contains("lissten"), "got: {err}");
}

#[test]
fn unknown_lineup_reference_in_source_is_rejected() {
    let toml = r#"
[[providers]]
kind = "gracenote"
name = "gn"
[[providers.lineups]]
name = "home"
lineup_id = "USA-X"
postal_code = "60601"
country = "USA"

[[sources]]
kind = "hdhomerun"
url = "http://h"
lineups = ["nowhere"]
"#;
    let err = Config::load(&HashMap::new(), Some(toml)).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("source #1 (hdhomerun)"),
        "names the source: {msg}"
    );
    assert!(msg.contains("nowhere"), "names the lineup: {msg}");
}

#[test]
fn zero_daily_budget_is_rejected() {
    let toml = r#"
[[providers]]
kind = "gracenote"
name = "gn"
daily_budget = 0
"#;
    let err = Config::load(&HashMap::new(), Some(toml)).unwrap_err();
    assert!(err.to_string().contains("daily_budget"), "got: {err}");
}

#[test]
fn negative_or_zero_retention_is_rejected() {
    let err = Config::load(&HashMap::new(), Some("[retention]\nimages_days = 0\n")).unwrap_err();
    assert!(err.to_string().contains("images_days"), "got: {err}");
    // Negative numbers fail to parse into an unsigned field.
    assert!(Config::load(&HashMap::new(), Some("[retention]\nraw_days = -1\n")).is_err());
    // Env overrides are validated too.
    let e = env(&[("PATINA_RETENTION_HISTORY_DAYS", "0")]);
    assert!(Config::load(&e, None).is_err());
}

#[test]
fn bad_env_values_name_the_variable() {
    let err = Config::load(&env(&[("PATINA_LISTEN", "nope")]), None).unwrap_err();
    assert!(err.to_string().contains("PATINA_LISTEN"), "got: {err}");
    let err = Config::load(&env(&[("PATINA_TIMEZONE", "Mars/Base")]), None).unwrap_err();
    assert!(err.to_string().contains("PATINA_TIMEZONE"), "got: {err}");
}

#[test]
fn bad_weights_are_rejected() {
    let err = Config::load(&HashMap::new(), Some("[scoring]\nfreshness = -1.0\n")).unwrap_err();
    assert!(err.to_string().contains("freshness"), "got: {err}");
    let all_zero = "[scoring]\nfreshness=0.0\ncompleteness=0.0\ntimeline_sanity=0.0\nstability=0.0\nreliability=0.0\nhindsight_accuracy=0.0\nagreement=0.0\n";
    assert!(Config::load(&HashMap::new(), Some(all_zero)).is_err());
}

#[test]
fn redacted_output_hides_tokens_and_db_password() {
    let e = env(&[
        ("PATINA_FEED_TOKEN", "feedsecret"),
        ("PATINA_ADMIN_TOKEN", "adminsecret"),
        (
            "PATINA_DATABASE_URL",
            "postgres://patina:dbsecret@db.local:5432/patina",
        ),
    ]);
    let c = Config::load(&e, None).unwrap();
    let out = c.redacted();
    for secret in ["feedsecret", "adminsecret", "dbsecret"] {
        assert!(!out.contains(secret), "{secret} leaked in: {out}");
    }
    assert!(out.contains("db.local"), "host should remain: {out}");
}

#[test]
fn example_config_file_is_valid() {
    let text = std::fs::read_to_string("config.example.toml").unwrap();
    let c = Config::load(&HashMap::new(), Some(&text)).unwrap();
    assert_eq!(c.providers.len(), 1);
    assert_eq!(c.sources.len(), 1);
}

fn static_src(channel: &str) -> String {
    format!("[[sources]]\nkind = \"static\"\n[[sources.channels]]\n{channel}\n")
}

#[test]
fn static_channel_with_number_name_callsign_parses() {
    let t = static_src("number = \"5.1\"\nname = \"Five\"\ncallsign = \"WFIV\"");
    let c = Config::load(&HashMap::new(), Some(&t)).unwrap();
    match &c.sources[0].kind {
        SourceKind::Static { channels } => {
            assert_eq!(channels[0].number, "5.1");
            assert_eq!(channels[0].name, "Five");
            assert_eq!(channels[0].callsign.as_deref(), Some("WFIV"));
        }
        _ => panic!("expected static"),
    }
}

#[test]
fn static_source_with_no_channels_is_rejected() {
    let t = "[[sources]]\nkind = \"static\"\nchannels = []\n";
    let err = Config::load(&HashMap::new(), Some(t)).unwrap_err();
    assert!(err.to_string().contains("channels"), "got: {err}");
}

#[test]
fn static_channel_missing_number_is_rejected() {
    let t = static_src("name = \"Five\"");
    let err = Config::load(&HashMap::new(), Some(&t)).unwrap_err();
    assert!(err.to_string().contains("number"), "got: {err}");
}

#[test]
fn static_channel_with_unknown_id_field_is_rejected() {
    let t = static_src("number = \"5.1\"\nname = \"Five\"\nid = \"x\"");
    let err = Config::load(&HashMap::new(), Some(&t)).unwrap_err();
    assert!(err.to_string().contains("id"), "got: {err}");
}

#[test]
fn invalid_log_filter_is_rejected() {
    let err = Config::load(&env(&[("PATINA_LOG", "patina=notalevel")]), None).unwrap_err();
    assert!(err.to_string().contains("log filter"), "got: {err}");
    assert!(Config::load(&HashMap::new(), Some("log = \"a=b=c\"\n")).is_err());
}

#[test]
fn duplicate_provider_names_are_rejected() {
    let t = "[[providers]]\nkind=\"gracenote\"\nname=\"gn\"\n[[providers]]\nkind=\"gracenote\"\nname=\"gn\"\n";
    let err = Config::load(&HashMap::new(), Some(t)).unwrap_err();
    assert!(err.to_string().contains("'gn'"), "got: {err}");
}

#[test]
fn empty_provider_or_lineup_names_are_rejected() {
    let t = "[[providers]]\nkind=\"gracenote\"\nname=\"\"\n";
    assert!(Config::load(&HashMap::new(), Some(t)).is_err());
    let t = "[[providers]]\nkind=\"gracenote\"\nname=\"gn\"\n[[providers.lineups]]\nname=\"\"\nlineup_id=\"x\"\npostal_code=\"1\"\ncountry=\"USA\"\n";
    assert!(Config::load(&HashMap::new(), Some(t)).is_err());
    let t = "[[providers]]\nkind=\"gracenote\"\nname=\"gn\"\n[[providers.lineups]]\nname=\"h\"\nlineup_id=\"\"\npostal_code=\"1\"\ncountry=\"USA\"\n";
    assert!(Config::load(&HashMap::new(), Some(t)).is_err());
}

#[test]
fn provider_base_url_is_optional_and_validated() {
    let with =
        |extra: &str| format!("[[providers]]\nkind = \"gracenote\"\nname = \"gn\"\n{extra}\n");
    let c = Config::load(&HashMap::new(), Some(&with(""))).unwrap();
    assert_eq!(c.providers[0].base_url, None);
    let c = Config::load(
        &HashMap::new(),
        Some(&with("base_url = \"http://127.0.0.1:9000/\"")),
    )
    .unwrap();
    assert_eq!(
        c.providers[0].base_url.as_deref(),
        Some("http://127.0.0.1:9000/")
    );
    for bad in ["not a url", "ftp://example.com/"] {
        let err = Config::load(
            &HashMap::new(),
            Some(&with(&format!("base_url = \"{bad}\""))),
        )
        .unwrap_err();
        assert!(err.to_string().contains("base_url"), "{bad}: {err}");
    }
}

#[test]
fn empty_token_variables_are_flagged_but_unset_ones_are_not() {
    use patina::config::empty_token_vars;
    assert!(empty_token_vars(&env(&[])).is_empty());
    assert!(empty_token_vars(&env(&[("PATINA_ADMIN_TOKEN", "x")])).is_empty());
    assert_eq!(
        empty_token_vars(&env(&[("PATINA_ADMIN_TOKEN", "")])),
        ["PATINA_ADMIN_TOKEN"]
    );
    assert_eq!(
        empty_token_vars(&env(&[
            ("PATINA_FEED_TOKEN", ""),
            ("PATINA_ADMIN_TOKEN", "")
        ])),
        ["PATINA_ADMIN_TOKEN", "PATINA_FEED_TOKEN"]
    );
}

#[test]
fn empty_token_variable_warns_only_when_no_config_file_token_applies() {
    use patina::config::{EmptyTokenNotice, empty_token_notices};
    let toml = "admin_token = \"from-file\"\n";
    let e = env(&[("PATINA_ADMIN_TOKEN", ""), ("PATINA_FEED_TOKEN", "")]);
    let c = Config::load(&e, Some(toml)).unwrap();
    // The admin token from the file applies; the feed has none at all.
    assert_eq!(c.admin_token.as_deref(), Some("from-file"));
    assert_eq!(
        empty_token_notices(&e, &c),
        [
            EmptyTokenNotice::ConfigFileTokenUsed("PATINA_ADMIN_TOKEN"),
            EmptyTokenNotice::Open("PATINA_FEED_TOKEN"),
        ]
    );
    // Variables that are unset, or set to a value, produce no notice.
    let e = env(&[("PATINA_FEED_TOKEN", "x")]);
    let c = Config::load(&e, None).unwrap();
    assert!(empty_token_notices(&e, &c).is_empty());
}

// ---- horizon_days is bounded to 1..=14 ----

#[test]
fn horizon_days_outside_1_to_14_is_rejected_via_env_and_toml() {
    for bad in ["0", "15", "4000000000"] {
        let err = Config::load(&env(&[("PATINA_HORIZON_DAYS", bad)]), None).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("PATINA_HORIZON_DAYS") && msg.contains("1") && msg.contains("14"),
            "env {bad}: {msg}"
        );
        let toml = format!("horizon_days = {bad}\n");
        let err = Config::load(&HashMap::new(), Some(&toml)).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("horizon_days") && msg.contains("1") && msg.contains("14"),
            "toml {bad}: {msg}"
        );
    }
}

#[test]
fn horizon_days_1_and_14_are_accepted_via_env_and_toml() {
    for good in [1u32, 14] {
        let v = good.to_string();
        let c = Config::load(&env(&[("PATINA_HORIZON_DAYS", &v)]), None).unwrap();
        assert_eq!(c.horizon_days, good);
        let toml = format!("horizon_days = {good}\n");
        let c = Config::load(&HashMap::new(), Some(&toml)).unwrap();
        assert_eq!(c.horizon_days, good);
    }
}

#[test]
fn horizon_arithmetic_never_panics_for_any_value() {
    use chrono::{TimeZone, Utc};
    use patina::scheduler::horizon_windows;
    let now = Utc.with_ymd_and_hms(2026, 11, 1, 0, 0, 0).unwrap();
    // 14 days of 6-hour windows, plus the one containing now.
    assert_eq!(horizon_windows(now, 14).len(), 56);
    // Values the config would reject still cannot panic or run away.
    assert!(horizon_windows(now, u32::MAX).len() <= 57);
    assert!(horizon_windows(now, 4_000_000_000).len() <= 57);
    assert!(horizon_windows(chrono::DateTime::<Utc>::MAX_UTC, 7).len() <= 57);
}
