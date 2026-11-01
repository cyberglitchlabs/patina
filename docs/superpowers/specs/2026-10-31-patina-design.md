# Patina — Design

Date: 2026-10-31
Status: Draft for approval (no code written)

## 1. Intent and why

Patina is a long-running service that builds one well-groomed XMLTV TV guide
from unreliable upstream guide providers and serves it to media servers,
primarily Jellyfin Live TV.

Why it exists. The author runs Jellyfin with an HDHomeRun tuner (over-the-air,
Minneapolis, ZIP 55401, lineup `USA-OTA55401`) and currently uses
`jef/zap2xml` in a container to scrape Gracenote. Every free option reviewed
has the same failure pattern:

- `jef/zap2xml` and `halkeye/zap2xml`: unmaintained since mid-2025.
- `iptv-org/epg` (`zap2it.com` grabber): actively maintained but returned
  sparse, gappy data for this lineup (about 4 programmes per channel over 1.5
  days, a ~21 hour hole, HTTP 400 on later windows) and has no health model.
- Stream-proxy tools (Dispatcharr, Threadfin) consume guide data; they do not
  produce it reliably or judge its quality.
- All free sources depend on an unofficial Gracenote endpoint that changes or
  breaks without notice.

Nothing treats guide data as a product with a health model: per-provider
status, staleness, merge rules, and a refusal to get worse. Patina is that.

Success looks like: Jellyfin's guide is always populated, pictures show, a
provider outage degrades the guide gracefully instead of breaking it, the
author is told (metrics) when quality drops, and the service never gets the
author's IP blocked by a provider.

## 2. Goals, non-goals, anti-goals

### Goals
- Always serve the best guide available; a bad fetch must never make the
  served guide worse than what is already held.
- Support multiple guide providers behind one interface; v1 ships Gracenote
  only.
- Merge provider data per programme slot using an explainable, weighted,
  stored score; fill missing fields from lower-ranked sources.
- Be a good neighbor to providers: hard daily request budgets, backoff,
  circuit breaking, caching, no redundant fetches.
- Run unattended: automatic migrations, automatic retention/cleanup, no
  manual care.
- Observable: health, status JSON, Prometheus metrics, structured logs.
- Public, open-source, shipped as a multi-arch container image with a full
  GitHub Actions pipeline.

### Non-goals (v1)
- A second real provider, Schedules Direct, or any paid provider.
- Image archive/proxy (v1 passes provider image URLs through).
- Web UI (design leaves room for it; status is exposed as JSON).
- M3U playlist channel sources.
- Learned/ML scoring.
- Being a stream proxy, tuner emulator, or DVR. Patina only produces guides.

### Anti-goals (failure even if it "works")
- Silently publishing a thinner guide than the previous one.
- Any behavior that could become a request storm against a provider.
- Required hand-maintenance (manual migrations, manual cleanup, manual
  restarts).
- SQLite-specific assumptions that block Postgres.
- CI that hits a real provider on its own.

## 3. Constraints

- Language: Rust. The author has no Rust or SQL experience, so the code
  should be plain and conventional, favor well-trodden libraries over clever
  ones, be well commented where non-obvious, and be explained in plain
  language when reviewed.
- Public project; license `MIT OR Apache-2.0`.
- Repo: `github.com/cyberglitchlabs/patina`.
- Image: `ghcr.io/cyberglitchlabs/patina`, multi-arch (amd64, arm64), semver
  tags, pinned base images.
- Package/binary name `patina`; not published to crates.io (the name is
  taken by an unrelated UEFI crate). *(Builder's call; an earlier idea of
  `patina-epg` was superseded by the repo name.)*
- Single-instance service (no clustering) in v1.
- Gracenote's endpoint is unofficial and against its terms of service; the
  rate budget must stay conservative and the design must tolerate it
  breaking.
- CI must never contact real providers except via a manually triggered
  workflow.

## 4. Decisions made with the author

1. **Scope/audience:** new public project producing a runnable image; the
   author is the only user for now.
2. **Language:** Rust.
3. **Process shape:** long-lived service with its own config, never a cron or
   one-shot. A web UI for status/configuration is wanted later.
4. **Delivery:** serves XMLTV over HTTP (Jellyfin accepts an XMLTV URL) plus
   health/status/metrics endpoints. Keeps data on a small persistent volume
   so restarts don't lose the guide.
5. **Channel sources:** configurable list of sources; v1 supports an
   HDHomeRun `lineup.json` URL and a static channel list, each with an
   override file for ambiguous matches. One XMLTV feed per source plus a
   combined feed, with stable channel IDs and display names (for example
   `4.1 WCCO`) so Jellyfin's "Map Channels" step is easy. Jellyfin supports
   multiple tuners; docs state Schedules Direct and XMLTV cannot be used
   together; whether multiple XMLTV providers are allowed is unconfirmed.
6. **Providers:** Gracenote first, behind a provider interface; merge logic
   tested against fake providers until a real second one is proven.
7. **Master store:** a living, persisted master guide. Provider results
   (including partial ones) upsert into it keyed by channel and time slot,
   with per-field provenance. A failed or thin fetch never deletes or
   downgrades existing data. XMLTV is rendered from the master.
8. **Storage:** SQLite by default; PostgreSQL is a first-class supported
   backend. Use `sqlx` behind a Patina-owned storage trait, portable SQL,
   separate migration directories per backend, and run the same storage
   tests against both in CI.
9. **Migrations:** embedded, automatic at startup, forward-only (never edit
   an applied migration); the app refuses to start if one fails.
10. **Scoring:** transparent weighted score. Each signal is normalized to
    0..1, multiplied by a configurable weight, summed; the breakdown is
    stored per slot. Weights start hand-tuned. Hindsight accuracy feeds a
    per-provider, per-channel running average so ranking adapts. Metrics are
    kept so scoring quality can be analyzed after it has run for a while.
    Signals: freshness, completeness, timeline sanity (gaps, overlaps,
    plausible durations and boundaries), cross-provider agreement (inert with
    one provider), stability (flip-flopping), hindsight accuracy, provider
    reliability.
11. **Conflict resolution:** arbitration per programme slot; highest score
    wins; missing fields (image, description, episode, rating) are filled
    from lower-ranked providers; a slot only one provider has is kept; future
    programmes are never deleted unless a fresher source contradicts them.
12. **Fetch layer (shared by all providers):** per-provider rate limiting
    with jitter and a browser-like User-Agent; exponential backoff;
    circuit breaker with cool-down; memory of time windows that returned
    HTTP 400 so they are not retried every cycle; honor `Retry-After` and
    HTTP 429; raw responses cached compressed (default 7 days) for replay,
    debugging, and fixtures.
13. **Request budget and schedule:** a hard, configurable daily request
    budget per provider (suggested default for Gracenote: 150) that the
    scheduler never exceeds, with tiered refresh (next ~24 hours every few
    hours; days 2 to 7 about daily). Budget use is exposed in status and
    metrics. Gracenote `api/grid` returned all 89 channels in one request per
    6-hour window, so a full 7-day guide is about 28 requests.
14. **Images:** v1 emits provider image URLs in XMLTV `<icon>`; Jellyfin
    downloads and caches them. The data model includes an image record
    (source URL, content hash, fetch time, local path) so a later archive
    needs no redesign. Images are needed only for days, so any future store is
    a time-limited, hash-deduplicated cache, not a permanent archive, with a
    pluggable storage backend (local volume first). Storing Gracenote's
    images is a legal/ToS step beyond linking; the author accepted this as a
    later, deliberate choice.
15. **Janitor:** one background janitor with per-category retention, runs
    daily, logs and counts what it deletes, plus a disk-usage cap that evicts
    oldest items first. Suggested defaults: images 3 days after the last
    referencing programme airs (or immediately when unreferenced); raw
    responses 7 days; aired programmes 7 days after they end; score and
    metrics history 90 days. All configurable.
16. **Configuration:** simple settings via environment variables (listen
    address, database URL, log level, retention); structured settings via a
    single TOML file whose path is an env var (channel sources, providers and
    budgets, scoring weights, overrides). Env wins on conflict. Strict
    validation at startup with clear errors; refuse to run if invalid.
17. **Visibility:** `GET /healthz` (always open), `GET /status` (JSON: per
    provider health, budget used, circuit state, guide age, channel coverage,
    janitor results), `GET /metrics` (Prometheus), structured JSON logs via
    `tracing` with level from an env var.
18. **Access control:** open by default (in-cluster use). Optional shared
    token enabled by env var: for XMLTV feeds the token is a URL path or
    query parameter (Jellyfin takes a URL and cannot send headers); for
    `/status`, `/metrics`, and any future admin UI, a bearer token.
    `/healthz` stays open for probes.
19. **Testing:** fully offline suite. Saved real provider responses as
    fixtures (a 89-channel lineup response for ZIP 55401 with thumbnails was
    captured on 2026-10-31 as a seed); a fake provider server that simulates
    outages, 429s, slow responses, partial data and 400 windows; identical
    storage tests against SQLite and Postgres in CI; golden-file XMLTV
    output plus validation that it parses; property tests for merge/scoring
    (for example, adding data never lowers a slot's quality; a failed fetch
    never deletes anything).
20. **CI/CD (GitHub Actions):** PR checks (`cargo fmt`, `clippy` with
    warnings as errors, full tests on SQLite and a Postgres service
    container); `cargo-deny` and `cargo-audit`; automatic semver versioning
    and changelog from conventional commits (release-please style); multi-arch
    image build pushed to GHCR with provenance attestation and an SBOM;
    Renovate or Dependabot for crates, base images and actions with pinned
    versions; a manually triggered (never scheduled) live-provider smoke test.
    Workflow syntax and action versions to be checked against current docs
    when planned.

## 5. v1 scope

In: Gracenote provider behind a provider interface; HDHomeRun and static-list
channel sources with overrides; master store on SQLite or Postgres with
automatic migrations; per-slot merge with the weighted stored score; shared
fetch layer with budget, tiered refresh, and raw response cache; the janitor;
XMLTV feeds per source plus combined, with provider image URLs passed through;
`/healthz`, `/status`, `/metrics`, `tracing` logs; env plus TOML config and the
optional token; full Actions pipeline and the multi-arch GHCR image.

Later: second real provider; image archive/cache; web UI (web front end for
status and configuration); a TUI; a Helm chart; a GitHub Pages site (docs);
M3U sources; Schedules Direct; learned scoring.

## 6. Known facts about Gracenote (from experiments on 2026-10-31)

- `GET https://tvlistings.gracenote.com/api/grid` with `lineupId=USA-OTA55401`,
  `headendId=lineupId`, `country=USA`, `postalCode=55401`, `device=-`,
  `time=<unix>`, `timespan=6`, `isOverride=true`, `pref=-`, `userId=-`,
  `aid=orbebb`, `languagecode=en-us` and a browser-like User-Agent returned
  HTTP 200 with 89 channels (each with `channelNo`, `callSign`, `channelId`,
  and `events`), and every sampled event had a `thumbnail`.
- `timespan=48` on that endpoint returned HTTP 400; `timespan=6` works. The
  real maximum is untested.
- `POST /api/sslgrid` (used by iptv-org and jef) returned 403 to a bare curl
  request but works from their tools; expect header/User-Agent sensitivity.
- Gracenote sometimes returns 400 for particular time windows; this must be
  tolerated, not fatal.
- Thumbnails map to `https://zap2it.tmsimg.com/assets/<thumbnail>.jpg`.
- The tuner (HDHomeRun EXTEND, HDTC-2US) lists 107 entries including
  duplicates and an `Unknown` entry at 5000, and many low-power stations with
  generic names; some of these may have no provider data. Gracenote also has
  duplicate channel numbers (for example two `5.6`), which is why the
  override file exists.

## 7. Risks

- Gracenote's endpoint is unofficial and can break, change, or block at any
  time. Mitigations: small fetch surface, raw cache for replay, last-known
  data kept, budget and circuit breaker, a provider interface so another
  source can be added.
- With only one provider in v1, the agreement signal is inert and the score
  mostly reflects freshness, completeness, sanity and stability. This is
  expected.
- Mapping tuner channels to provider channels will be imperfect for
  low-power stations; unmatched channels simply have no guide data.

## 8. Calls made by the designer (not discussed with the author; please check)

These resolve gaps a reviewer found. Each is the designer's call, open to
change before the plan is written.

1. **Slot identity.** A "slot" is a programme airing on one canonical channel.
   Two records are the same slot when they are on the same canonical channel
   and their time ranges overlap by at least 50% of the shorter one (this
   tolerates providers shifting times by a few minutes). The master keeps one
   stable internal slot id per matched group, and stores each provider's
   start, end and title as observations against it. Stability and hindsight
   signals compare observations of the same slot over time.
2. **Channel matching and IDs.** Tuner entries match provider channels
   automatically by channel number (major.minor) first and normalized call
   sign second. Each canonical channel gets a stable XMLTV id derived from
   the normalized call sign (for example `WCCODT.us`), the same id in
   per-source and combined feeds, so Jellyfin mappings survive provider
   changes. The override file maps a tuner entry (by number or name) to a
   provider channel id, a canonical id, or `ignore` (for example the
   `Unknown` entry at 5000 and duplicate `5.6`). A "static channel list" is a
   TOML list of `{number, name, callsign?}` entries.
3. **Sources and lineups.** Provider lineups are declared in the TOML file
   (for Gracenote: ZIP plus lineup id). A channel source references one or
   more named provider lineups. Fetching is scheduled per provider lineup, and
   the daily budget is per provider, shared across its lineups.
4. **"Never worse" as a testable rule.** Absence from a response never
   deletes or shortens a future slot. A future slot is replaced or removed
   only when a later response returns the same channel and window
   successfully (HTTP 200, channel present) and explicitly contradicts it.
   Publishing is not gated on a whole-guide threshold; the quality guarantee
   is that master data only changes by upsert or explicit contradiction.
   Coverage and horizon are reported as metrics and in `/status`, and
   alertable thresholds are configurable, but they never replace the master.
5. **Hindsight accuracy with one provider.** Ground truth is the same
   provider's later fetch of the same slot closer to air time. It measures
   forecast drift, not truth, so in v1 it is computed and stored but carries
   a low default weight. The cross-provider agreement signal stays inert
   until a second provider exists.
6. **Channels with no data.** Matched channels always appear in feeds, even
   with no programmes, so Jellyfin can map them. Unmatched tuner entries and
   entries marked `ignore` are omitted by default (configurable with an
   `include_unmatched` option).
7. **Horizon and budget behavior.** Default guide horizon is 7 days,
   configurable. Exhausting the daily budget stops fetching only; serving the
   master is never blocked, so the guide ages gracefully instead of failing.
   The real maximum `timespan` is discovered at build time with conservative
   probing, and the default stays at the proven 6 hours.
8. **Tokens.** One optional shared token applies to all XMLTV feeds
   (per-source and combined). A separate optional bearer token covers
   `/status`, `/metrics`, and future admin endpoints.

## 9. Left to the builder

- Crate selection and project layout (suggested: `tokio`, `axum`, `reqwest`,
  `sqlx`, `tracing`, `serde`, `toml`, a Prometheus client crate).
- Database schema, indexes, and migration contents.
- Exact signal formulas, normalization, and starting weights.
- Exact default intervals and budgets (within the constraints above).
- XMLTV writer details, channel ID format, and timezone handling (suggested:
  UTC internally, correct XMLTV offsets on output).
- Shape of the `/status` JSON and metric names.
- How feeds are cached/rendered (on request vs on a timer).
- Dockerfile details (suggested: small static or distroless-style base,
  non-root user, read-only root filesystem friendly).
- How the Kubernetes deployment in the author's cluster consumes the image
  (a separate follow-up in the author's own cluster repository, not part of this project).
