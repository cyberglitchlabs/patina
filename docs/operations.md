# Operating Patina

A running guide for the person who keeps Patina going. The [README](../README.md)
covers what Patina is and how to start it; this page covers what to do once it
is running: where the settings are, what the numbers mean, what to alert on and
what to do when something breaks.

Everything here was checked against the code at the time of writing. Where a
statement could not be verified it says so.

Contents

- [Overview of the moving parts](#overview-of-the-moving-parts)
- [Configuration](#configuration)
- [Tokens](#tokens)
- [Provider budgets, and why they exist](#provider-budgets-and-why-they-exist)
- [Channel matching and the overrides file](#channel-matching-and-the-overrides-file)
- [Jellyfin setup](#jellyfin-setup)
- [Retention and the janitor](#retention-and-the-janitor)
- [Backups and restore](#backups-and-restore)
- [Migrations](#migrations)
- [Upgrading the image](#upgrading-the-image)
- [Reading /status](#reading-status)
- [Prometheus](#prometheus)
- [Troubleshooting](#troubleshooting)
- [Security notes](#security-notes)
- [Known limitations](#known-limitations)

## Overview of the moving parts

One process, one container, one database. Inside the process:

| Part | What it does | How often |
| --- | --- | --- |
| **Scheduler** | Decides which 6-hour guide windows are due, asks the provider for them (through the fetch layer), parses, scores and merges the answer into the master guide | A pass every 60 s; most passes send nothing |
| **Fetch layer** | The only code that talks to a provider. Enforces the daily budget, a minimum gap between requests, backoff and the circuit breaker. State is stored in the database, so a restart does not reset it | On demand |
| **Master guide** | The database's merged view of the guide: one slot per programme, with the observations (what each provider said, and when) that explain it | n/a |
| **Channel refresher** | Reloads each source's tuner channel list (your HDHomeRun's `lineup.json`) | At start, then every 10 minutes plus up to 30 s of random delay |
| **Feeds** | Renders XMLTV on request from the master guide and the tuner list, using the overrides file to match channels. A rendered feed is cached for 30 s and re-rendered at once when a merge changes anything | Per request |
| **Janitor** | Deletes old data by retention rules and enforces the optional disk cap | First run 60 s after start, then about every 24 h |
| **Admin endpoints** | `/status` (JSON) and `/metrics` (Prometheus) | Per request |

The guide is a **pull and keep** design: a bad or failed fetch never removes
good data. If the provider disappears, Patina keeps serving the last guide it
has until it runs out of future programmes.

There is exactly one writer. Do not run two Patina instances against the same
database or the same provider (see [Known limitations](#known-limitations)).

## Configuration

Two inputs: environment variables, and an optional TOML file named by
`PATINA_CONFIG`. **Environment variables win over the file.** Everything is
validated at startup and a bad value stops the process with a message naming
the setting (exit status 1).

- The annotated, complete file is [`config.example.toml`](../config.example.toml).
- The README has the [settings table](../README.md#configuration) and the list
  of endpoints.
- A Kubernetes example is in [`docs/examples/kubernetes.yaml`](examples/kubernetes.yaml).

Environment variables, with defaults and what an **empty** value does:

| Variable | Default | If set to an empty string |
| --- | --- | --- |
| `PATINA_CONFIG` | none (no file) | Treated as unset: no file is read |
| `PATINA_LISTEN` | `0.0.0.0:8080` | Treated as unset: the TOML value, else the default |
| `PATINA_LOG` | `info` | Treated as unset |
| `PATINA_DATABASE_URL` | `sqlite://patina.db?mode=rwc` (the image sets `sqlite:///data/patina.db?mode=rwc`) | Treated as unset: the TOML value, else the built-in default. In the image the working directory is `/data`, so emptying the variable still lands on `/data/patina.db`; outside the image it means a `patina.db` in the current directory |
| `PATINA_TIMEZONE` | `America/Chicago` | Treated as unset |
| `PATINA_FEED_TOKEN` | none (feeds open) | Treated as unset, so **a `feed_token` in the TOML file still applies, and with none the feeds are open**. Startup says the variable is set but empty: at info level when the TOML token applies, as a warning (endpoints OPEN) when there is none |
| `PATINA_ADMIN_TOKEN` | none (`/status` and `/metrics` open) | Same as the feed token |
| `PATINA_HORIZON_DAYS` | `7` (1 to 14) | **Startup fails** (an empty string is not a number); a value outside 1 to 14 also fails startup |
| `PATINA_DISK_CAP_BYTES` | none | **Startup fails** |
| `PATINA_RETENTION_IMAGES_DAYS` | `3` | **Startup fails** |
| `PATINA_RETENTION_RAW_DAYS` | `7` | **Startup fails** |
| `PATINA_RETENTION_PROGRAMMES_DAYS` | `7` | **Startup fails** |
| `PATINA_RETENTION_HISTORY_DAYS` | `90` | **Startup fails** |

So: an empty string is a quiet way to say "unset" for text settings and a
mistake for numbers. A templating system that renders a missing value as `""`
therefore fails loudly for numbers but silently opens the endpoints for tokens.
If you template your environment, watch the startup log line
`endpoint authentication` (it says `enabled` or `disabled` for each token;
values are never logged).

Providers, lineups, sources and scoring weights exist only in the TOML file.
Provider `daily_budget` defaults to 150. A source is named by its kind plus its
position among sources of that kind: `hdhomerun-1`, `hdhomerun-2`, `static-1`.

## Tokens

Patina has two independent tokens. Set both unless the service is reachable
only from a network you fully trust.

| Token | Protects | How it is presented |
| --- | --- | --- |
| **Feed token** (`PATINA_FEED_TOKEN`) | `/xmltv/...` and `/t/...` | In the URL: `?token=<token>` or `/t/<token>/xmltv/...` |
| **Admin token** (`PATINA_ADMIN_TOKEN`) | `/status` and `/metrics` | Header only: `Authorization: Bearer <token>` |

`/healthz` never needs a token (probes carry no credentials).

Why two? The feed URL has to be pasted into Jellyfin, so it travels in a URL
and ends up in places URLs end up (Jellyfin's database, reverse-proxy logs).
That token is therefore low value by nature and only keeps casual visitors out.
The admin token never appears in a URL: it is only read from the
`Authorization` header, so it does not leak through access logs. A leaked feed
token exposes the guide; it does not expose `/status`.

Where to put them: in environment variables, from a Secret or an env file
(`docker run --env-file`), **not** in a TOML file that is in version control.
Generate long random values, for example `openssl rand -hex 24`.

Giving Jellyfin the URL:

```text
http://patina:8080/xmltv/hdhomerun-1.xml?token=<feed token>
http://patina:8080/t/<feed token>/xmltv/hdhomerun-1.xml
```

Both forms return the same document. Use the `/t/<token>/` form if Jellyfin (or
a proxy in between) mangles query strings. Rules, as implemented:

- The token is compared in constant time.
- A request with no token, or a wrong one, gets a bare `401` with an empty body,
  and the same `401` for paths that do not exist, so the response does not reveal
  which feeds exist.
- One token per request. A request that carries more than one (two `?token=`
  values, or a `/t/<token>/` path plus a `?token=`) gets the same bare `401`,
  even if one of them is right, so a single request can never try several
  guesses.
- When a feed token is configured, feed responses carry
  `Cache-Control: private, max-age=60`, so shared caches (proxies) do not keep
  a copy; without a token it is `max-age=60`.
- If no feed token is configured, any `?token=` is simply ignored.
- Percent-encoding in `?token=` is decoded (a token with `&` or a space works if
  you encode it), but plain hex tokens avoid the question.

## Provider budgets, and why they exist

The Gracenote provider reads an **unofficial, public** web endpoint. Nothing
guarantees it will keep answering, and nobody agreed to be a data feed for your
house. "Good neighbor" here means: ask for as little as possible, as slowly as
possible, and go quiet at the first sign of trouble. A blocked IP is the likely
cost of being greedy, and it would affect everyone who uses that address.

The rules, all enforced in one place (the fetch layer) so nothing can bypass
them. Defaults:

| Rule | Default |
| --- | --- |
| Daily request budget (per provider, per UTC day) | **150** (`daily_budget`) |
| Minimum gap between two requests to one provider | 2 s, plus or minus up to 50 % random jitter |
| One request in flight per provider | always |
| Backoff after a failure | 30 s, doubling per consecutive failure, at most 1 h |
| Circuit breaker opens | after 5 failures in a row, **at once** for a "fatal" answer such as HTTP 403 |
| Circuit stays open | at least 15 min, then one probe request; success closes it, failure re-opens it |
| `429` / `Retry-After` | honoured, but never shorter than our own backoff, never longer than a day. A 429 counts as one failure for the backoff and circuit breaker (like a 5xx) and once against the provider's `success_rate`; the windows it then holds back count nothing |
| A window the provider rejected with HTTP 400 | left alone for 6 h; three 400s in a row put the whole lineup on hold for 6 h (no requests for any of its windows; shown in `/status` as `providers[].lineup_holds`; the hold is in memory, so a restart may cost three more requests to set it again). A 400 never counts against the provider's `success_rate` |
| The database fails while a request is being checked | the request is not sent ("fail closed"); this is not held against the provider (no `success_rate` change, no backoff) and the window is simply due again next pass |
| Repeat requests for a window fetched in the last hour | served from the stored raw response, no request |
| Request timeout | 60 s |

How much a normal day costs: the guide is fetched in 6-hour windows. For a
7-day horizon that is 29 windows per lineup for the first fill. After that the
nearest 24 hours (5 windows) are refreshed every 3 hours and the rest once a
day, which comes to roughly 65 to 70 requests per lineup per day (our
arithmetic from those refresh rules, not a measurement; `/status` shows the
real number). The default budget of 150 therefore covers two lineups with
headroom. Do not raise it casually. `horizon_days` is limited to 14: each
extra day adds 4 windows per lineup to the daily sweep, and a long horizon
would spend the budget on far-away days at the cost of near-term refreshes.

The budget counts requests **before** they are sent and survives restarts: it is
kept in the database, so restarting Patina repeatedly does not reset it. The
day rolls over at 00:00 UTC.

What happens when things run out:

- **Budget exhausted:** no further requests that UTC day. The scheduler keeps
  running and the master guide keeps being served; windows that did not get
  fetched wait for the next day. Nearest windows are fetched first, so the part
  of the guide people look at is the part that gets the budget.
- **Circuit open:** no requests at all until the cooldown ends, then one probe.
  The guide keeps being served from the database.

How to see it:

- `GET /status`: `providers[].budget.used` / `limit`, `providers[].circuit`
  (`closed`, `open`, `half_open`), `consecutive_failures`, `last_error`,
  `lineup_holds`, and `scheduler.last_tick` (`skipped_budget`,
  `skipped_circuit`, `skipped_backoff`, `halted`, `skipped_store`).
- `/metrics`: `patina_provider_budget_used`, `patina_provider_budget_limit`,
  `patina_circuit_open`, `patina_provider_requests_total{outcome=...}` (the
  `skipped_*` outcomes are requests that were **not** sent).

## Channel matching and the overrides file

Patina asks your tuner (HDHomeRun) which channels you have, then matches each
tuner channel to a provider channel: first by channel **number**, then by
**call sign** (letters and digits of the name, upper-cased; "WCCO-DT",
"WCCODT" and "wcco-dt" are the same). Matched channels appear in the feed under
the provider's canonical id (for example `WCCODT.us`) with the display name
`<number> <tuner name>` (for example `4.1 WCCO-DT`).

Tuner channels that match nothing are left out of the feed unless the source
sets `include_unmatched = true` (then they appear as empty channels with the id
`UNMATCHED-<number>.us`). A tuner entry that is ambiguous (two provider
channels with the same number and nothing to tell them apart) counts as
unmatched until you resolve it.

### The overrides file

Set `overrides_path` on the source to a TOML file of `[[override]]` tables. Each
table needs exactly one **selector** and exactly one **action**:

| Selector | Meaning |
| --- | --- |
| `number = "9.1"` | The tuner channel with this guide number |
| `name = "FOX 9"` | Every tuner channel with this name. Exact match, but **ignoring case and surrounding spaces**: `FOX 9` matches `Fox 9` and `  fox 9 `, not `FOX 9+` or `FOX9`. Several tuner channels may share a name, and the override applies to all of them |

| Action | Meaning |
| --- | --- |
| `provider_channel_id = "..."` | Match to this specific provider channel |
| `canonical = "MYCHAN.us"` | Create a channel with this XMLTV id (the provider does not know it) |
| `ignore = true` | Leave this tuner channel out entirely |

If a `name` entry and a `number` entry both match one tuner channel, the
**earlier entry in the file wins**. The file is validated at startup: a typo
(two selectors, an empty name, an unknown key) stops Patina and names the entry.

A worked example, from the Minneapolis test lineup. The tuner lists two
entries both named "Fox 9" (`9.1` "FOX 9" and `9.9` "Fox 9"). Gracenote numbers
the stations the other way round: its 9.1 is WFTC-DT2 and its 9.9 is KMSP-DT.
Matching by number therefore pairs tuner 9.1 with the wrong station, and a
`name = "FOX 9"` override would hit **both** entries (names ignore case), so
number selectors are the right tool here:

```toml
# overrides.toml

# Tuner 9.1 ("FOX 9") is really KMSP-DT, which Gracenote numbers 9.9.
[[override]]
number = "9.1"
provider_channel_id = "123456"          # the provider id for call sign KMSPDT

# Tuner 9.9 is the station Gracenote numbers 9.1 (WFTC-DT2). Move it too, or
# two tuner channels would claim the same provider channel.
[[override]]
number = "9.9"
provider_channel_id = "123457"          # the provider id for call sign WFTCDT2

# Hide the junk "5000" entry.
[[override]]
number = "5000"
ignore = true
```

(The ids above are placeholders; your lineup's ids are different. The real
pairing for this lineup is exercised by `pinned_counts_with_realistic_overrides`
in `tests/channels.rs`: with five such overrides, 83 of the 107 tuner entries
match.)

### Finding provider channel ids

`/status` does not list provider channels. They are in the database, in the
table `provider_channels`, filled after the first successful fetch.

```sh
# SQLite (reading while Patina runs is fine)
sqlite3 /data/patina.db \
  "SELECT provider_channel_id, number, call_sign
     FROM provider_channels WHERE lineup = 'home' ORDER BY number;"

# PostgreSQL
psql "$PATINA_DATABASE_URL" -c \
  "SELECT provider_channel_id, number, call_sign
     FROM provider_channels WHERE lineup = 'home' ORDER BY number;"
```

`lineup` is the lineup `name` from your config. The image has no shell and no
`sqlite3`; run the query against a copy of the database file, or from the
host if you bind-mounted `/data`, or use the backup recipe below to get a
copy out of the volume.

### Testing a change

1. Edit the overrides file (mounted read-only into the container).
2. Restart Patina: overrides are read at startup only. A mistake stops the
   start and the log names the entry.
3. Check the match counts: `GET /status` and look at
   `sources[].channels_total` and `channels_matched` (and the metric
   `patina_source_channels{state="matched"|"unmatched"}`).
4. Fetch the feed and look for the channel you fixed:
   `curl -s 'http://patina:8080/xmltv/hdhomerun-1.xml?token=...' | grep -c '<programme '`
   and `grep 'display-name' ... | grep 'WCCO'`.
5. In Jellyfin, refresh the guide.

## Jellyfin setup

1. **Dashboard > Live TV > TV Guide Data Providers > Add > XMLTV**.
2. Under "File or URL" give the feed URL (see [Tokens](#tokens)):
   `http://<patina>:8080/xmltv/hdhomerun-1.xml?token=<feed token>`. Use
   `all.xml` to get every source combined (a station carried by two sources
   appears once).
3. Save, then **Dashboard > Scheduled Tasks > Refresh Guide** (or wait for its
   schedule).
4. Under the Live TV tuner's channel list, use **Map Channels** if Jellyfin does
   not map them by itself. The XMLTV ids are canonical ids like `WCCODT.us`, and
   the display names start with the tuner number, which is what Jellyfin uses
   to line channels up.

Behaviour you can rely on:

- The feed starts three hours before "now" and runs to the end of the horizon,
  so programmes on air are present.
- Until the first fetch has stored the provider's channels and the tuner list
  has loaded, a feed request returns **503** with a `Retry-After: 60` header and
  a one-line reason (an empty guide would make Jellyfin remember "no channels").
- Jellyfin pulls the file when it refreshes. Patina does not push anything.
  After Patina merges new data, a feed request sees it within seconds.
- Times carry the UTC offset of the configured `timezone` and change offset
  across daylight-saving changes.

Honest caveats about Jellyfin itself, which Patina cannot change and which we
have **not verified for every Jellyfin version**:

- Jellyfin's Live TV settings offer Schedules Direct and XMLTV as guide
  provider types. We believe you cannot usefully run Schedules Direct and XMLTV
  at the same time for the same tuner; pick one.
- Whether several XMLTV providers can be active together, and how Jellyfin
  resolves channels listed by two of them, is **unconfirmed**. The safe setup is
  one XMLTV provider per tuner. If you have several sources, prefer the
  combined `all.xml` as the only provider, or one feed per tuner, not both.
- If pictures do not show, see [Troubleshooting](#troubleshooting). Whether
  Jellyfin displays `<icon>` images is Jellyfin's decision; Patina only
  provides the URL.

## Retention and the janitor

The janitor trims old data so the database does not grow forever. Defaults
(each is a number of days, at least 1):

| Setting | Default | What goes |
| --- | --- | --- |
| `retention.raw_days` | 7 | Raw provider responses |
| `retention.programmes_days` | 7 | Programmes that **ended** more than this long ago (with their observations) |
| `retention.images_days` | 3 | Image links of programmes that ended more than this long ago, then image rows no programme uses |
| `retention.history_days` | 90 | Score and metrics history |

It also deletes bookkeeping that has outlived its use: window state older than a
day, provider channels not seen for 30 days, expired bad-window markers, budget
rows older than three days. It never prunes the learned provider accuracy and
reliability numbers, the tuner channel lists or the fetch layer's circuit state.

**It never deletes future slots.** Every delete of programmes is clamped to
"now" and compares strictly, so a programme that is on air or has not aired yet
cannot be deleted whatever the retention or the disk cap say.

Schedule: the first run 60 s after start, then about every 24 h (plus or minus
10 %). Deletes are done in small chunks (500 rows) so merges and feeds stay
responsive. Its last report is the `janitor` object in `/status` (`null` until
the first run finishes) and `patina_janitor_deleted_total{category=...}`
counts what it removed.

### The disk cap

`disk_cap_bytes` (optional) is a **soft** limit. If the database is still above
it after the normal deletes, the janitor evicts, oldest first: raw responses,
then history, then programmes that ended more than 3 hours ago (the feeds still
show the last 3 hours, so those are kept). Future programmes are never
evicted, so if the cap is smaller than what the live guide needs, the
janitor stops (it gives up after a bounded number of passes and a few passes
that free nothing) and leaves the database over the cap.

What "size" means differs by backend:

- **SQLite:** the number of pages in use times the page size. Pages freed by
  deletes count as free **even though the file does not shrink**, so the file on
  disk can be larger than the cap while Patina considers itself under it. Use
  `sqlite3 patina.db 'VACUUM'` (with Patina stopped) if you need the file itself
  smaller.
- **PostgreSQL:** `pg_database_size`, which includes dead space and only falls
  when the server gives space back to the OS. The cap is therefore
  **advisory** on Postgres: eviction may delete data without the measured size
  falling, and the janitor stops after a few passes that make no difference.

## Backups and restore

Patina's data is **rebuildable**: everything in it comes from the provider and
your config, and a fresh database fills in about 29 requests (within a day,
inside the budget) and a few minutes. What a backup saves you is that refill, the
learned provider accuracy numbers, and the history. Backups are therefore
optional; take them if the refill matters to you.

### SQLite

The database is in WAL mode, so copying only `patina.db` while Patina runs can
miss recent changes held in `patina.db-wal`. Do **not** `cp` the file of a
running instance. Use SQLite's own online backup, which is safe while Patina
runs:

```sh
sqlite3 /data/patina.db ".backup '/backups/patina-$(date +%F).db'"
# or
sqlite3 /data/patina.db "VACUUM INTO '/backups/patina-$(date +%F).db'"
```

`VACUUM INTO` also writes a compact copy. The image has no `sqlite3` and no
shell, so run these from a sidecar or from the host against the same volume, or
stop Patina and copy the three files (`patina.db`, `patina.db-wal`,
`patina.db-shm`) together. Where the volume is a Kubernetes PVC, a volume
snapshot taken while Patina is stopped (or briefly scaled to zero) is also
consistent.

Restore: stop Patina, put the backup file in place as `patina.db` (and delete any
leftover `patina.db-wal` and `patina.db-shm`), start Patina.

### PostgreSQL

```sh
pg_dump --format=custom --file patina-$(date +%F).dump "$PATINA_DATABASE_URL"
pg_restore --clean --if-exists --dbname "$PATINA_DATABASE_URL" patina-2026-11-01.dump
```

Stop Patina before a restore. Patina creates and migrates its own tables in the
database it is pointed at, so an empty database also works as a restore target
for "start over".

## Migrations

The database schema is created and upgraded **automatically at startup**, before
the server listens. Migrations are:

- **Forward-only.** There are no down migrations.
- **Required.** If a migration fails, Patina logs the error and exits with
  status 1; it never serves with a half-migrated schema.
- **Embedded** in the binary, one set per backend.

**Downgrading the image is not supported.** An older Patina does not know the
newer schema. We checked the failure mode on SQLite: a database that records a
migration the binary does not have makes startup fail with `cannot open
database` and exit status 1, and nothing is modified. If an upgrade goes wrong,
restore from a backup taken before it, or delete the database and let it
refill.

## Upgrading the image

1. Read the release notes (`CHANGELOG.md`) for the version you are moving to.
2. Optional but cheap: take a SQLite backup (see above).
3. Pin the tag you want (`ghcr.io/cyberglitchlabs/patina:X.Y.Z`; `X.Y`, `X` and
   `latest` also exist) and restart the container. Every push to `main` is also
   published (`linux/amd64` only) as `edge` and as `sha-<short>`. In production
   pin by digest (`patina@sha256:...`) or a `sha-<short>` tag, not `edge`: `edge`
   moves on every push, so a restart could silently change the version, and
   downgrading is not supported.
4. Watch the log for `database ready` and `patina listening`, then
   `GET /status` (`store.ok` true, `version` as expected).

Stopping the old instance gives it time to finish: `SIGTERM` makes Patina finish
the guide window it is merging and exit with status 0. Allow up to 90 s
(`terminationGracePeriodSeconds: 90` in the Kubernetes example). Because the
SQLite volume is single-writer, use `Recreate`, not a rolling update: old pod
down, then new pod up. A restart costs no extra provider requests: window state
and the budget are in the database.

## Reading /status

`GET /status` (bearer token if `admin_token` is set) returns JSON. Times are
UTC RFC 3339 strings.

| Field | Meaning |
| --- | --- |
| `version`, `uptime_seconds` | Build version; seconds since start |
| `store.backend` | `sqlite` or `postgres` |
| `store.ok`, `store.error` | Whether the database answered. The error text is generic on purpose (the real error is in the log) |
| `providers[].name` | Provider name from the config |
| `providers[].circuit` | `closed` (normal), `open` (no requests until the cooldown ends), `half_open` (next request is a probe), `unknown` (state unreadable) |
| `providers[].budget` | `used` and `limit` for the UTC `day` |
| `providers[].last_success` | Last good response, or `null` |
| `providers[].last_error` | Text of the last failure (kept until a success) |
| `providers[].consecutive_failures` | Failures in a row; 0 after a success |
| `providers[].success_rate` | Share of recent counted fetches that were good, 0 to 1; `null` until one is counted. Counted: each fresh response (good, or bad if more than 20 % of its timeline is gaps or more than 10 % overlaps), each failed request (5xx, 403, timeout, unparseable body) and each HTTP 429. Not counted: answers from the raw cache, HTTP 400 for one window, and anything where no request was sent (budget, circuit, backoff, a database error) |
| `providers[].response_quality` | The latest response's `gap_fraction`, `overlap_fraction`, `observed_minutes` and time; `null` before the first response |
| `providers[].lineup_holds` | Lineups on a bad-window hold after three HTTP 400s in a row: `[{ "lineup": "ota", "until": "..." }]`. None of that lineup's windows is asked for before `until`. Usually `[]`; in memory only, so a restart clears it |
| `providers[].error` | Set when this provider's state could not be read |
| `sources[]` | Per source: `name`, `kind`, `channels_total` (tuner entries), `channels_matched`, `stale` (the last reload failed, or the tuner answered with an empty list as it does during a channel scan, and the previous list is being used), `loaded_at`, `error` |
| `guide.horizon_hours` | Hours from now to the end of the newest programme; 0 if none |
| `guide.newest_slot_end` | That end time |
| `guide.channels_with_data` | Channels with at least one programme that has not ended |
| `guide.coverage_pct` | Share of matched channels (all sources, de-duplicated) that have such a programme |
| `guide.slots_total` | Programmes in the master guide |
| `scheduler.last_tick_at` | When the last scheduler pass ran |
| `scheduler.last_tick` | That pass in numbers: `windows_due`, `fetched`, `from_cache`, `failed`, `parse_failed`, `skipped_budget`, `skipped_circuit`, `skipped_backoff` (our own backoff, or a 429 just now), `skipped_bad_window`, `bad_window`, `halted` (lineup on hold), `skipped_store` (not sent: database error), `slots_inserted`, `slots_updated`, `slots_removed`, `channels_seen`, `merge_errors`, `cancelled` |
| `janitor` | `null` until the first run finishes, then `last_run`, `deleted` per category (`raw`, `programmes`, `images`, `history`, `state`, `disk`), `size_before`, `size_after`, `errors` |

`guide.*` is zero or null when the database cannot be read (and `store.ok` says
so). `merge_errors > 0` means a window was fetched but storing it failed (the
log has the reason; the window is retried after at least 30 minutes).

## Prometheus

`/metrics` uses the Prometheus text format and the same bearer token as
`/status`. Scrape config:

```yaml
scrape_configs:
  - job_name: patina
    scrape_interval: 1m
    metrics_path: /metrics
    authorization:
      type: Bearer
      credentials_file: /etc/prometheus/secrets/patina-admin-token
    static_configs:
      - targets: ["patina:8080"]
```

(Use `credentials:` for an inline token. Keep it out of version control.)

Metrics (all labels are your configured provider and source names or fixed
words):

| Series | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `patina_provider_requests_total` | counter | `provider`, `outcome` | Fetch attempts by outcome: `ok`, `cache`, `bad_window`, `rate_limited`, `transient`, `fatal`, `parse_failed`, and `skipped_budget`, `skipped_circuit`, `skipped_bad_window`, `skipped_backoff` (no request was sent) |
| `patina_provider_budget_used` | gauge | `provider` | Requests made today (UTC) |
| `patina_provider_budget_limit` | gauge | `provider` | Daily budget |
| `patina_circuit_open` | gauge | `provider` | 1 when open or half-open |
| `patina_guide_horizon_seconds` | gauge | none | Seconds until the newest programme ends |
| `patina_guide_coverage_ratio` | gauge | none | Share of matched channels with a current or future programme, 0 to 1 |
| `patina_slots_total` | gauge | none | Programmes in the master guide |
| `patina_janitor_deleted_total` | counter | `category` | Rows or files the janitor deleted |
| `patina_merge_dropped_total` | counter | `reason` | Provider events the parser discarded (`invalid_time`, `missing_title`, `duplicate`) |
| `patina_source_channels` | gauge | `source`, `state` | Tuner channels per source, `matched` or `unmatched` |
| `patina_build_info` | gauge | version label | Always 1 |

The gauges that mirror state are refreshed from the database on each scrape (the
guide figures are reused for up to 15 s). Counters start at zero when the process
starts, so use `increase()`/`rate()`, which handle resets.

### Suggested alert rules

Ready to paste. The expressions use only series that exist today. Four of the
alerts you might want (store not ok, scheduler tick stale, janitor errors, and a
direct success-rate gauge) have **no metric yet**; each is marked below with
what to do meanwhile. Tune the thresholds to taste.

```yaml
groups:
  - name: patina
    rules:
      - alert: PatinaGuideHorizonShort
        # Less than a day of guide left, for 30 minutes. Normally the horizon is
        # close to horizon_days (7 by default).
        expr: patina_guide_horizon_seconds < 86400
        for: 30m
        labels: {severity: warning}
        annotations:
          summary: "Patina has under 24 h of guide left"

      - alert: PatinaProviderCircuitOpen
        expr: patina_circuit_open == 1
        for: 1h
        labels: {severity: warning}
        annotations:
          summary: "Provider {{ $labels.provider }} circuit open for over an hour"

      - alert: PatinaBudgetNearlyUsed
        expr: patina_provider_budget_used / patina_provider_budget_limit > 0.9
        for: 5m
        labels: {severity: warning}
        annotations:
          summary: "Provider {{ $labels.provider }} has used over 90% of today's request budget"

      - alert: PatinaNoSuccessfulFetch
        # Counter-based stand-in for "last success timestamp", which does not
        # exist as a metric. Cache hits do not count: only real good responses.
        # Needs 12 h of history, so it stays quiet for the first 12 h after
        # the series first appears.
        expr: increase(patina_provider_requests_total{outcome="ok"}[12h]) == 0
        for: 5m
        labels: {severity: warning}
        annotations:
          summary: "No successful fetch from {{ $labels.provider }} in 12 hours"

      - alert: PatinaProviderSuccessRateLow
        # Derived from the counters: good responses over all requests that were
        # actually sent and got an answer or a failure. (/status shows the
        # provider's own success_rate, which is not exported as a gauge.)
        expr: |
          sum by (provider) (increase(patina_provider_requests_total{outcome="ok"}[3h]))
            /
          sum by (provider) (increase(patina_provider_requests_total{outcome=~"ok|transient|fatal|rate_limited|parse_failed"}[3h]))
            < 0.5
        for: 15m
        labels: {severity: warning}
        annotations:
          summary: "Under half of requests to {{ $labels.provider }} succeeded in 3 hours"

      - alert: PatinaCoverageLow
        # Not in the original list but cheap: matched channels with no current
        # or future programme.
        expr: patina_guide_coverage_ratio < 0.8
        for: 1h
        labels: {severity: warning}
        annotations:
          summary: "Under 80% of matched channels have guide data"

      # --- Wanted, but not possible yet --------------------------------------
      # store not ok:        requires a future metric. /status has store.ok;
      #                      meanwhile probe /status with the blackbox or json
      #                      exporter, or alert on the container's HEALTHCHECK.
      # scheduler tick stale: requires a future metric (/status has
      #                      scheduler.last_tick_at). PatinaGuideHorizonShort
      #                      catches the consequence.
      # janitor errors > 0:  requires a future metric (/status has
      #                      janitor.errors).
```

Notes: `PatinaNoSuccessfulFetch` and `PatinaProviderSuccessRateLow` use the
counters, so an idle but healthy instance (nothing due for 12 h) can in theory
trigger the first. With the default refresh rules (nearest windows every 3 h)
that does not happen in practice; if it does, your budget or circuit is the
reason and the other alerts will say which.

## Troubleshooting

**The feed returns 503.** Patina is up but has nothing to serve yet. The body
says why: the tuner channel list has not loaded (check `sources[].error` and
`loaded_at` in `/status`, and that the tuner URL is reachable from the
container), or the provider's channels are not stored yet (no successful fetch
yet: look at `providers[]` and the log). A freshly started instance serves 503
until its first successful fetch, usually within a minute.

**The guide is empty, or channels have no programmes.** Check `/status`:
`guide.slots_total` and `coverage_pct`. If zero, look at
`providers[].last_error`, the circuit, and the budget. If programmes exist but
a channel has none, it did not match: see the next item.

**401.** The token is missing or wrong. A feed 401 has an empty body by design;
an admin 401 sends `WWW-Authenticate: Bearer`. Check which token the URL or
header carries and that the environment variable was not set to an empty
string (which means "no token", not "empty token").

**Channels not matched.** `channels_matched` is lower than `channels_total` in
`/status`. Look up the tuner channel's number and call sign, and the provider's
(see [Finding provider channel ids](#finding-provider-channel-ids)), then add an
override. Common causes: the provider uses a different subchannel number, a
call sign with a suffix, or two provider channels share a number.

**Pictures are missing.** Patina passes the provider's picture URL through; it
does not download or host pictures. Confirm they are in the feed:

```sh
curl -s 'http://patina:8080/xmltv/hdhomerun-1.xml?token=...' | grep -m3 '<icon'
```

If `<icon src="https://...">` is there, Patina has done its part. Jellyfin needs
to reach the picture host itself, and may only show images for some views. If
no `<icon>` is present for a channel's programmes, the provider sent none for
them.

**Clock or timezone looks wrong.** XMLTV times carry the offset of
`timezone` (default `America/Chicago`). Set `PATINA_TIMEZONE` to your own IANA
zone (`Europe/Berlin`, not `CET`). The budget day and the refresh logic use UTC
and the system clock; a container with a badly wrong clock will misjudge which
windows are due. Check `date -u` on the host.

**`last_error` says Gracenote returned 403.** 403 is treated as "we may be
blocked": the circuit opens at once and stays open for at least 15 minutes, then
probes with one request. Do not raise the budget or restart in a loop. Wait;
check whether your IP is blocked by opening the listings site in a browser. If
it keeps coming back, lower your request volume (fewer lineups, a shorter
`horizon_days`).

**`last_error` says 400 / "provider rejected window".** The provider refused a
particular time window (often a lineup id or postal code it does not know, or a
window outside its range). That window is left alone for 6 hours; three
rejections in a row put the whole lineup on hold for 6 hours (no requests for
any of its windows; `/status` lists it under `providers[].lineup_holds` with
the time the hold ends).
Check `lineup_id`, `postal_code` and `country` in the config (the over-the-air
lineup in the test data is `USA-OTA55401`; use the id Gracenote's listings site
shows for your lineup). 400s do not count as provider failures and do not open the circuit.

**The circuit is open.** See [Provider budgets](#provider-budgets-and-why-they-exist).
It closes itself after the cooldown if the probe succeeds. `last_error` has the
reason. The guide continues to be served meanwhile.

**Memory is high.** Rendering a feed holds the whole document in memory (the
83-channel, 7-day feed in the end-to-end test is about 7 MB, and each feed name
is cached separately for 30 s), and a refresh parses a provider response of a
few hundred kilobytes. If memory keeps growing, check the log for repeating
errors, shorten `horizon_days`, use fewer feed names, and report it. We have not
measured a production memory footprint, so we give no number for a Kubernetes
limit.

## Security notes

- Set both tokens unless the service is only on a network you trust. With no
  tokens, feeds, `/status` and `/metrics` are open. The startup log says whether
  each is enabled, and warns when a token variable is set but empty and no
  token from the config file applies either (the endpoints are then open).
- The feed token is part of the URL and will be stored by Jellyfin and may be
  logged by any reverse proxy in front of Patina. Treat it as low-value and
  rotatable: change the variable, restart, and update Jellyfin. Patina itself
  never logs URLs, paths, queries or headers.
- Put Patina behind HTTPS if traffic crosses a network you do not control.
  Patina speaks plain HTTP only.
- `/status` and `/metrics` expose operational detail, not secrets: no tokens, no
  database URL, no passwords. The database URL in the startup log has its
  password masked.
- `/healthz` is always open.
- The image runs as the non-root user 65532, with no shell, and works with a
  read-only root filesystem; only `/data` is writable.
- Patina makes outbound requests to the provider's host and to your tuner's
  address (from your config). It does not follow redirects from the provider.

## Known limitations

- **Unofficial provider endpoint.** The Gracenote provider uses a public web
  endpoint that is neither documented nor licensed for this use. It can change,
  block you, or vanish, and its terms may not allow what you are doing. See the
  README's warning. You are responsible for your use.
- **One provider in v1.** The configuration accepts several `[[providers]]`
  tables but only the Gracenote kind exists. Merging several providers is
  implemented and tested, but there is nothing to merge against in a real
  deployment yet.
- **The agreement signal is inert.** The quality score has an `agreement`
  signal (how well providers agree) and the `scoring.agreement` weight, but its
  default weight is 0 and with one provider it is a neutral constant, so it
  changes nothing today.
- **Postgres disk cap is advisory** (see above), and the SQLite cap counts used
  pages, not file size.
- **Single instance only.** Do not run two Patina instances against one database
  or one provider. There is no leader election; two instances would double the
  requests the provider sees and fight over the same rows.
- **No hot reload.** Config, tokens and overrides are read at startup. Restart
  to apply changes.
- **Tuner channel lists refresh every 10 minutes.** A channel scan on your tuner
  shows up in the feed after the next reload, not instantly.
- **Downgrades are unsupported** (see Migrations).
