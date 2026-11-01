# Patina

Patina is a guide (EPG) aggregator for [Jellyfin](https://jellyfin.org). It
fetches TV listings from guide providers (Gracenote first), keeps them in one
persisted "master guide", scores and merges them, and serves well-groomed
[XMLTV](https://wiki.xmltv.org/) feeds that Jellyfin's Live TV can read.

**What Patina is:** a small, self-hosted service that sits between unreliable
guide sources and your media server, with a database of what each source said
and when, so a bad fetch does not wipe your guide.

**What Patina is not:** a media server, a DVR, a tuner proxy or a streaming
service. It never touches video. It does not include guide data of its own,
and it is not affiliated with or endorsed by Gracenote or Jellyfin.

> **Status: pre-release (0.x).** The feature set for v1 is implemented, but no
> version has been released yet. Expect rough edges and changes.

**Running it?** The operator's guide is [`docs/operations.md`](docs/operations.md):
tokens, request budgets, channel overrides, backups, alert rules and
troubleshooting.

## Important: Gracenote terms and fragility

The Gracenote provider reads the same public listings web pages that
`tvlistings.gracenote.com` serves to browsers. That is **not an official or
licensed API**. Please understand what that means before you use it:

- Gracenote's terms of service may not permit automated access or
  redistribution of its data. **You are responsible for checking the terms that
  apply to you and for how you use Patina.** Use it for personal, non-commercial
  purposes only, and do not republish the data.
- The endpoint can change, rate-limit, block or disappear at any time without
  notice. Patina is built to be gentle (a hard daily request budget, backoff,
  a circuit breaker and a minimum interval between requests), but it cannot
  make an unofficial source reliable.
- The defaults are deliberately conservative. Do not raise `daily_budget`
  casually, and do not run several instances against the same provider.

## Quick start (Docker)

Patina needs a config file to know your lineup and tuner. Start from
[`config.example.toml`](config.example.toml); a minimal one:

```toml
# patina.toml
timezone = "America/Chicago"

[[providers]]
kind = "gracenote"
name = "gracenote"

[[providers.lineups]]
name = "home"
lineup_id = "USA-IL12345-X"   # your lineup id
postal_code = "60601"
country = "USA"

[[sources]]
kind = "hdhomerun"
url = "http://192.168.1.50"   # your tuner
lineups = ["home"]
```

Run it. The image is `ghcr.io/cyberglitchlabs/patina` (see
[Container image](#container-image) for the tags). No release exists yet (see
Status above), so until the first one use `:edge` instead of `:latest` below,
or build it yourself with `docker build -t patina .` in a checkout and use
`patina`:

```sh
docker volume create patina-data
docker run -d --name patina \
  --read-only \
  -p 8080:8080 \
  -v patina-data:/data \
  -v "$PWD/patina.toml:/etc/patina/patina.toml:ro" \
  -e PATINA_CONFIG=/etc/patina/patina.toml \
  ghcr.io/cyberglitchlabs/patina:latest
```

Check it:

```sh
curl http://localhost:8080/healthz          # -> ok
curl http://localhost:8080/xmltv/all.xml    # combined XMLTV feed (503 until the first guide fetch completes)
docker logs patina                          # JSON logs
```

Notes about the image:

- It runs as the non-root user `65532`, has no shell, and works with
  `--read-only`. **Only `/data` is writable** (a `VOLUME`). A named volume is
  fine; for a bind mount make the directory writable for uid 65532
  (`sudo chown 65532:65532 ./data`).
- The image sets `PATINA_DATABASE_URL=sqlite:///data/patina.db?mode=rwc` and
  `PATINA_LISTEN=0.0.0.0:8080`. Environment variables override the TOML file,
  so do not put `database_url` or `listen` in the file unless you also unset
  those variables.
- `HEALTHCHECK` runs `patina healthcheck`, a built-in probe of
  `GET /healthz` on the listen address from `PATINA_LISTEN` (a wildcard such
  as `0.0.0.0` or `::` is probed on loopback; any other address is probed as
  configured) with a 3 s timeout; exit 0 only on HTTP 200 with body `ok`. You can
  run it yourself: `docker exec patina /usr/local/bin/patina healthcheck`.
- `docker stop` sends SIGTERM; Patina finishes the guide window it is merging
  and exits with code 0. Allow up to 90 seconds (see the Kubernetes example).

## Container image

Registry path: `ghcr.io/cyberglitchlabs/patina`

| Tag | Meaning |
| --- | --- |
| `edge` | The latest push to `main`. Moves on every push; for trying things out. |
| `sha-<short>` | One specific commit (7 characters of the commit id). Never moves. |
| `X.Y.Z`, `X.Y`, `X`, `latest` | Releases, once the first one exists. |

Automatic builds (`edge`, `sha-*`) are `linux/amd64` only; releases are
multi-arch (`linux/amd64` and `linux/arm64`). Pull and run it as in the quick
start above, for example `docker pull ghcr.io/cyberglitchlabs/patina:edge`.

GHCR packages can be private by default. If `docker pull` answers `denied`,
a maintainer must set the package visibility to Public (package settings,
Change visibility; this cannot be done through the API), or you must pull with
an image pull secret.

## Configuration

Settings come from environment variables and an optional TOML file named by
`PATINA_CONFIG`. **Environment variables win over the file.** Everything is
validated at startup; a bad value stops the service with a message that names
the setting. The full annotated file is
[`config.example.toml`](config.example.toml). What an *empty* variable does
(text settings count as unset, numeric ones fail startup) is in
[`docs/operations.md`](docs/operations.md#configuration).

| Environment variable | TOML key | Default | Meaning |
| --- | --- | --- | --- |
| `PATINA_CONFIG` | n/a | none | Path of the TOML file |
| `PATINA_LISTEN` | `listen` | `0.0.0.0:8080` | Address and port to serve on |
| `PATINA_LOG` | `log` | `info` | Log filter, e.g. `debug` or `patina=debug,info` |
| `PATINA_DATABASE_URL` | `database_url` | `sqlite://patina.db?mode=rwc` (image: `sqlite:///data/patina.db?mode=rwc`) | `sqlite:` or `postgres://` URL |
| `PATINA_TIMEZONE` | `timezone` | `America/Chicago` | IANA timezone for XMLTV time offsets |
| `PATINA_FEED_TOKEN` | `feed_token` | none (open) | If set, XMLTV feed URLs require this token |
| `PATINA_ADMIN_TOKEN` | `admin_token` | none (open) | If set, `/status` and `/metrics` require `Authorization: Bearer <token>` |
| `PATINA_HORIZON_DAYS` | `horizon_days` | `7` | Days of guide to keep filled (1 to 14) |
| `PATINA_DISK_CAP_BYTES` | `disk_cap_bytes` | none | Soft limit on stored data; oldest evicted first |
| `PATINA_RETENTION_IMAGES_DAYS` | `retention.images_days` | `3` | Keep images this long after their last programme |
| `PATINA_RETENTION_RAW_DAYS` | `retention.raw_days` | `7` | Keep raw provider responses |
| `PATINA_RETENTION_PROGRAMMES_DAYS` | `retention.programmes_days` | `7` | Keep aired programmes |
| `PATINA_RETENTION_HISTORY_DAYS` | `retention.history_days` | `90` | Keep score and metrics history |

Structured settings exist only in the TOML file:

- `[[providers]]`: `kind` (`gracenote`), `name`, `daily_budget` (hard cap on
  requests per day, default 150), optional `base_url` (testing only), and one or
  more `[[providers.lineups]]` with `name`, `lineup_id`, `postal_code`,
  `country`.
- `[[sources]]`: where your channel list comes from. `kind = "hdhomerun"` with
  the tuner `url`, or `kind = "static"` with a hand-written
  `[[sources.channels]]` list; plus `lineups = [...]` (names from
  `providers.lineups`), optional `overrides_path`, and `include_unmatched`.
- `[scoring]`: weights for the quality score (`freshness`, `completeness`,
  `timeline_sanity`, `stability`, `reliability`, `hindsight_accuracy`,
  `agreement`).

### Endpoints

| Path | Purpose |
| --- | --- |
| `/healthz` | Always open; returns `ok` |
| `/xmltv/<source>.xml`, `/xmltv/all.xml` | XMLTV feeds (token required if `feed_token` is set) |
| `/t/<token>/xmltv/<source>.xml` | Same feeds with the token in the path |
| `/status` | JSON status (bearer token if `admin_token` is set) |
| `/metrics` | Prometheus metrics (bearer token if `admin_token` is set) |

Sources have no name key: a source is named by its kind plus its 1-based
position among sources of that kind (`hdhomerun-1`, `hdhomerun-2`, `static-1`).

## Using the feeds in Jellyfin

1. In Jellyfin: **Dashboard > Live TV > TV Guide Data Providers > Add >
   XMLTV**.
2. File or URL: `http://<patina-host>:8080/xmltv/hdhomerun-1.xml` (or
   `all.xml`). If you set `feed_token`, use
   `http://<patina-host>:8080/t/<token>/xmltv/hdhomerun-1.xml`; that form works
   in clients that cannot carry a query string.
3. Save, then refresh guide data (**Dashboard > Scheduled Tasks > Refresh
   Guide**). Channels are matched to your tuner's channels by Patina, so
   Jellyfin sees the right programmes under the right channel numbers.

## Fixing channel matches: the overrides file

Patina matches tuner channels to provider channels by number, then call sign.
When it gets one wrong (or you want a channel hidden), point
`overrides_path` of the source at a TOML file of `[[override]]` tables. Each
needs exactly one selector (`number` or `name`) and exactly one action
(`provider_channel_id`, `canonical`, or `ignore`):

```toml
# overrides.toml

# The tuner calls this channel "FOX 9" but numbers it oddly: pin it to the
# provider's channel. A `name` selector is an exact match, ignoring case and
# surrounding spaces ("FOX 9" does not match "FOX 9+"), and applies to EVERY
# tuner channel with that name.
[[override]]
name = "FOX 9"
provider_channel_id = "12345"

# Hide a junk subchannel by number.
[[override]]
number = "5000"
ignore = true

# Create a channel the provider does not know, with a fixed XMLTV id.
[[override]]
number = "99.1"
canonical = "MYCH.us"
```

If a name and a number entry both match one channel, the earlier entry wins.
Overrides are validated at startup and read only then (restart to apply a
change); a typo fails the start and names the entry. To find provider channel
ids and to test a change, see
[`docs/operations.md`](docs/operations.md#channel-matching-and-the-overrides-file).

## Postgres

SQLite on a local volume is the default and is fine for one instance. To use
PostgreSQL instead, set the database URL (an environment variable, so it can
come from a Secret):

```sh
PATINA_DATABASE_URL='postgres://patina:secret@db.example:5432/patina?sslmode=require'
```

TLS to Postgres is supported through rustls (with `ring` as the crypto provider
and the bundled Mozilla root certificates), so managed databases that demand
`sslmode=require` work. Supported modes are the usual libpq ones (`disable`,
`prefer` (the default), `require`, `verify-ca`, `verify-full`); use
`sslrootcert=/path/to/ca.pem` for a private CA (mount the file into the
container). Patina creates and migrates its own tables at startup.

## Kubernetes

[`docs/examples/kubernetes.yaml`](docs/examples/kubernetes.yaml) is a complete
example: a Deployment (1 replica, PVC at `/data`, `readOnlyRootFilesystem`,
`runAsNonRoot`, `terminationGracePeriodSeconds: 90`, probes), a ConfigMap for
the TOML, a Secret for the tokens, and a Service.

## Security notes

- Set `feed_token` and `admin_token` unless Patina is reachable only from a
  trusted network. With no tokens the feeds, `/status` and `/metrics` are open;
  Patina logs this at startup, and warns if a token variable is set but empty.
- Prefer environment variables or a Secret for tokens. Keep them out of the
  TOML file if that file is in version control.
- A feed token appears in feed URLs (query string or `/t/<token>/` path). Patina
  never logs URLs, paths or headers, but a reverse proxy in front of it might;
  the admin token is only accepted in the `Authorization` header for that
  reason. Use HTTPS if the traffic crosses an untrusted network.
- `/healthz` is always open by design (probes carry no credentials).
- The container is non-root with a read-only root filesystem and no shell.

## Development

```sh
cargo test                       # SQLite only; Postgres tests are skipped
# With a Postgres test server:
docker run -d --rm --name patina-test-pg -e POSTGRES_PASSWORD=postgres -p 55432:5432 postgres:18
PATINA_TEST_POSTGRES_URL=postgres://postgres:postgres@localhost:55432/postgres \
  PATINA_REQUIRE_POSTGRES=1 cargo test
cargo deny check && cargo audit  # supply-chain checks
```

`tests/e2e_full.rs` starts the real startup wiring (`patina::app`) against a
fake provider and a fake tuner, on SQLite and (when configured) Postgres.
No test contacts a real provider. A manual live check exists
(`cargo test --test live_smoke -- --ignored`, two requests), also available as
the "Live smoke" GitHub workflow (manual trigger only).

## Releasing

Releases are automated with [release-please](https://github.com/googleapis/release-please).

1. Merge commits to `main` using
   [conventional commits](https://www.conventionalcommits.org) (`feat:`,
   `fix:`, `docs:`, ...). While the version is 0.x, release-please is set to
   `bump-minor-pre-major` and `bump-patch-for-minor-pre-major`
   ([`release-please-config.json`](release-please-config.json)): a `feat:`
   bumps the **patch** (0.1.0 to 0.1.1), a breaking change (`feat!:` or a
   `BREAKING CHANGE:` footer) bumps the **minor** (0.1.x to 0.2.0), and `fix:`
   also bumps the patch.
2. release-please opens (and keeps updating) a **release PR** that bumps the
   version in `Cargo.toml`/`Cargo.lock` and writes `CHANGELOG.md`. The first
   one proposes the first release after 0.1.0.
3. Merging the release PR creates the tag (`vX.Y.Z`) and GitHub release. The
   same workflow then builds the `linux/amd64` + `linux/arm64` image and pushes
   `ghcr.io/cyberglitchlabs/patina` with the tags `X.Y.Z`, `X.Y`, `X` and
   `latest`, plus an SBOM and a build-provenance attestation:
   `gh attestation verify oci://ghcr.io/cyberglitchlabs/patina:<version> -R cyberglitchlabs/patina`.

One-time repository setup:

- Settings > Actions > General > Workflow permissions: enable **Allow GitHub
  Actions to create and approve pull requests**.
- **Release PRs and CI:** pull requests created with the default
  `GITHUB_TOKEN` do not trigger other workflows, so release PRs get no CI run
  (a problem if branch protection requires CI). To fix it, create a
  fine-grained personal access token or a GitHub App token with **contents:
  write** and **pull-requests: write** on this repository and save it as the
  repository secret `RELEASE_PLEASE_TOKEN`. The release workflow uses it when
  present and falls back to `GITHUB_TOKEN` otherwise.

## License

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
