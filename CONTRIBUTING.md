# Contributing to Patina

For people and agents alike. `AGENTS.md` points here, and the README links here. Read the section that matches your change.


## Migrations and persisted data

- Migrations are forward-only, numbered per backend (`migrations/sqlite`, `migrations/postgres`). Both backends get every change; an applied migration stays as it is.
- Anything persisted that embeds a host or format (image URLs, for example) needs a data migration on both backends. Migration 0009 is the example. See ADR 0002.

## XMLTV output

- Each channel emits `<number> <name>` and then a bare numeric `<number>` as separate display-names, because Jellyfin learns the channel number only from the numeric one. See ADR 0001. `tests/xmltv.rs` and `tests/golden/fixture_slots.xml` pin it.
- Jellyfin caches the downloaded XMLTV for about an hour, so a feed change can look unapplied for that long.

## Fetching and request gates

- A failed or thin fetch leaves the master guide as it was.
- A gate that cannot be checked sends no request. Keep that when adding or changing a gate; the daily budget is per provider and survives restarts.
- CI contacts no real provider; only the manual `live-smoke` workflow does.

## Refactoring

- Run the whole `tests/` suite first and again at the end. Every test that passed before passes after, and the golden XMLTV file is unchanged.
- After any scripted bulk edit (`sed` and the like), run `cargo fmt`.

## Helm chart

- Run `scripts/chart-check.sh all` before and after any change under `charts/patina`; it is what `.github/workflows/chart.yml` runs (`install` needs kind and is separate). Tool versions are pinned in the script.
- Any change under `charts/patina` needs a `version` bump in `Chart.yaml`; `ct` enforces it on pull requests.
- After a release, bump `appVersion` (and `version`) in `charts/patina/Chart.yaml` by hand; release-please does not touch it. If that is automated later (extra-files), the release PR also touches `charts/**`, so it needs a `version` bump too or `ct lint` (check-version-increment) fails.
- The values table in `charts/patina/README.md` is generated: edit `values.yaml` comments (`# --`) or `README.md.gotmpl`, then run `scripts/chart-check.sh docs-update`.

## Documents

| File | Holds | Who writes it |
| --- | --- | --- |
| `GLOSSARY.md` | Shared vocabulary | Anyone; each change is a reviewed diff |
| `docs/adr/` | Hard-to-reverse choices made after v1 | Whoever makes the choice; facts checked with the session that holds them |
| `docs/superpowers/specs/2026-10-31-patina-design.md` | v1 intent, goals, anti-goals | Frozen |
| `docs/operations.md`, `charts/patina/README.md` | Running the service | Deployment/operations |

The user decides design questions and who owns which document. An agent recommends, checks facts and writes down what was settled; a decision that lives only in a chat is lost.
