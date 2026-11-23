# Helm chart design

Status: approved by the maintainer on 2026-11-22.

## Intent

Give Kubernetes users a supported way to install Patina, replacing the raw
`docs/examples/kubernetes.yaml`. The chart must be safe by default (tokens
fail closed) and must be held to the same standard as the Rust code: every
change to it is linted, rendered, schema-checked and install-tested in CI.

## Goals

- `charts/patina` installs a working Patina with SQLite on a PVC, or with
  Postgres, from `values.yaml` alone.
- Safe defaults: the pod security settings of the example manifest, and feed
  and admin tokens that are never empty.
- Config is values-driven: structured settings render to `patina.toml`.
- Full lint and automation is mandatory (see CI below). A chart change that
  skips any of it does not merge.

## Non-goals

- Publishing the chart (GHCR OCI) and release-please wiring. A later change,
  and it needs the maintainer's go-ahead because publishing is public.
- More than one replica. Patina runs a single scheduler.
- Managing a Postgres server.

## Approaches

One sensible way: a conventional single Helm chart in `charts/patina`.
Alternatives rejected: Kustomize (no values schema, weaker secret
generation), a chart that wraps the raw manifest with `.Files` (no real
templating of config), an operator (far too heavy for one pod).

## Design

- **Workload:** Deployment, 1 replica, `Recreate` strategy, user 65532,
  read-only root filesystem, dropped capabilities, seccomp `RuntimeDefault`,
  grace period 90 s, `/healthz` startup and readiness probes, exec liveness
  probe `patina healthcheck`. `/data` is the only writable path. Pod
  annotation checksums of the ConfigMap and chart-made Secret restart the pod
  on change.
- **Config:** `config:` in values is a plain TOML string (the format of
  `config.example.toml`), written to a ConfigMap as `patina.toml`. It is not
  rendered from structured values: Helm's `toToml` writes every number as a
  float (`daily_budget = 150.0`), which Patina's integer settings reject
  (verified 2026-11-22). The template parses the string with `mustFromToml`
  so a syntax error fails the install, and fails if it sets `listen` or
  `database_url` (the chart owns those). Optional `overrides` text is mounted
  and the template fails unless the TOML names it as `overrides_path`.
- **Tokens:** `existingSecret` names a user-made Secret. Otherwise the chart
  makes one with random feed and admin tokens, kept across upgrades with
  `lookup`.
- **Storage:** PVC by default (`size`, `storageClass`), or
  `persistence.existingClaim`. With Postgres (a Secret key holding
  `PATINA_DATABASE_URL`) no PVC is created.
- **Optional, off by default:** Ingress, ServiceMonitor (bearer token from the
  Secret), NetworkPolicy.
- **Values contract:** `values.schema.json` validates input; unknown or
  mistyped keys fail at install time.
- **Docs:** `charts/patina/README.md` (generated values table), README
  Kubernetes section points at the chart, the raw example is removed.

## CI and automation (mandatory)

All in `.github/workflows`, running on pull requests that touch `charts/**`
and on `main`:

1. `helm lint --strict` on every values scenario.
2. `helm template` for each scenario (default SQLite, Postgres, existing
   secret, existing claim, all optional extras on), piped to `kubeconform`
   with `-strict` against the pinned Kubernetes schemas and the
   ServiceMonitor CRD (Datree CRDs-catalog as a second schema location).
3. `chart-testing` (`ct lint`): chart version bump check, maintainers and
   yamllint.
4. `helm-unittest` suites covering the behaviours above (token generation,
   no PVC with Postgres, checksum annotations, config handling).
5. `trivy config --helm-values <scenario>` on the chart, failing on HIGH and
   CRITICAL, SARIF uploaded to code scanning. One policy scanner, not two.
6. A kind cluster install test, `scripts/chart-check.sh install`
   (`helm upgrade --install --wait` per installable scenario, then a check
   that `helm upgrade` keeps the generated tokens). It is not `ct install`
   because the postgres and all-extras scenarios cannot come up in a bare
   kind cluster (no database server, no ServiceMonitor CRD); the existing
   Secret is pre-created; the image is built from the Dockerfile and loaded
   into kind with `pullPolicy: Never` rather than pulling `edge`.
7. `values.schema.json` and the README values table checked for drift
   (`helm-docs` in CI, failing on a diff).
8. Renovate manages the chart's action pins and tool versions; actions are
   pinned by digest like the existing workflows.

Local equivalents live in a script (`scripts/chart-check.sh`) so contributors
run what CI runs; `CONTRIBUTING.md` and `AGENTS.md` say so.

## Calls made by the author of this doc

- Chart `version` starts at `0.1.0`; `appVersion` tracks the release.
- Namespace is chosen with `helm install -n`; Service is ClusterIP.
- Work happens on branch `feat/helm-chart` and lands through a PR.
- Scanner is Trivy (chosen after checking current docs, 2026-11-22). helm-docs v1.14.2 was confirmed (2026-11-22: repo active, last release 2024-07-08) and adopted for the values-table drift check (`scripts/chart-check.sh docs`).
- Standalone chart, not a bjw-s `app-template` wrapper (the maintainer's call).
