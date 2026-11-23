# Helm Chart Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `charts/patina`, a simple standalone Helm chart for Patina, with full lint, test, schema and security automation in CI.

**Architecture:** One conventional chart: Deployment (1 replica, Recreate), ConfigMap holding a raw TOML string, a Secret for tokens, a PVC or Postgres, a Service, and three optional extras (Ingress, ServiceMonitor, NetworkPolicy). Behaviour is pinned by helm-unittest suites; `scripts/chart-check.sh` runs everything CI runs.

**Tech Stack:** Helm 4 (templates must also work on 3.x), helm-unittest, chart-testing (`ct`), kubeconform, Trivy, kind, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-11-22-helm-chart-design.md` (read it first). Project rules: `AGENTS.md`, `CONTRIBUTING.md` (open it when editing docs or CI). Use example ZIP `55401` and address `192.168.1.50` in every fixture and doc.

## Global Constraints

- Container runs as user `65532`, `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, capabilities dropped `ALL`, seccomp `RuntimeDefault`; `/data` is the only writable path.
- 1 replica only (`replicaCount` schema `const: 1`), strategy `Recreate`, `terminationGracePeriodSeconds: 90`.
- Probes: startup and readiness `httpGet /healthz`; liveness `exec ["/usr/local/bin/patina","healthcheck"]` (image has no shell).
- Config file path in the pod: `/etc/patina/patina.toml`, via `PATINA_CONFIG`. `PATINA_DATABASE_URL` and `PATINA_LISTEN` come from the image or the chart, never the TOML.
- Tokens are never empty by default. Env var names: `PATINA_FEED_TOKEN`, `PATINA_ADMIN_TOKEN`, `PATINA_DATABASE_URL`.
- Image: `ghcr.io/cyberglitchlabs/patina`, tag defaults to `.Chart.AppVersion`; never `latest`.
- All GitHub Actions pinned by commit digest with a version comment (same as `ci.yml`); workflow `permissions: contents: read` unless a job needs more.
- Chart `version: 0.1.0`; labels follow `app.kubernetes.io/*`; selectors match pod labels.
- Do not publish anything and do not push to `main`. Work on `feat/helm-chart`.

## Review Focus

- Numbers in `config` must reach Patina as integers (`daily_budget = 150`, not `150.0`): the chart passes the TOML string through untouched; pinned in Task 2.
- A `config` that sets `listen` or `database_url` must fail the install with a message naming the key (pinned in Task 2).
- Invalid TOML in `config` must fail `helm template`, not the running pod (Task 2).
- `helm upgrade` must not rotate generated tokens, or every Jellyfin feed URL breaks (Task 7 install test).
- Postgres plus an existing claim or enabled PVC must fail with a clear message, and Postgres must still mount a writable `/data` (emptyDir) because SQLite scratch files use it (Task 4).
- `existingSecret` missing a required key: the pod fails at start, so the chart documents the required keys and the unit test pins the key names (Task 3).
- A config change must restart the pod (checksum annotation) (Task 4).

---

### Task 1: Chart skeleton, values, schema, scripts, first CI-free checks

**Files:**
- Create: `charts/patina/Chart.yaml`, `charts/patina/values.yaml`, `charts/patina/values.schema.json`, `charts/patina/templates/_helpers.tpl`, `charts/patina/.helmignore`, `charts/patina/ci/default-values.yaml`, `charts/patina/ci/postgres-values.yaml`, `charts/patina/ci/existing-secret-values.yaml`, `charts/patina/ci/all-extras-values.yaml`, `ct.yaml`, `scripts/chart-check.sh`
- Test: `charts/patina/tests/schema_test.yaml`

**Interfaces:**
- Produces helpers in `_helpers.tpl`: `patina.name`, `patina.fullname`, `patina.labels`, `patina.selectorLabels`, `patina.secretName` (returns `.Values.existingSecret` or `<fullname>`), `patina.image` (`repository:tag`, tag defaults `.Chart.AppVersion`).
- Produces the values contract (names later tasks rely on): `image.{repository,tag,pullPolicy}`, `replicaCount`, `config` (string), `overrides` (string), `existingSecret` (string), `database.urlSecret.{name,key}`, `persistence.{enabled,size,storageClass,existingClaim}`, `service.{type,port}`, `ingress.{enabled,className,annotations,hosts[],tls[]}`, `serviceMonitor.{enabled,interval,labels}`, `networkPolicy.{enabled,ingressFrom[]}`, `resources`, `podAnnotations`, `nodeSelector`, `tolerations`, `affinity`, `extraEnv`.
- `ci/*-values.yaml` are the shared scenarios used by lint, kubeconform, Trivy and `ct install`. `ci/default-values.yaml` carries a minimal valid `config` (timezone, one Gracenote lineup `USA-OTA55401`, ZIP `55401`, one HDHomeRun source at `http://192.168.1.50`).

- [ ] **Step 1: Write the failing schema tests** in `schema_test.yaml` (helm-unittest `failedTemplate` assertions against `templates/deployment.yaml` once it exists; until then against `templates/configmap.yaml`): `replicaCount: 2` fails; unknown top-level key `bogus: 1` fails; `service.port: "x"` fails; `persistence.size: 5` (non-string) fails.
- [ ] **Step 2: Run** `helm unittest charts/patina` (install the plugin if missing: `helm plugin install https://github.com/helm-unittest/helm-unittest`). Expected: FAIL, chart missing.
- [ ] **Step 3: Create the chart files** above. `values.schema.json` is draft-07 with `additionalProperties: false` at the top level and for each object, `replicaCount` `const: 1`, `config` and `overrides` strings. `values.yaml` has a comment per key; `config` defaults to the commented-out example from `config.example.toml` (valid empty TOML).
- [ ] **Step 4: Write `scripts/chart-check.sh`** (`set -euo pipefail`) with one function per CI check so Task 7 reuses them: `lint`, `unittest`, `kubeconform`, `trivy`. Each loops over `charts/patina/ci/*-values.yaml`. Pin tool versions in variables at the top. `kubeconform` runs `helm template ... | kubeconform -strict -summary -kubernetes-version <pinned> -schema-location default -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'`. `trivy` runs `trivy config --helm-values <file> --severity HIGH,CRITICAL --exit-code 1 charts/patina`. Missing tools print an install hint and exit 2.
- [ ] **Step 5: Run** `scripts/chart-check.sh lint` and `helm unittest charts/patina`. Expected: lint OK for each scenario (templates arrive next, so lint may only cover the skeleton); schema tests PASS.
- [ ] **Step 6: Commit** `feat(chart): chart skeleton, values contract and schema`.

### Task 2: Config ConfigMap and TOML validation

**Files:**
- Create: `charts/patina/templates/configmap.yaml`
- Test: `charts/patina/tests/configmap_test.yaml`

**Interfaces:**
- Consumes: `patina.fullname`, `patina.labels` (Task 1).
- Produces: ConfigMap `<fullname>` with key `patina.toml` and, when `overrides` is set, key `overrides.toml`. Template-level validation named `patina.validateConfig` in `_helpers.tpl`.

- [ ] **Step 1: Write failing tests** in `configmap_test.yaml`: (a) the config string with `daily_budget = 150` renders byte-for-byte in `data["patina.toml"]` (matchRegex `daily_budget = 150\n`, notMatchRegex `150\.0`); (b) invalid TOML (`timezone = `) fails with message containing `config is not valid TOML`; (c) `config` containing `listen = "0.0.0.0:9"` fails with message containing `listen`; (d) same for `database_url`; (e) `overrides` set but `config` lacks `overrides_path` fails with `overrides_path`; (f) `overrides` set and path present renders `overrides.toml`.
- [ ] **Step 2: Run** `helm unittest charts/patina -f tests/configmap_test.yaml`. Expected: FAIL.
- [ ] **Step 3: Implement** `configmap.yaml` and `patina.validateConfig`: parse with `mustFromToml`, `fail` with the messages above. Overrides mount path is `/etc/patina/overrides.toml`; (e) accepts `overrides_path` anywhere under `[[sources]]` by checking the parsed structure.
- [ ] **Step 4: Run** the suite. Expected: PASS.
- [ ] **Step 5: Commit** `feat(chart): config ConfigMap with TOML validation`.

### Task 3: Token Secret

**Files:**
- Create: `charts/patina/templates/secret.yaml`
- Test: `charts/patina/tests/secret_test.yaml`

**Interfaces:**
- Consumes: `patina.secretName`.
- Produces: Secret `<fullname>` with `PATINA_FEED_TOKEN` and `PATINA_ADMIN_TOKEN` (24 random hex chars each via `randAlphaNum`/hex), created only when `existingSecret` is empty. If a Secret of that name already exists in the cluster, its values are reused via `lookup` so upgrades keep tokens. Annotation `helm.sh/resource-policy: keep` is NOT set (uninstall removes it).

- [ ] **Step 1: Write failing tests**: default render has a Secret with both keys, each matching `^[0-9a-f]{48}$` or the chosen length (pick one length and pin it); setting `existingSecret: mine` renders no Secret; both tokens differ from each other.
- [ ] **Step 2: Run** the suite. Expected: FAIL.
- [ ] **Step 3: Implement** `secret.yaml` with `lookup "v1" "Secret" .Release.Namespace (include "patina.fullname" .)` reuse. The README (Task 8) documents the required keys of a user-made Secret.
- [ ] **Step 4: Run** the suite. Expected: PASS. (`lookup` returns empty under `helm template`, so reuse is proven by the install test in Task 7.)
- [ ] **Step 5: Commit** `feat(chart): generated tokens kept across upgrades`.

### Task 4: Deployment, PVC, Service

**Files:**
- Create: `charts/patina/templates/deployment.yaml`, `pvc.yaml`, `service.yaml`, `NOTES.txt`
- Test: `charts/patina/tests/deployment_test.yaml`, `pvc_test.yaml`, `service_test.yaml`

**Interfaces:**
- Consumes: all helpers; ConfigMap and Secret names from Tasks 2 and 3.
- Produces: Deployment `<fullname>`, PVC `<fullname>` (when `persistence.enabled` and no `existingClaim` and no `database.urlSecret.name`), Service `<fullname>` port `service.port` (default 8080) named `http`.

- [ ] **Step 1: Write failing tests** pinning every Global Constraint: security contexts, `strategy.type: Recreate`, grace period 90, the three probes, image `ghcr.io/cyberglitchlabs/patina:<appVersion>`, `envFrom` secretRef to the secret name, env `PATINA_CONFIG=/etc/patina/patina.toml`, volume mounts `/data` and `/etc/patina` (read-only), pod annotations `checksum/config` and `checksum/secret` (the latter only for the chart-made Secret). Postgres case: with `database.urlSecret` set, env `PATINA_DATABASE_URL` comes from that secret key, no PVC document, `/data` is an `emptyDir`. Setting both `database.urlSecret.name` and `persistence.existingClaim` fails with a message naming both. `existingClaim` uses that claim and renders no PVC. Selector labels match pod labels.
- [ ] **Step 2: Run** the suites. Expected: FAIL.
- [ ] **Step 3: Implement** the templates. Resources default to the example manifest's (requests 50m/64Mi, limit memory 256Mi). `NOTES.txt` prints how to read the generated tokens and the feed URL shape.
- [ ] **Step 4: Run** `helm unittest charts/patina` and `scripts/chart-check.sh lint kubeconform`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(chart): deployment, volume and service`.

### Task 5: Optional extras (Ingress, ServiceMonitor, NetworkPolicy)

**Files:**
- Create: `charts/patina/templates/ingress.yaml`, `servicemonitor.yaml`, `networkpolicy.yaml`
- Test: `charts/patina/tests/extras_test.yaml`

**Interfaces:**
- Consumes: values from Task 1; Service name from Task 4.
- Produces: all three off by default (no documents rendered). ServiceMonitor scrapes `/metrics` on port `http` with `authorization.credentials` pointing at the Secret key `PATINA_ADMIN_TOKEN` (name from `patina.secretName`). NetworkPolicy allows ingress to the `http` port only from `networkPolicy.ingressFrom`.

- [ ] **Step 1: Write failing tests**: default renders none of the three; enabled each renders the right kind, ServiceMonitor references the admin token key, Ingress has the host and `ingressClassName`, NetworkPolicy selects the pod labels.
- [ ] **Step 2: Run.** Expected: FAIL. **Step 3: Implement.** **Step 4: Run** unit tests plus `scripts/chart-check.sh kubeconform` against `ci/all-extras-values.yaml` (validates the ServiceMonitor via the CRDs-catalog). Expected: PASS.
- [ ] **Step 5: Commit** `feat(chart): optional ingress, servicemonitor and networkpolicy`.

### Task 6: Security scan gate

**Files:**
- Modify: `scripts/chart-check.sh` (the `trivy` function from Task 1), `.trivyignore` if a finding is a deliberate, documented exception
- Test: run of `scripts/chart-check.sh trivy`

- [ ] **Step 1: Run** `scripts/chart-check.sh trivy` over all four scenarios. Expected: exit 0. A HIGH or CRITICAL finding is fixed in the template, not suppressed; a suppression needs a one-line reason in `.trivyignore`.
- [ ] **Step 2: Add a negative proof**: run `trivy config --helm-set securityContext.privileged=true`-style override in a throwaway command (not committed) only if the chart exposes it; otherwise skip. Record in the commit body that the gate was exercised.
- [ ] **Step 3: Commit** `ci(chart): trivy misconfiguration gate`.

### Task 7: CI workflow

**Files:**
- Create: `.github/workflows/chart.yml`, `charts/patina/ct-lintconf.yaml` (yamllint rules) if `ct` needs it
- Modify: `ct.yaml`, `renovate.json` (add the `helm-values`/`custom` manager only if the chart pins tool versions in a parseable place; otherwise leave Renovate to the existing action pins)

**Interfaces:**
- Consumes: `scripts/chart-check.sh` functions.
- Produces: required jobs `lint`, `unittest`, `kubeconform`, `trivy`, `ct-lint`, `install` in workflow `Chart`. Triggers: `pull_request` and `push` to `main` with `paths: ['charts/**','scripts/chart-check.sh','.github/workflows/chart.yml']`, plus the same concurrency block as `ci.yml`.

- [ ] **Step 1: Write the workflow.** `lint`, `unittest`, `kubeconform`, `trivy` each call the script function (so local and CI cannot drift). `ct-lint` runs `ct lint --config ct.yaml` with `fetch-depth: 0` and `additional-commands` set to `helm unittest {{ .Path }}`. `trivy` uploads SARIF (`github/codeql-action/upload-sarif`, `security-events: write` on that job only). `install` uses `helm/kind-action` and `ct install --config ct.yaml --helm-extra-args "--timeout 180s"` with `image.tag=edge` and `image.pullPolicy=Always`, then runs `helm upgrade` on the release and fails if the generated Secret's `PATINA_FEED_TOKEN` changed (read with `kubectl get secret -o jsonpath` before and after).
- [ ] **Step 2: Pin every action by digest** (look up current commit SHAs with `gh api`; keep a `# vX.Y.Z` comment). Pin tool versions in `scripts/chart-check.sh` variables.
- [ ] **Step 3: Validate locally** with `actionlint` if available, and run `scripts/chart-check.sh` for all functions. Expected: all pass.
- [ ] **Step 4: Push the branch only after the user approves**, open the PR, and confirm every job is green. Do not claim CI passes until it has run.
- [ ] **Step 5: Commit** `ci(chart): lint, unit, schema, scan and kind install jobs`.

### Task 8: Docs and cleanup

**Files:**
- Create: `charts/patina/README.md`
- Modify: `README.md` (Kubernetes section), `docs/operations.md` (any Kubernetes pointers), `CONTRIBUTING.md` and `AGENTS.md` (one line each: run `scripts/chart-check.sh` before and after chart changes)
- Delete: `docs/examples/kubernetes.yaml` (after confirming nothing else links to it: `grep -rn kubernetes.yaml`)

- [ ] **Step 1: Write the chart README**: install (`helm install patina charts/patina -n patina --create-namespace -f my-values.yaml`), a minimal values example with ZIP `55401`, the token model (generated vs `existingSecret` with its required keys), Postgres setup, upgrade and uninstall notes, and a values table. Generate the table with `helm-docs` only if its current state checks out; otherwise write it by hand and add a CI step later. State plainly that the chart is not yet published.
- [ ] **Step 2: Update README, CONTRIBUTING, AGENTS**, remove the raw example and fix all links. Run the repo's own checks from `.github/workflows/ci.yml` (fmt, clippy, tests, deny, audit) because docs changed.
- [ ] **Step 3: Commit** `docs(chart): chart README and move off the raw manifest`.

---

## Self-review notes

- Spec coverage: workload, config, tokens, storage, extras, schema, CI items 1 to 8, docs, and the non-goals are each owned by a task. Spec item 7 (values-table drift) is conditional on helm-docs, called out in Task 8.
- The config design differs from the first-approved design on purpose (raw TOML string); the spec was updated first.
