#!/usr/bin/env bash
# Run the Helm chart checks that CI runs.
# Usage: scripts/chart-check.sh [lint|unittest|kubeconform|trivy|tokens|install|pins|all]
#
# `install` needs a running kind cluster and a local image named patina:ci
# (docker build -t patina:ci .); `all` does not run it. `pins` prints the tool
# versions below as KEY=value lines, which the Chart workflow appends to
# $GITHUB_ENV, so CI installs exactly what this script expects.
set -euo pipefail

# Pinned tool and schema versions. This block is the single source of truth:
# the Chart workflow reads it through `pins`, and Renovate bumps the lines that
# carry a "renovate:" comment (see customManagers in renovate.json). The checks
# for helm, helm-unittest, kubeconform and trivy print the installed version
# and fail if its major version (or major.minor while a tool is still 0.x)
# differs from the pin; a patch or minor difference is only printed. kind is
# printed by `install` but not enforced; ct is installed by CI at CT_VERSION and
# is not run by this script.
# renovate: datasource=github-releases depName=helm/helm
HELM_VERSION="4.3.1"
# renovate: datasource=github-releases depName=helm-unittest/helm-unittest
HELM_UNITTEST_VERSION="1.2.1"
# renovate: datasource=github-releases depName=yannh/kubeconform
KUBECONFORM_VERSION="0.8.0"
# renovate: datasource=github-releases depName=aquasecurity/trivy
TRIVY_VERSION="0.76.2"
# renovate: datasource=github-releases depName=kubernetes-sigs/kind
KIND_VERSION="0.33.0"
# renovate: datasource=github-releases depName=helm/chart-testing
CT_VERSION="3.15.0"
# Kubernetes schema version for kubeconform (not a tool; bumped by hand).
KUBERNETES_VERSION="1.32.0"

# The install check builds nothing itself: it expects this image in docker and
# loads it into the kind cluster. Scenarios under ci/ that can come up in a
# bare cluster: default (SQLite, generated tokens) and existing-secret (the
# Secret is created below). postgres needs a database server and all-extras
# needs the ServiceMonitor CRD, so those two are linted, templated and scanned
# but not installed.
IMAGE="patina:ci"
KIND_CLUSTER="${KIND_CLUSTER:-chart-testing}"
INSTALL_SCENARIOS=(default existing-secret)

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="$ROOT/charts/patina"
# Pinned to a datreeio/CRDs-catalog commit so schemas cannot change under CI.
# Bump: gh api repos/datreeio/CRDs-catalog/commits/main --jq .sha
CRD_CATALOG_SHA="04af58d740718c4dc719441bf39fb284c784f4a8"
CRD_SCHEMA="https://raw.githubusercontent.com/datreeio/CRDs-catalog/$CRD_CATALOG_SHA/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json"

need() {
  # need <tool> <install hint>
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: $1 is not installed. Install it with: $2" >&2
    exit 2
  fi
}

# require_version <tool> <actual> <pin>: print the installed version and fail
# when the major (or major.minor for 0.x) differs from the pin.
require_version() {
  local name="$1" actual="${2#v}" pin="$3" a p
  if [ "${pin%%.*}" = 0 ]; then
    a="$(cut -d. -f1,2 <<<"$actual")"
    p="$(cut -d. -f1,2 <<<"$pin")"
  else
    a="${actual%%.*}"
    p="${pin%%.*}"
  fi
  echo "    $name $actual (pinned $pin)"
  if [ "$a" != "$p" ]; then
    echo "error: $name $actual does not match pinned $pin; install the pinned version" >&2
    exit 1
  fi
}

helm_version() {
  require_version helm "$(helm version --template '{{.Version}}')" "$HELM_VERSION"
}

unittest_version() {
  require_version helm-unittest "$(helm plugin list | awk '$1 == "unittest" { print $2 }')" "$HELM_UNITTEST_VERSION"
}

pins() {
  printf '%s=%s\n' \
    HELM_VERSION "$HELM_VERSION" \
    HELM_UNITTEST_VERSION "$HELM_UNITTEST_VERSION" \
    KUBECONFORM_VERSION "$KUBECONFORM_VERSION" \
    TRIVY_VERSION "$TRIVY_VERSION" \
    KIND_VERSION "$KIND_VERSION" \
    CT_VERSION "$CT_VERSION" \
    KUBERNETES_VERSION "$KUBERNETES_VERSION"
}

lint() {
  need helm "brew install helm"
  helm_version
  local f
  for f in "$CHART"/ci/*-values.yaml; do
    echo "==> helm lint --strict ($(basename "$f"))"
    helm lint --strict "$CHART" -f "$f"
  done
}

unittest() {
  need helm "brew install helm"
  if ! helm plugin list 2>/dev/null | grep -q '^unittest'; then
    echo "error: helm-unittest plugin missing. Install it with: helm plugin install https://github.com/helm-unittest/helm-unittest --verify=false" >&2
    exit 2
  fi
  helm_version
  unittest_version
  echo "==> helm unittest"
  helm unittest "$CHART"
}

kubeconform() {
  need helm "brew install helm"
  need kubeconform "brew install kubeconform (pinned: v$KUBECONFORM_VERSION)"
  helm_version
  require_version kubeconform "$(command kubeconform -v)" "$KUBECONFORM_VERSION"
  local f
  for f in "$CHART"/ci/*-values.yaml; do
    echo "==> kubeconform ($(basename "$f"))"
    helm template patina "$CHART" -f "$f" |
      command kubeconform -strict -summary \
        -kubernetes-version "$KUBERNETES_VERSION" \
        -schema-location default \
        -schema-location "$CRD_SCHEMA"
  done
}

# trivy fails on HIGH and CRITICAL findings. With TRIVY_SARIF_FILE set it also
# writes one SARIF file that merges the findings of every scenario (the
# scenarios render the same templates, so results are de-duplicated); CI
# uploads it to code scanning. A failing scenario does not stop the loop: the
# merged SARIF is written first, then the function fails if any scenario had
# findings.
trivy() {
  need trivy "brew install trivy (pinned: v$TRIVY_VERSION)"
  require_version trivy "$(command trivy --version | awk '/^Version:/ { print $2 }')" "$TRIVY_VERSION"
  local f sarif_dir="" rc=0
  if [ -n "${TRIVY_SARIF_FILE:-}" ]; then
    need jq "brew install jq"
    sarif_dir="$(mktemp -d)"
  fi
  for f in "$CHART"/ci/*-values.yaml; do
    echo "==> trivy config ($(basename "$f"))"
    if [ -n "$sarif_dir" ]; then
      command trivy config --helm-values "$f" --severity HIGH,CRITICAL --format sarif \
        --output "$sarif_dir/$(basename "$f" .yaml).sarif" "$CHART" >/dev/null
    fi
    command trivy config --helm-values "$f" --severity HIGH,CRITICAL --exit-code 1 "$CHART" || rc=1
  done
  if [ -n "$sarif_dir" ]; then
    jq -s '. as $all | $all[0] | .runs[0].results = ([$all[].runs[].results[]?] | unique_by([.ruleId, (.locations | tostring), .message.text]))
      | .runs[0].tool.driver.rules = ([$all[].runs[].tool.driver.rules[]?] | unique_by(.id))' \
      "$sarif_dir"/*.sarif >"$TRIVY_SARIF_FILE"
    rm -rf "$sarif_dir"
    echo "SARIF written to $TRIVY_SARIF_FILE"
  fi
  return "$rc"
}

tokens() {
  need helm "brew install helm"
  echo "==> generated tokens (default-values.yaml)"
  local out feed admin
  out="$(helm template patina "$CHART" -f "$CHART/ci/default-values.yaml" --show-only templates/secret.yaml)"
  feed="$(printf '%s\n' "$out" | sed -n 's/^  PATINA_FEED_TOKEN: "\(.*\)"$/\1/p')"
  admin="$(printf '%s\n' "$out" | sed -n 's/^  PATINA_ADMIN_TOKEN: "\(.*\)"$/\1/p')"
  local name value
  for name in feed admin; do
    value="${!name}"
    if ! [[ "$value" =~ ^[A-Za-z0-9]{48}$ ]]; then
      echo "error: $name token is not 48 chars of [A-Za-z0-9]: '$value'" >&2
      exit 1
    fi
  done
  if [ "$feed" = "$admin" ]; then
    echo "error: feed and admin tokens are equal" >&2
    exit 1
  fi
  echo "ok: two distinct 48-char alphanumeric tokens"
}

# diagnose <namespace>: print what a failed install needs for debugging.
diagnose() {
  kubectl -n "$1" get pods -o wide || true
  kubectl -n "$1" describe pods || true
  kubectl -n "$1" logs -l app.kubernetes.io/name=patina --tail=100 --all-containers || true
}

# generated_tokens <namespace>: the generated Secret's two tokens (base64), one per line.
generated_tokens() {
  kubectl -n "$1" get secret -l app.kubernetes.io/instance=patina \
    -o jsonpath='{.items[0].data.PATINA_FEED_TOKEN}{"\n"}{.items[0].data.PATINA_ADMIN_TOKEN}{"\n"}'
}

# install: needs a kind cluster ($KIND_CLUSTER) and a local $IMAGE. Installs
# each bootable scenario with the local image (never pulled), waits for Ready,
# and proves helm upgrade keeps the generated tokens (the lookup branch of
# templates/secret.yaml cannot run under helm template).
install() {
  need helm "brew install helm"
  need kubectl "brew install kubectl"
  need kind "brew install kind (pinned: v$KIND_VERSION)"
  need docker "install Docker"
  helm_version
  echo "    kind $(kind version | awk '{ print $2 }') (pinned v$KIND_VERSION)"
  echo "==> loading $IMAGE into kind cluster $KIND_CLUSTER"
  kind load docker-image "$IMAGE" --name "$KIND_CLUSTER"
  local name ns before after
  for name in "${INSTALL_SCENARIOS[@]}"; do
    ns="patina-$name"
    echo "==> install ($name) in namespace $ns"
    kubectl create namespace "$ns"
    if [ "$name" = existing-secret ]; then
      # Fixture tokens for existingSecret: patina-tokens (ci/existing-secret-values.yaml).
      kubectl -n "$ns" create secret generic patina-tokens \
        --from-literal=PATINA_FEED_TOKEN="$(openssl rand -hex 24)" \
        --from-literal=PATINA_ADMIN_TOKEN="$(openssl rand -hex 24)"
    fi
    local args=(patina "$CHART" -n "$ns" -f "$CHART/ci/$name-values.yaml"
      --set image.repository="${IMAGE%%:*}" --set image.tag="${IMAGE##*:}" --set image.pullPolicy=Never
      --wait --timeout 180s)
    helm upgrade --install "${args[@]}" || {
      diagnose "$ns"
      exit 1
    }
    if [ "$name" = default ]; then
      echo "==> helm upgrade keeps generated tokens"
      before="$(generated_tokens "$ns")"
      helm upgrade "${args[@]}" || {
        diagnose "$ns"
        exit 1
      }
      after="$(generated_tokens "$ns")"
      if [ -z "$before" ] || [ "$before" != "$after" ]; then
        echo "error: generated tokens changed (or were empty) across helm upgrade" >&2
        exit 1
      fi
      echo "ok: tokens unchanged after upgrade"
    fi
    helm uninstall patina -n "$ns"
    kubectl delete namespace "$ns" --wait=false
  done
}

all() {
  lint
  unittest
  kubeconform
  trivy
  tokens
}

case "${1:-all}" in
  lint | unittest | kubeconform | trivy | tokens | install | pins | all) "${1:-all}" ;;
  *)
    echo "usage: $0 [lint|unittest|kubeconform|trivy|tokens|install|pins|all]" >&2
    exit 64
    ;;
esac
