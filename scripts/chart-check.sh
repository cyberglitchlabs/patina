#!/usr/bin/env bash
# Run the Helm chart checks that CI runs.
# Usage: scripts/chart-check.sh [lint|unittest|kubeconform|trivy|tokens|all]
set -euo pipefail

# Pinned tool and schema versions (Renovate and CI keep these in step).
KUBERNETES_VERSION="1.32.0"
KUBECONFORM_VERSION="0.6.7"
TRIVY_VERSION="0.58.0"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="$ROOT/charts/patina"
CRD_SCHEMA='https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'

need() {
  # need <tool> <install hint>
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: $1 is not installed. Install it with: $2" >&2
    exit 2
  fi
}

scenarios() {
  local f
  for f in "$CHART"/ci/*-values.yaml; do
    echo "$f"
  done
}

lint() {
  need helm "brew install helm"
  local f
  for f in $(scenarios); do
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
  echo "==> helm unittest"
  helm unittest "$CHART"
}

kubeconform() {
  need helm "brew install helm"
  need kubeconform "brew install kubeconform (pinned: v$KUBECONFORM_VERSION)"
  local f
  for f in $(scenarios); do
    echo "==> kubeconform ($(basename "$f"))"
    helm template patina "$CHART" -f "$f" |
      command kubeconform -strict -summary \
        -kubernetes-version "$KUBERNETES_VERSION" \
        -schema-location default \
        -schema-location "$CRD_SCHEMA"
  done
}

trivy() {
  need trivy "brew install trivy (pinned: v$TRIVY_VERSION)"
  local f
  for f in $(scenarios); do
    echo "==> trivy config ($(basename "$f"))"
    command trivy config --helm-values "$f" --severity HIGH,CRITICAL --exit-code 1 "$CHART"
  done
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

all() {
  lint
  unittest
  kubeconform
  trivy
  tokens
}

case "${1:-all}" in
  lint | unittest | kubeconform | trivy | tokens | all) "${1:-all}" ;;
  *)
    echo "usage: $0 [lint|unittest|kubeconform|trivy|tokens|all]" >&2
    exit 64
    ;;
esac
