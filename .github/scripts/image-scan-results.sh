#!/usr/bin/env bash
set -euo pipefail

policy=${1:?usage: image-scan-results.sh candidate|os-only INPUT_JSON}
input=${2:?}
case "$policy" in
  candidate)
    gate=$input
    ;;
  os-only)
    gate=${RUNNER_TEMP:-${TMPDIR:-/tmp}}/trivy-os-gate.json
    jq '.Results |= map(select(.Class == "os-pkgs"))' "$input" > "$gate"
    ;;
  *)
    echo "unknown scan policy: $policy" >&2
    exit 2
    ;;
esac

# Both policies report every HIGH/CRITICAL finding. Only the release gate differs: candidates gate all fixable
# CRITICAL findings, while patch rebuilds gate OS packages only (the only class that rebuild can repair).
trivy convert --severity CRITICAL --exit-code 1 --format table "$gate"
