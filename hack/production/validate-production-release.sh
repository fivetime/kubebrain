#!/usr/bin/env bash
set -euo pipefail

KUBE_CONTEXT="${KUBE_CONTEXT:-}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
INSTANCE_READY_COMMAND="${INSTANCE_READY_COMMAND:-${SCRIPT_DIR}/validate-instance-ready.sh}"
REGION_HEALTH_COMMAND="${REGION_HEALTH_COMMAND:-${SCRIPT_DIR}/validate-tikv-region-health.sh}"

die() { echo "$*" >&2; exit 1; }

[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required"
[[ "$INSTANCE_READY_COMMAND" == /* && -x "$INSTANCE_READY_COMMAND" ]] || \
  die "INSTANCE_READY_COMMAND must be an executable absolute path"
[[ "$REGION_HEALTH_COMMAND" == /* && -x "$REGION_HEALTH_COMMAND" ]] || \
  die "REGION_HEALTH_COMMAND must be an executable absolute path"

"$INSTANCE_READY_COMMAND"
"$REGION_HEALTH_COMMAND"

echo "KubeBrain production release gate passed: instance readiness and PD/TiKV Region/storage health are both verified"
