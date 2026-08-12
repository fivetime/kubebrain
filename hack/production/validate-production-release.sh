#!/usr/bin/env bash
set -euo pipefail

KUBE_CONTEXT="${KUBE_CONTEXT:-}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
TIDB_OPERATOR_COMMAND="${TIDB_OPERATOR_COMMAND:-${SCRIPT_DIR}/validate-tidb-operator-ready.sh}"
INSTANCE_READY_COMMAND="${INSTANCE_READY_COMMAND:-${SCRIPT_DIR}/validate-instance-ready.sh}"
REGION_HEALTH_COMMAND="${REGION_HEALTH_COMMAND:-${SCRIPT_DIR}/validate-tikv-region-health.sh}"
STORAGE_LATENCY_COMMAND="${STORAGE_LATENCY_COMMAND:-${SCRIPT_DIR}/validate-storage-latency-slo.sh}"

die() { echo "$*" >&2; exit 1; }

[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required"
[[ "$TIDB_OPERATOR_COMMAND" == /* && -x "$TIDB_OPERATOR_COMMAND" ]] || \
  die "TIDB_OPERATOR_COMMAND must be an executable absolute path"
[[ "$INSTANCE_READY_COMMAND" == /* && -x "$INSTANCE_READY_COMMAND" ]] || \
  die "INSTANCE_READY_COMMAND must be an executable absolute path"
[[ "$REGION_HEALTH_COMMAND" == /* && -x "$REGION_HEALTH_COMMAND" ]] || \
  die "REGION_HEALTH_COMMAND must be an executable absolute path"
[[ "$STORAGE_LATENCY_COMMAND" == /* && -x "$STORAGE_LATENCY_COMMAND" ]] || \
  die "STORAGE_LATENCY_COMMAND must be an executable absolute path"

"$TIDB_OPERATOR_COMMAND"
"$INSTANCE_READY_COMMAND"
"$REGION_HEALTH_COMMAND"
"$STORAGE_LATENCY_COMMAND"

echo "KubeBrain production release gate passed: TiDB Operator identity, instance readiness, PD/TiKV Region/storage health, and storage latency SLO are verified"
