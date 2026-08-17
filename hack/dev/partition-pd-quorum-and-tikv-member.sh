#!/usr/bin/env bash
# Run the existing PD-quorum and single-TiKV-store partitioners in one fault
# window. Each child owns uniquely tagged iptables rules and removes them from
# its EXIT/TERM trap; this wrapper makes child failure cancel the other child.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
FAULT_RUNNER="${FAULT_RUNNER:-$ROOT_DIR/hack/dev/backend-quorum-fault-smoke.sh}"
COMBINED_FAULT_HOLD_SECONDS="${COMBINED_FAULT_HOLD_SECONDS:-20}"
COMBINED_FAULT_START_GAP_SECONDS="${COMBINED_FAULT_START_GAP_SECONDS:-1}"
pd_pid=""
tikv_pid=""

die() { echo "$*" >&2; exit 1; }

[[ -x "$FAULT_RUNNER" ]] || die "FAULT_RUNNER must be executable: $FAULT_RUNNER"
[[ "$COMBINED_FAULT_HOLD_SECONDS" =~ ^[1-9][0-9]*$ ]] || \
  die "COMBINED_FAULT_HOLD_SECONDS must be a positive integer"
[[ "$COMBINED_FAULT_START_GAP_SECONDS" =~ ^[0-9]+$ ]] || \
  die "COMBINED_FAULT_START_GAP_SECONDS must be a non-negative integer"
(( COMBINED_FAULT_HOLD_SECONDS <= 300 && COMBINED_FAULT_START_GAP_SECONDS <= 30 )) || \
  die "combined fault limits exceeded"

cleanup() {
  local status=$? pid
  trap - EXIT INT TERM
  for pid in "$pd_pid" "$tikv_pid"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
    fi
  done
  for pid in "$pd_pid" "$tikv_pid"; do
    [[ -z "$pid" ]] || wait "$pid" 2>/dev/null || true
  done
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

PD_QUORUM_PARTITION_HOLD_SECONDS="$COMBINED_FAULT_HOLD_SECONDS" \
  "$FAULT_RUNNER" --partition-pd-quorum &
pd_pid=$!
sleep "$COMBINED_FAULT_START_GAP_SECONDS"
if ! kill -0 "$pd_pid" 2>/dev/null; then
  wait "$pd_pid"
fi

PARTITION_HOLD_SECONDS="$COMBINED_FAULT_HOLD_SECONDS" \
  "$FAULT_RUNNER" --partition-tikv-member &
tikv_pid=$!

completed_pid=""
set +e
wait -n -p completed_pid "$pd_pid" "$tikv_pid"
first_status=$?
set -e
if (( first_status != 0 )); then
  child="unknown"
  [[ "$completed_pid" == "$pd_pid" ]] && child="pd-quorum"
  [[ "$completed_pid" == "$tikv_pid" ]] && child="tikv-member"
  echo "combined backend partition child failed: child=$child pid=$completed_pid status=$first_status" >&2
  exit "$first_status"
fi
if [[ "$completed_pid" == "$pd_pid" ]]; then
  pd_pid=""
  wait "$tikv_pid"
  tikv_pid=""
else
  tikv_pid=""
  wait "$pd_pid"
  pd_pid=""
fi
trap - EXIT INT TERM
echo "combined PD quorum and TiKV member partition recovered"
