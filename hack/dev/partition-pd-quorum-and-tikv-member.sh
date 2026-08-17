#!/usr/bin/env bash
# Run the existing PD-quorum and selected TiKV partitioner in one fault window.
# The default preserves the single-Store profiles; quorum mode isolates two
# Stores. Each child owns uniquely tagged iptables rules and removes them from
# its EXIT/TERM trap; this wrapper makes child failure cancel the other child.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
FAULT_RUNNER="${FAULT_RUNNER:-$ROOT_DIR/hack/dev/backend-quorum-fault-smoke.sh}"
COMBINED_FAULT_HOLD_SECONDS="${COMBINED_FAULT_HOLD_SECONDS:-20}"
COMBINED_FAULT_START_GAP_SECONDS="${COMBINED_FAULT_START_GAP_SECONDS:-1}"
TIKV_COMBINED_FAULT_MODE="${TIKV_COMBINED_FAULT_MODE:-member}"
COMBINED_FAULT_READY_FILE="${COMBINED_FAULT_READY_FILE:-}"
COMBINED_FAULT_READY_TIMEOUT_SECONDS="${COMBINED_FAULT_READY_TIMEOUT_SECONDS:-180}"
pd_pid=""
tikv_pid=""
combined_fault_dir=""

die() { echo "$*" >&2; exit 1; }

[[ -x "$FAULT_RUNNER" ]] || die "FAULT_RUNNER must be executable: $FAULT_RUNNER"
[[ "$COMBINED_FAULT_HOLD_SECONDS" =~ ^[1-9][0-9]*$ ]] || \
  die "COMBINED_FAULT_HOLD_SECONDS must be a positive integer"
[[ "$COMBINED_FAULT_START_GAP_SECONDS" =~ ^[0-9]+$ ]] || \
  die "COMBINED_FAULT_START_GAP_SECONDS must be a non-negative integer"
(( COMBINED_FAULT_HOLD_SECONDS <= 300 && COMBINED_FAULT_START_GAP_SECONDS <= 30 )) || \
  die "combined fault limits exceeded"
[[ "$COMBINED_FAULT_READY_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] && \
  (( COMBINED_FAULT_READY_TIMEOUT_SECONDS <= 300 )) || \
  die "COMBINED_FAULT_READY_TIMEOUT_SECONDS must be in [1,300]"
[[ -z "$COMBINED_FAULT_READY_FILE" || "$COMBINED_FAULT_READY_FILE" == /* ]] || \
  die "COMBINED_FAULT_READY_FILE must be absolute"
case "$TIKV_COMBINED_FAULT_MODE" in
  member) tikv_fault_argument="--partition-tikv-member" ;;
  quorum) tikv_fault_argument="--partition-tikv-quorum" ;;
  *) die "TIKV_COMBINED_FAULT_MODE must be member or quorum" ;;
esac
combined_fault_dir="$(mktemp -d)"
pd_ready="$combined_fault_dir/pd-ready"
pd_release="$combined_fault_dir/pd-release"
tikv_ready="$combined_fault_dir/tikv-ready"
tikv_release="$combined_fault_dir/tikv-release"

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
  rm -r -- "$combined_fault_dir"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

FAULT_READY_FILE="$pd_ready" FAULT_RELEASE_FILE="$pd_release" \
  PD_QUORUM_PARTITION_HOLD_SECONDS="$COMBINED_FAULT_HOLD_SECONDS" \
  "$FAULT_RUNNER" --partition-pd-quorum &
pd_pid=$!
sleep "$COMBINED_FAULT_START_GAP_SECONDS"
if ! kill -0 "$pd_pid" 2>/dev/null; then
  wait "$pd_pid"
fi

FAULT_READY_FILE="$tikv_ready" FAULT_RELEASE_FILE="$tikv_release" \
  PARTITION_HOLD_SECONDS="$COMBINED_FAULT_HOLD_SECONDS" \
  "$FAULT_RUNNER" "$tikv_fault_argument" &
tikv_pid=$!

ready_deadline=$((SECONDS + COMBINED_FAULT_READY_TIMEOUT_SECONDS))
while [[ ! -e "$pd_ready" || ! -e "$tikv_ready" ]]; do
  if ! kill -0 "$pd_pid" 2>/dev/null; then
    wait "$pd_pid"
    die "PD quorum child exited before signaling fault readiness"
  fi
  if ! kill -0 "$tikv_pid" 2>/dev/null; then
    wait "$tikv_pid"
    die "TiKV $TIKV_COMBINED_FAULT_MODE child exited before signaling fault readiness"
  fi
  (( SECONDS < ready_deadline )) || die "combined backend fault did not become ready in time"
  sleep 0.1
done
if [[ -n "$COMBINED_FAULT_READY_FILE" ]]; then
  : >"$COMBINED_FAULT_READY_FILE"
fi
echo "combined PD quorum and TiKV $TIKV_COMBINED_FAULT_MODE partition ready"
sleep "$COMBINED_FAULT_HOLD_SECONDS"
: >"$pd_release"
: >"$tikv_release"

completed_pid=""
set +e
wait -n -p completed_pid "$pd_pid" "$tikv_pid"
first_status=$?
set -e
if (( first_status != 0 )); then
  child="unknown"
  [[ "$completed_pid" == "$pd_pid" ]] && child="pd-quorum"
  [[ "$completed_pid" == "$tikv_pid" ]] && child="tikv-$TIKV_COMBINED_FAULT_MODE"
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
rm -r -- "$combined_fault_dir"
echo "combined PD quorum and TiKV $TIKV_COMBINED_FAULT_MODE partition recovered"
