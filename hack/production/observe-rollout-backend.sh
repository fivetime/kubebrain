#!/usr/bin/env bash
# Independent, bounded diagnostics. Never starts/stops an executor or mutates
# workload state. The trusted callback owns backend identity verification.
set -euo pipefail
umask 077
[[ $# == 3 ]] || exit 2
owner=$1
callback=$2
phase=$3
[[ "$owner" == /* && -d "$owner" && ! -L "$owner" &&
   "$callback" == /* && -f "$callback" && -x "$callback" &&
   "$phase" == /* ]] || exit 2
samples=${BACKEND_OBSERVER_MAX_SAMPLES:-180}
seconds=${BACKEND_OBSERVER_MAX_SECONDS:-2400}
interval=${BACKEND_OBSERVER_INTERVAL_SECONDS:-10}
sample_seconds=${BACKEND_OBSERVER_SAMPLE_SECONDS:-60}
for value in "$samples" "$seconds" "$interval" "$sample_seconds"; do
  [[ "$value" =~ ^[1-9][0-9]{0,3}$ ]] || exit 2
done
(( samples <= 180 && seconds <= 2400 && interval <= 30 && sample_seconds <= 60 )) || exit 2
directory="$owner/backend-observer"
mkdir "$directory" # Do not restart or overwrite a consumed observation.
sample_pid=''
finish() {
  local result=$?
  trap - EXIT INT TERM
  if [[ -n "$sample_pid" ]]; then
    # timeout owns the callback group; reap only our child, never the executor.
    kill -TERM "$sample_pid" 2>/dev/null || true
    wait "$sample_pid" 2>/dev/null || true
  fi
  printf '%s\n' "$result" > "$directory/observer.exit"
  exit "$result"
}
trap finish EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
snapshot_phase() {
  local destination=$1 raw="$1.raw"
  # Missing/invalid lifecycle evidence never prevents backend diagnostics.
  if [[ -f "$phase" && ! -L "$phase" ]] &&
    head -c 65537 -- "$phase" > "$raw" &&
    (( $(stat -c %s "$raw") <= 65536 )) &&
    jq -cseS 'select(length==1) | .[0] | select(type=="object" and
      .format=="kubebrain.rollout-diagnostic-phase.v1" and
      (.phase=="stable" or .phase=="cleanup") and
      all(.probe_uid,.statefulset_uid,.image; type=="string" and length>0))' \
      "$raw" > "$destination"; then
    return 0
  fi
  printf '{"phase":"unknown"}\n' > "$destination"
}
deadline=$((SECONDS + seconds))
for ((i=0; i<samples && SECONDS<deadline; i++)); do
  [[ ! -e "$owner/backend-observer-stop" ]] || exit 0
  sample=$(mktemp -d "$directory/sample.XXXXXXXX")
  date -u '+%Y-%m-%dT%H:%M:%S.%NZ' > "$sample/started-at"
  snapshot_phase "$sample/phase-before.json"
  limit=$sample_seconds
  remaining=$((deadline-SECONDS))
  (( remaining > 0 )) || break
  (( remaining >= limit )) || limit=$remaining
  result=0
  timeout --signal=TERM --kill-after=1s "${limit}s" "$callback" \
    "$sample" "$sample/phase-before.json" > "$sample/callback.log" 2>&1 &
  sample_pid=$!
  wait "$sample_pid" || result=$?
  sample_pid=''
  printf '%s\n' "$result" > "$sample/callback.exit"
  snapshot_phase "$sample/phase-after.json"
  date -u '+%Y-%m-%dT%H:%M:%S.%NZ' > "$sample/ended-at"
  if [[ "$result" == 0 ]] &&
    jq -e '.format=="kubebrain.rollout-diagnostic-phase.v1"' "$sample/phase-before.json" >/dev/null &&
    cmp -s "$sample/phase-before.json" "$sample/phase-after.json"; then
    printf 'phase snapshots consistent; diagnostic only, not health or acceptance\n' > "$sample/phase-consistent"
  fi
  for ((pause=0; pause<interval && SECONDS<deadline; pause++)); do
    [[ ! -e "$owner/backend-observer-stop" ]] || exit 0
    sleep 1
  done
done
[[ ! -e "$owner/backend-observer-stop" ]] || exit 0
echo 'BACKEND_OBSERVATION_BOUND_REACHED; inspect executor independently'
exit 75
