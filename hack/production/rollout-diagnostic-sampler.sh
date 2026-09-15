#!/usr/bin/env bash
# Private worker of the rollout runner. The callback is a trusted, read-only
# sampler; it receives a fresh output directory and the captured phase receipt.
set -euo pipefail
umask 077
[[ $# == 2 ]] || exit 2
evidence=$1
sampler=$2
[[ "$evidence" == /* && -d "$evidence" && "$sampler" == /* && -f "$sampler" && -x "$sampler" ]] || exit 2
phase="$evidence/diagnostic-phase.json"
callback_pid=""
stop_callback() {
  [[ -n "$callback_pid" ]] || return 0
  # Nested timeout owns a separate process group; the outer runner's TERM
  # cannot reach it. This worker must terminate and reap its direct child.
  kill -TERM "$callback_pid" 2>/dev/null || true
  wait "$callback_pid" 2>/dev/null || true
  callback_pid=""
}
trap stop_callback EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
for ((sample=0; sample<8; sample++)); do
  directory=$(mktemp -d "$evidence/diagnostic-sample.XXXXXXXX")
  cp -- "$phase" "$directory/phase-before.json" || exit 0
  jq -e '.format=="kubebrain.rollout-diagnostic-phase.v1" and .phase=="stable" and
    all(.probe_uid,.statefulset_uid,.image; type=="string" and length>0)' "$directory/phase-before.json" >/dev/null || exit 0
  result=0
  # Leave time for the inner group to die before the outer 2s kill-after.
  timeout --signal=TERM --kill-after=1s 60s "$sampler" "$directory" "$directory/phase-before.json" > "$directory/sampler.log" 2>&1 &
  callback_pid=$!
  wait "$callback_pid" || result=$?
  callback_pid=""
  printf '%s\n' "$result" > "$directory/sampler.exit"
  cp -- "$phase" "$directory/phase-after.json" || exit 0
  cmp -s "$directory/phase-before.json" "$directory/phase-after.json" || exit 0
  if [[ "$result" == 0 ]]; then
    # This means only that the callback succeeded without a phase change.
    # Actual resource identity/metric validation is the callback's obligation.
    printf 'phase-stable\n' > "$directory/capture-complete"
  fi
  sleep 120
done
