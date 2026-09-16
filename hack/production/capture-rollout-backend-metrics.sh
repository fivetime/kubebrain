#!/usr/bin/env bash
# Independent observer callback. Phase classification belongs to the observer;
# the frozen TiKV receipt, not application readiness, authorizes this capture.
set -euo pipefail
umask 077
[[ $# == 2 && "$1" == /* && -d "$1" && ! -L "$1" &&
   "$2" == /* && -f "$2" && ! -L "$2" ]] || exit 2
[[ "${DIAGNOSTIC_TIKV_RECEIPT:-}" == /* && -f "$DIAGNOSTIC_TIKV_RECEIPT" &&
   ! -L "$DIAGNOSTIC_TIKV_RECEIPT" ]] || exit 2
# exec retains the observer's cancellation group. No application metrics,
# probe readiness, deployment or automatic refresh of runtime identities.
exec bash "$(dirname "$0")/capture-rollout-tikv-metrics.sh" "$1" "$DIAGNOSTIC_TIKV_RECEIPT"
