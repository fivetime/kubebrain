#!/usr/bin/env bash
# Sourced by fault runners. Never mutate the target on a timeout/dead child.
wait_apiserver_watch_ready() {
  local child_pid="$1" log_path="$2" timeout_seconds="$3"
  if [[ ! "$child_pid" =~ ^[1-9][0-9]*$ || ! "$timeout_seconds" =~ ^[1-9][0-9]*$ ]]; then
    echo "invalid apiserver watch PID or readiness timeout" >&2
    return 1
  fi
  local deadline=$((SECONDS + timeout_seconds))
  while true; do
    if ! kill -0 "$child_pid" 2>/dev/null; then
      echo "apiserver watch process exited before fault admission" >&2
      return 1
    fi
    if grep -Fxq 'APISERVER_WATCH_READY' "$log_path"; then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      echo "timed out waiting for acknowledged apiserver Watch; no fault injected" >&2
      return 1
    fi
    sleep 1
  done
}
