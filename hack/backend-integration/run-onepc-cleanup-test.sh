#!/usr/bin/env bash
set -euo pipefail
[[ "$EUID" != 0 ]] || { echo 'cleanup regression must run as a non-root user' >&2; exit 1; }
entry="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)/run-onepc.sh"
# Exercise the exact production-entry cleanup function, without preparing Go
# dependencies or launching integration tests. Only our own mktemp paths exist.
cleanup_source="$(sed -n '/^cleanup() {$/,/^}$/p' "$entry")"
[[ -n "$cleanup_source" ]]
for expected in 0 23; do
  scratch="$(mktemp -d /tmp/tikv-onepc.XXXXXXXXXX)"
  outside="$(mktemp -d /tmp/onepc-cleanup-external.XXXXXXXXXX)"
  printf 'external fixture\n' > "$outside/sentinel"
  chmod 555 "$outside"
  ln -s "$outside" "$scratch/external"
  mkdir -p "$scratch/tidb/readonly/nested"
  printf 'fixture\n' > "$scratch/tidb/readonly/nested/file"
  chmod 555 "$scratch/tidb" "$scratch/tidb/readonly" "$scratch/tidb/readonly/nested"
  set +e
  (
    eval "$cleanup_source"
    trap cleanup EXIT
    exit "$expected"
  )
  actual=$?
  set -e
  external_ok=0
  if [[ -f "$outside/sentinel" && "$(stat -c '%a' "$outside")" == 555 ]]; then
    external_ok=1
  fi
  chmod u+w "$outside"
  rm -f -- "$outside/sentinel"
  rmdir "$outside"
  left=0
  if [[ -e "$scratch" ]]; then
    left=1
    # Recover only this test's fixture after an expected RED implementation.
    chmod u+w "$scratch/tidb" "$scratch/tidb/readonly" "$scratch/tidb/readonly/nested"
    rm -rf -- "$scratch"
  fi
  [[ "$actual" == "$expected" && "$left" == 0 && "$external_ok" == 1 ]] || {
    echo "cleanup contract failed: expected=$expected actual=$actual leftover=$left external_ok=$external_ok" >&2
    exit 1
  }
done
echo ONEPC_NONROOT_CLEANUP_PASSED
