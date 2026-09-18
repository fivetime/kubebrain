#!/usr/bin/env bash
# Opt-in real Docker lifecycle test. No external endpoints or existing names.
set -euo pipefail
[[ $# == 1 && "$1" == --allow-local-containers ]] || {
  echo 'Usage: bash hack/backend-integration/run-real-local-interruption-test.sh --allow-local-containers' >&2
  exit 2
}
entry_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
work="$(mktemp -d /tmp/kubebrain-protocol-interruption.XXXXXXXXXX)"
chmod 700 "$work"
mkdir "$work/bin"
cp "$entry_dir/testdata/interrupt-docker.sh" "$work/bin/docker"
chmod 700 "$work/bin/docker"
cleanup_wrapper() {
  local result=$?
  trap - EXIT
  rm -- "$work/bin/docker" || result=1
  rmdir "$work/bin" || result=1
  printf 'INTERRUPTION_EVIDENCE directory=%s result=%s\n' "$work" "$result"
  exit "$result"
}
trap cleanup_wrapper EXIT
for phase in pd tikv; do
  result=0
  PATH="$work/bin:$PATH" INTERRUPT_PHASE="$phase" \
    timeout --signal=TERM --kill-after=120s 420s bash -c \
    'export INTERRUPT_TARGET_PID=$BASHPID; exec bash "$1" --allow-local-containers' \
    _ "$entry_dir/run-real-local.sh" > "$work/$phase.log" 2>&1 || result=$?
  [[ "$result" == 143 ]] || { echo "unexpected interrupted result: phase=$phase exit=$result" >&2; exit 1; }
  grep -Fx "INTERRUPT_AFTER_CREATE phase=$phase" "$work/$phase.log" >/dev/null
  grep -E '^LOCAL_PROTOCOL_END result=143 cleanup_failed=0 evidence=' "$work/$phase.log" >/dev/null
  if grep -q '^LOCAL_PROTOCOL_TESTS_PASSED' "$work/$phase.log"; then
    echo 'interruption happened too late' >&2; exit 1
  fi
  owner="$(sed -nE 's/^LOCAL_PROTOCOL_START evidence=[^ ]+ owner=([a-f0-9]{32})$/\1/p' "$work/$phase.log")"
  evidence="$(sed -nE 's/^LOCAL_PROTOCOL_START evidence=(\/tmp\/kubebrain-real-protocol\.[A-Za-z0-9]{10}) owner=[a-f0-9]{32}$/\1/p' "$work/$phase.log")"
  [[ "$owner" =~ ^[a-f0-9]{32}$ && -n "$evidence" ]] || { echo 'missing owned fixture identity' >&2; exit 1; }
  containers="$(timeout 30s /usr/bin/docker --host unix:///var/run/docker.sock container ls -a \
    --filter "label=io.kubebrain.local-protocol-owner=$owner" --format '{{.ID}}')"
  networks="$(timeout 30s /usr/bin/docker --host unix:///var/run/docker.sock network ls \
    --filter "label=io.kubebrain.local-protocol-owner=$owner" --format '{{.ID}}')"
  [[ -z "$containers" && -z "$networks" && ! -e "$evidence/protocol.test" && ! -e "$evidence/election.test" ]] || {
    echo "interrupted fixture cleanup incomplete: $evidence" >&2; exit 1
  }
  printf 'LOCAL_PROTOCOL_INTERRUPTION_PASSED phase=%s exit=143 resources_absent=true evidence=%s\n' "$phase" "$evidence"
done
