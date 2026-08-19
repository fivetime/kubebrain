#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: hack/backup/remediate-legacy-snapshot-history.sh

Runs the native KubeBrain legacy snapshot history remediation command. The
default ACTION=diagnose is read-only. ACTION=compact irreversibly removes only
the MVCC history through the minimum safe revision reported by Snapshot, then
creates and validates a snapshot.

Environment:
  ACTION              diagnose (default) or compact
  ENDPOINT            exactly one etcd endpoint; required
  OUTPUT              snapshot output path; required for ACTION=compact
  ETCDCTL_CACERT       optional CA certificate
  ETCDCTL_CERT         optional client certificate
  ETCDCTL_KEY          optional client private key
  ETCDCTL_USER         optional user[:password]
  TIMEOUT              optional positive Go duration, default 30m

ACTION=compact also requires all of:
  ALLOW_IRREVERSIBLE_LEGACY_HISTORY_COMPACTION=true
  CONFIRM_ENDPOINT     exact byte-for-byte match with ENDPOINT
  EXPECTED_CLUSTER_ID  cluster ID printed by a prior diagnose run
  EXPECTED_REVISION    current revision printed by a prior diagnose run

KUBEBRAIN_LEGACY_REMEDIATION_COMMAND may name an alternate executable for
testing or source-tree development. Otherwise the wrapper uses the installed
kubebrain-legacy-snapshot-remediation binary, falling back to a temporary native
build from a source checkout. All validation and mutation are performed by that
command; this wrapper contains no independent etcdctl remediation path.
EOF
}

case "${1:-}" in
  -h|--help) usage; exit 0 ;;
  "") ;;
  *) usage >&2; exit 2 ;;
esac

command_override="${KUBEBRAIN_LEGACY_REMEDIATION_COMMAND:-}"
if [[ -n "$command_override" ]]; then
  if [[ "$command_override" == */* ]]; then
    [[ -x "$command_override" ]] || {
      echo "native legacy remediation command is not executable: $command_override" >&2
      exit 1
    }
  else
    command_override="$(command -v "$command_override" || true)"
    [[ -n "$command_override" ]] || {
      echo "native legacy remediation command was not found" >&2
      exit 1
    }
  fi
  exec "$command_override"
fi

if native_command="$(command -v kubebrain-legacy-snapshot-remediation 2>/dev/null)"; then
  exec "$native_command"
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd -- "$script_dir/../.." && pwd -P)"
[[ -f "$repo_root/go.mod" ]] || {
  echo "native legacy remediation command is not installed and source checkout was not found" >&2
  exit 1
}
command -v go >/dev/null 2>&1 || {
  echo "native legacy remediation command is not installed and go is unavailable" >&2
  exit 1
}
cd -- "$repo_root"
build_dir="$(mktemp -d "${TMPDIR:-/tmp}/kubebrain-legacy-remediation.XXXXXX")"
native_command="$build_dir/kubebrain-legacy-snapshot-remediation"
cleanup_build() {
  rm -f -- "$native_command"
  rmdir -- "$build_dir"
}
trap cleanup_build EXIT
go build -trimpath -o "$native_command" ./hack/backup/cmd/legacy-snapshot-remediation
"$native_command"
