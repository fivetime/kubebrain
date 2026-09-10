#!/usr/bin/env bash
set -euo pipefail

# This entry is mock-only: no endpoint option or arbitrary test flags.
count=10
race=()
usage() { echo 'Usage: bash hack/backend-integration/run-onepc.sh [--race] [--count 1..100]' >&2; }
while (($#)); do
  case "$1" in
    --race) race=(-race); shift ;;
    --count)
      [[ $# -ge 2 && "$2" =~ ^[1-9][0-9]?$|^100$ ]] || { usage; exit 2; }
      count="$2"; shift 2 ;;
    --help) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done

for command in go jq patch sha256sum cp chmod mktemp find rm; do
  command -v "$command" >/dev/null || { echo "missing required command: $command" >&2; exit 1; }
done
module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
root_dir="$(cd "$module_dir/../.." && pwd -P)"
export GOFLAGS='' GOWORK=off GOTOOLCHAIN=go1.26.8
cd "$module_dir"

# Go does not inherit replace directives from dependency modules. Refuse to
# test a different client than the one the product actually builds against.
client_replace() {
  go mod edit -json "$1" | jq -cer '
    [.Replace[] | select(.Old.Path == "github.com/tikv/client-go/v2")] |
    select(length == 1) | .[0] |
    select(.New.Path == "github.com/fivetime/tikv-client-go/v2" and
      (.New.Version | type == "string" and length > 0))'
}
product_client="$(client_replace "$root_dir/go.mod")" || {
  echo 'product client must have exactly one pinned remote replacement' >&2; exit 1;
}
test_client="$(client_replace "$module_dir/go.mod")" || {
  echo 'test client must have exactly one pinned remote replacement' >&2; exit 1;
}
[[ "$product_client" == "$test_client" ]] || {
  echo 'test client replacement must match the pinned product client' >&2; exit 1;
}

scratch="$(mktemp -d /tmp/tikv-onepc.XXXXXXXXXX)"
cleanup() {
  local result=$?
  trap - EXIT
  if [[ "$scratch" == /tmp/tikv-onepc.* && -d "$scratch" && ! -L "$scratch" ]]; then
    # Go's module-cache directories are read-only; cp -a preserves that mode.
    # Restore owner write permission only on our private copy's directories.
    # -P avoids following symlinks into anything outside the temporary tree.
    find -P "$scratch" -type d -exec chmod u+w {} + || {
      echo "temporary test directory permission repair failed: $scratch" >&2; exit 1;
    }
    rm -rf -- "$scratch" || { echo "temporary test directory cleanup failed: $scratch" >&2; exit 1; }
  fi
  exit "$result"
}
trap cleanup EXIT

version=v1.1.0-beta.0.20230321033041-8ba2035203f7
go mod download -json "github.com/pingcap/tidb@$version" > "$scratch/dependency.json"
dependency_dir="$(jq -er --arg version "$version" '
  select(.Path == "github.com/pingcap/tidb" and .Version == $version and
    .Sum == "h1:yDY6E5NNdpN7KmUNkXo82iGaZHWPCykI3KhY1qmdGx0=") | .Dir
  | select(type == "string" and startswith("/"))' "$scratch/dependency.json")"
go mod verify
printf '%s  %s\n' f581b7a696bf70f9f33368cb6cb184e3941fa884a3de0de6dd60523729456230 \
  "$dependency_dir/util/printer/printer.go" | sha256sum --check --status

# A test-only shadow dependency avoids editing the Go module cache or checking
# an entire TiDB tree into this fork. Exact hashes fail closed on source drift.
cp -a "$dependency_dir" "$scratch/tidb"
chmod u+w "$scratch/tidb/util/printer" "$scratch/tidb/util/printer/printer.go"
patch --batch --forward --fuzz=0 -p1 -d "$scratch/tidb" < "$module_dir/compat/tidb-printer-go126.patch"
printf '%s  %s\n' 8cb923ada951bc164661bd67649ec3da2ee3b957284a6983089609006a41e20d \
  "$scratch/tidb/util/printer/printer.go" | sha256sum --check --status
cp "$module_dir/go.mod" "$scratch/go.mod"
cp "$module_dir/go.sum" "$scratch/go.sum"
go mod edit -modfile="$scratch/go.mod" \
  -replace="github.com/kubewharf/kubebrain=$root_dir" \
  -replace="github.com/pingcap/tidb=$scratch/tidb"
go test -mod=readonly -modfile="$scratch/go.mod" "${race[@]}" . \
  -run '^TestBackendResolvesActualOnePC' "-count=$count" -timeout=180s
