#!/usr/bin/env bash
set -euo pipefail
entry_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
root_dir="$(cd "$entry_dir/../.." && pwd -P)"
export GOFLAGS='' GOWORK=off GOTOOLCHAIN=go1.26.8
fixture="$(mktemp -d /tmp/backend-onepc-entry.XXXXXXXXXX)"
trap 'rm -rf -- "$fixture"' EXIT
mkdir -p "$fixture/hack/backend-integration"
cp "$entry_dir/run-onepc.sh" "$entry_dir/go.mod" "$fixture/hack/backend-integration/"
cp "$root_dir/go.mod" "$fixture/go.mod"
entry="$fixture/hack/backend-integration/run-onepc.sh"
expect_rejection() {
  local expected=$1 actual
  shift
  set +e
  bash "$entry" "$@" > "$fixture/output" 2>&1
  actual=$?
  set -e
  [[ "$actual" == "$expected" ]] || {
    echo "entry contract expected exit $expected, got $actual" >&2
    cat "$fixture/output" >&2
    exit 1
  }
}
for count in 0 01 101 -1 bad; do
  expect_rejection 2 --count "$count"
done
expect_rejection 2 --count
expect_rejection 2 --endpoint localhost:2379
expect_rejection 2 --with-tikv=true
expect_rejection 2 -args
bash "$entry" --help >/dev/null 2>&1

# Go only edits private copies; no download or mock dependency preparation is
# needed for malformed/mismatched pins to be rejected.
go mod edit -modfile="$fixture/hack/backend-integration/go.mod" \
  -replace=github.com/tikv/client-go/v2=github.com/fivetime/tikv-client-go/v2@v2.0.0
expect_rejection 1 --count 1
grep -q 'test client replacement must match' "$fixture/output"
go mod edit -modfile="$fixture/go.mod" \
  -replace=github.com/tikv/client-go/v2=/tmp/not-a-client
go mod edit -modfile="$fixture/hack/backend-integration/go.mod" \
  -replace=github.com/tikv/client-go/v2=/tmp/not-a-client
# Identically invalid local replacements must not compare as two empty strings.
expect_rejection 1 --count 1
grep -q 'product client must have exactly one pinned remote replacement' "$fixture/output"
echo BACKEND_ONEPC_ENTRY_PASSED
