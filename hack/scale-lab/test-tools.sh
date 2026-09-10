#!/usr/bin/env bash
# Local-only regression entrypoint, including the existing nested Go modules.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
"$HERE/check-go-version.sh"
(cd "$REPO" && go test -race ./hack/scale-lab/probes/... -count=1 && go vet ./hack/scale-lab/probes/...)
for module in loadgen bigstream; do
  (cd "$HERE/$module" && go test -race ./... -count=1 && go vet ./...)
done
