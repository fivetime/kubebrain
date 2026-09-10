#!/usr/bin/env bash
# Local build only: never load lab.env, SSH, or modify a cluster.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
OUTPUT="${SCALE_LAB_BIN_DIR:-$HERE/bin}"
[[ "$OUTPUT" = /* ]] || { echo 'SCALE_LAB_BIN_DIR must be absolute' >&2; exit 1; }
"$HERE/check-go-version.sh"
mkdir -p "$OUTPUT"
for module in loadgen bigstream; do
  (cd "$HERE/$module" && go build -trimpath -o "$OUTPUT/$module" .)
done
for probe in bulk foload elogprobe qlat slowwatch watchflood; do
  (cd "$REPO" && go build -trimpath -o "$OUTPUT/$probe" "./hack/scale-lab/probes/$probe")
done
