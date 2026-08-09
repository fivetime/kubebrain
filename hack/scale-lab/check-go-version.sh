#!/usr/bin/env bash
set -euo pipefail

required="${SCALE_LAB_MIN_GO_VERSION:-go1.26.5}"
actual="${SCALE_LAB_GO_VERSION_OVERRIDE:-$(go env GOVERSION)}"

version_pattern='^go[0-9]+\.[0-9]+\.[0-9]+$'
if [[ ! "$required" =~ $version_pattern || ! "$actual" =~ $version_pattern ]]; then
  echo "scale-lab requires stable Go version strings; required=$required actual=$actual" >&2
  exit 1
fi
if [[ "$(printf '%s\n' "$required" "$actual" | sort -V | head -n1)" != "$required" ]]; then
  echo "scale-lab requires Go $required or newer; found $actual" >&2
  exit 1
fi
