#!/usr/bin/env bash
# Compatibility entrypoint: never permits switching to shared storage.
set -euo pipefail
[[ ${CONTROLPLANE_BACKEND:-reference} == reference ]] || { echo 'reference entrypoint refuses shared backend' >&2; exit 2; }
export CONTROLPLANE_BACKEND=reference
exec bash "$(dirname "${BASH_SOURCE[0]}")/controlplane-smoke.sh" "$@"
