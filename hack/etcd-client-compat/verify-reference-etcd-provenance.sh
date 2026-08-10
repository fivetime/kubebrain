#!/usr/bin/env bash
set -euo pipefail

REFERENCE_ETCD_BIN="${REFERENCE_ETCD_BIN:-/root/etcd/bin/etcd}"
REFERENCE_ETCD_SOURCE_DIR="${REFERENCE_ETCD_SOURCE_DIR:-/root/etcd}"
expected_sha="${REFERENCE_ETCD_EXPECTED_GIT_SHA:-}"

if [[ ! -x "$REFERENCE_ETCD_BIN" ]]; then
  echo "reference etcd binary is not executable: $REFERENCE_ETCD_BIN" >&2
  exit 1
fi

if [[ -z "$expected_sha" ]]; then
  if ! expected_sha="$(git -C "$REFERENCE_ETCD_SOURCE_DIR" rev-parse HEAD 2>/dev/null)"; then
    echo "cannot derive reference etcd Git SHA from source directory: $REFERENCE_ETCD_SOURCE_DIR" >&2
    echo "set REFERENCE_ETCD_EXPECTED_GIT_SHA explicitly for an external reference build" >&2
    exit 1
  fi
fi

actual_sha=""
build_modified=""
build_info="$(go version -m "$REFERENCE_ETCD_BIN" 2>/dev/null || true)"
while IFS= read -r line; do
  case "$line" in
    *$'build\tvcs.revision='*)
      actual_sha="${line##*vcs.revision=}"
      ;;
    *$'build\tvcs.modified='*)
      build_modified="${line##*vcs.modified=}"
      ;;
  esac
done <<<"$build_info"

if [[ -z "$actual_sha" ]]; then
  version_output="$("$REFERENCE_ETCD_BIN" --version 2>&1)" || {
    echo "reference etcd --version failed: $REFERENCE_ETCD_BIN" >&2
    exit 1
  }
  while IFS= read -r line; do
    case "$line" in
      "Git SHA: "*)
        actual_sha="${line#Git SHA: }"
        break
        ;;
    esac
  done <<<"$version_output"
fi

expected_sha="${expected_sha,,}"
actual_sha="${actual_sha,,}"
if [[ ! "$expected_sha" =~ ^[0-9a-f]{7,40}$ ]]; then
  echo "expected reference etcd Git SHA is invalid: ${expected_sha:-<empty>}" >&2
  exit 1
fi
if [[ ! "$actual_sha" =~ ^[0-9a-f]{7,40}$ ]]; then
  echo "reference etcd binary reports an invalid Git SHA: ${actual_sha:-<empty>}" >&2
  exit 1
fi
if [[ "$build_modified" == true ]]; then
  echo "reference etcd binary was built from a modified worktree: $REFERENCE_ETCD_BIN" >&2
  exit 1
fi
if [[ "$expected_sha" != "$actual_sha"* && "$actual_sha" != "$expected_sha"* ]]; then
  echo "reference etcd Git SHA mismatch: binary=$actual_sha expected=$expected_sha" >&2
  echo "rebuild REFERENCE_ETCD_BIN from the intended source revision before running differential tests" >&2
  exit 1
fi

echo "reference etcd provenance verified: $actual_sha"
