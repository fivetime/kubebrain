#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
REFERENCE_ETCD_SOURCE_DIR="${REFERENCE_ETCD_SOURCE_DIR:-/root/etcd}"
REFERENCE_ETCD_BUILD_DIR="${REFERENCE_ETCD_BUILD_DIR:-}"

if [[ ! -x "$REFERENCE_ETCD_SOURCE_DIR/scripts/build.sh" ]]; then
  echo "reference etcd source has no executable scripts/build.sh: $REFERENCE_ETCD_SOURCE_DIR" >&2
  exit 1
fi
if [[ -n "$(git -C "$REFERENCE_ETCD_SOURCE_DIR" status --porcelain --untracked-files=normal)" ]]; then
  echo "reference etcd source worktree is not clean: $REFERENCE_ETCD_SOURCE_DIR" >&2
  exit 1
fi
expected_sha="$(git -C "$REFERENCE_ETCD_SOURCE_DIR" rev-parse HEAD)"

if [[ -z "$REFERENCE_ETCD_BUILD_DIR" ]]; then
  REFERENCE_ETCD_BUILD_DIR="$(mktemp -d "${TMPDIR:-/tmp}/kubebrain-reference-etcd-${expected_sha:0:12}.XXXXXX")"
else
  mkdir -p "$REFERENCE_ETCD_BUILD_DIR"
fi
REFERENCE_ETCD_BUILD_DIR="$(cd "$REFERENCE_ETCD_BUILD_DIR" && pwd -P)"
if [[ -n "$(find "$REFERENCE_ETCD_BUILD_DIR" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "reference etcd build directory must be empty: $REFERENCE_ETCD_BUILD_DIR" >&2
  exit 1
fi

relative_build_dir="$(realpath --relative-to="$REFERENCE_ETCD_SOURCE_DIR" "$REFERENCE_ETCD_BUILD_DIR")"
(
  cd "$REFERENCE_ETCD_SOURCE_DIR"
  BINDIR="$relative_build_dir" GOWORK=off GO_BUILD_FLAGS=-mod=readonly ./scripts/build.sh
)

for binary_name in etcd etcdctl etcdutl; do
  binary="$REFERENCE_ETCD_BUILD_DIR/$binary_name"
  REFERENCE_ETCD_BIN="$binary" \
    REFERENCE_ETCD_SOURCE_DIR="$REFERENCE_ETCD_SOURCE_DIR" \
    REFERENCE_ETCD_EXPECTED_GIT_SHA="$expected_sha" \
    "$ROOT_DIR/hack/etcd-client-compat/verify-reference-etcd-provenance.sh" >/dev/null
done

printf 'reference etcd toolchain built and verified: %s\n' "$expected_sha"
printf 'REFERENCE_ETCD_BIN=%q\n' "$REFERENCE_ETCD_BUILD_DIR/etcd"
printf 'REFERENCE_ETCD_BINARY=%q\n' "$REFERENCE_ETCD_BUILD_DIR/etcd"
printf 'ETCDCTL_BIN=%q\n' "$REFERENCE_ETCD_BUILD_DIR/etcdctl"
printf 'ETCDUTL_BINARY=%q\n' "$REFERENCE_ETCD_BUILD_DIR/etcdutl"
