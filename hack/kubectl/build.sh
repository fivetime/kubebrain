#!/usr/bin/env bash
set -euo pipefail

[[ $# == 1 && -n "$1" ]] || { echo 'usage: build.sh OUTPUT' >&2; exit 2; }
[[ "${KUBEBRAIN_GIT_SHA:-}" =~ ^[0-9a-f]{40}$ ]] || { echo 'KUBEBRAIN_GIT_SHA must be a full commit SHA' >&2; exit 1; }
[[ "${KUBEBRAIN_BUILD_DATE:-}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]] || { echo 'KUBEBRAIN_BUILD_DATE must be UTC RFC3339' >&2; exit 1; }
target_arch="${TARGETARCH:-$(go env GOARCH)}"
case "$target_arch" in
  amd64|arm64) ;;
  *) echo "unsupported TARGETARCH=$target_arch" >&2; exit 1 ;;
esac

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
output="$(realpath -m -- "$1")"
flags=''
for package in k8s.io/component-base/version k8s.io/client-go/pkg/version; do
  flags+=" -X $package.gitMajor=1 -X $package.gitMinor=36"
  flags+=" -X $package.gitVersion=v1.36.4+kubebrain"
  flags+=" -X $package.gitCommit=$KUBEBRAIN_GIT_SHA"
  flags+=" -X $package.gitTreeState=dirty -X $package.buildDate=$KUBEBRAIN_BUILD_DATE"
done
# "dirty" and the +kubebrain suffix identify a downstream dependency rebuild,
# not a byte-identical official Kubernetes release. gitCommit is our source SHA.
cd "$module_dir"
CGO_ENABLED=0 GOOS=linux GOARCH="$target_arch" go build -mod=readonly -buildvcs=false -trimpath -ldflags "$flags" -o "$output" .
