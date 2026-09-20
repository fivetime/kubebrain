#!/usr/bin/env bash
# Build portable test tools only. No cluster access, credentials or execution.
set -euo pipefail
umask 077
[[ $# == 2 ]] || { echo 'usage: build-lease-fault-tools.sh EXISTING_EMPTY_PRIVATE_DIRECTORY amd64|arm64' >&2; exit 2; }
bundle=$1
arch=$2
[[ $arch == amd64 || $arch == arm64 ]] || exit 2
[[ $bundle == /* && $bundle != / && $bundle != *$'\n'* && $bundle != *$'\t'* &&
   -d $bundle && ! -L $bundle && $(realpath -e -- "$bundle") == "$bundle" &&
   $(stat -c '%a:%u' -- "$bundle") == "700:$EUID" ]] || exit 2
[[ -z $(find "$bundle" -mindepth 1 -maxdepth 1 -print -quit) ]] || exit 2
repository=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
[[ $bundle != "$repository" && $bundle != "$repository/"* ]] || exit 2
cd -- "$repository"
mkdir "$bundle/bin" "$bundle/deploy" "$bundle/hack"
mkdir "$bundle/deploy/test-cluster" "$bundle/hack/production"
# Explicit package names prevent unrelated operational commands entering this
# bundle. Build failure preserves partial output; never delete or reuse it.
for tool in lease-term-probe lease-fault-plan lease-fault-recover lease-fault-response info-diagnostic-probe retirement-metrics-delta; do
 CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -trimpath -o "$bundle/bin/$tool" "./hack/production/cmd/$tool"
done
for name in capture-local-cilium-drops.sh capture-local-cilium-endpoint.sh observe-local-backend-drops.sh observe-local-backend-tcp.sh observe-local-fault-label.sh observe-local-network-restored.sh observe-local-nonces.sh observe-local-policy-state.sh local-label-identity-transition.jq local-policy-observation.jq local-nonce-endpoints.jq; do
 cp -- "deploy/test-cluster/$name" "$bundle/deploy/test-cluster/$name"
done
for name in protected-wait-worker.sh protected-metrics-worker.sh protected-stack-session.sh join-isolated-fault-workers.sh same-pod-process.jq expired-lease-wait-frames.jq; do
 cp -- "hack/production/$name" "$bundle/hack/production/$name"
 # NativeCommandPlan currently shares one directory for stack/metrics/network
 # scripts. Generated copies satisfy that contract; preserve the original
 # relative library path used by the network observers as well.
 cp -- "hack/production/$name" "$bundle/deploy/test-cluster/$name"
done
chmod 0700 "$bundle/bin/"* "$bundle/hack/production/"*.sh "$bundle/deploy/test-cluster/"*.sh
chmod 0600 "$bundle/hack/production/"*.jq "$bundle/deploy/test-cluster/"*.jq
cd -- "$bundle"
find bin deploy hack -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum > SHA256SUMS
sha256sum -c SHA256SUMS >/dev/null
echo 'LEASE_FAULT_TOOLS_BUILT_NOT_RUNTIME_OR_EXPERIMENT_ADMISSION'
