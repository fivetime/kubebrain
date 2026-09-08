# KubeBrain's kubectl dependency rebuild

This is the complete upstream kubectl command tree, imported from Kubernetes
v1.36.4 staging modules. The small main entry point follows
[upstream cmd/kubectl/kubectl.go](https://github.com/kubernetes/kubernetes/blob/v1.36.4/cmd/kubectl/kubectl.go),
including early verbosity handling, auth plugins and upstream error handling.
There is no second embedded Kubernetes source tree and no reduced command set.

The official v1.36.2 and v1.36.4 Linux binaries contained vulnerable symbols in
the 2026-09-08 binary scans (18 and 9 findings respectively). The v1.37.0 candidate
passed its amd64 scan, but would drop the supported Kubernetes 1.35 server from
kubectl's +/-1 minor window. This module retains v1.36 and pins a separately
reviewed dependency graph and Go 1.26.8 instead.

Build from the repository root, supplying the product source revision and build
time, and choosing an explicit output path outside the source tree:

```sh
KUBEBRAIN_GIT_SHA=<40-character-product-commit> \
KUBEBRAIN_BUILD_DATE=<UTC-RFC3339-time> TARGETARCH=amd64 \
  bash hack/kubectl/build.sh /path/to/artifacts/kubectl
```

Use `arm64` for the other published target; unknown architectures fail closed.
`-mod=readonly`, go.sum verification and the module's pinned toolchain prevent
an implicit dependency update. `-trimpath -buildvcs=false` allow the CI scan
build and Docker build to be byte-identical with the same explicit metadata.

The binary reports `v1.36.4+kubebrain`, `gitTreeState=dirty`, the **KubeBrain**
source commit and its build date. These values deliberately identify a downstream
dependency rebuild; they are not an official Kubernetes release signature or
the upstream Kubernetes commit. The imported modules retain their real upstream
versions in Go build information.

CI scans this module and both compiled architectures before image publication.
Before promoting `:dbaas`, it extracts kubectl from both image platforms and
compares the shipped bytes with the scanned binaries. Native arm64 runtime and
the refreshed Kubernetes 1.35/1.36 integration matrix remain separate gates.

Local checks: `go vet ./...`, `go test ./...`, `go test -race ./...`,
`go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...` and
`go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...`.
Also scan each output with govulncheck `-mode=binary`; source scans do not replace
inspection of the bytes actually shipped.
