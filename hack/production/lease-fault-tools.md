# Lease fault test tool bundle

Build on a trusted checkout, into a new private directory outside the repository:

```sh
bundle=$(mktemp -d /tmp/lease-fault-tools.XXXXXXXX)
bash hack/production/build-lease-fault-tools.sh "$bundle" amd64
(cd "$bundle" && sha256sum -c SHA256SUMS)
```

`amd64` and `arm64` are supported Linux build targets. The script refuses a
nonempty, public, symlinked or repository-local destination. Failed builds leave
their partial output for inspection; do not reuse it. No credentials, kubeconfig,
owner plans or running-cluster discovery are copied into the bundle.

The bundle contains six static Go binaries in `bin`, the versioned network
observers under `deploy/test-cluster`, and the protected stack/metrics scripts
and predicates under `hack/production`. Generated copies of the latter are also
placed alongside network observers to satisfy the current command adapter's
shared script-directory contract. The source files remain authoritative; do not
edit generated copies. `SHA256SUMS` covers every generated/copied file and is an
integrity inventory, **not CI provenance or permission to execute a fault**.

This is not a self-contained runtime image. The separately admitted execution
environment still needs Bash, GNU coreutils (including timeout/date/stat), find,
grep, jq, kubectl, and gh for online release authentication, plus their runtime
dependencies. It also needs pinned private credentials, an owner-specific audited
Join identity/admission, the protected diagnostic input layout (including owner/bin), and
the complete execution CLI/online gates. The builder does not manufacture these.
The existing source-specific stack classifier must approve the exact tested
source; packaging it does not add a candidate to its allowlist.

In the dedicated cluster, use a separately reviewed execution Pod in the test
namespace. Do not relax workload isolation to enable host access and do not run
the experiment inside an existing discovery/service container. Neither a bundle
build nor its checksum verification proves the original 30-second acceptance.

## Isolated PID namespace Join (not yet wired to a complete executor)

`join-isolated-fault-workers.sh OWNER` is a read-only final check, packaged in
both script directories. It does not kill or reap processes. The future executor
must run as PID 1 in its own non-host, non-shared PID namespace, stop and wait
its managed children, reap adopted orphans, and synchronously invoke Join with
no other goroutine launching commands. Admission must exclude concurrent exec
sessions, sidecars, and masked/restricted proc mounts; the script cannot prove
those external conditions. Never use it as a host cleanup script.

Before starting workers, capture `OWNER/join-namespace.tsv` (0600, in the admitted
0700 owner directory). Its one newline-terminated, tab-separated record is:

```text
v1<TAB>PID_NAMESPACE_DEVICE:INODE<TAB>BOOT_UUID<TAB>INIT_START_TICKS<TAB>OWNER_DEVICE:INODE
```

The namespace identity comes from dereferencing `/proc/1/ns/pid`; start ticks
are field 22 of `/proc/1/stat`; boot UUID comes from
`/proc/sys/kernel/random/boot_id`. Pin this exact file in the command plan's
`Files` map and independently admit its capture before execution. Do not create
or replace it during recovery. A namespace restart or owner-directory replacement
must refuse this Join; an independent recovery flow is then required.

Join checks its parent is PID 1 and validates the pinned identities. It then
uses only Bash builtins to scan `/proc`: any PID other than init and Join itself
(including escaped sessions and zombies) returns 75, not success. The generic
Join runner does not retry that status. This checker is not an orphan reaper,
source/image admission, or proof of the 30-second fault requirement.

Local integration tests create real isolated PID namespaces using `unshare`;
the clean case passes and an actual `setsid` child is rejected. Identity,
permissions and parent mismatches are rejected too. Environments denying PID
namespace creation explicitly skip that integration test, not claim coverage.
The previously verified image below predates this script and does not contain it.

## Runtime image (build verified; fault execution still incomplete)

`Dockerfile.lease-fault-tools` is a separate test runtime, not the product
image or an experiment launcher. Its build context must contain only `bundle/`
(the generated bundle) and `gh` (a separately reviewed static Linux executable
for the target architecture). Never send credentials, kubeconfig, plans or an
owner directory to the Docker build context. Use the provided
`lease-fault-tools.dockerignore` as the context's `.dockerignore` to exclude
everything except the bundle and gh.

The required build arguments are `BASE_IMAGE` (a reviewed
`ghcr.io/fivetime/kubebrain@sha256:...` reference), `BUNDLE_SHA256` (the independently
reviewed bundle `SHA256SUMS` digest), `GH_SHA256`, and `TOOLS_SOURCE` (the exact
40-character tool source revision). Build with `--network=none`
for the build steps; base-image resolution can still require registry access.
The image checks the bundle inventory, gh digest and runtime dependencies.
These checks do not authenticate CI provenance. Its default entrypoint only
validates a plan; it does not execute a fault.

The first successful build and container smoke run is recorded below. This
does not supply the missing complete execution CLI or its concrete online
admission and owner-specific Join. Do not treat the runtime as a completed
fault executor.

`.github/workflows/lease-fault-tools.yml` provides a **manual-only** self-hosted
build path for Linux amd64. The operator must independently verify `base_image`
and `gh_sha256` before dispatch; digest syntax alone is not that verification.
The workflow freezes the checked-out tool source, tests/builds/scans the bundle,
and smoke-tests the resulting image without network, writeable root filesystem,
capabilities or cluster credentials. The default entrypoint must refuse to run
without an approved plan. Only then does it publish a separate
`fault-tools-SOURCE-RUN-ATTEMPT` tag, never the product's `dbaas` tag, and archive
the runtime digest and checksums. Deployment must use the subsequently reviewed
digest, not the tag. Runtime provenance must be reviewed separately from product
release provenance; this is not the `image.yml` product release artifact schema.

A push changing this workflow on `dbaas` runs only a registration/preflight job;
the image job is explicitly disabled for push events. This allows the branch
workflow to be registered before API dispatch without modifying `main`.
The preflight prints the runner's gh version and executable SHA256 (or reports
it unavailable), never gh authentication/configuration. Review this identity
before selecting the dispatch digest; do not assume the runner matches the
developer machine or blindly substitute whatever digest is observed.
[GitHub documents](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#workflow_dispatch)
that a workflow which has run can be dispatched against another branch via API.
Registration and API dispatch were verified by the runs recorded below.

### Verified first build: 2026-09-20

Registration run `35506567911` succeeded, skipped the image job as intended, and
reported gh 2.100.0 with the reviewed executable digest
`553949e2efa12842771efe6012aa4de21f1d591530ec17fc435f610f10e017ee`.
Manual run `35506632638`, attempt 1, then succeeded for exact tool source
`5292255aedd9a51bf6aef6f66e734c7b0c6c765f`. It used the independently authenticated
a47 product base image
`ghcr.io/fivetime/kubebrain@sha256:a4512a5e4d589b943950f3ab0ee2b3b50c0fafb7b2a242b0e8f09ce53b17085b`.

The Linux amd64 tool image is
`ghcr.io/fivetime/kubebrain@sha256:9b2508e5bf6a6114d6355630c70de408b01ccbcd873fc0b6bb29e8a6847eb0a8`.
Its bundle manifest digest is
`1a60a10b328f9634f57299cbfdff678f2080f3a780d7f105ccd77aaa9b6a5710`.
Artifact `10604830603`, named `lease-fault-tools-35506632638-1`, was independently
downloaded and its ZIP digest verified as
`54b869c33a5bb5d611dd401d8d74209a549bcd6c24b88a6b9f23c1b8ac4b61ea`.
The six-entry archive was read without extraction; all internal checksums,
source/base/tool/bundle bindings and the default-entrypoint refusal were checked.
The registry manifest was fetched separately and matched the published image
digest. Run success, source, attempt and artifact digest/availability were
rechecked after download. Raw evidence and verified `SHA256SUMS` are retained at
`/root/.local/state/kubebrain/tool-runtime-35506632638.JMK9Whfg/`.

This establishes a built/tested tool runtime only. No executor Pod has been
created and no candidate deployment or fault injection occurred in this run.
Online fault admission, owner-specific Join, recovery acceptance and the original
30-second real-cluster result remain open. This is not an ARM runtime build.

### Dedicated-cluster connectivity check: 2026-09-20

A separate short-lived Pod `kb-fault-runtime-tls-35506632638`, UID
`a91612d7-6d43-41e7-92a3-073ad25ffc03`, ran the verified tool image on
`k8s3-worker3` in `kubebrain-dbaas-test`. It was not a fault executor. It had a
180-second deadline, no host network/PID/IPC, no ServiceAccount token, a read-only
root filesystem, all capabilities dropped and bounded CPU/memory/storage.
Only the existing probe TLS Secret was mounted read-only; no Kubernetes admin
configuration or GitHub credentials were provided.

TLS 1.2 handshakes with the probe client certificate succeeded directly to
`240.16.6.205:3379`, `240.16.10.109:3379` and `240.16.7.126:3379`, verifying the
CA chain and `kubebrain-local-client.kubebrain-dbaas-test.svc` peer name. The
container exited 0 without restarts, and its actual imageID matched the reviewed
`9b2508e5…` digest. This exercised the real namespace network path, not a host
port-forward. It sent no application RPC and proves neither term behavior nor
fault recovery; no NetworkPolicy or workload configuration was changed.

Target Pod UIDs, IPs, container IDs, restart counts, readiness and images matched
before/after. The StatefulSet remained generation/observedGeneration 138 with
three ready replicas on fixed baseline `50b9938f…`. The diagnostic Pod was
deleted with a UID precondition and a subsequent GET confirmed absence.
Manifests, admission response, terminal status, TLS logs, target comparisons,
delete response and final namespace/StatefulSet observations are retained with
verified checksums at
`/root/.local/state/kubebrain/tool-runtime-tls.0nSJMpRV/`.
