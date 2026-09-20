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
Join script, the protected diagnostic input layout (including owner/bin), and
the complete execution CLI/online gates. The builder does not manufacture these.
The existing source-specific stack classifier must approve the exact tested
source; packaging it does not add a candidate to its allowlist.

In the dedicated cluster, use a separately reviewed execution Pod in the test
namespace. Do not relax workload isolation to enable host access and do not run
the experiment inside an existing discovery/service container. Neither a bundle
build nor its checksum verification proves the original 30-second acceptance.

## Runtime image (not yet validated)

`Dockerfile.lease-fault-tools` is a separate test-runtime draft, not the product
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

No successful image build or container smoke test has been recorded for this
draft. Do not deploy it as the fault executor until those checks, source and
image admission, and the missing complete execution CLI are finished.

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
Actual registration and dispatch still need verification after this file is pushed.

This workflow has not yet been executed. Its successful completion would only
establish a built/tested tool runtime, not the missing online fault admission,
owner-specific Join, recovery acceptance or the original 30-second result.
