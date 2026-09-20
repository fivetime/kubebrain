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
