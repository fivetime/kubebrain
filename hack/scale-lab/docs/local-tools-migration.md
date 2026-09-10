# Consolidation of local scale-lab tools

The authoritative source is `hack/scale-lab` in KubeBrain's Git repository.
On 2026-09-10 the separately maintained `/root/kwok-scale-lab` was audited for
consolidation at the user's request. Do not recreate that directory or point
automation at its old binaries.

Consolidation completed: all 15 old files (including three compiled binaries)
were archived and compared with `tar --compare` before the old directory was
removed. No symlink or executable compatibility alias remains at the old path.
Recovery archive on the original operator host:
`/root/.local/state/kwok-scale-lab-retired.oE1noyLM/kwok-scale-lab.tar.gz`
(private directory; not committed). SHA256:
`babb85c9cc89bedeec3b1673210c03465a93ecd623ea582ae2667e567e344d0a`.
If recovery is required, extract into a separate audit directory, not the retired
path. The repository source remains authoritative.

| Local input | Repository disposition |
| --- | --- |
| `loadgen/main.go` | Keep repository version: it contains the older functionality plus createpods, additional resource types and targeted cleanup modes. Formatting differences are not new functionality. |
| `topology.yaml` | Identical to `config/tikv-topology.yaml`; retain one repository copy. |
| `kwok-stages-fast.yaml` | Identical to `config/kwok-stages-fast.yaml`. |
| `scale-out-tidb.yaml` | Identical to `config/scale-out-tidb.yaml`. |
| `loadgen/slowwatch` | Refactor into `probes/slowwatch` using root dependencies, explicit endpoint/prefix, TLS and tested cancellation/ordering checks. Remove the unsupported gap-free PASS claim. |
| `loadgen/watchflood` | Refactor into `probes/watchflood` using root dependencies, explicit target/load opt-in, full pagination, watch error accounting and joined shutdown. |
| Old module files and `bin/` | Do not import stale dependency locks or compiled binaries; rebuild from the authoritative source. |

Local build/test entrypoints are shared by direct use and `setup.sh tools`.
Existing loadgen/bigstream module boundaries and CLI paths are retained to avoid
breaking consumers; new probes do not add nested modules. This consolidation
does not claim to have run a new scale campaign or prove production readiness.

Verification: local probe and nested-module race tests/vet, the complete `build`
test package, the scale-lab build contract under race, eight-tool builds, shell
syntax/ShellCheck for new entrypoints, and CLI help/missing-target rejection all
passed. No real cluster load or deployment was run for this consolidation.
