# Dedicated lease fault driver

`lease-fault-run` connects the existing native command plan to concrete live
namespace/deployment, process, TLS member and file admission, then executes one
attempt. It is restricted to the dedicated cluster identities and `4906a5f8`
product candidate in `main.go`. It does not deploy that candidate or generate
plans, credentials, diagnostic snapshots, experimental peer configuration or
network-policy approval.

This candidate's image and release evidence passed workflow `36739259406`,
attempt 1 (artifact `11111945808`). Earlier `1e862c11` acceptance results do not
prove acceptance of this candidate; real-cluster cases must be rerun.
The matching regression workflow must independently succeed before admission;
the image workflow alone is insufficient. Later tool-only commits do not change
this product candidate: report tool source and product source separately.

The command must be PID 1 in the separately admitted execution Pod, with one
container, no sidecars/init containers/shared or host namespaces/host volumes,
all capabilities dropped, a read-only root filesystem and no automatically
mounted service-account token. The reviewed runtime image and all additional
files (including this executable) must be independently authenticated and pinned
before execution; Pod image syntax and a locally computed checksum are not CI
provenance. Do not run inside an existing workload/discovery container.

Start with `--owner /private/fresh-owner --execute`. The owner directory must
already exist, be empty and mode 0700. Startup captures the live PID namespace
identity and prints its digest. The external controller then stages and reviews
the existing `NativeCommandPlan`, pins that exact identity, and exits every
staging exec session. Supply exactly one JSON record followed by EOF on stdin:

```json
{"command_path":"/private/command.json","command_sha256":"INDEPENDENTLY_APPROVED_SHA256","release_path":"/private/release.json","release_sha256":"INDEPENDENTLY_APPROVED_SHA256"}
```

The handshake expires after five minutes; it does not start the fault clock.
The command plan uses the existing observer layout, including pinned
`executor-pod.json`, `observer-pod.json`, `observer-targets.json`, diagnostic
specification/manifests, `info.crt`, `bin/info-diagnostic-probe`, and the same
`tls/{ca.crt,probe.crt,probe.key}` for RPC and protected diagnostics. The diagnostic
TLS name is the info service name, not the public client service name. All shell
environment values and transitive observer dependencies are checked explicitly.
Use an approved release download plan; the driver authenticates product CI before
claim acquisition. Execution image provenance remains the external controller's
separate admission responsibility, as with the tool bundle.

Once staging has completed, the driver checks the namespace inventory, acquires
one durable claim, and calls `RunNativeCommand` once. The original fault deadline
remains 30 seconds. After all managed command waits finish, the packaged PID-1
Join reaps adopted zombies, preserves their exit statuses, and rejects live or
unsuccessful unaccounted children. The inventory script then independently checks
absence. Recovery has its own bounded budget; claim release requires fresh
post-recovery protocol/network/identity proof. A failed fault remains an error
even when recovery/release succeeds. No retry, marker adoption or cleanup of
unrelated resources is performed.

A crash, failed Join, ambiguous claim creation or failed recovery retains the
attempt for independent reconciliation. Never restart with the same owner, use
a different PID namespace's identity, or infer successful restoration from an
empty error field without `RecoveryAttempted`. Restore the outer candidate
deployment only after the fault resources have been independently reconciled.

Current verification is local: compilation, refusal tests and real PID namespace
startup/refusal. The complete driver has **not yet executed the real-cluster
case**; neither these checks nor a tool image build constitutes acceptance.
