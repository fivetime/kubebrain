# Dedicated-cluster recovery command

This is a recovery-only entry point, not an experiment driver or acceptance gate.
It never acquires a claim, injects a fault, restarts a probe, or releases ownership.
Use only an independently admitted build and operator-approved private plan.

Without `--execute`, the command checks the strict plan and pinned file contents
and does not construct Kubernetes/gRPC clients. Example argument forms:

```text
lease-fault-recover --plan /absolute/private/recovery-plan.json --approve-sha256 <approved-sha256>
lease-fault-recover --plan /absolute/private/recovery-plan.json --approve-sha256 <approved-sha256> --execute
```

Approval must come from independent review of the attempt, artifacts and tool
bundle. Hashing an unknown plan does not make it trusted. File validation is not
CI/image admission, valid credentials, runtime readiness or completed recovery.
The new command has not yet been used on the real cluster.

The JSON plan contains:

- `directory`: existing private 0700 attempt directory, with actual owner and
  network receipts and protocol intent where preparation reached that stage.
- `network` / `protocol`: independently retained `NetworkRecovery` and
  `ProtocolRecovery` bindings. Protocol integer IDs retain their string encoding.
- `api_server`: exact HTTPS endpoint from the admitted kubeconfig, not the SSH
  control-node address. The currently observed configuration uses
  `https://10.224.33.1:6443`; no value is inferred automatically from live resources.
- `endpoint`, `server_name`, `ca`, `certificate`, `key`: direct healthy-member
  recovery endpoint and pinned mutual-TLS files. Never place key contents here.
- `script_directory`: admitted `deploy/test-cluster` bundle directory with its
  relative `../../hack/production/same-pod-process.jq` dependency.
- `join_script`: independently audited Bash script invoked with exactly the
  attempt directory as its argument. It must verify and join all owned processes,
  including escaped descendants. A no-op script or PID list alone is not proof.
- `targets_sha256`: independent backend-target snapshot digest.
- `files`: absolute filename-to-lowercase-SHA256 map, including kubeconfig, Bash,
  TLS files, join script and all its dependencies, all composite observer scripts
  and jq dependencies, and `observer-pod.json` / `observer-targets.json` in the
  attempt directory. The Pod bytes must exactly match `network.pod_before`.
- `timeout_seconds`: one recovery budget, 1–300 seconds. It is not a fault clock.

Plans reject duplicate/unknown JSON fields, repeated/aliased options, unsafe
files, missing pins, incompatible bindings and a different observer scope.
The observer suite is bound to the existing `kubebrain-dbaas-test` namespace and
`kubebrain-local` StatefulSet UIDs; this command does not generalize it to another
cluster. It uses `/root/.kube/kubebrain-test-10.32.32.66.conf` and context
`kubebrain-test-10.32.32.66`, requiring pinned inline mTLS credentials, no auth
plugins, credential-file indirection, insecure TLS or proxy. Native API/gRPC
clients disable environmental proxy use. Kubernetes requests have a 15-second
timeout and an explicit 20 QPS / 40 burst client limit, still within the same
overall recovery context. The Kubernetes client sets `MaxRetries(0)` on every
request and rejects HTTP redirects: server `Retry-After` responses must not replay
a mutation. Errors are returned for reconciliation without automatic retries.

Children receive only `PATH=/usr/local/bin:/usr/bin:/bin`; the provisioned system
toolchain at those paths remains part of trusted operator admission. Pinning the
scripts does not authenticate arbitrary transitively loaded programs or dynamic
libraries. The operator must account for those dependencies before approving.

Execution checks pinned inputs, runs the owner-specific join script, then uses
the existing staged recovery coordinator and live cluster claim. No write is
retried. Join and observation outputs/statuses are durably recorded as private
`recovery-*.json` files (output bytes are base64 encoded). Record or observation
errors stop the process with no success marker; partial changes require explicit
reconciliation. Success reports `RECOVERY_VERIFIED_OWNER_CLAIM_RETAINED_NOT_FAULT_ACCEPTANCE`.
Claim release remains a separate explicit step after fresh full recovery proof.

Observer retention uses `leasefault.RetainRecoveryObserver`: it opens one private
owner-directory handle and creates an exclusive, non-overwriting 0600 record
relative to that handle. Both file and directory are synced before success; a
replaced or newly public owner path is rejected. Partial/original-directory
records remain for reconciliation. Stage names are bounded identifiers, output
is limited to the process supervisor's 1 MiB cap, and error text to 64 KiB.
An observed error is preserved as data; callers must still reject the failed
observation independently. Saving a log is not proof of recovery or admission.

Tests cover strict parsing, pinned private files, no-client default mode, failed
join and missing-record ordering with injected client/admission fixtures. An
execution-path test also prepares and recovers a real KubeBrain RPC service over
in-memory gRPC with memkv, a fake Kubernetes API and fixture observer/join scripts.
It checks protocol restoration, policy removal, preservation of unrelated labels,
retention of ownership on both success and identity-observation failure, and
private evidence contents/statuses. These tests do not establish a real cluster
recovery, TLS session, Cilium convergence or a valid real join script.
