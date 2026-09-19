# Endpoint nonce snapshot validation

`local-nonce-endpoints.jq` is a read-only snapshot classifier for fault preparation.
`observe-local-nonces.sh` collects its input with live identity checks.
`leasefault.NetworkObserver.NoncesSafe` now adapts it to
`FaultPreparation.NoncesSafe`, with fresh API identity/admission checks before and
after, exact private Pod-input comparison and the unchanged caller deadline.
The caller must supply live claim and pinned-tool/cluster admission plus durable
result retention. No complete real fault CLI has yet been run with this adapter.

Input contains independently admitted `expected_agents: [{uid,node}]`, collected
`agents: [{uid,node,endpoints}]` (full `cilium-dbg endpoint list -o json` output per
agent), and `target: {node,endpoint_id,namespace,pod,ipv4}`. Supply the exact active
and reserved nonces with `--arg active ... --arg reserved ...`.

The classifier requires complete matching agent membership, one agent per node,
unique endpoint IDs within each agent, and exactly one bound target endpoint.
It examines identity, security-relevant, derived, disabled, desired user and
realized user label sets. The reserved nonce must occur nowhere; the active nonce
may occur only on the target. A foreign fault-owner on the target is rejected.
Missing identity labels and malformed label sets fail closed. Transitional labels
on the target may pass this collision test; convergence remains a separate gate.

The fault policy uses an unqualified selector key. Cilium's local reference
`pkg/policy/api/selector.go` selects `LabelSourceAnyKeyPrefix` when decoding these
selectors, so checks include every label source, not just `k8s:`. Treating absent
Pod labels as absent endpoint labels, or scanning only identity labels, is unsafe.

The caller must still collect all nodes/agents without omissions, bind the target
to independently admitted Namespace/StatefulSet/Pod/CEP identities, verify agent
and target processes before/after collection, retain private evidence, recheck
membership and ownership, and enforce one bounded context. Sequential snapshots
are not atomic or a lock; this result cannot prove continuous absence, identity
convergence, policy enforcement, TCP connectivity, process joining or acceptance.

Local tests cover empty/owned/transitional target labels, reserved and foreign
active collisions (including non-k8s sources), stale label fields, missing/changed
agents, duplicate endpoints and target identity mismatches.

A development-only read-only snapshot was collected on 2026-09-19 under
`/root/.local/state/kubebrain/nonce-readonly.6Us4lOwV`: 11 agents, 89 endpoints.
Agent membership covered the observed Node list; agent and target Pod processes
were compared before/after. The classifier accepted unused diagnostic nonces
`term-readonlysnapshot` / `reserved-readonlysnapshot`. `SHA256SUMS` retains the
original scan; `ANY_SOURCE_SHA256SUMS` retains the later all-source classifier
result against the same captured input, not a fresh scan. This was no fault
experiment, did not check namespace/StatefulSet admission or final CEP/Node
stability, and does not authorize future writes. No cluster state was modified.

## Bounded live collector

Invoke with Bash and exactly five arguments: private attempt directory, private
independently admitted Pod JSON, active nonce, reserved nonce, and absolute Unix
nanosecond deadline. The deadline must be in the future and at most five minutes
away. Every kubectl operation uses the remaining budget, never a fresh overall
timeout; the caller must also supervise the entire process with that context.

The collector is fixed to the dedicated test kubeconfig/context and admitted
namespace/StatefulSet UIDs used by other local observers. It checks both before
and after the scan, requires complete ready Node/agent coverage, verifies agent
and target Pod processes, target controller and allowed label state, and compares
Node membership and CEP identity/network/owner before and after. It retains all
snapshots, input/tool-dependent observations, a manifest and exit status privately.
The caller remains responsible for pinning the actual script/tool bundle and
authenticating the independently admitted Pod input; the script does not acquire
ownership, validate CI artifacts or mutate the cluster.

There is no automatic retry or pending-success interpretation. Sequential reads
still do not prove continuous absence or fence concurrent administrators. A
successful output marker means only that the bounded snapshot checks passed.
Mocked kubectl tests cover collisions, omitted/paginated membership, wrong scope,
node/CEP replacement, agent restart, changed controller/label and expired budget.

The actual collector was also run read-only with a 60-second caller deadline on
2026-09-19. Evidence is at
`/root/.local/state/kubebrain/nonce-collector-readonly.ZGddIwtk`, inner capture
`nonce-observation.54gCCFOY`: 11 agents / 89 endpoints, exit 0, both manifests
verified. This fresh scan used unused diagnostic nonces `term-readonlycollector`
and `reserved-readonlycollector`; it created no label/policy/claim and did not
admit an actual fault attempt. The full lifecycle and real 30-second gate remain
unverified.

The Go adapter performs one attempt and retains its output/status, including on
failure. Exit 75 is an error here, not an implicit retry. It requires no policy
receipt because initial nonce admission must precede reservation CREATE. Tests
wire it through `PrepareFault` using real KubeBrain/memkv RPCs, a simulated API
and a boundary shell fixture; a successful preparation is explicitly recovered.
Input drift, lost admission and retention failures cannot permit reservation.
These tests do not replace the collector's Cilium tests or actual fault acceptance.
