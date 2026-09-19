# Endpoint nonce snapshot validation

`local-nonce-endpoints.jq` is a read-only snapshot classifier for fault preparation.
It is **not yet a complete `FaultPreparation.NoncesSafe` collector/hook**. It
must not be substituted for that hook with fabricated or incomplete snapshots.

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
