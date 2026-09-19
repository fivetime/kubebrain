# Offline retirement counter diagnostics

Build `./hack/production/cmd/retirement-metrics-delta` with the experiment's
reviewed tools revision. This command reads two completed `metrics.*` directories
from `protected-stack-session.sh`; it never contacts or modifies the cluster.

Required flags:

```
--before CAPTURE_DIRECTORY --after CAPTURE_DIRECTORY
--namespace-uid ADMITTED_NAMESPACE_UID --sts-uid ADMITTED_STS_UID
--pod-uid ADMITTED_POD_UID --spec-sha256 CANONICAL_ADMITTED_SPEC_SHA256
--stage local|peer --outcome OUTCOME
```

Use identities saved by the experiment's admission, not identities copied from
the unverified capture just to make it pass. `spec-sha256` is SHA-256 of the
admitted StatefulSet's **spec object only**, decoded case-sensitively with integer
preservation and re-encoded using Go `encoding/json.Marshal` (no trailing newline).
It is not the hash of the original pretty-printed JSON file.

Checks include:

- Exact COMPLETE marker; eight mandatory bounded regular files; no symlinks.
- Exact manifest membership and hashes. Absolute paths must still point to the
  selected directory. Moving a capture requires a separate archive workflow;
  do not rewrite the manifest to make this verifier accept it.
- Admitted namespace/StatefulSet/Pod UIDs, observed generation and spec hash.
- Unchanged Pod spec, namespace/name/IP, image ID, container ID, restart count,
  and running start time before/after; readiness may legitimately change.
- Successful ordered capture stages, with probe summary times inside the
  protected-probe stage, and strict non-overlap between samples.
- Probe summary/content binding and finite integer counters with known labels.

Success prints one JSON object with `count_delta`. Missing counters, first
appearance without a baseline, reset, process change, overlap, or invalid evidence
returns nonzero without a success object. Do not replace such failures with zero.
Lazy counter registration means a prefault capture may legitimately have no
retirement series; that is an **unknown delta**, not proof of zero events.

The manifest is an integrity check, not a signature. The caller must retain the
original experiment admission, tool/image provenance and fault timeline. This
command does not verify those, histogram durations, the entire 30-second fault
gate, successor readiness, or production readiness. Count changes do not locate
individual events in time or explain latency. No live experiment is authorized
merely because this offline check succeeds.
