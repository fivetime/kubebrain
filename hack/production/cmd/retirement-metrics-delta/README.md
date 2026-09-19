# Offline retirement counter diagnostics

Build `./hack/production/cmd/retirement-metrics-delta` with the experiment's
reviewed tools revision. This command reads two completed `metrics.*` directories
from `protected-stack-session.sh`; it never contacts or modifies the cluster.

Required flags:

```
--before CAPTURE_DIRECTORY --after CAPTURE_DIRECTORY
--namespace-uid ADMITTED_NAMESPACE_UID --sts-uid ADMITTED_STS_UID
--pod-uid ADMITTED_POD_UID --spec-sha256 CANONICAL_ADMITTED_SPEC_SHA256
--cluster ADMITTED_KUBEBRAIN_CLUSTER_NAME
--stage local|peer --outcome OUTCOME
```

Add `--duration` to require the matching completed-operation histogram and emit
`duration_delta: {count, seconds}`. This is the observed change in cumulative
seconds for the selected stage/outcome, not a per-event timestamp or total
failover latency. Floating-point sums retain exporter rounding limitations.
Both snapshots must have matching result/histogram counts; a scrape between
the two metric updates is rejected rather than silently reconciled.
Bucket bounds must match, cumulative bucket deltas must be monotonic, and
sum/count/bucket resets, missing components or duplicate samples are rejected.

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
- Probe summary/content binding and finite integer counters with known labels;
  the production `cluster` label must equal the independently admitted name.

Success prints one JSON object with `count_delta`. Missing counters, first
appearance without a baseline, reset, process change, overlap, or invalid evidence
returns nonzero without a success object. Do not replace such failures with zero.
Lazy counter registration means a prefault capture may legitimately have no
retirement series; that is an **unknown delta**, not proof of zero events.

The manifest is an integrity check, not a signature. The caller must retain the
original experiment admission, tool/image provenance and fault timeline. This
command does not verify those, the entire 30-second fault
gate, successor readiness, or production readiness. Count changes do not locate
individual events in time or explain latency. No live experiment is authorized
merely because this offline check succeeds.

## Scheduled later capture

To check the later capture against its scheduler receipt, add all three flags:

```
--schedule SCHEDULE_DIRECTORY
--fault-origin-ns ORIGINAL_UNIX_NANOSECONDS
--offset-ns PLANNED_OFFSET_NANOSECONDS
```

Supply the original clock and planned offset from the independently saved
experiment inputs, not from an unverified receipt. Decimal values must be
canonical (no sign or leading zeroes); the offset can be zero but must be less
than 30 seconds. Supplying any of these flags requires all three; an empty,
partial or invalid scheduled request never falls back to ordinary capture mode.

This calls `LoadScheduledCapture` for `--after`, checking the exact schedule
manifest, capture binding, completion marker, planned lower bound and original
30-second upper bound, including capture stage times. Fault-time anonymous
rearming is rejected. The schedule and later capture must be distinct sibling
directories. Ordinary mode and `--duration` semantics are unchanged.
It also calls `LoadPrefaultCapture` for `--before`: all recorded baseline stages,
including any anonymous rearming, must finish before the original fault clock.
The final microsecond-resolution timestamp is conservatively enclosed; checking
only the earlier probe completion is insufficient.

Successful scheduled output also includes `scheduled_capture_verified: true`,
`fault_origin_ns` and `offset_ns`; the timestamps are JSON strings to preserve
nanosecond precision. The existing fault/readiness/event-latency proof fields
remain false. This binds the recorded stages to the supplied clock, but does not
authenticate that clock or prove that the worker exited successfully or every artifact was flushed within
the deadline, or that the original request and successor gates passed. The
experiment driver must enforce those requirements and join workers before
restoration. This command remains an offline diagnostic, not the real fault
driver or a replacement for its admission and recovery procedure.
