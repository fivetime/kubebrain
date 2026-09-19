# Original expired-lease response evidence

The shell entrypoint is `hack/production/cmd/lease-fault-response`. Supply the
completed original JSONL through stdin and exactly one `--name value` pair for
each of `lease-id`, `cluster-id`, `initial-member-id`, `initial-term`,
`successor-term`, and `fault-origin-ns`. Values must be canonical decimal strings,
not values rounded by a JSON floating-point tool. Input is bounded to 12 KiB.
The caller must execute it within the remaining original fault deadline: stdin
can block and the offline validator does not establish a fresh execution budget.
On success stdout is one JSON summary with exact integers encoded as strings and
`fault_acceptance_proven: false`; validation failures emit no success summary.
The caller owns log provenance, redirection and output publication. This command
does not open files, mutate the cluster, or perform recovery.

`ValidateOriginalResponse` is the repository-owned offline validator for the
original `lease-term-probe` JSONL stream. It accepts exactly three newline-ended
events: expired preflight, request sent, response. It bounds each event to 4 KiB
and the whole log to 12 KiB, rejects unknown/duplicate/case-alias fields, and
decodes signed lease IDs and unsigned cluster/member/term IDs without float64.

The caller supplies independently admitted identities, the initial term, an
independently observed successor term, and the original fault clock. Preflight
must describe the admitted expired lease/member/term; request sent must precede
the fault clock. The response must carry the same lease/cluster, a nonzero member,
a nonnegative TTL, and a term at least as new as the observed successor. Its
timestamp must fall in the original inclusive [origin, origin + 30 seconds]
response window. A later forwarding member is permitted, as in the old driver.

The validator does not authenticate files or clocks, verify TLS, prove that the
request was blocked, or prove isolation/successor election. In particular:

- A valid log is not proof of a single connection: the admitted probe's behavior
  and successful process exit must also be checked.
- The outer experiment must independently check actual backend drops, demoted
  stack, lease/key state, all process joins, and restoration.
- The outer 30-second budget covers the entire acceptance phase, not just the
  logged response timestamp. This validator does not extend it.
- This package is not yet wired into the real fault driver; there is no new
  cluster acceptance result.

Tests include IDs above 2^53 and at uint64 limits, signed lease IDs, 1 ns boundary
violations, term/identity mismatches, malformed records and a round trip through
the actual probe encoder using an in-process gRPC fixture. That round trip uses
a synthetic post-hoc origin only to verify schema compatibility, not fault timing.
