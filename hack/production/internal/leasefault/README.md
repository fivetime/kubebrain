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

## Protocol recovery intent

`ArmProtocolRecovery` persists a create-once private record, syncing both the file
and its containing directory before returning success. The outer recovery owner
must call it before granting the fixture lease, writing the owned key or arming
the CORRUPT alarm. It binds owner, namespace/StatefulSet UIDs, cluster, alarm member,
lease and acceptance key; it never records credentials or child PIDs. Recovery
must conservatively reconcile all three protocol mutations, regardless of which
child-local attempt flags were lost. Existing or partially written records are
preserved and rejected for rearming, not silently replaced.

`LoadProtocolRecovery` checks exact independently retained admission and rejects
symlinks, non-regular/public files, oversized or malformed JSON, duplicate/unknown
fields and identity mismatches. A bad/missing record means manual reconciliation,
not permission to skip cleanup. The directory and admission must remain trusted;
this is not a cross-process lock or authenticated manifest. These filesystem tests
do not simulate storage hardware power failure or guarantee remote filesystem
durability. Network-policy and Pod-label identities still need separate recovery
records; this API does not perform RPCs, check live ownership or recreate healthy
member tunnels. Neither API is yet connected to the real fault driver.

The prepared-child regression arms the record in the parent before spawning a
real Bash child, then covers preparation failure, cancellation after readiness,
fault deadline expiration, and success. After `WithPreparedFault` returns it
checks that the direct child is gone and reloads the unchanged independently
bound record. This verifies local lifecycle ordering and record availability;
it does not exercise recovery RPCs, escaped descendants, or cluster acceptance.

`VerifyProtocolRecovery` issues only Alarm(GET), LeaseTimeToLive(keys=true), and
an exact-key linearizable Range using a caller-supplied authenticated gRPC
connection and an independent recovery context bounded to at most five minutes.
It requires matching cluster headers, no alarms, the exact lease ID with TTL=-1,
grantedTTL=0 and no keys, and an empty Range without count/more discrepancies.
An expired-but-not-revoked lease does not pass. RPC errors are retained; there is
no verifier retry or mutation. Tests use a fake ClientConn to check exact request
methods/fields, malformed responses, cluster mismatches and deadline propagation;
an additional in-memory gRPC server checks protobuf round trips at full-width IDs
and distinguishes revoked leases from expired-but-retained leases. It uses only
an in-memory listener, not cluster transport or TLS. Live Kubernetes identity,
network restoration and process joins remain external prerequisites. The driver
has not yet connected this verifier or the separate recovery writes below.

`RestoreProtocol` now provides the protocol write phase, still not wired to a
driver or used on a cluster. It requires a saved matching intent, bounded recovery
context, authenticated connection, and an external admission callback that checks
exclusive recovery ownership, process joins, restored network and live identities.
The callback and record are rechecked before each write. Read preflight rejects
unrelated alarms, other keys on the lease, or a fixture key with a different lease.
Only the exact CORRUPT alarm is deactivated and the recorded lease revoked; it never
deletes keys directly or edits Kubernetes resources. All three read-only recovery
checks must subsequently pass. Normal expiry may race revocation after alarm
removal: only the exact lease-not-found error proceeds to final verification.
Other write failures remain errors, with no retry or invented success. Partial
recovery needs reconciliation under the same owner, not a new experiment. Tests
use simulated RPCs; they do not establish live recovery or mutation atomicity.

`TestRestoreProtocolKubeBrainGRPC` additionally runs the real KubeBrain RPCServer,
backend and lease/alarm code over in-memory gRPC with memkv storage. It arms the
record before granting a fixture lease/key and activating CORRUPT, then verifies
restore and already-restored behavior for both live and expired-but-retained
leases. The expiry case waits for an observed negative TTL with retained key and
positive granted TTL, not a synthetic response. Peer leadership and Kubernetes
admission are fixed fixtures; this is not TiKV, TLS, network-fault or cluster proof.
The same real-service fixture also attaches an unrelated key to the lease and
checks that restore refuses before disarming: the CORRUPT alarm, fixture key,
unrelated key value and both lease attachments remain intact.

The `child-timeout` case composes `WithPreparedFault` with this real-service
fixture: after protocol setup in the parent, a real Bash child misses its fault
deadline and is killed and reaped. Recovery using the expired fault context is
rejected, with the alarm and lease attachment still present. A separate bounded
recovery context then restores and verifies the service; every admission callback
checks that the direct child is gone. This does not move protocol setup into the
child, simulate a network fault, prove escaped descendants are gone, or convert
the timed-out attempt into acceptance success. Three race runs of the real-service
cases passed (11.423s); package vet and diff checks also passed. The complete real
cluster coordinator remains outstanding.

## Network pre-mutation intent

`ArmNetworkRecovery` saves parent-owned namespace/StatefulSet/Pod identities,
policy name, active/reserved nonces, the original unlabelled Pod and admitted
active policy before either Pod labeling or inactive policy reservation. It is
create-once, private, size-bounded and syncs file and directory. Full original
objects are retained as raw JSON, including full-width numeric values; duplicate
fields are rejected recursively. Identity extraction is case-sensitive. Object
identity checks are not full Kubernetes schema validation or policy approval.

`LoadNetworkRecovery` requires independently retained admission and compares all
saved fields and original objects (JSON whitespace is normalized, object key order
is retained). Missing, altered, malformed, public, non-regular or linked records
fail closed. Do not infer that such a failure permits skipping cleanup. Records
may include sensitive Pod configuration and must remain in the private owner
directory, never public logs or the repository.

The prepared-child regression arms both protocol and network records before a
real child starts, then reloads both after preparation failure, cancellation,
timeout or success and direct-child join. These tests do not prove storage power
failure durability, distributed ownership, Kubernetes mutations or Cilium state.
This intent is NOT a receipt and cannot authorize deleting a same-name policy.
These APIs are not yet wired into the complete cluster fault coordinator.

`TestNetworkRecoveryFeedsLabelPlan` loads the persisted original Pod and identity
into the repository's actual jq label planner, together with synthetic current
observations. The emitted patch retains the exact Pod UID, full-width string
resourceVersion and nonce tests. This checks the journal-to-planner boundary;
the observations are fixtures, not evidence of live policy absence or recovery.

`SaveNetworkReservation` persists the authenticated inactive CREATE response after
checking the independently bound intent, policy identity, nonempty UID/string
resourceVersion and exact admitted inactive spec. It is create-once, private and
syncs both file and directory. Activation must not start until this returns nil.
`LoadNetworkReservation` revalidates both records before returning the raw receipt
for the deletion planner. Neither API establishes response provenance: callers
must use the actual non-dry-run CREATE response, not a same-name GET. Ambiguous
CREATE or failed persistence requires reconciliation, never a guessed UID or
unconditional deletion. The CREATE-to-save crash window still needs coordinator
reconciliation; a missing receipt is not evidence that no policy exists.

Receipt regressions cover rejected active/changed policies, duplicate fields,
identity mismatches, unsafe records, create-once persistence and the real jq
deletion planner with synthetic observations. Record readers explicitly check
the named entry with Lstat and compare its inode with the opened file: os.Root
can resolve an in-root link despite O_NOFOLLOW. Valid relative symlink regressions
cover protocol intent, network intent and reservation records. These checks do
not replace exclusive lifecycle ownership or prove live Kubernetes recovery.

`ReserveNetwork` connects intent and receipt persistence to an actual dynamic
Kubernetes client CREATE. It requires a bounded context and live admission callback,
arms the create-once intent before the request, submits the inactive selector, and
saves the actual response before returning success. It does not adopt a same-name
object, label a Pod, activate a policy or implement a retry loop. A second call
with the same owner directory fails at arming, including after an ambiguous first
request. Client transport retry policy remains the caller's responsibility.
HTTP fixture tests cover the request path/body, pre-request intent, full-width
resourceVersion receipt, conflict, malformed response, failed persistence and
admission rejection. They do not simulate Kubernetes admission enforcement or
Cilium endpoint selection. Real live admission, activation, fault gates and
recovery still need complete coordinator wiring.

`ActivateNetwork` loads the durable CREATE receipt, admits live ownership, reads
the current inactive policy and sends a conditional JSON Patch. Tests bind UID,
current resourceVersion and the complete inactive spec before replacing only the
owner selector. It rechecks the retained receipt and live admission before PATCH,
and validates the active response. The caller must pass the original fault origin
and a context deadline no later than origin + 30 seconds. It does not create a new
budget or retry conflicting writes. API acknowledgement is not Cilium enforcement;
the coordinator must still verify drops and all remaining gates on that same clock.
An ambiguous PATCH error requires recovery after joining workers, never assuming
the policy remained inactive. Fake-client tests apply the actual JSON Patch and
reject stale UID/resourceVersion/spec, missing receipt, lost ownership, reset
budget and invalid responses; these are not real API/Cilium acceptance evidence.

`RemoveNetworkPolicy` executes the API-removal portion of post-join recovery with
an independent bounded context. It requires the durable actual CREATE receipt,
rechecks it and live admission before deletion, and accepts only the exact
approved active or inactive spec with the recorded UID. DELETE carries both UID
and the freshly observed resourceVersion; conflicts and ambiguous errors are not
retried. A subsequent GET must report NotFound, with admission checked again.
An already absent object also requires receipt and admission checks. Missing
receipts fail closed, including the unresolved CREATE-to-save crash window.
Fake-client regressions cover both selectors, absence, replacement, changed spec,
conflict, lingering objects, DELETE NotFound, lost ownership/receipt and cancellation.
These tests inspect request preconditions, not server-side enforcement. API absence
does not establish Cilium withdrawal: the complete coordinator must join workers
before calling this function, then verify dataplane recovery before restoring
protocol state and finally the Pod label. That wiring remains incomplete.
