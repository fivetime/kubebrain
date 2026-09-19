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

`VerifyOriginalOutcome` connects this log validator to the original post-response
lease/key gate using authenticated healthy-member gRPC supplied by the caller.
Both reads and the final admission check must finish under a context ending no
later than the original origin + 30 seconds; a recovery context is not accepted.
Zero response TTL requires absent lease and key. Positive response TTL requires
the retained grant and the sole owned key with value `fixture` and matching lease;
natural expiry between response and observation remains permitted while the grant
and attachment persist. Foreign attachments fail the isolated-fixture check.
Headers must match the admitted cluster and observed successor term or later.
The original probe's successful exit, ongoing isolation, drops, successor, stack
and metric gates remain independent obligations; this does not declare acceptance.

The result retains partial RPC observations even on failure for caller-owned
private evidence. Simulated-client tests cover both outcomes, expiry, full-width
IDs, identity/key/value mismatches, lost final admission and exhausted/extended
budgets, including RPC timeout. They do not prove a real leadership handoff.

`RunFaultLifecycle` now requires `OriginalEvidence` and `OutcomeAdmit` and calls
this gate immediately after the prepared child exits successfully, before metric
completion checks and recovery. The independently supplied evidence origin must
exactly equal the clock dispatched to the child and workers. Failed child execution
does not read original evidence. Partial outcome observations are returned in
`LifecycleResult.Outcome`; failures remain execution failures even after recovery
succeeds. Wiring tests use a fixed-term real KubeBrain service with synthetic probe
and successor evidence, covering untrusted evidence, clock changes, incompatible
lease outcome and evidence collection timing out on the original fault context.
They verify ordering/budget propagation, not a real term change or isolated stream.

## Protocol recovery intent

`PrepareFault` composes the preparation half: validate shared owner and cluster
bindings, durably reserve an inactive policy, label the exact admitted Pod, then
prepare the protocol fixture. Before protocol setup and each protocol write it
rechecks live Pod/controller identity, the actual reservation UID and inactive
spec, ownership, nonce safety and independent dataplane readiness. It does not
activate the policy or start a fault clock. Required hooks must prove exclusive
ownership, absence of stale/foreign selected endpoints, target identity convergence
and inactive-policy connectivity; callbacks returning nil are not evidence.

Preparation errors never trigger a write retry or imply that nothing changed.
In particular, a successful CREATE with a lost response leaves intent without an
actual receipt and requires explicit reconciliation, not adoption by name. The
composition tests use real KubeBrain/memkv gRPC and a fake Kubernetes tracker,
covering successful preparation/recovery, owner and nonce rejection, replaced
reservation, unready dataplane, ambiguous CREATE, and admission lost after lease
grant. The latter restores the partial protocol fixture and owned label. These
tests do not establish Cilium behavior, transport security, TiKV operation or
leadership handoff. The CLI, admitted concrete hooks, original probe and fault
gates still need integration before any new live experiment.

`RunFaultLifecycle` now connects `PrepareFault`, the original prepared-child
session, the metric-worker supervisor and `RecoverFault`. It validates required
configuration and private child logs before preparation. A failed attempt is not
retried; after both supervisors return and join their children, recovery uses a
separate context bounded to at most five minutes, even if the caller was canceled.
The external Join hook must additionally reject escaped/unmanaged workers before
any recovery API/RPC work. Execution and recovery errors remain separately visible;
successful cleanup cannot convert failed fault gates into acceptance. Ownership
is never released by this entry point.

Real Bash children plus real KubeBrain/memkv RPCs and a simulated Kubernetes API
exercise success, rejected metric baseline, child failure, original deadline,
parent cancellation and failed external join. Tests verify identical fault/worker
clocks and direct-child reaping before recovery. These synthetic scripts do not
inject a network fault or validate real metrics. This remains a library entry:
admitted concrete scripts, evidence validators, ownership and observer hooks and
a deployable CLI are still required. The composition exposed and fixed a duplicate
timer race in the metric supervisor: workers now cancel from the single deadline
context, preserving `DeadlineExceeded` in the prepared fault callback.

The concrete test-cluster observer
`deploy/test-cluster/observe-local-network-restored.sh` can provide the network
portion of `ReservedReady` using mode `absent`, after Go verifies the recorded
policy still has its inactive selector. It first waits for the *present* Pod label
to appear in the independently bound Cilium identity, then checks policy absence,
all six admitted backend TCP endpoints, and policy absence again. Recovery uses
the same mode while the owned label remains; after label removal it uses
`absent-unlabelled`, which checks the *absent* identity transition first. Exit 75
remains pending under the existing bounded context; other errors stop the stage.
Neither mode proves the Kubernetes policy object's state, protocol health or
exclusive ownership. Those independent Go checks and lifecycle hooks remain
mandatory. This shared observer is not a standalone experiment driver.

`NetworkObserver.Prepared`, `.Restored` and `.Unlabelled` adapt that script to
the three Go lifecycle hooks. Every pending observation rechecks API identities,
label phase, unchanged actual reservation receipt, exact independently admitted
Pod JSON and the independent targets digest. Inputs must be bounded private
regular files named `observer-pod.json` and `observer-targets.json` in the owner
directory. A successful observation is followed by the same admission checks.
Only the recovery hook chooses between owned and absent label phases, using a
fresh Pod read; it rejects foreign labels or a phase change during observation.

The required Admit callback must enforce exclusive ownership and authenticate the
whole script/dependency bundle, explicit environment and kubeconfig/cluster binding
to the Go client. The required Retain callback must durably keep each output/status;
retention failure cannot become success. Neither callback has a permissive default.
Tests connect these adapters to protocol recovery with real KubeBrain RPCs, fake
Kubernetes and a boundary-fixture shell, covering pending, changed inputs before
and during observation, lost ownership and failed retention. They prove wiring
and failure propagation, not real Cilium/TCP observations or script authentication.

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
fields and identity mismatches. A bad/missing record cannot authorize protocol
writes or justify skipping cleanup. `RecoverFault` below handles missing intent
only with independent read-only absence checks; bad records remain fatal.
The directory and admission must remain trusted;
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

`TestNetworkLifecycleHTTP` joins ReserveNetwork, ActivateNetwork and
RemoveNetworkPolicy through the actual dynamic client's HTTP transport. The
fixture applies the JSON Patch, advances the resourceVersion past uint64 maximum,
and requires DELETE to carry that fresh string plus the original CREATE UID.
Both normal activation and an applied PATCH returning HTTP 500 recover with an
independent context after cancellation; a repeat absence check sends no DELETE.
The original receipt remains intact. This verifies component composition and
ambiguous-response handling, not real API admission, Cilium withdrawal, worker
joining, protocol/label restoration or the complete 30-second acceptance gate.

`CheckNetworkIdentity` supplies the live API-identity part of admission callbacks.
It reads the admitted Namespace, StatefulSet and Pod, checks exact UIDs and
non-deleting objects, requires the Pod's controller reference to identify that
StatefulSet, and enforces an explicit unlabelled/owned/recovery label phase. A
complete namespace Pod selection for both fault nonces must contain no reserved
nonce match and at most the admitted active Pod at the same resourceVersion.
Pagination, changed observations and API errors fail closed. It checks externally
held lifecycle ownership before and after reads. This does not acquire a lock or
make sequential reads atomic, and Pod-list absence does not prove absence of stale
Cilium endpoints. Callers still need phase-specific worker-join, endpoint and
dataplane checks. Fake-client tests cover identities, controller relationship,
label phases/collisions, incomplete lists, changed versions and lost ownership;
the complete coordinator is not yet wired to this helper.

`PrepareNetworkLabel` and `RestoreNetworkLabel` connect live identity admission to
conditional Pod PATCH requests. Both retain the actual reservation receipt and
recheck it before writes. Preparation requires the exact inactive reservation and
no other namespace CNP referencing the active nonce; it adds only the owner label
while preserving other labels, guarded by Pod UID/resourceVersion tests. Restore
requires a complete policy list with neither the recorded policy nor any other
CNP referencing that nonce, then tests UID/version/owner and removes only the
owner label. Even an already absent label requires admission and policy checks.
Both verify the final label state with fresh identity reads, without write retries.

The admission callback must hold exclusive ownership. Preparation additionally
requires stale Cilium endpoint checks; restoration requires joined workers and
completed dataplane/protocol recovery. These conditions are caller obligations,
not inferred from a successful API request. Label absence does not prove Cilium
identity convergence. Fake-client lifecycle tests apply the actual JSON Patch,
exercise absent label maps, preserve unrelated labels, reject concurrent versions,
wrong/active/missing reservation, policy remnants/references, pagination, foreign
labels and unconfirmed protocol recovery. The complete cluster coordinator and
original 30-second real fault acceptance remain outstanding.

`RecoverFault` now composes the recovery half: join workers, bind both journals to
the same owner/namespace/StatefulSet, remove the recorded policy, independently
verify network withdrawal, restore protocol state, remove the owned Pod label,
verify identity convergence, and perform final protocol/live identity checks.
It uses one independent bounded recovery context, not a reset fault deadline.
Network observation and ownership are repeated before protocol mutations; protocol
verification is repeated before label mutation. A failed stage prevents subsequent
stages, leaves all journals intact, and does not release lifecycle ownership.

If the protocol journal is absent, a valid actual network reservation receipt
still permits exact-policy removal and independent network withdrawal checks.
This path never performs protocol writes: fresh read-only checks must prove the
alarm, lease and fixture key absent before removing the owned Pod label. Missing
intent alone is not proof that preparation never ran. Live protocol state,
uncertain reads, a malformed journal or a journal appearing during this recovery
remain reconciliation errors. Tests cover absent-and-empty, absent-but-live,
newly appearing and malformed journals; none authorize protocol mutations.

The caller must supply real Join/Own/NetworkRestored/IdentityRestored hooks and an
authenticated recovery connection. These hooks are required evidence sources,
not optional success defaults. Stage tests use fake Kubernetes and protocol clients
to check ordering, exact conditional writes, failed joins, cross-owner plans,
withdrawal/protocol failures and unconverged identity. They do not establish real
worker teardown or Cilium recovery. This entry point does not yet supply the full
preparation/fault-gate controller or prove the original 30-second acceptance.

At source `6af298e5`, local race regression passed for `leasefault` (6.750s),
`metricsworker` (10.787s), and `build` (2.583s). The complete
`go test -race -count=1 -timeout=2m ./deploy/test-cluster` also passed (97.143s)
without increasing its timeout. These are local results, not CI/image admission
or a live fault outcome. Source inspection still finds preparation/recovery and
prepared-child/metrics composition in tests only, with no complete runnable fault
coordinator. Integration must supply exclusive ownership, admitted authenticated
clients and observer bindings, preserve the original probe and single fault clock,
then join all workers before independent recovery. A missing CREATE receipt remains
an explicit reconciliation case, not permission to adopt a policy by name.

`RunRecoveryObserver` provides a bounded subprocess adapter for independently
admitted read-only observation scripts. It uses the caller's existing recovery
deadline, explicit environment, process-group cancellation and bounded combined
output. Exit 75 returns `ErrObservationPending`; all other nonzero exits are fatal,
and cancellation/output-limit errors cannot become pending. It performs no retries
and does not infer packet connectivity or acceptance from exit 0. Tests cover
matched/pending/fatal states, explicit environment isolation, output overflow and
deadline cancellation with direct-child reaping. Callers must still bind scripts
and arguments to admitted identities, persist output privately, and account for
descendants that escape the process group. This adapter does not itself connect
the cluster-specific hooks or execute a fault experiment.

`WaitRecoveryObserver` can be used inside a recovery-stage observation hook. It
rechecks caller admission before each read-only command, retains every attempt's
output/status through a required evidence callback, and waits 200 ms only for the
exact typed pending result. Fatal errors, lost admission, evidence retention
failure and cancellation stop the stage; even a matched observation is rejected
if its evidence cannot be retained. Every attempt uses the same original recovery
context and copied arguments/environment. Tests cover pending-to-matched, fatal,
ownership loss between attempts, evidence failure after pending or matched,
cancellation and unchanged deadlines. This is not a mutation retry facility or
a new fault acceptance clock. Admission/retention callbacks must be bounded and
the retention implementation must preserve private evidence durably; concrete
cluster hook configuration remains the complete coordinator's responsibility.

`PrepareProtocol` supplies the protocol fixture preparation: fresh admission,
no alarm/fixture key/lease and an empty global lease set, durable create-once intent,
explicit-ID 10-second lease grant, version-zero conditional transaction putting
`fixture`, then exact-member CORRUPT activation. Each write rechecks intent and
admission; uncertain outcomes stop without retries. The caller must exclusively
own the dedicated test instance and unused lease-ID allocation and verify inactive
network reservation. It does not wait for expiry, launch the original probe or
start the fault clock. Transaction identity comes from the outer header; etcd's
inner Put header supplies only revision, which must match the outer revision.

Actual KubeBrain gRPC/memkv tests prepare then restore the fixture, refuse repeated
preparation, preserve a competing key written before the conditional transaction,
and perform no write when admission is lost before grant. Conflicting external
keys also prevent automatic reconciliation rather than being deleted. These tests
do not prove real TiKV, Kubernetes admission, election or network fault behavior.
