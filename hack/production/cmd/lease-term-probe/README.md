# Single-stream expired lease probe

This is one component of a dedicated-cluster fault experiment, **not a complete
acceptance runner**. It issues one raw gRPC KeepAlive message for an existing
owned lease, without clientv3 retry/reconnect behavior. It does not create or
revoke leases, arm/disarm alarms, inject faults, or restore network policies.
The caller must provide those operations and an independent cleanup path.

Required flags:

```text
--endpoint=<direct-member-host:3379>
--cacert=<ca-file> --cert=<client-cert> --key=<client-key>
--tls-server-name=<verified-server-name>
--cluster-id=<decimal-cluster-id> --member-id=<decimal-current-leader-id>
--lease-id=<decimal-owned-lease-id> --leased-key=<owned-attached-key>
--duration=2m
```

The endpoint must resolve to the intended member, not a load-balanced service.
TLS verification is mandatory. The probe checks cluster/member/leader identity,
then requires a negative TTL, positive granted TTL and the expected attachment.
Status and TTL must report the same member and nonzero RaftTerm. This detects
observed preflight leadership changes, not an atomic fence against a change after
preflight; the experiment still needs independent leader/term observations.
It permits only one TCP dial attempt and disables configured gRPC retries.
A connection or stream failure is a failed experiment, not transparently retried.
Deadline bounds the entire probe including preflight; SIGTERM cancels it.

JSON lines report `expired_preflight`, `request_sent`, and `response` with UTC
timestamps. Treat nonzero exit or missing final response as failure. `request_sent`
only means client Send succeeded, **not that the server entered expired wait**.
The caller must reject a response before confirmed term loss, bracket Pod UID,
container identity and leader term, verify fault enforcement/recovery, and compare
against the unfixed baseline. A positive response may come from successor lease
recovery; TTL=0 needs independent durable deletion validation. Neither response
alone establishes the precise internal race or the original availability gates.

The tool never prints certificate/key contents. Its output does contain lease
and member identifiers; retain it with the experiment evidence. No sample command
here authorizes fault injection against an arbitrary endpoint.

With explicit `--wait-for-expiry`, the probe polls TTL every 200ms until the
retained owned lease becomes negative, using the same connection and original
whole-probe deadline. It never retries an RPC error and never renews during the
wait. Every sample must retain the initial cluster/member/term, positive unchanged
granted TTL, and exactly the single owned key; disappearance, foreign attachments
or identity drift fail immediately. Both modes now require that sole attachment.
Default mode still rejects an unexpired lease immediately.

Polling emits no additional stdout events: only after expiry does the existing
three-event original-stream protocol begin, with one renewal request. The parent
must still independently prove the original request is blocked before allowing
the fault clock and activation. The expiry wait is preparation, not a fresh
30-second fault window or fault acceptance. CLI mTLS tests verify that polling
and the renewal use one TCP connection; these remain synthetic RPC fixtures.

Local tests use synthetic services over bufconn and loopback TCP/mTLS. The latter
exercise the CLI, mandatory client certificates, rejection of a wrong server
name, and connection loss without a replacement connection. They do not use real
PD/TiKV. Production CI runs vet and race tests; the real fault experiment remains
a separate gate.
