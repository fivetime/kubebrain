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

Local tests use bufconn and synthetic services, not real PD/TiKV or TLS. Production
CI runs vet and race tests; the real fault experiment remains a separate gate.
