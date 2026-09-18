# Read-only info listener diagnostic probe

Build with `go build ./hack/production/cmd/info-diagnostic-probe` (use `-o` to
place the binary in a private experiment directory, not the repository).

The caller must establish a loopback tunnel to the exact admitted Pod and retain
its namespace/StatefulSet/Pod UID, image, full spec and container state before and
after the probe. This command does **not** prove Pod identity or fault acceptance.
CA material, client keys, stack dumps and experiment receipts stay outside Git.

Required flags: `--endpoint https://127.0.0.1:PORT`, `--server-name INFO_DNS`,
`--server-spki-sha256 HEX`, `--cacert CA_FILE`, `--cert CLIENT_CERT`,
`--key CLIENT_KEY`, and `--mode protected|protected-stack|disabled`.

- `protected` requires `--stack-output NEW_FILE` and `--anonymous-endpoint
  https://127.0.0.1:OTHER_PORT`, a separate tunnel to the **same** admitted Pod.
  A negative TLS alert can terminate kubectl's entire port-forward process;
  isolating that check protects the authenticated tunnel without retrying it.
  Both tunnels must be independently started, bound and cleaned up by the caller.
  Checks authenticated `/ping`
  and `/ready`, rejects anonymous profile access at TLS, then captures authenticated
  `/debug/pprof/goroutine?debug=2`. The missing-certificate test requires normal
  server chain/name/SPKI verification, an observed client-certificate request,
  and a remote TLS alert after an empty certificate. Timeout, EOF, refusal, HTTP
  error text, untrusted server and bad pin do not satisfy this check.
- `protected-stack` is exclusively for fault-time observation when the admitted
  member may be unready. It has the same stack output, independent tunnel, TLS,
  `/ping`, anonymous rejection and bounded capture requirements as `protected`,
  but does not call `/ready`. Its summary explicitly reports
  `readiness_checked=false`. Do not use this mode for rollout admission or
  restoration verification: those continue to require `/ready`. The caller must
  bound the entire capture by the remaining original fault deadline; the probe's
  25-second ceiling does not grant a new acceptance window.
- `disabled` requires **no** stack-output and expects profile 404 for both the
  authenticated-capable and anonymous clients. This matches the original test
  baseline with pprof off and no info client-auth requirement, not every possible
  production configuration with pprof off.

Only GET is used. HTTP/1 with fresh connections, no proxy, redirects, compression
or retries; each request is bounded to 5 seconds, the full run to 25 seconds.
Health/error responses are limited to 4 KiB; stack responses to 8 MiB, with an
extra byte read to detect overflow. HTTP framing/read errors, unexpected status,
content encoding, missing `goroutine ` prefix or terminal newline fail closed.
A syntactically complete-looking partial stack cannot be detected by these
checks alone; source-bound frame classification and RPC identity binding remain
the fault runner's responsibility.

Stack output is exclusive-create, mode 0600, and never overwrites an existing
file or symlink. A partial write/close failure returns nonzero and must not be
treated as accepted evidence. Successful stdout JSON contains timestamps, stack
length/hash, `readiness_checked`, and explicit
`pod_identity_proven=false` / `fault_acceptance_proven=false`.
The caller must also retain stderr and process exit status on failures.

The real TLS socket tests cover TLS 1.2/1.3 with a verifying client CA, disabled
profiles, missing client-auth, forged alert text in HTTP headers, redirects,
oversized/incomplete bodies, wrong server identity, cancellation and private
non-overwriting output. Product listener behavior is separately covered by
`pkg/endpoint/TestInfoDiagnosticListenerRequiresMTLSAndExplicitPprof`.
