# KubeBrain KWOK Scale Lab

This directory is the authoritative scale-lab source. The retired local
`/root/kwok-scale-lab` directory is not a supported tool or configuration path.
See [migration notes](docs/local-tools-migration.md) for the consolidation audit.

Reproducible harness for standing up a very-large single Kubernetes cluster
(KWOK fake nodes + real control plane) on top of KubeBrain, and driving the
load/latency/failover tests from the 33M-key campaign.

Validated shape: **33,431,344 keys · 8.8M deployments = replicasets = pods (all
Running) · 100k nodes**, on three machines. Full write-up: `../../docs/scale-lab-10m-report.md`.

## Layout

```
hack/scale-lab/
  setup.sh              one-command bring-up (phases: storage|kubebrain|kwok|controlplane|tools)
  build-tools.sh        local-only build; no lab.env, SSH, or cluster access
  test-tools.sh         local race tests and vet (including nested modules)
  lab.env.example       machine + tuning config — copy to lab.env and edit
  config/               tiup TiKV topology, KWOK stage config, TiDB scale-out
  loadgen/              client-go object generator (own go.mod): nodes/workload/derive/runpods/storm/cleanup/status
  probes/               etcd-clientv3 probes (built by the main module)
    qlat/               write latency: bare PUT + contended CAS (single vs 3-replica quorum, #53)
    elogprobe/          watch history beyond the ring served from the event log (#45/#52)
    bulk/               bulk key writer for raw keyspace fill
    foload/             failover-under-load: continuous puts, per-3s gap detection (#46)
    slowwatch/          slow etcd watch consumer; observed ordering, not gap-free proof
    watchflood/         concurrent Kubernetes pod/node list+watch load
  docs/methodology.md   the full design guide (architecture, walls, staging)
  restest/              single-node functional test: full k8s resource-type coverage (badger backend, no TiKV)
```

## Quick start

```bash
cd hack/scale-lab
cp lab.env.example lab.env && $EDITOR lab.env    # set the three host IPs
./setup.sh all                                    # storage -> kubebrain -> controlplane -> kwok -> tools
./setup.sh status
```

Prerequisites the script checks for (install once): `tiup` (TiKV deploy), `kwok`
binary on the CTRL host, and Go 1.26.8 or newer on the machine you run
`setup.sh` from. Build phases fail before writing a binary when the host Go
toolchain is older than the current standard-library security baseline.

Re-run any single phase after a reboot, e.g. `./setup.sh kubebrain`. Teardown:
`./setup.sh teardown` (leaves storage; destroy it explicitly with
`tiup cluster destroy <cluster>`).

## Running tests

Build and test locally without deployment configuration:

```bash
bash hack/scale-lab/build-tools.sh
bash hack/scale-lab/test-tools.sh
```

`setup.sh tools` delegates to the same build entrypoint without loading `lab.env`.
Build output defaults to ignored `hack/scale-lab/bin/`; override with an absolute
`SCALE_LAB_BIN_DIR`. The two imported probes use the root module's dependencies,
not copies of the old standalone go.mod/go.sum or prebuilt binaries.

Read-only watch diagnostics (they still consume cluster resources; use a test cluster):

```bash
hack/scale-lab/bin/slowwatch -ep https://TEST-ENDPOINT:3379 \
  -prefix /dedicated-test-prefix/ -cacert /path/ca.crt \
  -cert /path/client.crt -key /path/client.key -duration 3m -delay 50ms
hack/scale-lab/bin/watchflood -allow-load -kubeconfig /path/test.conf \
  -context TEST-CONTEXT -namespace TEST-NAMESPACE -watchers 10 -node-watchers 0 -duration 5m
```

`slowwatch` requires concurrent writes from a separately controlled workload;
zero events fails by default. Equal revisions within a transaction are valid.
It detects observed revision regression, cancellation and early channel closure;
it **cannot prove no lost events**, because other prefixes can consume revisions.
`watchflood` requires an explicit kubeconfig/context and pod namespace (or explicit
`-all-namespaces`); node watches are always cluster-scoped. Even pod watchers
follow all list pages; odd pod watchers and node watchers take only the first page
to obtain a watch revision. Error events are failures, bookmarks are separate
from data events, reconnects re-list and may miss history: this is load diagnostics,
not a continuity test. Workers stop before the final counters are printed.

Do not run `setup.sh all` against the existing DBaaS test environment: it deploys
its own storage and control plane. Adapt endpoints/configuration deliberately;
local tests above do not perform a Kubernetes or TiKV deployment.

`./setup.sh tools` builds everything into `bin/`:

```bash
# Fill: 1000 fake nodes, then 100 ns × 10 deploy × 10 replicas (pods land Running via KWOK)
bin/loadgen -kubeconfig ~/.kube-lab/config -mode nodes    -count 1000
bin/loadgen -kubeconfig ~/.kube-lab/config -mode workload -ns 100 -deploy 10 -replicas 10 -qps 800

# Direct-inject RS+pods for uncovered deployments (fast path past kcm), idempotent with -count 0
bin/loadgen -kubeconfig ~/.kube-lab/config -mode derive   -count 0

# Storage-level probes (point at the KubeBrain leader's client port)
bin/bigstream -endpoint <brain>:3379 -mode stream -prefix /registry/ # streamed large-range read
bin/qlat      -endpoint <brain>:3379          # write-latency characterization
bin/elogprobe -endpoint <brain>:3379          # event-log replay past the ring
bin/foload    -endpoint <brain>:3379          # kill the leader mid-run; watch the gap
```

## Non-negotiable rules (learned the hard way)

- **The KubeBrain leader must not co-locate with TiKV.** Its ordered collector
  crawls under storage contention and every write falls to the 3s backstop.
  `lab.env` keeps the leader off `STORE_HOST`.
- **Bare PD+TiKV has no GC safepoint pusher** → MVCC versions pile up forever and
  reads rot. KubeBrain's leader self-pushes the safepoint (PR #16); no TiDB
  instance is needed. If you see `gc_safe_point=0`, the leader isn't running.
- **Pin static IPs.** DHCP drifted lab hosts across reboots and broke every
  `10.224.0.1x` reference. Give each host a static netplan address.
- **At 10M+ single resources**, turn the apiserver's giant caches OFF and let it
  page straight through KubeBrain (`--watch-cache-sizes=<res>#0`,
  `WatchListClient=false`, `SizeBasedListCostEstimate=false`). The exact flag set
  is in `../../docs/scale-lab-10m-report.md`.

See `docs/methodology.md` for the full architecture, the thirteen k8s scale
walls, and the staged bring-up plan.
