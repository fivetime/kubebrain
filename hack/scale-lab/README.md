# KubeBrain KWOK Scale Lab

Reproducible harness for standing up a very-large single Kubernetes cluster
(KWOK fake nodes + real control plane) on top of KubeBrain, and driving the
load/latency/failover tests from the 33M-key campaign.

Validated shape: **33,431,344 keys · 8.8M deployments = replicasets = pods (all
Running) · 100k nodes**, on three machines. Full write-up: `../../docs/scale-lab-10m-report.md`.

## Layout

```
hack/scale-lab/
  setup.sh              one-command bring-up (phases: storage|kubebrain|kwok|controlplane|tools)
  lab.env.example       machine + tuning config — copy to lab.env and edit
  config/               tiup TiKV topology, KWOK stage config, TiDB scale-out
  loadgen/              client-go object generator (own go.mod): nodes/workload/derive/runpods/storm/cleanup/status
  probes/               etcd-clientv3 probes (built by the main module)
    qlat/               write latency: bare PUT + contended CAS (single vs 3-replica quorum, #53)
    elogprobe/          watch history beyond the ring served from the event log (#45/#52)
    bulk/               bulk key writer for raw keyspace fill
    foload/             failover-under-load: continuous puts, per-3s gap detection (#46)
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
binary on the CTRL host, and Go on the machine you run `setup.sh` from.

Re-run any single phase after a reboot, e.g. `./setup.sh kubebrain`. Teardown:
`./setup.sh teardown` (leaves storage; destroy it explicitly with
`tiup cluster destroy <cluster>`).

## Running tests

`./setup.sh tools` builds everything into `bin/`:

```bash
# Fill: 1000 fake nodes, then 100 ns × 10 deploy × 10 replicas (pods land Running via KWOK)
bin/loadgen -kubeconfig ~/.kube-lab/config -mode nodes    -count 1000
bin/loadgen -kubeconfig ~/.kube-lab/config -mode workload -ns 100 -deploy 10 -replicas 10 -qps 800

# Direct-inject RS+pods for uncovered deployments (fast path past kcm), idempotent with -count 0
bin/loadgen -kubeconfig ~/.kube-lab/config -mode derive   -count 0

# Storage-level probes (point at the KubeBrain leader's client port)
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
