# KubeBrain Resource-Coverage Functional Test

Proves KubeBrain, as the etcd-v3 backend, correctly stores and serves the **full
breadth of Kubernetes API resources** — not just the pods/deployments/replicasets/
services/secrets/namespaces/nodes the scale campaign exercised. Runs on a single
host with an **embedded badger backend** (no TiKV/PD cluster needed).

## Run

```bash
cd hack/scale-lab/restest
./single-node-stack.sh up          # build badger KubeBrain + k3s + KWOK on this host
KUBECONFIG=/root/.kube-restest ./resource-coverage.sh
./single-node-stack.sh down        # tear down + wipe
```

Prereqs on PATH: `go`, the `k3s` and `kubectl` binaries, the `kwok` binary.

## What it checks

- **31 resource types across 7 categories** (Workloads, Service Discovery,
  Storage, Config/Metadata, RBAC, Cluster/Node, Extensibility) — create + persist
  through KubeBrain.
- **CRD custom-resource instance** — a runtime-registered type stored/served.
- **Full controller loop** — Deployment/StatefulSet/DaemonSet pods reach Running
  via controller → KubeBrain → scheduler → KWOK → controller-watch.
- **Read-back correctness** (Secret base64, ConfigMap), **watch** delivering
  add/update/delete, **rolling update**, **delete → NotFound**.

Exit code 0 and "ALL PASS" on success; non-zero listing the failed checks otherwise.

## How the single-node stack works

- **KubeBrain backend is chosen at build time by tag**: `go build -tags badger`
  gives the embedded persistent backend (default/no tag = tikv). Only `--data-dir`
  is needed — no PD/TiKV.
- **k3s** runs control-plane-only (`--disable-agent`) with
  `--datastore-endpoint=http://127.0.0.1:3379` pointing straight at KubeBrain
  (k3s treats the http endpoint as etcd).
- **KWOK** provides fake Ready nodes (label `type=kwok`); it needs an explicit
  `--config` stages file (reused from `../config/kwok-stages-fast.yaml`).

Notes: pod tolerations use `operator: Exists` so they tolerate the KWOK node
taint by key. A leftover campaign apiserver on `:6444` will block k3s — the
setup script disables such units first.
