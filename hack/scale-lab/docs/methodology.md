# KWOK 超大规模单集群压测落地指南

> 目标：构建一个**真实 Kubernetes/k3s 控制平面 + KWOK 模拟节点/Pod 状态**的超大规模单集群，用于测试控制面、调度器、控制器、etcd、API Server、watch/list 客户端、监控系统、用于测试在大量 Node / Pod / Deployment 场景下的表现。

---

## 1. 核心结论

本方案适合以下目标：

- 在**同一个 Kubernetes 集群**中模拟大量 Node。
- 创建大量 Namespace / Deployment / ReplicaSet / Pod。
- 不需要真实 kubelet。
- 不需要真实容器运行。
- 不需要真实业务流量。
- 重点压测控制平面和 Kubernetes 对象规模。

本方案不适合以下目标：

- 压真实 kubelet。
- 压 containerd。
- 压镜像拉取。
- 压 CNI 网络。
- 压 Service / CoreDNS / Ingress 真实流量。
- 压 Pod 内应用 QPS。
- 压磁盘 IO / conntrack / iptables 真实规则。

一句话：

```text
KWOK 是“假 worker”，不是“假 Kubernetes”。
控制平面必须是真的，而且控制平面正是主要压测对象。
```

---

## 2. 推荐总体架构

```text
                ┌──────────────────────────────┐
                │        LB / VIP : 6443        │
                └───────────────┬──────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        │                       │                       │
┌───────▼────────┐      ┌───────▼────────┐      ┌───────▼────────┐
│ cp-1            │      │ cp-2            │      │ cp-3            │
│ kube-apiserver  │      │ kube-apiserver  │      │ kube-apiserver  │
│ scheduler       │      │ scheduler       │      │ scheduler       │
│ controller-mgr  │      │ controller-mgr  │      │ controller-mgr  │
│ etcd            │      │ etcd            │      │ etcd            │
└────────────────┘      └────────────────┘      └────────────────┘

┌────────────────┐      ┌────────────────┐      ┌────────────────┐
│ kwok-worker-1   │      │ kwok-worker-2   │      │ kwok-worker-3   │
│ kwok controller │      │ kwok controller │      │ kwok controller │
└────────────────┘      └────────────────┘      └────────────────┘

┌────────────────┐      ┌────────────────┐
│ loadgen-1       │      │ loadgen-2       │
│ apply/scale     │      │ watch/list      │
└────────────────┘      └────────────────┘

┌────────────────┐
│ obs-1           │
│ Prom/Grafana    │
└────────────────┘
```

真实机器分为四类：

```text
control_plane:
  真实 kube-apiserver / etcd / scheduler / controller-manager

kwok_workers:
  少量真实 worker，用来运行 KWOK controller 和辅助组件

loadgen:
  负责批量创建、删除、扩缩容、watch、list

observability:
  Prometheus / Grafana / 日志 / 指标采集
```

fake Node 不在 Ansible inventory 中。

fake Node 不是 SSH 目标，不是宿主机上的进程，而是 Kubernetes API 对象。

---

## 3. 工具分工

| 层级 | 工具 | 作用 |
|---|---|---|
| 真实宿主机 | Ansible | 初始化系统、安装依赖、配置内核参数、部署真实控制平面 |
| Kubernetes 控制平面 | kubeadm / k3s / rke2 | 提供真实 apiserver、etcd、scheduler、controller-manager |
| 模拟节点 | KWOK | 模拟 Node Ready、Pod Running、Pod Ready 等状态 |
| 海量对象创建 | kubectl / client-go / YAML generator | 创建 Node、Namespace、Deployment、Pod |
| 压测客户端 | loadgen scripts / Go tools | apply、scale、watch、list、delete |
| 监控 | Prometheus / Grafana | 采集控制面、etcd、scheduler、controller-manager、KWOK 指标 |
| 操作入口 | Makefile / Taskfile | 固化执行流程，避免手工操作混乱 |

重要原则：

```text
Ansible 管真实世界。
KWOK 管假节点。
kubectl/client-go 管对象洪水。
不要让 Ansible 管 fake Node。
```

---

## 4. 推荐仓库结构

```text
kwok-scale-lab/
├── inventory/
│   ├── hosts.ini
│   └── group_vars/
│       ├── all.yml
│       ├── control_plane.yml
│       ├── kwok_workers.yml
│       ├── loadgen.yml
│       └── observability.yml
│
├── playbooks/
│   ├── 00-host-tune.yml
│   ├── 01-install-k8s-control-plane.yml
│   ├── 02-install-real-workers.yml
│   ├── 03-prepare-loadgen.yml
│   ├── 04-prepare-observability.yml
│   └── 99-reset.yml
│
├── k8s/
│   ├── kwok/
│   │   ├── namespace.yaml
│   │   ├── kwok-controller.yaml
│   │   ├── kwok-stages.yaml
│   │   └── kustomization.yaml
│   │
│   ├── monitoring/
│   │   ├── prometheus-values.yaml
│   │   ├── grafana-dashboards/
│   │   └── kustomization.yaml
│   │
│   └── manifests/
│       ├── generated/
│       │   ├── nodes/
│       │   ├── namespaces/
│       │   └── deployments/
│       └── base/
│
├── generator/
│   ├── gen-nodes.py
│   ├── gen-namespaces.py
│   ├── gen-deployments.py
│   ├── gen-all.sh
│   └── config.yaml
│
├── loadgen/
│   ├── apply-nodes.sh
│   ├── apply-deployments.sh
│   ├── delete-deployments.sh
│   ├── watch-pods.go
│   ├── list-loop.go
│   └── scale-test.go
│
├── scripts/
│   ├── kubeconfig-sync.sh
│   ├── health-check.sh
│   ├── collect-metrics.sh
│   └── count-objects.sh
│
├── Makefile
└── README.md
```

---

## 5. Inventory 示例

```ini
# inventory/hosts.ini

[control_plane]
cp-1 ansible_host=10.0.0.11
cp-2 ansible_host=10.0.0.12
cp-3 ansible_host=10.0.0.13

[kwok_workers]
kwok-1 ansible_host=10.0.0.21
kwok-2 ansible_host=10.0.0.22
kwok-3 ansible_host=10.0.0.23

[loadgen]
loadgen-1 ansible_host=10.0.0.31
loadgen-2 ansible_host=10.0.0.32

[observability]
obs-1 ansible_host=10.0.0.41

[all:vars]
ansible_user=ubuntu
ansible_ssh_private_key_file=~/.ssh/kwok-scale-lab
```

注意：

```text
fake Node 不要写进 hosts.ini。
fake Node 不是机器。
fake Node 不需要 SSH。
fake Node 只存在于 Kubernetes API 中。
```

---

## 6. Makefile 示例

Makefile 应该成为唯一入口，避免手动命令散落。

```makefile
ANSIBLE_INVENTORY := inventory/hosts.ini
KUBECONFIG ?= ./kubeconfig

.PHONY: tune
tune:
	ansible-playbook -i $(ANSIBLE_INVENTORY) playbooks/00-host-tune.yml

.PHONY: control-plane
control-plane:
	ansible-playbook -i $(ANSIBLE_INVENTORY) playbooks/01-install-k8s-control-plane.yml

.PHONY: workers
workers:
	ansible-playbook -i $(ANSIBLE_INVENTORY) playbooks/02-install-real-workers.yml

.PHONY: loadgen
loadgen:
	ansible-playbook -i $(ANSIBLE_INVENTORY) playbooks/03-prepare-loadgen.yml

.PHONY: obs
obs:
	ansible-playbook -i $(ANSIBLE_INVENTORY) playbooks/04-prepare-observability.yml

.PHONY: kwok
kwok:
	KUBECONFIG=$(KUBECONFIG) kubectl apply -k k8s/kwok

.PHONY: gen
gen:
	cd generator && ./gen-all.sh

.PHONY: apply-nodes
apply-nodes:
	KUBECONFIG=$(KUBECONFIG) ./loadgen/apply-nodes.sh

.PHONY: apply-deployments
apply-deployments:
	KUBECONFIG=$(KUBECONFIG) ./loadgen/apply-deployments.sh

.PHONY: status
status:
	KUBECONFIG=$(KUBECONFIG) ./scripts/count-objects.sh
	KUBECONFIG=$(KUBECONFIG) ./scripts/health-check.sh

.PHONY: reset
reset:
	ansible-playbook -i $(ANSIBLE_INVENTORY) playbooks/99-reset.yml
```

推荐执行顺序：

```bash
make tune
make control-plane
make workers
make loadgen
make obs
make kwok
make gen
make apply-nodes
make apply-deployments
make status
```

---

## 7. 规模配置文件

不要把规模参数写死在脚本里。统一放到 `generator/config.yaml`。

```yaml
# generator/config.yaml

cluster:
  fake_nodes: 10000
  namespaces: 1000
  deployments_per_namespace: 10
  replicas_per_deployment: 10

node:
  name_prefix: kwok-node
  zones:
    - zone-a
    - zone-b
    - zone-c
  racks_per_zone: 20
  allocatable:
    cpu: "8"
    memory: "32Gi"
    pods: "110"

pod:
  image: nginx:latest
  cpu_request: "10m"
  memory_request: "32Mi"

deployment:
  name_prefix: app
  labels:
    workload-type: fake
```

对应规模：

```text
10,000 fake nodes
1,000 namespaces
10 deployments / namespace
10 replicas / deployment

总计：
10,000 deployments
100,000 pods
```

---

## 8. fake Node 设计

fake Node 是 Kubernetes Node 对象，不是真实机器。

示例：

```yaml
apiVersion: v1
kind: Node
metadata:
  name: kwok-node-000001
  labels:
    type: kwok
    topology.kubernetes.io/zone: zone-a
    lab/rack: rack-001
    kubernetes.io/hostname: kwok-node-000001
    node-role.kubernetes.io/kwok: "true"
  annotations:
    kwok.x-k8s.io/node: fake
spec:
  taints:
    - key: kwok.x-k8s.io/node
      value: fake
      effect: NoSchedule
status:
  capacity:
    cpu: "8"
    memory: 32Gi
    pods: "110"
  allocatable:
    cpu: "8"
    memory: 32Gi
    pods: "110"
  conditions:
    - type: Ready
      status: "True"
```

建议：

- fake Node 名字固定、可排序。
- fake Node 加 zone / rack / hostname 标签。
- 初期给 fake Node 加 taint，避免 workload 过早调度。
- 压测 Deployment 时再通过 toleration 放行。
- 不要把 fake Node 放进 Ansible inventory。

---

## 9. Deployment 设计

不要只创建一个 `replicas: 100000` 的 Deployment。

推荐分片：

```text
1000 namespaces
每个 namespace 10 deployments
每个 deployment 10 replicas

总计：
10000 deployments
100000 pods
```

这样更接近真实平台，也更能压测：

- Namespace 数量。
- Deployment controller。
- ReplicaSet controller。
- Scheduler。
- API Server watch/list。
- etcd 对象存储。
- 自研 Operator。
- 监控系统 cardinality。

Deployment 示例：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app-000001
  namespace: ns-0001
  labels:
    workload-type: fake
spec:
  replicas: 10
  selector:
    matchLabels:
      app: app-000001
  template:
    metadata:
      labels:
        app: app-000001
    spec:
      tolerations:
        - key: kwok.x-k8s.io/node
          operator: Equal
          value: fake
          effect: NoSchedule
      containers:
        - name: fake
          image: nginx:latest
          resources:
            requests:
              cpu: 10m
              memory: 32Mi
```

注意：

```text
Pod 不会真的运行 nginx。
Pod 不会产生真实网络流量。
但是 Deployment、ReplicaSet、Pod、调度、状态更新都是真实 API 对象流程。
```

---

## 10. 对象创建策略

第一版可以用 shell + kubectl，后续建议改成 Go/client-go。

### 10.1 apply nodes

```bash
#!/usr/bin/env bash
set -euo pipefail

kubectl apply --server-side -f k8s/manifests/generated/nodes/
```

### 10.2 apply namespaces

```bash
#!/usr/bin/env bash
set -euo pipefail

kubectl apply --server-side -f k8s/manifests/generated/namespaces/
```

### 10.3 apply deployments

```bash
#!/usr/bin/env bash
set -euo pipefail

find k8s/manifests/generated/deployments -name '*.yaml' \
  | sort \
  | xargs -n 1 -P 16 kubectl apply --server-side -f
```

### 10.4 为什么后期要换 client-go

超大规模下，kubectl 会带来额外噪声：

- 进程启动开销。
- YAML 解析开销。
- 大量短连接。
- apply diff/field manager 开销。
- 难以精准控制 QPS/burst。
- 难以记录每个请求的延迟分布。

后续建议用 Go/client-go：

- 控制 QPS / Burst。
- 并发创建对象。
- 分 namespace 分片。
- 记录 request latency。
- 记录错误码。
- 支持 create / patch / delete / watch / list 混合压测。

---

## 11. Loadgen 分片策略

不要让一台机器创建所有对象。

推荐按 namespace 分片：

```text
loadgen-1:
  ns-0000 ~ ns-0249

loadgen-2:
  ns-0250 ~ ns-0499

loadgen-3:
  ns-0500 ~ ns-0749

loadgen-4:
  ns-0750 ~ ns-0999
```

好处：

- 分摊客户端 CPU。
- 分摊 YAML/JSON 序列化开销。
- 分摊网络连接。
- 更接近真实多客户端场景。
- 更容易定位瓶颈是客户端还是控制面。

---

## 12. KWOK 部署建议

推荐使用 `KWOK in cluster` 模式：

```text
真实 Kubernetes/k3s 控制面
+
真实 worker 节点承载 KWOK controller
+
KWOK 负责模拟 Node / Pod 状态
```

KWOK controller 建议固定运行在 `kwok_workers` 上：

- 给真实 worker 打标签。
- 给真实 worker 加 taint。
- KWOK deployment 使用 nodeSelector / toleration。
- 不要和控制面、Prometheus、loadgen 混跑。

示例策略：

```yaml
nodeSelector:
  node-role.kubernetes.io/kwok-worker: "true"

tolerations:
  - key: dedicated
    operator: Equal
    value: kwok
    effect: NoSchedule
```

是否需要多个 KWOK controller 副本，要分阶段验证：

```text
阶段 1：1 个 KWOK controller
阶段 2：2~3 个 KWOK controller
阶段 3：根据 apiserver/etcd/KWOK 指标决定是否继续扩
```

注意：

```text
KWOK 副本不是越多越好。
副本太多可能增加 watch、patch、conflict retry，反而压垮 apiserver/etcd。
```

---

## 13. 控制平面部署建议

### 13.1 如果目标是 k3s 控制面极限

使用：

```text
3 台 k3s server
embedded etcd
外部 LB / VIP
禁用不必要组件
```

建议禁用：

```text
traefik
servicelb
local-storage
metrics-server（视情况）
```

示例参数：

```bash
server \
  --cluster-init \
  --disable=traefik \
  --disable=servicelb \
  --disable=local-storage
```

### 13.2 如果目标是标准 Kubernetes 控制面极限

使用：

```text
kubeadm
独立 apiserver
独立 scheduler
独立 controller-manager
独立 etcd
```

优点：

- 组件边界清晰。
- 指标更直观。
- 更方便单独调参。
- 更容易定位瓶颈。

### 13.3 etcd 建议

etcd 是超大规模测试最容易成为瓶颈的组件之一。

建议：

- 独立 NVMe。
- 不和 Prometheus 混跑。
- 不和 loadgen 混跑。
- 关注 fsync latency。
- 关注 commit latency。
- 关注 DB size。
- 关注 leader changes。
- 关注 compaction/defrag。

---

## 14. 监控指标优先级

### 14.1 apiserver

重点看：

```text
request latency
request rate
inflight requests
max inflight
429
5xx
watch count
list count
watch cache
apiserver CPU / memory
```

### 14.2 etcd

重点看：

```text
fsync latency
commit latency
apply duration
db size
backend commit duration
leader changes
wal fsync
network latency between etcd members
```

### 14.3 scheduler

重点看：

```text
pending pods
scheduling latency
scheduling attempts
queue depth
unschedulable pods
scheduler CPU / memory
```

### 14.4 controller-manager

重点看：

```text
workqueue depth
workqueue retries
reconcile latency
deployment controller backlog
replicaset controller backlog
node lifecycle controller pressure
endpoint/endpointslice controller pressure
```

### 14.5 KWOK

重点看：

```text
pod status patch latency
node status update rate
patch conflict
retry count
controller error count
KWOK CPU / memory
```

### 14.6 监控系统自身

超大规模场景下，Prometheus 自己也可能先爆。

重点看：

```text
series 数量
scrape duration
sample ingestion rate
Prometheus memory
Prometheus WAL
remote write backlog
Grafana dashboard 查询耗时
```

---

## 15. 对象计数脚本

`scripts/count-objects.sh`：

```bash
#!/usr/bin/env bash
set -euo pipefail

echo "nodes:"
kubectl get nodes --no-headers | wc -l

echo "namespaces:"
kubectl get ns --no-headers | wc -l

echo "deployments:"
kubectl get deploy -A --no-headers | wc -l

echo "replicasets:"
kubectl get rs -A --no-headers | wc -l

echo "pods:"
kubectl get pods -A --no-headers | wc -l

echo "pending pods:"
kubectl get pods -A --field-selector=status.phase=Pending --no-headers 2>/dev/null | wc -l || true

echo "running pods:"
kubectl get pods -A --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l || true
```

---

## 16. 健康检查脚本

`scripts/health-check.sh`：

```bash
#!/usr/bin/env bash
set -euo pipefail

echo "== apiserver readyz =="
kubectl get --raw /readyz || true

echo
echo "== apiserver livez =="
kubectl get --raw /livez || true

echo
echo "== component pods =="
kubectl get pods -A -o wide | grep -E 'kwok|kube-system|monitoring' || true

echo
echo "== recent events =="
kubectl get events -A --sort-by=.lastTimestamp | tail -50 || true

echo
echo "== pending pods sample =="
kubectl get pods -A --field-selector=status.phase=Pending | head -30 || true
```

---

## 17. 推荐压测阶段

不要一开始直接上 100 万 Pod。

### 阶段 0：MVP

```text
3 台 control-plane
1 台 kwok-worker
1 台 loadgen
1 台 observability

规模：
1,000 fake nodes
100 namespaces
10 deployments / namespace
10 replicas / deployment

总计：
1,000 fake nodes
1,000 deployments
10,000 pods
```

目标：

- 验证控制面可用。
- 验证 KWOK 可模拟 Node/Pod。
- 验证 Deployment 能变成 Running。
- 验证监控可用。
- 验证脚本和 Makefile 流程完整。

### 阶段 1：扩大 Node

```text
5,000 fake nodes
50,000 pods
```

目标：

- 看 scheduler 调度延迟。
- 看 apiserver list/watch 压力。
- 看 etcd 写入压力。
- 看 KWOK patch 状态压力。

### 阶段 2：扩大对象数

```text
50,000 fake nodes
500,000 pods
50,0000 deployments
```

目标：

- 看 Deployment/ReplicaSet controller 积压。
- 看 API Server watch cache。
- 看 etcd DB size。
- 看监控系统 cardinality。

### 阶段 3：压 watch/list

添加多个 watch/list 客户端：

```text
kubectl get pods -A -w
kubectl get nodes -w
自研 controller watch Pods
自研 controller watch Deployments
Prometheus service discovery
Dashboard/UI list 查询
```

目标：

- 测真实平台常见 watch/list 压力。
- 验证自研组件是否会放大 API 压力。
- 验证大对象量下 UI / 控制台是否可用。

### 阶段 4：删除和重建

测试：

```text
批量删除 namespace
批量删除 deployment
批量 scale down
批量 scale up
重复创建和删除
```

目标：

- 看 GC 压力。
- 看 etcd tombstone/DB size。
- 看 controller-manager backlog。
- 看 apiserver 延迟恢复情况。

---

## 18. 常见误区

### 18.1 误区：fake Node 要分配到某台宿主机

错误。

fake Node 是 Kubernetes API 对象，不是宿主机上的进程。

多个宿主机用来扩展：

- 控制面。
- KWOK controller。
- loadgen。
- observability。

不是用来“承载 fake kubelet”。

### 18.2 误区：Ansible 管所有节点

错误。

Ansible 只管真实机器。

fake Node 不进 inventory，不 SSH，不安装软件。

### 18.3 误区：KWOK 可以压真实网络

错误。

KWOK 不运行真实容器，不产生真实 Pod 网络流量。

### 18.4 误区：一个巨大 Deployment 最简单

不推荐。

一个 `replicas: 100000` 的 Deployment 不能很好模拟真实平台对象分布。

更推荐：

```text
多 namespace
多 deployment
每个 deployment 小 replica
```

### 18.5 误区：KWOK controller 副本越多越好

不一定。

过多 KWOK 副本可能增加：

- watch 压力。
- patch 压力。
- conflict retry。
- apiserver QPS。
- etcd 写入。

应该逐步增加并观察指标。

---

## 19. 推荐落地顺序

### 第一步：初始化真实宿主机

```bash
make tune
```

内容包括：

- sysctl。
- ulimit。
- container runtime。
- 时钟同步。
- SSH。
- 防火墙。
- 基础工具。

### 第二步：部署真实控制面

```bash
make control-plane
```

内容包括：

- kube-apiserver。
- scheduler。
- controller-manager。
- etcd。
- LB/VIP。
- kubeconfig。

### 第三步：部署真实 worker

```bash
make workers
```

这些 worker 不是真实业务 worker，主要用于承载：

- KWOK controller。
- 辅助组件。
- 必要的系统 Pod。

### 第四步：准备 loadgen

```bash
make loadgen
```

内容包括：

- kubectl。
- jq/yq。
- Go/Python。
- kubeconfig。
- 压测脚本。
- 生成器依赖。

### 第五步：部署监控

```bash
make obs
```

内容包括：

- Prometheus。
- Grafana。
- etcd metrics。
- apiserver metrics。
- scheduler metrics。
- controller-manager metrics。
- KWOK metrics。

### 第六步：部署 KWOK

```bash
make kwok
```

确认：

```bash
kubectl get pods -A | grep kwok
```

### 第七步：生成对象 YAML

```bash
make gen
```

生成：

```text
k8s/manifests/generated/nodes/
k8s/manifests/generated/namespaces/
k8s/manifests/generated/deployments/
```

### 第八步：应用 fake Node

```bash
make apply-nodes
```

检查：

```bash
kubectl get nodes | wc -l
kubectl get nodes | head
```

### 第九步：应用 Deployment

```bash
make apply-deployments
```

检查：

```bash
make status
```

### 第十步：开始压测 watch/list/scale/delete

根据 loadgen 脚本逐步执行。

---

## 20. 选型建议

### 如果目标是 k3s 控制面极限

选择：

```text
k3s server + embedded etcd + KWOK
```

优点：

- 贴近 k3s。
- 部署简单。
- 适合验证 k3s 自身行为。

缺点：

- 组件打包程度高。
- 观测和调参不如 kubeadm 清晰。

### 如果目标是标准 Kubernetes 控制面极限

选择：

```text
kubeadm + 独立控制面组件 + KWOK
```

优点：

- 组件边界清楚。
- 指标清楚。
- 便于单独调参。
- 更适合严肃控制面压测。

缺点：

- 部署复杂一些。

---

## 21. 最终推荐

当前需求推荐方案：

```text
一个真实 Kubernetes/k3s 控制面
+
KWOK in-cluster
+
多个真实宿主机承载 KWOK controller、loadgen、observability
+
大量 fake Node / Namespace / Deployment / Pod 对象
```

不要做：

```text
Ansible 直连几万个 fake Node
每台宿主机运行几千个 fake kubelet
把 fake Node 当作真实机器管理
用一个巨大 Deployment 代表全部压力
在控制面机器上跑 loadgen/Prometheus
```

正确边界：

```text
Ansible:
  管真实宿主机和基础设施

Kubernetes:
  管对象和控制面流程

KWOK:
  模拟 Node/Pod 状态

loadgen:
  负责制造对象洪水和 watch/list 压力

Prometheus/Grafana:
  负责观测，但也要防止自己先爆
```

最终一句话：

```text
这是一个“真实控制平面 + 假 worker + 真实对象洪水”的超大规模单集群测试平台。
```
