# KubeBrain + TiKV 产品生产就绪验证清单

这份清单只关注 KubeBrain 产品本身能否稳定、有效地替代 etcd：etcd v3 API 兼容性、Kubernetes/k3s apiserver 存储路径、TiKV 后端一致性、watch/lease/compact 行为、故障恢复和备份恢复。节点规格、反亲和、NetworkPolicy、镜像策略、资源限额等属于集群运维决策，不作为这里的产品就绪判断。

## 验证边界

- 本地 kind、standalone kube-apiserver、临时 in-cluster kube-apiserver 只能证明一部分 API 行为。
- 判断能否替代 etcd，最终必须把真实 Kubernetes/k3s apiserver 的 `--etcd-servers` 指向 KubeBrain，在真实 TiKV/PD 后端上跑对象生命周期、list/watch、lease、compact、apiserver 重启和 KubeBrain leader 切换验证。
- `deploy/dev` 和 `deploy/production` 下的 YAML 只作为本地/测试脚手架，不表达推荐生产运维策略。

## 真-k3s 消费端驱动验证进展（2026-07-03）

用真实 kube-apiserver（k3s v1.36，`--datastore-endpoint` 指向 KubeBrain-on-TiKV）端到端驱动，已完成（脚本 `hack/dev/k3s-load-smoke.sh` 一键复跑）：

- **单节点带 agent 真调度**：Deployment→ReplicaSet reconcile、pod 真跑、EndpointSlices、events（lease TTL）、node 心跳。
- **多节点（3 节点 k3s，KubeBrain 当 datastore）**：跨节点调度（90 副本 30/30/30）、flannel 跨节点网络、service 路由（curl 10/10 200）、coredns DNS、3 node lease。（搭建关键：`fs.inotify.max_user_instances` 默认 128 会被 kind+KubeBrain+k3s 耗尽导致 agent containerd CRI 失败，须 `sysctl -w …=8192`；须 `--disable-cloud-controller`。）
- **failover-under-load**：负载中杀 KubeBrain leader → ~15s 新 leader、node lease 全存活、churn 不断、仅换主窗口几秒瞬态错误。
- **后端混沌（3 副本 TiKV+PD）**：杀 TiKV store 0.24%、杀 PD leader 0.28% 瞬态错误、全恢复（见故障注入段）。
- **大 DeleteRange**：195,928 键单前缀删成功（分块修复）。
- **满载 soak（单节点 30min + 多节点 25min）**：goroutine/内存平稳、零泄漏、apiserver watchcache stall=0（progress-notify 修复持续生效）。

**真-k3s 驱动挖出并已修复的缺陷（已合入 main）**：watch progress-notify 冻结（拖垮 apiserver watchcache）、MVCC 孤儿键自愈、scanner 跨-partition List 复活删除键、大 DeleteRange 单事务失败。

**仍属"未验证 / 需生产规模化"（非功能缺失）**：① 真正大规模（几百上千节点、10万+ 对象、持续高吞吐）；② 真分布式跨机/跨 AZ（当前全在单宿主，磁盘 IO 上限、网络分区未测）；③ 过载对 lease 敏感客户端的边界（p99 尖峰 1–2s 会触发续租超时 + KubeBrain leader 主动重启）；④ 数天/数周长时 soak（compaction 长周期、内存长期走势）；⑤ 更广 k8s 版本兼容矩阵（已系统测 v1.35/v1.36）。

## Kubernetes apiserver 接入前验证

KubeBrain 的兼容矩阵应按 Kubernetes server/kube-apiserver minor 版本记录，而不是按 kind 版本记录。kind 只是本地验证载体；只要它支持目标 Kubernetes node image，就可以用于快速回归。每次验证前先记录版本：

```shell
hack/dev/version-info.sh
```

本地重建指定 Kubernetes server 版本的 kind 环境：

```shell
hack/dev/down.sh
KIND_NODE_IMAGE=kindest/node:v1.36.1 KUBEBRAIN_REPLICAS=3 hack/dev/up.sh
```

本地批量验证多个 Kubernetes server 版本：

```shell
KIND_NODE_IMAGES="kindest/node:v1.35.4 kindest/node:v1.36.1" \
hack/dev/k8s-version-matrix.sh
```

矩阵脚本会为每个 node image 创建独立 kind 集群，并默认启用 standalone kube-apiserver list/watch soak。正式支持窗口应至少覆盖当前生产版本、计划升级版本和一个回滚版本。

完整 kind 矩阵耗时较长。日常兼容回归可以先运行轻量 standalone kube-apiserver 矩阵，它直接从官方 kube-apiserver 镜像提取二进制并指向当前 KubeBrain/TiKV endpoint：

```shell
APISERVER_IMAGES="registry.k8s.io/kube-apiserver:v1.35.4 registry.k8s.io/kube-apiserver:v1.36.1" \
hack/dev/apiserver-version-matrix.sh
```

当前本地已通过 `registry.k8s.io/kube-apiserver:v1.35.4` 和 `registry.k8s.io/kube-apiserver:v1.36.1` 的 standalone smoke 与 20x10 list/watch soak。该结果只能证明 kube-apiserver 存储调用路径兼容，不能替代完整 kind/预生产集群验证。

必须至少通过以下验证：

```shell
ENDPOINT=127.0.0.1:3379 \
IMAGE_NAME=kubebrain:dev \
hack/dev/verify.sh
```

`hack/dev/verify.sh` 默认覆盖单元测试、基础 etcd client smoke、3 副本 HA smoke、独立 kube-apiserver 对象生命周期 smoke，以及 3 副本 mTLS 部署下的 TLS HA smoke。TLS HA smoke 会同时验证 etcd client mTLS、standalone kube-apiserver mTLS，以及逐个删除 Pod 后的 mTLS client smoke。

全副本滚动重启持久性验证默认不运行，因为它会依次删除当前开发环境中的 3 个
KubeBrain、3 个 PD 和 3 个 TiKV Pod。该路径要求精确的 3/3/3 拓扑，并在每次删除后
等待同名 Pod Ready，始终保留后端 quorum：

```shell
RUN_RESTART_PERSISTENCE_SMOKE=true \
RUN_GO_TEST=false \
RUN_BASIC_SMOKE=false \
RUN_HA_SMOKE=false \
RUN_APISERVER_SMOKE=false \
RUN_TLS_SMOKE=false \
hack/dev/verify.sh
```

测试使用官方 `client/v3` 在重启前写入普通 key、已删除 key、长 TTL lease 附属 key
和 watch 历史；重启期间持续读取并拒绝成功响应的 revision 回退，恢复后验证当前值、
tombstone 与历史值、lease/附属 key、watch 历史回放，以及新写入 revision 严格增长。
当前 3×KubeBrain、3×PD、3×TiKV 环境已完成一次 9 Pod 顺序重启，用时 68.42 秒。
该结果证明 quorum-preserving 滚动恢复的客户端可观察持久性，不替代备份恢复、跨可用区
分区或同时失去多数副本的灾难恢复演练。

## 备份恢复生产边界

当前唯一通过端到端恢复验证的生产备份模式是 `kubebrain.logical.v2`。制品包含固定
snapshot revision、源 prefix、记录数和 SHA-256，先写临时文件并 `fsync` 后原子发布；
lease 元数据记录剩余 TTL；restore 为目标生成新 lease ID，同时保持 key 关联和共享
关系。restore 在写目标前完整校验，并默认拒绝覆盖已有 key。每次发布备份配置前必须通过：

```shell
BACKUP_MODE=logical hack/backup/production-mode-check.sh
```

TiDB Operator 的 BR full/PITR 不能用于 KubeBrain 数据恢复。真实 S3 full backup 和
独立 PD/TiKV Restore CR 都成功时，备份前已提交的 KubeBrain key 仍未出现在目标集群；
任务 `Complete` 只证明 TiDB 管理范围恢复成功。BR raw 每次只处理一个 CF 且仍为实验
功能，不能提供已经验证的 transactional KV 跨 CF 一致快照。因此控制面必须拒绝
`br-full`、`br-pitr` 和 `br-raw`，并保留物理 PITR 为显式未完成项。

上线前至少执行一次全 `/registry` 隔离恢复，要求非空、记录数一致、逐 key/value 校验
通过且清理成功：

```shell
ENDPOINT=127.0.0.1:3379 PREFIX=/registry \
  hack/backup/logical-drill.sh
```

同时必须执行 lease-aware 演练，验证临时 key 不会在恢复后永久化：

```shell
ENDPOINT=127.0.0.1:3379 hack/backup/lease-restore-smoke.sh
```

基础 etcd client smoke 当前覆盖 Txn create/update/delete、无 Compare 的多操作 Txn、Value Compare Txn、普通 Put、Range delete、普通空非 from-key range 返回空结果、普通空非 from-key DeleteRange 不删除数据、Txn 中空非 from-key DeleteRange 不删除数据、Watch create/update/delete 事件、update watch 在 `WithPrevKV` 下返回 previous value 且不会被误判为 create、follower proxy watch 保留 leader 返回的 `PrevKV`、点 watch 不会误收到子 key 事件、任意 `[start,end)` range watch 不漏掉范围内非 start 前缀 key且可经 follower proxy 转发、`KEY`/`VALUE`/`MOD`/`CREATE`/`VERSION` sort target、旧 revision watch 在 compact 后返回 `ErrCompacted` 和 `CompactRevision`、Lease grant/keepalive/ttl/revoke、Cluster MemberList，以及 Maintenance Status/HashKV/Compact/AlarmList/Defragment。`Compact` 已按写请求处理，follower 会通过 proxy 转发到当前 leader；带显式 `Revision` 的历史 `Range` 在 follower 上也会转发到 leader，以避免 compact 边界在 follower 上短暂不可见。

独立 kube-apiserver smoke 当前覆盖 namespace、ConfigMap create/update/get/watch/delete、ConfigMap label selector、field selector、chunked list、delete collection、Secret、coordination Lease update，以及 apps Deployment create/update/list 路径。这些路径会触发 kube-apiserver 对 etcd `Range` limit/continue、watch 和批量删除的常见使用形态。

可选真实 k3s datastore smoke：

```shell
RUN_K3S_DATASTORE_SMOKE=true \
RUN_GO_TEST=false \
RUN_BASIC_SMOKE=false \
RUN_HA_SMOKE=false \
RUN_APISERVER_SMOKE=false \
RUN_TLS_SMOKE=false \
hack/dev/verify.sh
```

该脚本启动真实 k3s server，并把 `--datastore-endpoint` 指向 KubeBrain。它会创建、更新、分页读取 namespace、ConfigMap、Secret 和 coordination Lease，随后直接连接 TiKV 校验对应 `/registry/...` 对象 key 存在且 value 非空；还会在 KubeBrain Deployment rolling restart 后继续读取，并重启 k3s 后确认对象仍能通过 apiserver 读回。当前本地已在 `k3s v1.36.2+k3s1` 上通过一次完整验证，覆盖 `/registry/configmaps/...`、`/registry/secrets/...` 和 `/registry/leases/...` 的 TiKV 直连校验。脚本会清理 k3s 相关 `/registry/...` 子前缀和 bootstrap key，避免历史 k3s 系统对象污染新的临时 data-dir，同时避免大范围清空 `/registry` 误扫其他测试前缀。

需要增加真实 watch 和批量 patch 时启用 soak：

```shell
RUN_K3S_DATASTORE_SMOKE=true \
K3S_SOAK=true \
SOAK_OBJECTS=12 \
SOAK_UPDATES=6 \
RUN_GO_TEST=false \
RUN_BASIC_SMOKE=false \
RUN_HA_SMOKE=false \
RUN_APISERVER_SMOKE=false \
RUN_TLS_SMOKE=false \
hack/dev/verify.sh
```

当前本地已通过 k3s soak：12 个 ConfigMap、每对象 6 次 patch，共收到 72 个 `MODIFIED` watch 事件，并直接从 TiKV 校验首尾对象。该轮曾暴露出 KubeBrain Pod 重启后新实例内存 watch cache 为空、旧 revision watch 被误判为 compacted 的问题；当前已增加基于 TiKV object history 的 watch catch-up fallback，并补充起始 revision inclusive 的回归测试。脚本随后会独立执行 KubeBrain rolling restart、重启 k3s，并确认对象仍能通过 apiserver 读回。soak 阶段默认不再夹带 KubeBrain rolling restart，因为单个外层 `kubectl --watch` 流在后端 Pod 替换、leader proxy 重连和 compaction 交叠时可能按 Kubernetes watch 语义退出并由客户端重新 list/watch；rolling restart 应通过 apiserver 恢复能力、最终对象状态和 TiKV 持久化单独验证。该脚本会清理 k3s 相关 `/registry/...` 子前缀，避免旧 `kube-system/k3s-serving` TLS Secret 等系统对象与新临时 data-dir 的 CA 不匹配。

需要覆盖真实 apiserver delete collection 路径时启用：

```shell
RUN_K3S_DATASTORE_SMOKE=true \
K3S_DELETE_COLLECTION=true \
DELETE_COLLECTION_OBJECTS=24 \
RUN_GO_TEST=false \
RUN_BASIC_SMOKE=false \
RUN_HA_SMOKE=false \
RUN_APISERVER_SMOKE=false \
RUN_TLS_SMOKE=false \
hack/dev/verify.sh
```

该路径会通过 k3s apiserver 创建一批带 label 的 ConfigMap，执行 `delete configmap -l ...`，确认 list 结果清空，并直连 TiKV 校验首尾对象的 revision index 已变为 tombstone。当前本地已通过 8 个对象和 24 个对象多轮验证；最近一次在最新 KubeBrain 镜像上验证 24 个 ConfigMap 删除成功，首尾对象 tombstone 可从 TiKV 直读，随后完成 KubeBrain rolling restart、k3s restart 和保留对象读回。

范围删除使用单个 `TxnApply` 提交，普通 key tombstone、lease attachment 和 watch event 全部共享一个 TiKV 事务与一个 MVCC revision；提交失败不会暴露已删除的前缀。生产环境通过 `--max-delete-range-keys=1024` 在写入前约束事务规模，超限只扫描 `limit+1` 个 key 后返回标准 `ResourceExhausted`，不分配 revision。默认 `0` 保持 etcd 不限 key 数的兼容行为。watch ring 已从“一 revision 一事件槽位”改为同一 revision 可承载多个事件，批量删除能通过 watch cache 一次返回多个同 revision DELETE 事件。

事务重试遵循公开请求的 context deadline：Put/DeleteRange/Txn 与 Lease Grant/Revoke 的 etcd unary 入口默认注入 10 秒，并自动取客户端更短 deadline；backend 不再用内部 1 秒预算提前截断 `TxnApply`。没有 deadline 的后台/直接调用仍保留 1 秒安全兜底，防止持久冲突形成无界重试。

需要覆盖真实 namespace 删除和 namespace controller 清理路径时启用：

```shell
RUN_K3S_DATASTORE_SMOKE=true \
K3S_NAMESPACE_DELETE=true \
NAMESPACE_DELETE_OBJECTS=8 \
RUN_GO_TEST=false \
RUN_BASIC_SMOKE=false \
RUN_HA_SMOKE=false \
RUN_APISERVER_SMOKE=false \
RUN_TLS_SMOKE=false \
hack/dev/verify.sh
```

该路径会通过 k3s apiserver 创建独立 namespace，写入多组 ConfigMap 和 Secret，删除 namespace，等待 namespace 消失，并直连 TiKV 校验首尾 ConfigMap、Secret 以及 namespace 自身的 revision index 已变为 tombstone。当前本地已通过 3 个对象和 8 个对象多轮验证；最近一次在最新 KubeBrain 镜像上验证 8 组 ConfigMap/Secret 删除成功，首尾对象和 namespace 自身 tombstone 可从 TiKV 直读，随后完成 KubeBrain rolling restart、k3s restart 和保留对象读回。

可选集群内 kube-apiserver smoke：

```shell
RUN_INCLUSTER_APISERVER_SMOKE=true hack/dev/verify.sh
```

该脚本把 kube-apiserver 作为临时 Pod 运行，并让 apiserver 通过 `kubebrain.kubebrain-dev.svc:3379` 访问 KubeBrain Service；宿主机只通过 port-forward 访问这个临时 apiserver。当前本地已通过 namespace、ConfigMap create/update/watch、chunked list 和 Secret 路径验证。该路径比宿主机 NodePort smoke 更接近生产拓扑，但仍不替代预生产 apiserver 真实接入。

可选集群内 kube-apiserver list/watch soak：

```shell
RUN_INCLUSTER_APISERVER_WATCH_SOAK=true hack/dev/verify.sh
```

该脚本复用临时 kube-apiserver Pod，通过 KubeBrain Service 访问后端，并执行批量 ConfigMap watch/list 验证。当前本地已通过 20 个对象、每对象 10 次 patch，共 200 个 `MODIFIED` 事件。

可选集群内 kube-apiserver 滚动升级 smoke：

```shell
RUN_INCLUSTER_APISERVER_ROLLOUT_SMOKE=true hack/dev/verify.sh
```

该脚本在临时 kube-apiserver Pod 的 watch 建立后触发 KubeBrain Deployment rolling restart，并继续通过 KubeBrain Service 验证 watch/list。当前本地已通过 12 个对象、每对象 6 次 patch，共 72 个对象版本更新，最近一次复测为 `observed_updates=72`、`modified_events=72`。该路径曾暴露两个 watch 恢复问题：一是 KubeBrain 内部每分钟后台 compact 到 `currentRevision-1000` 会把 kube-apiserver 正在追的 watch revision 过早 compact，导致 apiserver cacher 报 `required revision has been compacted` 并漏掉外层 watch 事件；当前默认后台 compact 已停用，保留客户端/显式 `Compact` 语义，由 Kubernetes apiserver compactor 或外部调用驱动 compaction。二是 KubeBrain leader/proxy 重启后，新实例内存 watch cache 的 oldest revision 可能高于 kube-apiserver 恢复 watch 请求的 revision，旧逻辑会直接返回 compacted；当前 `ret.low` 与空 cache 路径都会先回源 TiKV object history 做 catch-up，只有请求 revision 真正早于持久 compact revision 时才返回 compacted。

可选故障注入入口：

```shell
RUN_FAULT_SMOKE=true hack/dev/verify.sh
```

当前 dev 故障注入会删除 KubeBrain、PD、TiKV Pod 并等待恢复后重新运行基础 smoke。

**已在 3 副本 TiKV + 3 副本 PD 上做过负载中混沌（2026-07-03）**：C=100 写负载下杀 1 个 TiKV store → 0.24% 瞬态错误（TiKV region-leader 重选窗口，超时型）、store ~90s 重建、3 store 全 Up；杀 PD leader → 0.28% 瞬态错误、新 PD leader ~65s 重选；两者均无级联失败、无数据丢失、恢复后读写正常。注意：后端故障还会让当前 KubeBrain leader 以 `klog.Fatal("leader lost")` 退出重启（丢主即干净重启的设计，非缺陷）。生产预演仍应在**跨机多副本**上重跑并记录恢复时间与错误率（当前仍是单机同宿主）。

可选 lease 过期 smoke：

```shell
RUN_LEASE_EXPIRY_SMOKE=true hack/dev/verify.sh
```

默认创建 TTL 为 2 秒的 lease，绑定 key，确认 `TimeToLive` 返回 attached key，然后等待 key 自动删除并确认 watch 收到 DELETE 事件。可以通过 `LEASES` 扩大并发 lease 数量。预生产应扩大到多个并发 lease，并覆盖 KubeBrain leader 重选期间的 lease keepalive 和过期行为。

当前本地已通过 TTL=2、TTL=3 的单 lease 过期 smoke，以及 TTL=2、25 个并发 lease 的过期 smoke，均确认 watch 收到 PUT 与 DELETE 事件，且过期后 Get 返回空。

可选 lease 故障 smoke：

```shell
RUN_LEASE_FAULT_SMOKE=true hack/dev/verify.sh
```

默认创建 10 个 TTL 为 6 秒的 lease，在 lease 活跃期间删除一个 KubeBrain Pod，等待 Deployment 恢复后确认所有 key 都过期删除并产生 DELETE 事件。预生产还应覆盖删除当前 leader、连续滚动重启、apiserver watch 重连和 lease keepalive 并发场景。

当前本地已通过默认 10 lease 故障 smoke，以及 TTL=8、25 lease 的 Pod 删除故障 smoke；最近一次在最新 KubeBrain 镜像上通过默认 10 lease、TTL=6 的 Pod 删除故障验证，最终收到 10 个 PUT 和 10 个 DELETE 事件并确认 key 清空。此前 TTL=8、25 lease 验证在 pod 删除期间出现一次 client EOF 重试，最终仍收到全部 DELETE 事件并确认 key 清空。

可选 watch 扇出 soak：

```shell
RUN_WATCH_SOAK=true hack/dev/verify.sh
```

默认规模为 25 条 watcher、50 个事件，主要用于快速发现 watch fan-out、事件丢失和 watch buffer 问题。预生产应按真实 apiserver watch 数量和对象写入速率扩大规模。

可选轻量负载 smoke：

```shell
RUN_LOAD_SMOKE=true hack/dev/verify.sh
```

默认规模为 8 个 worker、每个 worker 25 轮 Txn create、Get、Txn update、Delete，并输出 `p50_us`、`p95_us`、`p99_us`。它用于快速发现明显的并发读写退化，不替代正式容量压测。预生产应按真实对象规模、apiserver QPS、写比例和 watch 数量扩大规模并记录延迟分位。

当前本地已通过 16 个 worker、每个 worker 50 轮的轻量负载 smoke，共 800 轮 create/get/update/delete，无失败；最近一次默认规模 smoke 为 8 个 worker、每个 worker 25 轮，共 200 次操作，无失败，p95 约 108ms。

可选集群内轻量负载 smoke：

```shell
RUN_INCLUSTER_LOAD_SMOKE=true hack/dev/verify.sh
```

该脚本从 Kubernetes Job 内部通过 `kubebrain.kubebrain-dev.svc:3379` 访问 KubeBrain Service，避免宿主机 NodePort 干扰，更接近生产访问拓扑。当前本地已通过默认规模 8 个 worker、每个 worker 25 轮，共 200 轮 create/get/update/delete，无失败，p95 约 110ms。

可选 compact soak：

```shell
RUN_COMPACT_SOAK=true hack/dev/verify.sh
```

默认规模为 20 轮 compact 验证、4 个并发 latest reader。脚本按 Kubernetes 官方 compactor 行为执行：通过 `compact_rev_key` CAS 记录 compact 目标 revision，再调用 etcd `Compact`，并验证 `< compactRev` 的历史 `Range` 和 `Watch` 返回 compacted；并发 reader 用于发现 compact 期间普通最新 list 的明显异常。预生产应扩大规模，并在 KubeBrain leader 切换、apiserver watch 重连和 TiKV/PD 故障场景下重复运行。

当前本地已多次通过默认 compact soak；最近一次为 20 轮 compact 验证、4 个并发 reader、约 938 次 latest reader 操作，无失败。该脚本曾捕获到后端实际 compact revision 低于请求 revision 但 RPC 返回成功的问题；当前已改为返回可重试 `Unavailable`，脚本会等待后续 compact 成功。脚本也曾使用 `Maintenance.Status` 间接选择 compact revision，但这不符合 Kubernetes compactor 实现，已改为直接对齐 `/root/kubernetes/staging/src/k8s.io/apiserver/pkg/storage/etcd3/compact.go` 的 `compact_rev_key` CAS 行为。

可选 compact 故障 smoke：

```shell
RUN_COMPACT_FAULT_SMOKE=true hack/dev/verify.sh
```

默认会在 compact soak 运行期间删除一个 KubeBrain Pod，等待 Deployment 恢复，并确认 compact/read/watch 验证仍能完成。预生产应扩大规模，并分别覆盖删除当前 leader、删除 follower、连续滚动重启和 apiserver watch 重连。

当前本地已多次通过默认 compact fault smoke；最近一次为 60 轮 compact 验证、4 个并发 reader、运行期间删除 1 个 KubeBrain Pod，最终约 2029 次 latest reader 操作，无失败。该脚本曾复现 Pod 重启期间 compact 后旧 revision `Range` 偶发成功的问题；当前通过历史 revision `Range` 转发 leader，以及 `Compact` 返回前等待 compact revision 可见进行加固。

可选滚动升级 smoke：

```shell
RUN_ROLLOUT_SMOKE=true hack/dev/verify.sh
```

默认会在轻量 load smoke 运行期间触发 KubeBrain Deployment rolling restart，并确认升级过程中 Txn create、Get、Txn update、Delete 没有失败。预生产应增加更长时间、更高并发、更接近真实 apiserver QPS 的滚动升级验证，并分别覆盖 leader/follower 退出时的客户端重试行为。

当前本地已通过 3 副本滚动升级 smoke：8 个 worker、每个 worker 50 轮，共 400 轮 create/get/update/delete，无失败，最近一次 p95 约 75ms。该路径曾暴露两个问题：一是脚本等待所有同 label Pod 时会被旧 ReplicaSet terminating Pod 卡住，已改为按 Deployment ready/updated 状态判断；二是滚动期间 gRPC `Stop()` 会让命中退出 Pod 的客户端收到 EOF，当前已改为先 `GracefulStop()`、超时再 `Stop()`。同时修复了 backend resource lock `Update` 成功后未刷新 `lastVal`，导致连续 renew 后 release/update CAS 可能失败的问题。

可选集群内滚动升级 smoke：

```shell
RUN_INCLUSTER_ROLLOUT_SMOKE=true hack/dev/verify.sh
```

该脚本在集群内 Job 通过 KubeBrain Service 访问时触发 Deployment rolling restart，用于验证 Service/EndpointSlice 路径。当前本地已通过 3 副本集群内滚动升级 smoke：8 个 worker、每个 worker 50 轮，共 400 轮 create/get/update/delete，无失败，最近一次 p95 约 172ms、p99 约 212ms。

该路径曾暴露出滚动期间 follower proxy 持续等待旧 leader IP，导致写请求拖到客户端 deadline 并以 `Unknown desc = context deadline exceeded` 失败的问题；当前 proxy readiness 等待已加短上限，超时返回可重试 `Unavailable`，避免单次请求被旧 leader 窗口拖死。复测中仍能看到退出 Pod 连接关闭后的 etcd client 重试日志，但集群内 load 最终 400 轮操作全部完成、无失败。后续长压应继续观察该窗口对 apiserver watch/list 的影响。

prefix watch catch-up 还修复了一个边界：当 watch cache 中混有其他 prefix 的更高 revision 时，后续 live watch 起点应从最后一个已发送的匹配 prefix 事件之后继续，而不是从全局 newest revision 之后继续，避免跳过本 prefix 事件。

该路径后来又暴露出 follower 通过 HTTP `/status` 向旧 leader 同步 read revision 时会把超时直接返回给 kube-apiserver；当前 revision sync 已改为使用请求 context、对 leader 未选出/连接拒绝/超时/旧 leader 返回非 OK 等 rollout 窗口错误做短周期重试，并在 leader 地址变化后使用新 leader 继续同步。follower watch proxy 也已改为在 leader 变化或可重试连接错误时内部重连，并从最后已发送 revision 的下一位继续 watch，避免主动把 KubeBrain leader 切换暴露为 watch channel 关闭。

可选 standalone kube-apiserver 滚动升级 smoke：

```shell
RUN_APISERVER_ROLLOUT_SMOKE=true hack/dev/verify.sh
```

当前本地已通过 3 副本 standalone kube-apiserver rollout smoke：12 个 ConfigMap、每个对象 6 次 patch，共 72 个 `MODIFIED` 事件；测试在 watch 建立后触发 KubeBrain rolling restart，最终对象状态和 watch 事件均通过。该脚本在本地 NodePort 暴露 `127.0.0.1:3379` 的 dev 拓扑下允许外层 `kubectl --watch` 重连，因为 NodePort 到正在退出 Pod 的既有连接可能被 reset；生产环境不应依赖 NodePort，而应使用稳定的集群内 Service 或受控内部负载均衡，并验证 apiserver/informer 在 KubeBrain 滚动更新期间的重连行为。最近一次组合验证已连续通过默认 load smoke、rollout load smoke 和 standalone kube-apiserver rollout smoke。

可选 standalone kube-apiserver list/watch soak：

```shell
RUN_APISERVER_WATCH_SOAK=true hack/dev/verify.sh
```

默认规模为 20 个 ConfigMap、每个对象 10 次 patch，并通过带 label selector 的 apiserver watch 校验至少收到 200 个 `MODIFIED` 事件，同时用 chunked list 校验最终对象数量。预生产应按真实控制器 watch 数量、对象规模和 apiserver QPS 扩大规模。

当前本地已通过 standalone kube-apiserver list/watch soak 的 20 个对象、每对象 10 次 patch，共 200 个 `MODIFIED` 事件；该轮验证覆盖 update watch `PrevKv` 兼容路径，确认 kube-apiserver watcher 未再触发更新事件 `PrevKv=nil` 内部错误。

当前本地也已通过 basic etcd client smoke、默认 load smoke 和 8x4 standalone kube-apiserver watch soak 并发运行；basic smoke 的 future revision 检查已使用大偏移 revision，避免被并发写推进 revision 后误报。基础 smoke 的总超时时间已从硬编码 10 秒改为 `SMOKE_TIMEOUT_SECONDS`，默认 120 秒，避免并发验证时最后几个请求因全局 deadline 耗尽被误报为语义失败。

可选 standalone kube-apiserver 版本矩阵：

```shell
RUN_APISERVER_VERSION_MATRIX=true \
APISERVER_IMAGES="registry.k8s.io/kube-apiserver:v1.35.4 registry.k8s.io/kube-apiserver:v1.36.1" \
hack/dev/verify.sh
```

预生产环境还应增加：

- Kubernetes 官方 e2e 中涉及 namespace、configmap、secret、pod、deployment、watch、lease 的用例。
- 长时间 list/watch 压测。
- apiserver 重启、KubeBrain 滚动升级、KubeBrain leader 重选。
- PD/TiKV leader 切换、TiKV Pod 重启、网络延迟和短时中断。

## 已实现的 etcd 兼容能力

- `Range`，并支持 `RangeRequest.KeysOnly`、按 `Key`/`ModRevision`/`Value` 排序、`MinModRevision`、`MaxModRevision`
- 普通空非 from-key `Range`，例如 `[key,key)` 或 start 大于 end 的范围，按 etcd 行为返回空结果而不是错误。
- Kubernetes 常用 `Txn` create/update/delete 路径
- 基础通用 `Txn` CAS put 路径：`Compare(ModRevision)==rev` + `Then(Put)`，可无 `Else`
- 基础通用 `Txn` create put 成功响应允许 `PutRequest.PrevKv`，创建成功时 `PrevKv` 为空
- 基础通用 `Txn` CAS put 成功响应支持 `PutRequest.PrevKv`
- 普通 `Put` 和 `Txn` Put 支持 `PutRequest.IgnoreLease`，用于更新 value 时保留现有 lease 绑定
- 普通 `Put` 和 `Txn` Put 支持 `PutRequest.IgnoreValue`，用于更新 lease 时保留现有 value
- 基础通用 `Txn` CAS delete 路径：`Compare(ModRevision)==rev` + `Then(DeleteRange)`，可无 `Else`
- 基础通用 `Txn` CAS delete 成功响应使用标准 `DeleteRangeResponse`，并支持 `DeleteRangeRequest.PrevKv`
- 基础通用 `Txn` 中的空非 from-key `DeleteRange` 返回 `Deleted=0` 且不删除数据。
- 基础通用 `Txn` create conflict 路径：`Compare(ModRevision)==0` + `Then(Put)` + 可选 `Else(Get)`
- 基础通用 `Txn` 无 Compare 顺序执行路径：`Then(Put|Range|DeleteRange...)`
- 基础通用 `Txn` Compare 分支路径：支持单 key `ModRevision`、`Value`、`Lease`、`Version`、`CreateRevision` 判断，然后顺序执行 `Success` 或 `Failure` 中的 `Put|Range|DeleteRange`
- 基础通用 `Txn` range compare：支持 range 上的 `ModRevision`、`Value`、`Lease`、`Version`、`CreateRevision`；range 内任一 key 不满足 compare 时事务失败
- `KeyValue.CreateRevision` 和 `KeyValue.Version` 由 KubeBrain 内部版本化 metadata 维护；新写入对象可返回创建 revision 和随更新递增的 version，并支持 `RangeRequest.MinCreateRevision/MaxCreateRevision` 后置过滤。
- Kubernetes compactor 使用的 `compact_rev_key` CAS 路径
- 普通 `Compact` 调用会通过 leader 执行后端 compaction；旧 revision 的 `Range`、`Txn` range precheck、`HashKV` 和 `Watch` 会按已 compact revision 返回错误或 canceled compacted watch 响应
- 普通 `Put`
- 普通 `DeleteRange`
- 普通空非 from-key `DeleteRange`，例如 `[key,key)` 或 start 大于 end 的范围，按 etcd 行为返回 `Deleted=0` 且不删除数据。
- `Watch`、progress notify、watch progress request；PUT 更新事件会按 `WithPrevKV` 返回 previous value，并避免被 clientv3/Kubernetes 误判为 create 事件；点 watch、prefix watch 和任意 `[start,end)` range watch 会按 etcd key 范围语义过滤事件；follower proxy watch 会在 leader 变化或可重试连接错误后内部重连并从下一 revision 继续
- `LeaseGrant`、`LeaseRevoke`、`LeaseKeepAlive`、`LeaseTimeToLive`、`LeaseLeases`
- `Maintenance.Status`、`Hash`、`HashKV`、`Alarm`、`Defragment`
- `Cluster.MemberList` 兼容视图
- `Auth` service 已注册但显式返回 `Unimplemented`，避免客户端看到 unknown service；KubeBrain 当前不提供 etcd auth/role/user 语义。

## 仍需补齐或确认

- **`Maintenance.Snapshot` 刻意不实现（设计决定，非缺口）。** etcd 的 snapshot API 存在是因为 etcd 是自包含单机 bbolt 库、数据只在自己肚子里；KubeBrain 的数据在 TiKV，备份/DR 由 **TiKV 原生 BR + PITR（日志备份）** 承担，能力全面强于 etcd snapshot（全量+增量、秒级时间点恢复、S3、各 region 并行、TB 级）。把整个 TiKV 数据集通过 etcd 流式 snapshot API 拉成单文件反而是倒退。**备份恢复走 TiKV/PD 侧，不走 etcd snapshot；DR 待办 = 做一次 TiKV BR 全量+PITR 恢复演练，确认恢复后 KubeBrain MVCC 修订号连贯（PD TSO 单调，会推进过任何已恢复修订值）。**
- 仓库另提供 `hack/backup/logical-export.sh` / `logical-restore.sh` 作为 Kubernetes 对象级**逻辑**备份/恢复演练入口（用于迁移/隔离前缀校验，不保留 etcd revision/lease 语义，非主备份路径）。当前本地已通过 `/registry` 4091 条记录的导出、隔离前缀恢复、计数校验和清理。
- `MemberAdd`、`MemberRemove`、`MemberUpdate`、`MemberPromote` 不支持，因为 KubeBrain 不是 etcd raft 成员管理模型。
- etcd Auth user/role/permission 管理不支持；生产访问控制应依赖 client mTLS、网络策略、Kubernetes apiserver 认证授权和运维侧凭据管理。
- 通用 etcd v3 `Txn` 语义未完整实现，当前目标仍是 Kubernetes apiserver storage path；已补基础 CAS put、CAS delete、create conflict fallback、无 Compare 顺序执行、基础 Compare 分支执行、基础 range compare、version/create revision compare，但还没有提供完整 etcd 原子事务隔离。
- 多副本下 `Compact` 后旧 revision 立即读的可见性已通过 leader 转发、不足额 compact 可重试错误、`Compact` 返回前可见性等待，以及历史 revision `Range` 转发 leader 做初步加固；本地 smoke、默认 compact soak、默认 compact fault smoke，以及 **3 副本 TiKV+PD 上的负载中混沌 + 单节点/多节点满载 soak** 已通过；仍需**数天级**长时间并发 compact/list/watch 压测确认长周期行为。
- 对已有历史数据，若写入发生在 metadata 机制引入前，`CreateRevision/Version` 会回退为 `CreateRevision=ModRevision, Version=1`；生产迁移前需要用真实数据集验证是否存在旧数据兼容影响。
- TLS、认证、授权、证书轮换需要按生产环境补齐。
- 需要为 TiKV/PD 与 KubeBrain 建立监控告警，包括请求错误率、延迟、leader 切换、watch 关闭、lease 数量、TiKV/PD 健康和磁盘容量。

## 监控指标

产品验证和真实接入测试应持续观察这些 KubeBrain 指标，具体告警阈值由运行环境自行决定：

- gRPC 非 OK 响应、写失败、watch 后端错误、watch buffer overflow。
- leader election 短时间频繁丢失。
- watch revision lag 过高。
- gRPC p99 延迟超过 1 秒。

这些阈值是预生产起点，不应直接作为最终生产阈值。正式上线前应基于真实对象规模、apiserver QPS、watch 数量和 TiKV 延迟重新校准。

## 回滚要求

生产灰度前必须准备并演练：

- 从 KubeBrain 切回官方 etcd 的步骤。
- 从 TiKV/PD 备份恢复 KubeBrain 数据的步骤。
- apiserver 停写窗口和数据一致性校验流程。
- 回滚后 Kubernetes 核心对象的 read/list/watch 验证。

## 逻辑备份和恢复演练

逻辑备份脚本会运行仓库内 Go command，依赖随主 `go.mod` 管理，不会在每次演练时创建临时 Go module 或动态拉取依赖。默认请求超时时间为 `10m`，大集群演练可以通过 `TIMEOUT=30m` 这类 Go duration 字符串调整导出、恢复、count、内容校验和清理超时。所有 `hack/backup/*.sh` 脚本都支持 `--help` 查看参数。

当前格式为 `kubebrain.logical.v2`：首行 manifest 固定源 prefix 与 snapshot revision，
lease 行固定源 ID 和导出时的正数剩余 TTL，尾行记录/lease 总数和覆盖 manifest/全部
记录的 SHA-256。导出先写同目录临时文件，完成
`fsync` 后原子 rename 并同步父目录；中断导出不会把不完整内容发布到目标路径。
`logical-status.sh`、restore 和 verify 都会先复制并验证完整文件，缺 footer、记录数
不符、内容篡改或 footer 后附加数据均 fail closed。restore 在任何 etcd 写入前完成
验证，并按 `BATCH_SIZE` 把 compare 与 Put 放入同一个 Txn，使单批冲突不会部分落盘。
恢复为每个源 lease 生成新目标 ID并保留多 key 共享关系。v1 无 lease 制品继续可恢复；
v1 中记录非零 lease 时因缺少 TTL 元数据会在任何写入前拒绝。没有 manifest/footer 的
旧 JSONL 无法证明完整性，同样明确拒绝。

导出默认覆盖 `/registry` 前缀：

```shell
ENDPOINT=127.0.0.1:3379 \
OUTPUT=/tmp/kubebrain-registry-backup.jsonl \
hack/backup/logical-export.sh
```

导出脚本会在首个分页 `Range` 响应的 revision 上固定后续分页请求，避免导出过程跨多个 etcd revision 形成混合快照。导出日志会打印本次 logical backup 使用的 snapshot revision。

可离线检查格式、revision、记录数与摘要：

```shell
INPUT=/tmp/kubebrain-registry-backup.jsonl \
hack/backup/logical-status.sh
```

恢复演练建议先重写到隔离前缀，避免覆盖真实对象：

```shell
ENDPOINT=127.0.0.1:3379 \
INPUT=/tmp/kubebrain-registry-backup.jsonl \
REWRITE_FROM=/registry \
REWRITE_TO=/registry-restore-drill \
hack/backup/logical-restore.sh
```

恢复脚本默认拒绝覆盖已有 key，防止误把备份写回真实 `/registry` 前缀；需要执行有停写窗口的覆盖恢复时必须显式设置 `ALLOW_OVERWRITE=true`。日常演练建议始终使用 `REWRITE_TO` 指向新的隔离前缀。

可以用专用 smoke 验证覆盖保护行为。该脚本只操作独立小前缀，不会导出或写入完整 `/registry`：

```shell
ENDPOINT=127.0.0.1:3379 \
hack/backup/restore-guard-smoke.sh
```

可以用另一个专用 smoke 验证内容校验会捕获恢复结果损坏。该脚本会导出一个小前缀，恢复到隔离前缀后故意篡改 value，并期望 `logical-verify` 返回失败：

```shell
ENDPOINT=127.0.0.1:3379 \
hack/backup/verify-content-smoke.sh
```

可以用完整性 smoke 截断 v1 footer，并确认 restore 在写入任何目标 key 前拒绝损坏
文件：

```shell
ENDPOINT=127.0.0.1:3379 \
hack/backup/backup-integrity-smoke.sh
```

可以用脚本自动完成导出、隔离恢复、计数校验、逐条 key/value 内容校验和清理：

```shell
ENDPOINT=127.0.0.1:3379 \
PREFIX=/registry \
hack/backup/logical-drill.sh
```

如果 KubeBrain client endpoint 启用了 TLS/mTLS，备份脚本支持 etcdctl 风格证书环境变量：

```shell
ENDPOINT=https://kubebrain-client.kubebrain-system.svc:3379 \
ETCDCTL_CACERT=/path/to/ca.crt \
ETCDCTL_CERT=/path/to/client.crt \
ETCDCTL_KEY=/path/to/client.key \
PREFIX=/registry \
hack/backup/logical-drill.sh
```

也可以使用较短的 `CACERT`、`CERT`、`KEY` 环境变量。v1 格式当前已在真实
TiKV/PD 环境完成 34 条 `/registry` 全前缀隔离恢复、逐值核验和清理；截断 footer
被拒绝且目标计数保持 0，两条记录批次中第二条冲突时目标计数保持原有 1。此前旧格式
曾通过明文 4091 条、TLS/mTLS 4148 条及 `/registry/smoke` 712 条演练；这些历史结果
证明当时的数据路径规模，但旧文件本身不满足 v1 完整性契约，升级后必须重新导出。
专用 verify-content smoke 仍确认恢复结果 value 被篡改时 `logical-verify` 会失败。

恢复后至少验证：

- 恢复记录数与导出记录数一致。
- 隔离前缀下对象可 list/get。
- apiserver 真实恢复前必须有停写窗口，且不能依赖该逻辑备份保留原始 revision、lease 或 watch 历史。
