# Kubernetes + TiKV 开发环境

这个目录提供一个本地开发栈，用来逐步补齐 KubeBrain 还没有完成的 etcd 兼容能力。

## 目标

- 用 kind 启动本地 Kubernetes。
- 用 TiDB Operator 部署一个最小 TiKV/PD 环境。
- 用 TiKV 后端运行 KubeBrain。
- 用真实 `go.etcd.io/etcd/client/v3` 做 smoke test，避免只测内部接口。

## 依赖

本地需要先安装：

- Docker
- kind
- kubectl
- helm
- Go

当前仓库提供检查脚本，但不会自动安装这些系统依赖。

## 启动

```shell
hack/dev/up.sh
```

脚本会创建 `kubebrain-dev` kind 集群，构建 `kubebrain:dev` 镜像，安装 TiDB Operator，部署 `TidbCluster/kb`，再部署 KubeBrain。

默认 kind 配置不固定 Kubernetes node image，由当前 kind 版本选择默认镜像。需要验证指定 Kubernetes server 版本时，先清理旧集群，再显式指定 kind node image：

```shell
hack/dev/down.sh
KIND_NODE_IMAGE=kindest/node:v1.36.1 KUBEBRAIN_REPLICAS=3 hack/dev/up.sh
```

`KIND_NODE_IMAGE` 只在新建 kind 集群时生效；已有集群不会被原地改 Kubernetes server 版本。当前环境版本可以这样记录：

```shell
hack/dev/version-info.sh
```

需要批量验证多个 Kubernetes server 版本时，可以使用矩阵脚本。它会为每个 node image 创建独立 kind 集群并运行同一套验证：

```shell
KIND_NODE_IMAGES="kindest/node:v1.35.4 kindest/node:v1.36.1" \
hack/dev/k8s-version-matrix.sh
```

矩阵脚本默认会启用 standalone kube-apiserver list/watch soak；故障注入、逻辑备份演练和直接 etcd watch soak 仍需通过 `RUN_FAULT_SMOKE=true`、`RUN_BACKUP_DRILL=true`、`RUN_WATCH_SOAK=true` 显式打开。

如果只需要快速验证 kube-apiserver 对 etcd 的调用语义，可以复用当前 KubeBrain/TiKV 环境，从官方 kube-apiserver 镜像中提取二进制运行 standalone smoke/soak：

```shell
APISERVER_IMAGES="registry.k8s.io/kube-apiserver:v1.35.4 registry.k8s.io/kube-apiserver:v1.36.1" \
hack/dev/apiserver-version-matrix.sh
```

当前本地已通过 `registry.k8s.io/kube-apiserver:v1.35.4` 和 `registry.k8s.io/kube-apiserver:v1.36.1` 的 standalone smoke 与 20x10 list/watch soak。

KubeBrain 会通过 NodePort 暴露到宿主机：

```text
127.0.0.1:3379
```

## 验证

```shell
hack/dev/smoke-etcd-client.sh
```

基础 smoke 默认总超时时间为 120 秒，可按环境调整：

```shell
SMOKE_TIMEOUT_SECONDS=180 hack/dev/smoke-etcd-client.sh
```

验证内容包括：

- Txn create
- Get
- Txn update
- 普通 Put / 覆盖写
- Lease grant / keepalive / ttl / revoke
- Watch
- Range delete
- Txn delete
- Cluster MemberList
- Maintenance Status / HashKV / Compact / AlarmList / Defragment

完整回归入口：

```shell
hack/dev/verify.sh
```

默认会依次运行：

- `go test ./...`
- 基础 etcd client smoke
- 3 副本 HA smoke
- 独立 kube-apiserver 指向 KubeBrain 的对象生命周期 smoke，覆盖 ConfigMap watch、label/field selector、chunked list、delete collection、Secret、Lease 和 Deployment 存储路径
- 3 副本 mTLS 部署下的 TLS HA smoke，包括 etcd client mTLS、standalone kube-apiserver mTLS，以及逐个删除 Pod 后的 mTLS client smoke

可以用环境变量跳过单项，例如：

```shell
RUN_TLS_SMOKE=false hack/dev/verify.sh
```

真实 k3s 外部 datastore 验证默认不运行。需要验证 k3s server 直接把 KubeBrain 当作 datastore、并确认 Kubernetes 对象真实落到 TiKV 时启用：

```shell
RUN_K3S_DATASTORE_SMOKE=true \
RUN_GO_TEST=false \
RUN_BASIC_SMOKE=false \
RUN_HA_SMOKE=false \
RUN_APISERVER_SMOKE=false \
RUN_TLS_SMOKE=false \
hack/dev/verify.sh
```

该脚本会启动临时 k3s server，写入 namespace、ConfigMap、Secret 和 Lease，通过 KubeBrain 重启与 k3s 重启后再次读取，并从 TiKV 直连校验对应 `/registry/...` 对象 key 存在且 value 非空。脚本会清理 k3s 相关 `/registry/...` 子前缀和 bootstrap key，避免历史 k3s 系统对象污染新的临时 data-dir。

需要增加真实 watch 和批量 patch 时启用：

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

需要覆盖真实 k3s delete collection 路径时启用：

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

该路径会通过 k3s apiserver 批量删除带 label 的 ConfigMap，并直连 TiKV 校验被删除对象的 revision index 已变为 tombstone。

需要覆盖真实 namespace 删除路径时启用：

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

该路径会通过 k3s apiserver 创建独立 namespace，写入 ConfigMap 和 Secret，删除 namespace，等待 namespace 消失，并直连 TiKV 校验对象和 namespace 自身的 tombstone。

逻辑备份恢复演练默认不运行，因为它会导出 `/registry` 并向隔离前缀写入同等数量的临时记录。需要覆盖该路径时显式启用：

```shell
RUN_BACKUP_DRILL=true hack/dev/verify.sh
```

3×KubeBrain、3×PD、3×TiKV 的全副本滚动重启持久性 smoke 默认不运行。它会逐个
删除并等待 9 个 Pod 恢复，同时通过官方 etcd client 验证 revision 不回退、普通值和
tombstone 不丢失、lease 附属关系保留、历史 watch 可回放，以及恢复后 revision 继续
增长：

```shell
RUN_RESTART_PERSISTENCE_SMOKE=true \
RUN_GO_TEST=false \
RUN_BASIC_SMOKE=false \
RUN_HA_SMOKE=false \
RUN_APISERVER_SMOKE=false \
RUN_TLS_SMOKE=false \
hack/dev/verify.sh
```

脚本会拒绝非精确 3/3/3 的拓扑。当前开发集群已完成一次 9 Pod 顺序重启验证，用时
68.42 秒，最终三组 StatefulSet 均 3/3 Ready。

恢复覆盖保护 smoke 默认不运行。它只会创建一个独立小前缀，导出后尝试恢复回同一前缀，并确认默认 restore 会拒绝覆盖已有 key：

```shell
RUN_RESTORE_GUARD_SMOKE=true hack/dev/verify.sh
```

内容校验保护 smoke 默认不运行。它只会创建独立小前缀，导出并恢复到隔离前缀后故意篡改恢复 value，并确认 `logical-verify` 会拒绝该恢复结果：

```shell
RUN_VERIFY_CONTENT_SMOKE=true hack/dev/verify.sh
```

备份文件完整性 smoke 默认不运行。它会截断 v2 footer，确认 restore 在写入任何目标
key 前拒绝损坏文件：

```shell
RUN_BACKUP_INTEGRITY_SMOKE=true hack/dev/verify.sh
```

备份脚本同时支持 TLS/mTLS endpoint，可通过 `ETCDCTL_CACERT`、`ETCDCTL_CERT`、
`ETCDCTL_KEY` 或 `CACERT`、`CERT`、`KEY` 传入证书。导出固定首个分页 Range
revision。`kubebrain.logical.v2` 格式包含源 prefix、snapshot revision、记录/lease
数和 SHA-256；每个 lease 记录导出时的正数剩余 TTL。制品经临时文件 `fsync` 后原子
发布；restore 为目标生成新 lease ID并保持多 key 共享关系，restore/verify 在访问目标
key 前验证完整文件。v1 无 lease 制品仍可恢复，v1 中有非零 lease 的记录会在任何写入
前拒绝，旧版无 manifest 的 JSONL 也会被拒绝。可用
`INPUT=... hack/backup/logical-status.sh` 离线查看状态。恢复按 `BATCH_SIZE` 使用一个
Txn，批内任一 overwrite compare 失败时不会部分写入。默认隔离目标使用
`/kubebrain-restore-drill-*`，不会与默认源 `/registry` 重叠。当前真实环境已通过 35 条
`/registry` 全前缀隔离恢复、逐值核验与清理；截断 footer 被拒绝且目标计数保持 0，
两条记录批次中第二条冲突时目标计数保持原有 1。

恢复默认 `BATCH_SIZE=128`、`MAX_TXN_OPS=128`；前者不得超过目标实例
`--max-txn-ops`。非覆盖模式先用批量只读 Txn 检查全部目标 key，再创建 lease/写入，
写批仍带缺失 compare 关闭预检后的竞争。overwrite guard 使用 `BATCH_SIZE=1` 把冲突
放到第二批，确认预检拒绝后第一批也没有写入。

已确认写批会记录 commit revision；后续批失败时，默认非覆盖恢复按逆序用
`ModRevision==commitRevision` compare+Delete 回滚，key 被并发修改时拒绝误删。
`ALLOW_OVERWRITE=true` 不自动回滚。真实 fault injection smoke 将 3 条记录按每批 1 条
恢复，在第 2 批提交后注入失败，要求目标 count 回到 0：

```shell
RUN_RESTORE_ROLLBACK_SMOKE=true hack/dev/verify.sh
```

lease-aware smoke 默认不运行；它创建一个永久 key 和两个共享 120 秒 lease 的 key，
要求 v2 manifest 恰有一个 lease，并验证恢复后永久 key 不带 lease、两个临时 key 共享
同一新 lease 且 TTL 为正：

```shell
RUN_LEASE_BACKUP_SMOKE=true hack/dev/verify.sh
```

生产控制面必须先对备份实现执行 fail-closed 预检：

```shell
BACKUP_MODE=logical hack/backup/production-mode-check.sh
hack/backup/production-mode-check-smoke.sh
```

目前只有 `logical` 会通过。2026-07-18 在独立 `kb-restore` PD/TiKV 集群中完成了
TiDB Operator v1.6.5 + BR v8.5.3 的 S3 full backup/Restore CR 实测：备份 131 ranges、
422188 bytes，Backup 和 Restore 均为 `Complete`，commit TS 为
`467759848010022914`；但备份前 revision `467759280324349580` 写入的
`/dbaas/a143/pre-backup` 在恢复后的 KubeBrain endpoint 中不存在。这证明成功状态只
代表 TiDB 管理数据恢复成功，不代表 KubeBrain transactional key 被备份。BR raw
一次只处理一个 RocksDB CF 且仍为实验功能，不能作为事务型 KubeBrain 的生产一致快照。

此前还完成过 4091 条 `/registry`、TLS/mTLS 4148 条及 `/registry/smoke` 712 条旧格式
演练；这些历史结果证明当时的数据路径规模，但旧文件本身不满足 v1 完整性契约，升级后
必须重新导出。`TIMEOUT` 可调整超时，默认 `10m`；所有脚本支持 `--help`。

故障注入 smoke 默认不运行，因为它会删除当前 dev 环境中的 KubeBrain、PD 和 TiKV Pod。需要覆盖该路径时显式启用：

```shell
RUN_FAULT_SMOKE=true hack/dev/verify.sh
```

lease 过期 smoke 默认不运行。它会创建短 TTL lease、绑定 key、等待 lease 到期后 key 自动删除，并确认 watch 收到 DELETE 事件：

```shell
RUN_LEASE_EXPIRY_SMOKE=true hack/dev/verify.sh
```

lease 故障 smoke 默认不运行。它会在多个短 TTL lease 活跃期间删除一个 KubeBrain Pod，等待 Deployment 恢复，并确认 lease 过期 DELETE 事件仍完整：

```shell
RUN_LEASE_FAULT_SMOKE=true hack/dev/verify.sh
```

滚动升级 smoke 默认不运行。它会在轻量并发读写运行时触发 KubeBrain Deployment rolling restart，并确认 load smoke 没有失败：

```shell
RUN_ROLLOUT_SMOKE=true hack/dev/verify.sh
```

standalone kube-apiserver 滚动升级 smoke 默认不运行。它会先启动一个独立 kube-apiserver 和 ConfigMap watch，在 watch 建立后触发 KubeBrain Deployment rolling restart，再持续 patch 对象并确认 watch/最终 list 状态：

```shell
RUN_APISERVER_ROLLOUT_SMOKE=true hack/dev/verify.sh
```

集群内 kube-apiserver smoke 默认不运行。它会把 kube-apiserver 作为临时 Pod 跑在集群内，并通过 `kubebrain.kubebrain-dev.svc:3379` 访问 KubeBrain，宿主机只通过 port-forward 访问这个临时 apiserver：

```shell
RUN_INCLUSTER_APISERVER_SMOKE=true hack/dev/verify.sh
```

集群内 kube-apiserver list/watch soak 默认不运行。它使用同样的临时 apiserver Pod，但执行批量 ConfigMap watch/list 验证：

```shell
RUN_INCLUSTER_APISERVER_WATCH_SOAK=true hack/dev/verify.sh
```

集群内 kube-apiserver 滚动升级 smoke 默认不运行。它会在临时 apiserver Pod watch 建立后触发 KubeBrain Deployment rolling restart，再验证 watch/list 完整性：

```shell
RUN_INCLUSTER_APISERVER_ROLLOUT_SMOKE=true hack/dev/verify.sh
```

watch 扇出 soak 默认不运行，因为它会创建多条 watch 流并批量写入事件。需要覆盖该路径时显式启用：

```shell
RUN_WATCH_SOAK=true hack/dev/verify.sh
```

轻量负载 smoke 默认不运行。它会并发执行 Txn create、Get、Txn update、Delete，并输出每轮操作延迟分位。需要覆盖该路径时显式启用：

```shell
RUN_LOAD_SMOKE=true hack/dev/verify.sh
```

集群内轻量负载 smoke 默认不运行。它会创建一个临时 Kubernetes Job，从 Pod 内通过 `kubebrain.kubebrain-dev.svc:3379` 访问 KubeBrain Service，避免经宿主机 NodePort：

```shell
RUN_INCLUSTER_LOAD_SMOKE=true hack/dev/verify.sh
```

集群内滚动升级 smoke 默认不运行。它会在上述 Job 运行期间触发 KubeBrain Deployment rolling restart，用于验证生产式 Service/EndpointSlice 访问路径：

```shell
RUN_INCLUSTER_ROLLOUT_SMOKE=true hack/dev/verify.sh
```

compact soak 默认不运行。它会按 Kubernetes compactor 行为通过 `compact_rev_key` CAS 记录 compact 目标 revision，再执行 etcd `Compact`，并立刻验证 `< compactRev` 的历史 Range 与 Watch 都返回 compacted；同时并发执行最新 revision 的 prefix count 读，观察 compact 期间普通 list 是否异常：

```shell
RUN_COMPACT_SOAK=true hack/dev/verify.sh
```

compact 故障 smoke 默认不运行。它会在 compact soak 运行期间删除一个 KubeBrain Pod，等待 Deployment 恢复，并确认 compact/read/watch 验证仍能完成：

```shell
RUN_COMPACT_FAULT_SMOKE=true hack/dev/verify.sh
```

standalone kube-apiserver list/watch soak 默认不运行，因为它会启动一个独立 apiserver、批量创建 ConfigMap、启动带 label selector 的 watch，再持续 patch 对象并校验 watch 事件和最终 list 状态。需要覆盖该路径时显式启用：

```shell
RUN_APISERVER_WATCH_SOAK=true hack/dev/verify.sh
```

standalone kube-apiserver 版本矩阵默认不运行。需要覆盖多个 kube-apiserver 版本时显式启用：

```shell
RUN_APISERVER_VERSION_MATRIX=true \
APISERVER_IMAGES="registry.k8s.io/kube-apiserver:v1.35.4 registry.k8s.io/kube-apiserver:v1.36.1" \
hack/dev/verify.sh
```

## 多副本验证

启动时可以直接设置副本数：

```shell
KUBEBRAIN_REPLICAS=3 hack/dev/up.sh
```

也可以在单副本环境启动后运行：

```shell
hack/dev/ha-smoke.sh
```

`ha-smoke.sh` 会把 KubeBrain scale 到 3 副本，先跑一次 smoke test，然后逐个删除当前 KubeBrain Pod，每次等待 Deployment 恢复后再跑 smoke test。这样可以覆盖 follower proxy、leader 重选和重连后的基本读写/watch/lease 路径。

这个脚本不是线性一致性测试，也不会替代后续的故障注入和 Jepsen 类测试；它是开发阶段的快速回归入口。

## 故障注入验证

```shell
hack/dev/fault-smoke.sh
```

`fault-smoke.sh` 会先跑一次基础 smoke，然后依次删除一个 KubeBrain Pod、一个 PD Pod、一个 TiKV Pod，每次等待组件恢复后重新运行基础 smoke。当前 dev TiKV/PD 是单副本，PD 或 TiKV 重启期间会有短暂不可用，脚本会重试直到恢复或超时。

## Count-Index 容灾回归（边写边杀 leader）

```shell
hack/dev/countindex-failover-smoke.sh
```

`countindex-failover-smoke.sh` 专门验证 **leader 维护的 count 索引在持续故障转移下的正确性**：在 48 个 writer 持续写入期间，按 `count_index_keys` gauge 识别当前 leader 并连续强杀 `KILLS`（默认 6）次，每次触发新 leader 重建索引。判定 oracle 是**索引服务值与全量 scan 必须一致**——`rev=0` CountOnly 由 leader 内存索引服务（~ms）时返回的计数，必须等于由 follower/未就绪 leader 走的全量 scan（慢），并且:

- 重建期间任何一次快路径（索引）读都不能返回严重偏低（近 0 / 残缺）的计数（守护 Reset 安装缺口 / 半加载被服务 / Ready-Count TOCTOU 三类 bug）；
- 写入静默后所有读（无论索引还是 scan）必须返回同一个计数；
- 存活副本零重启。

读用**每次新建连接**经 NodePort 分散到各副本（持久 gRPC 连接会固定到单个 Pod，压不到 leader 索引）。需要 `--enable-count-index=true`。可调 `KILLS`、`WRITE_SECONDS`、`QUIESCE_SECONDS`、`WORKERS`、`ENDPOINT`。已在 6 次连杀、~10 万 key 规模下验证索引与 scan 逐一对齐、零错误计数。

## Lease Expiry Smoke

```shell
hack/dev/lease-expiry-smoke.sh
```

默认创建 TTL 为 2 秒的 lease，绑定一个 key，确认 `TimeToLive` 能返回 attached key，然后等待 key 自动删除并确认 watch 收到 DELETE 事件。可以通过 `TTL_SECONDS`、`LEASES` 和 `TIMEOUT_SECONDS` 调整。

Pod 重启期间的 lease 过期行为可以单独运行：

```shell
hack/dev/lease-fault-smoke.sh
```

默认创建 10 个 TTL 为 6 秒的 lease，在 lease 活跃期间删除一个 KubeBrain Pod，等待恢复后确认所有 key 都过期删除并产生 DELETE 事件。可以通过 `TTL_SECONDS`、`LEASES`、`SETUP_WAIT_SECONDS` 和 `TIMEOUT_SECONDS` 调整。

## Watch Soak

```shell
hack/dev/watch-soak.sh
```

默认会启动 25 条 watcher，对同一个 `/registry/watch-soak/...` 前缀写入 50 个事件，并确认每条 watcher 都收到完整事件数。可以通过 `WATCHERS`、`EVENTS` 和 `TIMEOUT_SECONDS` 调整规模。

## Load Smoke

```shell
hack/dev/load-smoke.sh
```

默认会启动 8 个 worker，每个 worker 执行 25 轮 Txn create、Get、Txn update、Delete，并输出 `p50_us`、`p95_us`、`p99_us`。可以通过 `WORKERS`、`OPS_PER_WORKER` 和 `TIMEOUT_SECONDS` 调整规模。

从集群内 Pod 访问 KubeBrain Service 的负载 smoke 可以单独运行：

```shell
hack/dev/incluster-load-smoke.sh
```

它默认在 `kubebrain-dev` namespace 创建临时 Job，并通过 `kubebrain.kubebrain-dev.svc:3379` 访问 KubeBrain。可以通过 `WORKERS`、`OPS_PER_WORKER`、`TIMEOUT_SECONDS` 和 `JOB_TIMEOUT_SECONDS` 调整。

集群内 Service 路径的滚动升级 smoke 可以单独运行：

```shell
hack/dev/incluster-rollout-smoke.sh
```

## Compact Soak

```shell
hack/dev/compact-soak.sh
```

默认会执行 20 轮 compact 验证，并启动 4 个并发 reader 反复做最新 revision prefix count。compact 路径对齐 Kubernetes `/root/kubernetes/staging/src/k8s.io/apiserver/pkg/storage/etcd3/compact.go` 的 `compact_rev_key` CAS 行为。可以通过 `ITERATIONS`、`READERS` 和 `TIMEOUT_SECONDS` 调整规模。预生产应扩大该脚本规模，并在 KubeBrain leader 切换、apiserver watch 重连和 TiKV/PD 故障场景下重复运行。

compact 与 KubeBrain Pod 重启叠加可以单独运行：

```shell
hack/dev/compact-fault-smoke.sh
```

默认会执行 60 轮 compact soak，在运行期间删除一个 KubeBrain Pod。可以通过 `ITERATIONS`、`READERS`、`SETUP_WAIT_SECONDS` 和 `TIMEOUT_SECONDS` 调整。

经 kube-apiserver 的 list/watch soak 可以单独运行：

```shell
hack/dev/apiserver-watch-soak.sh
```

默认会创建 20 个 ConfigMap，每个对象 patch 10 次，并通过 standalone kube-apiserver watch 验证至少收到 200 个 `MODIFIED` 事件。可以通过 `OBJECTS`、`UPDATES` 和 `WATCH_TIMEOUT_SECONDS` 调整规模。

经 kube-apiserver 的滚动升级 watch smoke 可以单独运行：

```shell
hack/dev/apiserver-rollout-smoke.sh
```

该脚本默认创建 12 个 ConfigMap、每个对象 patch 6 次，并在 watch 建立后重启 KubeBrain。因为本地 dev 环境通过 kind NodePort 暴露 `127.0.0.1:3379`，rollout 时命中正在退出 Pod 的连接可能被重置；脚本会在该场景下允许外层 watch 客户端重连，但仍要求全部 `MODIFIED` 事件和最终对象状态通过。普通 `apiserver-watch-soak.sh` 和 k3s soak 默认仍保持严格连续 watch；需要模拟可重连客户端时可以显式设置 `ALLOW_WATCH_RESTARTS=1`，该开关只接受 `0` 或 `1`。

经集群内 kube-apiserver Pod 的对象生命周期 smoke 可以单独运行：

```shell
hack/dev/incluster-apiserver-smoke.sh
```

该脚本会创建临时 kube-apiserver Pod 和 Service，apiserver 自身通过 KubeBrain Service DNS 访问后端，宿主机只通过 port-forward 访问该临时 apiserver。它用于排除 kind NodePort 对 apiserver -> KubeBrain 链路的影响。

经集群内 kube-apiserver Pod 的 list/watch soak 可以单独运行：

```shell
hack/dev/incluster-apiserver-watch-soak.sh
```

默认创建 20 个 ConfigMap、每个对象 patch 10 次，并通过临时 apiserver 的 watch 校验 200 个 `MODIFIED` 事件。

经集群内 kube-apiserver Pod 的滚动升级 smoke 可以单独运行：

```shell
hack/dev/incluster-apiserver-rollout-smoke.sh
```

默认创建 12 个 ConfigMap、每个对象 patch 6 次，在 watch 建立后重启 KubeBrain，并通过可重连 watch 验证 72 个对象版本更新和最终 list 状态。watch 断开后重新 list/watch 时，当前对象可能以 `ADDED` 形式重新出现；脚本按对象名和版本统计 `ADDED`/`MODIFIED` 中的目标更新，同时仍输出 `modified_events` 便于观察是否发生过 relist。

TLS 路径可以单独验证：

```shell
IMAGE_NAME=kubebrain:dev hack/dev/tls-smoke.sh
```

`tls-smoke.sh` 会临时创建 namespace、CA、client/peer 证书和 3 副本 KubeBrain mTLS 部署，完成基础 Put/Get、standalone kube-apiserver TLS smoke、TLS logical backup drill 后逐个删除原始 Pod，并在每次恢复后重新运行 mTLS client smoke。默认备份 drill 只覆盖脚本自有的 `/registry/tls-smoke` 前缀；需要做 TLS 全量 `/registry` 备份恢复演练时显式设置 `BACKUP_PREFIX=/registry`。

脚本还会启动 standalone kube-apiserver，通过 `--etcd-cafile`、`--etcd-certfile`、`--etcd-keyfile` 指向 mTLS KubeBrain endpoint，验证 ConfigMap watch、label/field selector、chunked list、delete collection、Secret、Lease 和 Deployment 存储路径。

## 清理

```shell
hack/dev/down.sh
```

## 后续实现路线

建议按下面顺序推进，每一步都先补 smoke/integration 测试，再改实现：

1. 补 `pkg/server/etcd` 的兼容层单测，覆盖 kube-apiserver 实际会发出的 Txn/Range/Watch 形态。
2. 实现普通 `Put` 和 `DeleteRange`，或明确只支持 kube-apiserver storage path 并在文档中禁止通用 etcd 用法。
3. 补齐 lease 语义，至少实现 `LeaseGrant`、`LeaseKeepAlive`、`LeaseTimeToLive`、`LeaseRevoke` 和 key 绑定关系。
4. 做 kube-apiserver 真实接入测试：启动 kube-apiserver 指向 KubeBrain，创建/更新/删除核心对象。
5. 多副本 KubeBrain + 单 TiKV 集群，验证 leader 切换、follower proxy、watch 重连和 revision 同步。
6. 故障注入：重启 KubeBrain、重启 PD/TiKV、网络中断、长 watch、compact 后 relist。
7. 设计一致性测试，再考虑 Jepsen 或同类线性一致性测试。

## 注意事项

- 当前 `--key-prefix "/"` 会被代码拒绝；开发环境暂不设置 `--key-prefix`。
- 这套环境是开发和回归用，不代表可以直接用于生产。
- `deploy/dev/tidb-cluster.yaml` 为单副本 TiKV/PD，适合功能开发，不适合验证高可用。
