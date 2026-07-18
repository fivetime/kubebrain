# KubeBrain + TiKV 产品生产就绪验证清单

这份清单只关注 KubeBrain 产品本身能否稳定、有效地替代 etcd：etcd v3 API 兼容性、Kubernetes/k3s apiserver 存储路径、TiKV 后端一致性、watch/lease/compact 行为、故障恢复和备份恢复。节点规格、反亲和、NetworkPolicy、镜像策略、资源限额等属于集群运维决策，不作为这里的产品就绪判断。

## 验证边界

- 本地 kind、standalone kube-apiserver、临时 in-cluster kube-apiserver 只能证明一部分 API 行为。
- 判断能否替代 etcd，最终必须把真实 Kubernetes/k3s apiserver 的 `--etcd-servers` 指向 KubeBrain，在真实 TiKV/PD 后端上跑对象生命周期、list/watch、lease、compact、apiserver 重启和 KubeBrain leader 切换验证。
- `deploy/dev` 只用于本地验证；`deploy/production` 是经过结构化测试的高可用基线，但
  平台仍必须注入生产镜像、StorageClass、跨可用区调度、网络策略、证书和监控栈，不能
  不经环境适配直接发布。

## TiKV/PD 升级完成门槛

TiDB Operator 通过 StatefulSet `rollingUpdate.partition` 逐成员协调 PD、TiKV 升级。
`kubectl rollout status` 在只更新一个成员时也可能输出
`partitioned roll out complete`，不能作为 DBaaS 控制面宣告升级完成的依据。发布或
配置变更后必须运行：

```shell
KUBE_CONTEXT=production \
NAMESPACE=tidb-cluster \
TIDB_CLUSTER=kb \
TIMEOUT_SECONDS=1800 \
hack/production/wait-tidbcluster-ready.sh
```

等待器同时要求：

1. TidbCluster 的 `Ready` condition 为 `True`；
2. PD/TiKV StatefulSet 的 generation 已被 controller 观察；
3. desired、ready、updated 副本数相等且大于零；
4. `currentRevision` 与 `updateRevision` 相等。

任一条件超时都会返回非零并打印 CR 与两个 StatefulSet 的诊断信息。控制面随后仍应
执行 KubeBrain endpoint health 和实际 Put/Get/Delete；资源收敛不单独证明数据面语义
健康。

## 生产镜像追踪

Docker 构建不会把 `.git` 复制到镜像上下文，因此版本、完整 commit SHA 和 UTC 构建时间
必须由 CI 显式注入，缺少任一字段时 Dockerfile 会在编译前失败：

```shell
docker build \
  --build-arg STORAGE=tikv \
  --build-arg "KUBEBRAIN_VERSION=${RELEASE_VERSION}" \
  --build-arg "KUBEBRAIN_GIT_SHA=${GIT_COMMIT}" \
  --build-arg "KUBEBRAIN_BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -t "${IMAGE_REPOSITORY}:${RELEASE_VERSION}" .
```

`KUBEBRAIN_GIT_SHA` 必须是 40 位十六进制完整 commit ID。构建结果同时把三个值写入
`kube-brain version` 和 OCI `org.opencontainers.image.version/revision/created`
labels；发布门槛必须比较两处值，并确认 revision 对应 CI checkout，而不是仅检查镜像
tag。

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

## 实例发布门禁

控制面在创建、扩缩或升级实例后，不能只把 Kubernetes rollout 完成当作成功。必须传入
期望镜像、拓扑和 endpoint 执行：

```shell
KUBE_CONTEXT=production \
KUBEBRAIN_NAMESPACE=kubebrain-instance-a \
EXPECTED_IMAGE=registry.example/kubebrain@sha256:<digest> \
TIDB_NAMESPACE=kubebrain-storage-a \
TIDB_CLUSTER=kb \
EXPECTED_KUBEBRAIN_REPLICAS=3 \
EXPECTED_PD_REPLICAS=3 \
EXPECTED_TIKV_REPLICAS=3 \
ENDPOINT=https://instance-a.example:2379 \
ETCDCTL_CACERT=/run/secrets/ca.crt \
ETCDCTL_CERT=/run/secrets/client.crt \
ETCDCTL_KEY=/run/secrets/client.key \
  hack/production/validate-instance-ready.sh
```

门禁先要求 TidbCluster `Ready=True` 且 PD/TiKV StatefulSet generation、ready/updated
replicas 和 revision 全部收敛，再校验期望 PD/TiKV 数量；随后要求 KubeBrain
StatefulSet observed generation、ready/updated replicas、revision 和精确 image 全部
匹配，最后通过官方 `etcdctl endpoint health` 提交线性化 proposal。缺少
`EXPECTED_IMAGE`/`ENDPOINT`、任一状态缺失、旧 revision、错误拓扑、错误镜像或 endpoint
不健康都会 fail closed。

生产必须使用 image digest；脚本做精确字符串比较，允许本地验证使用不可变测试 tag，
但不会替控制面判断 tag 是否可变。该门禁可关闭创建/扩缩/升级的“数据面已就绪”阶段，
不能替代备份、恢复和销毁各自的幂等状态机与回滚证据。

## 证书轮换完成门禁

client/peer CA rollover 必须按 `begin`、`overlap`、`complete` 三阶段执行
`hack/production/validate-certificate-rotation.sh`。控制面先持久化旧、新 CA、client
证书和私钥，再使用同一个 `ROTATION_ID`、`INSTANCE`、`STATE_DIR` 和 endpoint 调用：

```shell
common_env=(
  ROTATION_ID=instance-a-20260718
  INSTANCE=instance-a
  STATE_DIR=/var/lib/kubebrain-operations/certificate-rotations
  KUBEBRAIN_NAMESPACE=kubebrain-instance-a
  EXPECTED_REPLICAS=3
  ENDPOINT=https://instance-a.example:2379
  OLD_CACERT=/run/rotation/old-ca.crt
  OLD_CERT=/run/rotation/old-client.crt
  OLD_KEY=/run/rotation/old-client.key
  OVERLAP_CACERT=/run/rotation/overlap-ca.crt
  NEW_CACERT=/run/rotation/new-ca.crt
  NEW_CERT=/run/rotation/new-client.crt
  NEW_KEY=/run/rotation/new-client.key
  RECEIPT_OUTPUT=/var/lib/kubebrain-operations/instance-a-20260718.json
)
env "${common_env[@]}" ACTION=begin hack/production/validate-certificate-rotation.sh
# 发布 old+new CA trust bundle 后：
env "${common_env[@]}" ACTION=overlap hack/production/validate-certificate-rotation.sh
# 切换叶证书并从 trust bundle 删除 old CA 后：
env "${common_env[@]}" ACTION=complete hack/production/validate-certificate-rotation.sh
```

`begin` 要求全部 Pod Ready，记录排序后的 Pod name、UID、`kubebrain` container restart
count，并用旧凭据完成 `endpoint health`。`overlap` 要求 Pod 快照完全不变，且旧、新
client 凭据都能通过 overlap bundle 提交 proposal。`complete` 再次固定 Pod 身份和
重启计数，要求新凭据成功、旧凭据失败，随后立刻用新凭据复检；因此 endpoint outage
不能冒充旧证书撤销。任一阶段次序错误、rotation 参数/证书指纹变化、Pod 替换/重启、
副本不 Ready 或证据冲突都 fail closed。

完成后原子发布 `kubebrain.certificate-rotation.receipt.v1`，绑定 instance、
rotation ID、endpoint、replicas、旧/新证书 SHA-256、完成时间，并明确记录
`pods_unchanged=true` 和 `old_certificate_rejected=true`。状态与 receipt 使用 0600
权限、临时文件 `fsync`、不可覆盖 hard-link 和目录同步；同输入重试会重做在线检查并
复用原 receipt。`STATE_DIR` 和 receipt 必须位于持久、受访问控制的操作记录卷，完成后
再归档到不可变审计存储。该门禁验证数据面完成条件，不替控制面实现 Secret 发布超时、
阶段回滚或跨实例任务调度。

## 实例销毁状态机

生产清单中的 KubeBrain 与 TidbCluster 都固定携带 `app.kubernetes.io/instance`
所有权标签；StatefulSet、Pod、Service 和 PDB selector 同步包含该标签。模板化时必须
替换为 DBaaS instance ID。销毁不得调用 namespace 级泛删，也不得只按资源名称执行
`kubectl delete`，因为后者不带 UID precondition，可能删除同名重建的新实例。

控制面使用同一组参数依次执行：

```shell
common_env=(
  OPERATION_ID=instance-a-delete-20260718
  INSTANCE=instance-a
  STATE_DIR=/var/lib/kubebrain-operations/destroy
  BACKUP_INPUT=/backup/instance-a-final.jsonl
  BACKUP_PREFIX=/registry
  BACKUP_MAX_AGE_SECONDS=3600
  BACKUP_MIN_RECORDS=1
  KUBE_CONTEXT=production
  KUBEBRAIN_NAMESPACE=kubebrain-instance-a
  KUBEBRAIN_STATEFULSET=kubebrain
  TIDB_NAMESPACE=kubebrain-storage-a
  TIDB_CLUSTER=instance-a
  EXPECTED_PVCS=6
)
env "${common_env[@]}" ACTION=prepare hack/production/destroy-instance.sh

confirm=destroy:instance-a:instance-a-delete-20260718
env "${common_env[@]}" ACTION=quiesce CONFIRM_DESTROY="$confirm" \
  hack/production/destroy-instance.sh
env "${common_env[@]}" ACTION=destroy CONFIRM_DESTROY="$confirm" \
  hack/production/destroy-instance.sh
env "${common_env[@]}" ACTION=complete CONFIRM_DESTROY="$confirm" \
  RECEIPT_OUTPUT=/var/lib/kubebrain-operations/instance-a-delete-20260718.json \
  hack/production/destroy-instance.sh
```

`prepare` 先通过 `logical-status` 完整校验 `kubebrain.logical.v2` artifact 的 prefix、
最少记录数、受 SHA-256 保护的创建时间和最大年龄，再记录 artifact digest/revision；
随后固定 KubeBrain StatefulSet、client/peer Service、PDB、ServiceAccount、
TidbCluster、PD/TiKV PDB/metrics Service 及每块 PD/TiKV PVC 的 UID。PVC 数量必须精确
等于 `EXPECTED_PVCS`，component 只能是 `pd` 或 `tikv`。

`quiesce` 只有在确认令牌精确等于 `destroy:<INSTANCE>:<OPERATION_ID>` 时才把
KubeBrain StatefulSet 缩到 0，并等待 ready replicas 与实例 Pod 都归零。备份最大年龄
表示该套餐接受的销毁 RPO；状态机不把“最近一小时备份”宣称成零数据损失。要求 RPO=0
时，控制面必须先 fence 客户流量，在停写窗口导出最终 artifact，再调用 prepare。

`destroy` 再次核对所有仍存在资源的 UID，并拒绝任何新出现或同名换 UID 的实例 PVC。
删除由 `hack/production/cmd/uid-delete` 直接发送 Kubernetes
`DeleteOptions.preconditions.uid` 与 foreground propagation；中断重试允许资源已经
不存在，但名称复用立即失败。最终还要求全部固定资源、实例 PVC、PD/TiKV Pod 和
StatefulSet 均为空。`complete` 重复 absence gate 后原子发布
`kubebrain.destroy.receipt.v1`，绑定 instance、operation ID、两个 namespace、
TidbCluster 名、备份 digest/revision 和完成时间；同输入重试复用原 receipt。

脚本刻意不删除 namespace、TLS Secret、外部对象存储 artifact、监控规则或控制面账单
记录：namespace 可能共享，而审计/备份数据必须按独立保留策略处理。平台只有在 receipt
归档到不可变审计存储并完成外围资源清单对账后，才能删除专属 namespace 和凭据。

## 备份恢复生产边界

当前唯一通过端到端恢复验证的生产备份模式是 `kubebrain.logical.v2`。制品包含固定
snapshot revision、源 prefix、记录数和 SHA-256，先写临时文件并 `fsync` 后原子发布；
lease 元数据记录剩余 TTL；restore 为目标生成新 lease ID，同时保持 key 关联和共享
关系。restore 在写目标前完整校验，并默认拒绝覆盖已有 key。每次发布备份配置前必须通过：

```shell
BACKUP_MODE=logical hack/backup/production-mode-check.sh
```

生产备份 Job 必须设置 `METRICS_OUTPUT`，将成功结果写入 node-exporter 或等价
Prometheus textfile collector 的共享目录；`BACKUP_INSTANCE` 必须与实例名一致，
生产清单默认使用 `kubebrain`。指标文件仅在 artifact 已原子提交且可 `stat` 后原子
替换，失败导出不会刷新上一次成功时间：

```shell
METRICS_OUTPUT=/var/lib/node_exporter/textfile_collector/kubebrain-backup.prom \
BACKUP_INSTANCE=kubebrain \
ENDPOINT=127.0.0.1:3379 PREFIX=/registry \
OUTPUT=/backup/kubebrain-logical-backup.jsonl \
  hack/backup/logical-export.sh
```

控制面必须采集 `kubebrain_logical_backup_last_success_timestamp_seconds`、
`kubebrain_logical_backup_artifact_bytes`、`kubebrain_logical_backup_records`、
`kubebrain_logical_backup_leases` 和 `kubebrain_logical_backup_snapshot_revision`。
生产规则在成功指标缺失 1 小时后 warning，最近成功备份超过 25 小时且持续 15 分钟后
critical。25 小时阈值为每日备份留出 1 小时调度抖动，不代表所有套餐都采用同一 RPO；
更严格套餐必须下调规则。

### 对象存储不可变发布与保留删除

本地 artifact 完成不等于异地备份完成。生产对象存储 bucket 必须启用 versioning 和
S3 Object Lock；上传角色需要 Put/Get/Head/GetObjectRetention 权限，删除角色应独立
授予 DeleteObjectVersion，且工具不会发送 governance bypass。对象发布使用独立模块
`hack/backup/objectstore`：

```shell
retain_until=$(( $(date +%s) + 2592000 ))
ACTION=upload \
S3_ENDPOINT=https://s3.example.internal \
OBJECT_STORE_ID=primary-backup-account \
S3_BUCKET=kubebrain-backups \
S3_OBJECT_KEY=instance-a/backup-20260718.jsonl \
S3_FORCE_PATH_STYLE=false \
AWS_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=... \
AWS_SECRET_ACCESS_KEY=... \
INPUT=/backup/instance-a.jsonl \
INSTANCE=instance-a \
BACKUP_ID=backup-20260718 \
EXPECTED_PREFIX=/registry \
MIN_RECORDS=1 \
MAX_AGE_SECONDS=3600 \
RETENTION_MODE=COMPLIANCE \
RETAIN_UNTIL_UNIX="$retain_until" \
RECEIPT_OUTPUT=/var/lib/kubebrain-operations/backup-20260718.object.json \
  hack/backup/logical-object.sh
```

`OBJECT_STORE_ID` 是控制面分配给对象存储账户的稳定 ID，不能使用可能变化的 endpoint
DNS 代替。`RETAIN_UNTIL_UNIX` 必须由控制面固定为绝对时间，不能在每次重试时用相对
秒数重新计算。
上传前工具只接受完整 `kubebrain.logical.v2`，并校验 exact prefix、记录下限和受 artifact
SHA-256 保护的创建时间。S3 Put 使用 `If-None-Match: *`，因此并发调用不能覆盖同 key；
冲突时仅在现有 version 的大小和全部 KubeBrain metadata 一致时进入幂等复核。

Put 成功后不能依赖 ETag 或 metadata：工具按返回的 version ID 下载完整对象到临时文件，
重新执行 artifact parser/SHA-256/record 校验，再读取远端 Object Lock mode 和
retain-until。全部一致后才原子发布 `kubebrain.object-backup.receipt.v1`，绑定 instance、
backup ID、object store ID、bucket/key/version、artifact format/digest/revision/时间/
记录数/lease 数、对象字节数和保留策略。已有 receipt 的重试会重新下载该精确 version
并复核 retention，不会产生新 version 或移动保留期限。

保留删除必须读取上传 receipt，并使用独立确认令牌：

```shell
ACTION=delete \
S3_ENDPOINT=https://s3.example.internal \
OBJECT_STORE_ID=primary-backup-account \
S3_FORCE_PATH_STYLE=false \
AWS_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=... \
AWS_SECRET_ACCESS_KEY=... \
RECEIPT_INPUT=/var/lib/kubebrain-operations/backup-20260718.object.json \
DELETE_RECEIPT_OUTPUT=/var/lib/kubebrain-operations/backup-20260718.deleted.json \
DELETE_CONFIRM=delete:instance-a:backup-20260718 \
  hack/backup/logical-object.sh
```

删除前重新核对 exact version 的大小、artifact digest 和远端 retention；retain-until
未到直接失败，到期后仅删除 receipt 指定的 version，并以 Head 确认该 version 不可读后
发布 `kubebrain.object-backup-deletion.receipt.v1`。删除后崩溃重试可根据“到期且精确
version 已不存在”补发/复用 receipt；保留期内提前消失则 fail closed。两个 receipt
都必须归档到不可变审计存储。bucket 生命周期规则只能作为调度器，不能替代该完成证据。

`deploy/production/monitoring.yaml` 还以 1 分钟周期生成实例级计量序列：

- `kubebrain_dbaas:cpu_usage_cores:sum`；
- `kubebrain_dbaas:memory_working_set_bytes:sum`；
- `kubebrain_dbaas:network_receive_bytes_per_second:sum` 和
  `kubebrain_dbaas:network_transmit_bytes_per_second:sum`；
- `kubebrain_dbaas:storage_provisioned_bytes:sum` 和
  `kubebrain_dbaas:storage_used_bytes:sum`；
- `kubebrain_dbaas:logical_backup_artifact_bytes:last` 和
  `kubebrain_dbaas:logical_backup_age_seconds:last`。

每条序列固定带 `dbaas_instance="kubebrain"`，只聚合本实例的 3 个 KubeBrain、
3 个 PD、3 个 TiKV 容器和 6 个存储 PVC。部署模板化时必须同步替换实例标签、Pod/PVC
选择器和备份 `BACKUP_INSTANCE`。这些序列是计量输入，不是最终账单；控制面必须另外
实现缺测处理、不可变采样留存、跨周期积分、价格版本和审计对账。

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

非覆盖恢复会先用只读 Txn 按批检查全部目标 key，再创建 lease 和写数据；每个写批仍
保留 `Version(key)==0` compare，关闭预检后的并发写竞争。`BATCH_SIZE` 不得超过目标
实例 `--max-txn-ops`，通过 `MAX_TXN_OPS` 告知恢复工具；两者默认都是 128。etcd 对
Txn 操作数取 compare/success/failure 三组长度的最大值，因此 128 compare + 128 Put
符合默认上限。

默认非覆盖恢复还记录每个成功批的 key 和响应 revision。后续批失败时按逆序执行
`ModRevision(key)==batchRevision` 条件删除；并发修改会使整批回滚拒绝，不会误删新数据。
`ALLOW_OVERWRITE=true` 无法仅靠删除重建旧 value/lease，因此明确不自动回滚。若提交
成功但响应丢失，该批不在已确认列表中，仍属于需要人工核对的事务不确定结果。

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

当前格式为 `kubebrain.logical.v2`：首行 manifest 固定源 prefix、snapshot revision
与导出开始时间，
lease 行固定源 ID 和导出时的正数剩余 TTL，尾行记录/lease 总数和覆盖 manifest/全部
记录的 SHA-256。导出先写同目录临时文件，完成
`fsync` 后原子 rename 并同步父目录；中断导出不会把不完整内容发布到目标路径。
`logical-status.sh`、restore 和 verify 都会先复制并验证完整文件，缺 footer、记录数
不符、内容篡改或 footer 后附加数据均 fail closed。restore 在任何 etcd 写入前完成
验证，并按 `BATCH_SIZE` 把 compare 与 Put 放入同一个 Txn，使单批冲突不会部分落盘。
恢复为每个源 lease 生成新目标 ID并保留多 key 共享关系。v1 无 lease 制品继续可恢复；
v1 中记录非零 lease 时因缺少 TTL 元数据会在任何写入前拒绝。没有 manifest/footer 的
旧 JSONL 无法证明完整性，同样明确拒绝。

新导出的 v2 manifest 还包含 `created_at_unix`，它与 snapshot revision、prefix 和
records 一起进入 footer SHA-256；控制面不能修改时间戳来伪造 RPO。备份 Job 上传
artifact 后，必须对最终下载对象执行完成门禁，例如每日 `/registry` 备份：

```shell
INPUT=/backup/kubebrain-logical-backup.jsonl \
EXPECTED_PREFIX=/registry \
MIN_RECORDS=1 \
MAX_AGE_SECONDS=90000 \
  hack/backup/logical-status.sh
```

该命令依次验证整个 artifact、精确 source prefix、最小记录数和受保护创建时间。旧 v1
以及没有 `created_at_unix` 的早期 v2 仍可 inspect/restore，但设置
`MAX_AGE_SECONDS` 时会 fail closed，不能使用文件 mtime 作为替代证据。时间戳超过
Prometheus/控制面时钟 5 分钟也会拒绝；生产节点必须保持时间同步。

默认非覆盖恢复还会在写入前以批量只读 Txn 扫描所有目标 key；已存在 key 会使整个恢复
在创建 lease 或 Put 前终止。该预检按 `BATCH_SIZE` 分批，不产生逐 key 网络往返；
写批的 compare 仍用于防止预检后的并发创建。若实例调整了 `--max-txn-ops`，必须将同一
值通过 `MAX_TXN_OPS` 传给恢复脚本。

后续写批失败时，默认非覆盖恢复会按成功响应 revision 逆序条件删除已确认批；只有 key
仍停留在恢复写入 revision 时才会删除，并发更新会使回滚 fail closed。真实回滚演练：

```shell
ENDPOINT=127.0.0.1:3379 hack/backup/restore-rollback-smoke.sh
```

控制面宣布 restore 成功前必须再次逐值 verify，并为 restore operation ID 使用唯一
receipt 路径：

```shell
ENDPOINT=https://instance.example:2379 \
INPUT=/backup/kubebrain-logical-backup.jsonl \
REWRITE_FROM=/registry \
REWRITE_TO=/registry-restore-operation-123 \
RECEIPT_OUTPUT=/audit/restore-operation-123.json \
  hack/backup/logical-verify.sh
```

只有 artifact 完整性、每个目标 key/value、永久/lease 绑定关系和目标 lease 正 TTL
全部通过后，工具才原子发布 `kubebrain.restore-verification.v1`。receipt 绑定 artifact
format/SHA-256/snapshot revision/创建时间、源/目标 prefix、record/lease count 和验证
时间，不记录 endpoint 或证书。启用 receipt 且 rewrite 时，`REWRITE_FROM` 必须精确等于
artifact prefix，禁止对子树验证后声称完成整份恢复。

receipt 使用同目录临时文件、`fsync`、原子 hard-link 和目录 `fsync`，目标已存在时拒绝
覆盖；控制面必须把 operation ID receipt 写入不可变审计存储。receipt 证明
`verified_at_unix` 时刻的状态，不保证目标之后未被其他客户端修改；切换业务流量前仍应
执行访问冻结或 revision fencing。

恢复实例进入业务流量前使用 `hack/production/switch-restore-traffic.sh`。该门禁按
`prepare -> cutover -> verify -> complete` 执行；失败可在 complete 前执行
`rollback`。prepare 要求 Service selector 恰好为
`app.kubernetes.io/name=kubebrain` 与源 instance，冻结 Service UID/resourceVersion、
源和目标全部 Ready Pod 的 name/UID/restart count，以及
`kubebrain.restore-verification.v1` 的 artifact hash/revision/prefix。cutover 使用
JSON Patch `test` 同时比较 Service UID、resourceVersion 和旧 instance selector，再
原子替换 selector；随后要求 EndpointSlice 由同一 Service UID 控制，全部 endpoint
Ready/Serving/非 Terminating，且 targetRef Pod UID 集精确等于冻结的目标 Pod UID 集。

verify 和 complete 都通过公开 Service endpoint 对完整 logical artifact 再做逐 key/value
及 lease 校验，结果的前九个稳定字段必须与 prepare receipt 一致。complete 才签发不可
覆盖的 `kubebrain.restore-cutover.receipt.v1`；receipt 还包含 cutover state 文件
SHA-256，将冻结的 Service/Pod UID 行绑定到完成证据。rollback 使用相同 CAS 从目标切回源，并
要求 EndpointSlice 精确恢复到冻结的源 Pod UID 集；已 complete 的 operation 禁止回滚，
已 rollback 的 operation 禁止 complete。所有状态、marker 和 receipt 均为 0600、
file/directory `fsync` 且不覆盖发布。

切流完成后使用 `hack/production/audit-restored-instance.sh` 运行持续观察窗口。默认持续
3600 秒、间隔 60 秒且至少 10 个样本；生产控制面应按实例 SLO 调大窗口。脚本先核对
A189 state SHA-256 与 receipt，再在每个样本前后检查 Service UID/精确 selector、目标
Pod name/UID/restart/Ready 快照和 EndpointSlice targetRef UID 集。每个样本经公开 endpoint
执行 60 秒 lease grant、`createRevision=0` 条件 Put、线性 Get（核对 value 与 lease）、
value 条件 Delete、删除确认和 lease revoke；探针 key 使用加密随机 nonce，失败时也由
lease 限制残留时间。跨样本 revision 必须单调不降，持续时间使用单调时钟计算。

窗口内任一拓扑或数据检查失败都不发布成功凭据。完整持续时间及最少样本均满足后，才以
0600、file/directory `fsync`、不可覆盖 hard-link 发布
`kubebrain.post-restore-audit.receipt.v1`，记录 cutover operation、artifact/state 身份、
窗口、样本数及首末 revision。已有 receipt 的重试仍会重新执行一次完整拓扑检查与真实
数据探针。KubeBrain 网关共享同一 TiKV MVCC 后端，不存在 etcd 各成员独立 backend；
因此不能用成员间 `endpoint hashkv` 代替上述端到端审计。

### DBaaS 持久操作 API

生产管理集群先安装 `deploy/production/kubebrain-operation-crd.yaml`，operation worker
使用 `deploy/production/kubebrain-operation-worker-rbac.yaml`。namespaced
`KubeBrainOperation.dbaas.kubebrain.io/v1alpha1` spec 包含稳定 operation ID、instance、
操作类型、完整参数文件 SHA-256 和 maxAttempts，并由 CEL 保证创建后不可变。当前类型
覆盖 Backup、RestoreCutover、PostRestoreAudit、CertificateRotation 和 Destroy。

`hack/production/cmd/operationctl` 提供 submit、claim、heartbeat、retry、succeed、fail
和 get。claim 按创建时间稳定排序，通过 status resourceVersion CAS 从 Pending 或租约
过期的 Running 中认领；每次认领递增 attempt。owner+attempt 是 fencing token，旧 worker
在接管后不能 heartbeat、retry 或提交终态。heartbeat 延长 lease；retry 清除 owner/
lease 回到 Pending 但保留已消耗 attempt；达到 maxAttempts 的 operation 由下一次扫描
CAS 标记 Failed。Succeeded/Failed 终态由 CRD admission 保证不可变。同 owner/attempt/
receipt 的 finish 可幂等重试，不同结果不能覆盖。

worker RBAC 只允许 get/list/watch operation 及 get/update/patch status，不允许 create、
delete 或修改 spec；submit 权限只应授予管理面 API 身份。成功状态必须记录不可变操作
receipt 的 SHA-256。`hack/production/run-post-restore-audit-operation.sh` 已把 A190 接入：
只 claim PostRestoreAudit，核对参数 JSON 摘要，在子审计运行期间续租；heartbeat 失败会
终止本地进程，审计失败 requeue，成功才将 receipt 摘要写入 Succeeded。

`hack/production/run-backup-operation.sh` 接入受保护 Backup。参数文件固定 endpoint、
prefix、operation 专属 artifact/receipt 路径、分页大小、Object Store ID、bucket/object
key、绝对 retain-until、retention mode 与 completion gate。首次执行导出逻辑 v2
artifact；崩溃重试若 artifact 已存在则不覆盖，而是重新校验 exact prefix、最少记录数
与 freshness 后继续。Object Lock upload 会重新下载 exact version 并核对 digest、
revision、records、retention，成功后 operation status 绑定 object receipt SHA-256。
整个导出/上传期间维持 operation heartbeat；失败 requeue，fencing 时终止本地流程。
S3 access key/secret 和 etcd TLS 凭据只通过 worker Secret/env 注入，不进入参数文件或 CR。

`hack/production/run-restore-cutover-operation.sh` 接入 RestoreCutover。参数绑定 A184
restore receipt、logical artifact、A189 state/receipt 路径、Service、源/目标 instance、
replicas、公开 endpoint 和 Kubernetes context。执行器按 prepare、cutover、verify、
complete 驱动，每阶段独立续租。prepare 失败可 retry；从 cutover 调用开始，任何失败都
必须执行 rollback 并写 Failed 终态，避免已改 selector 的操作被当成普通重试。

worker 接管时依据 A189 持久证据恢复：只有 state 从 cutover 继续，有 cutover marker 从
verify 继续，已有 receipt 则重做 complete 在线复检后提交；rollback marker 直接记 Failed。
heartbeat fencing 时旧 worker 终止子进程且不再回滚或提交，由新 owner/attempt 接管。
A189 rollback 允许仅凭 prepare state 运行：这覆盖 Service JSON Patch 已提交、但等待
EndpointSlice 失败而尚未生成 cutover marker 的窗口；rollback 仍用 UID/resourceVersion
CAS 并要求源 Pod UID 集恢复。

`hack/production/run-certificate-rotation-operation.sh` 接入 CertificateRotation。
参数同时绑定旧/新/overlap CA、client cert/key 路径及每个文件的 SHA-256，executor 在
任何发布前重新计算内容摘要，防止固定路径被替换。服务端 Secret 发布不由 CR 提供命令；
worker 镜像配置受控、可执行且必须幂等的 `PUBLISH_OVERLAP_COMMAND` 与
`PUBLISH_FINAL_COMMAND`。hook 只接收固定 operation/instance/参数文件环境。

完整顺序为 begin gate、发布双 CA、overlap gate、发布仅新 CA/叶证书、complete gate。
state-only 接管从双 CA 发布继续，overlap marker 从最终发布继续，已有 receipt 则重做
complete 在线验证。任一步失败 requeue；新 owner/attempt 可安全重跑幂等 hook。旧 worker
heartbeat fencing 后立即停止，不发布后续 Secret或提交状态。complete 仍要求新凭据成功、
旧凭据失败及新凭据再次成功，之后 operation Succeeded 绑定 A185 receipt SHA-256。
A185 也支持显式 `KUBECONFIG_PATH`。

所有 operation executor 的 heartbeat 均使用独立续租进程，主进程直接 `wait` 工作子进程；
工作结束后终止 heartbeat。续租失败时 heartbeat 杀掉工作进程并返回 fencing 状态。禁止
使用 `kill -0` 轮询工作进程完成，因为未 wait 的 zombie 仍可能返回存在并造成无限续租。

参数文件不得包含私钥内容；TLS 凭据由 worker Secret/env 提供。参数文件及 A189 state/
receipt 必须位于 worker 可读的受保护持久卷。CRD 保存编排状态和摘要，不保存大文件或
凭据，也不应安装在被该 operation 运维的 KubeBrain 数据面中。

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
