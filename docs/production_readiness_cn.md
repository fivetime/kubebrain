# KubeBrain + TiKV 产品生产就绪验证清单

这份清单只关注 KubeBrain 产品本身能否稳定、有效地替代 etcd：etcd v3 API 兼容性、Kubernetes/k3s apiserver 存储路径、TiKV 后端一致性、watch/lease/compact 行为、故障恢复和备份恢复。节点规格、反亲和、NetworkPolicy、镜像策略、资源限额等属于集群运维决策，不作为这里的产品就绪判断。

## 验证边界

- 本地 kind、standalone kube-apiserver、临时 in-cluster kube-apiserver 只能证明一部分 API 行为。
- 判断能否替代 etcd，最终必须把真实 Kubernetes/k3s apiserver 的 `--etcd-servers` 指向 KubeBrain，在真实 TiKV/PD 后端上跑对象生命周期、list/watch、lease、compact、apiserver 重启和 KubeBrain leader 切换验证。
- `deploy/dev` 只用于本地验证；`deploy/production` 是经过结构化测试的高可用基线，但
  平台仍必须注入生产镜像、StorageClass、跨可用区调度、网络策略、证书和监控栈，不能
  不经环境适配直接发布。

生产平台在创建 TiDBCluster 或 KubeBrain StatefulSet 前，必须先安装共享的非抢占优先级：

```shell
kubectl apply -f deploy/production/dbaas-priority-class.yaml
```

`kubebrain-dbaas-critical` 的值为 1000000，低于 Kubernetes 保留的 system-critical classes，
`globalDefault=false` 且 `preemptionPolicy=Never`。KubeBrain、PD、TiKV 都显式引用它，以改善
节点压力下的保留顺序和待调度排序，但不会抢占其他租户 Pod。删除或改名该 PriorityClass 前
必须先迁移所有引用；缺失时新 Pod 会保持 Pending。优先级不能替代足够的节点容量、资源
requests/limits、跨区放置、PDB 和故障演练。

核心数据面网络隔离基线位于 `deploy/production/dbaas-network-policy.yaml`。平台必须先创建
`kubebrain-system`、`tidb-cluster` namespace，并保留 production 清单中的 instance/dedicated
标签，再应用策略，最后启动 KubeBrain 与 TidbCluster，避免工作负载先以全通网络运行。默认
Operator namespace 是 `tidb-admin`；安装位置不同时必须在策略中替换。获准访问 client 3379
的 namespace 必须显式标记：

```shell
kubectl label namespace <client-namespace> dbaas.kubebrain.io/client-access=true
kubectl label namespace <monitoring-namespace> dbaas.kubebrain.io/monitoring-access=true
kubectl apply -f deploy/production/dbaas-network-policy.yaml
```

策略只选择当前实例的 KubeBrain、PD、TiKV Pod：同 namespace peer 流量、kube-dns TCP/UDP
53、KubeBrain 到 PD 2379/TiKV 20160、Operator 到 PD API 2379/TiKV status 20180，以及显式
标记的 client/monitoring 入口被放行，其余 ingress/egress 默认拒绝。实例化时若修改 namespace、
instance 名或 Operator 标签，必须同步修改 selector，不能临时增加无 selector 的全放行规则。

API server dry-run 只能证明 schema，不能证明 CNI 执行。生产发布必须确认目标 CNI 支持
NetworkPolicy，并从允许的 client namespace 验证 3379 和业务读写、从 monitoring namespace
验证 3378/2379/20180、验证 DNS 与三副本 leader/raft 正常；同时从未标记 namespace 对这些
端口执行带超时的拒绝探针。任一允许路径失败或拒绝路径成功都必须阻止发布。

策略安装且三类探针 namespace 已准备后，运行可执行门禁；`PROBE_IMAGE` 必须是包含
`bash`、`timeout`、`sleep` 的不可变 digest（生产 KubeBrain 镜像满足该契约），`PROBE_ID`
必须对本次发布唯一：

```shell
KUBE_CONTEXT=production \
PROBE_ID=release-20260721 \
PROBE_IMAGE=registry.example/kubebrain@sha256:<64-hex-digest> \
CLIENT_NAMESPACE=apiserver-client \
MONITORING_NAMESPACE=monitoring \
DENIED_NAMESPACE=network-policy-negative-probe \
  hack/production/validate-network-policy.sh
```

脚本不修改 namespace 标签；它创建三个临时非重启 Pod，验证 client 3379、monitoring
3378/2379/20180、KubeBrain 到 PD 2379/TiKV 20160 均可达，再验证未标记 namespace 到
KubeBrain/PD/TiKV 均不可达。任何失败都返回非零，trap 清理所有已创建 Pod；已有同名 Pod
会被拒绝而非复用。该门禁只证明 TCP/CNI enforcement，仍须随后运行 endpoint health 和
实际 Put/Get/Delete，不能把端口连通替代 etcd 语义验证。

`--advertise-host` 是副本间选主/转发身份，不能同时充当 clientv3 Sync/AutoSync 的公开
地址。生产必须单独设置 `--advertise-client-urls`：仓库基线使用集群内 client Service；
向集群外提供 DBaaS endpoint 时，平台必须替换为所有目标客户端可解析、可路由的公共
`http(s)` URL。TLS URL 的主机名必须存在于服务端证书 SAN，并与客户端验证名称一致；
MemberList 会把该列表交给 clientv3 替换原 endpoint 集合，错误的内部 DNS 会使已成功
bootstrap 的客户端在下一次 AutoSync 后整体断连。实例发布门禁必须从目标客户端所在的
网络域运行；它会逐一探测逗号分隔的 advertised URL，不能从仅能访问 bootstrap endpoint
但无法解析最终地址的控制面网络执行后仍宣称发布成功。

每个实例必须使用全局唯一且创建后不可变的 `--keyspace`。production 基线中的
`kubebrain-system` 仅是清单默认值；DBaaS 控制面实例化清单时必须替换为稳定实例 ID，并
在发布、扩缩和升级门禁中逐字校验。复用或变更 keyspace 会让实例读到其他租户数据，或让
原数据看似消失，因此不能依赖默认空 keyspace，也不能把 namespace 名直接当作跨集群唯一值。

## 租户逻辑配额

标准 dedicated 实例必须显式设置 `--quota-backend-bytes`。production 明文和 TLS 基线均
使用 400 GiB（429496729600 bytes），相对单个 500 GiB TiKV volume 保留容量余量；该值
统计 tenant 存活 key/value 的逻辑字节，不等于 TiKV 的 MVCC、Raft 副本或 compaction 前
物理占用。平台按实例规格调整时，必须保持三个 KubeBrain 副本参数完全一致，并同时调整
容量规划与告警阈值，不能把 `0` 或缺省参数当作有限配额。

从无配额实例启用配额必须分两个发布批次。第一批只升级到支持共享 dirty tracking、sticky
NOSPACE 和有界流式 usage rebuild 的版本，等待全部 KubeBrain 副本及 PD/TiKV 收敛并完成
读写验证；第二批才在所有副本 Pod 模板中增加同一个正整数 quota，等待首次 usage rebuild、
三副本 `quota_backend_bytes`/`quota_logical_usage_bytes`/`quota_nospace` 指标一致，再开放
控制面操作。若当前 usage 已超过新 quota，NOSPACE 是预期的 fail-closed 状态，必须删除
数据或提高 quota 后显式 disarm，不能通过临时移除部分副本的 quota 绕过。

回滚同样不能形成混合配置：先停止会扩大数据量的控制面操作，将整个 StatefulSet 一次性
恢复为上一份 Pod template 并等待滚动收敛。移除 quota 会把 tracking 标为 dirty；后续再次
启用时必须重新流式统计，发布门禁不得复用旧 usage 指标宣告完成。

发布门禁要求 `EXPECTED_QUOTA_BACKEND_BYTES` 为正整数，并核对 StatefulSet Pod template
中恰好一个同值参数；镜像和 revision 收敛不能替代该检查。运行期
`KubeBrainQuotaMetricsInconsistent` 要求 NOSPACE、backend quota 和 logical usage 三类
series 各恰好三份，并比较所有副本的 NOSPACE 与 backend quota 值；缺失、重复或阈值漂移
持续超过一分钟必须告警。

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
健康。等待器会在调用 kubectl 前拒绝非 DNS label 格式的 `NAMESPACE` 与
`TIDB_CLUSTER`，避免错误发布参数进入集群操作阶段。

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

构建上下文必须使用仓库根目录的 `.dockerignore`。`.git`、`.dev`、`bin`、IDE 配置、
本地 output/coverage 和根级临时二进制不得发送给 Docker daemon；这些目录可能包含
数百 MiB 的 kube-apiserver、测试数据、私钥或开发日志，既不参与 production 编译，也
不应影响缓存键。`build/dockerignore_test.go` 固定这些排除项，并同时保护
`build`、`cmd`、`hack`、`pkg`、`go.mod`、`go.sum` 不被根级规则误排。发布构建日志
应记录最终 context 大小；异常增长必须先审计新增本地工件或 COPY 依赖再放行。

仓库内独立 Go module 必须在 `COPY . .` 前单独复制其 `go.mod/go.sum` 并执行
`go mod download`。当前 `hack/backup/objectstore` 与根模块各有独立依赖层；普通源码
变更只应使源码复制和编译层失效，不得重新下载任一模块依赖。对应模块的
`go.mod/go.sum` 变更则必须使该模块下载层失效。`build/dockerignore_test.go` 同时固定
Dockerfile 中根模块下载、objectstore 下载和源码复制的先后顺序；发布构建应检查缓存
日志，避免新增嵌套 module 绕过该边界。

版本、Git SHA 和构建时间的 build-stage `ARG` 必须位于完整源码复制之后、最终编译之前；
runtime-stage 的同名 `ARG` 与 OCI `LABEL` 必须位于 `apk add` 和所有制品复制之后。
只改变发布元数据时，根模块与 objectstore 的依赖下载、`COPY . .` 和 runtime 包安装
必须命中缓存，仅最终编译及元数据层重建。`build/dockerignore_test.go` 固定该顺序；
发布流水线应至少保留一次 metadata-only cache probe，防止每次 commit SHA 变化都重新
下载依赖或安装运行时包。

所有 production Dockerfile `FROM` 必须同时保留可读版本 tag 和不可变
`@sha256:<OCI-index-digest>`，不能只依赖 `golang:...` 或 `alpine:...` 的可变 tag。
`build/dockerignore_test.go` 会逐行拒绝未固定 digest 的 stage。升级 Go 或 Alpine 时，
必须从 registry 重新解析目标 tag 的 OCI index digest，确认发布架构（当前
`linux/amd64`）存在对应子 manifest，再在同一提交中更新 tag、digest 和测试定位字符串；
完整 production build、镜像内 `kube-brain version`、OCI labels、非 root 用户及真实
TiKV/PD smoke 全部通过后才能发布。镜像自身使用 digest 部署不能替代基础镜像固定：
前者保证部署不可变，后者保证同一源码提交可重建到已审计的工具链和根文件系统。

runtime stage 的每个显式 `apk add` 包也必须使用 `name=version-rN` 精确版本；新增包或
升级版本必须同时更新 Dockerfile 和构建门禁，并用固定 Alpine index 实际安装、再从
最终镜像 `apk list --installed` 交叉核验。版本从 v3.23 仓库消失时构建应 fail closed，
不得删除版本约束临时放行。该约束防止顶层工具静默升级，但 Alpine 仓库和传递依赖仍是
外部输入；需要离线或字节级长期重建时，平台还必须使用受控 APK repository snapshot，
或把已验证最终镜像按 digest 复制到受保留策略约束的内部 registry。

镜像内 `kubectl` 是 DBaaS backup、restore、certificate rotation、destroy 和 release
gate 的运行时依赖，版本必须落在受支持 Kubernetes server 的 `±1 minor` skew 窗口内。
当前支持并系统验证 v1.35/v1.36，因此使用官方 v1.36.2 二进制，而不是 Alpine v3.23
提供的 v1.34.2。amd64/arm64 下载必须分别固定官方 SHA-256，使用 HTTPS/TLS 1.2+，
校验成功后才复制进 runtime；不支持的 `TARGETARCH` 必须在源码编译前失败。发布门槛需
从最终非 root 镜像运行 `kubectl version --client -o json`，并用同一二进制连接目标
Kubernetes API。checksum 门禁不等于跨架构运行验证；当前 arm64 仍需独立 CI runner。

`TARGETARCH` 必须同时控制 kubectl 下载和 build stage 的 `GOARCH`；所有 KubeBrain
数据面、备份、计量和 operation 二进制必须设置 `GOOS=linux GOARCH=${TARGETARCH}`，
且 `kube-brain version` 的 `Go OS/Arch` 必须与最终 image platform 一致。发布多架构
manifest 前，应从每个 build stage/final image 提取全部可执行文件并检查 ELF machine，
禁止同一镜像混入不同架构。交叉编译和静态 ELF 检查不能替代运行验证：amd64 与 arm64
都必须在对应原生 runner 上完成容器启动、kubectl/API、TiKV/PD 和 readiness smoke，
再合并 manifest list。

CI 的 Go 版本必须与 Docker build stage 精确一致，当前均为 1.26.5；Dockerfile 同时固定
精确 patch tag 和不可变 digest。CI 使用固定 `govulncheck` 版本扫描根模块与生产对象存储
子模块的可达漏洞，使用固定 `staticcheck` 版本扫描两模块，并使用固定 tag+digest 的
ShellCheck 镜像检查全部 Git 跟踪 shell 脚本的 warning/error；任一命中均阻止发布。CI 构建 TiKV 和
Badger image 时必须显式传入 `TARGETARCH=amd64` 及上述三项 metadata，并回读 OCI
revision、运行用户和 kubectl 版本。release workflow 必须使用 QEMU、Buildx 和
`linux/amd64,linux/arm64`，compiler stage 固定运行在 `${BUILDPLATFORM}` 上执行原生
交叉编译，runtime stage 则保留目标平台，避免在 QEMU 下运行整套 Go compiler 或把宿主
架构 runtime 误装进目标 image。发布必须同时生成 max-mode provenance 与 SBOM，使用
12 位和 40 位 commit SHA tag，并按 build digest 回读 raw OCI index；过滤 attestations
后必须恰好存在 `linux/amd64`、`linux/arm64` 两个 runtime manifest。任一架构构建、
attestation 或 index 检查失败都不得更新 `latest` 作为可消费发布。

GitHub Actions 自身也是发布供应链的一部分。所有 `uses:` 必须固定到 40 位 commit SHA，
不能仅写 `@v4`、`@v6` 或 branch；版本注释只用于 Dependabot/人工升级可读性，不参与
解析。所有 checkout 必须设置 `persist-credentials: false`，避免后续仓库脚本继承
GitHub token。手工 image 发布只接受 40 位完整 commit，checkout 后必须比较实际 HEAD，
并确认该 commit 是 `origin/main` 的 ancestor；未合并 feature branch、缩写 SHA 和可变
branch/tag 都应 fail closed。只有 main push 可自动触发 packages 写入，工作流必须先
验证不可变双架构 digest，再 promotion `latest`。

integration runner 下载的 kind、kubectl 和 Helm 也必须固定版本、仅允许 HTTPS/TLS 1.2+
并在安装前校验官方 SHA-256；禁止直接执行 `main` 分支安装脚本。默认 kind node image
必须同时保留可读 Kubernetes tag 和 OCI index digest。当前固定 kind v0.32.0、
kubectl v1.36.1、Helm v3.18.4 和
`kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5`。
升级时必须从对应上游 release/checksum 和 registry manifest 重新取得值，在独立提交中
更新，并跑真实 integration workflow；本地 YAML/契约测试只能证明声明，不证明托管
runner、GHCR 权限或外部服务可用。

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
EXPECTED_KUBEBRAIN_STATEFULSET_UID=<immutable-kubebrain-statefulset-uid> \
EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID=<immutable-kubebrain-client-service-uid> \
EXPECTED_IMAGE=registry.example/kubebrain@sha256:<digest> \
EXPECTED_KEYSPACE=instance-a \
EXPECTED_PD_ADDRS=kb-pd.kubebrain-storage-a.svc:2379 \
EXPECTED_TIDB_CLUSTER_UID=<immutable-tidbcluster-uid> \
EXPECTED_CLUSTER_ID=<immutable-pd-cluster-id> \
EXPECTED_INITIAL_CLUSTER=kubebrain-0=https://kubebrain-0.kubebrain-peer.kubebrain-system.svc.cluster.local:3380,kubebrain-1=https://kubebrain-1.kubebrain-peer.kubebrain-system.svc.cluster.local:3380,kubebrain-2=https://kubebrain-2.kubebrain-peer.kubebrain-system.svc.cluster.local:3380 \
EXPECTED_QUOTA_BACKEND_BYTES=429496729600 \
EXPECTED_ADVERTISE_CLIENT_URLS=https://instance-a.example:2379 \
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
replicas 和 revision 全部收敛，再校验期望 PD/TiKV 数量；TidbCluster metadata UID 和 status
中的非零 cluster ID 必须分别与实例创建 receipt 中的 immutable
`EXPECTED_TIDB_CLUSTER_UID`/`EXPECTED_CLUSTER_ID` 一致；随后要求 KubeBrain
StatefulSet metadata UID 与 operation receipt 中的 `EXPECTED_KUBEBRAIN_STATEFULSET_UID`
一致，并校验 observed generation、ready/updated replicas、revision、精确 image；再要求精确的
ordinal Pod 集合全部为 Running/Ready、非终止、带有当前 controller revision、唯一的目标镜像，且
controller StatefulSet name/UID 与 receipt 精确一致；随后校验 Pod template 中唯一的
`--keyspace`、`--pd-addrs`、`--initial-cluster`、`--quota-backend-bytes`
和 `--advertise-client-urls` 全部匹配，随后
要求 client Service metadata UID 与 receipt 一致，且其 EndpointSlice 的 ready/serving/non-terminating
Pod targetRef UID 集合与当前 Pod 集合完全相同；随后
读取运行时 MemberList，要求 cluster ID 与同一 immutable storage identity 精确一致、精确成员数、唯一且非零的 member ID/name、
每个成员的 name/peer URL 映射与 immutable initial cluster 完全相同，且每个成员的 client
URL 集合与期望完全相同；最后通过官方
`etcdctl endpoint health` 对 bootstrap `ENDPOINT` 和每个 advertised client URL 分别提交
线性化 proposal。缺少 `EXPECTED_IMAGE`/
`EXPECTED_KUBEBRAIN_STATEFULSET_UID`/`EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID`/`EXPECTED_KEYSPACE`/`EXPECTED_PD_ADDRS`/`EXPECTED_TIDB_CLUSTER_UID`/`EXPECTED_CLUSTER_ID`/`EXPECTED_INITIAL_CLUSTER`/`EXPECTED_QUOTA_BACKEND_BYTES`/`EXPECTED_ADVERTISE_CLIENT_URLS`/`ENDPOINT`、任一状态
缺失、旧 revision、错误拓扑、错误镜像、Pod owner/revision/Ready/终止状态漂移、quota/client URL 缺失/重复/不匹配或 endpoint
不健康、KubeBrain StatefulSet/client Service/TidbCluster UID 或 TidbCluster/MemberList cluster ID 漂移、EndpointSlice Pod 集合漂移、initial cluster 成员/peer URL 为空或重复、MemberList 缺失/重复/不完整/与声明不一致、advertised URL 列表含空成员或任一地址
从门禁网络不可达都会 fail closed。脚本使用可覆盖的 `JQ`（默认 `jq`）结构化解析 JSON，
不得用文本匹配替代成员身份和 URL 集合检查。

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
按严格 JSON 顶层字段集合、类型和值复核原 receipt 后复用。`STATE_DIR` 和 receipt
必须位于持久、受访问控制的操作记录卷，完成后再归档到不可变审计存储。该门禁验证
数据面完成条件，不替控制面实现 Secret 发布超时、阶段回滚或跨实例任务调度。

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
不存在，但名称复用立即失败。该工具在未显式提供 kubeconfig 时优先使用 Pod
ServiceAccount 的 in-cluster 配置，销毁 worker 不依赖 home 目录 kubeconfig。最终还
要求全部固定资源、实例 PVC、PD/TiKV Pod 和
StatefulSet 均为空。`complete` 重复 absence gate 后原子发布
`kubebrain.destroy.receipt.v1`，绑定 instance、operation ID、两个 namespace、
TidbCluster 名、备份 digest/revision 和完成时间；同输入重试会按严格 JSON 顶层字段
集合、类型和值复核原 receipt 后复用。

脚本刻意不删除 namespace、TLS Secret、外部对象存储 artifact、监控规则或控制面账单
记录：namespace 可能共享，而审计/备份数据必须按独立保留策略处理。平台只有在 receipt
归档到不可变审计存储并完成外围资源清单对账后，才能删除专属 namespace 和凭据。
边界清理读取 destroy receipt 时同样要求严格 JSON 顶层字段集合、类型和值匹配，并要求
持久 boundaries state 精确包含 1 个 HEADER 和按 `CREDENTIAL_SECRETS` 顺序排列的全部
SECRET UID 行；未知行、额外列、缺失凭据行或顺序漂移都 fail closed，不能漏删凭据后
发布 cleanup receipt。

## 备份恢复生产边界

当前唯一通过端到端恢复验证的生产备份模式是 `kubebrain.logical.v2`。制品包含固定
snapshot revision、源 prefix、记录数和 SHA-256，先写临时文件并 `fsync` 后原子发布；
lease 元数据记录剩余 TTL；restore 为目标生成新 lease ID，同时保持 key 关联和共享
关系。restore 在写目标前完整校验，并默认拒绝覆盖已有 key。每次发布备份配置前必须通过：

```shell
BACKUP_MODE=logical hack/backup/production-mode-check.sh
```

独立 PD/TiKV 集群若计划建设停机冷物理 full snapshot，必须先运行只读能力与身份预检：

```shell
KUBE_CONTEXT=production \
KUBEBRAIN_NAMESPACE=kubebrain-instance-a \
TIDB_NAMESPACE=kubebrain-storage-a \
TIDB_CLUSTER=kb \
VOLUME_SNAPSHOT_CLASS=retained-csi \
EXPECTED_KUBEBRAIN_STATEFULSET_UID=<uid> \
EXPECTED_TIDB_CLUSTER_UID=<uid> \
EXPECTED_TIKV_CLUSTER_ID=<numeric-cluster-id> \
EXPECTED_PD_PVCS=3 \
EXPECTED_TIKV_PVCS=3 \
ALLOW_COLD_PHYSICAL_SNAPSHOT=true \
  hack/backup/cold-snapshot-preflight.sh > cold-snapshot-inventory.json
```

预检要求集群提供 `snapshot.storage.k8s.io` API、VolumeSnapshotClass driver 非空且
`deletionPolicy=Retain`，并精确锁定 KubeBrain StatefulSet UID、TidbCluster UID、TiKV
cluster ID，以及全部 3+3 Bound PD/TiKV PVC 的 name/UID/PV/storage class/volume mode。
输出 `kubebrain.cold-physical-snapshot-preflight.v2` 还固定原 TidbCluster spec，以及每个 PVC 的
access modes 和 requested storage，作为后续 operation 的不可变恢复蓝图。旧 v1 清单缺少这些
字段，当前执行器明确拒绝，不能补默认值后继续。
缺少 CSI API 的集群必须 fail closed。

具备 CSI snapshot 能力的隔离预生产集群可使用候选执行器消费该不可变清单：

```shell
PREFLIGHT_FILE=cold-snapshot-inventory.json \
OPERATION_ID=instance-a-20260721t120000z \
RECEIPT_FILE=cold-snapshot-receipt.json \
SEMANTIC_WITNESS_FILE=cold-snapshot-witness.jsonl \
EXPECTED_WITNESS_PREFIX=/registry \
KUBE_CONTEXT=preproduction \
  hack/backup/cold-snapshot-execute.sh
```

控制面必须先阻断该实例的新写入，再用当前 `logical-export.sh` 对实例完整 keyspace prefix 生成
`kubebrain.logical.v2` witness；executor 默认要求 witness 至少一个记录、创建不超过 300 秒，并
在任何 mutation 前验证内部 digest、prefix、格式，并以 `REQUIRE_GRANTED_TTL=true` 逐条确认 lease
同时具有合法的 remaining TTL 与 granted TTL。旧 artifact 仍可用于逻辑恢复，但包含 lease 且缺
`granted_ttl` 时会在 pause、缩容或创建 VolumeSnapshot 前失败，不能作为物理 lease identity 证据。
snapshot receipt 将 witness 的 format/prefix/revision/record/lease count、内部 SHA-256 和整个文件
SHA-256 一并绑定。若 witness 后仍有 Put/Delete/Txn，最终恢复的 current-exact 门禁会因 value 或
create/mod/version/lease 元数据漂移而失败，不能生成语义成功 receipt。

执行器重新运行预检并规范化比对清单，在任何变更前拒绝漂移；用 TidbCluster UID 和
resourceVersion 设置 `spec.paused=true`，等待 pause 可见并重新取得 StatefulSet fence，随后
按 KubeBrain、TiKV、PD 顺序将副本缩至零。全部 Pod 退出后才为清单内每个固定 UID PVC 创建
operation-labeled VolumeSnapshot。只有所有 snapshot `readyToUse=true`，且绑定
VolumeSnapshotContent 为 `Retain`、class/UID 引用一致并提供非空 snapshot handle，才按 PD、
TiKV、解除 operator pause、KubeBrain 的顺序恢复。任一错误都由退出 trap 尝试同样的恢复，
失败不发布成功 receipt；Retain 策略下的部分制品必须进入人工审计，不能自动误删。

成功恢复服务并原子 fsync 发布 `kubebrain.cold-physical-snapshot.v2` receipt 后，仍只证明冷
快照集合已生成。尚未从 receipt 在隔离集群恢复全部 PD/TiKV volume、核验 cluster identity、
启动 KubeBrain 并完成 revision/key/lease/watch 验证，因此 `BACKUP_MODE=cold-csi` 仍不受
支持，更不等于日志型 PITR。生产调用还必须由持久 operation worker 独占实例维护窗口，不能
从交互终端并发运行。

v2 receipt 可离线渲染隔离恢复清单：

`kubebrain.cold-physical-snapshot.v2` receipt schema 必须与 snapshot executor 的真实输出
一致：顶层 `created_at` 为 RFC3339 时间，`inventory` 保留完整 preflight inventory
format、KubeBrain/storage identity、VolumeSnapshotClass、recovery blueprint 和 PVC 字段，
`snapshots` 保留 source PVC、snapshot/content UID、handle 和 restore size，`semantic_witness`
包含 records、leases 以及小写 hex SHA-256/file SHA-256。renderer 和后续 restore verify
都使用严格 schema 解码；未知字段、尾随 JSON、非法 `created_at` 或 inventory format
漂移都必须 fail closed，不能为了兼容测试夹具而放宽生产 receipt 契约。snapshot receipt、
restore receipt 和 rendered restore manifest 均是小型 JSON，读取上限为 4 MiB；超限必须
在 schema 解码、manifest 比对或语义验证前 fail closed。逻辑 witness 文件可能按实例
keyspace 放大，不适用该小型 JSON 上限，仍由 logical backup parser 解析，并以流式
SHA-256 校验整文件 digest。

```shell
go run ./hack/backup/cmd/cold-restore-render \
  --receipt cold-snapshot-receipt.json \
  --target-snapshot-class retained-csi \
  --target-storage-class encrypted-csi \
  --output cold-restore-manifest.json \
  --confirm-isolated-target
```

renderer 要求 PVC 与 snapshot handle 完整一一对应、handle 唯一、restore size 不大于请求容量、
PD/TiKV PVC 数与原 spec replicas 一致，并拒绝 blueprint 身份漂移。输出为静态预绑定的 retained
VolumeSnapshotContent、VolumeSnapshot、使用 dataSource 的原名 PVC，以及 `paused=true` 的原
TidbCluster。PD 数据包含原 member identity 和 peer/client URL，因此 namespace、TidbCluster、
StatefulSet/PVC 名不能改；目标必须是与源数据面网络隔离、但使用相同名字的独立 Kubernetes
集群。renderer 不访问目标集群，也不验证 snapshot handle 可导入、PVC 已 Bound、恢复后的
TiKV cluster ID 或 KubeBrain 数据语义；这些仍须由后续 restore executor 和真实 CSI 演练门禁。
renderer 读取 `kubebrain.cold-physical-snapshot.v2` receipt 时必须拒绝未知字段和尾随第二个
JSON 值，不能把恢复证据合同之外的字段静默忽略后继续渲染清单。

隔离目标的候选恢复 executor 必须显式绑定两个 Kubernetes UID，不能隐式使用当前 context：

```shell
KUBE_CONTEXT=isolated-restore \
EXPECTED_TARGET_KUBE_SYSTEM_UID=<target-kube-system-namespace-uid> \
EXPECTED_TARGET_NAMESPACE_UID=<target-tidb-namespace-uid> \
RECEIPT_FILE=cold-snapshot-receipt.json \
RESTORE_MANIFEST=cold-restore-manifest.json \
RESTORE_RECEIPT_FILE=cold-restore-receipt.json \
ALLOW_COLD_PHYSICAL_RESTORE=true \
  hack/backup/cold-restore-execute.sh
```

执行器先用同一 renderer 重新生成并规范化比对 manifest，验证 kube-system/目标 namespace UID、
VolumeSnapshot/TidbCluster API、snapshot class 的 driver+Retain policy、storage class provisioner，
并要求目标 TidbCluster、PD/TiKV StatefulSet、全部 VSC/VS/PVC 名均不存在。任何门禁失败发生在
`kubectl create` 前。创建后依次等待全部 VolumeSnapshot ready、PVC Bound，确认新 TidbCluster
仍为 `paused=true` 后用 UID/resourceVersion Patch 解除 pause，再等待 Ready condition 和 PD/TiKV
rollout。恢复后的 cluster ID 必须精确等于源 receipt；否则 executor 重新 UID-fenced pause，并按
TiKV、PD 顺序用 StatefulSet UID/resourceVersion 条件 Patch 缩至零，保留 retained 资源审计。

成功时原子发布 `kubebrain.cold-physical-restore.v1`，绑定源 receipt SHA-256、目标 cluster/
namespace UID、新 TidbCluster UID/cluster ID、VSC UID/driver/handle 和 PVC UID/PV。该 receipt 只
能在实际 VSC name/driver/handle 与已审核 manifest 完全一致、PVC name/UID/PV/Bound phase 与
manifest 完全一致且 UID/PV/handle 无重复时发布。它只证明存储层恢复与身份一致；必须继续在
隔离目标部署受审 KubeBrain release，并执行固定 revision、key/value、lease TTL/attachment、
watch continuity 和写后读验证，才能将这次恢复记为可用演练。

部署受审 KubeBrain release 并指向恢复后的 PD 后，运行物理语义门禁：

```shell
ENDPOINT=https://restored-kubebrain:2379 \
WITNESS_FILE=cold-snapshot-witness.jsonl \
SNAPSHOT_RECEIPT_FILE=cold-snapshot-receipt.json \
RESTORE_RECEIPT_FILE=cold-restore-receipt.json \
SEMANTIC_RECEIPT_FILE=cold-semantic-receipt.json \
VERIFY_PREFIX=/__kubebrain/cold-restore-verify/instance-a \
ETCDCTL_CACERT=<ca> ETCDCTL_CERT=<client-cert> ETCDCTL_KEY=<client-key> \
  hack/backup/cold-restore-verify.sh
```

门禁先校验 witness→snapshot receipt→restore receipt 的双 SHA-256 链，再分别在 witness revision
与当前 revision 全量分页读取 prefix，要求 key/value/create revision/mod revision/version/lease ID
及记录数完全一致。restore receipt 中的 manifest digest 只接受小写 hex SHA-256，不能用大小写宽松
的等价字符串绕过 schema。每个 lease 必须保留原 ID、granted TTL 和精确 attached key 集合且当前
TTL 为正。最后以 CreatedNotify watch 建立探针，执行附 lease 的 Put、线性读、Delete、两次精确 watch
event 和 Revoke；成功才原子发布 `kubebrain.cold-physical-semantic-verify.v1`。这仍不能替代真实
CSI restore 演练，但它是物理恢复完成门禁，而不是普通 endpoint health 检查。
语义门禁读取 snapshot/restore receipt 链时同样使用严格单 JSON 值解析，拒绝未知字段和
尾随 JSON；restore manifest 则继续以 canonical digest、资源计数和逐资源 identity 绑定证明内容。

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

对象存储工具中由控制面注入的 JSON 字符串数组环境变量必须是单一 JSON array，且每个
元素必须为非空字符串。`RECEIPT_INPUTS_JSON`、`ARTIFACT_FORMATS_JSON` 和
`ALLOWED_FORMATS_JSON` 均拒绝 `null`、非字符串元素和尾随第二个 JSON 值；空
`RECEIPT_INPUTS_JSON=[]` 只用于生成空 inventory manifest 以证明受管 prefix 无期望
version，不能用 `null` 代替。`ALLOWED_FORMATS_JSON` 仍必须由 usage 业务层校验为非空、
排序且唯一的 artifact format allowlist。
`S3_FORCE_PATH_STYLE` 可留空表示 false；非空时必须是合法布尔值，非法值要让对象存储
executor fail closed，不能静默降级为 virtual-host/path-style 的另一种访问形态。
生产 wrapper 捕获本地对象存储/计量 executor 的 stdout/stderr 时必须使用
`processgroup.CombinedOutput`，总输出最多 1 MiB；超限会终止进程组并 fail closed，
只保留截断证据用于错误信息，避免异常子进程通过 `CombinedOutput()` 无界占用内存。

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
objectstore 所有小型 receipt/manifest digest 校验只接受小写 hex SHA-256；backup/deletion
receipt 的 `artifact_sha256` 也必须是真实 64 位 digest，不能只是非空字符串。

本地 receipt 丢失后的跨进程恢复还必须 Head 精确 version，复核 version ID、metadata、
size，并用远端 `LastModified` 固化 `uploaded_at_unix`；不得使用当前重试时间。缺失或
非法远端时间戳时不发布 receipt。发布验证必须换用全新本地 receipt 路径再次上传相同
key，确认 canonical receipt 逐字节一致；这同时保证备份完成 operation 使用稳定的
receipt SHA，后续 exact-version 删除和 inventory manifest 不会因 Pod 重启绑定不同证据。
Put 返回非冲突错误也不能假定未提交或盲目重传：工具用不继承原请求取消信号的独立
30 分钟上限 Head 当前 version；只有 metadata/size 完全一致，且后续精确 version
LastModified、完整 artifact 下载解析和 retention 全部通过，才确认已提交并发布 receipt。
当前对象不存在、冲突或任一证明失败时同时保留原 Put 错误并 fail closed。

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
都必须归档到不可变审计存储。删除 receipt 的 `deleted_at_unix` 固定等于
`retain_until_unix`，表示该 version 的最早合法删除边界，而不是对象存储无法恢复的物理
删除时间；每次重试仍以 exact-version Head NotFound 重新证明当前不存在，实际工作流完成
时间由上层 BackupDeletion operation receipt 记录；已有上层 operation receipt 复用前
必须按严格 JSON 顶层字段集合、类型和值复核。BackupDeletion runner 还会对 source
backup receipt、pre/post inventory receipt 和 deletion receipt 执行同样的 strict schema
与字段绑定校验。这样即使本地删除 receipt 随 Pod
丢失，重建后的 canonical JSON 和 SHA 也保持稳定。bucket 生命周期规则只能作为调度器，
不能替代该完成证据。`DeleteObject` 返回错误也不等于服务端未提交：工具使用不继承原
请求取消信号的独立 5 秒预算 Head 指定 version；只有 NotFound 才确认提交并发布 receipt。
version 仍可读或 Head 本身失败时，同时保留原删除错误和检查错误并 fail closed，不盲目
重发其他 version 的删除。

`kubebrain-logical-object` 读取本地 canonical JSON 证据时也必须有资源边界：backup/
deletion/audit/blob/usage/inventory receipt、inventory manifest 以及 inventory manifest
builder 的 receipt header sniff 都最多接受 1 MiB。超限文件在 JSON 解码、unknown-field/
trailing/canonical 校验或幂等恢复前 fail closed。通用 immutable blob artifact 的业务
payload 上限仍是 16 MiB；它不扩大这些小型 JSON 证据文件的预算。`blob-read` 写本地
output 时，若目标文件已存在，只读取受远端 metadata/digest 保护的期望对象大小加 1 字节；
超限会 fail closed，不为判断幂等而无界读取错误的大文件。

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
选择器和备份 `BACKUP_INSTANCE`。规则同时输出 8 个 `*:sources:count` 和
`kubebrain_dbaas:metering_data_complete`；只有 9 个容器、6 个 PVC 及唯一备份
artifact/timestamp 源全部存在时，完整性才为 1，8 条计量输入才会产出。控制面必须把
完整性不为 1 或计量序列缺失的区间标为不可计费并 fail closed，禁止按零用量结算。
规则还生成小时窗口序列：

- `kubebrain_dbaas:metering_hour_complete` 要求整个 `[1h]` 窗口的分钟完整性最小值为
  1，且至少存在 60 个 evaluation 样本；
- CPU 使用量通过原始 counter 的 `increase[1h]` 输出
  `kubebrain_dbaas:cpu_usage_core_seconds:hour`；
- 网络收发通过原始 counter 的 `increase[1h]` 输出
  `kubebrain_dbaas:network_{receive,transmit}_bytes:hour`；
- 内存、provisioned storage 和 used storage 输出完整窗口的 `hour_avg`。

这些序列不是最终账单。`deploy/production/kubebrain-metering-archive.yaml` 每小时在
UTC 第 17 分钟查询已结束且等待 10 分钟的完整小时槽位；只有小时完整性序列唯一等于
1，且 8 条资源/备份输入及后述 4 条对象请求输入各自唯一、实例标签精确匹配、值为
非负有限数、样本不晚于查询时间并且不超过 5 分钟陈旧时，才生成
`kubebrain.metering-sample.v3`。规则部署后不足完整 60 分钟历史时不得产生首个 v3
artifact。Prometheus HTTP 200 response 必须声明 JSON media type；兼容性扩展字段允许，
但超过 1 MiB、响应体中拼接第二个 JSON 值或 HTML/text 成功页都会被拒绝。
v1/v2 仅用于读取历史对象。artifact ID、对象键和 JSON 字节均由实例与
槽位确定，重试同一槽位不得生成新内容或第二个对象版本。
本地 object usage receipt、sample 和 rollup 都是小型 canonical JSON，读取最多接受
1 MiB；超限会在 schema 解码、canonical 比对、日汇总构造或 Object Lock 归档前
fail closed。sample/rollup 原子写入在比较已存在文件时只读取目标 canonical 长度加
1 字节；Roller 使用 `ReadSnapshot` 已验证的 bytes/digest 与 blob-read receipt 比对，
不再为了二次校验 receipt 而重新无界读取 sample 文件。

归档器通过 `kubebrain-logical-object` 的通用 `ACTION=blob` 路径执行条件上传，要求
bucket 已启用 versioning 和 Object Lock。上传固定使用 SHA-256 checksum、
`If-None-Match: *`、exact version ID 与 COMPLIANCE retain-until；随后重新下载精确
version、逐字节核对 artifact，并读取远端 retention。只有全部验证通过才发布
`kubebrain.object-immutable-blob.receipt.v1`。条件冲突仅在远端元数据、大小、digest、
保留期和内容全部匹配时视为幂等恢复；任何缺测、重复序列、陈旧/未来样本、对象冲突、
无 version ID、保留策略漂移或收据不匹配都会 fail closed。生产模板默认保留 7 年，
使用独立对象存储 Secret、禁止 ServiceAccount token、只读根文件系统和非 root 身份；
多实例部署必须同步替换 `--instance`、对象前缀、recording-rule 标签与选择器，并为每个
实例保留唯一归档职责。Prometheus 跨网络访问时必须配置 HTTPS、CA、server name 和
bearer token 参数，不能沿用模板中的集群内明文地址。Prometheus CA bundle 文件最多
接受 1 MiB，bearer token 文件最多接受 16 KiB；空 token、包含换行的 token 或超限文件
都必须在创建 collector 和发起 HTTP 请求前 fail closed，不能构造异常大的 Authorization
header 或把错误 CA 文件交给 TLS 初始化继续处理。

对象请求费不从 `kubebrain-logical-object` 子进程次数、S3 重试次数或当前 inventory
反推。供应商/网关 exporter 必须按实例持续暴露前一个已完成小时的精确最终值：
`kubebrain_object_store_request_count{dbaas_instance,request_class,window="1h"}`，其中
`request_class` 恰好为 `write`、`list`、`read`、`delete`；并同时暴露
`kubebrain_object_store_request_period_end_seconds{dbaas_instance,window="1h"}`。
每个实例/类别和 period-end 必须各只有一条序列。分类必须由供应商账单 API 操作表
明确映射，并计入供应商实际收费的重试、复制和生命周期请求；不同供应商不能未经审批
复用分类。exporter 输出必须是非负整数且不大于 IEEE-754 可精确表达上限 `2^53`，
period-end 必须是对应 UTC 小时边界。

recording rules 对五个源分别计数，缺失时用显式零值保持告警可见；只有整个小时至少
60 次 evaluation 都恰好存在五个唯一源时，
`kubebrain_dbaas:object_request_hour_complete` 才为 1。小时请求规则只转发 exporter
提供的 finalized gauge，不对 counter 做 `increase` 或舍入。
`kubebrain.metering-sample.v3` 在 v2 八项指标后追加四类请求数，并内嵌
`object_request_period_end_unix`；归档器要求该值精确等于 artifact `slot_end`。
缺类、重复、非整数、超出 `2^53`、陈旧、未来或错窗都会 fail closed。v1/v2 sample
继续严格可读，已有不可变对象不重写。
本地 metering sample 和 rollup 都是小型 canonical JSON，读取最多接受 1 MiB；超限会在
schema 解码、canonical 比对、日汇总构造或 Object Lock executor 启动前 fail closed。
sample/rollup 原子写入在比较已存在文件时只读取目标 canonical 长度加 1 字节；Roller
使用 `ReadSampleStatus` 已验证的 bytes/digest 与 blob-read receipt 比对，不再为了二次
校验 receipt 而重新无界读取 sample 文件。

`deploy/production/kubebrain-metering-rollup.yaml` 每日 UTC 00:47 处理前一完整 UTC 日，
且只从不可变小时对象读取，不重新查询 Prometheus。每个对象键由实例和 slot 确定；
`ACTION=blob-read` 先枚举 exact key，要求恰好一个 version 且无 delete marker，再按
version ID 核对 format allowlist、artifact ID、instance、store、大小、SHA-256 metadata、
Object Lock mode 和 retain-until，下载后重新计算字节 digest。24 个 canonical sample
必须按小时连续覆盖完整日期、实例一致、指标顺序固定且每个源保留期达到自身
`slot_end + retention_duration`；Roller 会把该下限传给本地 read receipt parser 二次
校验。任一小时缺失、重复、损坏或保留不足时不得生成日汇总。

`kubebrain.metering-rollup.v2` 内嵌全部 24 个源的 artifact format、key、version ID、
digest、大小和 retain-until。v2 CPU core-seconds 与网络 bytes 已是小时 counter
increase，直接跨槽求和；内存和两项存储的 hour_avg 乘以 3600 后求和，输出
byte-seconds；备份 artifact 大小和 age 仅输出 min/max/last 观测值，不计入对象存储
费用。升级日允许显式 allowlist 中的 `metering-sample.v1` 与 v2 混合：v1 CPU/网络
仍按旧的小时末 rate 保持法乘 3600，v2 按 counter increase 直接累加，每个 source 的
format 固定其公式。未知格式、format 与下载内容不一致或重复 allowlist 都会拒绝。
旧 v1 artifact 保持不可变，不原地重解释或改写。日汇总经同一 immutable blob 路径
归档，保留截止点取最早源样本的保留截止点，避免汇总仍在而引用源已过期。

只有 24 个小时源全部为 sample v3 时才生成 `kubebrain.metering-rollup.v3`，并在原六项
资源 quantity 后按固定顺序追加 write/list/read/delete 四类整数请求总数。任何 v3
rollup source 不是 sample v3、请求量为小数或累计超过 `2^53` 都会在独立读取时拒绝。
v2/v3 混合升级日仍生成 v2 rollup，不静默生成不完整请求 quantity，因此不能使用 v3
价格目录结算；应等待首个完整 v3 UTC 日，缺失日进入人工不可计费/adjustment 流程。

默认任务只处理上一 UTC 日。CronJob 长时间停机后必须逐日使用
`--period-end-unix=<UTC 日界 Unix 秒>` 回补；显式 period end 必须按 24 小时对齐且
不晚于 finalization delay 后的最近完整日，未来或未完成周期会 fail closed。正常与
回补任务共享 deterministic artifact ID/object key，重复执行只能复用同一个 exact
version。小时归档凭据只需写 `metering-samples`；日汇总凭据应限制为读取该前缀并写
`metering-rollups`，两个 CronJob 使用不同 Secret。

不可变小时采样和跨日积分仍不构成最终账单。Prometheus `increase` 会按 scrape
边界执行 counter reset 处理和窗口外推，价格版本必须明确采用该测量语义。下面的
不可变价格目录和 charge 关闭资源 quantity 的版本化定价；对象存储成本、adjustment
和最终账单审计仍未完成。采样缺口必须保持不可计费状态，禁止按零用量补算。

价格目录使用 `kubebrain.metering-price-catalog.v1`。目录必须包含稳定 version、
三位大写 currency、覆盖完整计量周期的 `[effective_start_unix,effective_end_unix)`、
固定 `measurement_policy="kubebrain.metering-rollup.v2"`，以及与 rollup 六项 quantity
同顺序的 rate。`unit_price` 是每个 quantity unit 的货币单位十进制字符串，最多 18 位
小数，禁止指数、负值、前导零和无意义尾零；目录不包含备份观测项或对象存储价格。
canonical JSON 必须先经过财务/产品审批，再执行专用 publisher：

```shell
jq -c . approved-price-catalog.pretty.json > approved-price-catalog.json

S3_ENDPOINT=https://s3.example.internal \
AWS_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=... \
AWS_SECRET_ACCESS_KEY=... \
S3_FORCE_PATH_STYLE=false \
  kubebrain-metering-price-publish \
    --input=approved-price-catalog.json \
    --price-scope=global \
    --object-store-id=primary-metering-account \
    --bucket=kubebrain-metering \
    --price-prefix=metering-prices \
    --retention-mode=COMPLIANCE \
    --retention-duration=61320h
```

publisher 在任何 S3 请求前执行 strict/canonical schema 校验，并把已校验目录重写到临时
canonical frozen copy；Object Lock executor 只上传该 frozen copy。对象键固定为
`metering-prices/<scope>/<version>.json`，artifact ID 固定为 version，retain-until
固定为 `effective_end + retention_duration`。同 version 内容、有效期、价格或保留期
不同都会与既有对象冲突，禁止覆盖。只有 publisher receipt 已独立核验后，才把
`deploy/production/kubebrain-metering-charge.yaml` 的
`replace-with-approved-version` 替换为该 version；占位值不得直接上线。
approved price catalog 是小型 canonical JSON，本地读取最多接受 1 MiB；超限会在 schema
解码、canonical 比对和 Object Lock executor 启动前 fail closed。所有 billing canonical
artifact 原子写入在比较已存在文件时也只读取目标 canonical 长度加 1 字节，避免为了判断
幂等而无界读取错误的大文件。

charge CronJob 每日 UTC 01:17 读取前一日 exact rollup 和 ConfigMap 固定的 exact price
version。两个对象都必须单 version、无 delete marker、format/ID/store/digest/retention
匹配，且目录有效期覆盖完整 rollup；任一条件失败都不得生成 charge。
Biller 对 blob-read 下载到本地的 rollup、catalog 和 object storage rollup 做二次 digest
校验时，只按 read receipt 的 `object_bytes + 1` 有界读取；超限会在 charge 构造前 fail
closed，不能为了校验 receipt 而无界读取异常大的本地文件。
同一 source helper 也用于 invoice finalization、invoice number assignment、provider
reconciliation、payment ledger 和 general ledger export 的 exact-read 输入校验；这些
路径按各自 read receipt 的大小上限复算 digest 后才构造下游 canonical artifact。
charge canonical JSON 本地读取最多接受 1 MiB；超限会在 invoice plan/final invoice 的
schema 解码、canonical 比对和 source 绑定校验前 fail closed。
`kubebrain.metering-charge.v1` 内嵌 rollup 和 catalog 的 key、version ID、digest、
bytes、retain-until，逐行记录 quantity 的无指数十进制表示、unit price 和
`amount_micros`。计算先把 quantity 与 unit price 转为任意精度有理数，再逐行执行
`half_even_to_currency_micro.v1`，总额只对已舍入行求和，所有金额使用非负 int64 微
货币单位。二进制浮点乘法不得决定金额。

charge 对象键只由 instance 和 period 构成，不含 price version。同一周期更换价格版本
会得到不同内容并被 immutable key 冲突拒绝，禁止生成两份并行 charge 或静默重定价；
后续更正必须使用尚未实现的 adjustment/credit artifact。正常任务和
`--period-end-unix` 回补共用同一键。charge Secret 应只允许读取 rollup/price prefix
并写 charge prefix；价格 publisher 使用独立审批身份。
adjustment、invoice plan 和 finalized invoice 是月度/周期级控制证据，本地读取最多接受
4 MiB；超限会在 settlement kind sniff、strict schema 解码、canonical 比对或 Object Lock
发布前 fail closed。provider statement、payment ledger 和 general ledger export 可能随
交易行数放大，不套用该 4 MiB 控制证据预算，仍由各自 CSV/生成路径和 exact-read digest
门禁约束。

对象存储保留量不能由 `logical_backup_artifact_bytes:last` 推导；该指标只有最近一次
备份大小，既不包含仍被保留的旧 version，也不能发现 delete marker 或控制面漏报对象。
`deploy/production/kubebrain-metering-storage.yaml` 因此每小时直接扫描实例专属 source
prefix。`ACTION=usage` 使用 `ListObjectVersions` 的 key/version 双 marker 完整分页，
拒绝任意 delete marker、重复/无 ID version 和未前进分页；随后对每个 exact version
执行 Head 和 GetObjectRetention，核对 allowlist format、account ID、bytes、artifact
digest metadata、COMPLIANCE/GOVERNANCE mode 与 retain-until。总字节使用 int64
溢出保护，排序后的完整 version identity/evidence 计算 `versions_sha256`。prefix
必须只属于一个 DBaaS 实例；共享 prefix 会把其他实例费用计入本实例，不能上线。

小时 CronJob 在整点后第 27 分钟运行，10 分钟 finalization delay 只决定选择最近已经
结束的小时；实际 checked-at 允许位于 slot end 后 45 分钟内，以覆盖调度和重试。
`kubebrain-metering-storage-archive --allowed-formats-json` 必须是单一 JSON string array，
拒绝 `null` 和尾随第二个 JSON 值；空数组、重复、未排序或空 format 会在 archiver 配置
校验中 fail closed。
`SOURCE_S3_FORCE_PATH_STYLE` 与 `METERING_S3_FORCE_PATH_STYLE` 可留空表示 false；非空时
必须是合法布尔值，非法值要让 archive CLI fail closed，不能静默降级为 false 后访问错误的
对象存储路径风格。
`kubebrain.object-storage-sample.v1` 固定 source store/bucket/prefix、allowlist、version
数量、总字节、versions digest 和 checked-at，再写入独立计量 Object Lock bucket。
usage executor 的 stdout 必须与文件 receipt 使用同一 strict JSON schema，不允许未知字段或
trailing JSON；两份内容逐字段相等后才会进入 sample 构建。
source 凭据只能 List/Head/GetRetention 实例 prefix，evidence 凭据只能写/读
`metering-storage-samples` 与 `metering-storage-rollups`；二进制通过两套显式前缀环境
变量启动子执行器，禁止让 source 写权限或 evidence 凭据访问备份内容。
sample 与 rollup 归档 receipt 必须精确回显本次请求的 retention mode、绝对
retain-until、version ID、digest 和 bytes；receipt parser 使用 unknown/trailing JSON
拒绝，避免把合法但错误策略的 Object Lock 写入误当作计量证据。

每日 UTC 00:57 的 Roller 只读取前一 UTC 日 24 个 exact snapshot；任一缺槽、时间窗
超限、scope/allowlist 变化、digest/bytes/retention 不匹配都会 fail closed。
sample read receipt 的 retain-until 必须不早于该小时 slot end 加配置保留期，不能只
覆盖当天 rollup period。这样低保留窗口的 sample 不会进入日汇总 source evidence。
`kubebrain.object-storage-rollup.v1` 采用明确的离散计费策略：
`object_storage_byte_seconds = sum(hour_end_total_object_bytes * 3600)`。它不是对象创建/
删除事件的连续时间积分；产品价格必须按该小时末持有量语义审批。历史时点无法从当前
bucket 状态可靠重建，所以小时 snapshot 不支持伪造回填；CronJob 中断造成的缺槽必须
进入人工不可计费/调整流程，不能拿当前 inventory 补写旧小时。

`kubebrain.metering-price-catalog.v2` 固定 measurement policy
`kubebrain.metering-rollup.v2+object-storage-rollup.v1`，在 v1 六项 rate 后追加
`object_storage_byte_seconds/byte_seconds`。Biller 只有在 ConfigMap 显式固定 v2
format 时才读取第二份 rollup，并生成 `kubebrain.metering-charge.v2`；charge 内嵌
resource rollup、storage rollup 和 catalog 三份 exact-version source。v1 目录/charge
仍可读取但不能携带 storage source。生产模板默认 v2，启用前必须先连续获得完整 24
小时 storage snapshot 并发布已审批 v2 catalog。v2 仍可用于历史账期，不能把它宣称为
包含请求成本的最终发票。

`kubebrain.metering-price-catalog.v3` 固定 measurement policy
`kubebrain.metering-rollup.v3+object-storage-rollup.v1`，rate 顺序是六项资源、write/
list/read/delete 四类 `requests`，最后是 `object_storage_byte_seconds`。Biller 只有在
ConfigMap 固定 v3 format 时才读取 exact v3 resource rollup、storage rollup 和 v3
catalog，并生成 11 行 `kubebrain.metering-charge.v3`。charge 自身再次要求四项请求
quantity 为 `0..2^53` 的十进制整数，金额仍采用逐行精确有理数乘法和 half-even 到
微货币。resource rollup、storage rollup 和 catalog source 的 SHA-256/bytes 必须分别匹配本地
canonical artifact status。生产模板默认 v3；启用前必须部署经财务批准的供应商分类
exporter、积累完整 24 小时 v3 sample，并发布覆盖该账期的 approved v3 catalog。invoice
plan/finalizer 接受 v1/v2/v3 charge exact version，历史 invoice 不重写。

账单纠错不得覆盖既有 charge。`kubebrain.metering-adjustment.v1` 使用非零 signed
`amount_micros`：正数是补收，负数是 credit；reason 只允许 `billing_error`、
`service_credit`、`sla_credit` 或 `tax_correction`。每个 adjustment 固定唯一 ID、
实例、单日 charge 周期/币种、charge exact-version source，以及
`kubebrain-billing-approver` 身份、外部 approval ID 和不得早于 charge 周期结束且不得
晚于发布时刻的批准时间。adjustment 还必须在批准时绑定唯一 `invoice_id`，防止同一
credit 被两个 invoice plan 重复应用。任何自由文本客户信息都不应写入该长期保留对象。

最终结算使用 `kubebrain.metering-invoice-plan.v1`，而不是列举 prefix 自动吸收对象。
plan 固定 invoice ID、实例、UTC 账期、币种、按日连续且有序的 charge format 列表，
以及严格排序且无重复的 adjustment ID 列表；同样携带不可变审批证据。由此迟到的
charge/adjustment、同 ID 不同内容或第二份 plan 都不能改变已批准账期。先用
`kubebrain-metering-settlement-publish --kind=adjustment` 发布所有 approved
adjustment，再用 `--kind=plan` 发布 plan；publisher 在任何 S3 请求前执行 strict
schema/canonical、审批时间和保留期校验，并把已校验对象重写到临时 canonical frozen copy；
Object Lock executor 只上传该 frozen copy，避免校验后原始输入路径被替换。
所有 billing 写入型 Object Lock receipt 都必须精确回显本次请求的 retention mode、
retain-until、digest 和 bytes；合法但非请求模式的 COMPLIANCE/GOVERNANCE receipt 会被拒绝。

`kubebrain-metering-invoice-finalize` 先读 exact plan，再按 plan 顺序读取每个日
charge 和 adjustment。它独立复核 receipt 与下载字节 digest、format/ID/instance/
period/currency/retention，要求 adjustment 的 charge source 与该日实际 charge
receipt 完全相同，并要求 adjustment 的 invoice ID 等于 plan ID。plan、charge、adjustment
source 的 SHA-256/bytes 必须分别匹配本地 canonical artifact status；subtotal 和 signed
adjustment total 都使用 int64 溢出保护；最终 total 不允许为负，负余额必须进入下一账期
credit 流程。`kubebrain.metering-invoice.v1` 固化 plan、全部 charge 和 adjustment exact-version
source、分项金额与三个合计。finalized-at 固定为 plan 批准时间，使崩溃重试逐字节确定；
invoice key 只由实例和 plan ID 构成，禁止覆盖。

所有 settlement 对象的 retain-until 取账期首日第一个小时 source 的
`period_start + 1h + retention_duration`，invoice 不得比其最早 charge source 活得
更久；finalizer 要求每个输入至少保留至该边界。`deploy/production/
kubebrain-metering-invoice.yaml` 默认每月 2 日 UTC 02:17 运行，ConfigMap 的
`replace-with-approved-invoice-plan` 只能在 adjustment、plan publisher receipt
独立核验后替换。CronJob 使用只读根文件系统、非 root、无 ServiceAccount token 和
只允许读取 charge/adjustment/plan、写 invoice prefix 的独立 Secret。

供应商账单对账使用 `kubebrain.metering-provider-reconciliation.v1` 固化周期级成本证据。
provider statement 行必须按 provider、account、外部 invoice/line、category、object store
和 bucket 排序且逐行合计；reconciliation 从 Object Lock exact-read provider statement 与
finalized invoice，聚合跨 account/bucket 成本分类，并计算 customer invoice total、provider
statement total 和 gross margin。statement source 与 invoice source 的 SHA-256/bytes 必须分别
匹配本地 canonical artifact；source receipt 与下载字节不一致、retention 不足、账期/币种/
实例不一致或 source 漂移都会 fail closed。provider statement publisher 发布前也会把已校验
statement 重写到临时 canonical frozen copy，Object Lock executor 不读取可变原始输入路径。
normalized provider statement CSV 导入最多接受 16 MiB 输入和 100,000 条数据行；超限会在生成
canonical settlement artifact 前 fail closed。

外部收款系统的结果通过 normalized payment ledger 固化，而不是修改 invoice。
`kubebrain.metering-payment-ledger.v1` 绑定 finalized invoice 的 exact-version source、
invoice total、payment/refund/chargeback 交易、net paid 和 remaining balance。CSV header
固定为 `processor,external_transaction_id,kind,amount_micros,occurred_at_unix`；交易按
processor/外部交易 ID 排序且无重复，金额必须为正，未来交易、退款/chargeback 超过已收款、
net paid 超过 invoice total 或 source 漂移都会 fail closed。生产路径使用
`kubebrain-metering-payment-ledger` 从 Object Lock exact-read finalized invoice，并以 read receipt
生成 invoice source；离线模式才接受本地 invoice/source，且 source SHA-256/bytes 必须匹配本地
canonical invoice。离线 invoice source 是小型 source binding JSON，本地读取最多接受 1 MiB；
超限会在 schema 解码、invoice/source 绑定校验和 ledger 写出前 fail closed。发布前会重新校验
canonical JSON、invoice source retention，把 ledger 重写到
临时 canonical publish input；publisher 还会再生成自己的 frozen copy 并校验上传 receipt，避免
用户 output path 被替换后污染 Object Lock。该 ledger 是应收账款状态证据，不直接发起收款、
退款或催收。normalized payment ledger CSV 导入同样最多接受 16 MiB 输入和 100,000 条交易；
超限不会写出 ledger 或启动 Object Lock 发布。

法规发票编号由 `kubebrain.metering-invoice-number-assignment.v1` 固化为本地证据。
`kubebrain-metering-invoice-number` 从 Object Lock exact-read finalized invoice，绑定 read
receipt、invoice total、jurisdiction、approved series、positive sequence 和固定
`series-sequence-12digit.v1` display number。source receipt 与下载字节不一致、assignment
时间早于 invoice finalized、未来 assignment、retention 不足、source SHA-256/bytes 与本地
canonical invoice 不一致或 display number 被篡改都会 fail closed。发布时会把已生成的
assignment 重写到临时 canonical frozen copy，避免用户 output path 被替换后污染 Object Lock。
该 assignment 为外部税务开票系统 ingest 输入，不替代当地法规校验、发票号段审批、作废/
红冲流程或开票平台回执。

外部总账系统的 ingest 输入使用 `kubebrain.metering-general-ledger-export.v1`。exporter 从
Object Lock exact-read finalized invoice、可选 provider reconciliation 和可选 payment ledger，
生成 canonical、借贷平衡的 journal：invoice 生成 accounts receivable/revenue，provider
allocation 生成 provider cost/accounts payable，payment/refund/chargeback 生成 cash/accounts
receivable。每条 journal line 绑定来源 artifact format/ID/source line，debit 与 credit 总额必须
完全相等，line 数组必须按 line ID 递增排序，且 line source 必须命中 export 顶层绑定的
invoice/provider reconciliation/payment ledger source allowlist；任一输入 source 与 invoice source
不一致、source SHA-256/bytes 与本地 canonical artifact 不一致、read receipt 与下载字节不一致
或 retention 不足都会 fail closed。发布时会把已生成的 export 重写到临时 canonical frozen
copy，避免用户 output path 被替换后污染 Object Lock。该 export 是外部总账过账输入证据，
不替代 ERP/GL 的实际 posting、period close、账号映射审批或反向回执。

该 invoice 是 KubeBrain 数据面资源结算证据，不是完整税务/收款系统。供应商请求
分类/exporter 与账单的周期性对账、税率计算、折扣规则、真实支付渠道、发票编号法规、
外部总账过账和跨账户财务对账仍须由财务控制面实现；`tax_correction` 只记录已由外部
审批系统算出的微货币调整，不能替代税引擎。

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
`ALLOW_OVERWRITE` 可留空表示 false；非空时必须是明确布尔值（`true/false/1/0/yes/no`），
非法值必须在读取 artifact 和创建 lease 前 fail closed，不能把拼写错误静默解释为非覆盖恢复。

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

三副本 StatefulSet 还必须验证官方 clientv3 自身的 endpoint balancer，而不只验证 Service：

```shell
RUN_INCLUSTER_BALANCER_SMOKE=true hack/dev/verify.sh
```

该 smoke 在集群内先只连接指定 Pod DNS，建立 unary 连接和带 CreatedNotify 的 prefix Watch，
再把 client endpoint 集合扩为三个 Pod。临时 fault sidecar 使用仅允许 get/delete 指定 victim
Pod 的 namespaced Role 删除该副本。探针要求 replacement UID 改变且 Ready、20 个唯一 Watch
事件严格按 revision 连续到达，并在切换后完成 Put、线性/serializable Get、Txn 和 Delete。
若删除的是当前 KubeBrain leader，首个使用唯一 key 的条件写允许在有界窗口内观察
`Unavailable` 并按读取结果消解不确定性；后续操作必须恢复。不能把“零瞬态错误”作为
leader election 的虚假承诺，也不能把无界重试当成恢复成功。

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

换主或重启时 `ReloadLeases` 必须从持久 lease meta 和 per-key attachment 重新构建内存索引；
lease meta 的 legacy user-MVCC 与 internal record 都必须是严格的单一 JSON，未知字段或尾随
拼接 JSON 应让 reload fail closed。legacy user-MVCC attachment 与 internal attachment 的
value 都必须是合法 lease ID；任何非法 attachment metadata 都不能被静默跳过，否则该 key 会在
新 leader 上丢失 lease 绑定并绕过后续过期/Revoke 删除。

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

`--skip-key-prefix` 是共享 TiKV keyspace 中用户 key 的物理 GC carve-out，与 KubeBrain
内部协调 `--prefix` 及 `--keyspace` 相互独立。每个值必须非空、不能以 `/` 结尾，多个
值不得相同、嵌套或重叠；配置错误必须在连接存储前 fail closed。后端还会对直接传入的
配置排序、去重并折叠嵌套前缀，避免重叠扫描边界重新进入排除区。

该参数不改变 etcd 逻辑 compact watermark：被排除 key 的旧 revision 仍会按 etcd API
返回 compacted，但 TiKV 中被替代的物理版本不会被回收。它会持续占用存储，只能用于
明确划分所有权、由其他系统负责版本回收的共享 keyspace；普通独占 DBaaS 实例不应设置。
发布前应同时验证 included key 在 physical compact 后只保留当前版本、excluded key 的
所有物理版本仍存在，并确认最新值均可读。

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

follower proxy 成为本地 leader 时必须同时关闭 forwarding client 并清空缓存的
`curLeader`。失去领导权后，即使 successor 恰好等于其成为 leader 前连接的地址，也必须
重新拨号；`curLeader` 相同但 client 为 nil 时不得走 same-leader 快路径。生产故障门禁
应暂停当前 leader 进程略超过 election LeaseDuration、但短于 liveness 重启阈值，确认
另一副本接任后恢复原进程；原 Pod UID 和 restartCount 必须不变，日志必须出现
`stopped leading`、同进程重试以及到 successor 的 `conn to new leader`，所有副本
`/ready` 最终返回 200。
共享 leader-election record 是选主安全状态，读取时必须使用严格 JSON：未知字段或尾随
拼接 JSON 应 fail closed，不能让坏 metadata 退化为看似合法的旧字段记录。

prefix watch catch-up 还修复了一个边界：当 watch cache 中混有其他 prefix 的更高 revision 时，后续 live watch 起点应从最后一个已发送的匹配 prefix 事件之后继续，而不是从全局 newest revision 之后继续，避免跳过本 prefix 事件。

该路径后来又暴露出 follower 通过 HTTP `/status` 向旧 leader 同步 read revision 时会把超时直接返回给 kube-apiserver；当前 revision sync 已改为使用请求 context、对 leader 未选出/连接拒绝/超时/旧 leader 返回非 OK 等 rollout 窗口错误做短周期重试，并在 leader 地址变化后使用新 leader 继续同步。follower 只接受严格的单个 leader status JSON，未知字段、拼接 JSON、零 revision 或超过 4 KiB 的 body 都不能推进本地 read revision，避免代理/LB 异常 body 被静默采纳、拖内存或刷大日志。follower watch proxy 也已改为在 leader 变化或可重试连接错误时内部重连，并从最后已发送 revision 的下一位继续 watch，避免主动把 KubeBrain leader 切换暴露为 watch channel 关闭。

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
- `Maintenance.Status`、`Hash`、`HashKV`、`Alarm`、`Defragment`；其中
  Status/Hash/HashKV 与 etcd 一样是成员本地诊断；Hash/HashKV 在 leader 可达时尽力刷新
  副本 revision cache，但不要求 leader read barrier 成功，可在选主和 leader 故障窗口
  用于 endpoint/hash 排障。Status 的 `Errors` 与 etcd 一样先报告 no-leader，再追加
  当前持久 alarm 的 `AlarmMember.String()`；因此 NOSPACE 生效时，每个 KubeBrain
  副本都会向 `etcdctl endpoint status` 暴露同一 alarm owner，disarm 后同步消失。CORRUPT
  同样以 TiKV internal metadata 持久保存 member 集合；非空时 Put/Delete/写 Txn/Compact/
  LeaseGrant/Revoke 返回标准 DataLoss，Range 仍可用于诊断和恢复。CORRUPT member metadata
  只接受严格单 JSON array；`null`、拼接 JSON、非数组或非严格递增集合会让 alarm 读取失败，
  不能被当作空集合自动清除。
  传统 `/health` 先检查 active NOSPACE/CORRUPT：生效时返回 503 和对应
  `ALARM NOSPACE`/`ALARM CORRUPT` reason；`exclude=NOSPACE` 或 `exclude=CORRUPT` 可显式跳过，
  `serializable=true` 不跳过 alarm。成功与失败响应均使用 upstream 的
  `text/plain; charset=utf-8`；失败响应另带 `X-Content-Type-Options: nosniff`。
  `/ready`/`readyz` 刻意不因 NOSPACE 摘流，以便客户端继续读取和执行恢复操作；CORRUPT
  会使 `/readyz/data_corruption` 失败，紧急诊断时可显式 `exclude=data_corruption`。
  平台健康检查不能只看 gRPC/HTTP Ready，还必须把非空 Status Errors 或失败的传统
  `/health` 视为需处置状态。
- `Cluster.MemberList` 兼容视图
- client HTTP 入口与 etcd 一样默认返回
  `Access-Control-Allow-Origin: *`，并在 OPTIONS 预检时直接返回 200；可通过
  `--cors` 配置精确 Origin allowlist。plaintext HTTP 可通过 `--host-whitelist`
  限制 Host，未知 Host 返回 421，防止 DNS rebinding；TLS 请求不依赖 Host
  allowlist。生产若允许浏览器或不可信网络访问 client 端口，应同时启用 TLS、收紧
  CORS 和 Host，而不是保留兼容默认 `*`。这些控制只包装 client HTTP，不向
  peer 或 info/metrics 端口扩散。
- client HTTP 入口默认启用 etcd v3 JSON gateway，支持 KV、Watch、Lease、Cluster、
  Maintenance、Auth、Lock 和 Election generated routes。请求经本机 gRPC 回环，
  仍受数据面认证、admission、metrics、请求大小和并发/速率限制；TLS-only 与
  client-cert-auth 模式
  会使用服务配置的客户端身份完成内部 mTLS。HTTP token 使用标准
  `Authorization: <token>` 或 `Authorization: Bearer <token>`。不需要 JSON API 的
  实例可显式设置 `--enable-grpc-gateway=false` 缩小 HTTP surface，并在发布门禁确认
  `/v3/*` 返回 404、`/health` 仍可用。启用 client-cert-auth 时，Kubernetes 原生
  HTTPS probe 无法携带客户端证书；生产 Pod 应继续用不暴露数据的 info 端口
  `/ping`/`/ready` 探针，不能把无证书访问 client `/health` 当作进程故障。
  Lock/Election 使用 upstream client/v3 concurrency recipe 及现有 KV/Lease/Watch
  后端；生产必须验证 lease-backed Lock 竞争接棒和 Election campaign/proclaim/resign，
  还必须在 blocked waiter 已写入队列键后取消 HTTP request，确认 context 传播、等待键
  删除且后继不会被幽灵 waiter 抢占，不能只检查路由 200。Watch/Observe generated
  HTTP streaming 已覆盖有限请求体
  CloseSend 后继续推送、逐帧 flush、chunked NDJSON envelope、响应取消和 logical
  Watch 配额释放，并通过 reference etcd 双端差分；仍需预生产长时故障与慢消费者
  soak，不得据此声明长期流稳定性。LeaseKeepAlive HTTP 双向流还覆盖同一有限 body
  内多条 JSON 请求、逐请求响应、未知 lease 的 TTL=0 零值省略以及 body EOF 后正常
  结束；发布门禁应保留多消息形状，不能只验证单次 grant/keepalive。
  专用 Lock/Election convenience API 的不存在 lease、空 unlock key、缺失 leader 和
  无当前 leader 错误在当前 upstream 中均为 gRPC Unknown/HTTP 500；不得擅自按底层
  KV/Lease API “规范化”为 400/404，发布门禁需比较精确 code 和 message。
  Lock/Campaign 的 `lease=0` 不是永久键：upstream 会自动 Grant 独立的默认 60 秒
  session lease 并立即 Orphan keepalive。Unlock/Resign 只删除键、不撤销自动 lease；
  Campaign response 暴露 lease，Lock response 不暴露，后者需从返回 key 的 KV metadata
  反查或等待自然到期。容量与泄漏监控应计入这类短时 orphan lease。
  发布验证还必须让未 Unlock/Resign 的自动 lease 自然过期，确认内部 keepalive 已停止、
  lease TTL 最终为 -1 且键自动删除。到期前最后一秒 TTL=0 在 JSON 中会省略 `TTL`
  字段但仍保留 `grantedTTL=60`，不能将该瞬间误判为 lease 已不存在。
  多副本换主会按 etcd `lessor.Promote` 语义从 granted TTL 一次性恢复未 checkpoint 的
  短 lease，因此换主后 TTL 回到约 60 秒不是 keepalive 泄漏。门禁应记录换主前后的 TTL，
  确认发生这次 promotion，并继续等待它最终到期及删除键；不要用原始创建时间作为到期上限。
  连续换主时每轮都必须先观察 TTL 从上轮恢复值重新下降，再触发下一轮；最后一次换主后
  不再注入故障并等待 TTL=-1。这样可区分合法的逐轮 promotion 与进程残留的持续 keepalive。
- `Auth` 已实现 etcd 兼容的用户、角色、key-range 权限和 token 生命周期，并覆盖
  Watch/Lease 持续鉴权及多副本故障转移；生产启用流程见 Auth 设计与兼容性计划。
  `--auth-token=jwt,...` 的 option 只按第一个 `=` 分隔 key/value，key 文件路径可合法包含
  `=`；缺少 `=`、空 key 或重复 option 仍必须 fail closed。
  HTTP Lock/Election dedicated service 同样必须使用调用者 token 校验其生成队列键所在
  prefix；撤销权限应立即影响已签发 token，改密应使旧 token 失效。simple token 的
  HMAC 只证明 payload 未被外部篡改，验证端仍必须拒绝 claims 中的未知字段和 trailing
  JSON，避免合同外 token 形状在滚动升级或代理路径中被静默接受。其 upstream 错误契约
  特殊：HTTP 500、gRPC code 2(Unknown)，消息仍为 permission denied/invalid auth token。
  Election Observe/底层 Watch 则只在创建时鉴权：follower 转发必须携带 caller token，但
  已建立流不能因后续撤权或改密被追溯关闭；新建的无权限流按 upstream 返回 HTTP 200 空体。
  follower 对 leader 的内部连接就绪检查必须使用 peer `grpc.health.v1.Health/Check` 并要求
  `SERVING`，不得用未认证的 Maintenance RPC，也不得为探测签发或配置内部 root token。
  clientv3 AutoSync 可在 Auth 开启时由普通用户执行，但其结果会完全替换当前 endpoint；所有
  副本必须配置相同、完整的 `--initial-cluster`，且 MemberList ClientURLs 必须从客户端网络
  可解析、可拨号并匹配 TLS 身份，不能把仅 peer 可达的地址暴露为 ClientURL。

## 仍需补齐或确认

- **`Maintenance.Snapshot` 刻意不实现，但物理 PITR 仍是明确缺口。** etcd Snapshot RPC
  输出单成员 bbolt 文件，不能表示独立 PD/TiKV 集群。真实 A143 演练已证明 TiDB BR
  full/PITR 不包含 KubeBrain transactional keys，BR raw 也不能提供跨 CF 一致快照，因此
  不能再把“使用 BR”写成已完成替代方案。当前唯一通过端到端恢复验证的生产模式是
  `kubebrain.logical.v2`；它不保留原 etcd revision/watch 历史。冷 CSI 多 PVC full
  snapshot 仍需完成全停机 executor 和隔离恢复演练，日志型 PITR 继续未完成。
- `hack/backup/logical-export.sh` / `logical-restore.sh` 是当前生产备份与隔离恢复入口；上线
  前必须按本节后文完成 artifact 完整性、Object Lock、恢复 receipt 和持续审计门禁，不能
  只用一次本地导出成功声称具备 DR。
- `MemberAdd`、`MemberRemove`、`MemberUpdate`、`MemberPromote` 不支持，因为 KubeBrain 不是 etcd raft 成员管理模型。
- Auth 可用于数据面用户、角色和 key-range 权限控制；生产仍应叠加 client mTLS、
  网络策略、凭据轮换与运维审计，不把任一单层控制当作完整租户隔离。
- 通用 etcd v3 `Txn` 已覆盖缺失键与范围 phantom guard、嵌套分支、同 revision
  staged commit、写前全分支校验及后端冲突重试；当前差分矩阵无已知原子性差异，仍需
  继续扩大生成式嵌套输入和多点故障 soak。
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
header、record、lease 和 footer 行均使用严格 JSON schema 解码，拒绝未知字段和同一行内
拼接的第二个 JSON 值；扩展逻辑备份格式必须先升级 format/schema，不能把额外字段混入
现有 `kubebrain.logical.v2` 制品。
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
`rollback`。prepare 先按严格 JSON 顶层字段集合、类型和值复核 restore verification
receipt，再要求 Service selector 恰好为
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
file/directory `fsync` 且不覆盖发布；已有 cutover/verify/rollback marker 复用或被后续
阶段消费前必须是绑定目标实例或时间戳的单行封闭格式；已有 cutover receipt 复用前必须按
严格 JSON 顶层字段集合、类型和值复核。

切流完成后使用 `hack/production/audit-restored-instance.sh` 运行持续观察窗口。默认持续
3600 秒、间隔 60 秒且至少 10 个样本；生产控制面应按实例 SLO 调大窗口。脚本先按严格
JSON 顶层字段集合、类型和值核对 A189 state SHA-256 与 cutover receipt，同时要求 source instance 非空且不同于 target、source/target
prefix 非空且不同，并把 receipt 的 source instance 精确绑定到冻结 state；再在每个样本前后检查 Service UID/精确 selector、目标
Pod name/UID/restart/Ready 快照和 EndpointSlice targetRef UID 集。每个样本经公开 endpoint
执行 60 秒 lease grant、`createRevision=0` 条件 Put、线性 Get（核对 value 与 lease）、
value 条件 Delete、删除确认和 lease revoke；探针 key 使用加密随机 nonce，失败时也由
lease 限制残留时间。跨样本 revision 必须单调不降，持续时间使用单调时钟计算。

窗口内任一拓扑或数据检查失败都不发布成功凭据。完整持续时间及最少样本均满足后，才以
0600、file/directory `fsync`、不可覆盖 hard-link 发布
`kubebrain.post-restore-audit.receipt.v1`，记录 cutover operation、artifact/state 身份、
窗口、样本数及首末 revision。已有 receipt 的重试仍会重新执行一次完整拓扑检查与真实
数据探针，并按严格 JSON 顶层字段集合、类型和值复核原 receipt。KubeBrain 网关共享同一
TiKV MVCC 后端，不存在 etcd 各成员独立 backend；
因此不能用成员间 `endpoint hashkv` 代替上述端到端审计。

### DBaaS 持久操作 API

生产管理集群先安装 `deploy/production/kubebrain-operation-crd.yaml`，operation worker
使用 `deploy/production/kubebrain-operation-worker-rbac.yaml`。namespaced
`KubeBrainOperation.dbaas.kubebrain.io/v1alpha1` spec 包含稳定 operation ID、可选
tenant/requestedBy、instance、操作类型、完整参数文件 SHA-256 和 maxAttempts，并由
CEL 保证创建后不可变。当前类型覆盖 Backup、BackupDeletion、RestoreCutover、
PostRestoreAudit、CertificateRotation 和 Destroy。Go 队列层也会在 submit 时拒绝
CRD enum 外的 type，并要求参数 digest 为小写 hex SHA-256；成功 finish 同样要求
receipt digest 为小写 hex SHA-256，不能只依赖 apiserver admission 才发现错误。
所有 shell operation runner 在把参数 JSON 转成 TSV 环境变量前，必须先拒绝空必填字段；
可选 kube context/path 或 metrics output 使用哨兵占位，避免 Bash whitespace IFS 把中间空
字段左移并误绑定后续参数。

`hack/production/cmd/operationctl` 提供 submit、claim、heartbeat、retry、succeed、fail
和 get。claim 按创建时间稳定排序，通过 status resourceVersion CAS 从 Pending 或租约
过期的 Running 中认领；每次认领递增 attempt。owner+attempt 是 fencing token，旧 worker
在接管后不能 heartbeat、retry 或提交终态。heartbeat 延长 lease；retry 清除 owner/
lease 回到 Pending 但保留已消耗 attempt；达到 maxAttempts 的 operation 由下一次扫描
CAS 标记 Failed。Succeeded/Failed 终态由 CRD admission 保证不可变。同 owner/attempt/
receipt 的 finish 可幂等重试，不同结果不能覆盖。

worker RBAC 只允许 get/list/watch operation 及 get/update/patch status，不允许 create、
delete、修改 spec 或读取 Secret；submit 权限只应授予管理面 API 身份。六类 executor
分别使用同名 ServiceAccount，`kubebrain-operation-worker-type` AdmissionPolicy 将 status
更新用户与 `spec.type` 绑定，跨类型 SA 不能 claim、heartbeat 或提交终态。成功状态必须记录不可变操作
receipt 的 SHA-256。审批只能由
`system:serviceaccount:kubebrain-operations:kubebrain-operation-approver` 写入；AdmissionPolicy
和 Go 队列层都拒绝非专用 approver，避免生成不可 claim 的审批证据。
operation audit artifact 与 archive receipt 的 digest 校验同样只接受小写 hex SHA-256，
保持离线审计证据与 CRD schema 一致。
`hack/production/run-post-restore-audit-operation.sh` 已把 A190 接入：
只 claim PostRestoreAudit，核对参数 JSON 摘要，在子审计运行期间续租；heartbeat 失败会
终止本地进程，审计失败 requeue，成功才将 receipt 摘要写入 Succeeded。runner 会先拒绝
空的 state/receipt 路径、Service 身份、target instance、public endpoint、audit prefix
和 receipt output，不能把缺失必填参数传给子审计脚本后再依赖下游失败。

`kubebrain-operation-worker` 把每次 executor 放入独立进程组。Pod SIGTERM、supervisor
context 取消或 heartbeat 触发的脚本退出必须终止 shell 及仍在同组的全部后代，避免备份、
恢复、切流、证书轮换或销毁命令在 worker 已退出后继续产生本地或远端副作用。executor
脚本禁止使用 `setsid` 或自行创建脱离 supervisor 的 session；需要异步工作的外部系统
必须通过 Operation receipt 和 fencing API 显式建模，不能依赖孤儿进程。进程组取消只
封闭本地执行树，远端幂等仍必须依赖稳定 operation ID、owner/attempt fencing 与
exact-version receipt。

外部提交入口使用 `deploy/production/kubebrain-operation-api.yaml`。Deployment 默认
`replicas: 0`，必须先创建 `kubebrain-operation-api-oidc` Secret（`issuer`、`audience`）
和 `kubebrain-operation-api-tls` TLS Secret，再扩容。API 只提供
`POST /v1/operations`、`GET /v1/operations/{name}` 和不含依赖状态的 `/healthz`；
必须通过 HTTPS。OIDC token 必须使用 RS256，包含匹配的 issuer/audience、有效 exp、
sub、DNS label 格式 tenant 及字符串数组 `kubebrain_instances`。tenant 必须与请求一致，
instance 必须在数组中（受信任的控制面 token 可显式使用 `*`）。提交时 sub 固化为
immutable `requestedBy`，tenant 同样进入 spec、worker claim 和终态审计 artifact。
参数 Secret 只能引用受信控制面预置的 `params-<tenant>-*` 对象，且 key 固定为
`parameters.json`；API ServiceAccount 不具备 Secret 读取或写入权限。
JWKS 默认缓存 5 分钟；并发 cache miss 合并为单次刷新，unknown `kid` 或刷新失败后
fail closed 退避 5 秒，避免外部 IdP 故障或伪造 key ID 造成刷新惊群。可用
`--oidc-jwks-cache-ttl` 和 `--oidc-jwks-refresh-backoff` 调整，但退避期间不会继续信任
已经过期的 key。discovery 与 JWKS 的 HTTP 200 响应必须声明 JSON media type；
`application/json` 和 `+json` 类型可用，HTML/text 错误页即使 body 是合法 JSON 也会被拒绝。
完成 Secret 配置后建议扩为 3 副本；清单包含 zone/hostname topology spread，PDB
`maxUnavailable: 1`。
跨租户或未授权实例的 submit/get 统一返回 404，避免实例和 operation 枚举。

API 每 30 秒在线重载 `kubebrain-operation-api-tls`。轮换契约与 parameter broker
相同：只接纳匹配且处于有效期内的完整 key pair，无效更新保留旧证书；readiness
`/readyz` 检查当前证书有效期，liveness `/healthz` 保留诊断。更新同一 CA 签发的叶证书
后必须逐 Pod 验证新 serial、UID 不变和零重启。更换签发 CA 时，外部负载均衡器、调用方
和探针必须先进入旧/新 CA 双信任窗口，再更新 API 叶证书，最后撤旧 CA。

`/readyz` 还会在 `--dependency-request-timeout=5s` 内 GET 一个固定不存在的 Operation，
只接受带精确探测名称的 NotFound；CRD 路由缺失、RBAC、transport、timeout 或 API 过载
均返回 503 并摘流。OIDC/JWKS 和业务 Operation 请求使用相同 deadline：JWKS provider
不可用或刷新失败、Kubernetes Forbidden/Unauthorized/timeout/503/429 返回可重试 503，
真正的无效 token、签名、issuer/audience/claim 或 unknown kid 仍返回 401。`/healthz`
不访问 OIDC 或 Kubernetes，保持 liveness 语义。上线应临时撤销并恢复 API Role 的
Operation `get/create`，验证外部 GET 与 readiness 按 `404/204 -> 503/503 -> 404/204`
变化，并确认所有 Pod UID 不变、零重启。

验证器通过 OIDC discovery 获取 JWKS，未知 kid 会触发刷新；缓存过期且刷新失败时拒绝
token，不继续信任可能已撤下的旧 key。除 loopback 测试外 issuer/JWKS 必须使用 HTTPS。
API ServiceAccount 仅有 namespaced operation `create/get`，没有 list/watch、status、
Secret、Lease、update 或 delete 权限。启用示例：

```shell
kubectl -n kubebrain-operations create secret generic kubebrain-operation-api-oidc \
  --from-literal=issuer=https://idp.example.com \
  --from-literal=audience=kubebrain-operation-api
kubectl -n kubebrain-operations create secret tls kubebrain-operation-api-tls \
  --cert=/path/to/tls.crt --key=/path/to/tls.key
kubectl apply -f deploy/production/kubebrain-operation-api.yaml
kubectl -n kubebrain-operations scale deployment/kubebrain-operation-api --replicas=2
```

`hack/production/run-backup-operation.sh` 接入受保护 Backup。参数文件固定 endpoint、
prefix、operation 专属 artifact/receipt 路径、分页大小、Object Store ID、bucket/object
key、绝对 retain-until、retention mode 与 completion gate。首次执行导出逻辑 v2
artifact；崩溃重试若 artifact 已存在则不覆盖，而是重新校验 exact prefix、最少记录数
与 freshness 后继续。Object Lock upload 会重新下载 exact version 并核对 digest、
revision、records、retention，成功后 operation status 绑定 object receipt SHA-256。
整个导出/上传期间维持 operation heartbeat；失败 requeue，fencing 时终止本地流程。
S3 access key/secret 和 etcd TLS 凭据只通过 worker Secret/env 注入，不进入参数文件或 CR。

定期备份由 `KubeBrainBackupPolicy` 和双副本
`kubebrain-backup-scheduler` 触发。先安装：

```shell
kubectl apply -f deploy/production/kubebrain-operation-crd.yaml
kubectl apply -f deploy/production/kubebrain-operation-managed-namespace-rbac.yaml
kubectl apply -f deploy/production/kubebrain-backup-policy-crd.yaml
kubectl apply -f deploy/production/kubebrain-backup-scheduler.yaml
```

模板 Secret 的 `parameters.json` 使用与 Backup executor 相同的结构，但
`artifact_output`、`receipt_output` 和 `s3_object_key` 必须包含
`{operation_id}`。scheduler 会覆盖 `backup_id`、`retain_until_unix`，并记录
`scheduled_unix`；模板不得包含 S3 或 TLS 凭据。策略示例：

```yaml
apiVersion: dbaas.kubebrain.io/v1alpha1
kind: KubeBrainBackupPolicy
metadata:
  name: instance-a-daily
  namespace: kubebrain-operations
spec:
  tenant: tenant-a
  instance: instance-a
  intervalSeconds: 86400
  retentionSeconds: 2592000
  maxAttempts: 3
  parametersTemplateSecretRef:
    name: instance-a-backup-template
    key: parameters.json
```

时间槽以 Unix epoch 的 UTC 整数倍对齐。策略创建后只触发最新到期槽，不回填创建前
或 scheduler 停机期间的历史槽，避免恢复后形成无界队列。operation 名称为
`backup-<policy>-<slot-unix>`；多个 scheduler 副本竞争时只会创建同一个不可变参数
Secret 和同一个 operation。已有同名资源内容不同会 fail closed。worker 未设置
`PARAMETERS_INPUT` 时通过 `operationctl --action parameters` 读取 operation 绑定的
immutable Secret，并再次校验 SHA-256；手工参数文件模式继续保留。

一个 scheduler Deployment 从 `kubebrain-operations/kubebrain-backup-scheduler-inventory`
ConfigMap 的 `namespaces.json` 读取严格 JSON namespace allowlist，并在每轮 reconcile
重新读取，因此更新无需重启 Deployment。allowlist 最多 256 项，拒绝空数组、空值、重复项、非法 DNS
label、`null`、非字符串数组和尾随 JSON；ConfigMap 缺失、key 缺失或 JSON 非法时整轮 fail closed，不沿用
进程内旧值，也不会自动扫描所有 namespace。backup parameter template 必须是 JSON object，
解析时保留 JSON number 精度并拒绝 trailing JSON；`null` 或拼接模板不会创建 Operation。
scheduler 未显式传入 kubeconfig 时先使用 Pod ServiceAccount 的 in-cluster 配置，显式
kubeconfig 会跳过 in-cluster 探测，避免本地运维身份和生产 Pod 身份混用。
scheduler ServiceAccount 只能 `get` 这个
resourceName，不能 list/watch 或读取其他 ConfigMap。清单中的 ClusterRole 本身不授予
权限，默认 RoleBinding 只绑定 `kubebrain-operations`。每增加一个 namespace，必须先在
该 namespace 创建 RoleBinding：

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: kubebrain-backup-scheduler
  namespace: tenant-a-operations
subjects:
  - kind: ServiceAccount
    name: kubebrain-backup-scheduler
    namespace: kubebrain-operations
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: kubebrain-backup-scheduler-managed-namespace
```

确认 scheduler、worker、approver、archiver 四个 binding 的授权矩阵通过后，再把目标
namespace 加入 inventory，例如：

```shell
kubectl -n kubebrain-operations patch configmap kubebrain-backup-scheduler-inventory \
  --type merge \
  -p '{"data":{"namespaces.json":"[\"kubebrain-operations\",\"tenant-a-operations\"]"}}'
```

目标 namespace 还必须部署 operation worker 及其 namespaced RBAC。策略、模板 Secret、
生成的 immutable Secret 和 Operation 始终留在同一 namespace；一个 namespace 失败只会
产生带 `namespace/policy` 的聚合错误，不阻止其他 namespace 提交。策略 tenant 与
scheduler ServiceAccount 身份会进入 immutable Operation spec 和终态审计。Deployment
包含 zone/hostname topology spread，PDB 最多允许 1 个副本不可用。

scheduler 每轮 reconcile 使用 `--reconcile-timeout=2m`，预算覆盖 inventory、Policy、
模板 Secret、immutable parameters Secret 和 Operation 的全部 Kubernetes API 调用。
API endpoint 黑洞、网络分区或 admission 卡顿必须在 deadline 后记录失败并进入下一轮；
不得因为一个无响应调用永久占住长驻副本。`--once` 模式超时返回非零，供发布门禁直接
判定失败。调整 namespace/policy 规模时可显式增加预算，但禁止设为零或依赖 Pod 重启
终止挂起调用。

scheduler 每轮最多实际尝试 `--max-policies=256` 个 Policy，该上限作用于 inventory
内所有 namespace 的全局候选集合。候选按 `namespace/name` 稳定排序；游标仅在一次
Policy reconcile 实际返回后推进，整轮 context 取消后立即停止，下一轮从最后尝试身份的
后继恢复。游标是单进程吞吐公平状态，Pod 重启后从排序起点重新开始；双副本 correctness
仍由确定性的 slot Operation ID、immutable parameters Secret 内容校验和 Kubernetes
create/AlreadyExists 语义保证。扩大上限前必须确认 2 分钟预算能覆盖对应 API 调用量。

中央 worker、approver 和 archiver 可分别通过以下未绑定 ClusterRole 获得目标
namespace 的最小权限：

- `kubebrain-operation-worker-managed-namespace`
- `kubebrain-operation-parameter-broker-managed-namespace`
- `kubebrain-operation-approver-managed-namespace`
- `kubebrain-operation-archiver-managed-namespace`

每个角色都必须在目标 namespace 建立独立 RoleBinding，subject 分别指向
`kubebrain-operations` 中对应 ServiceAccount。worker RoleBinding 必须同时列出六个
`*-executor` SA，parameter-broker RoleBinding 只列出 broker SA。ClusterRole 清单不会
创建绑定，也不授予 cluster-wide 访问。worker 只读 Operation、更新 status 并管理 Lease；
parameter broker 只 get Operation/Secret；approver
只能读取和更新 Operation 主资源；archiver 只能 get/update Operation 主资源，不能修改
status 或读取 Secret。

类型专属 worker 可设置以下环境变量，让 `kubebrain-operationctl --action claim` 使用与
scheduler 相同的动态 inventory：

```shell
OPERATIONCTL=/usr/local/bin/kubebrain-operationctl
OPERATION_NAMESPACE_INVENTORY_CONFIGMAP=kubebrain-backup-scheduler-inventory
OPERATION_NAMESPACE_INVENTORY_NAMESPACE=kubebrain-operations
OPERATION_NAMESPACE_INVENTORY_KEY=namespaces.json
```

容器内未设置 `KUBECONFIG_PATH` 时，operationctl 使用 Pod ServiceAccount 的 in-cluster
config；显式 kubeconfig 会跳过 in-cluster 探测，本地运维执行才回退 client-go 标准
kubeconfig 搜索。不得把管理员 kubeconfig 挂入 worker Pod。

跨 namespace claim 先比较各队列同类型 Operation 的最近启动时间，优先处理最久未被
服务的 namespace；进入目标 namespace 后继续复用原有实例 Lease、attempt 和
resourceVersion fencing。claim response 固化 `namespace`，六类 executor 脚本随后把
parameters、heartbeat、retry/fail/succeed 全部固定到该 namespace，禁止跨队列续租或提交。
inventory 无效时 claim 在读取任何 Operation 前 fail closed。中央 worker Role 只能
`get` 指定 inventory ConfigMap；各目标 namespace 的 Operation/Secret/Lease 权限仍来自
逐 namespace RoleBinding。

全局公平排序要求 inventory 中每个 namespace 的同类型队列检查全部成功。任一 list
超时、权限拒绝或 API 故障时不得在其余健康队列继续 claim；context 取消后立即停止后续
检查。排序后最久未服务队列的 claim 若返回非 `ErrNoOperation` 错误，也必须原样失败，
不能跳到较新的队列。`operationctl` 退出码 3 只表示所有成功检查的队列确实没有可认领
Operation；上述依赖故障退出 1，供 supervisor 使用 failure backoff 和告警区分空闲与
控制面故障。

实例 claim 是 Lease 与 Operation status 的两阶段提交。Lease create/update 成功后，
status CAS 若冲突、超时或失败，worker 必须用脱离原请求取消信号但最多 5 秒的 cleanup
context，按 holder identity 和 Lease UID precondition 释放刚取得的实例锁。status 主错误
与 cleanup 错误必须同时返回；补偿失败时不能继续扫描其他候选。Requeue/Finish 已成功
提交 Pending/终态 status 后也使用相同独立预算释放 Lease，并把释放失败暴露给调用方。
调用方可用完全相同的 owner、attempt 和结果重试已提交的 Requeue/Finish；Queue 不重复
写 status，只重新执行 5 秒 Lease cleanup。Finish 必须精确匹配终态 phase、receipt 和
message；Requeue 必须精确匹配 Pending phase、已清空 owner、原 attempt 和 message。
任何不一致仍被 fencing 拒绝。重试清理发现 Lease 已不存在或 holder 已被替换时视为
旧 holder 已完成清理，绝不删除替代 holder。
Heartbeat 同样是先续租实例 Lease、再 CAS 更新 Operation status。status 请求失败后必须
在独立 5 秒预算内重读 Operation：若 Running phase、owner、attempt 和目标
`leaseUntilUnix` 精确匹配，则判定写入已提交，按成功返回并保留 Lease；否则按旧
holder 和 Lease UID 清理后 fencing。若重读失败，必须同时返回 status 与重读错误并保留
Lease，不能在提交结果未知时开放同一实例给其他 Operation；Lease TTL 作为最终恢复边界。
实例 Lease 自身的 Create/Update 也可能已提交但响应丢失。非 AlreadyExists 的 Create
错误和任意 Update 错误必须在独立 5 秒预算内 GET 同名 Lease，并精确比较
holderIdentity、leaseDurationSeconds、acquireTime 和 renewTime；四项全部匹配才按写入
成功继续。任一字段漂移仍返回原写错误；GET 失败与写错误聚合返回，不能凭名称或 holder
单独相同推断提交成功。
Requeue/Finish 的 status PUT 失败后也必须在独立 5 秒预算内重读 Operation。Requeue
只有 Pending、空 owner、原 attempt/message、`leaseUntilUnix=0` 全部匹配才确认提交；
Finish 只有目标终态、原 owner/attempt、receipt/message、`leaseUntilUnix=0` 和正数
`completedAtUnix` 全部匹配才确认提交。无论写入已确认还是线性化 GET 证明未提交，退出
worker 都按 holder+UID 清理旧 Lease；GET 失败时保留 Lease 并聚合错误。冲突且未提交
继续返回 fencing，畸形或部分 status 不得进入幂等成功分支。

Operation 管理写也必须处理“服务端已提交、客户端丢失响应”。Submit 的 Create 返回任意
错误后，用独立 5 秒预算 GET 同名对象；只有 immutable spec 完全一致且仍带 audit
finalizer 时才按成功返回。Approve 的 Update 失败后只接受原 UID 且
`approved-by`/`approval-id` 完全一致的对象；同名替换必须报错。UID-precondition Delete
失败后，GET 为 NotFound 或返回不同 UID 表示原目标已消失，可按成功返回且不得删除替代
对象；相同 UID 仍存在时保留原错误。回读失败必须与原写错误聚合。`operationctl --help`
必须列出实际支持的 `approve` action，生产审批 smoke 必须使用专用 approver 身份并通过
ValidatingAdmissionPolicy，不得用管理员身份绕过。

Audit finalizer release 的 Update 也按不确定提交处理。写失败后使用脱离原请求取消信号、
最多 5 秒的 GET：仅当 UID 未变、audit finalizer 已消失且 receipt SHA、artifact SHA、
object version 三项 annotation 与已验证归档证据完全一致时确认成功；对象已 NotFound
表示 finalizer 已释放且删除完成。不同 UID、部分 annotation、finalizer 仍存在或回读失败
均 fail closed，回读错误与原写错误聚合。生产镜像必须包含
`kubebrain-operation-audit`，发布 smoke 需以 executor 身份创建合法终态、以专用
archiver 身份 capture/release，并确认其他控制器的 finalizer 未被移除；同一证据重试
不得改变 resourceVersion。手工 `operation-audit --action release` 必须同时传入期望的
object store ID、bucket、object key、retention mode 与 retain-until，缺少任一项不得释放
finalizer；内部 release API 也不再提供无 expected scope 的捷径。`operation-audit`
在未显式提供 kubeconfig 时先使用 Pod ServiceAccount 的 in-cluster 配置，再回落到标准
kubeconfig 规则，确保生产镜像内的专用身份 smoke 不依赖 home 目录 kubeconfig。

该补偿只处理 Kubernetes coordination Lease；外部系统副作用仍由 Operation fencing 与
幂等 receipt 约束。

终态审计归档器使用同一 inventory。先创建仅供 archiver 使用的对象存储 Secret；bucket
必须已启用 versioning 与 Object Lock，凭据不得与 backup executor 或 worker 共用：

```shell
kubectl -n kubebrain-operations create secret generic \
  kubebrain-operation-archive-object-store \
  --from-literal=endpoint=https://s3.example.invalid \
  --from-literal=region=us-east-1 \
  --from-literal=access-key-id='<archiver-access-key>' \
  --from-literal=secret-access-key='<archiver-secret-key>' \
  --from-literal=force-path-style=false \
  --from-literal=object-store-id=operations-audit-primary \
  --from-literal=bucket=kubebrain-operation-audit
```

先用零副本 Deployment 做清单和 Secret 引用检查，再显式扩为两个副本：

```shell
kubectl -n kubebrain-operations scale deployment/kubebrain-operation-archiver --replicas=2
kubectl -n kubebrain-operations rollout status deployment/kubebrain-operation-archiver
```

每个终态对象固定写入
`operation-audit/<namespace>/<operation-uid>.json`；保留截止时间固定从
`status.completedAtUnix` 加 Deployment 的 `--retention-duration` 计算，不随重试时间
滑动。多副本可安全竞争同一对象：Object Lock executor 只接受同 body/metadata/retention
的 exact-version 恢复；archiver 在释放 audit finalizer 前还会独立检查 receipt 回显的
object store ID、bucket、object key、retention mode 和 retain-until 等于本次请求，
并在远端下载和 retention 复核后才释放 audit finalizer。若终态
已晚于完整保留窗口，archiver 会 fail closed，必须按审计事件处置，禁止缩短保留期或手工
移除 finalizer。手工 `archive-operation-audit.sh` 也必须把相同 object store ID、bucket、
object key、retention mode 和 retain-until 透传给 release 步骤，缺少或漂移时不得释放
finalizer。中央 archiver Role 只能读取指定 inventory，并 list/get/update Operation
主资源；它不能读取 worker Secret、修改 status、管理 Lease、创建或删除 Operation。
本地 operation audit artifact 与 archive receipt 均是小型 canonical JSON，读取上限为
1 MiB；archive executor 使用已通过 canonical/digest/size 校验的 frozen bytes 上传，远端复核
阶段重新有界读取本地 artifact 以检测 TOCTOU 替换，不能退回无界 `os.ReadFile`。
archiver 未显式传入 kubeconfig 时同样先使用 Pod ServiceAccount 的 in-cluster 配置；
显式 kubeconfig 会跳过 in-cluster 探测，发布 smoke 必须覆盖这两种配置加载分支。

跨进程恢复不得用当前重试时间重新生成 archive receipt。上传成功或
`If-None-Match: *` 冲突恢复取得 version ID 后，executor 必须 Head 精确 version，重新核对
version ID、metadata 和 size，并把该 version 的远端 `LastModified` 固化为
`archived_at_unix`。缺失、非正数或不早于 retain-until 的时间戳均不得发布 receipt。
发布门禁应删除本地 receipt 后再次运行相同 archive，请求时间可以变化，但两份 canonical
receipt 必须逐字节相同；只复用同一路径上的已有本地 receipt 不能证明跨 Pod 幂等。
Put 返回 generic 错误时使用与 backup 相同的独立 30 分钟完整证明链；只有当前对象、
精确 version metadata、完整审计 artifact 和 Object Lock retention 全部匹配才可恢复。
该预算覆盖大对象下载，不能缩成只够一次 Head 的管理写对账窗口。

archiver 每轮使用 `--reconcile-timeout=15m`，预算覆盖 inventory/Operation 扫描、最多
32 个 Object Lock executor 和 finalizer release。deadline 会传入 `CommandContext` 并
终止卡住的对象存储子进程；超时对象不得生成 receipt、不得释放 audit finalizer，下一轮
仍按同一 object key、retention 和 exact-version 规则重试。扩大 `--max-batch` 或跨区域
对象存储延迟时必须同步核算该预算，不能通过关闭 timeout 获得表面吞吐。

每个 Operation 另有 `--archive-timeout=2m` 独立预算，且必须为正并不大于整轮预算。
executor 在独立进程组中运行；单项超时会终止 shell 及其全部后代，避免后代继承输出管道
后拖住 reconcile。该 Operation 保留 audit finalizer，控制器聚合错误后继续处理本批次
后续候选，防止一个失效对象存储请求长期饿死其他租户的终态归档。

若整轮 `--reconcile-timeout` 先耗尽，archiver 会停止向剩余候选传递已取消的 context。
公平游标只在一个候选被实际调用后推进，并记录其
`completedAtUnix/namespace/name` 稳定排序身份；下一轮从该身份之后恢复，即使已完成候选
因 finalizer 释放而从列表消失，也不会因数组下标收缩跳过紧随项。游标是单个 archiver
进程的吞吐公平状态，不是 correctness fence；Pod 重启后从最旧候选开始，多副本仍依赖
Object Lock exact-version 与 Kubernetes resourceVersion 保证幂等和并发安全。

```shell
NS=tenant-a-operations
kubectl -n "$NS" create rolebinding kubebrain-operation-worker \
  --clusterrole=kubebrain-operation-worker-managed-namespace \
  --serviceaccount=kubebrain-operations:kubebrain-backup-executor \
  --serviceaccount=kubebrain-operations:kubebrain-backup-deletion-executor \
  --serviceaccount=kubebrain-operations:kubebrain-restore-cutover-executor \
  --serviceaccount=kubebrain-operations:kubebrain-post-restore-audit-executor \
  --serviceaccount=kubebrain-operations:kubebrain-certificate-rotation-executor \
  --serviceaccount=kubebrain-operations:kubebrain-destroy-executor
kubectl -n "$NS" create rolebinding kubebrain-operation-parameter-broker \
  --clusterrole=kubebrain-operation-parameter-broker-managed-namespace \
  --serviceaccount=kubebrain-operations:kubebrain-operation-parameter-broker
kubectl -n "$NS" create rolebinding kubebrain-operation-approver \
  --clusterrole=kubebrain-operation-approver-managed-namespace \
  --serviceaccount=kubebrain-operations:kubebrain-operation-approver
kubectl -n "$NS" create rolebinding kubebrain-operation-archiver \
  --clusterrole=kubebrain-operation-archiver-managed-namespace \
  --serviceaccount=kubebrain-operations:kubebrain-operation-archiver
```

namespace 下线顺序必须是：先 suspend/delete BackupPolicy，停止新提交；等待所有 Operation
进入 Succeeded/Failed；完成 Object Lock 审计归档并确认
`dbaas.kubebrain.io/operation-audit` finalizer 已移除；再从 `namespaces.json` 删除目标
namespace，并至少等待一次 scheduler reconcile，确认日志不再访问该 namespace；最后停止
worker/archiver、删除 RoleBinding 和 namespace。直接删除仍含 Pending/Running 或未归档终态 Operation 的
namespace 会按设计停在 Terminating，禁止绕过 admission 强删 finalizer。

`hack/production/run-restore-cutover-operation.sh` 接入 RestoreCutover。参数绑定 A184
restore receipt、logical artifact、A189 state/receipt 路径、Service、源/目标 instance、
replicas、公开 endpoint 和 Kubernetes context。执行器按 prepare、cutover、verify、
complete 驱动，每阶段独立续租。prepare 失败可 retry；从 cutover 调用开始，任何失败都
必须执行 rollback 并写 Failed 终态，避免已改 selector 的操作被当成普通重试。

worker 接管时依据 A189 持久证据恢复：只有 state 从 cutover 继续，有 cutover marker 从
verify 继续，已有 receipt 则重做 complete 在线复检后提交；rollback marker 直接记 Failed。
cutover、verified 和 rollback marker 在接管判定或最终提交 succeed 前都必须通过 A189
单行封闭 schema 校验，不能仅凭文件存在推进 operation。heartbeat fencing 时旧 worker
终止子进程且不再回滚或提交，由新 owner/attempt 接管。
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

参数读取服务使用
`deploy/production/kubebrain-operation-parameter-broker.yaml`，默认零副本。先签发服务端
证书，SAN 必须包含
`kubebrain-operation-parameter-broker.kubebrain-operations.svc`，写入 Secret
`kubebrain-operation-parameter-broker-tls` 的 `tls.crt`/`tls.key`；签发 CA 以 `ca.crt`
写入 ConfigMap `kubebrain-operation-parameter-broker-ca`。应用
`kubebrain-operation-worker-admission.yaml` 后扩 broker 到两个副本，并先验证：

```shell
kubectl -n kubebrain-operations auth can-i get secrets \
  --as=system:serviceaccount:kubebrain-operations:kubebrain-backup-executor
kubectl -n kubebrain-operations auth can-i get secrets \
  --as=system:serviceaccount:kubebrain-operations:kubebrain-operation-parameter-broker
```

结果必须依次为 `no`、`yes`。broker SA 另有且只有 TokenReview create ClusterRole；禁止授予
Secret list/watch、Operation list/watch/status 或 Lease 权限。projected token audience
固定为 `kubebrain-operation-parameters`，不能复用默认 Kubernetes API token。broker
不可用、CA 错误、token 失效或 worker Lease 过期时 executor 必须 fail closed 并 requeue，
不得回退为直接读取 Secret。`/v1/parameters` 的 `namespace`、`name`、`owner` 和
`attempt` 必需 query 参数必须各恰好出现一次；缺失、重复或非正 attempt 都应返回 400，
避免代理、审计日志或客户端对重复参数取值不一致。`operationctl --action parameters`
只接受不含 userinfo、query 或 fragment 的 HTTPS broker origin；从 broker 读取的响应超过
4 MiB、`Content-Type` 不是 `application/json`，projected token 为空/超过 16 KiB，或 broker
CA bundle 超过 1 MiB 时必须 fail closed，不能把 `LimitReader` 截断结果或 HTML 错误页写成
参数文件再依赖后续 digest 校验兜底，也不能构造超大 Authorization header 或无界读取错误
CA 文件。

broker 的 `/readyz` 不只检查当前 TLS 证书，还会在同一个
`--kubernetes-request-timeout=5s` 预算内探测 TokenReview create、Operation get 和 Secret
get 三条实际服务路径。探测使用固定不存在的对象名和无效 token，不读取业务 Secret；
NotFound/未认证结果表示 API 路径可用，transport、discovery、超时或 RBAC 错误均返回 503
并把 Pod 摘出 Service。`/healthz` 仍只表示进程存活，不能作为接流条件。业务参数请求也
使用相同 deadline，Kubernetes API 故障时不得让 handler 无界堆积。上线后应临时撤销并
恢复 broker 的 TokenReview 权限，确认所有副本按 `204 -> 503/NotReady -> 204/Ready`
变化，且 UID 不变、零重启。

broker 每 30 秒重新读取 mounted TLS Secret。新 `tls.crt`/`tls.key` 只有在公私钥匹配、
叶证书已生效且未过期时才会原子接管新握手；无效更新保留上一份有效证书并记录错误。
readiness 使用 `/readyz` 检查当前证书有效期，证书最终过期时必须摘流；liveness
`/healthz` 不因轮换失败杀死仍可诊断的进程。同一 CA 下轮换叶证书时，记录两个 broker
Pod UID，更新 Secret 后在 30 秒加 probe 容差内验证 endpoint 呈现新 serial，且 Pod UID
不变。轮换 CA 时必须先把旧、新 CA 同时发布到 executor trust bundle，再换 broker
叶证书，最后确认所有 executor 使用新 CA 后撤旧；服务端热加载不能替代该双信任窗口。

六类生产 executor 模板位于
`deploy/production/kubebrain-operation-executors.yaml`，默认全部为零副本。启用任意一类
之前必须创建同名 `*-executor-env` Secret 和 `*-executor-workspace` PVC；证书轮换还必须
创建 `kubebrain-certificate-rotation-executor-hooks` Secret，键
`publish-overlap`、`publish-final` 必须是可执行、幂等且受发布流程审计的程序。生产 PVC
必须支持 RWX 和 `runAsUser/fsGroup=65532`；两个副本会竞争同一队列并依赖 Lease/attempt
fencing，RWO 卷或节点本地卷不能满足跨节点接管。参数里的 artifact、state、receipt 路径
必须位于 `/var/lib/kubebrain-operation`，临时文件才可放 `/tmp`。

Secret 只保存该类型所需 endpoint、对象存储或 Kubernetes context 配置，不得放集群管理员
kubeconfig，也不得把私钥写入 Operation parameters。长驻 Pod 不设置 `PARAMETERS_INPUT`：
executor claim 后由 operationctl 按 immutable Secret 名称和 SHA-256 获取参数，避免 Pod
启动配置与实际 claim 错配。上线顺序是应用零副本模板、完成 Secret/PVC 权限检查、以
one-shot Operation 做 claim/heartbeat/receipt 演练，再扩到两个副本并观察无重复副作用。
滚动策略允许升级期间短暂三副本竞争，所有外部 hook 因此必须按 operation UID 和 attempt
幂等。

六类模板使用独立 ServiceAccount。它们能读取 Operation、更新 status 和管理实例 Lease，
但不能调用 Secret API；动态参数只能由 broker 在验证 SA 类型、owner、attempt 和 Lease
后返回。该边界阻断同 namespace 的跨类型 Secret 读取，但 env Secret/PVC 本身仍由 kubelet
挂载，节点或 broker 被攻陷不在此边界内。更高等级租户仍应拆分 operation namespace、
inventory、broker、KMS 密钥和精确 RoleBinding。

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
