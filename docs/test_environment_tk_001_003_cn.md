# tk-001-003 测试环境交接记录

最后核验：2026-09-08。本文件记录用户明确授权的测试环境，供长会话恢复时重新核验；不以历史状态代替实时检查。

## 授权与连接

- 用户授权通过 SSH `root@10.32.32.66` 登录控制节点部署测试，并允许复制远端 `/etc/kubernetes/admin.conf` 到本机。
- 控制节点 hostname：`control1.cloud.local`；Kubernetes Node：`control1`。
- 本次首次连接接受并记录的 ED25519 host key 指纹：`SHA256:9oKH+KtMEDDPFHpzwscXj1OzLnRTw9uzbtGMNfhoU7c`；后续变化应停止并核验，不自动覆盖 known_hosts。
- 本机 kubeconfig：`/root/.kube/kubebrain-test-10.32.32.66.conf`，权限 `0600`，位于仓库外。
- 本机该文件中的 context：`kubebrain-test-10.32.32.66`；cluster：`tk-001-003`。
- 集群身份锚点：`kube-system` namespace UID `ca72e2c8-53a6-4c40-ba04-6d07b2b03aa6`。
- 每条 kubectl/Helm 命令显式指定该 kubeconfig；不切换默认 kind context，不操作原 `kind-kubebrain-dbaas` 实验数据。
- SSH 密码由用户通过会话提供，不记录在本文、Git、命令行参数或日志；kubeconfig 的私钥、证书及 token 内容同样不得提交或打印。

## 存储硬边界

**仅使用 rook-ceph 消费者集群的 StorageClass；禁止使用 rook-ceph-secondary 集群。**

已核验并选定的 StorageClass：`nvme-rep3-rbd-pool`。

| 字段 | 核验值 |
| --- | --- |
| StorageClass UID | `d3283670-e235-4676-ad5a-11f42ce6526e` |
| provisioner | `rook-ceph.rbd.csi.ceph.com` |
| parameters.clusterID | `rook-ceph` |
| pool | `nvme-rep3-rbd-pool` |
| provisioner/node-stage/controller-expand Secret namespace | 全部为 `rook-ceph` |
| filesystem | `ext4` |
| reclaimPolicy / binding / expansion | `Delete` / `Immediate` / `true` |

所有本次 PVC 必须显式指定该 StorageClass，不能依赖默认 `nvme-ec42-rbd-pool`。创建后还需核对 PV 的 CSI driver、volumeHandle、claimRef 以及实际文件系统容量。不能修改现有 StorageClass、Ceph pool 或 rook-ceph-secondary 的任何对象来使测试通过。`rook-ceph/rook-ceph-external` 显示 external.enable=true、health=HEALTH_OK，但 phase=Progressing；是否可供卷必须以实际 PVC/挂载测试确认。

## 已观察的环境与部署边界

- Kubernetes `v1.36.0`，Ubuntu 26.04、amd64，CRI-O `1.35.3`；11 个节点当前 Ready。
- 候选测试 worker：`k8s3-worker1/2/3`，IP `10.32.32.70/71/72`；每节点 allocatable CPU=15、内存约 28 GiB，无 taint。
- 三 worker 没有 zone 标签；独立 Node 不等于已证明独立物理宿主机或可用区，不伪造 zone 标签。
- worker 上存在用户既有 Cilium、OpenStack、rook-ceph-secondary 等工作负载。**本次部署授权不等于可重启整台 worker、隔离宿主网络或影响既有 Ceph 服务；此类故障注入需另行确认精确范围。**
- 本次新工作负载限定于独立 namespace `kubebrain-dbaas-test`，UID `6c57c242-912b-41bb-9020-f4fdb3225ef3`；首次核验该 namespace 不存在。共享 CRD/Operator 等前置资源先检查已有安装和兼容性，不覆盖用户现有配置。
- 首次检查没有 TiDB Operator/TidbCluster CRD；本次已新增官方 v1.6.5 CRD 和 namespace-scoped Operator。Prometheus Operator 仍未安装。
- VolumeSnapshot 三类 CRD 原已存在。按 Deployment 名称检查未发现独立 snapshot-controller，但进一步按容器镜像核验，在 `openebs/openebs-zfs-localpv-controller` 中发现通用 `snapshot-controller:v8.2.0`；已复用它，未安装第二套控制器。rook-ceph CSI 的 snapshotter sidecar 为 v8.5.0。
- 部署阶段使用小数据量与有界测试资源；还原/故障测试只针对本次创建并核验 UID 的专属对象，不删除用户已有 PVC/PV。

## 进度

### 已部署的独立后端

- Helm release：`kubebrain-tidb-operator`，namespace 为 `kubebrain-dbaas-test`，chart/app 均为 v1.6.5；`clusterScoped=false`、`scheduler.create=false`、controllerManager 两副本。共享 nodes/PV/StorageClass 权限仍按官方 chart 配置，不能把 namespace-scoped 描述成完全没有 cluster-scope RBAC。
- CRD 来自 [官方 v1.6.5 清单](https://raw.githubusercontent.com/pingcap/tidb-operator/v1.6.5/manifests/crd.yaml)，下载文件 SHA-256 为 `a90106c487335a942e4ade08a757cc07484d9c26101a04d00352bdabb4409281`；创建前已确认没有 PingCAP CRD。
- TidbCluster `kb`，UID `28d3e529-6790-4dd1-a6c9-7bccc48a11f1`；PD/TiKV v8.5.3，3+3 副本，不部署 TiDB SQL。
- PD requests 为每副本 1 CPU/2 GiB/20 GiB PVC，TiKV 为 4 CPU/8 GiB/100 GiB PVC；limits 分别为 2 CPU/4 GiB 和 8 CPU/16 GiB。仅限 worker1/2/3，每组件硬 hostname 反亲和。
- 六份数据 PVC 全部为 `nvme-rep3-rbd-pool`，独立 PV UID 与 CSI volumeHandle，clusterID 全部为 `rook-ceph`；后端共申请 360 GiB 逻辑容量。`pvReclaimPolicy=Retain`，初始 `maxFailoverCount=0` 控制资源增长；这不是生产故障接管配置验收。
- 最终 `kb-pd-0/1/2` 分别位于 worker3/1/2；`kb-tikv-0/1/2` 分别位于 worker1/2/3；六 Pod Ready、restart 0。PD cluster ID `7683177044639569228`，三个 store ID `2001/2004/2005` 为 Up。
- 已观察的 PD imageID：`docker.io/pingcap/pd@sha256:b32c69d8b9cc08cead83649d54c58942c441492b459c4cf190cd8c4747bf85f3`；TiKV imageID：`docker.io/pingcap/tikv@sha256:00502c3c74915577ff3a669a223a3740c3f1f7e151de745620a95a5f1450a909`。当前 CR 使用版本 tag，尚未宣称完成镜像供应链发布门禁。

首次配置有两个已处理的问题：instance label `kubebrain-test` 与期望的 `kb` 不一致，且 NetworkPolicy 误选 discovery，阻断其读取 API。已收紧 policy 只选择 PD/TiKV，并统一 instance label 为 `kb`。由于 selector 不可变，按本次对象的 UID/resourceVersion 删除重建 discovery/PD 工作负载，保留原三份 PD PVC；未删除用户已有资源。首次 PD 启动出现过一次重启，最终重建后的六个数据 Pod 均为 restart 0，不能用最终计数抹掉该启动历史。

### rook-ceph 快照恢复验证

- 创建了专属 VolumeSnapshotClass `kubebrain-test-rook-ceph-rbd`，driver 为 `rook-ceph.rbd.csi.ceph.com`，clusterID 和 snapshotter Secret namespace 均为 `rook-ceph`；不是默认 class。
- 源 PVC `storage-smoke`：UID `ed73d09a-c7a1-421e-bd21-c6c8a679a2af`，1 GiB，在 worker2 挂载；实际文件系统容量 996,780 KiB，不是宿主根盘容量。
- 源文件 65,536 bytes，sync 后 SHA-256 为 `84f879be8ef136113274a48472fe2953005de77fd112759a745ff93d297d59f8`。
- VolumeSnapshot `storage-smoke`：UID `ab1f45ed-cb7a-4df5-a6e0-c2d30cbf8baf`，readyToUse=true；content UID `6e348164-e040-4d8e-901f-abf49b84d434`，source volumeHandle 精确绑定源卷。
- 恢复 PVC `storage-smoke-restored`：UID `724bb1fa-ef78-4817-b81d-b5d048437e86`，仍显式指定同一 rook-ceph StorageClass，但分配不同 volumeHandle；在 worker3 挂载后逐字节摘要相同，验证 Pod Succeeded。
- 该结果证明此 CSI 路径的基本快照/跨节点恢复，不代替 KubeBrain native PITR、多个数据卷的一致性恢复或跨可用区故障证明。两个 1 GiB 测试卷及快照暂时保留供后续核验。

### Region/存储门禁与网络边界

原样运行 `validate-tikv-region-health.sh` 的 API Service proxy 路径返回 `cannot read PD stores response`。Cilium 定向 drop 观测确认 proxy 源为 `240.16.0.57`、identity `remote-node`；既不是外部 CIDR 身份，也不是 `kube-apiserver` 实体，且集群 `enable-node-selector-labels=false`。没有更改 CNI 全局配置或放开所有 node。两份无效诊断放行 policy 已按 UID/resourceVersion 删除；保留 namespace 内 PD/TiKV peer 通信和 DNS 的隔离策略。

使用本机适配器 `kubectl-pd-exec.sh`，仅将该门禁的六类只读 PD GET 改为固定 `kb-pd-0` 容器内 localhost curl，其余 kubectl 请求原样执行；每次请求先核对 Pod UID `a06f3224-daac-4966-a69a-033c0fbe8d4b`，Pod 替换后必须重新审计并更新锚点。门禁判定代码、PVC/PV/claimRef/容量/水位阈值和采样次数均未修改。结果：3 PD、3 Up store、连续 3 次无异常 Region、六卷身份/容量/磁盘水位通过。**这是 exec 传输适配后的通过，不是原 Service proxy 入口已恢复。**

### 本机持久材料与下一步

非凭据部署材料位于 `/root/.local/state/kubebrain/tk-001-003/`（目录 0700），包括 namespace/PVC/探针、快照/恢复、TidbCluster 清单、Operator 渲染输出与只读传输适配器。`storage-gate-pd-exec.log` 保存门禁终态输出。TidbCluster 清单 SHA-256 为 `818538d9b94e26dba221a958c58d42a96b9256fe752638c2bb1780aa116250f2`，适配器 SHA-256 为 `0b8b8817ab9c0adebfe3d8df0ce872239d4718fab56595d05edb58119f989bd5`。

KubeBrain 本体尚未部署：已有本机 A5788 OCI 包，但新集群无已发现的镜像 registry Service，控制节点到 worker 的 BatchMode SSH 认证失败。未向其他节点复用控制节点密码，也未修改 worker 的 CRI-O/证书/registry 配置。用户随后确认使用 KubeBrain 仓库的 GitHub Actions 发布镜像，要求 `runs-on: self-hosted`，并授权推送 `dbaas`、触发构建和使用已登录的本机 `gh` 跟踪状态。

### CI 镜像交接约定

- 仓库为 `fivetime/kubebrain`，不是 kubetron；不将 `main` 合并进当前开发分支，不强推。
- `dbaas` 分支的 `.github/workflows/image.yml` 使用 self-hosted Runner，支持该分支 push 与手动触发；构建仓库根 `Dockerfile` 的 TiKV 镜像，平台为 linux/amd64 和 linux/arm64。
- 先推送 `ghcr.io/fivetime/kubebrain:dbaas-<完整 commit SHA>`，核对双架构索引、运行时版本/SHA/构建时间、TiKV 后端、OCI labels 与非 root 用户后，才更新 `:dbaas`；不改 `:latest`。
- 部署时使用成功运行返回的 `ghcr.io/fivetime/kubebrain@sha256:<digest>`，不能仅凭可变 tag 或 workflow 已触发就认定镜像可用。需记录 Actions run URL、源 commit、最终 digest、验证结果，并在测试集群核对实际 imageID。
- 本节记录配置与授权；构建成功、worker 拉取能力和 KubeBrain 部署仍须以运行结果核验，不以计划代替成功记录。

本次已快进推送 `8963d2ff590bb4a0b4f779f701843c34a9c5a350` 到 `dbaas`，由 push 自动触发 [Actions run 34247183115](https://github.com/fivetime/kubebrain/actions/runs/34247183115)，未重复手动派发。self-hosted Runner `raas-1482` 构建、双架构索引验证、运行时 metadata 验证及 `:dbaas` 标签发布均成功；镜像为 `ghcr.io/fivetime/kubebrain@sha256:033668da577610611b5e9c82b81f7002d062320135463043a940d4f7cc4c7b81`，未覆盖 `latest`。该镜像对应客户端分仓前的产品提交，不能记为分仓后的构建结果；测试集群尚未部署该 KubeBrain 镜像。该提交前后均执行 `--verify 4` 与四个生产测试分片，两轮各 703 项全部通过；workflow 的 Go 回归测试及 actionlint v1.7.12 也通过。

### 客户端分仓后的真实传输测试

独立客户端固定版本、补丁历史、CI 和安全扫描结果见[分仓维护说明](tikv_client_maintenance_cn.md)。本次使用远端依赖编译原有 `TestLargeKeyRoundTripTiKV`，通过 worker3 上专属 Pod `client-split-large-key` 连接 `kb-pd`；PD cluster ID 与本环境锚点一致。600 KiB 和 2 MiB 物理 Key 的 Put/Get/Iter 校验通过（1.95 秒），Pod UID `0291d0d9-6b06-4de1-a035-f346c433577f`、Succeeded、exit 0。用例清理自己的前缀；未新建 PVC，不接触 rook-ceph-secondary。

二进制 SHA-256 `a1af25f6b947367e974806ecc5a465e45504ab3fca9775e8f19684a0e7491058`；结果保存在上述仓库外材料目录的 `client-split-large-key.log` 与 `client-split-large-key-result.json`。测试 Pod 按 UID 删除，本机临时二进制已清理。该测试不是 KubeBrain 本体部署，也不是整个产品恢复/故障验收。

分仓镜像 [run 34253661949](https://github.com/fivetime/kubebrain/actions/runs/34253661949) 已 success，
源 commit `522da3334662200606d01e32dad8556238a3fc40`，双架构 digest
`sha256:1e1dd21b06c7cd0ff1fbfda04ea26d55f14e98949ca9333c0eff4f8fd12871cb`。
该镜像仍是 Go 1.26.5 的旧安全基线，尚未部署；不得用其构建成功代替后续安全修复镜像的验证。

后续安全升级客户端 `b5b63af11282` 的真实大 key 复测也已通过，1.45 秒；专属 Pod
UID `743095a9-d977-41f5-954a-896f906ed101`，已按 UID 删除，本机临时测试二进制已清理。
环境材料目录新增 `client-security-large-key.log` / `client-security-large-key-result.json`，
详情和未完成门禁见[安全升级记录](security_baseline_20260908_cn.md)。

补充安全核验：镜像原附带官方 kubectl v1.36.2 的二进制扫描报告 18 项漏洞；现采用独立模块
重编译的 `v1.36.4+kubebrain`，保留 v1.35/v1.36 server 的版本差窗口。Go 1.26.8 下的
amd64/arm64 二进制扫描均无漏洞报告，容器构建输出与本地扫描产物逐字节一致。使用该 amd64
工具只读核验本环境 namespace UID、PD cluster ID 和 server-side dry-run，未创建实际 Pod。
API `/version` 本次返回 v1.36.1；此前 v1.36.0 来自节点 kubelet 信息，二者不混同。
本机全局 kubectl 和用户 Kubernetes 集群未升级。记录位于环境材料目录 `security-kubectl-*`；
临时下载的候选工具和本地/容器构建输出已清理，后续可按固定模块及 metadata 重新构建。

安全更新产品提交 `5645caa6b014c2c17b84af968f242efeee3a388a` 已完成提交前、提交后的
703 项生产四分片验证并推送。新镜像由 push 自动触发
[run 34264830428](https://github.com/fivetime/kubebrain/actions/runs/34264830428)，首次回读 queued；
待验证成功后记录不可变 digest。完整本地预提交镜像的 66 个 Go 可执行文件均通过二进制扫描，
该验证镜像、提取容器和临时二进制已清理；`security-product-image-*` 日志留存。
本地构建使用预提交 metadata，不冒充该产品提交的发布产物。

取得新安全基线已验证的镜像后，部署三副本 KubeBrain、客户端语义测试及产品恢复验证，再补监控和故障演练。不要将当前后端与 CSI 冒烟测试标为整个产品验收完成。

### 安全基线部署准备（2026-09-08）

镜像 run `34264830428` 已由 self-hosted Runner `raas-1494` 接手，源码模块扫描和 kubectl
双架构二进制扫描均 success，产品镜像构建仍 in_progress。文档提交 `d1f63ee8` 使用
`[skip ci]`，未取消或替换该代码提交的构建任务。

本次重新核验两个 namespace UID、StorageClass UID/rook-ceph 参数、PD cluster ID；
原判定脚本通过同一只读 exec 适配器再次得到 3 PD/3 Up store、连续 3 次无异常 Region、
六份后端数据卷身份/容量/磁盘水位通过。日志 `security-predeploy-storage-gate.log`。

基于产品 `deploy/production/kubebrain-tls.yaml` 准备的测试 overlay 已通过 server-side dry-run：
三副本限定 worker1/2/3，保留硬 hostname 反亲和、非 root、只读根文件系统和 mTLS；
独立 keyspace/cluster-name 为 `kubebrain-dbaas-test`。本次小数据测试设逻辑配额 2 GiB，
每副本 snapshot/spill 各 4 GiB、spill 进程上限 2 GiB，共将新增 24 GiB 逻辑工作卷。
全部显式使用 `nvme-rep3-rbd-pool`；独立 PVC 不等于独立物理 Ceph I/O 故障域，
不以本测试规格证明生产容量或性能。尚未创建这些 KubeBrain 工作卷或 StatefulSet。

已创建的前置对象：

- PriorityClass `kubebrain-dbaas-critical`，UID `2b5d406a-0ae0-4e38-bcfc-9f34e7c18037`，
  value=1000000、非默认、`preemptionPolicy=Never`；未修改既有 PD/TiKV 工作负载或其他用户 Pod。
- namespace 内 NetworkPolicy `kubebrain-test-isolation`，仅选择本次 KubeBrain 标签；
  允许同 namespace 通信及 kube-dns，未放开外部 namespace 或节点网络。
- Secret `kubebrain-client-tls` / `kubebrain-peer-tls` / `kubebrain-info-tls`，UID 分别为
  `6209f0a8-36e1-4abe-96f7-f26fe1aebdc9`、`17124849-aa91-4033-81f2-a41e5730bcea`、
  `155aa30d-d10b-4778-9c53-da2716b3b3ee`。
- 单独的探针 Secret `kubebrain-test-client-tls`，UID
  `d9ed215c-0818-413e-88d4-04e41d4122e3`，仅包含客户端 CA 和 CN=root 的测试客户端叶身份，
  不向探针提供服务端或 CA 私钥。

三套独立测试 CA 与叶证书、独立 CN=root 的测试客户端身份存于材料目录的 `tls/`，
目录 0700、私钥 0600，不提交 Git。客户端叶证书到期时间为 2026-12-07 18:51:12 UTC；
测试 PKI 不等于生产证书签发/自动轮换已验收。当前 PD/TiKV 仍为隔离网络内明文，
前端/peer mTLS 不替代后端传输加密，后续必须单独完成该门禁。

材料目录新增 `generate-test-tls.sh`（拒绝覆盖已有 tls 目录）、`kubebrain-test-overlay.sed`、
`kubebrain-network-policy.yaml`、`kubebrain-tls-candidate.yaml`。candidate 仍含 `kubebrain:dev`
占位，仅作 dry-run，**不可直接部署**；取得成功 CI 的固定 digest 后重新渲染并审计。
继续禁止对整个材料目录执行 apply，避免应用历史诊断或未完成清单。

待执行的 `kubebrain-security-smoke-template.yaml` 同样必须先替换 IMAGE_DIGEST/STATEFULSET_UID；
使用镜像内既有 audit/availability 二进制，限定 worker3、非 root、无 SA token、900 秒总寿命。
计划逐副本运行事务读写审计，再完成 300 轮公共/三副本直连 Watch、Lease、RangeStream、Snapshot
与 PD TSO/TiKV Region 探针。其独占前缀为
`/kubebrain-rollout-availability/security-smoke-5645caa6/`，外部预记录 lease IDs 为
`2026090819050001/2026090819050002/2026090819050003`，程序会拒绝复用已存在 ID。
该探针尚未创建、测试尚未运行，不把计划或模板当作 PASS。失败后必须先按 Pod/StatefulSet UID
和上述 fixture 身份核验清理，不能直接重跑来掩盖残留；成功后也需回读数据面零重启、卷身份与工作目录清理。

### 发布产物核验中（后续架构检查已拒绝此候选）

固定源码标签现已上传，双架构 digest 为
`sha256:6ab33dc81572dfc318b02d4f840252111790690e1fa04d20af1f772db309a224`。
独立下载 amd64 后，版本/SHA/Go 1.26.8/TiKV/构建时间/OCI labels/非 root 均匹配；
实际镜像中的 66 个 Go 可执行文件二进制扫描全部通过。证据与边界见
[安全基线记录](security_baseline_20260908_cn.md)。CI 同一 run 仍 build/push in_progress，
未宣称发布完成或部署成功；继续等待 Verify/Promote 及终态。

从固定源码 `5645caa6` 加测试 overlay 重新渲染 `kubebrain-tls-5645caa6.yaml`，
全部镜像使用上述 digest，文件 SHA-256 为
`5d8d39df467d8f660d10095a87e1b790531d2fc0cc26cb437a7d7b7928ca07f8`。
再次通过 server-side dry-run，且逐项核对恰好五个 namespaced 对象、三副本、两类工作卷的
rook-ceph StorageClass 与 4 GiB 配置；没有创建 StatefulSet 或这些工作卷。

本次读取实时负载时，Metrics API 返回不可用；没有安装或修改用户的 metrics-server。
节点资源请求和 Ready/Pressure 状态可读，调度余量不替代实时 CPU/内存负载或监控系统验收。
PD/TiKV 六个 Pod 的 UID 未变化，均 Ready、restart 0。

后续核验直接使用 arm64 子镜像 digest，发现其主程序与 kubectl 实际均为 x86-64，
与 amd64 产物字节完全相同。因此 **`6ab33dc8…` 不可部署**，此前索引/amd64 安全扫描通过
不等于全平台发布通过。错误来自 Dockerfile 的 TARGETARCH 默认值覆盖自动平台参数，
修复和失败/通过证据见[安全基线记录](security_baseline_20260908_cn.md)。
`kubebrain-tls-5645caa6.yaml` 清单已被判定为无效候选，不能因为此前 dry-run 通过而创建它；
StatefulSet 和六份 KubeBrain 工作卷仍未创建。保留原有 TLS/隔离前置资源，等待修复提交的新镜像。

原 run `34264830428` 已在 Verify published test image 步骤失败，Promote skipped；未部署候选。
修复已通过真实 BuildKit 自动平台回归和完整 arm64 编译阶段的 69 个程序架构/漏洞扫描；
其中 66 个属于主运行镜像，三个属于独立备份/恢复镜像。提交前 703 项四分片也已全部通过。
这些本地验证尚不能代替修复提交的新 CI 和真实部署。错误候选及编译阶段的本地镜像、大体积
提取文件均已清理，日志保留；最后暂留的 kubectl 及整个
`/tmp/kubebrain-release-audit.rIYu3W` 临时目录也已删除，未替换全局 kubectl 或清空共享缓存。

架构修复已本地提交 `19c23ca6`，提交后 verifier 及全部四分片通过，尚未推送。
CI 原失败日志显示先遇到同一索引 digest 切换平台拉取的 `cannot overwrite digest`，并非已经
运行到 arm64 `cmp`。下一步还须改成按平台子镜像 digest 拉取，再对修复后的整套工作流
重新提交验证；旧镜像的 arm64 程序错误已由本地直接子镜像提取独立证明。
不能跳过该拉取错误或部署 amd64-only 候选来宣称双架构发布完成。

`19c23ca6` 提交后测试结束，已开始后续工作流修复：验证阶段按唯一子镜像 digest
pull/run/create/inspect，提升阶段继续使用完整双架构索引。解析器与工作流回归已加入，
完整提交前后测试及新镜像 CI 尚待完成。当前仍不得部署旧候选。

子镜像 digest 修复的提交前 verifier/四分片现已全部通过（703 项）；提交后完整验证与
新镜像 CI 仍是后续门禁。详细日志名、时长与验证范围见[安全基线记录](security_baseline_20260908_cn.md)。

2026-09-08 20:11 UTC 只读复核：kube-system 与测试 namespace 的 UID 仍匹配前述锚点；
PD/TiKV 六个 Pod 均 Ready、restart 0；`kubebrain` StatefulSet 查询仍为空。
`nvme-rep3-rbd-pool` UID、CSI provisioner、clusterID 及三项 Secret namespace 仍匹配
rook-ceph 消费者集群。未新增 KubeBrain Pod/PVC 或修改集群配置；后续实际部署前仍须重新核验。

### 后端 TLS 门禁预审（未执行迁移）

当前前端/peer/info 证书不覆盖 PD/TiKV。KubeBrain 已提供 `--tikv-ca-file`、
`--tikv-cert-file`、`--tikv-key-file` 和 `--tikv-verify-cn`，但本测试 overlay 尚未挂载
后端客户端证书或配置这些参数，不能把前端 mTLS 当作端到端加密验收。

已查阅 [TiDB Operator v1.6 组件间 TLS 指南](https://docs.pingcap.com/tidb-in-kubernetes/stable/enable-tls-between-components/)：
现有明文集群的迁移不等价于普通滚动修改 `spec.tlsCluster.enabled`，官方流程涉及 PD
临时缩容、TLS 重启、内嵌 etcd peerURL 变更和恢复副本。须先审计维护窗口、现有数据保护
与回退步骤，不能直接在当前三副本上试改开关，也不删除既有 PVC 来绕过迁移。
证书应使用独立后端 CA，按 Operator 约定提供 `kb-pd-cluster-secret`、
`kb-tikv-cluster-secret`、`kb-cluster-client-secret`；SAN 必须覆盖实际 peer/service DNS。
迁移后需同步 HTTPS PD 探针、客户端 TLS、只读适配器的新 Pod UID 锚点，以及正向连接、
错误 CA/CN 拒绝、Region 健康和大 Key 回归。本文只记录预审，不代表这些资源或步骤已执行。

预审运行 `go test ./cmd/option ./pkg/storage/tikv -run 'Test.*(TLS|Security|CN)' -count=1 -v`：
cmd/option 的前端 TLS 参数、后端证书全有或全无校验、后端参数绑定共三个顶层测试通过；
pkg/storage/tikv 明确输出 `[no tests to run]`，不计为后端传输测试通过。
证据为材料目录 `security-backend-tls-unit-preflight.log`。本轮按 VerifyCN/ClusterVerifyCN
检索仓库测试，仅定位到参数绑定断言；须补充实际后端 TLSConfig 的可信 CA/匹配 CN 正向
握手与错误 CA/CN 拒绝回归，并与授权 TiKV/PD 真正开启 TLS 后的端到端测试分别记录。

### 修复镜像 CI 已触发

子镜像 digest 修复 `339381afb74eb225d7bab8e67196ccea07596509` 的提交前后两轮 verifier
及四分片（各 703 项）均已通过，前置架构修复 `19c23ca6` 也已完成自己的两轮验证。
两提交已快进推送 dbaas，自动触发 [run 34275611099](https://github.com/fivetime/kubebrain/actions/runs/34275611099)，
创建于 `2026-09-08T20:34:39Z`，首次状态 queued，源码 SHA 一致；尚未取得可部署的固定
digest。继续保留旧候选 `6ab33dc8…` 的拒绝结论，不以新 CI 启动代替发布门禁通过。

### 后端 TLS loopback 回归已补充

`pkg/storage/tikv/security_test.go` 新增实际 TLS 1.2/1.3 双向握手与客户端证书轮换测试，
覆盖及限定范围见[客户端维护说明](tikv_client_maintenance_cn.md)。新用例普通测试与 race
连续 10 次通过；整个存储包本机单测/race、vet 通过，但三个真实 TiKV 集成测试明确跳过，
不计为本环境后端 TLS 验收。日志为 `security-backend-tls-handshake.log`、
`security-backend-tls-handshake-race.log`、`security-backend-storage-unit.log`、
`security-backend-storage-unit-race.log`。首次用例误把 TLS 1.2 缺失客户端证书时的
handshake failure 断言成 TLS 1.3 的证书错误，后按协议分别断言，仍要求连接失败，未改产品逻辑。

负向日志 `security-backend-tls-isolated-baseline-negative.log`：在临时副本中只将固定
客户端的 `config/security.go` 替换为上游 v2.0.7 对应文件，wrong_CN 用例在两个协议版本
均因“期望错误但实际连接成功”失败；检查退出码及两个明确失败用例后才认定复现。
首次尝试直接 overlay 模块缓存被 Go 拒绝，日志 `security-backend-tls-upstream-baseline-negative.log`
不构成行为证据；当时外层命令未对后续匹配失败立即退出，打印的预期复现尾行无效。
后改用独立客户端归档、临时 modfile/overlay 并严格检查失败输出，未改正式 go.mod、fork
工作树或模块缓存。临时副本清理后只保留日志。新增测试尚未提交，不会改变在运行的
image run `34275611099`（源码仍是 `339381af`）。

临时归档 `/tmp/kubebrain-tls-baseline.L06FtU` 已完整删除；正式 `go mod verify` 全部通过，
fork 工作树保持干净，未清空共享 Go/BuildKit 缓存。新测试的提交前 verifier 确认 703 项
完整分配（170/193/180/160），四分片全部通过（251.971/452.596/308.033/731.406 秒），
日志为 `security-backend-tls-pre-verify.log` 及 `security-backend-tls-pre-shard-{0,1,2,3}.log`。
接着提交测试补充，并立即运行相同的提交后 verifier/四分片；提交后终态仍待回读。

镜像 CI `34275611099` 已由 self-hosted Runner `raas-1502` 接手，job
`102227752676` 开始于 `2026-09-08T20:35:09Z`。自动目标架构回归与 runtime manifest
选择回归均于 `20:40:06Z` 成功完成，build/push 从 `20:40:07Z` 开始，当前仍在运行。
这证明新增的两个构建前检查已在 Runner 执行，不替代后续实际镜像字节/架构核验或最终发布成功。

为避免当前镜像构建被 concurrency 策略取消，TLS 测试补充提交暂留本地，不立即推送。
该提交仅增加测试及证据文档，不改运行时代码或客户端依赖；在运行镜像的源码仍为 `339381af`。

TLS 回归提交为 `0f78c31a4885cae154fd5833dff24e24b5fd75fa`，提交后 verifier 与四分片
703 项全部通过（285.912/486.156/322.032/764.257 秒），日志为
`security-backend-tls-post-verify.log`、`security-backend-tls-post-shard-{0,1,2,3}.log`。
提交后存储包普通/race、vet 也通过（本机模式，三个真实 TiKV 集成用例仍未执行），
日志为 `security-backend-storage-post-unit.log`、`security-backend-storage-post-unit-race.log`。
该提交仍仅在本地，未推送以免取消在运行的镜像 CI。

等待期间创建隔离工作树 `/tmp/kubebrain-staticcheck.gUgTUr/repo`（detached 于同一
`0f78c31a`）处理既有 52 项 Staticcheck 诊断；基线日志
`security-staticcheck-isolated-before.log` 与此前诊断一致。草稿仅在此工作树，尚未提交或
合回主工作树；须验证后迁移，并执行新代码提交前后完整测试。迁移完成前不要清理该目录，
完成后定向移除临时 worktree，不清空共享缓存。

### Staticcheck 清理与传输回归（2026-09-08）

隔离草稿已通过根模块 Staticcheck/vet，以及 objectstore 子模块 Staticcheck。删除的是
无调用的旧辅助函数和未使用赋值；保留告警解除的 CAS 冲突失败、事务见证验证、领导权
就绪后的 epoch、Watch 首响应校验。两个故意传 nil context 的负向测试保留原输入及
断言，仅对对应行标注 SA1012 的原因；未全局屏蔽诊断或减弱门禁。

native-pitr-preflight 从弃用的 DialContext/WithBlock 迁移到 NewClient，显式保留
passthrough 地址解析、连接 Ready 屏障和调用方 context。新增本机 TCP/gRPC 测试覆盖
HTTP/2 尚未就绪时等待、deadline/cancel、空请求正常响应、意外 checkpoint 和 RPC 错误。
普通测试通过；与 TiKV 存储包一起 race 连续三轮通过。protobuf 测试改用 V2/protoadapt
桥接固定的 V1 kvproto 消息，保留所有字节所有权、错误响应、复用对象和分配阈值断言。

核心包普通测试全部通过：backend 50.654 秒、server/etcd 143.087 秒、endpoint 22.601 秒、
etcdproxy 1.776 秒、storage/tikv 0.206 秒。备份验证和发布探针等七个受影响包也全部通过，
其中 rollout-availability-probe 124.463 秒。告警/事务见证/领导权相关筛选 race 在 backend
和 server/etcd 分别 6.342/12.066 秒通过，不等同于这两个包的全量 race。
TiKV 存储包仍未设置真实 PD 地址，三个环境依赖测试跳过，不计作真实 TiKV 验收。

编解码 Scan/Batch 两项 fuzz 各 30 秒通过（86,711/76,851 次执行）；本机分配 benchmark
为 generated 6,159 allocs/op、owned-frame 4 allocs/op，原分配阈值测试通过。此数据仅说明
测试 API 迁移没有丢失相应检查，不代表生产负载性能验收。

证据均在仓库外材料目录，前缀 `security-staticcheck-`：`isolated-final.log`、
`isolated-with-transport.log`、`isolated-vet.log`、`objectstore.log`、`core-unit.log`、
`probes-unit.log`、`transport-unit.log`、`transport-codec-race.log`、`fence-race.log`、
`codec-fuzz-scan.log`、`codec-fuzz-batch.log`、`codec-benchmark.log`。

已用 apply_patch 迁回主工作树，并逐字节比较全部 Go diff 和新增测试，确认一致后定向
移除 `/tmp/kubebrain-staticcheck.gUgTUr/repo` 及空父目录；改动完整保留在主工作树，未清理
共享 Go/BuildKit 缓存。主工作树 Staticcheck 再次通过（`main-pre.log`）。提交前 verifier
通过，仍是 703 项、170/193/180/160；四分片已启动，结果待回读（`pre-verify.log`、
`pre-shard-{0,1,2,3}.log`）。本批改动尚未提交，提交后门禁尚未运行。

`2026-09-08T21:37Z` 回读镜像 run `34275611099` 仍为 build/push in_progress；尚未执行
Verify/Promote，不推送新代码取消它，不部署旧的已拒绝镜像。本轮没有对测试集群执行写入。

补充构建检查：`go build -tags tikv ./...` 和 `go build -tags badger ./...` 均通过，
只使用 Go 编译缓存，不生成仓库 bin 产物。额外 `staticcheck -tags badger ./...` 失败于
既有 `cmd/option/initial_cluster_test.go` 三处直接引用 TiKV 专用 `pdAddrs` 字段；该文件
在 HEAD 与本轮工作树相同，属于此前未覆盖的标签测试编译缺口。普通根模块 Staticcheck
PASS 不扩大为 Badger 标签测试 PASS。日志为 `security-staticcheck-build-tikv.log`、
`security-staticcheck-build-badger.log`、`security-staticcheck-badger.log`。后续须保留
成员身份校验断言并使 fixture 按存储配置初始化，不能直接给通用测试加标签来跳过 Badger。

后续只读复查：namespace、SC、六个 PD/TiKV Pod UID 均与锚点一致，六 Pod Ready/零重启；
KubeBrain StatefulSet 仍不存在。使用原脚本及固定 PD Pod UID 的 exec 适配器重新通过
3 PD/3 Up store、连续三次无异常 Region 和六卷身份/容量/水位检查，日志为
`security-staticcheck-predeploy-storage-gate.log`。未变更集群资源或网络策略。

Staticcheck 清理的提交前 verifier/四分片 703 项全部通过：258.613/470.765/308.194/745.028 秒，
日志为前述 `security-staticcheck-pre-*`。随后提交本批变更，并立即执行相同提交后门禁；
提交后结果待回读。Badger 标签测试编译缺口仍单列保留，不影响本批默认 TiKV 配置的结果边界。

### 修复镜像发布成功（339381af）

image run `34275611099` 已终态 success。Verify published test image 于
`2026-09-08T21:44:22Z` 成功，Promote 于 `21:44:29Z` 成功，之后收尾也成功。
源码为 `339381afb74eb225d7bab8e67196ccea07596509`，不是本地 TLS 测试/Staticcheck 后续提交。
registry 按完整源码 tag 回读的不可变索引为：
`ghcr.io/fivetime/kubebrain@sha256:a245c95fea36c387358d86e3808a9d29073a327028d5a4e3a80e4d272663e865`。
linux/amd64 子镜像 `sha256:b14371c47b78fc7d3eaf632296ebd6158374becf38d3fee6cb96b6f27d5222b1`，
linux/arm64 子镜像 `sha256:d7429739f9e35c99cafef595286c9ba4a74a5a04bd29672597b2fa417ff94686`。
CI 实际两架构各 66 个 Go 程序的 GOARCH/ELF、运行时平台/SHA、kubectl 字节对比等发布检查
均通过；arm64 运行使用 Runner QEMU，不声明原生 arm64 生产验收。

重新渲染 `kubebrain-tls-339381af.yaml`，SHA-256
`01a841e2e5536f317ff84ba95aa895755baa98b76c26b9efb6ff9c4bbb0a79ee`，固定使用上述索引。
server-side dry-run 通过，并断言恰好五个 namespaced 对象、三副本、非 root/只读根文件系统、
两类 4 GiB 工作卷都使用 rook-ceph 消费者 SC。证据 `security-339381af-dry-run.json`。
首次汇总误按 List 解析 kubectl 输出的五个连续 JSON 对象，jq 失败；改用 slurp 并对五项
完整断言后通过，没有忽略校验错误。本记录时尚未创建 StatefulSet，下一步才执行部署。
