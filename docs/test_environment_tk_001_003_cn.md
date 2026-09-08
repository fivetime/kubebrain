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

### 三副本首次部署与冷启动延迟

已创建上述五个对象，StatefulSet UID `2650ad15-1d37-41c4-836c-d40dd4502720`；
ServiceAccount UID `6389933b-c121-446b-81a1-b2399ce9f0a7`、PDB UID
`9680eb28-e0d2-47ab-91e1-172b01d24215`，client/peer Service UID 分别为
`a0fbf77b-010b-4595-a28b-471b73d2d640` / `6a2695bb-e303-4788-999b-9ae4e35f30f4`。
创建响应保存在 `security-339381af-created.json`。六个 4 GiB 工作 PVC 已 Bound，均为指定
rook-ceph 消费者 SC。Pod 0/1/2 分别落在 worker3/1/2，UID 为
`f4f7c830-5a2a-485d-bd43-b53f18c53bcb`、`af235510-0e4b-4e55-bbb5-ff51df089311`、
`fcec128c-edce-4689-9576-8fd85c820864`。三个 Pod imageID 均报告上述固定索引；Pod 内实际
version 核对得到 TiKV、linux/amd64、Go 1.26.8、源码 `339381af`、构建时间 `20:35:19Z`。

首次 10 分钟 rollout 等待超时，不能写成一次正常启动。leader kubebrain-2 在 checkpoint
阶段反复报 `decode durable revision watermark: invalid revision watermark length 0`；
followers 正确拒绝向 NOT_SERVING leader 转发。只读诊断确认当前持久 watermark 是 1，
但 Store safe timestamp 为 `468946512982310913`（17:36:20.798 UTC），远早于本次启动。
该历史快照的 GetAt 返回 not found、BatchGetAt 不含该键；代码直接对缺失 map 值解码，
把历史快照尚未包含 watermark 表现成损坏值。不能通过当作当前 revision 或跳过安全时间戳
来放行。后续需要区分缺失/损坏，并调查冷启动安全时间戳推进延迟。

诊断程序只读指定 keyspace 的 watermark、TSO 和 range safe timestamp，没有执行数据写入、
GC、锁清理或集群配置变更；临时上传程序已从 Pod snapshot 工作卷移除。
日志 `security-339381af-checkpoint-readonly.log`。其中 compact 范围首次诊断使用了未附加
keyspace 后缀的协调前缀，不能拿它证明实际 compact Region；object/durable 范围和当前
watermark 读取直接使用正确的 `kubebrain-dbaas-test` coder，不受该范围错误影响。

三个副本随后自然变为 Ready、零重启；再次 rollout 观察通过，日志
`security-339381af-rollout-after-delay.log`，首次超时保留在 `security-339381af-rollout.log`。
启动错误/恢复记录为 `security-339381af-startup-watermark.log`、`security-339381af-startup-recovery.log`。
TiKV 的 resolved-ts.enable=true、advance-ts-interval=20s；没有更改参数来掩盖延迟。

下一步专属 probe 清单 `kubebrain-security-smoke-339381af.yaml` 固定同一镜像与 StatefulSet UID。
Pod 名 `security-smoke-339381af`，fixture 前缀
`/kubebrain-rollout-availability/security-smoke-339381af/`，预留 lease IDs 为
`2026090822060001/2026090822060002/2026090822060003`；旧 5645caa6 probe 未执行，旧 ID 不复用。
先三副本逐一审计，再跑 300 轮可用性/Watch/Lease/RangeStream/Snapshot 与 PD/TiKV 探针。
此处为执行前身份登记，尚未取得 probe 结果。

Staticcheck 提交 `e156a9f7e79b2d5aa20214bc80d5ee2e73e68b28` 的提交后 verifier 与四分片
703 项全部通过（255.788/455.628/308.469/731.352 秒），日志 `security-staticcheck-post-*`。
前后两轮均完成，随后快进推送；新提交的镜像构建不替代当前已部署 `339381af` 的验收身份。

快进推送已成功，自动触发 image run
`34284304697`（https://github.com/fivetime/kubebrain/actions/runs/34284304697），源码精确为
`e156a9f7e79b2d5aa20214bc80d5ee2e73e68b28`，当前 build/push in_progress，不重复派发或推送
后续代码取消它。当前服务继续固定使用已经验证的 `339381af` 索引。

综合探针已创建，UID `c4bad481-abbc-48ee-b8fa-d90181c1e660`，创建响应
`security-339381af-smoke-created.json`，持续日志 `security-339381af-smoke.log`。三个直连
审计均通过：依次 Put/Delete revision 为 2/3、4/5、6/7，同一 PD/数据面 cluster ID，
三个 60 秒 audit lease 的回收验证通过。随后综合探针打印 PROBE_STARTED；当前 Pod Running，
尚无 PROBE_SUMMARY/终态，不能把审计通过当作 300 轮、Snapshot 或恢复已通过。

六个工作 PVC/PV 的精确 claimRef UID 一一匹配、六个 volumeHandle 唯一，CSI driver 与
clusterID 全为 rook-ceph 消费者；清单为 `security-339381af-work-pvcs.json` 与
`security-339381af-work-pvs.json`。三个副本的 snapshot/spill 挂载实际文件系统均
4,046,560 KiB、当时使用 1%，不是 node-root 容量。只读诊断源保存在仓库外
`checkpoint-readonly-diagnostic.go`；本机 `/tmp/kubebrain-checkpoint-diagnostic.2LlyPP`
及其编译产物、Pod 内上传文件已清理，源码与日志仍可恢复诊断过程。

### checkpoint 缺失 watermark 错误分类修复草稿

根据真实启动观察新增 `TestSerializableCheckpointWaitsForDurableWatermarkVisibility`。
旧代码在“安全快照缺失 watermark”用例中实际返回 InvalidMVCCMetadata，负向测试按预期
失败；空值但存在的键仍要求损坏错误。修复读取 map 的存在位，缺失时返回带 timestamp
的 ErrSerializableCheckpointUnavailable，继续 fail-closed；不发布 checkpoint，不用
当前 watermark 或假定 revision=1 替代历史快照，也不改变 TiKV safe timestamp。
测试还验证 marker 真正进入后续安全快照后恢复创建 checkpoint。

`security-checkpoint-missing-before.log` 记录复现；修复后 checkpoint 系列普通测试
0.275 秒通过，新用例 race 连续 20 次 2.109 秒通过，backend vet/Staticcheck 通过。
日志 `security-checkpoint-missing-after.log`、`security-checkpoint-missing-race.log`。
该草稿只解决缺失与损坏混淆，不宣称修复了 10m26s 的真实冷启动 safe-ts 延迟。
新批次 verifier 已通过 703 项、170/193/180/160，提交前四分片运行中，日志前缀
`security-checkpoint-missing-pre-`；草稿尚未提交或包含在运行镜像内。Badger 标签 fixture
兼容性缺口仍待后续处理。

### 综合探针失败终态与恢复 TLS 诊断

`security-smoke-339381af`（UID `c4bad481-abbc-48ee-b8fa-d90181c1e660`）于
22:18:32 UTC 终止，Pod Failed、exit 1。最终错误为
`stream integrity probe did not complete one RangeStream and Snapshot within 8m2s: range=461 snapshot=0: context deadline exceeded`。
没有 PROBE_SUMMARY；三个直连审计通过不等于综合验收通过。曾观察到 33,681,440 字节
Snapshot 文件与恢复目录，只能证明进入恢复阶段，不能证明恢复验证完成。日志及终态分别为
`security-339381af-smoke.log`、`security-339381af-smoke-latest.json`。

另建精确绑定原 probe UID、StatefulSet UID、fixture 前缀及三个 lease ID 的清理审计 Pod
`security-smoke-339381af-cleanup`（UID `dc45ba83-0bfc-4fd5-b496-5f9d8d94d9a7`）。
22:21:49 UTC Succeeded、exit 0，输出 `FIXTURE_CLEANUP_OK status=absent`，
keys/users/roles/leases 全为零；审计在任何删除操作前确认不存在残留，没有额外删除数据。
证据 `security-339381af-cleanup.log` 与 `security-339381af-cleanup-result.json`。
两个终态 Pod 暂留，未重跑失败测试、未重启后端或放宽门禁。

代码核查发现恢复探针把源连接的 client certificate/key 同时用作本地嵌入式 etcd 的
server certificate/key。实际挂载的 test-client.crt 是 CN=root、仅 clientAuth、无 SAN；
现有 TLS 恢复测试使用含 serverAuth/clientAuth 与 SAN 的 SelfCert，未覆盖该合法客户端身份。
下一步以 client-only 证书复现并分离本地恢复 PKI；不向 probe 挂载生产服务端或 CA 私钥，
不关闭 TLS 验证。尚不能把此代码问题已定位写成修复后综合验收通过。

checkpoint 修复提交前 verifier 与四分片已全部通过，703 项，耗时
253.171/451.373/305.544/728.379 秒，证据 `security-checkpoint-missing-pre-*`。
22:29:59 UTC 查询 image run `34284304697` 遇 GitHub API rate-limit 403；这是观测失败，
不是构建终态，不重新派发、取消或以新代码推送替换该运行。

### checkpoint 已提交，恢复探针 TLS 修复本机验证

checkpoint 错误分类修复提交 `67b31c2f` 已完成提交后 verifier 与四分片 703 项，耗时
272.506/477.971/328.523/753.739 秒；日志 `security-checkpoint-missing-post-*`。
尚未推送以免取消仍未取得终态的 image run `34284304697`。22:45:30 UTC 同一 run API
仍返回 rate-limit 403；匿名网页返回 404，不据此判断私有仓库的 CI 已停止。

恢复 TLS 回归首先证明旧实现复用源客户端 certificate/key，负向对照失败记录为
`security-restore-tls-before.log`。第一步分离服务端身份后，新的 CN=root 客户端用例又
揭示权限测试把已有证书管理员的连接当作匿名连接：期望拒绝的 Range 实际成功，记录为
`security-restore-tls-after.log`。两项失败均保留，不以最终修复结果覆盖原证据。

修复草稿现在使用仅限本地 loopback 的短期 serverAuth 身份；源客户端证书/私钥及 CN
保持原样，由恢复服务端继续核验源 CA。另生成 clientAuth-only、空 CN 的恢复专属身份，
供匿名与密码权限矩阵使用，防止源 CN=root 使拒绝测试失真。两份新身份均不是 CA，
只在这次恢复内部固定信任；源连接、生产 trust bundle 和源数据库权限均不修改。
所有临时身份文件 0600、目录 0700，随恢复目录在成功/失败后移除。无需生产 CA 私钥
或服务端私钥，不禁用双向 TLS，也不放宽 Snapshot/恢复超时或通过条件。

本机验证结果：

- 探针整个包普通测试通过，194.905 秒，`security-restore-tls-full.log`。
- 新 client-only 三成员恢复覆盖“恢复后启用 auth”和“已启用 auth、仅证书管理员”两种
  状态，以及生产规模数据、权限矩阵和既有复制/成员变更验证；race 通过，64.843 秒，
  `security-restore-tls-integration-race.log`。
- 实际 TCP/TLS 验证原 CN=root 与空 CN 传输身份，拒绝错误主机名、不可信/过期服务端、
  不可信或缺少客户端证书；还验证身份用途、非 CA、权限、配置缺失和无效源 CA。
  边界 race 连续 20 次通过，2.770 秒，`security-restore-tls-race.log`。
- vet 与 `go run honnef.co/go/tools/cmd/staticcheck@v0.8.1` 通过；首次直接调用
  `staticcheck` 因未安装可执行程序退出 127，不能把该次尝试算成功。后续指定版本检查
  的终态日志为 `security-restore-tls-staticcheck.log`。

隔离工作区 `/tmp/kubebrain-restore-tls.aiSeig/repo` 的四个源码文件已逐字节核对并用补丁
同步回主工作区。当前草稿尚未提交、发布或替换运行镜像；提交前 verifier 已通过 703 项，
四分片正在运行，日志 `security-restore-tls-pre-*`。真实集群仍固定 `339381af`，三副本
Ready；首轮综合失败仍是有效失败记录，修复探针的真实重测尚未执行。

上述隔离工作区的测试进程均已取得终态；四文件与主工作区逐字节一致后，已移除该临时
worktree 及空父目录。修复源码仍完整保留于主工作区，测试日志位于仓库外材料目录，
没有清理共享 Go 或镜像构建缓存。

### 修复探针真实重测准备

22:54 UTC 通过同一源码提交的 GraphQL statusCheckRollup 核验到 `build-and-push`
IN_PROGRESS，detailsUrl 精确对应 image run `34284304697` / job `102256177342`。
REST core 配额重置时间为 22:55:05 UTC，随后 REST 查询恢复，仍为同一 build/push 步骤
in_progress。没有重新派发构建或把限流视为失败。

仓库外 `kubebrain-security-smoke-restore-tls.template.yaml` 预留独立重测 Pod
`security-smoke-restore-tls`、fixture 前缀
`/kubebrain-rollout-availability/security-smoke-restore-tls/` 和 lease IDs
`2026090823000001/2026090823000002/2026090823000003`。目前仅生成模板、确认同名 Pod
不存在，未创建或执行。模板沿用已验证 `339381af` 不可变镜像，但等待上传单独编译的修复
probe，通过精确 SHA-256 后才运行；创建前必须填入已提交源码 SHA 与实际二进制摘要。
此方式是修复探针的诊断性集群重测，不是修复后发布镜像的验收，不能把基础镜像标签
误记成包含新修复。三副本服务、后端、证书 Secret、存储卷均不因此变更；不触及 secondary。

重测前再次核验 kube-system/测试 namespace UID 与原锚点一致，三个服务副本仍 Ready、
后端 3 PD + 3 TiKV 均 Ready、restart 0。固定 PD Pod UID 的只读 exec 适配 Region/存储
门禁再次通过：3 个连续 Region 样本无异常，证据 `security-restore-tls-region-gate.log`；
仍不声明原 API Service proxy 网络入口已经恢复。

本次只读对照 `/root/etcd` commit `5cd9f4ee13801e18825d661e5005ae599460bc3a`：
`server/etcdserver/v3_server.go` 的 AuthInfoFromCtx 先检查 token，再在 ClientCertAuth
启用时读取 TLS 身份；`server/auth/store.go` 的 AuthInfoFromTLS 使用已验证链叶证书 CN。
因此 CN=root 的无密码连接并非匿名连接，不能期待其在启用 auth 后被拒绝；这不是要求
KubeBrain 改变证书认证语义，而是修复恢复探针的身份隔离。参考源码没有改动。

恢复 TLS 修复提交前 verifier 与四分片 703 项已全部通过，耗时
268.762/498.608/331.407/753.403 秒，日志 `security-restore-tls-pre-*`。
下一步提交该批修复，立即执行提交后同样的 verifier/四分片，并从精确提交构建诊断探针。

### 修复探针已提交并启动诊断性集群重测

提交为 `669ac47033f1f2681c1f7a5e5cf3e10bc36c78c3`；提交后 verifier 已通过 703 项，
四分片正在运行，`security-restore-tls-post-*`。从该干净提交编译诊断探针，buildinfo
确认 Go 1.26.8、linux/amd64、CGO_ENABLED=0、vcs.modified=false、精确源码 SHA，
二进制 SHA-256 `1b1e204227e6684bb5cc7a9b963f3e25e42363f7103538f15db0133ac98354fc`。
本机暂存 `/tmp/kubebrain-restore-probe.YtiJH5/restore-probe`，待重测/清理结束后移除。
完整记录 `security-restore-tls-binary-buildinfo.log`、`security-restore-tls-binary-sha256.log`。
govulncheck v1.6.0 binary 扫描无可达及导入 package 漏洞；模块级仍报告
GO-2026-5932（x/crypto/openpgp，不在本探针调用路径），不宣称全部依赖零漏洞。
证据 `security-restore-tls-binary-scan.log` 与 `security-restore-tls-binary-scan-verbose.log`。

实际清单 `kubebrain-security-smoke-restore-tls.yaml` SHA-256
`9e85d348b6e074e3e2764bdd203d1c247b9a5ec030fe4814336f2b8876ab0746`，server-side dry-run
及非 root、只读根、无 SA token、无新增 PVC 等断言通过。创建前重新核验 cluster/StatefulSet
UID、三副本 Ready、旧 cleanup Pod 精确 UID+Succeeded+exit 0、同名新 Pod 不存在。
新 Pod UID `758d5c3f-f2c3-48c5-a848-22d72ee5b727`，worker3；上传先写 stage 文件，
核对摘要后 chmod 0500 并原子改名，入口再次验证摘要后才执行。
创建/状态/日志为 `security-restore-tls-smoke-created.json`、`security-restore-tls-smoke-latest.json`、
`security-restore-tls-smoke.log`。

23:08:21 UTC 三个直连审计已通过，Put/Delete revisions 为 356/357、358/359、360/361；
随后 PROBE_STARTED。当前综合重测尚无终态，不能把审计通过当作 Snapshot/恢复通过。
提交后本机四分片与这一独立诊断 Pod 并行；未推送取消 image run，未变更服务镜像或后端。
源服务证书认证五项回归 race 三轮通过（2.647 秒），记录
`security-restore-tls-source-auth-race.log`。

### 诊断重测因工作卷容量被驱逐，独占 fixture 已回收

新 probe 于 23:09:25 UTC Failed、exit 137，无 PROBE_SUMMARY。仅据 137 不能判断 OOM；
随后读取精确 Pod UID 的事件确认 23:09:23 Evicted：
`Usage of EmptyDir volume "work" exceeds the limit "512Mi".`，接着 Killing。
证据 `security-restore-tls-smoke-events.json`。完成后无法 exec 读取 cgroup，cadvisor
查询也未匹配该已退出容器；不编造内存峰值。worker3 当前 MemoryPressure/DiskPressure
均 False。此轮不算综合通过，不重跑相同容量配置以掩盖失败。

创建精确绑定失败 probe UID/前缀/租约的 cleanup Pod `security-smoke-restore-tls-cleanup`，
UID `da2cffd1-6b3d-44ce-8236-932f04b1c857`。23:11:34 UTC Succeeded、exit 0，输出
`FIXTURE_CLEANUP_OK status=recovered owner_uid=758d5c3f-f2c3-48c5-a848-22d72ee5b727 keys=529 users=0 roles=0 leases=3`。
已清除该失败测试的独占键与租约，没有用户/角色残留或其他租户数据删除。
日志 `security-restore-tls-cleanup.log`、终态 `security-restore-tls-cleanup-result.json`。
测试数据可由新一轮专属 fixture 重新生成，旧失败记录保持不变。

原 512 MiB 是此次环境手工探针清单的容量，不是产品恢复协议要求。真实恢复需要三个
成员、后续 voter/learner 数据目录及官方 WAL 预分配，还包含 Snapshot 与 47 MiB 上传
探针；不能仅按下载的 Snapshot 文件大小分配空间。下一轮使用新的 Pod/fixture/lease ID，
把工作 emptyDir 和容器 ephemeral-storage limit 显式配置为 2 GiB、request 为 2 GiB；
CPU/内存、原测试规模、超时和验证条件不变。不调整节点驱逐阈值，不修改 TiKV 或 etcd
WAL 实现，不把这次容量修正当作产品性能通过。三个服务副本保持原镜像和 Ready 状态。

恢复 TLS 提交 `669ac470` 的提交后 verifier/四分片 703 项全部通过，耗时
256.984/468.300/315.747/747.043 秒；前后两轮门禁均完成。

2 GiB 重测 Pod 为 `security-smoke-restore-space`，UID
`6a7035e2-07b3-4312-968e-8a8e2add2942`，前缀
`/kubebrain-rollout-availability/security-smoke-restore-space/`，lease IDs
`2026090823200001/2026090823200002/2026090823200003`。实际清单
`kubebrain-security-smoke-restore-space.yaml` SHA-256
`b2cbae6d62b9c11a5cf47b02d1b5cbd020154761fa5841ed06d02bb474b12ea9`，server-side dry-run
与新增临时存储 request/limit 精确断言通过；使用同一个 `669ac470` 修复二进制和同一个
已验证基础镜像，没有重新编译或改动验证逻辑。原失败 fixture 已回收后才创建新 Pod。
23:15:38 UTC 三个直连审计通过，Put/Delete revisions 为 641/642、643/644、645/646。
日志 `security-restore-space-smoke.log`，创建响应 `security-restore-space-smoke-created.json`。
当前重测仍在进行；启动早期工作目录 47 MiB、memory.current=91,598,848 bytes、
oom/oom_kill=0，证据 `security-restore-space-resources-initial.log`；该采样不是峰值。

### 修复后二次诊断重测通过

`security-smoke-restore-space` 于 23:17:15 UTC Succeeded、exit 0。最终输出：
`ok=300 fail=0 total=300 watch=300 direct_watch=300x3 lease=alive direct_lease=alive`
`range_stream=64 snapshot=1 stream_retries=0 stream_partial_retries=0`。
public/direct lease restart 均为零，max operation/direct latency 为 562 ms、Put 533 ms、
Watch-after-Put 93 ms、TSO 102 ms、Region 21 ms。完整日志及终态为
`security-restore-space-smoke.log` 与 `security-restore-space-smoke-latest.json`。
其中 Snapshot 成功计数在 checksum、官方 etcdutl restore、三成员启动、历史/规模数据、
权限矩阵及既有 quorum/voter/learner 验证全部完成后才增加，不只是下载成功。
后续采集工作目录容量时 Pod 已正常完成，exec 被拒绝；因此没有有效恢复峰值容量/内存
测量，不能用失败采集文件推断峰值。此前 512 MiB 驱逐记录仍保留。

这证明运行中的 `339381af` KubeBrain 三副本，在此次无故障注入场景下可通过修复后的
`669ac470` 探针综合验证；不等于包含修复的发布镜像已验收，不等于真实 HA 故障演练、
后端 TLS 轮换、长时间 soak 或完整生产就绪目标已完成。

独立 cleanup Pod `security-smoke-restore-space-cleanup`，UID
`d7088514-fc73-4614-980b-710adc21e0a3`，于 23:18:48 UTC Succeeded、exit 0，输出
`FIXTURE_CLEANUP_OK status=absent owner_uid= keys=0 users=0 roles=0 leases=0`。
确认成功探针自身已完成独占 fixture 清理，没有再执行额外数据删除；证据
`security-restore-space-cleanup.log`、`security-restore-space-cleanup-result.json`。

三轮探针及对应三个 cleanup Pod 共六个均已终止；在保存完整终态、日志、事件后，通过
Kubernetes DeleteOptions 的精确 UID + resourceVersion preconditions 删除这六个对象，
再次查询均不存在。身份清单 `security-smoke-terminal-pods-before-cleanup.json`；各次
删除请求/响应为 `delete-security-smoke-*.json` / `delete-security-smoke-*-result.json`。
同时核验摘要后删除本机 `/tmp/kubebrain-restore-probe.YtiJH5/restore-probe` 及空父目录。
本次删除的是专属已终止测试 Pod、其临时工作制品和本机编译程序；未删除 PVC/PV、
Secret、StatefulSet 或后端数据卷。源码、清单、摘要和日志均保留，程序可从提交重建；
临时 Snapshot 文件不作为备份保留。没有清理共享 Go/BuildKit 缓存。

### e156a9f7 镜像完成，准备快进发布恢复修复

image run `34284304697` 最终 completed/success，job `102256177342` 用时 1h16m17s。
发布验证 23:21:47–23:24:07 UTC 成功，promotion 23:24:07–23:24:14 成功，
整个运行于 23:24:44 UTC 更新为完成；未被后续推送取消。
源码为 `e156a9f7e79b2d5aa20214bc80d5ee2e73e68b28`，索引 digest
`sha256:b017e3421d670c7b0d9223601e9c75528eae7ddb4f579f3167d5f06c3e1a2185`，
amd64 子镜像 `sha256:3e7e26f8f4a009ef41f1cd2f3cee0e6756a33b2b470f6a384bbce98fdf0109ad`，
arm64 子镜像 `sha256:fd000dd82789b5bdf9632e52ff7baf7ac7314bd29537ea9a030047c658bfdb0b`。
registry 回读索引与 CI 成功状态均已核验；证据 `security-e156a9f7-image-manifest.json`、
`security-e156a9f7-image-ci.json`、`security-e156a9f7-image-ci.log`。
Runner 报告部分固定 action 使用 Node 20 声明并被强制以 Node 24 运行的弃用警告，
但本次步骤均成功；后续应独立升级 action，不以此警告掩盖发布失败。

该镜像尚不包含后来的 checkpoint `67b31c2f` / restore TLS `669ac470` 修复；不替换当前
已通过诊断测试的 `339381af` 三副本。当前准备将修复及其前后两轮 703 项门禁、真实重测
和清理记录一起快进推送 `dbaas`，由新 push 自动构建完整新镜像，不重复 workflow_dispatch。
