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

### 发布产物核验中

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
