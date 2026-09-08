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

本次已快进推送 `8963d2ff590bb4a0b4f779f701843c34a9c5a350` 到 `dbaas`，由 push 自动触发 [Actions run 34247183115](https://github.com/fivetime/kubebrain/actions/runs/34247183115)，未重复手动派发。self-hosted Runner `raas-1482` 已接单，完成 checkout/QEMU/Buildx/GHCR 登录，正在构建；该记录尚不表示构建或镜像验证成功。提交前后均执行 `--verify 4` 与四个生产测试分片，两轮各 703 项全部通过；workflow 的 Go 回归测试及 actionlint v1.7.12 也通过。

取得已验证的镜像后，部署三副本 KubeBrain、客户端语义测试及产品恢复验证，再补监控和故障演练。不要将当前后端与 CSI 冒烟测试标为整个产品验收完成。
