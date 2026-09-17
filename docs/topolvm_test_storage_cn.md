# 专用测试集群 TopoLVM 存储准备

2026-09-17：隔离实例升级至 `8ac67ca3` 通过原 6000 次可用性门限，随后恢复原镜像并清理本轮临时卷；见[升级验收及范围限制](acceptance_local_upgrade_8ac67ca3_20260917_cn.md)。

最新验收：独立本地盘实例默认 2PC 的 6000 次同镜像滚动重启测试已通过原门限，见[完整结果、诊断限制与清理记录](acceptance_local_2pc_bb89c3f8_20260916_cn.md)。这不是镜像升级、数据迁移或整体生产就绪结论。

随后同实例的 6000 次 leader Pod 硬故障测试也已通过原门限，见[硬故障验收报告](acceptance_local_2pc_hard_failover_bb89c3f8_20260916_cn.md)；该结果不等于节点断电或 TiKV 存储故障验收。

## 当前状态（2026-09-16）

用户要求将延迟敏感的测试后端改用 TopoLVM 本地盘，并已新增三块数据盘，随后确认“是独立的磁盘”。已核验设备身份、完成三台节点的专用 VG 初始化和 TopoLVM 安装，并另建使用本地盘的 `kb-local` PD/TiKV 后端及隔离的 `kubebrain-local` 测试实例。2026-09-16 23:55 UTC 只读复查：TopoLVM 八个 Pod 全部 Ready，新旧六个 StatefulSet 均为 3/3 Ready。**旧实例没有连接切换或数据迁移，已有 VG 不可重复初始化。**来宾系统只能看到 QEMU 设备，不能独立证明物理 SSD/NVMe 型号或底层存储来源，不把用户的独立盘确认写成物理介质检测结果。

下文按阶段保留安装、smoke、后端及前端部署时的状态；其中“尚未切换”等阶段描述不代表当前尚未创建隔离实例。验收结果以本页顶部链接的报告为准，Ready 状态不代替运行时验收。

此前默认 2PC 验收已结束并完成恢复、清理；结果见 [验收报告](acceptance_2pc_bb89c3f8_20260916_cn.md)。旧消费者 rook-ceph 的六个 Retain 数据卷保留，不允许把新增本地盘授权解释为清空旧卷、系统盘或 secondary 集群的许可。

## 新增盘身份

| 节点 | IP | 用户给出的 QEMU 挂载点 | 实际来宾盘符 | 容量 |
| --- | --- | --- | --- | --- |
| k8s3-worker1 | 10.32.32.70 | scsi0-0-0-7 | `/dev/sdk` | 1 TiB |
| k8s3-worker2 | 10.32.32.71 | scsi0-0-0-7 | `/dev/sdk` | 1 TiB |
| k8s3-worker3 | 10.32.32.72 | scsi0-0-0-1 | `/dev/sda` | 1 TiB |

用户表格中的 `sdl/sdl/sdb` 与来宾实际盘符不同，但 QEMU 序列号和路径对应。后续必须在各自节点内按以下 by-id 重新核验，不能盲用盘符：

- worker1/2：`/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_drive-scsi0-0-0-7`。
- worker3：`/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_drive-scsi0-0-0-1`。

三块新增盘的 `lsblk` 未显示分区、文件系统或挂载，`wipefs --no-act` 未发现签名，sysfs holders 为空；三个节点均已安装 LVM，`pvs/vgs --readonly` 结果为空。以上只证明检查时状态，初始化前必须复核，不能永久视为可覆盖授权。

worker1 的其他数据盘存在 `ceph_bluestore`、`crypto_LUKS` 签名；worker2 的其他盘即使未显示文件系统，也不在此次授权目标内。`vda` 为系统盘，所有 RBD 映射均排除。

## 部署与切换要求

- 固定并审核 TopoLVM chart、应用版本和镜像摘要，记录安装 values。当前已部署 chart `17.2.0`、应用 `0.41.1`。
- 集群为 Kubernetes `1.36.0`。安装前核对固定版本的支持范围、证书依赖和安全状态，不照抄旧文档的 cert-manager 版本。
- 只在上述三个节点启用本地卷服务，专用 VG；不扫描、认领或初始化其他磁盘。
- 新 StorageClass 非默认、使用 `WaitForFirstConsumer` 与 `Retain`；保持现有默认 StorageClass 不变。
- 优先使用 CSI 容量跟踪能力，避免无必要修改集群默认调度器；审核实际渲染资源和 webhook 作用范围。
- 先完成三个节点的 PVC 调度、持久化、重建读取与清理验证，再执行有恢复路径的 PD/TiKV 数据迁移或另建测试后端。不能仅修改已有 PVC 的 StorageClass 字段当作迁移。
- 本地存储不提供跨节点共享盘能力；PD/TiKV 副本跨节点分布、节点失效及数据恢复仍须单独验收。
- 保留旧 Ceph 数据及恢复路径，未经明确的数据处置决定，不销毁旧后端和旧卷。
- 使用相同代码、负载及原验收门限记录新结果，不用改变存储来掩盖超时或放宽门限。

官方参考：[Getting Started](https://github.com/topolvm/topolvm/blob/main/docs/getting-started.md)、[项目支持范围与能力](https://github.com/topolvm/topolvm/blob/main/README.md)、[Chart 配置](https://github.com/topolvm/topolvm/blob/main/charts/topolvm/values.yaml)。实际部署应固定版本，不能把可变的 main 文档当作固定发布的兼容性证明。

仓库不得保存 SSH 密码、私钥或 kubeconfig 内容。操作证据中也不得记录认证材料。

## 安装配置与实际部署

[测试集群 values](../deploy/test-cluster/topolvm-values.yaml) 固定 chart `17.2.0` 对应的应用镜像摘要，使用 VG `kubebrain-local`、设备类 `local-test`，保留 20 GiB 容量余量。CSI node、lvmd 和 controller 同时要求专用节点标签 `kubebrain.io/topolvm=enabled` 与上述三个 hostname 的硬亲和性；标签和 VG 已创建。

初始化只使用上述 by-id：逐节点确认 hostname、序列号、精确 1 TiB 容量、无分区/文件系统/挂载/签名/holders、无现有 PV/VG 后执行 `pvcreate --yes` 和 `vgcreate --yes kubebrain-local`，未使用强制覆盖、wipe 或其他设备扫描认领操作。证据目录中的 `worker1-vg.log`、`worker2-vg.log`、`worker3-vg.log` 保存了设备身份及 PV/VG UUID；初始化脚本不可在已有 VG 上重复执行。

[专用命名空间](../deploy/test-cluster/topolvm-namespace.yaml) 仅为 CSI 所需的宿主设备访问启用 privileged Pod Security。2026-09-16 21:27 UTC 安装 release `topolvm` 至 `topolvm-system`，Helm `--wait` 返回成功、revision 1/deployed。两个 controller、三个 lvmd 和三个 CSI node Pod 全部 Ready，分别仅落在上述三个节点。StorageClass `kubebrain-local-lvm` UID 为 `15cb06d4-26ed-4c85-9980-b4cb70109189`，非默认、ext4、Retain、WaitForFirstConsumer，允许扩容。

已完成 Helm lint、针对 Kubernetes 1.36 的本地 template 和 kubectl client dry-run，并检查渲染产物：三个工作负载均受节点范围限制、仅创建一个非默认 Retain/WaitForFirstConsumer StorageClass、CSI 容量跟踪启用、容器镜像摘要固定。

固定版本 Chart 在 Pod webhook 关闭时不渲染 Certificate/Issuer，不挂载 webhook TLS Secret，controller 使用 `--enable-webhooks=false`。因此本配置不需要部署 cert-manager，也不修改集群 scheduler 或增加 admission webhook。不要仅因 Chart 列有可选 cert-manager 子依赖就安装它。

此次使用普通厚置备 LV，`snapshot.enabled=false`，不宣称支持 CSI 快照。Chart 即使关闭 snapshotter，仍渲染相关 RBAC；权限审核包含这些资源。应用级备份、恢复与持久化验收仍需独立完成。

本机配置、初始化、安装及后续 smoke 证据统一保存在 `/root/.local/state/kubebrain/topolvm-preparation.7q92U9MR/`。Helm 就绪仅证明安装成功，**不代替磁盘延迟、PD/TiKV 原门限或故障恢复验收**。

配置约束已由 `deploy/test-cluster/topolvm_values_test.go` 的四项测试锁定，运行 `go test -race -count=1 ./deploy/test-cluster` 通过。测试解析实际 values，检查显式关闭可选全局组件、三个工作负载的节点硬约束、专用 VG/保留策略以及固定镜像；它不代替对固定 Chart 渲染产物及真实集群的验证。常规 CI 的 `go test ./...` 包枚举可覆盖该测试包；未额外触发 CI，也未将本地结果描述为新的 CI 成功记录。

## 三节点持久化 smoke 结果

安装后，在独立临时命名空间 `kb-topolvm-smoke-7q92u9mr` 为三个 worker 各创建一个 1 GiB PVC；Pod 使用 hostname nodeSelector（不是绕过调度器的 nodeName），验证 WaitForFirstConsumer 调度及本地 PV 节点亲和性。三个 PVC 均 Bound，容量跟踪分别发布三个节点的可用容量。

每个写入 Pod 使用已固定摘要的原 KubeBrain 镜像，生成 8 MiB 随机数据，执行 `dd conv=fsync`、保存 SHA-256 校验和并 `sync`。删除写入 Pod 后创建新的读取 Pod，重新挂载同一 PVC，三次 `sha256sum -c` 全部通过，smoke 执行终态为 0。它验证正常卸载/重挂载的持久化，不是断电测试，也不是可用于性能比较的基准。

清理前逐卷核对已保存 PV/PVC UID、claimRef、StorageClass 和 CSI driver，仅将此次三个临时 PV 的回收策略改成 Delete（带 UID/原策略 JSON Patch 前置检查），随后删除对应 PVC，并等待 PV 删除，最后删除临时命名空间。清理脚本终态为 0；测试随机数据已删除、不可恢复。**StorageClass 的 Retain 策略和所有旧 Ceph 卷未更改。**日志为 `smoke.log`、`cleanup-smoke.log`，各次 Pod/PV JSON 和读写日志保存在同一证据目录。

安装及 smoke 后，现有 PD、TiKV、KubeBrain StatefulSet 均为 3/3 Ready；KubeBrain generation/observedGeneration 仍为 102，仍使用恢复后的原固定镜像。下一阶段需为本地盘 PD/TiKV 后端制定保留旧后端的部署/切换/回退方案，再按原门限测试；当前不宣称整体任务或性能验收完成。

## 独立本地盘后端已部署（2026-09-16 21:39 UTC）

[独立后端清单](../deploy/test-cluster/tidb-cluster-local.yaml) 在同一专用测试命名空间创建 `kb-local`，而不是修改原 `kb` 的 PVC。新的 TidbCluster UID 为 `512e36f6-306d-407d-ad79-aab446ad34e2`、PD cluster ID 为 `7686251028133611667`，与旧后端独立。**这是空白新后端，不是已完成的数据迁移；KubeBrain 的 PD 地址尚未切换。**

PD/TiKV 均为 v8.5.3、各三副本、每节点各一副本，保留原资源配置：PD 每副本 requests 1 CPU/2 GiB、limits 2 CPU/4 GiB、20 GiB 卷；TiKV requests 4 CPU/8 GiB、limits 8 CPU/16 GiB、100 GiB 卷。仅使用 `kubebrain-local-lvm`，六卷均 Retain；硬节点亲和性同时限制三个 hostname 和 TopoLVM 标签，硬 Pod 反亲和性按新实例区分。

部署前各节点已请求 CPU 约 5.7–5.9/15 核、内存约 11.2–11.5/27.8 GiB，可容纳新增 requests。Metrics API 不可用，因此使用 kubelet stats/summary 核对，内存 available 约 23.6–24.6 GiB、memory PSI avg10/60/300 均为零。并行保留旧后端会造成 limits 超配和资源竞争，**不可在两套后端同时施压后宣称存储介质的严格因果比较**；正式测试前须重新检查资源压力和背景负载。

仓库配置测试 `go test -race -count=1 ./deploy/test-cluster` 通过（1.045s），server dry-run 通过，随后使用 create 新建资源。`wait-tidbcluster-ready.sh` 终态 0：两个 StatefulSet 全部 Ready/current，三台 TiKV Debug gRPC 均响应。`validate-tikv-region-health.sh` 终态 0：三 PD 成员、三 Up stores、连续三次 abnormal regions=0，PV/PVC 和文件系统容量检查通过。PD 只读 API 经固定 Pod UID 的 exec/curl 通道访问，不扩大 NetworkPolicy。

新六个实际容器镜像摘要与旧后端一致：PD `sha256:b32c69d8b9cc08cead83649d54c58942c441492b459c4cf190cd8c4747bf85f3`，TiKV `sha256:00502c3c74915577ff3a669a223a3740c3f1f7e151de745620a95a5f1450a909`。清单沿用原 v8.5.3 标签，因此未来重建仍必须核对实际摘要，不能把标签相同当作二进制相同。

启动期间 `kb-local-pd-0` 有一次退出码 1 重启，日志显示另一个成员尚未完成加入，随后恢复。不得把该次启动写成零重启；之后是否持续稳定仍需观察。部署前后旧六个 PD/TiKV Pod UID、container ID、restart count 与容器状态完全一致。

证据仍在前述 owner 目录：`local-backend-dry-run.json`、`local-backend-ready.log`、`local-backend-health.log`、`local-backend-pods.json`、`old-backend-before.json`、`old-backend-after.json`。这些只证明后端供应、启动和 Region 健康，**不证明 etcd 事务兼容性、原 900 秒验收或数据迁移成功**。后续优先为新后端准备隔离的 KubeBrain 测试实例和相同负载，避免把现有实例直接指向空库；任何正式连接切换前保留明确回退路径。

## 隔离 KubeBrain 实例已部署（2026-09-16 21:45 UTC）

[独立前端清单](../deploy/test-cluster/kubebrain-local.json) 从当时实际 `kubebrain` 配置提取部署参数并剥离运行时元数据，另建 `kubebrain-local` StatefulSet、ServiceAccount、PDB、NetworkPolicy 和两个 Service。实例标签、成员名称、初始成员地址、服务选择器、keyspace/cluster-name 均独立；PD 地址为 `kb-local-pd.kubebrain-dbaas-test.svc:2379`。Snapshot/RangeStream 临时卷也使用 TopoLVM，未挂载旧 Ceph 卷。

镜像固定为已审核 bb89c3f8 的 index digest `sha256:50b9938fe3e5ad379136957438abe3c2d4bf250dc6c5973c869e0ef1341d1537`，默认 2PC，不启用 1PC/async commit。该清单不是新产品镜像的 CI 证据。StatefulSet UID `7d760f53-5bb5-4429-a2f8-651b89665616`，generation 1、三个 Ready/current 副本，revision `kubebrain-local-695cd759c6`，三个容器 restartCount 均为 0。测试配置 race 检查及 server dry-run 通过。

使用单独生成的测试 CA，以及新的 client/peer/info/probe 四个 TLS Secret；未读取或复用旧实例私钥。CA 有效期 30 天、叶证书 7 天（2026-09-16 签发，后续长时间运行必须检查到期并轮换）。证书/私钥仅在 owner 目录下的权限受限 `local-tls/` 和集群 Secret 中，不入仓库。client 叶证书包含公共服务及成员 DNS SAN，具有 serverAuth/clientAuth；peer 叶证书包含 peer 服务/成员 DNS；probe 客户端 CN 为 root。Info CA ConfigMap `kubebrain-local-info-ca` immutable，info 校验名为 `kubebrain-local-info.kubebrain-dbaas-test.svc`。初次生成的 client 证书在正式 smoke 前补全了 clientAuth 和直连 DNS SAN；更新仅作用于新实例 Secret，并已核对三个 Pod 的投影证书摘要一致。

冷启动 leader 曾连续报告 durable revision watermark 尚未在安全读时间可见，约 31.72 秒后 checkpoint 阶段正常完成，未绕过 readiness 或修改后端数据。此现象是本次观察到的初始化等待，不应误写成已确认的永久死锁或已经修复的产品 bug。

端到端 mTLS smoke 最终通过：公共服务 Put/Get/Delete 和三个成员直连 `etcdctl endpoint health` 均成功，测试键已删除。前两次 smoke 分别因 etcdctl 不支持 `--tls-server-name`、命令行 endpoints 与同名环境变量冲突退出，属于探针命令错误，已保留失败日志；第三次修正命令后成功，未关闭 TLS 校验。三次临时 Pod 均已删除，日志与 Pod JSON 保留在 owner 目录。该短 smoke 不替代正式 6000 次 rollout/Watch/Lease/Snapshot 验收。

旧 `kubebrain` 仍为 generation 102、3/3 Ready，固定原镜像 `sha256:0ce85e66320b27bf4cdb8f11981a76cba58926fa0e835fc47dc663f709b588ce`，没有连接切换或数据迁移。下一步为新实例准备独立的验收 owner、身份与清理约束，再使用原负载和原门限运行；不得复用旧 Ceph 实验中绑定旧 UID 的单次执行脚本。临时 workspace PVC 使用 Retain，因此以后 rollout 还需跟踪旧临时卷的精确身份和回收，不得批量删除所有 Released PV。

### 首次验收预检发现的指标采集问题

2026-09-16 21:54 UTC，新 owner `/root/.local/state/kubebrain/local-2pc.wcwe26Jr/` 的首次驱动在 TiKV 指标预检退出（终态 1），未创建正式探针、未领取执行 claim、未滚动重启。`kb-local-tikv-1` 有 Raft/gRPC 指标，但未暴露 `tikv_scheduler_command_duration_seconds`。不能仅因新实例缺少这项按需出现的指标就丢弃其余诊断证据，也不能将缺失解释为零延迟或证明零请求。

采集器增加逐 Pod 的 `*-metric-availability.json`：该直方图写为 present/absent，仍要求 Raft/gRPC 类型正确、身份前后不变、响应非空且大小受限；错误类型及缺少类型声明的 scheduler 样本仍拒绝。后续计算 scheduler 延迟必须要求两个端点均存在有效指标和相同运行时身份，不能用 absent 补零。168 个身份/就绪/缺失/错误类型等子用例通过（两个采集入口，四种就绪设置，21 种模式，67.140s）。此修改只影响诊断证据采集，不改变产品镜像、协议或任何验收延迟门限。
