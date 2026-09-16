# 专用测试集群 TopoLVM 存储准备

## 当前状态（2026-09-16）

用户要求将延迟敏感的测试后端改用 TopoLVM 本地盘，并已新增三块数据盘，随后确认“是独立的磁盘”。已再次核验设备身份及空盘状态，完成三台节点的专用 VG 初始化和 TopoLVM 安装；**尚未迁移或切换 PD/TiKV**。来宾系统只能看到 QEMU 设备，不能独立证明物理 SSD/NVMe 型号或底层存储来源，不把用户的独立盘确认写成物理介质检测结果。

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
