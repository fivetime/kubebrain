# 本地盘默认 2PC 镜像升级验收（2026-09-17）

## 结论与边界

**本轮升级通过原可用性门限，驱动最终退出 0，随后恢复原固定镜像。**
6000/6000 操作成功，公共及三个成员的 Watch 完整，Lease、RangeStream、Snapshot
通过。探针 00:32:26 UTC 完成；驱动完成恢复及清理后才确认终态。

升级发生在专用本地盘隔离实例，不是旧 Ceph 实例的数据迁移。回退发生在负载完成后，
本轮证明回退后的配置、镜像及副本恢复，**不证明回退期间持续负载的可用性门限**。
这也不是整体 DBaaS 生产就绪、节点断电、存储故障或多日稳定性结论。

## 固定对象和发布前验证

- 命名空间 `kubebrain-dbaas-test`，UID `6c57c242-912b-41bb-9020-f4fdb3225ef3`。
- 前端 `kubebrain-local`，UID `7d760f53-5bb5-4429-a2f8-651b89665616`；始终默认 2PC，未启用 1PC/async commit。
- 后端 `kb-local`，UID `512e36f6-306d-407d-ad79-aab446ad34e2`；PD cluster ID `7686251028133611667`；PD/TiKV v8.5.3，各三副本，使用 TopoLVM `kubebrain-local-lvm`。
- 候选源码 `8ac67ca3b21423e3d87c3c882850ce655eb23dd9`；镜像 CI `35163623338`、探针回归 CI `35163623307` 均成功，独立镜像审计成功；见[发布前验证](pre_ci_validation_20260916_cn.md)。
- 候选 index `sha256:99535eb62b41d78de573c1d7162a20946424d33a4aa30e8324a3ef2adaf644a7`。
- 原镜像及最终恢复镜像 index `sha256:50b9938fe3e5ad379136957438abe3c2d4bf250dc6c5973c869e0ef1341d1537`（bb89c3f8）。
- Probe `kb-local-upgrade-8ac67-nzccr9qr`，UID `e8ffeb71-2d23-4ac7-89b8-0806c993acc2`。

候选 amd64 为 `sha256:8de8457ef517345f1b00ccc0a5432f9554c599cf8cecd231ef10aa17890be2da`，
arm64 为 `sha256:d517214f90cfcb81715c981ea2e490d5afebb80dd2760a0f43bea8fa649b5f23`。
三节点隔离预拉取、正式变更前的新鲜运行时身份验证均通过。

原门限保持不变：6000 次、间隔 100ms，rollout 后完成等待 900 秒；公共操作 5 秒、
直连流 30 秒、单命令 10 秒、TSO/Region 各 1 秒。总 elapsed 835529ms 包含升级阶段，
不能把它写成单独的 post-rollout 耗时。源码及运行中脚本未修改，未重启探针。

## 最终结果

| 项目 | 结果 |
| --- | --- |
| 操作 / Watch | 成功 6000、失败 0；公共 6000，直连 6000 × 3 |
| Watch 完整性 | missing / ambiguous / invalid 均为 0 |
| 公共 / Put / Put 后 Watch 最大耗时 | 2104 / 2092 / 76ms |
| 直连最大耗时 | 9981ms，低于原 30000ms |
| TSO / Region 最大耗时 | 14 / 15ms，各低于 1000ms |
| 公共 Lease | alive，426 次响应，0 次重建 |
| 直连 Lease | alive，1242 次响应，5 次重建，最大恢复 3472ms |
| RangeStream / Snapshot | 672 / 1，流重试 3、部分响应重试 0 |
| SDK Put | 6000 次，最终错误 0 |

不能把最终无错误解释为无连接重建或无重试。诊断采集的 86 个后端 callback 和
6 个应用 sampler 均退出 0，独立后端 observer 最终退出 0；未把这些计数当作性能 SLO。

## 恢复、清理和证据

generation 从 2 升至 3，再恢复为 4；候选 revision `kubebrain-local-879884bd7`，
恢复至原 revision `kubebrain-local-568bd68448`、3/3 Ready。驱动核对原完整 spec、
三个 Pod 的原镜像 runtime identity；外层再次核对原 spec 与 Ready/updated 状态。
旧 Ceph 前端 UID、generation、完整 spec 保持不变且 Ready3；本地 PD/TiKV 六个
Pod 的 UID 和 containerStatuses 在前后完全一致。

测试 keys/users/roles/leases 为 0，探针及其清理 Pod、owner ConfigMap 已消失；
预拉取资源有 `PREPULL_CLEANUP_CONFIRMED`。此前 apiserver 遗留空 lease 已在升级前
自然到期确认，未靠本轮清理撤销，见[空 lease 对标](empty_lease_semantics_cn.md)。

仅清理本轮原实例六个和候选阶段六个 scratch PV：各自有此前 Bound 身份记录，
逐一核对 UID/full spec、Released、Retain、TopoLVM driver、同名新 PVC 已绑定
不同卷且无 VolumeAttachment，之后用 UID/RV/full spec/phase 前置条件改变回收策略。
十二卷均已删除，临时数据不可恢复；清理退出 0。最终十二个本地 PV 全部 Bound，
LogicalVolume 数为十二；未修改 StorageClass 的 Retain 策略或旧 Ceph 数据卷。

证据根目录 `/root/.local/state/kubebrain/local-upgrade-8ac67ca3.NZccr9qR/`：
`execute.qqHh7w5G/`、`execute.exit`、`runtime/`、`backend-observer-evidence/`、
`candidate-volumes.Xbksiiwl/`、`retire-scratch.log` 和精确清理收据。
驱动及 observer 终止后才从原 `/tmp` 目录归档。此实验单次执行标记已消费，不可重跑。
