# TopoLVM 默认 2PC：leader Pod 硬故障验收

## 结论

2026-09-16 本轮**通过原门限**：驱动退出码 0，`mode=hard-failover`，6000/6000 成功，Watch 无丢失，公共/直连 Lease、RangeStream、Snapshot 均通过。独立实例仍使用默认 2PC 和已审核 bb89c3f8 镜像，未关闭持久化保护或放宽超时。

范围仅为专用测试集群 `kubebrain-dbaas-test/kubebrain-local` 当前 leader 的 UID/RV 受限、零宽限期 Pod 删除。**这不是宿主机断电、节点失联、TiKV 磁盘故障、多副本同时失败或版本升级测试，也不证明整体生产就绪。**旧 Ceph 后端和旧 KubeBrain 实例没有切换或删除。

## 配置、故障及恢复身份

配置与[本地盘重启验收](acceptance_local_2pc_bb89c3f8_20260916_cn.md)一致：PD/TiKV v8.5.3 各三副本、六个 TopoLVM Retain 数据卷；KubeBrain 镜像 index `sha256:50b9938fe3e5ad379136957438abe3c2d4bf250dc6c5973c869e0ef1341d1537`，源码 `bb89c3f849565f997e347f1f9f947711c50010ca`。本地执行工具 HEAD 为 `9f48d728`，不是新产品镜像发布记录。

- StatefulSet UID `7d760f53-5bb5-4429-a2f8-651b89665616`，generation 2；revision `kubebrain-local-568bd68448` 前后不变。
- 后端 TidbCluster UID `512e36f6-306d-407d-ad79-aab446ad34e2`，PD cluster ID `7686251028133611667`。
- Probe `kb-local-2pc-hard-syp4ssxa`，UID `90fa504b-5877-4501-ba26-06a1e52e5815`。探针开始持续负载后，执行器两次确认 leader，再按 UID/resourceVersion 删除目标。
- 被删除 leader：`kubebrain-local-1`，Pod UID `3f3e43bc-bd86-46b2-bf72-f50292dbd635`，member ID `2176893298`。
- 同名重建 Pod UID：`c3ddd7e0-fe7e-4316-af2d-dc164cd45837`，容器启动 22:18:29 UTC，restartCount 0。
- 最终 leader：`kubebrain-local-2`，member ID `1543124563`。驱动输出 `HARD_FAILOVER_RECOVERED`；最终三个副本 Ready。

原门限保持：6000 次、100ms pacing，恢复等待后完成窗口 900s，公共操作 5s、直连流 30s、命令 10s、TSO/Region 各 1s。探针最终时间 22:32:05.522 UTC，elapsed 822031ms 包含故障阶段，不能把它当作单独的 post-recovery 时长；门限通过依据为执行器完整结果。

## 最终指标

| 项目 | 结果 |
| --- | --- |
| 成功 / 失败 | 6000 / 0 |
| Watch | 公共 6000，直连 6000 × 3；missing/ambiguous/invalid 全为 0 |
| 公共最大耗时 | 1138ms（门限 5000ms） |
| Put / Put 后 Watch 最大耗时 | 1103ms / 35ms |
| 直连最大耗时 | 5975ms（门限 30000ms） |
| TSO / Region 最大耗时 | 14ms / 12ms（各 1000ms） |
| 公共 Lease | alive，419 次响应，0 次重建 |
| 直连 Lease | alive，1238 次响应，1 次重建，最大恢复 1113ms |
| RangeStream / Snapshot | 697 / 1；流重试 2，部分响应重试 0 |

故障时 SDK Put 出现一次 `Unavailable: leader changed`，提交结果确认 Get 调用一次且 RPC 无错误；累计 SDK Put 6001 次、错误 1，最终完成 6000 次操作。不能把最终零失败改写为“无 RPC 错误”或“所有请求不经重试”；结果确认 Get 没有 RPC 错误也不等于它必然观察到了原写入成功。最终 Watch 完整性为独立校验。

## 诊断证据与清理

应用采样七次：故障切换时首份因 Pod 身份/就绪条件不满足而退出 1，未纳入完整样本；随后六份成功且具有 phase-stable 标记。后端采集 69 次 callback 成功，observer 终态 0。上一轮修复的普通非升级模式镜像字段已在本轮自动采样中实际验证。相关 `TestRolloutDiagnostic(Phase|Sampler)` race 测试通过（36.608s）。不将失败首样本或未知阶段样本解释为有效稳定窗口。

后验核对：

- 六个本地 PD/TiKV Pod UID、container ID、启动时间、restart count 前后一致；六个数据 PVC/PV UID 和完整 spec 一致、仍为 Bound/Retain；连续三次 Region 健康检查通过。
- 旧 KubeBrain 的 UID、generation 102、完整 spec 不变且 3/3 Ready。
- 驱动确认测试 keys/users/roles/leases 均为 0；probe、cleanup Pod、owner ConfigMap 已消失。
- 仅删除原 leader 的两个 Released scratch PV：`pvc-62957524-038f-45db-bed4-d8567c971ea2`、`pvc-e5daf316-1461-4125-a26a-fbae45689c58`。先核对保存的 UID/full spec、claim 身份及无 VolumeAttachment，再用 UID/resourceVersion/full spec 前置检查将这两卷改为 Delete，并等待删除。对应 LogicalVolume volumeID 已消失，临时数据不可恢复；其余 12 个本地 LogicalVolume 保留。
- 驱动及采集器终止后，按预先记录的 SHA-256 验证并删除本轮 `uid-delete` 辅助二进制，可按源码重建。实验脚本单次 claim 已消费，并设置 HOLD 防止误重跑。

证据目录：`/root/.local/state/kubebrain/local-2pc-hard-failover.sYP4SSXA/`，含 `runner.log`、`runner.exit`（0）、`runtime/`、`backend-observer-evidence/`、后端及旧实例 before/after、六组 PVC/PV 基线、`backend-postflight-health.log`、`retire-scratch.log`、`scratch-retirement.ho6qhXex/`、race 日志。两个原 `/tmp` 证据目录已在采集终止后移入此处；凭据不入仓库。

仍需推进真正的版本升级/回退、存储与节点故障恢复、兼容性差距、生产规模及长时间测试。本报告不替代这些开放项。
