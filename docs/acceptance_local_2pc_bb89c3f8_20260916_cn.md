# TopoLVM 本地盘默认 2PC 重启验收（2026-09-16）

## 结论与边界

**本轮通过原门限。**驱动退出码 0，6000/6000 成功，公共及三个成员的 Watch 完整，Lease、RangeStream、Snapshot 均通过。22:11:47 UTC 探针完成，随后清理完成，驱动输出 `gate passed: mode=restart`。

本轮是独立新实例的**同镜像滚动重启**，不是镜像升级、数据迁移、节点硬故障或整体生产就绪验收。不能直接把与此前 Ceph 实验的差异归因于单一存储因素：实例、数据历史、时间窗口和 rollout 类型不同，旧后端也仍保留运行。

## 固定对象与原门限

- 专用命名空间：`kubebrain-dbaas-test`，UID `6c57c242-912b-41bb-9020-f4fdb3225ef3`。
- KubeBrain：`kubebrain-local`，UID `7d760f53-5bb5-4429-a2f8-651b89665616`；默认 2PC，无 1PC/async commit。
- 源码/镜像：已审核 `bb89c3f849565f997e347f1f9f947711c50010ca`；固定 index `sha256:50b9938fe3e5ad379136957438abe3c2d4bf250dc6c5973c869e0ef1341d1537`。本次本地工具提交不是新镜像 CI 证据。
- 后端：`kb-local`，UID `512e36f6-306d-407d-ad79-aab446ad34e2`，PD cluster ID `7686251028133611667`；v8.5.3，PD/TiKV 各三副本，TopoLVM `kubebrain-local-lvm`，六个 Retain 数据卷。
- 负载 6000 次、间隔 100ms；rollout 后完成等待 900 秒；公共操作 5 秒、直连流 30 秒、单命令 10 秒、TSO/Region 各 1 秒。未放宽。
- Probe `kb-local-2pc-wcwe26jr`，UID `794b95be-866d-4018-bd39-ea731bb0e547`；独立 mTLS 身份，信息端点验证独立 CA。

generation 从 1 到 2，revision 从 `kubebrain-local-695cd759c6` 到 `kubebrain-local-568bd68448`，最终 3/3 Ready/updated。探针最终 elapsed 829249ms **包含滚动重启阶段**，不是单独的 post-rollout 时间；原 900 秒门限通过以驱动终态和 gate 记录为准。

## 最终结果

| 项目 | 结果 |
| --- | --- |
| 操作 / Watch | 6000 成功、0 失败；公共 6000，直连 6000 × 3 |
| Watch 完整性 | missing / ambiguous / invalid 均为 0 |
| 公共操作最大耗时 | 2225ms，低于 5000ms |
| Put / Put 后 Watch 最大耗时 | 2188ms / 65ms |
| 直连最大耗时 | 9575ms，低于 30000ms |
| TSO / Region 最大耗时 | 14ms / 12ms，各低于 1000ms |
| 公共 Lease | alive，422 次响应，0 次重建 |
| 直连 Lease | alive，1233 次响应，6 次重建，最大恢复 4311ms |
| RangeStream / Snapshot | 654 / 1；流重试 14，部分响应重试 1 |
| SDK Put | 6000 次，最终错误 0；期间存在内部 Unavailable 重试 |

不能把最终无错误写成没有发生连接迁移、重试或短暂不可用。

## 诊断与修复

首次启动驱动在只读预检失败：一个新 TiKV 副本未暴露 scheduler 直方图；当时未创建正式探针或变更 StatefulSet。采集器修复为显式记录 present/absent，不用缺失补零，身份/响应大小/其他必需指标检查保持不变。168 个子用例通过，提交 `3cefcc00`。原失败证据保留。

正式测试又暴露普通重启模式的诊断 phase 使用空 `TARGET_IMAGE`，导致应用采集器拒绝该记录。主验收不受影响，不能声称该自动应用采集器成功。独立后端采集器有 70 次 callback 成功，终态 0；这些并不全是已分类的稳定阶段样本。运行期间另行执行两次身份固定的手动应用/TiKV 采集，均退出 0；未改写运行中的脚本或其 phase 文件。

驱动退出后，诊断 phase 改为候选镜像缺省时使用原镜像。新增 stable/cleanup 两个回归用例先红（0.044s），修改后 `TestRolloutDiagnostic(Phase|Sampler)` 通过（35.529s）。此修复只改变诊断记录，不改变产品语义、镜像和验收门限。

两次手动采样的三副本容器身份在采样前后及两次之间一致；稳定窗口 leader `kubebrain-local-1` 从 22:00:57.509 到 22:07:11.194 UTC，2799 次 Put 服务端平均 23.291431ms，batch commit 平均 8.937596ms，prewrite 平均 4.786404ms。它们是均值而非尾延迟，阶段有重叠不能相加，也不是严格的存储介质对照实验。

## 清理与保持不变的对象

- 驱动确认测试 keys/users/roles/leases 均为 0，正式探针、cleanup Pod、owner ConfigMap 已消失。
- 六个本地 PD/TiKV Pod 的 UID、container ID、启动时间和 restart count 在测试前后完全相同；后验连续三次 Region 健康检查通过。
- 旧 `kubebrain` 的 UID、generation 102、完整 spec 不变，3/3 Ready；旧 Ceph 后端保留。
- 六个旧 scratch PV 只在驱动终态后清理：逐一核对保存的 UID/full spec、Released 状态、TopoLVM driver、claim 身份；确认同名新 PVC 绑定不同卷、没有 VolumeAttachment；带 UID/resourceVersion/full spec 前置检查仅修改这六卷的回收策略。PV 和对应 LogicalVolume 均已消失，临时数据不可恢复。当前仍有 12 个 Bound 本地卷（六个后端数据卷、六个新 scratch 卷），StorageClass 仍为 Retain。
- 清理脚本首个只读检查曾因 kubectl 对不存在对象输出空内容、jq 无输入而退出，未发生删除；修正空结果检查后执行成功。
- 本轮编译的 `uid-delete` 辅助二进制已在驱动和采集器终止后按精确路径删除，可重新构建；未删除项目或用户文件。

## 证据与未完成事项

证据根目录 `/root/.local/state/kubebrain/local-2pc.wcwe26Jr/`：`runner.log`、`runner.exit`（0）、`runtime/`、`backend-observer-evidence/`、两个 `manual-sample.*`、`manual-pair-latency.txt`、`backend-postflight-health.log`、`retire-scratch.log`、`scratch-retirement.UhZzwHLA/`、回归 red/green 日志。原 `/tmp` 证据已在采集器终止后移入此目录；凭据不入仓库。

还需实际升级/回退、硬故障与恢复、多日负载等验收，以及继续完成兼容性和生产能力差距。该通过结果不代表整体 DBaaS 目标完成，也不将旧 Ceph 默认 2PC 失败记录改写为成功。
