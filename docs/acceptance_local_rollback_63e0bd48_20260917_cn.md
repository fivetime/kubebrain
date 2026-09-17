# 本地盘持续负载回滚验收（2026-09-17）

## 结论与边界

专用本地盘实例从 `63e0bd48` 反向滚动至原固定镜像，在同一探针持续运行期间通过原 6000 次可用性门限；外层执行、恢复及预拉取清理均退出 0。**附加应用指标采集七次全部失败，不能宣称诊断完整或整体项目验收完成。**

候选源为 `63e0bd48b8f3b0536a68d2a92ae41c11c7daec73`，镜像 CI `35186635186`、探针 CI `35186634732` 同源成功。独立镜像审计在 `release-63e0bd48.8pcmIS6U/audit.N5lhT0Rv`，候选 index 为 `sha256:c8df2c7972760dd007dc9cf9b472c061dcea3139f5183b67bd170d8fa17e3ae3`。旧目标 index 为 `sha256:50b9938fe3e5ad379136957438abe3c2d4bf250dc6c5973c869e0ef1341d1537`；旧目标不可变清单重新核验，二进制身份复用历史审计。

先把隔离前端准备到候选，再由同一新候选探针覆盖反向滚动；不是负载结束后才恢复旧镜像。探针 UID `1ef6c913-789a-41e4-9739-2a55775e5854`，驱动记录 `ROLLOUT_PROBE_COVERAGE_CONFIRMED`。默认 2PC，未放宽公共 5 秒、直连 30 秒、TSO/Region 各 1 秒及 rollout 后完成等待 900 秒的门限。

## 原始结果

06:29:39 UTC 最终结果：6000 成功、0 最终失败；公共 Watch 6000，三个直连 Watch 各 6000；Watch 缺失、歧义、无效均为零。全程耗时 851207ms，包括滚动阶段，不能当作 rollout 后耗时。

| 项目 | 观测值 |
| --- | --- |
| 公共最大延迟 / Put / Put 后 Watch | 2136 / 2066 / 70 ms |
| 直连最大延迟 | 10649 ms |
| TSO / Region 最大延迟 | 8 / 12 ms |
| 公共 Lease | alive，431 响应，0 重启 |
| 直连 Lease | alive，1267 响应，6 次重启，最大恢复 4832ms |
| RangeStream / Snapshot | 704 / 1 |
| Stream 重试 / 部分结果重试 | 9 / 0 |

6000 次 SDK Put 最终错误为零、confirm 调用为零，但滚动期间有 `Unavailable: there is no connection available` 的 SDK 重试日志；不能写成零尝试错误或零重试。测试键、用户、角色及租约清理检查均为零。

## 附加诊断失败

七个 `runtime/diagnostic-sample.*` 的 `sampler.exit` 均为 1，均停在空的 `capture/probe-before.json`，未获取应用指标。

代码与保存身份的对照显示：`capture-rollout-tls-metrics.sh` 的 `identity()` 将 phase 中的目标镜像同时用于校验探针和服务 Pod。此次 phase 为旧目标 `50b993…`，但特意保持新候选探针 `c8df…`，因此探针镜像检查不成立。该限制不支持独立探针镜像；不是已证实的业务请求失败，也不能通过忽略退出码补成有效样本。后续需分别绑定两类身份并补回归测试，本轮不得追溯改写采集结果。

独立后端 observer 退出 0 不能代替失败的应用指标采集，也不能单凭累计指标证明存储延迟或事务协议分布。

后续已在本地修复上述双镜像身份校验并通过定向回归，见[采样身份约束及验证](rollout_diagnostics_cn.md#探针与服务使用不同镜像)。这是实验之后的工具修改，没有重新采集或追溯改变本轮失败结果。

## 恢复、清理和证据

原 StatefulSet 完整 spec、实际镜像及三副本 Ready 已恢复，generation 14 → 15 → 16。独立后置检查确认本地 PD/TiKV 和旧 Ceph 前端保持原身份及配置。仅精确回收本轮有历史 Bound 身份证据的 12 个临时卷，其数据不可恢复；另外两块历史 Released 卷继续保留。最终本地卷为 12 Bound、2 历史 Released。未重复初始化 VG，未迁移或删除旧 Ceph 数据。

证据根目录为本机 `/root/.local/state/kubebrain/local-rollback-preparation.IbvdS7pb/`：外层 `deploy-execute.5ao2jRCT`、内层 `reverse-execute.ZDfj7M6M`、恢复 `restore.bunwCqyj`、最终后置检查 `postflight.tO2UcuN0`。终态后已将探针及后端证据归档到 `runtime/` 和 `backend-observer-evidence/`，原临时路径不再有效；两份本轮辅助二进制经摘要核验后删除，转发端口释放。

`runtime/probe-final.log` SHA-256：`d799970f68f602a52690c2f217a18d0ce4d9724d9de282b0f9fdba6d837b06a1`。

本项不替代全控制器/调度器接入、节点断电、存储故障、长期稳定性或所有内部 Lease 写入故障语义验收。
