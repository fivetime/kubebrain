# 2026-09-16 默认 2PC 滚动验收：完成时限未通过

## 结论

本轮验收未通过。执行器按原“滚动完成后 900 秒”门限判定超时并退出 1，随后成功恢复原镜像和完整配置。探针最终完成 6000 次且没有操作错误，但该迟到结果不能覆盖执行器的超时结论，也不能写成默认 2PC 验收通过。

这不证明丢数据，也不证明 Ceph 是超时或此前偶发慢写的根因。整个 DBaaS 生产就绪目标仍未完成。

## 版本与范围

- 源码：`bb89c3f849565f997e347f1f9f947711c50010ca`，分支 `dbaas`。
- 镜像 CI：[35143004834](https://github.com/fivetime/kubebrain/actions/runs/35143004834)，全部成功；本机独立镜像校验通过。
- 索引：`sha256:50b9938fe3e5ad379136957438abe3c2d4bf250dc6c5973c869e0ef1341d1537`。
- amd64：`sha256:fb95e438dfe27898160c69c01d9a65e76b8c1e3e959db695c4a80e30e6831d25`。
- arm64：`sha256:3651d4e9fa65eec2bdc89f1d6592c53021a40ba48d772d122f5baeb71761ebce`。
- 实际运行镜像内版本为 `0.0.0-dbaas-bb89c3f84956`，Git SHA 匹配，Go `1.26.8`；fork 模块与安全依赖版本已核对。
- 专用集群：控制节点 `10.32.32.66`，namespace `kubebrain-dbaas-test`；消费者 `rook-ceph` 的 `nvme-rep3-rbd-pool`，未使用 secondary 集群。
- 全程默认 2PC，1PC/async commit 均未启用；未重启 PD/TiKV 或节点，未拆分 Region。
- 6000 次操作、100ms pacing、滚动完成后 900 秒时限、公开路径 5 秒、直连路径 30 秒、命令 10 秒、PD TSO/Region 读取各 1 秒，均未放宽。

本次相对 `42903fed1c1d8972e421f5a70bb3d38ddf859be3` 仅修改部署工具、测试和文档，产品及探针运行代码未变。工具 744 项本地测试通过；产品/探针 CI 证据明确归属于此前源码的 [35137630493](https://github.com/fivetime/kubebrain/actions/runs/35137630493)，不冒称本次源码的新探针 CI。

## 探针最终输出（不能替代时限结论）

| 项目 | 最终输出 |
| --- | --- |
| 操作 | 6000/6000，失败 0 |
| Watch | 公开 6000，直连 6000 × 3；缺失/歧义/非法事件均为 0 |
| 公开路径最大延迟 | 1715ms |
| Put / Put 后 Watch 最大延迟 | 1611ms / 147ms |
| 直连 Watch 最大延迟 | 13164ms |
| PD TSO / Region 最大延迟 | 6ms / 14ms |
| 租约 | 公开和直连均存活；公开重连 0、直连重连 8 |
| 直连租约最大恢复时间 | 7979ms |
| 流式读取 / 快照 | 845 / 1 次成功；流重试 11，部分结果重试 0 |
| 探针最终进度时间 | 2026-09-16 20:42:18.034798329 UTC |

最终进度 `elapsed_ms=1058986` 包含滚动期间，不能直接拿它减去 900 秒计算超时幅度。原执行器使用自身单调时间预算，在截止前未观察到探针 Pod `Succeeded`，记录 `availability probe did not complete within 900s`。探针完整日志在恢复期间采集，含后来产生的成功计数。精确超过截止时间多少，仍需结合执行器时间证据分析，本文不推算。

SDK Put 调用 6000 次、最终返回错误 0；滚动期间存在 SDK 内部 `Unavailable` 重试，不能声称没有网络或连接重试。

## 恢复与清理

原镜像恢复为 `ghcr.io/fivetime/kubebrain@sha256:0ce85e66320b27bf4cdb8f11981a76cba58926fa0e835fc47dc663f709b588ce`，完整 spec 与实验前相同。独立检查确认 generation/observedGeneration 均为 102、Ready/updated 均为 3、revision 为 `kubebrain-855b5bfb88`，三个实际运行 imageID 与原成员一致。

三个 PD 和三个 TiKV 的 Pod/容器身份、重启次数、启动时间均未改变；六个 PVC/PV 的 UID、完整 spec 和 Bound 状态未改变。后置检查确认 PD 3、TiKV 3，连续 3 次异常 Region 为 0。

探针、清理 Pod、owner ConfigMap、本轮三个 `kb-prepull-*` Job 及其 Pod 均已不存在；预拉取回执的三个目标均标记 removed。fixture 清理记录 keys/users/roles/leases 全为 0。服务日志观察器和后端观察器均退出 0；这些退出码不表示每个过渡期采样均完整，也不表示验收通过。

执行 claim 已消耗，`DEPLOYMENT_HOLD.md` 已恢复。独立镜像校验的临时容器、提取二进制及本轮自有部署辅助二进制已清理；源码、摘要、日志保留，可重新构建。原 Ceph 数据卷未删除。

## 证据与后续

本机受限证据目录：`/root/.local/state/kubebrain/default-2pc.p9lMtV8I/`。

- `execute.log` / `execute.exit`：唯一执行记录，退出 1。
- `execute.ohch6chU/independent-restoration/`、`restoration.log` / `restoration.exit`：独立恢复复核通过。
- `post-experiment/runtime/`：完整探针、恢复、fixture 清理证据。
- `post-experiment/backend-observer/`：独立后端采样与退出记录。
- `post-experiment/pods.json`：包含实际 `kb-prepull-*` 名称前缀的残留检查。
- 镜像校验证据：`/root/.local/state/kubebrain/default-2pc-restore-ci.OfuQYro3/`。

用户已要求后续改用 TopoLVM 本地盘测试。新增盘已只读核对；实际宿主机底层存储仍待确认，尚未初始化或迁移。具体映射和边界见 [TopoLVM 测试存储准备](topolvm_test_storage_cn.md)。该方向不追溯改变本轮失败结论，也不能替代持久化、节点故障恢复、长稳及其他生产验收。
