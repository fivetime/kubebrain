# 2026-09-16 临时 async commit 滚动验收结果

## 结论与范围

本轮真实专用测试集群验收通过，执行器退出码 0，实验结束后已恢复原完整 StatefulSet spec、固定镜像和默认 2PC。此结论仅覆盖本次临时 async commit 滚动更新，不代表默认 2PC 已通过同样验收、此前偶发慢写根因已解决，或整个 DBaaS 项目已经完成。

- 源码：`0e7af672503ed12362eaad63f99c2f6bff71c663`，分支 `dbaas`。
- 镜像 CI：[35130192439](https://github.com/fivetime/kubebrain/actions/runs/35130192439)，成功；独立镜像核验通过。
- 候选索引：`sha256:e52edfeda4dca812bb2ec949543e2bafd1c6a9fbc23b9640fb06464b5f760d4b`。
- 专用集群：控制节点 `10.32.32.66`，namespace `kubebrain-dbaas-test`；仅使用消费者 `rook-ceph` 的 `nvme-rep3-rbd-pool`，未使用 `rook-ceph-secondary`。
- 临时启用 async commit，1PC 关闭；未重启后端或节点、未拆分 Region，未放宽门限。

## 验收结果

保留原 6000 次、100ms pacing、滚动后 900 秒完成时限、公开路径 5 秒及直连路径 30 秒门限；命令超时 10 秒，PD TSO/TiKV Region 读取门限各 1 秒。

| 项目 | 本轮结果 |
| --- | --- |
| 操作 | 6000/6000，失败 0 |
| Watch | 公开 6000，直连 6000 × 3；缺失/歧义/非法事件均为 0 |
| 公开 Put-to-Watch 最大延迟 | 2587ms（门限 5000ms） |
| Put 最大延迟 | 2555ms |
| 直连 Watch 最大延迟 | 17367ms（门限 30000ms） |
| PD TSO / Region 读取最大延迟 | 14ms / 11ms |
| 租约 | 公开与直连均存活；公开重连 0，直连重连合计 11 |
| 直连租约最大恢复时间 | 12784ms |
| 流式读取 / 快照 | 840 / 1 次成功；流重试 12 次，部分结果重试 0 |
| 探针最终进度时间 | 2026-09-16 18:44:27.185 UTC |

探针计数阶段总耗时 1007289ms，包括滚动期间，不应误作“滚动后完成耗时”。执行脚本按原滚动后时限等待成功，并输出 `KubeBrain rollout availability gate passed`。SDK Put 调用 6000 次、返回错误 0；滚动期间仍有 SDK 内部 `Unavailable` 重试日志，因此不能声称完全没有传输重试。

## 恢复与独立复核

原镜像恢复为 `ghcr.io/fivetime/kubebrain@sha256:0ce85e66320b27bf4cdb8f11981a76cba58926fa0e835fc47dc663f709b588ce`。恢复后 generation/observedGeneration 均为 100，revision 为 `kubebrain-855b5bfb88`，Ready/updated 均为 3。完整 spec 与实验前快照相同，三个运行容器的 imageID 与实验前对应成员相同。

实验探针、清理 Pod、owner ConfigMap 和三个本轮预拉取 Pod 均已删除。fixture 清理记录 keys/users/roles/leases 全部为 0。三个 PD 和三个 TiKV 的 Pod/容器身份、重启次数、启动时间均与实验前一致。后置健康检查确认 PD 3、TiKV 3、连续 3 次异常 Region 为 0；检查覆盖后端卷健康。

执行器与两路观察器均退出 0。独立后端观察器完成 100 次成功回调，采样前阶段分布为 unknown 19、stable 69、cleanup 12；阶段标签只是采样上下文，不等于健康或验收证明。它持续覆盖清理及恢复阶段。此次没有复现上一轮慢写，不能据此宣布根因消除。

## 证据与后续

本机受限证据目录：`/root/.local/state/kubebrain/async-observed.DaCbD8l2/`。

- `execute.log` / `execute.exit`：本次唯一执行记录，退出码 0。
- `execute.otU1Lwuz/`：实验前快照、执行及观察器退出记录、恢复验证。
- `post-experiment/runtime/`：完整探针输出、候选与恢复证据、fixture 清理日志。
- `post-experiment/backend-observer/`：独立后端观察原始数据与退出记录。
- `post-experiment/{statefulset,pods,configmaps}.json` 和 `backend-health.log`：独立收尾复核。

执行 claim 已消耗并重新设置 `DEPLOYMENT_HOLD.md`，禁止复用该执行目录重跑。两个本轮自有辅助二进制经 SHA-256 校验后已删除；保留源码、摘要和证据，可按需重新构建。未保存密码或私钥到仓库。

后续仍需分别推进默认协议验收、此前偶发慢写根因、长稳及其余生产验收项目；不得将本轮临时实验结果外推为全部完成。
