# 三节点标签同步专项验证（2026-09-19）

使用提交 `6430bf38` 的冻结源码，在专用测试集群三个现有 KubeBrain
Pod 上逐个添加、观察并撤销独立测试标签。没有部署候选镜像、创建网络
隔离策略、改变事务模式、滚动 Pod 或创建存储资源。

执行前核对原 StatefulSet 完整 spec/UID、generation 98、三副本就绪，
并检查命名空间 NetworkPolicy、CiliumNetworkPolicy 和集群级 Cilium
策略没有引用测试标签键。添加使用 Pod UID/resourceVersion/原标签
条件补丁；恢复只撤销同一 Pod 上本轮 token，不覆盖外来标签。
每次添加和撤销观察分别限定在固定 120 秒内，单次调用最多 30 秒；
仅 75 可重试，其他错误中止并尝试恢复。

## 结果

三个主流程、恢复流程均退出 0。每一阶段只在完整身份校验及目标标签
一致后成功；64 次采集中 58 次 pending、6 次 matched。

| Pod | 节点 | 添加：pending → matched | 撤销：pending → matched |
| --- | --- | --- | --- |
| kubebrain-local-0 | k8s3-worker3 | 3 → 1 | 4 → 1 |
| kubebrain-local-1 | k8s3-worker1 | 20 → 1 | 4 → 1 |
| kubebrain-local-2 | k8s3-worker2 | 12 → 1 | 15 → 1 |

现场捕获了 `waiting-for-identity`、`regenerating` 和 `ready`。worker2
还捕获一次 CEP/端点身份 ID 更新不同步，该采集未被提前判为成功。
每次观察的摘要及其引用的完整采集摘要均重新验证通过。

全部撤销后的新鲜集群复查确认：三个 Pod 与各自实验前为同一进程，
原标签完整恢复，三副本 Ready；StatefulSet spec/UID/generation 98
未改变，所检查的策略 UID/spec/specs 未改变，无本轮标签残留。

私有证据目录分别为 `label-transition-validation.oP8541rf`、
`label-transition-validation.MeDtrEPT`、`label-transition-validation.k4qVa8wF`。
包含冻结源码、条件补丁、前后快照、逐次结果和摘要；没有编译新二进制。

## 边界与下一步

本次只证明这三个 Pod 各一次标签添加/撤销的有界同步路径和恢复成功，
不是流量隔离、租约响应、候选协议或整体生产就绪验收。不改变此前
失败轮次的结论，也不保证所有 Cilium 状态、网络故障或并发变化均已
在真实环境覆盖。取消/超时和异常身份仍由对应回归测试覆盖。

下一次完整故障验收需建立新实验身份、重建工具、重新验证证书/镜像/
基线，完整摘要必须包括 `local-label-identity-transition.jq`。不可复用
已消费并清理的旧实验目录，原 30 秒门限和默认 2PC 保持不变。
