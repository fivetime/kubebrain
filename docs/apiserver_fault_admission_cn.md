# apiserver 故障注入前置确认

原 standalone rollout runner 启动 Watch 子进程后固定 `sleep 12`，然后重启
工作负载。慢启动时 Watch 可能尚未建立，测试即使通过也不能证明覆盖了已建立流
上的切换。

现在 `apiserver-watch-soak.sh` 先确认 `soak-1` 的初始 ADDED/version=0，再提交
本来就属于测试矩阵的第一项更新 `(soak-1, version=1)`，等待同 namespace 的
MODIFIED/version=1 实际出现在流中，才输出独立整行 `APISERVER_WATCH_READY`。
随后的更新循环跳过这一个已完成项，保持总量 `OBJECTS * UPDATES` 和最终逐项
完整性门禁不变。未看到初态、修改回执、Watch 进程死亡或超时均失败。

standalone rollout runner 使用 `wait-apiserver-watch-ready.sh` 等待此标记，并
检查子进程仍存在。无标记、部分文本匹配、死亡进程、非法 PID/timeout 或等待
超时都不能进入重启命令。该确认不是“未来不会断流”的证明，也不替代目标
UID 检查、后续完整性和健康恢复门禁。

本轮没有修改 in-cluster rollout 的故障触发方式。独立 rollout 的 kind 来源
约束及外部连接适配也仍待处理，不能把这个前置确认写成完整外部集群故障验收。

另外已核对当前 `MoveLeader` 实现：非 leader 返回 not-leader；目标是当前
leader 时幂等成功；转移到其他成员返回 platform-managed 错误。未发出无效
转移请求，不能用“MoveLeader 调用成功但目标未变”冒充真实 leader 切换。

本地测试包含五种前置确认场景和调用顺序检查；新增 helper 最初加载位置破坏了
无工具环境下的早期拒绝契约，已移至参数/工具检查之后。扩大 runner/apiserver/
version-matrix race 测试最终通过（9.233 秒）。本轮并非全部测试从首次运行就通过。

真实验证使用已核验官方 v1.36.1 apiserver、ephemeral PKI、原 bb89c3f8 默认 2PC
本地盘后端，20 对象 × 10 更新，禁止 Watch 重连。证据目录为
`/root/.local/state/kubebrain/local-apiserver-barrier.xfA0ESEA/`。
本次只验证新的前置确认与完整性检查接线，**未注入故障**。

真实运行终态 0：输出 `APISERVER_WATCH_READY`，最终 `modified_events=200`、
`integrity=passed`；清理后本轮 prefix 为空、lease 集合为 0、endpoint health
通过，三个 StatefulSet 前后的 UID/generation/spec 不变且 3/3 Ready。
临时 WORK_DIR/PKI 已由 runner 清理，两处本机转发/服务端口均无监听。
本地 HEAD 记录为 `cf8b756a` 加本次工作树变更，未部署新产品镜像。
