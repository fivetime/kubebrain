# 标签身份同步实验（2026-09-19）

私有目录 `fault-identity-pending.hWAQEBdf`，尝试
`deploy-execute.nvZ7RmLJ`，产品候选仍为 `2ad79751`。
四个扩展阶段均通过，标签准备阶段失败，故障控制器退出 70，
故障清理退出 0。未进入原 30 秒响应验收，不能作产品门限结论。

标签准备先得到 12 次退出 75 的 pending 结果，随后
`identity-observation.ePxNpj3j` 退出 65。对应 `endpoint.lZEvzV4A`
状态为 `regenerating`，端点 ID 799；CEP 与端点身份 ID 均为 212017，
均带本轮标签。旧采集器因状态白名单拒绝，在后快照之前停止。
回滚时 `endpoint.vt3QdWcp` 仍为 `regenerating`，CEP 身份 212017，
端点身份已回到 211368，显示同步视图不同步。外层故障清理随后成功。

## 完整状态判定准备

新增纯判定器 `local-label-identity-transition.jq`，输入 CEP 前后快照
与中间端点快照。它仅判定标签身份同步，不负责采集，不证明流量隔离。

- CEP UID、端点 ID、ownerReferences 和网络配置必须稳定。
- 身份 ID 必须是有效正整数，标签集合不得缺失或重复。
- 三份视图仅允许本轮 fault-owner 标签有差异；其他身份标签必须一致，
  外来 owner 立即失败。同一 ID 对应不同标签或同一标签对应不同 ID 也失败。
- 已知 `waiting-for-identity`、`regenerating` 以及 CEP 滞后只能 pending；
  只有 `ready`、三个身份一致且目标标签状态匹配才可 matched。

19 个正反场景三轮 race 测试通过（1.537 秒），`go vet` 与 diff 检查
通过。测试使用合成完整快照；现场失败记录缺失后快照，不能补造现场
通过证据，也不能用该判定器追溯改变本轮失败结论。

现已接入仓库采集器的显式 `--label-transition MODE TOKEN` 路径，
在独立验证原 Pod 与 Cilium agent 前后完整进程身份、命名空间/owner、
IP 对应及 CEP 结构后，才进行同步判定并发布证据摘要。标签观察器仍
对两份 Pod 快照验证目标 API 标签；pending 只能返回 75。默认调用
和策略/丢包采集保持严格身份一致及 ready 要求，不能将标签路径用于
证明策略生效。下一轮依赖清单必须额外绑定新判定器文件。

接入后的端点、标签/丢包、策略与纯判定回归首次通过（race 62.769 秒）；
随后扩展为 31 个标签/丢包场景与 19 个纯判定场景，三轮 race 通过
（127.286 秒），`go vet`、Bash 语法与 diff 检查通过。新增脚本场景
验证标签添加/撤销时 CEP 滞后的首次采集返回 75，第二次完整采集
收敛后返回 0，且两次证据摘要有效；未知状态、非目标标签变化等仍拒绝。

标签专用的取消/超时验证及有界真实标签专项验证仍待完成，不能直接
重跑完整候选滚动与故障实验来代替这些验证。运行中的旧实验始终使用
冻结副本，没有以新代码改写失败现场。

## 恢复状态

原执行器（会话 13106）已终止，主流程退出 70；逆序恢复及 Secret
清理均退出 0。新鲜复查 `restoration-check.mGm33haw` 确认原完整
StatefulSet spec/UID、原 Secret UID/resourceVersion/data 恢复或保持
一致；generation/observedGeneration 为 98，3 Ready / 3 updated，
revision 回到 `kubebrain-local-568bd68448`。两份实验 Secret、策略与
本轮标签均不存在。当前 12 Bound / 104 Released；相比基线
12 Bound / 56 Released，多出的对象还需按历史创建凭据核对。
存储和编译残留尚未清理，不能把部署恢复等同于全部清理完成。
