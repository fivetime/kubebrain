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

标签专用的外层取消和内层超时验证现已补齐，三轮 race 通过
（10.314 秒）。测试在真实脚本的端点采集处阻塞模拟 kubectl，验证
阻塞进程已退出、观察及采集退出码正确，且两层均未发布成功证据。
内层用例将测试中的 20 秒调用预算压缩为 0.2 秒，仍使用真实 timeout
信号流程；不声称验证了生产环境的实际等待时长。随后综合标签、端点、
策略与纯判定 race 回归通过（73.537 秒），`go vet` 和 diff 检查通过。

有界真实标签专项验证仍待完成，不能直接重跑完整候选滚动与故障实验
来代替这些验证。运行中的旧实验始终使用
冻结副本，没有以新代码改写失败现场。

## 恢复状态

原执行器（会话 13106）已终止，主流程退出 70；逆序恢复及 Secret
清理均退出 0。新鲜复查 `restoration-check.mGm33haw` 确认原完整
StatefulSet spec/UID、原 Secret UID/resourceVersion/data 恢复或保持
一致；generation/observedGeneration 为 98，3 Ready / 3 updated，
revision 回到 `kubebrain-local-568bd68448`。两份实验 Secret、策略与
本轮标签均不存在。当前 12 Bound / 104 Released；相比基线
12 Bound / 56 Released，不能按数量差直接决定删除范围。

12 项归属规划和 6 项清理控制器模拟通过，创建凭据确认 42 个不属于
基线的临时卷可回收。`scratch-cleanup.lAg9NpZl` 退出 0，逐个以
UID/resourceVersion/spec 条件补丁回收并等待删除；无引用、无挂载、
旧 Pod 不存在及 CSI 身份独占检查均通过。基线与非目标 PV UID/spec
不变，命名空间 Pod UID/spec/容器状态不变。最终 12 Bound /
62 Released，额外保留的 6 个 Released 属于原基线。
新鲜复核 `scratch-cleanup.f6SjgLix` 返回 0 个本轮待回收目标。
临时卷数据不可直接恢复，历史快照和删除证据保留。

`compiled-cleanup.IMBT53d4` 随后退出 0，删除 44 个匹配本轮构建摘要、
ELF 类型且未被进程使用的编译产物路径，保留源码、脚本、证书及原始
证据；这些程序可重新编译。旧二进制清单指向已退休路径，不应再对
这个已消费的实验目录执行准入或部署。
