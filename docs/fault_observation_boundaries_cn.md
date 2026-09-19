# 故障观察：身份、健康和收敛边界

本说明依据 `fault-2ad-recheck.HegMKL6X` 已消费脚本及现场失败记录整理。
该轮原始响应在 28.698617070 秒到达，但撤销隔离后的采集把 Ready 恢复
误判为身份变化；完整实验仍失败，不追溯改判。

## 身份检查审查

| 位置 | 目的与处理 |
| --- | --- |
| 候选 Pod 准入、prepare-label、install-policy | 故障前的健康基线及无漂移门限，不能把所有完整 status 比较机械替换为身份比较 |
| 故障前后受保护采栈 | 使用 `same-pod-process.jq`；Ready 变化不等于进程替换 |
| 端点采集内应用及 Cilium agent 的前后对照 | 仓库版 `capture-local-cilium-endpoint.sh` 已使用同一身份规则；真实新实验尚未验证 |
| drop 采集窗口之间的 Pod/agent、标签及策略观察 | 仓库版现已统一进程身份规则，并单独检查 CEP identity、Pod 标签和 endpoint 标签；已消费脚本不变，新驱动接入及真实验证仍待完成 |

身份规则包含 UID、完整 spec、IP、容器 ID/镜像标识、重启次数及启动
时间，不以 Ready 标志证明身份，也不以身份稳定证明服务健康或网络
隔离。Cilium endpoint ready、策略 revision 收敛及实际丢包是不同证据。

## 固定恢复截止时间

`hack/production/wait-policy-absence.sh OUTPUT DEADLINE_NS CALLBACK SHA256`
只负责限时调用只读观察器，不删除策略或标签，不重发原始业务请求。
调用者必须在确认原请求、采栈和持久化断言后，为恢复阶段记录一次绝对
截止时间（剩余时间不超过 60 秒）；工具不会每轮重新开始计时。
每个观察上限 30 秒并受剩余时间约束，重试间隔最多 250 毫秒。

观察器是经过审核并固定摘要的 Bash 脚本，仅接收 `absent` 参数：

- 退出 0：已证明同一目标、同一进程上的指定策略不存在。
- 退出 75：身份已核验，但指定策略仍存在或其 revision 尚未收敛。
- 其他退出码：立即失败，包括身份冲突、未知错误及超时，不重试。

成功必须在原截止时间前，且观察器摘要仍一致。每轮保留开始/结束时间、
stdout/stderr、退出码及摘要检查；失败不写 `matched-sample`。时钟倒退
会拒绝结果。调用者须使用进程组级外层 watchdog；foreground timeout
避免阻塞观察器脱离外层取消范围，取消不转成成功。

该工具不能自行证明观察器的退出码语义，也不能把旧观察器的一般退出 1
包装成 75。仓库现已提供下述明确区分状态的实际观察器，但尚未接入新的
真实故障驱动或冻结部署。不得把该等待逻辑放入原 30 秒响应门限内来延长
门限，也不得借恢复成功补写原实验缺失的断言。

验证：12 个返回码/期限/摘要场景加外层进程组取消测试，三轮 race
通过（17.374 秒）；`go vet ./hack/production`、Bash 语法和 diff 检查
通过。测试只证明工具的重试、拒绝、截止和取消行为，未调用真实集群。

## 实际策略观察器

`deploy/test-cluster/observe-local-policy-state.sh OWNER MODE POLICY_UID POLICY_NAME EXPECTED_POD`
调用仓库版端点采集，验证其全部文件摘要，并将采集前后 Pod 与本轮
已固定的 `EXPECTED_POD` 用同一进程身份规则比较。其 owner 权限、集群
和目标 Pod 约束沿用专用集群采集工具；MODE 为 present 或 absent。
调用者应从本轮真实创建凭证绑定策略 UID，而不是根据同名资源猜测 UID。

`local-policy-observation.jq` 只在身份标签、完整策略证据和可精确表示的
非负整数 revision 均有效时分类：

- revision 相同且指定策略状态符合 MODE：matched，退出 0。
- desired revision 大于 realized，或相同 revision 下状态尚不符合
  MODE：pending，退出 75。
- 标签不符、策略名与 UID 只出现其一、revision 倒退、缺少 realized
  L4 ingress/egress 数组、非 ready 端点或任何身份/摘要错误：失败，
  不得当作 pending。未知/API 错误也不盲目重试。

观察器产生 `result.json`、`observation.exit` 和证据摘要；有效 pending
也保留证据，非法/不完整观察不生成成功证据清单。该分类只证明 agent
报告的策略实现状态，不证明逐包隔离或业务恢复。

适配等待工具时，由新实验生成只接受 `absent` 的固定参数 callback，
将 owner、创建凭证 UID、policy name 和原 Pod 文件路径绑定进去。完整
冻结清单必须包括 callback、观察器、采集脚本、两个 jq 规则以及绑定
配置/Pod 文件，不能只固定 callback 本身的摘要。所有同步 timeout
使用 foreground 模式，避免子观察命令脱离外层进程组 watchdog。

当前组合测试实际运行这些 shell/jq 文件，只有 Kubernetes API 被替换为
合成响应，覆盖 pending→matched、pending 后身份漂移、Ready 恢复、
证据错误和外层取消。保存的上一轮 `endpoint.7az7R6au/endpoint.json`
通过新分类器的 absent 检查（revision 32/32），但那一轮缺失的后续
采集不能补写，其整体失败结论不变。下述 drop/标签工具已统一规则，
但新 owner 驱动接入和真实门限重测仍未完成。

最终验证：14 个策略观察/等待组合场景，加 12 个端点身份场景，三轮
race 测试通过（83.939 秒）；`go vet ./deploy/test-cluster`、Bash 语法
及 diff 检查通过。早期“pending 后漂移”测试在首次采集中就注入变化，
因此失败；调整测试注入时机后验证了首次退出 75、第二次退出 65，
没有修改身份拒绝条件或放宽成功断言。

## Drop 窗口与标签观察

`deploy/test-cluster/capture-local-cilium-drops.sh OWNER POD SECONDS` 将原先
私有实验目录中的只读监控纳入仓库，使用同一端点采集器作前后快照。
窗口前后的应用和 Cilium agent 都必须满足完整进程身份规则；CEP UID、
endpoint ID、identity 和 networking 也必须一致。首次快照失败立即
停止，不启动 monitor；不能因 Bash 命令替换中的错误被忽略而继续。

本地同步 timeout 为 foreground，外层仍须提供进程组 watchdog；远端
monitor 保留独立的限时运行，避免连接断开后无期限驻留。必须同时得到
退出 124 和 kubectl 的明确远端退出报告，否则不视为完成采集。结果
及前后证据清单一起绑定摘要；成功采集不是丢包、隔离或任期丢失结论，
仍需要原来的流解析、后端端口匹配及应用断言。

`observe-local-fault-label.sh OWNER MODE EXPECTED_POD TOKEN` 将标签观察
与进程身份分离。Pod 采集前后都必须与原始 Pod 同进程，且 Kubernetes
标签符合 MODE；Cilium identity 中标签同步则退出 0，仅本轮标签尚未
同步则退出 75。外来 owner 标签、多个 owner 标签、进程变化或缺失证据
均失败，不当作收敛等待。标签恢复是独立恢复步骤，不重置原响应时钟。

两个工具均限定已有专用集群和私有 owner，不改变集群对象。未来控制器
必须绑定同一轮原 Pod、token、固定工具摘要和总截止时间；不得复用已
消费 owner，也不能把标签/策略观察的 pending 当作成功。

验证：18 个 drop/标签完整脚本模拟场景三轮 race 通过（81.189 秒），
覆盖跨窗口 Ready 变化、应用/agent 重启、镜像与 CEP identity 变化、
远端错误/缺失退出标记、首次快照拒绝、外层取消、标签收敛及外来身份。
`go vet ./deploy/test-cluster`、Bash 语法和 diff 检查通过。API 与 monitor
输出为测试夹具，未执行真实 Cilium 监控或集群故障。
