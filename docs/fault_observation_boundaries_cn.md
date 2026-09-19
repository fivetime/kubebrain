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

## 新控制器接入后的准备复核（2026-09-19）

新实验目录 `fault-unified-observer.83IKaf7V` 保持 HOLD。此前完成的
27 项集成模拟之外，本轮通过 13 项 Secret 清理、10 项策略归属规划、
6 项策略预留/激活/撤销控制器模拟。覆盖未知创建结果、UID 替换、
数据或 resourceVersion 变化、工作负载引用、删除冲突、策略漂移；
这些测试没有调用真实集群 API，不是故障验收结果。

只读复核三个 Pod 实际挂载的公开健康/info 证书：证书用途、信任链、
info 主机名和至少四小时剩余有效期通过，采集前后 Pod 进程信息一致；
原 StatefulSet 完整 spec、UID、generation 82 和三副本就绪检查通过。
没有采集私钥。证据及摘要保存在上述私有实验目录。

候选 `2ad79751ebc35291ed8caac144a6e73442b927a6` 的构建、探针与后端
CI（35370904081、35370904212、35370925211）重新查询均为成功。
镜像审计、预拉取、恢复与阶段包装器在新目录中的测试、最终依赖摘要
和新鲜准入检查仍未完成；未部署候选或执行新一轮隔离故障，不改变
前两轮验收失败的结论，也不代表整体生产就绪。

随后补齐：8 项逆序恢复和 14 项阶段包装器模拟全部通过，覆盖阶段失败、
恢复失败、凭据创建响应丢失及 HOLD 拒绝。新镜像审计验证了原始索引摘要、
amd64/arm64 子摘要、源码标签、非 root 用户以及隔离容器的版本输出；
只实际执行 linux/amd64，没有把 arm64 摘要检查写成运行测试。

预拉取先通过 Job 与 Pod 服务端 dry-run，再在三个节点创建精确镜像的
临时 Job。准备、验证和清理均成功，原 StatefulSet 完整 spec、UID、
generation 82、三副本就绪及一致 revision 保持不变。清理后的新鲜
Job/Pod 查询确认三个记录的 Job 名称及其 UID 所属 Pod 均无残留。
私有证据：`prepull-execute.ZxsUqRLD`（main/cleanup 均退出 0）。
仅这些临时 Job 发生了集群变更；没有切换候选、创建实验 Secret 或注入故障。

上述后续结果替代本节先前列出的相应待办。剩余准入项是完整运行依赖
摘要冻结、新鲜阶段 preflight 与最终准入检查。仍保持 HOLD；预拉取后的
缓存记录不等于未来拉取保证，也不等于清理后仍有镜像持有 Job。

### 运行依赖与标签准备复核

运行清单已绑定 49 个脚本、分类器、探针和审计输入，恢复与清理也绑定
完整清单。阶段材料 preflight 通过。复查发现标签准备的旧循环会重试
所有观察错误，现已接入同一类型化标签观察器：仅退出 75 可重试，
身份错误和超时立即失败；固定 120 秒截止时间、单次最多 30 秒且不超过
剩余预算，拒绝时钟回退和逾期成功。无策略检查仍保留。

4 项针对性模拟通过；完整控制器的干净重跑 `term-controller-tests.gDNJOOf4`
也以退出 0 通过 27 项场景。之前一次运行期间误改测试入口默认参数，
导致 shell 收尾读取错误，虽然各场景断言通过，仍按整轮失败处理，
没有计作验收成功。后续重跑期间未修改运行脚本。

只读快照确认原 StatefulSet spec/UID/generation 82、三副本就绪及原
Secret UID/resourceVersion/data 保持一致，本轮策略、标签和临时 Secret
不存在，TopoLVM PV 为 12 Bound / 50 Released。仍保持 HOLD；下一步是
执行前重新检查准入，然后受控运行并恢复。上述均不替代真实故障验收。

实际执行在标签准备时遇到 `waiting-for-identity`，旧端点采集器将其
拒绝，未进入故障响应验收。后续修正给标签观察显式启用
`--allow-identity-pending`，仍完成全部前后身份校验且只返回 75；
未知状态、外来身份及进程替换仍失败。其他调用默认仍要求 `ready`，
不能用该选项证明策略生效或流量隔离。详见
[本轮失败证据与恢复记录](peer_fault_unified_observer_20260919_cn.md)。

### 修正后的新一轮准备

旧轮恢复与残留清理完成后，建立新目录
`fault-identity-pending.hWAQEBdf`，工具源码固定为 `1013ce92`，产品候选
仍为 `2ad79751`。新鲜基线核对 generation 90、三副本就绪及
12 Bound / 56 Released，重新验证 PD 身份、实际挂载证书和发布镜像，
重建六个工具并生成新的独立成员证书及离线 Secret 计划。

新目录的 4 项标签等待、8 项恢复、13 项凭据清理、14 项阶段包装器、
10 项策略规划及 6 项策略控制器模拟全部通过，共 55 项。它们不代表
真实故障验收；主控制器集成回归、预拉取、完整运行摘要和执行前准入
仍待完成。保持 HOLD，本次准备没有修改集群对象。
