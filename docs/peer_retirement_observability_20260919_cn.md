# 退任通知可观测性补充

## 最新 CI 核验（2026-09-19）

本地后续将实际 worker/会话脚本测试接到 `metricsworker.Run`，通过
Baseline/Completed 钩子核验前后 LoadCapture、计数差值、原始故障
起点/offset、调度清单、COMPLETE 和退出回执。正常路径三轮 race
通过（15.793s），并检查三个模拟转发子进程均已消失。此处只下发
时钟，没有注入真实 Kubernetes 故障。

取消改为 TERM 后最多 5 秒升级 KILL，宽限仅供回收，不计入成功
验收预算。增加实际基线拒绝路径后，首轮三次运行中一次缺少退出
回执（整个测试命令失败，24.578s）；代码分析发现关闭 stdin 与
异步 TERM 可竞争：EOF 可先触发 EXIT 清理，再被 TERM 退出打断。
现将 stdin 关闭移到 worker 等待结束之后；修复后成功/拒绝路径
五轮 race 通过（41.943s）。另以 TERM trap 内有界 read 明确检查
输入仍打开（应超时而非 EOF），防止仅靠概率性重复测试。
最终将失败回执严格限定为 TERM 的 `143` 后，再跑三轮实际脚本
集成通过（25.577s）；含确定性输入管道检查和 TERM 忽略后升级的
协调器三轮 race 通过（23.218s），vet/diff-check 通过。
这些修改晚于正在运行 CI 的 `cc06a777`，尚未由该候选的 CI 覆盖。

随后本地增加父进程 worker 协议读取器，并用于实际 worker/会话脚本
产物回归。首轮协议单测的空输入夹具错误追加了合法 READY，导致
预期拒绝断言失败；已移除该追加，另以显式非法消息测试永久失败。
修复后协议与全部 worker 三轮 race 通过（1.031s / 27.952s），
workflow 合约 race 通过（2.549s），vet 与 diff-check 通过。
这些本地修改尚未由远端 CI 验证，也尚非完整多 worker supervisor。

候选 `03d27d3881297984aef0b93e770a46f567a22cf4` 的探针任务
`35430808031` 已成功结束（25m51s），包括最终全探针 race 回归；
同源后端任务 `35430808015` 也已成功。探针 API、jobs 和完整日志
保存在私有证据目录 `probe-ci-35430808031-terminal.p3SctDO1`，已核对
源码身份和文件 SHA-256。此前缺少运行时 rg 导致的工作流失败已在
此候选验证通过，不回写旧候选的失败结论。

镜像任务 `35430807890` 随后成功结束，发布镜像校验及晋级全部通过。
API、jobs 和日志保存在 `image-ci-35430807890-terminal.Afau0SPw`，
源码身份和 SHA-256 已核对，原 watch 正常退出。上述 CI 不覆盖本地
后续 `ef9a2b20` 至 `e9256764` 的所有者终止
检查和独立 worker 实现。多 worker 协调及恢复前取消、回收仍需完成，
本次未部署候选、未重跑故障实验，原 30 秒验收失败结论不变。

协调器新增 `internal/metricsworker.Run`：并发启动最多 16 个独立
进程组，全部 READY 通过调用方基线核验后仅调用一次故障注入钩子，
再向各 worker 下发相同原始时间戳。原起点 +30 秒与调用方 deadline
取更早约束，不能从注入回调返回时间重算预算。任一失败取消所有
进程组；函数返回前等待各直接子进程和协议读取协程。拒绝复用证据
路径，完成消息、EOF 和退出成功之后还必须调用独立证据核验钩子。
真实实验准入、owner 状态/唯一端口检查、LoadCapture/调度/退出回执
核验及恢复驱动仍需接入，当前不是可直接运行的真实故障控制器。

协调器协议/进程测试三轮 race 通过（6.249s），覆盖双 worker 就绪
屏障及统一起点、屏障不完整时取消两进程、基线拒绝、提前退出、
挂起、过期/未来起点、完成证据拒绝、重复路径，以及仅剩约 0.2 秒
时不重置故障预算。最终组合 race：协议/协调器 2.768s、全部 worker
10.037s、工作流 2.597s；vet/diff-check 通过。直接子进程的 /proc
记录在 Run 返回时已消失；这不扩大为任意后代已回收的证明。

## 动机与边界

候选 `2fd00721` 的真实故障实验未通过原 30 秒完整门限，集群已恢复，
详见 [失败与恢复记录](peer_retirement_budget_ci_20260919_cn.md)。
源码复查发现 `peerRetirementSender.onTermRetired` 原来直接丢弃发送
结果，缺少区分未发送和未确认的指标。本次补充诊断，不是延迟根因
修复，也不改变该历史失败结论；没有重跑集群实验或改变门限。

对照本地 `/root/etcd/server/etcdserver/raft.go`：etcd 根据 Raft
SoftState 更新 leader/leadership 指标。KubeBrain 的 TiKV 锁选举及
额外退任通知不是同一机制，通知成功不能当作 etcd 等价的 leader
就绪事件，也不应据此填充或重定义兼容指标。

## 新指标

仅显式启用实验性 peer retirement 的构造路径注入指标客户端。
普通构造路径不启用该协议。发送回调每次产生一个计数和一个秒数
样本；没有指标客户端时继续按原路径工作。

| Prometheus 名称 | 含义 |
| --- | --- |
| `leader_retirement_peer_result` | 回调结果计数 |
| `leader_retirement_peer_duration_seconds` | 回调耗时直方图，包含跳过的回调，不含此前生命周期 join 和本地释放 |

唯一新增标签为固定枚举 `outcome`：`missing_condition`、
`canceled_before_send`、`confirmed`、`unconfirmed`。
`confirmed` 只代表原发送逻辑在预算内接受了 HTTP 204，**不证明
新 leader 已就绪或原业务请求已完成**。取消发生在发送过程中时，
归入 `unconfirmed`，不伪装成未发送。

不输出 holder、端点、scope、所有权条件、令牌或传输错误内容。
指标返回错误不会触发重试、重新激活旧任期或跳过安全检查。
仍保留原发送预算、单端点单次尝试、mTLS 和生命周期 join 顺序。
指标需及时采集；旧 Pod 删除后不能依靠新 Pod 的计数追溯旧进程，
本次新增指标也不能补全已经结束实验的历史数据。

## 本机验证

- `go test -race -count=1 ./pkg/server ./pkg/server/service/leader -run 'Test(PeerRetirement|ScopedPostJoin|PostJoinRelease)'`：通过，38.197s / 2.317s。
- `go test -race -count=1 ./pkg/metrics/prometheus`：通过，1.093s。
- `go vet ./pkg/server ./pkg/server/service/leader ./pkg/metrics/prometheus`：通过。

新增测试覆盖四种固定结果、跳过路径不发网络请求、指标写入失败不
重试、nil 依赖安全行为及导出的名称、标签、单位和说明。本次尚无
新 CI 或集群部署结果；下一步仍需定位退任与选举各阶段的真实延迟。

## 本地条件释放的独立观测

随后补充 `leader_retirement_local_result` 和
`leader_retirement_local_duration_seconds`。仅原实验性作用域释放
路径在实际调用本地条件释放后记录，缺少条件不会伪造一次本地调用。
固定 `outcome` 为 `confirmed`、`unconfirmed`、`deadline`、`canceled`。
同时检查返回错误和调用结束时的上下文状态，取消由我们自己的清理
操作触发之前采样；不输出原始错误或所有权条件。

本地耗时不含前面的生命周期 join 和后面的 peer 通知。未增加重试，
本地预算仍为一个 RetryPeriod，原始冻结条件原样传递给 peer。
这仍不是整段退任耗时，也不补证历史实验的发送结果。

首次两项 leader 测试在后台 mock 失败后阻塞于读取缺失的 peer 回调，
已分别取栈并终止（会话 56828、39442，非通过结果）。单例进一步
确认：测试的 10ms 预算下，内存后端 nil 返回发生在约 23ms，因而
指标正确为 deadline，而初版测试错误预期 confirmed。测试现按真实
上下文核验，并在 Campaign 已退出后先断言回调存在，避免无限等待；
确定性单测单独锁定晚到成功、包裹 deadline、取消和不确定提交分类。
生产预算、提交语义及旧任期安全约束未变。

后续验证：完整 leader race 包通过（4.301s），本地释放/指标相关
三轮 race 通过（4.709s），完整 Prometheus race 包通过（1.093s），
`TestPeerRetirementCampaignPartitionToStorageRelease` race 通过
（1.404s），相关 `go vet` 通过。一次误用名称的筛选没有执行测试，
不计覆盖；已按上述真实名称补测。未触发 CI 或变更集群。

## 组合验证与 CI 启动

候选 `4aab067fcec420e6443dde2dfac3c6c45ad55ac9` 组合 race 检查
通过：server 33.132s、leader 2.307s、Prometheus 1.052s（会话
93458；`retirement-observability-validation.yw2Rq2vc` 保存日志和摘要）。
推送会话 14029 退出 0，origin/dbaas 已更新，自动触发以下任务：

- 镜像：[35427286402](https://github.com/fivetime/kubebrain/actions/runs/35427286402)。
- 后端协议：[35427286386](https://github.com/fivetime/kubebrain/actions/runs/35427286386)。
- 探针回归：[35427286439](https://github.com/fivetime/kubebrain/actions/runs/35427286439)。

初始 API 记录均绑定该候选，尚无通过结论；证据目录
`retirement-observability-ci.fjZ3pXP3`。没有重复手动触发、部署或
故障注入，原集群基线保持。本节是启动记录，不是 CI 终态。

## 完整本地释放回退链路的覆盖补充

源码审查发现原 `TestPeerRetirementCampaignPartitionToStorageRelease`
包装锁仅实现条件快照接口，没有暴露 `RetiredOwnershipReleaser`。
因此它能验证 peer 的真实条件 CAS，但没有经过显式作用域配置下的
“先本地释放，再通知 peer”分支。原测试保留，不追溯扩大其覆盖。

新增 `TestPeerRetirementCampaignLocalTimeoutThenHealthyPeerRelease`：
旧节点包装锁暴露真实作用域，本地释放等待上下文截止，健康 helper
仍连接同一内存存储并通过真实 mTLS handler 执行条件 CAS。测试要求
本地调用仅一次、预算不超过 RetryPeriod、结束后才通知 peer；同时
保留生命周期 join、清理、不可重新激活、持有者清空等断言。

新旧两条链路连续五轮 race 通过（3.936s）；全部
`TestPeerRetirement*` race 通过（38.637s），`go vet ./pkg/server`
通过。该测试只补覆盖，不改变产品行为，不模拟真实 TiKV 的延迟，
也不能证明集群 30 秒门限已修复。

三个候选 CI 已由排队转为运行；当前查到镜像在 Go 安全检查环境
准备，后端和探针在 Go 环境准备，没有成功/失败终态。此处新增的
测试尚未推送，避免取消正在运行的候选 `4aab067f` 工作流；不将它
算作该候选 CI 的覆盖。

## 按实集群选举参数的本机计时回归

`go list -m` 确认实际使用 client-go `v0.36.2`。其 `renew` 每轮以
RetryPeriod 调度、在 RenewDeadline 内轮询；后端锁的 `genContext`
继承父上下文。KubeBrain 在 elector 结束后仍须等待生命周期 join、
清理旧租约，再执行本地条件释放及 peer 通知。因此 RenewDeadline
不是端到端 failover 上限，不能把剩余所有时间都归因于某一次 RPC。

新增 `TestPeerRetirementCampaignProductionTimers` 使用与专用集群
一致的 LeaseDuration=30s、RenewDeadline=25s、RetryPeriod=500ms，
保留旧节点本地释放超时、健康 peer mTLS + 条件 CAS 的完整路径。
从设置隔离到通知回调结束采用不可重置的 30 秒上限；没有改变既有
集群门限。本机 race 通过（会话 86354，包 27.194s），回调耗时
**26.041285651s**。原两条快速路径再跑三轮通过（2.791s），vet 通过。

这只说明该状态机在可控内存存储、快速生命周期清理条件下不必然
耗满 30 秒；没有测量真实 TiKV、下一任完整初始化或故障后诊断采集，
不能据此宣称实集群应当通过，也不能把余下差值直接归因于存储。
后续采集必须分别绑定退任、local release、peer notification、新任
初始化及请求转发时间，避免把状态观测延迟当成实际选举时刻。

候选 `4aab067f` 的三项 CI 最新仍在运行：镜像模块扫描、后端 race、
etcd 服务与 Watch 回归。新增计时测试仍仅本地提交，不取消当前 CI。

## 故障期间采集的现有工具边界

已审查 `capture-rollout-tls-metrics.sh`：它要求 stable rollout phase、
每个目标容器 Ready，且通过 probe Pod 内 curl 仅提供服务端 CA。
这与当前故障场景不兼容：旧 leader 可因后端隔离不 Ready，临时
diagnostic info listener 又要求客户端证书。不能原样调用后将失败
或缺少计数解释为“没有发送退任通知”，也不能删掉既有安全检查。

`info-diagnostic-probe` 已提供 SPKI 绑定、认证连接和独立匿名连接
拒绝证明，但目前采集的是 goroutine 栈，不是指标。
`pod-log-capture` 可在固定身份下保留不重连的日志流，但日志不含
新增的 Prometheus 计数，不能替代指标采集。

下一轮真实故障之前需要单独完成故障态指标采集能力：保留 namespace、
StatefulSet、Pod UID/containerID/imageID 前后身份核验，使用验证名称
和 SPKI 的 mTLS，明确不以 Ready 为前提、也不声称 Ready；限制响应
大小和总时长、输出不覆盖旧证据。采集应在恢复滚动之前结束并持久化，
缺失样本不可补零，跨进程计数不可相减。这些是尚未实现的准入要求，
不是已有采集结果；不为此重新启动旧实验或关闭 TLS 校验。

CI 只读复查证据保存于 `retirement-observability-ci.fjZ3pXP3/`
`progress.CX7oO7Se`，三个 run/job 仍实际运行；没有重复 dispatch。

## 受保护的原始指标采集模式已实现（尚未接入集群实验）

`info-diagnostic-probe --mode protected-metrics --metrics-output <新文件>`
复用现有固定服务端名称/SPKI、mTLS 及独立匿名通道的真实 TLS 拒绝
校验，访问 `/ping` 与 `/metrics`，不访问 `/ready` 或 pprof。
原 protected、protected-stack、disabled 模式行为及栈输出字段保留。
禁止同时指定 stack-output；其他模式不得指定 metrics-output。

受现有 25 秒总上下文和 8 MiB 响应上限约束，不跟随重定向；响应
必须非空且以换行结束，文件以 0600 排他创建，已有文件不覆盖。
新摘要字段为 `metrics_bytes`、`metrics_sha256`，不冒充 stack 字段。
这是**原始传输捕获**，未解析或证明 Prometheus 语义；摘要明确
`metric_semantics_proven=false`、`pod_identity_proven=false`、
`fault_acceptance_proven=false`、`readiness_checked=false`。
不能仅凭换行就宣称响应是有效指标，更不能把缺失计数补零。

完整探针包 race 通过（3.477s），vet 与 diff-check 通过。新增测试
覆盖 TLS 1.2/1.3、错误 pin/名称、未保护监听、重定向、空/截断/
超大响应、文件不覆盖、独立通道、模式参数隔离及输出权限/声明。
仍须补齐调用方的 Pod/进程身份前后核验、有限采样调度、指标语义及
同进程差值校验，才能接入真实故障实验；本次没有部署或集群采集。
候选 `4aab067f` 三项 CI 复查仍运行，本机新增工具改动不属于该 CI。

### 原始采集的语法校验补强

随后采集模式改为调用已有依赖中的 Prometheus 文本解析器，保存前
要求格式可解析且至少含一个样本；拒绝 HTML、仅注释/类型声明、
非法数值、非法标签和冲突 TYPE 声明。解析错误统一返回固定描述，
不泄露指标名称或值。解析后重新检查总上下文是否已经到期。
新增 `metrics_text_syntax_validated=true`，仍明确
`metric_semantics_proven=false`：NaN/Inf、目标类型/标签、缺失 family
以及跨进程重置需要专门语义验证，不因语法检查通过而获准做差值。
最终完整探针包三轮 race 通过（9.120s），vet/diff-check 通过，未部署。

候选 `4aab067f` 的后端协议 CI **35427286386 已成功结束**，精确 SHA、
run/job API 和完整日志已保存在 `retirement-observability-ci.fjZ3pXP3/`
`backend-terminal.CuWeti5w` 并生成摘要（会话 12356 退出 0）。镜像和
探针 CI 最新仍运行，不能称三项全通过；本机未推送工具改动不在这次
后端 CI 的验证范围内。

### 后续终态与进程清理回归

镜像 CI `35427286402` 已成功，API 核对源码为
`4aab067fcec420e6443dde2dfac3c6c45ad55ac9`。run/job API、完整日志和
已校验摘要保存在私有证据目录 `retirement-observability-ci.fjZ3pXP3/`
`image-terminal.IhjsW4hL`。这是候选源码的构建结果，不覆盖之后的本机工具改动。

新增 metrics session 外部取消回归，与 stack session 共用断言：
必须已进入探针阶段，外层返回 124，两个转发进程与阻塞探针均已退出，
没有 COMPLETE 或事后身份采样。哈希校验截止测试也记录并检查子进程 PID，
不能仅凭调用返回超时判定清理完成。

stack/metrics 外部取消、匿名通道重建取消、哈希截止四项三轮 race
通过（32.651s）；metrics 成功路径单独通过（4.737s），vet 和改动文件
diff-check 通过。未修改集群、未重跑故障验收；指标语义和同进程差值
校验仍待完成，不能据此宣称原 30 秒门限或整体生产就绪通过。

### 取消和写入失败边界回归

#### 离线采集目录加载与命令

#### 生产导出标签联测发现并修复不匹配

#### 退休阶段累计耗时差值

#### 候选 3f64735d 探针 CI 失败：采集测试缺少运行时工具

worker 的 READY 后输入等待改为单秒分段读取，持续复查 owner；原固定
60 秒总期限不因读取分片而重置，完整记录到达后再检查期限。新增保持
stdin 打开时 HOLD/终态退出、跨一次 read 超时的分片输入成功，以及
“read 成功但已到总期限”拒绝的测试。失败不输出 CAPTURED，仍检查子
进程回收。首轮出现 baseline-failed 合成用例达到 8 秒上下文上限；检查
发现合成库仅 TERM 后无限 wait，已改为有界 TERM/KILL/join。尚不能单凭
后续通过证明该超时的唯一原因，原失败保留。最终 worker 全组三轮 race
通过（27.706s），vet/bash/diff-check 通过。探针与镜像 CI 仍运行，后端
此前成功；新修改未推送或部署，不视为完成真实故障验收。

worker 增加真实会话库集成测试：仅复用 Kubernetes/转发/探针夹具，
启动实际独立入口与 session 库；父进程收到 READY 后先由 LoadCapture
核验基线，再交付原始故障起点，验证 CAPTURED 的产物、调度摘要清单和
完成标记、worker 退出码，并计算合成事件 0→1 的差值。退出后 auth、
原匿名及重建匿名三个转发 PID 均已不存在。最终 worker 三轮 race 通过
（16.885s），vet/diff-check 通过。这不是实际 TLS/Kubernetes/TiKV 故障
实验，控制进程的多 worker 协调与恢复集成仍待完成。

候选 `03d27d3881297984aef0b93e770a46f567a22cf4` 的后端协议 CI
`35430808015` 已成功。run/job API、完整日志和已验证摘要保存于
`backend-ci-35430808015-terminal.lqvidOKf`。探针 `35430808031` 与镜像
`35430807890` 仍运行，不能称三项 CI 全绿；本机后续 owner/worker 改动
不在该候选的验证范围。本轮未推送或部署。

新增独立进程入口 `protected-metrics-worker.sh`：自有转发与退出清理，
完成基线后发送 READY，再从 stdin 接收一个有长度/时间限制的故障起点，
执行一次原截止时间内的定时采集，输出 CAPTURED 并保存退出码。工具
自身必须纳入准入摘要，默认使用独立 18586/18587 端口；未启动真实进程
连接集群。合成库测试覆盖成功、基线/采集失败、清单漏项、EOF、非法/
未来起点、TERM、基线后 HOLD，并检查子进程已回收、失败不发成功消息。
首轮 race 发现测试在 Wait 前读取 stderr 缓冲区，改为仅在进程汇合后
读取；修复后 worker 三轮 race 通过（2.194s），build race 通过（2.611s），
vet、shell 语法及 diff-check 通过。CI 已纳入新入口路径和测试表达式。
这些仅验证 worker 协议，未完成外层多进程故障注入/汇合/恢复编排，也
不是原 30 秒门限通过的证据。候选三项 CI 仍运行，后续改动未推送或部署。

新增 owner 生命周期复查：准备完成后若外层写入 HOLD/终态或移除
deployment-claimed，后续 capture/rearm、定时等待每轮和完成标记写入前
都会拒绝继续。准备后终止时不调用探针；探针期间终止时可能保留原始
文件，但不写 COMPLETE、不重建匿名通道。测试仍检查原有转发子进程
由 owner 的清理路径回收。该复查不是并发恢复锁，外层仍必须先取消、
join 控制器后再恢复资源。
stack/metrics 的准备后/探针中终止，以及调度等待中 HOLD/终态的三轮
race 通过（51.211s），完整受保护会话/调度 race 通过（143.816s），
相关 vet/bash/diff-check 通过。候选 `03d27d38`
三项 CI 仍运行，本次后续改动不在其验证范围，未推送或部署。

后续镜像 CI `35429292124` 成功终态，源码确认仍为
`3f64735dbf29faeb3b226b6b195850680083a1dc`；run/job API、完整日志及
已验证摘要保存在 `image-ci-35429292124-terminal.1P8wCLHL`。同源探针
CI 失败仍有效，该镜像不获实验准入。两项旧任务均已结束后才推送后续修复。

新增单次定时入口 `stack_session_capture_metrics_at`：独立控制器 prepare
后传入原故障起点和小于 30 秒的偏移，等待分段检查同一截止时间。已过
目标偏移但未过期时立即采样，以实际时间为准；拒绝重复安排、更换起点、
未来/过期时钟，失败不重试。调度输入、结果路径和采集清单有独立摘要，
最终预算检查后才产生非验收性质的完成标记。外层并行进程启动/起点交付/
汇合恢复仍待实现，不据此开展真实实验。
首两轮模拟时钟测试因 Bash 动态作用域遮蔽同名 now 变量失败，改用独立
test_clock_ns 后三轮通过（9.505s）。取消测试记录每个 sleep 子进程，
断言退出且未采集。完整会话+调度集成 race 通过（129.961s），含真实
会话脚本到离线加载器的产物核验；build race 通过（2.613s），vet、shell
语法和 diff-check 通过。CI 路径和测试表达式已包含新增调度测试。

为后续独立指标控制器补上端口隔离：受保护会话可显式指定不同的 info/
anonymous 本地端口（1024–65535，无前导零），默认 18584/18585 不变。
prepare 后 capture/rearm 检查端口仍与准备时相同；所有监听检查、日志、
转发和探针 URL 使用该端口对。完整会话 race 通过（118.070s），随后新增
metrics 自定义/同端口/越界/漂移四场景三轮 race 通过（15.866s），相关
vet、shell 语法及 diff-check 通过。首次补丁因函数内已有注释未匹配而未
应用，核对当前文件后重新应用；无部分修改或集群操作。
这只是独立采样进程的前置条件，不宣称已实现并行采样调度。不同控制器
必须自行 prepare/拥有子进程，不能继承已准备 shell 的 job 表；外层仍
须固定故障起点并在恢复前收齐结果。旧镜像 CI 仍在运行，未取消/重发。

CI `35429292114` 已以 failure 结束，失败步骤为 protected capture evidence
and retirement deltas。日志明确为 `/usr/bin/timeout: failed to run command
‘rg’: No such file or directory`。离线校验库先通过，随后采集脚本在输入
绑定检查提前失败；部分预期失败场景因此未执行到目标分支，不能把它们的
PASS 解释为有效覆盖。后续整套探针回归步骤未完成，不算候选 CI 通过。
run/job API、完整日志与已验证摘要保存在私有目录
`probe-ci-35429292114-failed.ryCekcna`，原失败保留。

修复仅在仓库中进行，不登录或维护 Runner 主机：运行时精确行匹配改为
`grep -Fxq`，转发就绪日志用固定字符串 `grep -Fq`；保留 stack_session_run
原截止预算。测试夹具在 exercise 之前检查实际依赖，缺失即退出 127，
避免反例测试把环境错误当作预期业务拒绝。CI 也预检基础命令，工作流契约
测试锁定预检；另以失败 rg 替身验证成功采集不再调用 ripgrep。
build race 通过（2.737s），相关 vet/bash 语法检查通过；完整受保护采集
race 通过（113.678s），包括不依赖 rg、取消清理及哈希截止。
镜像 CI `35429292124` 仍运行，未取消或重发。本地修复不是 CI 成功证据。

补充整包验证（当前本地源码，非旧候选 CI）：server、leader、Prometheus
三个完整包 race 通过，分别 79.653s / 4.187s / 1.095s，不仅是新增测试的
筛选运行。工作流契约测试现已锁定退休差值命令、受保护采集脚本及身份谓词
的 push 路径，以及库/CLI/session 的 uncached race 执行命令；build 包 race
通过（2.720s），vet/diff-check 通过。候选 `3f64735d` 的两项 CI 仍运行，
未取消、重发或部署。本地后续修复仍需要新的同源构建验证。

离线工具增加可选 `--duration`：选定 local/peer 和 outcome 后，同时要求
直方图与结果 counter 对应，输出完成操作的 count/seconds 差值。不输出
总 failover latency，不覆盖 lifecycle join，也不能确定单个事件发生时刻。
累计 sum 的浮点舍入限制仍存在。counter 与 histogram 分两次写入，因此
抓取恰好发生在两次更新之间时可能不一致；校验器拒绝该样本，不自行补齐。

检查固定阶段类型、准入 cluster/outcome、有限非负 sum、整数 count、
完整 sum/count/+Inf 桶、桶累计单调与总数、跨样本桶边界一致及无回退。
零 count 不允许非零 sum，零增量不允许新增 sum。Prometheus 文本解析器
会合并直方图组件，为避免漏检重复 sum/count，另去除目标 TYPE 后按独立
untyped 组件解析、逐项查重；数值等价的重复桶边界同样拒绝。

真实产品适配器 `/metrics` 导出联测涵盖空 histogram 和一次 0.25 秒
观测；CLI 入口子进程涵盖三个观测增加 0.75 秒及缺失 histogram 时不输出
成功 JSON。库/CLI/探针三轮 race 分别通过（4.812s/8.315s/12.553s），
相关 vet/diff-check 通过。未部署，旧候选两个 CI 仍运行；新增代码不在其
验证范围，完整故障实验仍待新源码 CI、镜像核验及新准入。

启动顺序补验：在真实 `NewServerWithPeerRetirement` 构造测试的后台
PrevalidateLeadershipRevision 入口，复制指标注册快照，再进入原阻塞
夹具；核对八个零 counter 与八个各一次的 histogram 注册均已完成。
不只在构造函数返回后检查，避免漏掉后台线程先于初始化开始的回归。
不安全 TLS 的五类构造失败路径改用严格 metrics mock，禁止任何注册。
首次测试包装器只嵌入 Backend 基础接口，遗漏可选的预校验方法，导致
编译失败；改为嵌入原具体测试后端后重新验证，未把失败记为通过。
构造顺序/初始化/TLS 拒绝三项三轮 race 及 vet 已通过。本轮只改测试，
未部署；候选两个 CI 仍在既有 run 中进行，不重发构建。

`cmd/option/option.go` 实际以 `cluster=ClusterName` 构造 Prometheus
适配器。此前仅允许 outcome 标签的解析器会拒绝真实产品序列；已有合成
测试未覆盖该差异。因此候选 `3f64735d` 即使 CI 全绿，也不能作为真实
退休指标诊断工具准入，需包含零基线及本次 cluster 绑定修复的新版本。

新增 `ParseForCluster`，生产采集加载器和 CLI 必须接收独立准入的 cluster，
仅允许匹配的 cluster/outcome 两个标签。错误或缺失 cluster、额外标签、
重复标签均拒绝；空 cluster 的旧 Parse 只用于不带全局标签的单元夹具。
cluster 同时进入 Sample 进程身份比较，不能跨 cluster 做差值。

新增独立子进程测试使用产品 Prometheus 适配器真实 `/metrics` HTTP 输出，
验证八个零序列与一次 peer confirmed 后的 0→1 差值，而非手写导出文本。
该测试验证导出/解析联通，不声称启动了真实服务器或产生真实退休事件。
最终库/CLI 三轮 race 通过（4.507s/5.064s），会话产物加载成功路径 race
通过（4.700s），相关 vet/diff-check 通过；此前完整探针三轮 race 通过
（12.468s）。旧候选两个 CI 仍运行，分别在 etcd service/Watch 检查及
镜像构建；本次修复未推送、未部署，不取消或重复触发原构建。

后续补充 CLI 真正入口的子进程测试：完整的合成采集包前后计数 2→5，
核对退出码 0 和唯一成功 JSON（delta=3，三项 readiness/latency/acceptance
结论仍为 false）；基线缺失、回退、进程变化、倒序、重叠、篡改、不完整、
错误准入均核对退出码 1 且 stdout 为空。三轮 race 通过（4.885s），vet
通过。这覆盖命令入口，不把合成产物称为真实集群证据。

另修复首次退休事件前无法取得计数基线的问题：显式 opt-in 的
`NewServerWithPeerRetirement` 在全部配置验证后、启动 campaign 前注册
local/peer 共八个有限 outcome 的零值 counter；可选 HistogramRegistrar
仅注册空 histogram，绝不 Observe(0)。重复初始化仅 Add(0)，不重置已有
计数或发明事件。普通 NewServer 路径不变，指标注册错误不改变退休协议。
离线工具仍拒绝缺失序列：旧镜像或采集缺项不能追溯补零。

本地 etcd 对照：`/root/etcd/server/etcdserver/metrics.go` 的 init 注册
leaderChanges 等 collector；这里采用启动时可观察的基线原则，但退休
指标是 KubeBrain 自有协议诊断，不等于 etcd Raft 指标语义。
PeerRetirement/退休服务器构造 race 通过（59.389s），Prometheus 包三轮
race 通过（1.161s），相关 vet 通过。当前运行的候选 `3f64735d` CI
`35429292114` / `35429292124` 不包含这些后续改动；暂不推送以免取消
正在运行的任务。未部署，仍须新源码 CI 与镜像验证后才可用于实验。

新增 `retirementmetrics.LoadCapture` 与
`hack/production/cmd/retirement-metrics-delta`（使用方式和限制见其 README）。
加载器核对 COMPLETE、八个固定文件、原绝对路径清单及哈希；使用独立准入
传入的 namespace/STS/Pod UID 和规范化 spec 摘要检查对象，并比较采集前后
完整 Pod spec、地址、容器进程及镜像身份。允许 readiness 改变，不允许重启。
逐阶段记录必须按序成功，探针摘要时间必须位于 protected-probe 阶段内；
跨样本还要求身份一致、区间不重叠。命令只输出单个指定 outcome 的计数差值，
不计算故障门限、直方图延迟或 successor readiness。

首轮和随后三轮回归均暴露静态符号链接未被拒绝：仅向 os.Root.OpenFile
传入 O_NOFOLLOW 不足以实现所需契约。已增加 Root.Lstat 普通文件检查和
打开后 SameFile 比对；此前失败不计为通过。修复后校验库/新命令/真实探针
三轮 race 通过（分别 1.201s / 1.034s / 12.379s），vet 通过。
会话测试增加实际脚本产物到加载器的成功路径；完整受保护会话组（包含外部
取消、重建取消及哈希截止）race 通过（110.668s），工作流 race 测试通过
（2.616s）。新命令的 CLI 目前覆盖失败参数，成功加载由库与会话集成测试
覆盖，不宣称 CLI 已做真实实验端到端验证。

探针 CI 已加入对应路径触发和显式回归步骤。尚未执行新的集群故障实验；
完整文件包的哈希一致性不等于来源认证，原始准入、工具/镜像来源与故障时钟
仍须由实验驱动核验。旧指标首次出现仍为未知差值，不得补零或判通过。

新增离线校验库 `hack/production/internal/retirementmetrics`：仅解释
local/peer result counter，校验类型、唯一 outcome 标签、阶段对应的
有限 outcome 集、有限非负整数及 float64 精确计数范围。拒绝重复序列
和显式样本时间戳。缺失序列保持缺失，首次出现不能假定零基线。
差值接口要求前后非空且一致的 Pod UID/container ID/startedAt/restartCount，
拒绝计数回退。身份须由采集器的 Kubernetes 核验结果提供，而非指标标签。
三轮 race 和 vet 通过。该库尚未接入真实采样驱动，未校验 histogram、
采样顺序和文件证据绑定；不改变原始探针的 `metric_semantics_proven=false`，
不把局部计数差值当作切换成功或完整延迟解释。

后续补充 `NewSample` / `SampleDelta`：绑定探针真实 JSON 摘要的
字节数、SHA-256、显式能力标志和开始/结束时间；拒绝缺项、重复字段、
大小写别名、多个 JSON 文档、超过 25 秒或倒序的采集区间。样本内部
不暴露可修改的计数 map；前后区间必须严格不重叠。TLS 1.2/1.3 测试
分别执行两次真实探针采集，将实际文件和摘要送入该接口，核验零差值。
进程身份仍为测试夹具，不能称为真实 Kubernetes 验证。完整三轮 race
和 vet 通过后提交；尚未实现 COMPLETE/manifest/Pod 文件的离线加载器，
因此不能直接以这些 API 作为真实故障采样准入。哈希只约束文件配对，
不是来源认证；样本间计数变化也不提供事件精确时间或耗时。

新增故障模式测试验证 metrics 响应体传输中取消/截止；明确断言已经
进入响应体阶段，并检查对应 context 错误，避免把 TLS 阶段失败当作
响应取消覆盖。另验证时序输出失败、目标目录不存在、最终摘要输出
失败。最终探针包三轮 race 通过（12.556s），vet/diff-check 通过。

重要契约：最终摘要写出失败时，已完成的原始文件可能保留，作为
不完整采集证据，不能仅凭文件存在判定成功。调用方须同时核对成功
退出、有效摘要、字节数及哈希，再独立完成进程身份和指标语义验证。
取消或传输失败不保存响应体，未生成成功摘要；没有重试或覆盖旧文件。

此次仅新增本机回归测试，未改生产行为或集群。CI 复查：原候选探针
任务正在 endpoint transport/credential lifecycle 检查，镜像仍在构建
推送；后端已成功，另外两项尚无终态。不重复触发正在运行的任务。
