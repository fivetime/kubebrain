# 退任通知可观测性补充

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

### 取消和写入失败边界回归

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
