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
