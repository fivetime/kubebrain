# 选主配置的持久租期与重试边界

2026-09-18，基于 `4479078a93188b097af98e897b9b57094a2550e1`。

此前 `leader.Config.Validate` 只检查 `retry < renew < lease`。
实际使用的 client-go v0.36.2 在 `tools/leaderelection/leaderelection.go`
中以 `int(LeaseDuration / time.Second)` 写入 `LeaseDurationSeconds`，并按该
整数秒记录判断其他 holder 的租期。因此 1.5 秒 lease、1.2 秒 renew、
100 毫秒 retry 虽能通过原校验，却只有 1 秒持久租期，无法维持本机
自我隔离期限严格短于候选者等待期限的设计约束。这是配置安全边界缺口，
不是已经观察到实际集群双主或丢失数据。

此外，client-go 构造选举器时要求 `renew > retry * JitterFactor`，该版本
的因子为 1.2。原校验会放过随后被选举器拒绝的配置，直到 Campaign 内才
记录错误并退出。应在启动选项校验阶段报错，而不是让服务启动后失去选举循环。

修复检查向下取整后的持久租期严格大于续约期限，并复用 client-go 的
`JitterFactor` 检查重试边界及计算溢出。未改变默认值，不把安全的小数租期
一概拒绝，也不调整选主算法、RPC 重试或故障验收时限。命令行帮助同步说明
这两个约束。现有测试集群的 30 秒 lease、25 秒 renew、500 毫秒 retry 不变。

## 验证

新增表格测试在原代码中复现五个预期失败：持久租期为零、短于或等于 renew，
以及 renew 低于或等于 jitter 边界。正例包括默认配置、安全小数租期、
刚超过 jitter 边界和现有测试集群配置。另覆盖持久租期下沿及 jitter
时长计算溢出。命令行选项测试确认通过真实 `KubeBrainOption.Validate` 拒绝，
不依赖单独调用 leader 包校验。最终 leader 与 option 完整 race 各重复五次
通过（8.559 秒、1.372 秒），build 完整 race 通过（2.515 秒）；三个包的
vet 和 diff 检查通过。CI 新增 option 包完整 race/vet 及对应路径触发，
工作流契约测试防止该门禁遗漏。尚待本次源码的独立 CI 与镜像核验。

这些是本地配置和选举包回归，不是实际故障转移或生产就绪证明。
该修复不缩短现有 30 秒持久租期，故不能关闭原 30 秒端到端验收缺口。
对照 `/root/etcd` 的 `5cd9f4ee13801e18825d661e5005ae599460bc3a`，
其 `server/lease/lessor.go` 中过期 Renew 仍等待撤销或退位；不能用伪造
撤销结果替代这里仍需实现和验证的安全接棒。
