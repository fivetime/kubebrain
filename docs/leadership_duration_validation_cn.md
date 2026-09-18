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
工作流契约测试防止该门禁遗漏。同源码 CI 与镜像核验结果见下文。

这些是本地配置和选举包回归，不是实际故障转移或生产就绪证明。
该修复不缩短现有 30 秒持久租期，故不能关闭原 30 秒端到端验收缺口。
对照 `/root/etcd` 的 `5cd9f4ee13801e18825d661e5005ae599460bc3a`，
其 `server/lease/lessor.go` 中过期 Renew 仍等待撤销或退位；不能用伪造
撤销结果替代这里仍需实现和验证的安全接棒。

## 发布证据（2026-09-18）

提交 `f0b50b9a50188913dbb0cdb7b5ff4129e03ba479` 的
[回归 CI](https://github.com/fivetime/kubebrain/actions/runs/35306616402) 和
[镜像 CI](https://github.com/fivetime/kubebrain/actions/runs/35306616305)
均在 attempt 1 成功。完整日志确认新的配置边界、命令行准入，以及既有
初始化、释放和过期等待退位回归全部 PASS。另在该源码本地执行后端选主
与 server 包完整 race，通过（1.151 秒、2.221 秒）；这不冒充真实后端 CI。

独立镜像核验退出 0，确认源码、版本 `0.0.0-dbaas-f0b50b9a5018`、
Go 1.26.8、TiKV 类型、非 root 配置、OCI 标签，以及客户端 fork 和 gRPC
依赖。核验时发布标签与不可变索引一致：

- 索引：`sha256:cb5fb2b5dd9df8145b4adfa121cecd5adc79bbc066fb5ec8c19c4e9de5177e7d`
- amd64：`sha256:03cca8952d8468cf15c2d19183eaebcaa21272d2ee04102918fe0b2270b64fa7`
- arm64：`sha256:04d4ea8ffe4af9b4e9264cdaa01558a4f63c345ec81215dc2bc4008a76e5b14a`

仅实际执行了 amd64 二进制，arm64 仅核验平台清单身份。临时容器和提取
二进制已清理，并再次确认不存在。证据：
`/root/.local/state/kubebrain/release-f0b50b9a.1xiWLs9s/audit.xsygisJv`。
本轮没有部署候选镜像或修改集群；发布核验不关闭真实 30 秒故障验收及总体
生产就绪差距。
