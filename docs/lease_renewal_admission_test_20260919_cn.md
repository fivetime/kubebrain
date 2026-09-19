# 租约续约任期回归的同步修正

## CI 失败与复现

候选 `69bc933407164b0c77c1c481491634bfdb6e04b9` 的探针 CI
`35433943696` 在 etcd/Watch 步骤失败（11m56s），后续探针检查未执行。
私有证据目录为 `probe-ci-35433943696-failed.iTutC8Ht`，已核对
API 中的源码 SHA、失败终态及 API/jobs/完整日志的 SHA-256。
该候选不准入集群实验，镜像任务单独成功也不能补足失败的探针门禁。

失败测试：`TestLeaseKeepAliveRejectsStaleOrChangedEpochWhileWaitingForRenewal/changed_epoch`。
预期为带 follower 路由信息的 Unavailable，实际为
`etcdserver: lease state is reloading`（同为 Unavailable）。未修改版本
的本地 100 次 race 重复测试复现相同失败，测试耗时 6.558s。

原测试在 peer epoch 查询回调内先关闭 `routed` 通知，再读取 epoch；
主测试收到通知后立即切换 epoch/freshness。这既不能保证本次查询
返回旧 epoch，也不能保证请求已越过后续 auth/readiness 路由检查。
请求可在尚未进入续约锁等待时看到新 epoch 与旧 leaseReadyEpoch，
合法进入 reload fence，因此原通知不能证明测试标题要求的竞争位置。

## 修正范围与边界

只修改测试，不改产品实现、错误文案、重试或验收门限。测试继续持有
checkpoint 屏障，并等待运行时栈确认：同一 lease manager 的
`lockLeaseCheckpointContext` 经 semaphore Acquire 阻塞，且调用链
包含 `leaseKeepAlive`。之后才切换 epoch/freshness、释放屏障。
保留原精确错误、没有成功响应、私有 lease deadline 未延长的断言。
新增可取消请求和 join 清理，观察与返回等待各限 3 秒。

第一版栈条件要求包含测试调用帧，验证失败：public LeaseKeepAlive
会启动内部 goroutine，实际锁等待栈不含该帧。该版单次 6.275s 失败，
100 次组触及 2m 超时（120.162s），大范围租约组也失败（88.411s），
均不计为通过。修正版用当前 manager 的接收者地址绑定对应栈；快照
从 64KiB 扩至最多 4MiB，截断时拒绝接受。该观察只用于本机 Go 测试，
不是分布式身份或真实故障时间证据。

对照 `/root/etcd/server/lease/lessor.go` 的 `Renew`：非 primary 及
等待过期 lease 时发生 demotion 均返回 ErrNotPrimary。当前回归保持
“失去任期后不能成功续约”的安全约束；不把 KubeBrain 的 TiKV 任期
锁或 reload fence 声称为与 etcd Raft 生命周期完全等价。

## 验证进度

修正版单次 race 通过（1.243s）；100 次 race 重复通过（7.939s），
vet/diff-check 通过。与 CI 相同筛选范围的
`(Lease|Revoke|Expiry|Checkpoint|Attachment)` race 回归通过（82.521s）。
完整日志、退出码 0 及核验过的哈希保存在私有目录
`lease-renewal-sync-check.4pHtRMkQ`。尚未推送修复、未重跑 CI、
未部署或重跑集群实验；远端原镜像任务仍由原监控跟踪。
