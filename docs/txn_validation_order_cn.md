# 可串行化只读 Txn 请求校验顺序

对照本机 `/root/etcd` 提交 `5cd9f4ee13801e18825d661e5005ae599460bc3a`
的 `server/etcdserver/api/v3rpc/key.go`：`Txn` 在调用实际存储处理前先
检查操作数、compare key 和两个分支的请求。

KubeBrain 的可串行化只读事务入口原先先读取 leadership/checkpoint，
随后才在内部单次执行入口校验请求。现将这一读路径的校验提前到首次
peers/backend 访问之前；内部入口继续保留校验。写事务不进入这个提前
返回分支，继续遵守原有外层 quota 的错误优先级。

`TestSerializableTxnValidationBeforeBackend` 覆盖空 compare key、成功及
失败分支的空 range key、非法排序、compare 错误先于排序错误、操作数
错误先于空 key。测试同时调用公共入口、单次执行入口和 follower proxy
入口，并核对响应为空及 gRPC 错误状态码、消息。

测试刻意不提供 peers/backend，作为“不得访问依赖”的哨兵。修复前
`empty-compare` 在公共入口访问 nil peers 而失败，定位于原 `kv.go:1001`；
这证明校验顺序问题，不表示已在真实服务中观察到 nil peers 崩溃。

该修复不改变有效请求的检查点回退策略，不证明原 30 秒真实故障验收通过。

本地回归命令如下，通过（10.920 秒）：

```sh
go test -count=1 -timeout=3m ./pkg/server/etcd -run 'TestSerializableTxnValidationBeforeBackend|Test.*Serializable.*Txn|Test.*Txn.*Serializable|TestQuota|TestTxnRejectsTooMany'
```

该组包含有效只读事务的快照/检查点回退测试、写事务配额优先级测试及
操作数上限测试；不是全包或真实集群验收结果。

新增校验用例连续 20 次通过（0.059 秒），`go vet ./pkg/server/etcd` 和
`git diff --check` 通过。相同回归范围的竞态检测已启动，结果待收取。
