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
`git diff --check` 通过。相同回归范围的竞态检测也通过（41.468 秒）：

```sh
go test -race -count=1 -timeout=3m ./pkg/server/etcd -run 'TestSerializableTxnValidationBeforeBackend|Test.*Serializable.*Txn|Test.*Txn.*Serializable|TestQuota|TestTxnRejectsTooMany'
```

## 嵌套事务：完整请求校验先于重复 key

同一上游文件的 `checkTxnRequest` 只递归检查请求和操作数；公共 `Txn`
在该检查全部成功后才调用 `checkIntervals`。KubeBrain 原先在递归校验
子事务时就检查其 key 重叠，导致嵌套重复 key 遮蔽后续分支的非法请求。
新增用例在修复前复现：父事务 failure 分支含空 key 时，预期
`etcdserver: key is not provided`，实际却为
`etcdserver: duplicate key given in txn request`；多层嵌套亦可复现。

现将递归结构/操作数校验与区间检查拆成两个阶段。只有整棵请求树通过
第一阶段，才递归检查成功、失败分支的 key 重叠；原区间规则和配额入口
保持不变，也避免为每个祖先重复检查同一子树的区间。

`TestTxnValidatesWholeTreeBeforeNestedDuplicateKeys` 检查空 key、非法排序、
空操作、嵌套操作数超限以及“合法结构仍拒绝重复 key”五类情况，分别放在
父事务 failure、后续 sibling、多层嵌套外层 failure 三种位置，共 15 个
场景；同时验证校验函数和实际 `Txn` 处理入口的状态码、消息及空响应。

修正后 `go test -count=1 -timeout=3m ./pkg/server/etcd -run
'TestTxn|TestSerializableTxnValidationBeforeBackend|TestQuota'` 通过（2.608 秒），
`go vet ./pkg/server/etcd` 和 `git diff --check` 通过。

包含两项 Txn 校验顺序修复的 etcd 包全包竞态回归已完成：
`go test -json -race -count=1 -timeout=12m ./pkg/server/etcd` 退出 0，
JSON 包级终态 `pass`，耗时 354.095 秒。日志、退出码及启动时的源码差异
保存在私有目录 `txn-validation-full-race.YLJTIK7d`；启动时清除了
`KUBEBRAIN_*` 环境变量，未以该回归替代需要显式连接配置的真实 TiKV/PD
实验。此结果不代表 production 分组、候选镜像 CI 或原 30 秒故障验收通过。
