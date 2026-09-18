# 初始化不得延长领导权有效期

2026-09-18，基于 `4947bfbb3b44c370117b91eec7ef98a2076fa629` 发现独立问题：
`Campaign.OnStartedLeading` 在 `InitializeLeadershipRevision` 完成后调用
`stampRenew`。初始化完成不是成功更新共享选主锁；如果初始化耗时超过
`RenewDeadline` 且续约失败，这一步会错误重置本机领导权新鲜度。
存储事务的隔离令牌不能单独证明内存续租响应安全。

修复只移除初始化回调中的时间重置，保留成功的选主锁 Create/Update 更新
时间。新 epoch 仍先于 leader 标志发布，RPC 通过既有新鲜度检查拒绝旧任期。
不修改 30 秒租期、25 秒续约截止配置，不提前接管尚有效的共享选主记录。

## 回归覆盖

`initialization_freshness_test.go` 使用真实 client-go Campaign 与可控锁替身：

- 只允许首次获取领导权成功，初始化将注入的新鲜度时钟前进 26 秒；
  之后必须判为不新鲜。原产品在此断言失败。
- 初始化期间允许真实选主循环成功续约，并等待生产 `renewStampingLock`
  更新实际时间记录；初始化完成后仍应判为新鲜。该正例在原产品上也通过。

测试不直接调用 `stampRenew`。注入时钟不推进 client-go 的调度时钟，目的是
隔离初始化发布边界；它不是实际等待 26 秒的集群故障实验。所有退出路径
取消并等待 Campaign 结束。

私有编译映射中，最小修复的完整 leader 包 race 重复五次通过（8.317 秒），
正反用例 race 重复二十次通过（1.368 秒）。原源码加测试的 race 运行中，
正例通过、反例在预期断言失败。合入工作区后的验证单独记录，不借用这些
映射结果宣称发布或部署完成。

现有 probe CI 虽由 `pkg/**` 触发，但没有直接执行 leader 包的测试；导入
leader 不会运行其 `_test.go`。本次增加该包显式 vet、完整 race 步骤及对应
工作流约束断言。此新增步骤不属于先前 `4947bfbb` 的 CI。

这不是此前真实集群 30 秒切换失败的已证实原因，也没有解决 30 秒选主租期
与整体 30 秒门限之间的预算冲突。真实 TiKV、多节点恢复及生产就绪仍待验证。

## 工作区验证

合入后，`go test -race ./pkg/server/service/leader ./build -count=1 -timeout=5m`
通过；完整 leader 包 race 重复十次通过（15.842 秒），leader 与 etcd 包 vet
通过，`git diff --check` 通过。完整 etcd 包回归通过（146.843 秒）：
`go test ./pkg/server/etcd -count=1 -timeout=10m`。尚未发布或部署本修复；
这些本地结果不属于此前 `4947bfbb` 的 CI。
