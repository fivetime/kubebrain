# Txn 删除区间边界对照（2026-09-20）

对照 `/root/etcd` 提交 `5cd9f4ee13801e18825d661e5005ae599460bc3a`
的 `server/etcdserver/api/v3rpc/key.go:checkIntervals`。该校验使用
`adt.NewStringAffineInterval` / `NewStringAffinePoint` 和区间树
`Intersects`；不能直接用 MVCC 执行时对 from-key 哨兵的解释替代。

新增 `pkg/server/etcd/txn_interval_parity_test.go`，直接调用当前依赖
`go.etcd.io/etcd/pkg/v3 v3.7.1` 的区间树作为比较基准。已用 `cmp`
确认依赖与上述参考仓库的 `adt/interval_tree.go` 字节相同；参考仓库
相关两个文件无工作区修改。未复制或另写一个推测的上游算法。

七种非空二进制键包含 NUL、NUL 前缀、普通字符串、字符串前缀及 0xff。
枚举七种起点、八种终点（包括空终点）和七种 Put 键，共 392 个边界
组合，涵盖点删除、正常区间、空/反向区间及单字节 NUL 终点。逐个比较
本地区间包含判断与上游区间树，再验证完整请求校验的成功/失败分支和
Put/Delete 两种顺序，共 1568 个事务请求，并比较重复键错误状态。

结果未发现该范围内的语义差异，因此没有修改产品逻辑。最终定向竞态
测试连续三次通过（1.193 秒），`go vet ./pkg/server/etcd` 和
`git diff --check` 通过。命令：

```sh
go test -race -count=3 -timeout=2m ./pkg/server/etcd -run '^TestTxnDeleteIntervalParityWithEtcd$'
```

范围限制：这是输入校验的边界对照，不是实际 TiKV 事务执行、任意深度
嵌套事务、全部 etcd API 或真实故障验收的证明。原 30 秒验收仍未通过。
