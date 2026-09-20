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

## CI 失败与断言修正

随后提交 `0a0b0dfd01f19a0e5dbebda65ebb37609d65764f` 的回归 CI
`35491608346` 在 etcd 整包测试中失败，唯一失败测试为本新增边界测试。
日志归档：`/root/.local/state/kubebrain/ci-35491608346-terminal.pxj6ZtN9`，
包含终态、完整日志及 SHA256SUMS。不能将此前的孤立测试通过当作整包通过。

失败差异位于 protobuf 私有 `atomicMessageInfo`：此前断言对整个 gRPC
Status 做反射深比较，受之前测试是否触发消息缓存初始化影响。本地在
循环前序列化预期 Status 后，旧断言稳定复现同样失败。因此保留这个
预热条件，改为比较客户端可见的错误码、错误消息并要求无附加详情；
不修改产品代码、不减少边界枚举，也不改动原 30 秒验收门限。

修正后的定向竞态测试连续三次通过（1.191 秒），同包 `go vet` 与
`git diff --check` 通过。另已启动与 CI 首轮相同范围的 etcd 整包测试
（`-count=1 -timeout=10m`），输出目录
`/root/.local/state/kubebrain/txn-parity-full-regression.ojiqTrTx`；记录时
仍在执行，不能报告整包通过。远端镜像 CI 仍在运行，修正尚未推送。

该整包进程随后正常结束：退出码 0，148.768 秒，2764 个顶层测试通过，
无失败事件，边界对照测试在完整顺序中通过。已检查原始 JSON 事件、
stderr、退出码及 SHA256SUMS，三份文件的摘要均正确。没有重新启动、
跳过失败用例或延长既有 10 分钟包测试预算。此为本地整包验证，远端失败
记录仍保留；修复仍需后续提交的 CI 验证，不等于真实 TiKV 故障验收。
