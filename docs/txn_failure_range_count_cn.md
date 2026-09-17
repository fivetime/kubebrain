# Txn 失败分支 Range Count 修复

2026-09-17，来自 [602912b7 集群验收](acceptance_apiserver_fault_602912b7_20260917_cn.md)
的故障前 DataLoss 线索：Txn 的失败分支返回一条 KV，但 Count 为 0。

## 原因和修改

`backendShim.Update` 的比较失败分支，以及旧 `backendShim.Delete` 的 Range 响应构造，
填写了 Kvs 却遗漏 Count。前者由公开 Txn 的无租约单键 MOD 比较更新优化路径使用，
更新冲突本应成功返回 `Succeeded=false` 和当前值，却会被 follower 的严格响应校验拒绝。
后者不是当前公开 CompareDelete 优化路径，作为保留的适配接口一并补齐响应一致性。

仅在两处构造中填写 `Count=int64(len(kvs))`；不修改事务提交逻辑、响应校验或重试策略。
不能据此宣称修复内部 Lease 故障或 Watch revision 超时，也不能确认历史日志中的每条错误
都来自同一请求形态。

## 对照与验证

参考 `/root/etcd` 源码 `5cd9f4ee13801e18825d661e5005ae599460bc3a`：
`server/etcdserver/txn/txn.go` 执行选中分支的 Range，`txn/range.go` 的
`asembleRangeResponse` 从 MVCC 结果填写 Count。
另启动独立本机参考 etcd，用 etcdctl 执行 MOD 比较失败后的 Get：存在键 Count=1，
不存在键 Count=0，冲突不修改值或推进 revision。对照进程退出 0，所属 etcd 已停止。
首次对照脚本未适配 etcdctl 的 `Response` JSON 包装及省略的 false 字段，断言退出 1；
修正断言后的独立对照通过，未覆盖原失败证据。

新增 `txn_failure_count_test.go`：公开 Txn 的存在键冲突/缺失键分支，检查 Count、KV、
revision、不发生写入以及现有 Range payload 校验；另覆盖旧 Delete 成功和冲突的 Count。
修复前测试退出 1，三个子用例均复现 expected=1/actual=0；修复后 race 重复 10 次通过
（5.356 秒）。这些测试使用 memkv，不替代 TiKV 集群验收。

私有证据目录：`/root/.local/state/kubebrain/txn-failure-count.H1eFbHGg`，
成功参考对照 `reference.GvK5GnDX`。完整服务包非 race 测试通过（138.740 秒），
`go vet ./pkg/server/etcd` 和 diff 空白检查通过。没有额外全包 race 通过的声明，
发布 CI 和集群复验仍待完成，目前不宣称新镜像或集群已通过。
