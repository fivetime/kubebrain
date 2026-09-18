# 转发响应任期回退修复

## 现场与确定性复现

2026-09-18 的 da303fdb 故障实验中，后继节点任期已达 125，原续租
RPC 返回 TTL=10，但入口响应头仍为 124。完整现场结论见
[实验记录](peer_fault_da303fdb_20260918_cn.md)：该实验失败，不能据此宣布
30 秒验收通过；现场没有捕获转发边界的原始响应头，不能断言这是唯一原因。

确定性回归在真实 clientv3 / gRPC 拦截器路径上固定入口缓存为 124，
已校验后继响应为 125。修复前，正常任期结束及延迟 demotion 回调两种测试
均失败：期望 125，实际 124。原因是发送拦截器无条件用本地缓存覆盖转发头。

## 实现边界

成功转发响应在方法特定的结果、请求 ID、负载校验之后，观察响应元数据。
公共观察入口再检查集群、成员、非负 revision 和非零 term，原子保存只增的
`forwardedResponseTerm`。发送响应取本地选主任期与该下限的较大值；二者都
未知时仍读取共享选主记录，并保留原有错误处理。

此下限仅用于响应元数据，不更新选主缓存、epoch、路由、租约新鲜度或写入
隔离。Status 正文和头仍使用同一次任期快照。原有 committed revision
观察语义不变，不推进 watch 发布水位，也不重新生成续租 revision。

对照 `/root/etcd/server/etcdserver/api/v3rpc/lease.go`：上游在续租之前填充
响应头，保护 revision 不超前。本修复没有移动 revision 的采样点；响应任期
下限是 KubeBrain 共享 TiKV/转发架构的处理，不能声称上游实现同样的缓存机制。

## 验证与未完成项

新增测试覆盖并发只增下限、非法头与失败响应不污染下限、低/零本地缓存时
避免后端读取、本地更高任期优先、Status 头/正文一致，以及原续租流只消费
一次请求、不新建流、返回后继任期且不改选主缓存。

本地验证：定向 race 回归通过；原流续租及响应任期测试在 `-race -count=20`
下重复通过（10.651 秒）；`go vet ./pkg/server/etcd` 通过。
`go test -race -count=1 -timeout=15m ./pkg/server/etcd` 整包通过
（351.579 秒）。`pkg/server` 的三个真实网络交接测试
`TestPeerRetirementFullServerNetworkLeaseHandoff`、
`TestPeerRetirementPendingExpiredStreamDuringStorageFailure`、
`TestPeerRetirementPendingStreamWithReloadedProxyCredentials` 在 race 模式
通过（34.793 秒）。上述本地验证对应产品修复提交 `009ce5ca`，不是
实际 Kubernetes 集群故障验收。原始日志保存在本机私有状态目录
`/root/.local/state/kubebrain/forwarded-term-fix.qQlAxyQj/`。

修复尚未通过新镜像 CI 和现场原 30 秒门限
验收；上轮缺失的 demoted 栈证据、完整故障时序及恢复证明仍须重新取得。
