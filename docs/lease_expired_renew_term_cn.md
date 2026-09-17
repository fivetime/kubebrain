# 过期租约续租等待的任期退出

2026-09-17，在已完成 `31c0a1fb` 集群验收之后继续对照固定参考
`/root/etcd`（`5cd9f4ee13801e18825d661e5005ae599460bc3a`）。
etcd `server/lease/lessor.go:Renew` 等待过期租约撤销时，同时监听 `demotec`
和 `stopC`，并在任期结束后返回 `ErrNotPrimary`。

KubeBrain 原 `refreshLeaseHoldingLocks` 的过期分支仅监听 `st.revoked` 和
请求取消。任期结束会取消旧任期的撤销工作并撤下租约快照，因此旧请求可能一直
等待不会再关闭的撤销通道，直到客户端自行取消。

新增确定性测试 `TestExpiredLeaseRenewStopsWithLeadershipTerm`：在释放续租
准入锁的回调处确认进入过期等待，再取消所属任期，不依赖 sleep 猜测执行位置。
修复前失败（session 94666，1.054s）：任期取消一秒后请求仍未退出。
修复捕获受 leaseMu 保护的原任期 Done 通道，等待期间任期结束返回既有
`errLeaseDemotedDuringRenew`，由既有调用层处理路由；不关闭 revoked，
不宣称持久租约已被删除，也不提前返回 lease-not-found。

针对 `TestExpiredLease(Renew|KeepAlive)` 的 race 重复十次通过
（session 53379，3.460s），包含撤销失败后等待及请求取消测试。
完整服务测试通过（session 4692，148.478s）；使用 CI 同样筛选条件
`(Lease|Revoke|Expiry|Checkpoint|Attachment)` 的扩展 race 回归通过
（session 94608，81.325s）；vet 通过（session 21131）。
代码对照确认既有 LeaseKeepAlive 调用层将此错误交给降主转发或 Unavailable
分支，而不是转换成 lease-not-found/TTL=0；这不替代真实多节点路由验证。
这是内部边界验证，不是完整生产故障证明。CI、镜像和集群验证尚未完成；
此前成功的 31c 集群验收不覆盖本修改。

## 后续服务层路由覆盖

新增 `TestExpiredLeaseKeepAliveRoutesAfterTermEnds`，直接调用完整
`leaseKeepAlive` 服务工作循环，在过期分支取得所属任期 Done 通道的确定边界
结束任期并切为 follower。禁用代理时要求 Unavailable、无响应、无转发；
启用代理时要求恰好转发一次并返回远端 ID/TTL（37），不能误发 TTL=0。
这是内存 peer 替身测试，不是实际网络或多节点部署。
与任期退出用例一起 race 重复 20 次通过（session 58361，2.905s）。
该测试补充在后续独立测试提交中记录；当前 `142d44e8` 的 CI 不包含这项后续补充。

通过私有 Go overlay 保留 Done 观察点、仅移除 select 中任期退出分支，两个
服务路由用例均在任期取消后未完成路由而失败（session 15616，2.072s）。
更早的 overlay 同时删掉观察点，失败于等待准备阶段（session 58488），不计为
有效行为负例。真实产品文件未替换；新增测试的 vet 通过（session 77593）。

## df783b7f 发布与真实后端回归

上述后续测试提交 `df783b7f` 的两项 CI 已成功，原始日志确认两个新测试的普通和
race PASS；独立镜像审计通过。随后在独立本地盘 PD/TiKV 上完成 6000 次持续
负载回滚，原延迟门限、Watch/Lease/流验证及独立诊断审核通过，原完整配置已恢复，
本轮临时资源已清理。详见[本轮验收记录](acceptance_local_rollback_df783b7f_20260917_cn.md)。
这取代上文当时“CI、镜像和集群验证尚未完成”的发布状态，但不等于真实集群命中
了“已过期、撤销仍阻塞、此时任期结束”的精确竞态；该故障注入覆盖仍开放。
