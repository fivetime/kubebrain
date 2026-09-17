# Lease 读取状态锁取消准入

2026-09-17，基于 `0ad6d7b6` 的本地产品修改，尚未部署。

TTL、TTL(Keys=true)、LeaseList 在等待 `leaseMu` 时使用普通不可取消 Lock。
持锁回归让 100ms deadline 到期，旧实现仍阻塞，直到 1 秒时释放锁才退出；
session 12769 三个场景均 RED，3.091s。属于处理协程资源回收风险。

对标固定 etcd `5cd9f4ee13801e18825d661e5005ae599460bc3a`：
`server/etcdserver/v3_server.go` 的 TTL/List 查询调用 lessor，
`server/lease/lessor.go` 的 Lookup/Leases 同样使用无 context 的 RLock。
因此不把本项称为已证实的 wire 协议不兼容，也不承诺取消已经执行的状态变化。

本地修复把状态锁改用既有可取消锁实现，始终独占使用；仅 TTL/List 首次准入
改为 LockContext。内部状态转换仍使用阻塞 Lock/Unlock，锁顺序、持锁范围、
鉴权与领导权二次检查不变。回归检查取消错误、租约 deadline 不变，以及后续
读取成功以验证锁没有泄漏。不能据此宣称所有租约锁等待均支持取消。

定向五轮 race session 91615 退出 0，3.148s。完整服务回归 148.080s，
随后 vet 通过（session 17034 退出 0）；Lease/Revoke/Expiry/Checkpoint/
Attachment 扩展 race 81.671s（session 1090 退出 0）。diff 检查通过。
没有该产品修改的 CI 或真实集群验收成功声明；待前一轮 CI 完成再推送，
避免触发 concurrency 取消正在收集的回归结果。
