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

## 推送前的状态锁开销复核

初版 `ce81eb8d` 尚未推送。微基准 session 18726 退出 0，三轮、
GOMAXPROCS=1/4、空临界区比较普通 Mutex 与复用的 weighted 锁。四线程竞争
下前者 113.7–120.8 ns/op、0 allocs/op，后者 641.8–668.7 ns/op、
175 B/op、2 allocs/op。这不是业务性能对照，但提示全局状态锁分配代价。

后续工作树改为专用 `leaseStateMutex`：容量 1 的 channel 仅提供独占访问，
保留阻塞 Lock/Unlock；LockContext 在取消时退出，并在获锁后的取消竞态中
释放刚取得的所有权。绑定/checkpoint 的读写锁不变。不创建等待辅助 goroutine。
零值、已取消 context、阻塞 deadline、取消后重新获锁、非法 Unlock 和并发
互斥测试十轮 race 通过（session 41537，1.481s）。

新实现微基准 session 87600 退出 0，四线程竞争 287.3–296.9 ns/op、
0 B/op、0 allocs/op；串行约 60–79 ns/op，仍高于普通 Mutex 的约 16–17 ns/op。
数据来自本机微基准，非受控业务负载；不能推算实际 RPC 吞吐或宣布性能门限通过。
基准保留三种实现以便后续复核。

新实现五轮定向 race 3.476s、扩展租约 race 79.754s（session 21834 退出 0）；
完整服务 147.309s，随后 vet 通过（session 16988 退出 0）。这些是新实现的
独立结果，不复用上节 ce81eb8d 的完整回归。仍未推送、部署或取得对应远端 CI。

09:58 UTC 发布安排：旧回归 `35206137761` 已全部成功，但旧镜像构建
`35205724426` 尚未结束。证据更新采用 `[skip ci]` 随两个产品提交一起推送，
避免 push 自动取消旧镜像；随后仅手动触发新源码回归。新镜像需等旧构建终态后
单独触发，并重新取得同源 CI/镜像审计，才具备部署准入资格。
