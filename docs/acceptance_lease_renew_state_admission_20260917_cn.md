# 续租状态锁的取消入场

2026-09-17，在 `69ae8e8f` 基础上检查发现：续租的绑定锁、检查点屏障和
检查点分片锁已支持取消，但随后首次读取租约状态仍使用阻塞锁。持锁者停顿时，
超时请求的工作协程继续等待，并占用已取得的检查点/绑定锁。

## 复现与修改边界

在 `TestLeaseAuthorizedRenewalCanceledAdmission` 增加 `state` 锁持有者，
保持原 100ms 请求期限和 2s 工作协程退出门限。未改产品前，root/state、
writer/state 均失败（session 92875，包耗时 4.069s），错误为
`authorized renewal outlived canceled admission`。失败分支先释放锁并回收
协程，避免留下阻塞服务。这是资源回收缺陷，不声称已证明线上长租约残留由此造成。

修改 `lease.go`：

- 普通续租首次取得状态锁改为 `LockContext`，失败释放绑定和检查点锁。
- 授权续租首次复制附着键使用可取消的状态锁，再执行原 WRITE 权限检查。
- 无关租约回收期间的无检查点快速续租，同样支持状态锁等待取消。
- 后台 `keysForLease` 调用保留 Background 阻塞语义。检查点 CAS 开始后的
  锁重取、提交状态核对、权限重检和续租收尾均未改为可取消，避免丢失已提交写入的收尾。

本地参考 `/root/etcd/server/lease/lessor.go` 的 `Renew` 在清除检查点后才
刷新租约；参考实现的检查点请求使用 Background。此次保持该提交边界，不将
KubeBrain 的可取消入场描述为上游已有的完全相同实现。参考源码固定为
`5cd9f4ee13801e18825d661e5005ae599460bc3a`。

## 测试与尚未证明的范围

KeepAlive worker 的 cancel/deadline 用例、root/writer 授权续租用例均新增
状态锁覆盖，检查取消后的 deadline 不变、后续检查点锁及续租可用。
新增 `TestLeaseRenewTeardownStateAdmission` 覆盖绑定 writer 被回收任务占用时
的快速路径，要求在状态锁仍被持有时退出，且不续期、不泄漏前置锁。

第一轮 KeepAlive/授权入场 race 重复三次通过（session 41858，6.204s）。
最终含快速路径的新代码通过扩展入场/检查点/续租/主从切换 race 测试
（session 76814，10.497s），`go vet ./pkg/server/etcd` 通过（session 49603）。
完整服务测试通过（session 89641，144.004s）；root/state、writer/state
分别重复十次 race 均通过（session 69251，3.815s）。
后续精确 `66b42fcd29f3d52cee5a46c7e66dff2e61dcc13f` 的 CI `35220675194`
attempt 1 全部成功：服务 137.699s，Auth/Lease/Watch race 分别
86.460/76.601/26.283s，完整探针 race 303.701s。归档明确包含授权续租
root/state、writer/state 和 teardown 状态入场新用例的 PASS。
同次 push 自动触发镜像 CI `35220675175`，仍在执行；尚无完成的镜像审计
或真实集群部署证据。未改默认 2PC、租约 TTL、
原清理门限或测试集群；此前整体退出 70 的记录保持不变。

私有续接记录：`/root/.local/state/kubebrain/lease-renew-state-admission.5Wn5K0SQ/STATE.md`。
CI 完整日志：`/root/.local/state/kubebrain/lease-renew-state-epoch.u5DtiMzf/probe-35220675194.log`，
SHA-256 `bdbf4074ffc6f68196ba3f98d2ba12e207afc435e9fb83997ecb88da04a53dd4`。
