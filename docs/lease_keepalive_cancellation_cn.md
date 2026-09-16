# KeepAlive 入口等待取消

## 问题与对标边界

此前外层 `LeaseKeepAlive` 可在请求取消时返回，但内部 `leaseKeepAlive` worker
仍可能等待绑定锁、checkpoint 全局屏障或 checkpoint 分片锁。旧实现的六个回归场景
（三种锁 × 显式取消/超时）均在取消后继续阻塞，直至测试释放锁。
仅测试外层 RPC 返回不能证明内部任务已经退出。

对标 `/root/etcd` 提交 `5cd9f4ee13801e18825d661e5005ae599460bc3a`：
`server/lease/lessor.go` 的 `Renew(id LeaseID)` 不接收 context，其内部也存在
不可取消的等待。因此本项是 KubeBrain 的资源管理改进，不声称发现或修复了
已经证实的 etcd 线协议差异。

## 实现

- checkpoint 屏障和固定 256 个分片复用现有可取消、偏好 writer 的
  `leaseWriteMutex`；后台 checkpoint/reload/withdraw 仍使用阻塞接口。
- KeepAlive 在屏障、分片和绑定锁的初次获取阶段使用请求 context。
  获取分片或绑定锁失败时释放已获取的上层锁。
- 无鉴权/root 的共享绑定锁路径和普通用户的独占绑定锁路径均覆盖。
  无关 lease teardown 的内存续租快速路径在状态锁内检查 context。
- 持久化 checkpoint 清除已经开始后，保留原有阻塞式绑定锁重获、状态身份、
  鉴权、leadership epoch 和提交结果核对，不能用取消跳过这些步骤。

这不是“取消等于回滚”的承诺：取消与实际状态修改并发时，不能推断操作未发生；
也没有把所有 lease 内部互斥锁改成可取消锁。保证范围是尚未进入修改阶段的
上述入口等待，不扩展为端到端时延上限。

## 回归覆盖

`pkg/server/etcd/lease_keepalive_admission_test.go` 直接等待 worker 终止，
而非仅等待外层 RPC 的取消分支：

- 三类锁分别覆盖显式取消及 deadline，锁仍被测试持有时 worker 必须退出。
- 退出后 deadline 不变；同一个 lease 后续正常续租成功。
- checkpoint 屏障可以再次独占获取，防止遗漏释放其共享锁。
- root 与普通用户的内部授权续租入口分别覆盖三类锁的 deadline。
  该测试构造 caller 以区分锁路径，不替代完整鉴权 RPC 集成验收。

测试开发期间另纠正了两个测试问题：重复 Recv 关闭同一通知 channel，以及把内部
返回的原始 context 错误当作已编码的 gRPC status。最终使用单次通知并检查
`errors.Is` 语义；这些测试脚手架失败不作为产品回归证据。

## 发布边界

本地验证：

- `go test ./pkg/server/etcd -count=1`：通过，141.259 秒。
- `go test -race ./pkg/server/etcd -run 'TestLease(KeepAlive|AuthorizedRenewal|Checkpoint|Renew|Revoke|Grant)' -count=1`：通过，4.625 秒。
- `go test -race ./pkg/server/etcd -run 'TestLease(KeepAliveCanceledAdmission|AuthorizedRenewalCanceledAdmission)$' -count=5`：通过，7.455 秒，包含最终的屏障独占重获检查。
- `go test -race ./pkg/server/etcd -run 'Lease|Checkpoint' -count=1`：通过，73.211 秒，扩大覆盖鉴权、checkpoint、撤销及续租并发回归。
- `go vet ./pkg/server/etcd`：通过。

本次仅修改本地源码和测试，没有变更测试集群、发布镜像或替换正在运行的实例。
此前 bb89c3f8 镜像的 TopoLVM 验收不覆盖这次新实现；仍需后续 CI 和真实后端验收。
