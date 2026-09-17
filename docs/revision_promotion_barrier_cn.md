# revision 同步期间本机成为 leader

2026-09-17：[602912b7 故障实验](acceptance_apiserver_fault_602912b7_20260917_cn.md)
中，入口 local-2 成为 leader 后，内部 Watch 在 03:56:45.672 和 53.672 报告 revision 同步超时。

## 复现与原因

`SyncReadRevision` 只在入口检查本机的 fresh leadership。已经按 follower 身份进入的
共享 fetch 在重试期间没有重新检查角色；HTTP 响应校验正确地拒绝本机成为 leader 后的
远端 revision，但外层继续重试，耗尽 8 秒预算。排队的下一批也可能再耗尽 8 秒。
运行时的两个时间点与该机制一致，但不把时序对应视为所有 Watch 错误的完整因果证明。

隔离诊断以真实 HTTP 测试服务阻塞首个响应，在请求进入后发布本机 leadership：
未切换角色的对照通过，而切换后的旧请求连续三次耗尽 1.5 秒调用方预算；同时新请求
可直接通过既有 fresh-local 检查。修正诊断契约为“允许成功或明确的领导权变化错误”后，
仍连续三次超时；原始测试更严格地要求成功，不作为 etcd 必须无错返回的依据。

参考 `/root/etcd` 源码 `5cd9f4ee13801e18825d661e5005ae599460bc3a` 的
`server/etcdserver/read/read.go`：`requestCurrentIndex` 在 leader-change 通知后结束旧批次，
返回 `ErrLeaderChanged`，并拒绝通知前后的失效 ReadState；它不是接受旧 leader 的读索引。

## 修复边界

共享重试循环每次 HTTP 尝试前检查 `IsLeader`，若本机已成为 leader，则以现有
`errLeaderChanged` 结束当前批次；下一排队批次也在发送请求前结束。
上层既有 `readBarrierStatusErr` 将普通协调错误映射为 gRPC Unavailable。
调用方可以重新开始读屏障；新调用仍必须通过原 fresh leadership 检查。

不安装远端 revision、不将陈旧本地 lease 视为成功、不增加重试预算、不重放写入。
保留原 leader 地址/term 响应校验和双批次新鲜度规则。已经在途的 HTTP 请求仍受原请求
超时约束；此修改不是即时中断网络请求，也不承诺领导权切换期间 Watch 完全无错。
内部 Lease Txn 不确定结果是另一问题，本修改不解决它。

## 本地验证

新增 `TestReadRevisionEndsOldFetchOnLocalPromotion` 覆盖未切换、fresh local、stale local，
并验证切换前排队的下一批也返回领导权变化、HTTP 总请求数保持一次、旧 revision 未写入。
完整 revision 包 race 重复三次通过（22.743 秒），包括现有地址/term 切换、取消、Close 和
中途加入读取的新鲜度测试。初版聚焦测试 race 十次通过（6.203 秒）。

回归 CI 增加 revision 包的 vet 和全包 race，工作流契约测试锁定这两条命令。
build 包全量 race 通过（2.485 秒），revision 包 vet 和 diff 空白检查通过。
在本地提交 `93c4a9b3` 上，上层 etcd 包的 `ReadBarrier|FollowerWatch` 既有回归
race 重复三次通过（9.744 秒）；这是上层错误映射与 Watch 的补充检查，
不是接入真实 revision syncer 的完整角色切换端到端验收。
私有证据：`/root/.local/state/kubebrain/revision-promotion.2d5y5x1A/`。

## 回归 CI

源码 `886179af3cc203dcb1b52cb01b3b0f2b9db17c08` 的
[回归 CI 35183014224](https://github.com/fivetime/kubebrain/actions/runs/35183014224)
attempt 1 已成功。完整日志确认新增角色切换测试的三个子用例实际通过，
revision 包全量 race 为 8.317 秒；etcd 非 race 全包 130.375 秒，
Auth/Lease/Watch race 分组分别 85.898、71.434、24.619 秒，
rollout probe 全量 race 316.736 秒，均通过。
日志及摘要位于私有 `release-886179af.pcMGEHC3/probe-ci.log`、`probe-ci.sha256`。
同源镜像构建与独立镜像核验随后通过；[集群复验](acceptance_apiserver_fault_886179af_20260917_cn.md)
完成 2000 次更新、Watch 审计、清理及原镜像恢复，均退出 0。
本轮旧查询在本机升主后明确返回 leader changed，未检出此前 revision 查询超时；
但仍有 Range leader changed 警告，单次实验不覆盖所有切换时序，也不改写此前失败记录。

## RPC 与真实同步器联动回归

新增 `TestReadBarrierRangeWithRealSyncerDuringPromotion` 将真实 revision syncer、
HTTP `/status` 请求及 Range handler 接在一起，使用内存存储与受控 election 状态。
以已提交键为对照：未升主时完成同步并返回原值；升主前的旧 Range 返回 Unavailable，
不安装被拒绝的远端 revision、不进入后端 Get；下一次请求仅在 fresh leadership 下
返回已提交值，陈旧租约仍返回 Unavailable，三个场景各只发送一次 HTTP 请求。

隔离 Go overlay 仅替换为 `fafbd95b` 的旧 revision 实现：未升主对照通过，
fresh/stale 两种升主均耗尽 2 秒调用预算，测试退出 1（4.260 秒）。
这里直接调用 handler，旧错误的 `status.Code` 为 Unknown，不据此推断 gRPC 线上代码；
gRPC 的 context 错误转换不在本测试范围内。
当前实现聚焦 race 重复三次通过（2.977 秒），`ReadBarrier|FollowerWatch`
组合 race 重复三次通过（11.489 秒），etcd 包 vet 通过。

证据 `range-promotion-integration.3og17xvS`，不修改产品实现、不提高原预算、
不改集群或重跑 CI。本测试覆盖直接读屏障路径，不覆盖 follower gRPC 转发、
真实选主实现、存储故障或 Watch 与真实同步器的联动；这些边界不能由测试名称推定通过。
