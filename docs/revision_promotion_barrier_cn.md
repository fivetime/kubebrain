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
新源码尚未完成 CI、镜像核验或集群复验；不覆盖此前失败记录。
