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
真实选主实现或存储故障；这些边界不能由测试名称推定通过。Watch 联动见下节。

## Watch 创建与真实同步器联动

`TestReadBarrierWatchWithRealSyncerDuringPromotion` 覆盖 follower 的创建请求已经进入
真实 HTTP revision fetch 后发生 fresh/stale 本机升主的两种情况。
旧流返回 Unavailable，不发送 Created、不安装远端 revision、不调用后端 Watch，
并释放已预留的唯一 Watch 配额。客户端显式新建流后，fresh leader 可以发送 Created
并交付随后 Put 的同 revision/value 事件；stale leader 仍拒绝创建，最终无配额泄漏。
每个场景只请求一次旧 peer；测试不通过自动重连掩盖旧请求失败。

最终源码下，恢复 `fafbd95b` revision 文件的隔离 overlay 对照两个场景均耗尽 2 秒预算，
退出 1（4.238 秒）；当前源码 `ReadBarrier|FollowerWatch` 组合 race 重复三次通过
（13.120 秒），etcd 包 vet 通过。测试失败收尾也显式取消并等待 Watch goroutine，
避免断言失败后后台流越过后端清理。证据 `watch-promotion-integration.ZXTeRt6z`。

该测试检验 KubeBrain 的共享后端读屏障设计，不宣称 upstream Watch 必须先执行同样的
线性化屏障：参考 etcd 的 `server/etcdserver/api/v3rpc/watch.go` 在本地 watchStream
注册监听后生成 Created；两者内部架构不同。这里没有覆盖 gRPC 线上编码、follower
代理转发或真实选主，产品实现及集群均未修改。

## CI race 覆盖补齐

检查发现原 Watch race 分组会匹配 Watch 联动测试，但不匹配 Range 联动测试；后者此前
只由 etcd 全包普通测试覆盖。现将 `ReadBarrier` 加入既有分组，保留原 8 分钟超时，
并更新工作流契约测试锁定命令。先仅更新契约时测试退出 1，更新工作流后 build 全包
race 通过（2.459 秒）。完整新分组在本地通过（29.396 秒），详细日志确认 Range 和 Watch
两个联动测试均实际执行通过。证据 `read-barrier-ci.Q4whCsQ7/local-race.log`。
这些是本地结果，新源码的 GitHub CI 结果须另行核验，不沿用 886179af 的绿色状态。

后续同源 CI 已核验：`63e0bd48b8f3b0536a68d2a92ae41c11c7daec73` 的
[回归 35186634732](https://github.com/fivetime/kubebrain/actions/runs/35186634732)
attempt 1 成功。完整日志确认新增 Range 和 Watch 联动测试在普通全包及加入 ReadBarrier
后的 race 分组均执行通过。etcd 普通全包 140.690 秒；Auth/Lease/Watch+ReadBarrier
race 分别 90.860、73.105、26.181 秒；proxy race 5.281 秒、revision race 8.343 秒、
探针全量 race 328.808 秒。日志和 SHA-256 位于私有 `release-63e0bd48.8pcmIS6U`。
该结果不是镜像身份核验或持续负载回滚通过；后两项仍需独立证据。
