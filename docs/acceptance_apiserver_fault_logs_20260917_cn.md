# 带成员日志的真实 apiserver 故障实验（2026-09-17）

## 结论

**工作负载通过，但整体实验退出 70：lease 清理未通过。** 20 个对象各 100 次更新完成，
公共 Watch 的 2000 个 MODIFIED 完整性校验通过；未重启公共 Watch，未在驱动中重试
失败 PATCH。原固定镜像恢复成功，后端及旧 Ceph 前端身份检查通过。

本轮未复现[前一次 Txn 失败](acceptance_apiserver_continuous_fault_20260917_cn.md)：
相似故障时机下中断的是 Range，etcd 客户端重试后继续，而不是写 Txn 自动重放。
**前一次失败仍有效，本次不证明写事务故障可用性问题已修复。**

## 对象、来源与证据采集

专用 `kubebrain-local` / `kb-local`、TopoLVM、默认 2PC；前端 UID
`7d760f53-5bb5-4429-a2f8-651b89665616`。原镜像 index
`sha256:50b9938fe3e5ad379136957438abe3c2d4bf250dc6c5973c869e0ef1341d1537`，
临时候选为已审核源码 `8ac67ca3b21423e3d87c3c882850ce655eb23dd9` 的 index
`sha256:99535eb62b41d78de573c1d7162a20946424d33a4aa30e8324a3ef2adaf644a7`。
重新核验 CI 35163623338/35163623307、审核收据、平台摘要、源配置、TLS 生命周期、
LeaseList=0、唯一 prefix 为空、隔离预拉取准入及正式部署前运行时身份。没有部署 df 镜像。

使用同一官方 kube-apiserver v1.36.1；执行仓库 HEAD `ff7e102d`，与候选产品源码差异
仅为文档、测试和诊断命令。故障前三个成员的[日志采集器](pod_log_capture_cn.md)均就绪且
存活；源身份前后核对通过。重建 Pod 使用新 UID/新目录采集，并保存其 Bound PVC/PV
快照。四条流均终态 0、已保存字节的摘要独立复算通过，但不宣称完整历史日志无缺失。

## 观测时间线

| UTC | 观测 |
| --- | --- |
| 01:46:45 | 公共 Watch 已就绪，观察到 3 个 MODIFIED；以 UID/RV 前置条件强制删除 leader `kubebrain-local-1`，UID `51d4242c-6e99-44d0-ab95-ccc86ffaa895` |
| 01:46:45.988 | 入口 `kubebrain-local-2` 将 `soak-9` 的 Range 转发至旧 leader |
| 01:46:46.612 | 入口成员记录 acquired lease / start leading |
| 01:46:51.460 | 入口对旧 peer 的 Health/Check 超时 |
| 01:46:51.462 | 日志记录因检查连接失败重置共享 client；旧 Range 返回 leader changed |
| 01:46:51.495 | apiserver 的 `soak-9` GET 完成，trace 耗时 5508ms；后续更新继续 |

时间线来自各组件自身日志，不把墙钟顺序单独当作跨进程因果证明；`start leading` 也不
等于领导权初始化已全部完成。源码及后续确定性回归进一步核对连接检查行为。
本次证明完整性通过，不宣称 GET 满足 5 秒 SLO；5508ms 不能省略。
apiserver 内部 cacher Watch 曾出现 no leader 并重试，不能把公共 Watch 未重启写成
内部没有重连。参考 etcd 的不同拓扑观察仍见[对照记录](acceptance_reference_apiserver_fault_20260917_cn.md)。

## 终态、恢复与清理

原生结果 `operation_exit=0, cleanup_failed=1, archive_failed=0, runner_exit=70`；
独立 Watch 审计退出 0。外层部署驱动为
`operation_exit=70, restore_exit=0, holder_cleanup_exit=0`，会话 16290 终态 70。
原 spec、runtime image identity、Ready/updated 3/3 已恢复，generation 6 → 7 → 8。
六个本地 PD/TiKV Pod UID/containerStatuses 不变，旧 Ceph 前端 UID/generation/spec 不变。

60 秒清理窗口结束仍有空 lease `0001a0ad09e60a01`。01:54:40 UTC 只读观测：
granted TTL 3660、remaining TTL 3371、关联键数 0，测试 prefix 为空。
没有发送 KeepAlive 或 Revoke；不能只靠 before/after lease 差集证明归属并撤销。
仍待自然到期后另行确认，这不会追溯改变本轮 cleanup_failed=1 的结果。

后续终态：2026-09-17 02:51:10 UTC 只读确认该 lease `TTL=-1`、granted TTL 0、
关联键数 0，`lease list` 为 `found 0 leases`，测试 prefix 为空；自然到期已确认。
会话 91299 退出 0，证据 `lease-observation.LK4fv7BJ/`。未发送 Revoke/KeepAlive，
本轮原先的 60 秒清理失败和整体退出 70 不变。

### 后续诊断：空 lease 不等于失效 lease

2026-09-17 02:26:02 UTC 再次只读观测：同一 lease 剩余 TTL 1489、granted TTL
3660、关联键数 0，prefix 仍为空。成员日志 `logs/original-2/container.log:783`
记录故障前 01:46:36.930 的 `forward lease grant`：ID `458140032567809`
（即上述十六进制 ID）、**请求 TTL 已是 3660**。因此不能把 granted TTL 3660
归因于这次故障后的额外延长。日志随后有 Event 写入，但缺少 key/lease 绑定证据，
不能据此证明该 lease 的对象归属或授权撤销。

本机参考 etcd `5cd9f4ee13801e18825d661e5005ae599460bc3a` 的
`server/lease/lessor.go:Detach` 只移除键的关联，不删除 lease。独立单成员对照：
Grant 3660 秒、写入一个附租约 key、删除该 key，再等待 65 秒；空 lease 仍有效，
granted TTL 仍为 3660。会话 53433 退出 0。此最小对照证明“删除最后一个 key”
不意味着 lease 应在 60 秒内消失，**不是本轮 KubeBrain 自然过期正确性的证明**。
测试最后只撤销由自身 Grant 返回 ID 的本机租约，然后停止自身 etcd；端口已释放。
未触碰远端集群 lease。证据目录：
`/root/.local/state/kubebrain/empty-lease-reference.5Rl3rEzF/`。
首次准备 `empty-lease-reference.eiVireAg` 因启动参数中的多余 `+` 失败，
会话 32170 退出 1；没有执行 Grant，不计为对照结果，失败证据保留。

本机 Kubernetes 源码 `5b0cba2ee0da06385b12a9ae20cd0671ea3f860d` 的
`staging/src/k8s.io/apiserver/pkg/storage/etcd3/lease_manager.go:GetLease`
还会为请求 TTL 增加最多 60 秒的复用余量；Event TTL 默认一小时。
这为 3660 秒提供了与日志相容的解释，但不是官方 v1.36.1 二进制的逐行来源证明，
也不是这条 lease 的完整因果追踪。

结论：本轮仍是 cleanup_failed=1、整体退出 70；暂不因这项观测修改产品的
空 lease 生命周期，不缩短 Event TTL 或放宽原清理窗口来制造通过结果。
继续独立验证自然到期，并保留写故障可用性与清理门限两个不同问题。

部署前、候选及重建阶段三份 Bound 快照确认本轮 14 个独立 scratch PV。逐卷核对
UID/full spec、Released/Retain、替代 PVC 绑定不同卷、无 VolumeAttachment 后，用
UID/RV/spec/phase 前置条件仅改变这些卷的回收策略并等待删除；会话 94990 退出 0。
临时数据不可恢复，全部非目标 PV 的 UID/spec 未变。最终 12 Bound、2 Released，
LogicalVolume 共 14；另 2 个是上一轮缺少历史 Bound 证据而保留的卷，没有混入本轮清理。
三份辅助二进制按 `helpers.sha256` 验证后删除，源码、日志、归档保留。

## 后续代码修复的边界

2026-09-17 后续 CI：修复提交 `96a004800858bdd5cc8db50fbac8264903d387b2`
的[回归作业 35174037159](https://github.com/fivetime/kubebrain/actions/runs/35174037159)
attempt 1 已成功。下载完整日志后确认三个新增连接监控测试与
`TestTxnDoesNotReplayAmbiguousForwardResult` 实际执行通过；日志采集 race 测试
4.835 秒、完整 rollout probe race 测试 307.088 秒通过。etcd 非 race 全包及原有
Auth/Lease/Watch race 分组分别为 137.792、87.867、70.582、24.440 秒，均通过。
这不覆盖下面记录的额外全包 race 五分钟超时，也不等于集群故障验收通过。
镜像作业 `35174037137` 随后也成功完成；新镜像独立审计已启动，尚未部署。
CI 原始日志与摘要保存在私有 `release-96a00480.yeI5nxKJ/probe-ci.log`、
`probe-ci.sha256`；下载会话 67225 退出 0。

原 `trackLeaderConnectionAttempt` 在创建时核对一次，其后主要靠新请求的 `waitReady`
取消过期连接尝试。已经转发的请求、或新 leader 直接本地处理的请求，不一定进入
`waitReady`；后台检查可继续等满 5 秒。这是与观测一致的额外延迟路径。

新增 `TestBackgroundConnectorPreemptsStaleAttemptWithoutRequestWaiters` 在原实现上
确定性失败（会话 28732）：已公布新 leader，但不调用 `waitReady` 时后台连接器在
1 秒内不能就绪。修复为仅在连接尝试期间以 100ms 间隔观察已公布 leader；不同且有效
的身份会取消旧尝试，完成时取消并等待监视协程退出。不修改未知 leader 的处理、
写入提交判定、事务重放策略或外部验收门限。

新增测试还覆盖已发布连接的阻塞健康检查、未知/未变 leader 不应取消、继任者发布后
应取消，并重复验证歧义 Txn 不重放。**这是缩短连接退休延迟的修复，不是已经通过
新镜像故障验收的结论，也不能保证所有在途写请求成功。**

本地定向 race 十轮通过（3.922s），扩展到阻塞健康检查及未知 leader 后再十轮通过
（8.980s），`go vet` 通过。额外联合全包 race 中，转发包 4.085s、leader 包 2.492s
通过，但 etcd 服务包在 300.985s 失败；该命令的预算为 5 分钟，工具返回被大量截断，
尚不能排除隐藏的其它失败，**不宣称全量全包 race 通过**。按现有 CI 的原范围/预算
另行顺序执行的结果均通过，完整输出已落盘：

| 范围 | 顶层测试数 | 耗时 |
| --- | --- | --- |
| etcd 全包非 race（原 10m 预算） | 2735 | 138.367s |
| Auth/JWT race（原 8m 预算） | 255 | 109.328s |
| Lease 等 race（原 8m 预算） | 339 | 74.629s |
| Watch 等 race（原 8m 预算） | 275 | 27.218s |

逐组通过名称与源测试清单/筛选表达式核对一致，未遗漏或跳过；分组有重叠，不累加为
唯一测试总数。驱动退出 0，前后源码摘要一致。证据
`/root/.local/state/kubebrain/connector-regression.pELXhRZw/`。之后使用**原三包命令和
原 5m 预算**再次复现（会话 67932，退出 1），完整日志明确记录
`panic: test timed out after 5m0s`，服务包 301.020s 失败。此前没有断言失败或 race
告警；转发包 4.050s、leader 包 2.635s 通过。超时时当前测试
`TestClientDoOpTxnNoOpDeleteWithPrevKVDoesNotConsumeRevisionMatchesEtcd` 刚运行约 0 秒。
该测试与首轮截断堆栈涉及的另一个 Txn 测试独立 race 各十轮通过，共 2.212s。

这些证据支持“附加全包 race 命令累计耗尽预算”，不支持“这两个测试存在固定死锁”；
不能追溯补全首轮丢失的输出，也不能把两次全包 race 失败改记为通过。原 CI 的非 race
全包与 race 分组范围全部通过，预算未改；新镜像仍须经过 CI 和独立审计才能部署。
复现日志 `full-race.log` SHA-256：
`f578ab20696425f1aa43733a0085d8418dceb8d843f853807fa73c216ac27702`。

另外补齐 `pod-log-capture` 的 dbaas CI 触发路径、vet 和 race 步骤，工作流契约测试
先在缺失配置上失败、补齐后通过。原有测试预算、self-hosted Runner 和只读权限未改。
最终 build 包及日志采集器 race 回归均通过（2.541s / 4.250s）。

## 私有证据

根目录 `/root/.local/state/kubebrain/apiserver-fault-logs.OmzeV2sN/`：
`deploy-execute.6WIKd2EQ/`、`archive/`、`logs/original-{0,1,2}/`、`logs/replacement-1/`、
`replacement-pvs.json`、`replacement-pvcs.json`、`restore.CbtyhJHi/`、
`lease-observation.l3AePSrP/`、`proven-scratch.T1xcCr7K/`、`postflight.Iuku0zRC/`。
HOLD 已恢复，所有实验 claim 已消费，不可重用。

Watch SHA-256：`28e9790ad30789f285784affb23c340459cba3586ea76c99ac7e6b4cc6542fc0`。
apiserver 日志 SHA-256：`927f77cc95e9155643cf4f2bf23a1b96047620b3ab2f02082ba036c92f4a40fa`。
