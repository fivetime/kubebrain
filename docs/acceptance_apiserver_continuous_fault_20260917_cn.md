# 真实 kube-apiserver 连续更新期间 leader 故障（2026-09-17）

## 结论

**未通过。** 连续更新期间硬删除 KubeBrain leader Pod 后，ConfigMap `soak-9`
的 PATCH 因底层 Txn 返回 `etcdserver: leader changed` 失败。归档保留了原始
Watch 和 apiserver 日志；不能把恢复成功或较早的 6000 次 SDK 验收通过当成本轮通过。

本轮未改变门限、未重试失败的 PATCH、未重启公共 Watch。不是此前的空 lease
清理超时，也不是此前丢失 Watch 归档的问题。操作退出 1，清理及归档均成功。
外层完整性审计因缺少事件退出 5；部署驱动最终为
`operation_exit=5, restore_exit=0, holder_cleanup_exit=0`，两个层级的退出码不可混用。

## 配置和时间线

- 官方 kube-apiserver v1.36.1，二进制 SHA-256
  `9b4dba0a5b945f1fe0ce18f47535c5ff0c46ae384f9222047bce39fe91b6023e`。
- 已审核源码 `8ac67ca3b21423e3d87c3c882850ce655eb23dd9`；镜像 index
  `sha256:99535eb62b41d78de573c1d7162a20946424d33a4aa30e8324a3ef2adaf644a7`。
  发布前验证沿用[升级报告](acceptance_local_upgrade_8ac67ca3_20260917_cn.md)中的精确 CI/镜像审计，
  本次另做新鲜的预拉取、身份及运行时摘要验证。没有部署后续 df1717d4 镜像。
- 独立 `kubebrain-local` / `kb-local`，TopoLVM 本地盘、默认 2PC；没有修改旧 Ceph 实例。
- 20 个 ConfigMap，各更新 100 次，共要求 2000 个 MODIFIED；`PRE_UPDATE_SLEEP_SECONDS=0`。
  先确认 Watch barrier，再在更新循环未完成时注入故障。没有 controller/scheduler 接入，
  因而不代表完整 Kubernetes 控制面的验收。
- 01:11:48 UTC，UID/resourceVersion 前置条件保护下强制删除 `kubebrain-local-2`，
  UID `8694f338-d1cb-436c-814f-b54f210953a2`。故障前 leader ID `1543124563`、term 12；
  apiserver 经 follower member `2176893298` 进入。注入前观察到 3 个 MODIFIED，
  这只是注入时机证据，不证明某个特定 RPC 恰好正在提交。
- 01:11:49.072，`soak-9` PATCH 开始；01:11:54.499，底层 Txn 返回 Unavailable /
  leader changed，事务调用约 5426ms、整个 PATCH 约 5427ms。客户端日志 `attempt=0`。
- 原始 Watch 共 28 个事件：20 ADDED、8 MODIFIED；未满足 2000 个 MODIFIED 完整性要求。
  不能从失败响应或缺少剩余事件推断该事务一定未提交，也不能推断已发生数据丢失。

## 已核实的语义与尚未解决的问题

实际 apiserver 日志使用 etcd client v3.6.8。其 `retry.go:isSafeRetryMutableRPC`
仅对没有可用地址/连接的特定 Unavailable 描述允许重试写请求；普通 leader changed
不满足条件。其目的为保持写操作至多一次，不能把任何 Unavailable 都解释成可安全重放。

KubeBrain 的 `forwardUnaryWithDrainRetry` 只对明确的“peer 在接纳请求前已 drain”
信号进行有限重试。普通 leader changed 或内部连接取消不能证明未提交。
新增 `TestTxnDoesNotReplayAmbiguousForwardResult` 通过真实 gRPC 上游模拟“先产生副作用，
后返回 leader changed / 内部 Canceled / 普通 Unavailable”，要求保留错误且副作用仅一次。
该测试保护转发安全边界，**不是对真实 TiKV commit 的模拟验收，也没有修复本次可用性失败**。

本轮新增及相关定向 race 测试通过（1.164s）；整个转发模块
`go test -race -count=1 -timeout=5m ./pkg/server/service/etcdproxy` 通过（3.390s）。
这些为本地源码测试，不是新 CI 或新镜像验收结论。

本地参考 etcd 源码 `5cd9f4ee13801e18825d661e5005ae599460bc3a` 的
`server/etcdserver/read/read.go` 在 ReadIndex 等待期间遇到 leader 变化可返回
ErrLeaderChanged；`v3_server.go` 的另一处同名错误属于 LeaseTimeToLive，不能据此声称
参考 etcd 的写 Txn 在同一场景必然产生相同错误。
后续[参考 etcd 对照](acceptance_reference_apiserver_fault_20260917_cn.md)已完成：相同
真实 apiserver 更新负载在本机三成员参考 etcd leader SIGKILL 后通过 2000 次完整性校验。
两次拓扑与注入时机不同，不能视为严格性能 A/B，但不能把本次失败解释为参考必然行为。
**仍缺 KubeBrain 请求阶段证据以定位差异根因**；不通过改成无条件重试来消除差异。

## 恢复、后检和待清理内容

自动恢复原镜像 `sha256:50b9938fe3e5ad379136957438abe3c2d4bf250dc6c5973c869e0ef1341d1537`，
generation 4 → 5 → 6，原完整 spec 和运行时镜像验证通过，预拉取资源清理成功。
故障脚本因完整性失败提前退出，原脚本后检没有执行；之后独立只读后检补齐：

- 原前端完整 spec/UID、Ready/updated 3/3、current/update revision 一致。
- 本地 PD/TiKV StatefulSet UID/generation/spec 不变，六个 Pod UID/containerStatuses 不变。
- 旧 Ceph 前端 UID/generation/spec 不变且 Ready 3/3。

首次后检发现本地 PV 为 12 Bound、14 Released。随后从部署前及候选阶段两份完整快照
确定 12 个历史 Bound scratch 卷，逐一核对 UID/full spec、Released/Retain、同名替代
PVC 已绑定不同卷、无 VolumeAttachment，再以 UID/RV/full spec/phase 前置条件将
这 12 个 PV 回收策略改为 Delete 并等待删除。临时数据已删除、不可恢复；清理会话
84716 终态 0。全部非目标 PV 的 UID/spec 前后不变，未修改 StorageClass 或旧 Ceph 卷。

故障重建时产生的另 2 个卷缺少历史 Bound 快照，**仍保留，不按 Released 状态批量删除**。
最终本地 PV 为 12 Bound、2 Released，LogicalVolume 数为 14；独立后检再次通过，
证据 `postflight.YPNNzekb/`。精确清理证据 `proven-scratch.OLqo3hsI/`，单次 claim 已消费。
`uid-delete` 和 `tools/image-prepull` 两份生成二进制按 `helpers.sha256` 验证后删除，
源码及证据保留、可重新编译。保留原始证据，不重复使用实验 claim；HOLD 已恢复。

## 证据

私有证据根目录 `/root/.local/state/kubebrain/apiserver-fault-archive.gPggHmCc/`。
主要文件：`archive/result.json`、`archive/configmap-watch.jsonl`、`archive/kube-apiserver.log`、
`execution.log`、`fault-time.txt`、`pre-fault-status.json`、`target-admission.json`、
`deploy-execute.ExI8VKas/result.json`、`restore.EpcChp3o/`、`postflight.mUYDzR95/`。

原始 Watch SHA-256：`7fdfcfda10d481be4e6124db841aacbce86e27945c84928ef38a111d9e42fbc4`。
apiserver 日志 SHA-256：`bc9d886eaf623dc43c040e556dfbe2a1bc113c3a1971110fe23d07286433bc6b`。
证据目录不进入仓库；不保存密码、私钥或 kubeconfig 到测试文档。
