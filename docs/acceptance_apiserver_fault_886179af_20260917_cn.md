# 886179af 连续更新期间 leader Pod 删除验收

## 负载结果

2026-09-17：真实 kube-apiserver v1.36.1 下，20 个 ConfigMap 各更新 100 次，
2000 条 MODIFIED 独立完整性审计通过。原生结果 operation_exit=0、cleanup_failed=0、
archive_failed=0、runner_exit=0，成员日志采集与独立 Watch 审计均退出 0。
本轮未检出先前的 Txn Range Count DataLoss、内部 Lease 更新失败或
`get revision from leader failed: context deadline exceeded`；这是单次观察，不是这些问题在所有场景消失的证明。

故障期间仍有 Range 的 `Unavailable: etcdserver: leader changed`，不能写成零错误。
本次不替代控制器/调度器、长稳、节点断电、TiKV 存储故障或整体生产就绪验收。

## 来源与门限

- 源码 `886179af3cc203dcb1b52cb01b3b0f2b9db17c08`，默认 2PC；包含
  [Count 修复](txn_failure_range_count_cn.md)和[本机升主时旧 revision 查询退出修复](revision_promotion_barrier_cn.md)。
- 镜像 CI [35183014256](https://github.com/fivetime/kubebrain/actions/runs/35183014256)
  与回归 CI [35183014224](https://github.com/fivetime/kubebrain/actions/runs/35183014224)
  均为同源 attempt 1 成功；独立镜像审计 `release-886179af.pcMGEHC3/audit.n8W0MQGL` 退出 0。
- 固定镜像 `ghcr.io/fivetime/kubebrain@sha256:966a13e70e1482c4c67a3b41632da4c2e0cb90a674231828caa989ad7bca7a54`；
  amd64 实际摘要 `069cee63704713e36bf38b8afd4be936ff2d984c62811d4dd3cb2393f7d2a401`。
- 专用 TopoLVM 本地盘 `kb-local` PD/TiKV 和隔离前端 `kubebrain-local`，旧 Ceph 实例不切换。
- 官方 apiserver SHA-256 `9b4dba0a5b945f1fe0ce18f47535c5ff0c46ae384f9222047bce39fe91b6023e`。
- 唯一前缀 `/registry-kubebrain-apiserver-local-failover-v1361-ai5edvww`；准入时租约及前缀为空。
- 保持 20×100、原超时预算、无更新前暂停、无外部 Watch 重启或 PATCH 重试。

## 故障语义与日志

05:18:00 UTC，在已观察到 5 条 MODIFIED 且负载未结束后，以 UID/resourceVersion
前置条件和 grace=0 删除 leader `kubebrain-local-2`，UID
`d896b057-0b7f-4778-ae22-60b47b31a373`。入口 local-1 随后成为 leader。

旧 local-2 在 05:18:01.937 关闭公共端口、01.951 停止领导权，03.938 关闭内部 peer 端口。
实际执行了关闭流程，不能将 grace=0 API 删除等同于瞬间 SIGKILL、节点断电或已知事务提交结果。
local-1 在 02.739–02.742 明确结束升主前发起的 revision 查询并返回 leader changed；
这是预期的旧查询退出，不保证旧读请求成功，也不允许重放不确定写入。

四条成员日志流覆盖原三个 Pod 及替代 local-2，打开前后检查身份；
`complete_history=false`，不宣称完整历史。日志未检出问题不等于证明未发生。

## 恢复与证据

证据目录 `apiserver-fault-886179af.AI5edVww`，单次部署 `deploy-execute.jE8VFxcN`。
单次驱动 session 74263 终态 0，operation_exit=0、restore_exit=0、holder_cleanup_exit=0。
`restore.x1VP9Aq8` 核验原完整 spec 与运行镜像恢复，generation 12→13→14，三个副本 Ready。
后置检查 `postflight.0WgXcRQC` 通过：PD/TiKV 六个 Pod 身份及容器状态、后端 spec、旧前端未变。

清理前从三组历史 Bound PV/PVC 快照推导目标；核验当前 Released、Retain、UID/spec、
替代 PVC 指向不同卷且无 VolumeAttachment，再以 UID/resourceVersion 条件更新回收。
只读验证 session 95006 和清理 session 4850 均退出 0，回收本轮 14 个临时卷；
其临时数据不可恢复，其他 PV UID/spec 不变。最终 12 Bound、2 历史 Released，后两卷继续保留。
四个本轮自建工具经摘要核验后删除，日志证据保留；18379/18380/18381/18449 无监听残留。
四条成员日志摘要重新核验通过。试验目录已 HOLD，禁止重跑。

归档 SHA-256：

- Watch：`0c02a22da7f256ef679bcfd080d9e7aa8a855318c70cc00554d047a0a0b0d177`。
- apiserver 日志：`5a8a30e7a204539b1a1b7c78c3e6b788e0b40b0afe50392ba4a7661be9e255aa`。

保留[602912b7 失败记录](acceptance_apiserver_fault_602912b7_20260917_cn.md)，
本轮成功不追溯改变其清理失败或内部 Lease 写入失败的事实。
