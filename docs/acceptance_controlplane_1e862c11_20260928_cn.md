# 固定候选控制面复验：2026-09-28

状态：本用例通过。07:10:52 UTC 完成控制面功能与长租约清理，07:13:13 UTC
完成基线恢复和隔离资源清理；三个阶段退出码均为 0。不代表本次首版全部验收完成。

## 候选与用例

- 产品源码：`1e862c110a82081e68ffa9612de4893d74decbc6`。
- 镜像：`ghcr.io/fivetime/kubebrain@sha256:8aa38da6669729ecff864008e5e92df51be754f59e138a7b32424d1fca11589c`。
- 复用 `hack/scale-lab/controlplane-smoke.sh`，固定 Kubernetes v1.36.1
  API Server、controller-manager、scheduler 和既有 KWOK 二进制摘要。
  验证 Deployment/ReplicaSet/三个 Pod 及一次 Pod 替换；KWOK 只模拟节点和
  Pod 状态，不代表真实容器运行或宿主机故障验收。
- 本地入口包含已通过普通/race/参考 etcd 验证的清理修复，尚无新 CI 覆盖。
  运行前固定入口摘要，不能把已发布产品镜像版本与本地测试入口版本混为一谈。
- 独立 TiKV/PD、TopoLVM、本地 keyspace、默认 2PC；不操作历史 Ceph 实例。

## 已执行与当前状态

1. 基线核对：StatefulSet UID 不变，generation/observed=138/138、Ready=3，
   原固定镜像 `50b9938f…`。完整 TLS 检查及固定 cluster/member 身份通过，
   零租约、零告警。组件摘要及证书有效期检查通过。
2. 首次尝试因 dry-run 与正式预拉取共用收据名称被拒绝：
   `openat prepull: file exists`，退出 1，未部署候选，无遗留预拉取 Job。
   该失败保留，不改判通过。
3. 修正本次调用参数，dry-run 使用 `admission`、正式使用 `prepull` 收据，
   在全新目录执行。dry-run 再次通过，但预拉取未在原 600 秒内完成；最终
   报 API rate limiter 等待超过截止，驱动退出 1，未部署候选。最终事件显示
   三节点在约 10 分 22 秒完成拉取（镜像 2050823053 bytes），晚于窗口，
   不改判通过。Job/Pod/隔离策略已清理，完整原 spec、generation、所有原 Pod
   UID/容器状态前后比较一致，原服务仍 Ready=3。
4. 镜像现已进入节点缓存，作为明确的新环境信息，启动一次同候选、同 600 秒
   预拉取窗口的重跑。未更改入口内部重试策略、降低功能负载或更改产品验收门限。
5. 重跑预拉取成功；固定候选已滚动部署，generation=139、Ready=3，三个成员
   的实际镜像摘要和 `kube-brain version` 源码提交均核验通过。滚动中出现成员
   未 Ready 窗口，不报告为零影响升级。
6. 06:10 UTC 核对：`operation-result.json` 为 operation_exit=0、cleanup=pending。
   控制器链路、3 Pod 调度与 KWOK 状态、Pod 替换状态和审计检查通过。功能结果
   不等于清理或整体通过。随后两条 GrantedTTL=3660 的空租约在固定预算内
   自然到期，最终零租约、专属前缀为空。固定预算为 3660+60 秒，不续租、
   不主动撤销，未在等待期间改回原镜像。
7. 工作负载最终 `result.json` 的 operation/backend_cleanup/runner 均为 0；
   外层 operation/restore/holder_cleanup 均为 0。恢复后完整 StatefulSet spec
   与原快照一致，generation/observed=140/140、Ready=3，三个成员运行时镜像
   回到原固定基线。原 Job/Pod/隔离策略均已清理，18383/18453 无监听。
   07:20 UTC 独立只读核验再次确认原 spec、运行镜像、Ready 与资源清理结果。

预拉取 holder 保持 7200 秒，要求部署前至少剩余 90 分钟，以覆盖默认一小时
Event 租约、功能测试和恢复。只调整外围资源存活预算，不改变既定功能或故障
门限。基线恢复和精确预拉取资源清理均已由最终退出状态及独立后置检查核验。

## 证据与恢复

- 首次失败：`/root/.local/state/kubebrain/controlplane-candidate-1e862c11.IQfd5m9h/`。
- 预拉取超时：`/root/.local/state/kubebrain/controlplane-candidate-1e862c11.T3PVEWjk/`，
  `result.json` 为 operation=1、restore=0（未部署，无需恢复）、holder_cleanup=0。
  事件与独立后置核验见 `prepull-events-final.json`、`postflight-*.json`。
- 通过的缓存就绪后重跑：`/root/.local/state/kubebrain/controlplane-candidate-1e862c11.6atMnXy2/`。
  含部署前快照、发布核验、入口摘要、驱动及日志。凭据只引用私有路径，不入仓库。
- 本次工作负载子目录：`controlplane-kubebrain.cYWLG3LL/`；结果见
  `operation-result.json`、`audit-check.json`、`kwok-audit-check.json`、
  `replacement-state-check.json`、`replacement-audit-check.json`。清理过程见
  `backend-cleanup-budget.txt`、`backend-final-leases.txt`、`backend-lease-*.json`。
- 恢复证据：`result.json`、`restore.log`、`restored-sts.json`、`restored-pods.json`、
  `holder-cleanup.log`。`operation-result.json` 保留功能结束时的 pending 状态，
  清理最终状态以工作负载及外层各自的 `result.json` 为准。
