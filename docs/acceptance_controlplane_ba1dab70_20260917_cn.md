# ba1dab70 候选真实控制面调度测试

后续自然到期核验：09:14:03 UTC，只读观察 session 38538 成功退出 0，
`expiry-observer.GREJGdFN/sample-78` 确认两条 TTL=-1、LeaseList 为零、
原前缀为空，恢复后的入口 Pod 身份未变。未执行 Revoke/KeepAlive，转发已停止，
18383/18453 已释放。这补齐残留自然消失证据，不改变原 60 秒清理失败结论。

2026-09-17：**操作通过，60 秒租约清理失败，整体退出 70；原镜像恢复和临时卷回收成功。**
不是完整生产验收，也不证明真实容器运行、规模性能或控制面 HA。

## 来源与准入

源码 `ba1dab709c4a6362552cfc5ab0876bbbfbee55ca`；镜像 CI `35193444673`、
探针 CI `35193444669` 同源、attempt 1、成功。独立发布审计
`release-ba1dab70.YKfEnhFq/audit.jLnRBmhQ` 成功，部署前再次校验。
镜像固定为 `ghcr.io/fivetime/kubebrain@sha256:d08ad362bef61f13ba00b15700ef8810854cfd3055b988e1bb6d52b7448f0fff`。
实际运行时摘要按审计的 index/amd64 摘要核验，三个成员 Ready 后才启动负载。

前一轮旧基线的两条租约于 08:05:16 UTC 被只读观察确认自然过期，LeaseList 为零，
原前缀为空。随后重新读取原始 spec/runtime/backend/PV/PVC 快照，通过只读准入
`deploy-verify.u2WMZyPf`；执行尝试 `deploy-execute.rxHhBfrd`，驱动 session 83888。
隔离预拉取成功；`kubebrain-local` generation 16→17，三成员运行候选。
PD/TiKV 仍为独立 TopoLVM 本地盘后端，未改事务模式、存储配置或旧 Ceph 集群。

## 操作结果与错误边界

使用固定官方 v1.36.1 apiserver/controller-manager/scheduler、隔离 PKI、Node/RBAC，
禁用匿名认证；控制器及调度器使用独立非管理员身份，身份和拒绝越权检查通过。
实际创建一个 Deployment，由真实控制器产生一个 ReplicaSet、三个 Pod，真实
scheduler 完成三次绑定；UID 所有权链、observedGeneration、PodScheduled 和
审计实际写入者均通过。仅使用一个模拟 Ready 节点，Pod 并非真实容器 Running。

本轮完整保存三成员服务日志（共 2032 行，采集退出 0）、控制面日志及审计，
采集前后 Pod 身份一致。未检出旧基线的 `DataLoss` / `invalid range payload` /
`count 0 for 1 key-values`，是该负载下的修复对照，不代表所有错误场景覆盖。
仍有以下记录，不能报告零错误：

- Deployment 两次资源版本冲突，随后重试完成。
- ReplicaSet 两次 informer 进度检查失败：34505<34519、34519<34523；与旧基线
  一样，此日志不是一次已证实返回陈旧值的线性一致 Range，仍需后续追踪 Watch 交付。
- apiserver 启动期有 kube-system 尚未创建时的 Lease/ConfigMap 重试，另有空 manifest
  和 endpoints 清理日志；服务日志包含一次 Maintenance/Status context canceled，
  scheduler 停止时记录 event broadcaster 已停止。保留原始日志，不自动归因于存储缺陷。

## 清理、恢复和残留

子结果为 `operation_exit=0, backend_cleanup_exit=1, runner_exit=70`。
停止三个本地组件后删除本轮专属前缀的 211 个键并核验空；两条租约未在固定
60 秒窗口内自然过期，没有 Revoke/KeepAlive，也未调低事件 TTL。

外层结果为 `operation_exit=70, restore_exit=0, holder_cleanup_exit=0`。
恢复目录 `restore.Hgx5wklN` 验证原完整 spec、原镜像运行时摘要及 Ready3，
generation 18；HOLD 已恢复，消费过的驱动不能重跑。独立后置检查
`postflight.Gx0pa6wO` 确认六个 PD/TiKV Pod 身份和容器状态未变，其他 StatefulSet
未变。实际剩余键及租约检查 session 59833 退出 0，入口固定到恢复后的原镜像：

| 08:13:51 UTC Lease ID | granted TTL | 剩余 TTL | 附着键 |
| --- | --- | --- | --- |
| `0003a0ae691ea602` | 3660s | 3587s | 无 |
| `0003a0ae691ea603` | 3660s | 3587s | 无 |

这是恢复后的观测，不能将恢复/领导权变化前后的到期时刻视为不变；未逐次记录
恢复期间租约 TTL，不能由此归因于某一版本。预计约 09:14 UTC 自然过期仍需核验，
不是已清理证明；下一次共享负载仍须重新满足零租约准入。18383/18453 已释放。

临时卷只根据本轮部署前及候选 Ready 后两个 Bound PV/PVC 快照确定，逐个验证
UID、完整 spec、Released/Retain、不同 UID 的现用替代 PVC 和无 VolumeAttachment。
只读核验 session 56808 成功；执行 session 10698 成功，证据
`proven-scratch.aWdCGk3G`：12 个精确临时卷已删除，其余 PV UID/spec 保持不变。
删除不可恢复；两个历史来源证据不足的 Released 卷未删除，当前业务及后端卷未动。

## 证据与后续

私有根目录 `/root/.local/state/kubebrain/controlplane-candidate-ba1dab70.EaxkqpNF/`，
工作负载 `controlplane-kubebrain.KbucQpEs`，残留检查 `residual-inspection.Z23MeWcK`。
认证材料不入库。本报告记录一次候选部署，不替代持续负载、故障语义、完整控制器、
KWOK 规模或生产就绪证明。应继续明确默认长事件租约与测试清理契约的差距，
不能为了让当前门限通过而改变产品租约语义或追溯改写本轮失败。
