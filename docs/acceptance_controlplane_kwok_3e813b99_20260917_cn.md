# 3e813b99 候选 KWOK 控制面测试

2026-09-17：操作检查通过，但固定 60 秒后端租约清理失败，整体退出 70。
原镜像已恢复，临时卷已回收。不是生产就绪、真实容器运行、规模或 HA 验收。

## 来源与范围

源码 `3e813b998ff78d8da6f34894512f9d9619a5271e`；镜像 CI `35200203854`
和探针 CI `35200243908` 同源成功。独立发布审计为
`release-3e813b99.eExmyBVn/audit.ZBtxjwnh`，部署前核对审计产物。
候选镜像固定为
`ghcr.io/fivetime/kubebrain@sha256:172a8b6caceac6a4094354e7589f1bcf972acc9ba3e2e3cfae3099690e62ddde`。

后端继续使用独立磁盘上的 TopoLVM、PD/TiKV 和默认 2PC；未重新初始化磁盘，
未改旧消费者 Ceph 或 secondary 集群。前轮租约自然到期核验完成后，重新准入，
执行 `deploy-execute.NwtFjeHi`。前端 generation 18→19→20。

## 操作证据及边界

固定官方 v1.36.1 apiserver/controller-manager/scheduler，加固定源码本地构建的
KWOK；隔离 PKI，禁用匿名认证，组件使用独立非管理员身份。
实际身份、授权及拒绝越权检查通过。真实 Deployment→ReplicaSet→三个 Pod
所有权链、调度绑定及审计写入者检查通过。

KWOK 状态审计通过：三个模拟 Pod Running/Ready，Deployment readyReplicas 和
availableReplicas 均为 3。NodeLease UID 为
`7f4f657c-7adc-47a2-a760-9d2803aa4d70`，renewTime 从
`09:18:04.498554Z` 增至 `09:18:14.848300Z`。没有实际运行容器。

保留以下异常，不报告零错误：

- KWOK 启动时一次 CreateLease 403，随后同步预创建租约并成功续租。没有扩大
  Lease create 权限。启动 informer 缓存尚未同步是待验证假设，不能作为已证实根因。
- KCM 四次 informer 版本进度重试（34783<34796、34796<34801、
  34801<34805、34805<34808），不是已证明的线性一致 Range 陈旧读。
- 前端四次 Maintenance/Status canceled 警告，另有空 manifest 及 scheduler
  停止时 broadcaster 日志。三成员日志采集成功，未检出所搜索的 Count/DataLoss/
  invalid payload 错误；不代表覆盖所有错误路径。

## 清理与恢复

子结果 `operation_exit=0, backend_cleanup_exit=1, runner_exit=70`；
外层 `operation_exit=70, restore_exit=0, holder_cleanup_exit=0`。
专属前缀的 220 个键已删除并核验空，未 Revoke/KeepAlive 或调低事件 TTL。

09:22:12 UTC，恢复原镜像后只读检查发现两条无附着键的租约：
`0003a0aea810bc02` TTL 3624 秒，`0003a0aea810bc04` TTL 3623 秒，
granted TTL 均为 3660 秒。预计约 10:23 UTC 到期，**尚未核验自然到期**；
不能假设切换镜像及领导权前后到期时刻不变。下一轮共享负载须重新满足零租约准入。

`restore.gMFC1cy2` 恢复原完整 spec、bb89c3f8 固定镜像及 Ready3。
独立后置检查 `postflight.Z8VZRgYb`、回收后 `postflight.76QIFSAj`（退出 0）
确认原运行时、六个 PD/TiKV Pod 身份及其他 StatefulSet 未变。

仅针对本轮历史 Bound PV/PVC 快照证明的 12 个临时卷，核对精确 UID/spec、
Released/Retain、现用替代 PVC 及无挂载后回收。
`proven-scratch.rbnEeWsF` 的前后快照再次核验：12 个目标全部消失，其余 PV
UID/spec 完全一致。删除不可恢复；两个历史证据不足的 Released 卷保留。
最终为 12 Bound、2 Released。未重复执行部署或回收脚本。

私有证据根目录：
`/root/.local/state/kubebrain/kwok-candidate-3e813b99.ZNyd87Zs/`。
工作负载目录 `controlplane-kubebrain.yfZdM1ie`，残留检查
`residual-inspection.ywz0JbUV`。认证材料不入库。
