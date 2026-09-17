# 742e 产品 / ab043 夹具：控制面 Pod 替换验收

后续自然到期核验：13:06:12 UTC，`expiry-observer.qjOVU1iy/sample-115`
中两条租约 TTL 均为 -1、无键，LeaseList 为零，原前缀 Range 含响应 header
且无 KV/count。固定 Pod 与 cluster/member 身份检查通过，观察进程退出 0，
18383 转发已释放。未 Revoke/KeepAlive。**不改变原 60 秒清理失败和整体退出 70。**

2026-09-17：**Pod 替换操作及状态/审计校验通过；原 60 秒租约清理失败，
整体退出 70。原镜像恢复及本轮临时卷回收完成。** 不代表生产就绪、真实
容器运行、节点故障、规模或持续性能验收。

## 固定版本与准入

产品源码 `742e8b8c2196e985b208394e79007262570dcb36`，固定镜像
`ghcr.io/fivetime/kubebrain@sha256:1be08597afee17775f0810e6584421430f0b046a21182df8b7f5b3be441205e0`。
镜像身份审计见[前轮报告](acceptance_controlplane_kwok_742e8b8c_20260917_cn.md)。
本轮夹具源码 `ab043b11d2c3e245c84c96698b6c2dd8ecd04903`；与产品版本的
差异为测试、文档及 CI，没有声称现有镜像包含 ab043。

回归 CI `35216878670` attempt 1、精确 ab043 HEAD，全部成功。
工作流合约 2.288s、控制面夹具 race 8.647s、服务整包 144.258s，
Auth/Lease/Watch race 分别 85.391/77.142/26.342s，完整探针 race 293.604s。
周期进度测试在普通与扩展 race 中均有 PASS；新增 Pod 替换负例、租约锁
取消及恢复认证应用屏障测试均包含在归档中。未重新构建产品镜像。

前轮租约自然到期后，新鲜零租约准入 `deploy-verify.YAsAe0FB` 通过。
执行 `deploy-execute.mYAjUfF4`，候选 generation 23；独立 TopoLVM 后端、
默认 2PC、原 Ceph 实例不变。候选部署与恢复均有一个副本未 Ready 的窗口，
不能报告零可用性影响。

## 实际行为与范围

固定 Kubernetes v1.36.1 apiserver/controller-manager/scheduler 和本地 KWOK。
独立组件身份、RBAC 越权拒绝、Deployment→ReplicaSet→三个 Pod 所有权链、
调度绑定、KWOK 状态写入及 NodeLease 续期检查通过。

12:01:41.509533 UTC，测试管理员以 UID 和 resourceVersion 双重前置条件
删除 `chain-749846d549-849r6`，HTTP 200：

- 旧 UID：`e6a6c764-987b-4f40-93b2-5b6a50c621d9`，RV `35365`。
- 新 Pod：`chain-749846d549-9xrlx`，UID `5a3d695a-6338-4ee0-aafe-f07667e9c116`。
- 另外两个 UID `a87de347-9cf1-4ebb-a43d-158a1ce7a74e`、
  `5602ec39-d7cb-483f-afbc-ca6d8b9d275c` 保持不变。

同一 Deployment/ReplicaSet 回到三个 Ready/available；状态与审计校验均为
true，分别证明新对象由 ReplicaSet 控制器创建、scheduler 绑定、KWOK 写入
Ready，而非管理员直接伪造。KWOK 不实际运行容器。

保留异常：KCM 六次 read-version 进度重试、四次 Deployment 对象已修改冲突。
前者不能直接判定为线性一致 Range 陈旧读。前端一次 KV/Txn context canceled
及对应 scheduler Lease 转发失败，一次 Maintenance/Status context canceled；
发生在测试尾部，但没有证据将其全部确定归因于退出。三成员日志采集成功，
未检出所搜索的 DataLoss/count mismatch/invalid range payload，不代表零错误。

## 清理、恢复和待观察项

子结果 `operation_exit=0, backend_cleanup_exit=1, runner_exit=70`；外层
`operation_exit=70, restore_exit=0, holder_cleanup_exit=0`。删除专属前缀的
222 个键并确认空，但两条长租约超过原 60 秒清理门限，不能追溯改判通过。
未 Revoke/KeepAlive、缩短 TTL 或放宽门限。

`restore.DirdrHj9` 恢复完整原 spec 和 bb89c3f8 固定镜像，generation 24、
Ready3、revision `kubebrain-local-568bd68448`。独立最终检查
`postflight.P6qGTO9I` 确认恢复运行时、后端及其他 StatefulSet 身份不变。
HOLD 已恢复，已消费的驱动不得重跑。

`proven-scratch.7ul9TBEO` 精确回收有历史 Bound 证据的 12 个临时卷；删除
不可恢复，其他 PV UID/spec 不变。两个历史来源证据不足的 Released 卷保留，
最终 12 Bound、2 Released。

恢复后 12:07:20 UTC，只读检查租约 `0003a0af3ddd1f02`、
`0003a0af3ddd1f03`，剩余 TTL 均 3516 秒、无附着键。
历史：只读观察 `expiry-observer.qjOVU1iy` 启动后，12:12 UTC 确认进程仍存活，
sample-9 第一条 TTL 3226、granted TTL 3660，当时自然到期尚未核验。
观察固定入口 Pod、cluster/member 身份；不重复启动、不发送续期或主动撤销。
恢复或领导权切换可能影响 TTL，预计时间不作为到期证明。

## 证据定位

私有根目录 `/root/.local/state/kubebrain/replacement-candidate-ab043b11.8cNJXAuE/`，
工作负载 `controlplane-kubebrain.LIVHF5R0`，CI 归档 `ci-archive.KqlxbEC0`。
CI 原始日志 SHA-256：
`3b42beca3f68209e9d5f4e2219058406af1d732ef0bd10f47a8a28e10108f5f7`。
认证材料不入库。完整控制器、规模/长期负载及存储/节点故障等生产验收仍未完成。
