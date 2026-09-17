# 742e8b8c 候选控制面/KWOK 回归

10:50 UTC 后续：已启动只读自然到期观察（session 25778，证据目录
`expiry-observer.EZ1YH2TQ`），首次两条 TTL 均为 3214 秒，恢复后固定 Pod
及 cluster/member 身份检查通过。仍未证明到期；观察期间临时占用 18383，
不发送 Revoke/KeepAlive，不改变下述原清理失败结论。

2026-09-17：**操作通过，固定 60 秒租约清理失败，整体退出 70；原镜像恢复及
本轮临时卷回收成功。** 不代表生产就绪、真实容器运行、规模性能或 HA 验收。

## 发布与部署

源码 `742e8b8c2196e985b208394e79007262570dcb36`；回归 CI `35207999892`、
镜像 CI `35208578969` 同源、attempt 1、成功。发布审计 session 64983 退出 0，
证据 `release-742e8b8c.HKcubH7A/audit.h59s97aj`，核验 index 原始摘要、平台
选择、发布标签、实际 amd64 二进制源码/版本/BuildTime/Go 与依赖及非 root 用户。
临时审计容器和提取二进制已删除。固定候选镜像：
`ghcr.io/fivetime/kubebrain@sha256:1be08597afee17775f0810e6584421430f0b046a21182df8b7f5b3be441205e0`。

前轮租约于 10:22:58 UTC 实际自然到期。新鲜 generation 20 快照及零租约准入
`deploy-verify.5AxcsfBl` 通过后，执行 `deploy-execute.VTCIuetp`，驱动 session
40243。隔离预拉取成功，generation 21 三成员候选 Ready；滚动期间存在一个
副本未 Ready 的窗口，不报告零不可用窗口。独立 TopoLVM PD/TiKV、默认 2PC
和原 Ceph 实例未改动。

## 操作与诊断

固定 v1.36.1 真实 apiserver/controller-manager/scheduler，加固定本地 KWOK。
独立身份及 RBAC 拒绝越权检查、Deployment→ReplicaSet→三个 Pod 所有权链、
真实调度绑定和实际写入者审计均通过；三个模拟 Pod Running/Ready，Deployment
ready/available 为 3。并非真实容器运行，也不是现场注入状态锁阻塞的取消测试。
租约取消修复的普通/race 回归证据见 [对应报告](acceptance_lease_read_admission_20260917_cn.md)。

NodeLease UID `550abc95-0737-4db2-9b5b-c17d32c23a4d` 与预创建对象一致，
renewTime 从 `10:39:01.196150Z` 增至 `10:39:11.513213Z`，KWOK 审计为 true。
保留以下异常，不能报告零错误：

- 10:38:50.980353 UTC，KWOK Lease List 200，响应 RV 35052，实际包含预创建
  reference-node（创建 RV 35050、同一 UID）；随即 10:38:50.981584 Create 403。
  后续同 UID Update 成功。这次明确不是 List 返回空列表，与参考 etcd 上复现的
  启动 informer 缓存时序解释一致；没有扩大 Lease create 权限。
- KCM 三次 informer 版本进度重试：35071<35082、35082<35087、35087<35092。
  这些日志不是已证明的线性一致 Range 陈旧读，仍保留后续追踪边界。
- 前端三次 Maintenance/Status context canceled。三成员日志采集退出 0，前后
  身份一致；未检出所搜索的 Count/DataLoss/invalid range payload 错误，不代表
  所有错误路径覆盖。

## 清理、恢复与残留

子结果 `operation_exit=0, backend_cleanup_exit=1, runner_exit=70`；
外层 `operation_exit=70, restore_exit=0, holder_cleanup_exit=0`，驱动终态 70。
删除本轮专属前缀的 220 个键并确认空；长事件租约没有在固定 60 秒内自然消失。
未 Revoke/KeepAlive，未调低事件 TTL 或放宽门限。

`restore.BOeEaZSi` 恢复完整原 spec、固定 bb89c3f8 镜像及 Ready3，generation 22。
`postflight.qqv0fIfe` 和回收后的 `postflight.KFmTZSCg` 独立确认原运行时、六个
PD/TiKV Pod 身份、其他 StatefulSet 未变。HOLD 已恢复，已消费驱动不得重跑。

只读临时卷核验 session 24931 退出 0；回收 session 89483 退出 0，证据
`proven-scratch.8HfVHzAq`：本轮历史 Bound PV/PVC 快照证明的 12 个临时卷全部
删除，其他 PV UID/spec 不变。删除不可恢复；两个历史来源未充分证明的 Released
卷保留，最终 12 Bound、2 Released。

恢复后只读残留检查 session 46726 退出 0：10:43:39 UTC，租约
`0003a0aef224d802`、`0003a0aef224d804` 均剩余 TTL 3595 秒、granted TTL 3660，
均无附着键。预计约 11:43:34 UTC 到期，**尚未核验自然到期**。恢复或领导权
切换可能影响截止时间，不将恢复前后到期时刻视为不变。下一轮共享负载仍须
新鲜零租约准入。转发已回收，18383/18453 无监听。

私有证据根目录 `/root/.local/state/kubebrain/kwok-candidate-742e8b8c.fVVvI0Fy/`，
工作负载 `controlplane-kubebrain.fkDZqBbg`，残留 `residual-inspection.hSlHgjku`。
认证材料不入库。自然残留、完整控制器/规模/长期负载和故障验收仍不能由本轮代替。
