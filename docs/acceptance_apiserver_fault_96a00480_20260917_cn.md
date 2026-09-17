# 96a00480：真实 kube-apiserver 连续更新与 leader 故障验收

## 结果及边界

2026-09-17，本轮 **20 个 ConfigMap × 100 次更新及 2000 条 MODIFIED 完整性校验通过**。
原生工作负载、清理、归档，独立 Watch 审计、成员日志采集，以及部署驱动的恢复和
预拉取资源清理均退出 0。原固定镜像已恢复；本轮 14 个临时卷精确清理完成。

**不是所有 Kubernetes 写请求均成功，也不是整体生产就绪。** 故障期间
kube-apiserver 自身的 coordination Lease 更新仍出现一次 `leader changed`。
本轮 ConfigMap 负载没有失败，不覆盖该内部写失败，也不覆盖之前的 ConfigMap Txn
失败记录。没有增加代理不确定写重放、外部 PATCH 重试或 Watch 重启来获得通过。

## 来源与环境

- 源码：`96a004800858bdd5cc8db50fbac8264903d387b2`。
- [镜像 CI 35174037137](https://github.com/fivetime/kubebrain/actions/runs/35174037137)
  与[回归 CI 35174037159](https://github.com/fivetime/kubebrain/actions/runs/35174037159)
  均为同一源码、attempt 1、成功。
- 独立镜像审计会话 25182 退出 0；验证 OCI index/平台摘要、二进制源码与版本、
  build time、TiKV client fork/grpc 依赖及非 root 用户。审计不等于运行时验收。
- index：`sha256:08bec90bf41f63bf6875597125aeaf4ff96f50d1f0159714c0d7e3b14bb7caaa`。
- amd64：`sha256:e39c6e8784b40e8bfb9f602949e51c0dd505f2ed5b5852f98951f6ceb955620f`。
- arm64：`sha256:fc56d971b26e2b8d3c5bf20640de7f51e20690cb1826891bc30902a4f641a5c9`。
- 专用 `kubebrain-local` / 独立 `kb-local` PD/TiKV 后端、TopoLVM 本地盘，默认 2PC。
  旧 Ceph 前端与后端未切换。环境身份见 [TopoLVM 测试记录](topolvm_test_storage_cn.md)。
- 同一官方 kube-apiserver v1.36.1；20×100、无预更新暂停、外部 Watch 重启数 0，
  沿用原超时和清理门限。03:01 前工作负载完成，不是全请求延迟分布或长稳压测。
- 上轮空 lease 于 02:51:10 UTC 自然失效、列表清空后才准入；未撤销共享端点未知归属
  lease。上轮退出 70 的历史结果保留，不追溯改判。

## 故障与诊断时间线（UTC）

| 时间 | 证据 |
| --- | --- |
| 02:56:25 | 已观察到 3 条 MODIFIED；以 UID/RV 前置条件、grace 0 删除 leader `kubebrain-local-2`，UID `9d9b09ee-3c7f-4604-a0a7-ff5ddb6f0e02` |
| 02:56:26.047 | 入口 `kubebrain-local-1` 向旧 leader 转发自身 Lease Txn；紧接着转发 `soak-9` Range |
| 02:56:27.232 | `kubebrain-local-0` 记录 acquired lease / start leading，不能等同所有初始化已完成 |
| 02:56:27.268–27.272 | 入口旧 peer Health/Check 被取消、共享 client 重置，开始连接新 leader；旧 Range/Txn 返回 leader changed |
| 02:56:27.275 | apiserver 自身 Lease 更新失败，trace 1230ms；保留为未解决的写可用性差距 |
| 02:56:27.641 | `soak-9` GET 完成，trace 1593ms；后续 ConfigMap 更新继续 |

此次旧 peer 检查未再等待上轮可见的约 5 秒超时，符合新增监控取消路径的预期。
日志时钟与执行调度不能单独证明精确因果关系；本轮 successor 与 ingress 不同，
上轮 ingress 自己成为 successor，不能当作严格同拓扑 A/B。1593ms 是单个已记录 GET，
不是所有请求的最大延迟，也不能据此宣称全部 5 秒 SLO 已证实。
内部 etcd client 有安全读重试及连接错误重试；“外部 Watch 未重启”不代表所有内部
watch/连接从未重连。泛化 Txn 的不确定结果仍不重放。

## 恢复、清理与证据

- 驱动会话 88099 退出 0：`operation_exit=0, restore_exit=0, holder_cleanup_exit=0`。
- `archive/result.json`：`operation_exit=0, cleanup_failed=0, archive_failed=0, runner_exit=0`。
- `watch-audit.exit=0`、`log-capture.exit=0`；四路日志重新核对 SHA256，均有开流前后身份
  检查。原 leader 流 EOF，其他流主动结束；`complete_history=false`，不宣称完整历史。
- 测试 prefix 为空、`lease list` 为 `found 0 leases`。
- StatefulSet generation 8→9→10；完整原 spec、runtime image identity、Ready/updated 3
  恢复为 `sha256:50b9938fe3e5ad379136957438abe3c2d4bf250dc6c5973c869e0ef1341d1537`。
- 两次 postflight 通过：六个本地 PD/TiKV Pod UID/containerStatuses 未变，后端 StatefulSet
  UID/generation/spec 未变，旧 Ceph 前端 UID/generation/spec 未变且 Ready 3。
- 清理验证会话 84315、执行会话 56633 均退出 0。三阶段历史 Bound PV/PVC 的 UID 与
  volumeName 绑定证明 14 个临时卷；逐卷核对 Released/Retain、完整 spec、替代 PVC
  已绑定不同卷、无 VolumeAttachment，再以 UID/RV/spec/phase 前置条件改变回收策略。
  14 个临时卷已删除，临时数据不可恢复；所有非目标 PV UID/spec 未变。
  最终 12 Bound、2 历史 Released；历史两个证据不足的卷未删除。
- 本轮三个辅助二进制及独立审计的一个辅助二进制均校验摘要后删除，源码/日志/收据保留。
  HOLD 已恢复，执行声明已消耗，禁止重跑本轮脚本。

私有证据：`/root/.local/state/kubebrain/apiserver-fault-96a00480.XsIVaaMA/`；
部署 `deploy-execute.Qx1fkOmX`、恢复 `restore.mifSZ1zW`、最终复查 `postflight.7S9nL5Yw`、
临时卷清理 `proven-scratch.hC2kIeEU`。
镜像审计：`/root/.local/state/kubebrain/release-96a00480.yeI5nxKJ/audit.RZSZ9vY1/`。

Watch 归档 SHA256：`e828399e2f795ad047b18d266f986688dfd03034071918ed8488f6c986a7a6a2`。
apiserver 日志 SHA256：`9123256bbae916c096fc52c4978615afa8b3260b429834d58ba40b311b452844`。

后续重点仍是故障窗口写可用性及其与参考 etcd 的差异、带负载回滚、更完整的
Kubernetes/controllers/规模与长期故障测试；不得用本次单轮通过替代这些要求。
