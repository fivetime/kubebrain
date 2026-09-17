# 31c0a1fb 连续更新期间 leader Pod 删除验收

## 结果与边界

2026-09-17，真实 kube-apiserver v1.36.1 下，20 个 ConfigMap 各更新 100 次，
2000 条 MODIFIED 的原生及独立完整性检查通过。工作负载、租约清理、归档、
成员日志采集、原完整配置恢复和镜像预拉取资源清理均退出 0。

这是一次默认 2PC、独立 TopoLVM 后端上的功能及领导权切换回归，不是整体生产就绪证明。
它不覆盖真实节点断电、网络分区、TiKV 故障、完整控制器/调度器规模与长稳、备份恢复。
也不追溯改变先前控制平面替换实验的 60 秒租约清理失败。

## 来源与门限

- 源码：`31c0a1fb1ebc882c71922bf9ceb17f2ba0e8e59e`。
- 镜像 CI `35226599117`、探针 CI `35226599112`，均为该提交 attempt 1 成功。
  原始探针日志确认租约状态锁准入、两个 epoch 检查和快照取消测试在普通及 race 模式通过。
- 独立镜像审计：`release-31c0a1fb.JzvXbcK3/audit.0b4mXVEm`，退出 0。
- 固定镜像：`ghcr.io/fivetime/kubebrain@sha256:36bed4a260047f6769eeb52b998c886c6e3c563658fd44234a62bf5e4ec3e52b`。
- amd64 摘要：`a44f5c60c6c4b908b4d5867175a1852dc42253f8873adf3492a4e3d83a1f815a`。
- 仅操作专用 `kubebrain-local` 前端和 `kb-local` TopoLVM 后端；不切换旧消费者 Ceph，
  不使用 secondary，不启用 1PC 或 async commit。
- 保留 20×100、Watch 等待 120 秒、租约清理 60 秒及原执行预算；不增加外部
  Watch 重启、PATCH 重试或更新前暂停。部署前、预拉取后重新检查零租约与空前缀。
- 测试前缀：`/registry-kubebrain-apiserver-local-failover-v1361-2g0kzo64`。

## 故障与诊断

13:56:49 UTC，在已观察到 10 次 MODIFIED 且更新未结束后，按 UID/resourceVersion
前置条件删除 leader `kubebrain-local-1`，原 UID
`396a1575-e9ac-476d-b0a4-58e3e32f4a1d`。替代 Pod UID
`eefa52c2-c96b-47d3-ad2e-985297be9d83`，后置检查 Ready。
故障准入还检查候选镜像、容器 ID、镜像 ID、重启次数和就绪状态。

grace=0 的 API 删除不能等同于已证明的 SIGKILL 或宿主机崩溃：旧 leader 日志
在 13:56:50.162–.163 明确记录 TLS/public server shutdown。
采集的四条成员日志覆盖原三个 Pod 和替代 Pod，不能宣称完整历史或零错误。
已有 PD 时间戳获取约 110ms 的慢请求警告，以及切换期间 peer Range 连接关闭重试；
apiserver 启动阶段也有连接取消警告。需要与操作结果分开解读，不能因测试通过而抹除。

四条成员日志中的结构化 warn/error 分类另计：Range 连接关闭 1 次、Range leader changed
2 次、Txn leader changed 1 次、Maintenance Status Canceled 5 次、Health Check
准入前 drained 3 次及 Canceled 2 次。该计数仅针对这些采集日志，不能外推为完整服务错误率；
尤其不能因客户端最终成功就推断每个不确定写入的提交结果。

## 恢复、证据及待办

私有证据根目录：`/root/.local/state/kubebrain/apiserver-fault-31c0a1fb.2G0KzO64`。
驱动 session 33627 终态 0；`deploy-execute.aNpyRAPk/result.json` 的
operation/restore/holder_cleanup 均为 0。`archive/result.json` 的 operation_exit、
cleanup_failed、archive_failed、runner_exit 均为 0。`independent-watch-audit.json`
记录 20×100、2000 条事件和 integrity=passed；后置 LeaseList 为 0，前缀为空。

`restore.OjihW4nl` 验证原完整 spec 与运行镜像恢复，generation 24→25→26，
原 revision `kubebrain-local-568bd68448`，三个副本 Ready。
`postflight.mTRXCFGe` 只读复核通过：后端六个 Pod 身份/容器状态、后端 spec 及旧前端未变。
实验已重新 HOLD，不重复使用已消费的执行标记。

已从部署前、候选部署后和故障替换后三组历史 Bound PV/PVC 快照推导出 14 个临时卷，
逐一核对替代 PVC、无 VolumeAttachment 及当前 Released/Retain/UID/spec 后回收。
只读验证 session 58737、执行 session 38481 均退出 0；执行证据
`proven-scratch.RR5GToCO`。每次更新带 UID/resourceVersion/spec 前置条件，
其余 PV UID/spec 未变。最终为 12 Bound、2 历史 Released；后两卷继续保留。
这 14 个临时卷的数据已回收，不可恢复。
Watch、apiserver 与四条成员日志的摘要再次核验通过。本轮三个辅助程序及镜像审计使用的
另一个 image-prepull 副本均在核验编译摘要后删除，可从固定源码重建；脚本与日志证据保留。
本轮端口 18379/18380/18381/18383/18449 无监听，未发现对应实验进程残留。

归档 SHA-256：

- Watch：`15839aaa8f4ed130b5dad5721a558d04a1ac77fb002d62ce48e8480a030e929c`。
- apiserver：`f3b2107ac4465fb9bfbf3cc5d70719e250e9460733c7faf19c98cb88267cffce`。

相关修复说明：[租约 epoch 检查](acceptance_lease_renew_state_epoch_20260917_cn.md)、
[状态锁准入](acceptance_lease_renew_state_admission_20260917_cn.md)、
[快照取消观察边界](acceptance_snapshot_cancel_observation_20260917_cn.md)。
