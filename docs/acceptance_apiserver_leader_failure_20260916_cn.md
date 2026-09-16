# 真实 apiserver：leader Pod 故障实验（未完整通过）

## 结论

官方 v1.36.1 apiserver 对接 bb89c3f8 默认 2PC、本地盘 TiKV 后端，在已确认 Watch
收到了首个修改事件后删除 KubeBrain leader Pod。后续 Watch 的 20 对象 × 10 版本
完整性门禁通过，leader 更换、三个副本 Ready。但**本轮不能记为完整验收通过**：

1. 测试结束仍有一个有效空 lease，60 秒只读清理窗口未收敛，runner 报告清理失败。
2. 外层保存原始 Watch 流时路径错误，提前退出 1；没有保留原始流及独立 smoke
   exit 文件。现有证据是执行日志中的完整性校验输出，不能冒称已归档原始流。

没有放宽门限、强制撤销该 lease，或为补齐结果重复注入故障。

## 实验与故障身份

- runner 源码 HEAD `aca23633`，镜像仍为
  `ghcr.io/fivetime/kubebrain@sha256:50b9938fe3e5ad379136957438abe3c2d4bf250dc6c5973c869e0ef1341d1537`。
- apiserver 二进制与[上一轮](acceptance_local_apiserver_v1361_20260916_cn.md)相同，
  每轮重新核验官方摘要；ephemeral PKI，mTLS 后端连接，禁止 kubectl Watch 进程重连。
- 三条 Pod 直连转发逐一核对 member/leader；apiserver 连接固定在未删除的 follower
  `kubebrain-local-1`，避免 leader 的端口转发退出导致测试入口失效。
- 删除目标 `kubebrain-local-2`，原 UID `6821f934-b48d-43c8-b0af-9ee74b000bdd`，
  原 leader member ID `1543124563`。再次检查 leader 和 Pod UID/resourceVersion 后，
  执行带两项前置条件、grace=0 的删除，API 明确接受。
- 重建 UID `fa17f9d6-af57-4cc7-8fe2-8f99a65265b3`，Ready；后验 leader
  `3358157933`，raft term 6（原 term 5）。三个 StatefulSet 的 UID/generation/spec
  前后一致；六个 PD/TiKV Pod UID 与 containerStatuses 一致。

故障发生于首条修改已确认之后的 30 秒预更新停顿内，随后继续其余 199 次修改。
因此它证明本工作负载的 Watch 跨切换恢复及后续写入，不证明故障瞬间持续写入的
尾延迟、节点断电、TiKV 故障、升级或完整 Kubernetes 控制面兼容性。

## 有限通过证据与失败记录

证据目录 `/root/.local/state/kubebrain/local-apiserver-failover.ZTjWvv5Z/`：
`execution.log` 包含 `APISERVER_WATCH_READY`、200 MODIFIED、`integrity=passed`，
随后明确输出 `lease set differs from preflight after cleanup`。
`delete.log`、`fault-time.txt`、前后 Pod/StatefulSet/PV 和 endpoint status 均保留。

外层脚本生成时替换字符串中的 `$$` 被折叠为 `$`，保存命令最终尝试读取
`/proc/$/fd/9`，导致 `runner.exit=1`；原始流随清理丢失。这是实验封装问题，
不是产品故障。未把此前内部校验通过扩展成整个 runner 成功。

首轮预检目录 `/root/.local/state/kubebrain/local-apiserver-failover.VJVySeHB/`
因 etcdctl 多次 `--endpoints` 被合并而识别失败，退出 1；没有启动 apiserver 或
删除 Pod。第二轮改为每次仅一个 endpoint 且要求返回恰好一条状态，使用新目录与
单次 execution.claim。两轮均已终止，不得直接重跑已消费脚本。

## 清理和待观察项

只读后验：测试 prefix `/registry-kubebrain-apiserver-local-failover-v1361-ztjwvv5z`
为空，endpoint health 正常，临时 WORK_DIR/PKI 已删除、进程和转发端口已退出。
首次 postflight 因仍有 lease 而退出 1；随后诊断记录明确只称 HEALTH_CAPTURED，
不称完全清理成功。

截至 2026-09-16 23:17 UTC，剩余 lease `0002a0ac4ccc6c01`（十进制
`739611836705793`）授予 TTL 3660 秒，观察剩余 TTL 3480 秒、keys=null。
这是仍在有效期内的空 lease，**尚未证明到期回收异常，也尚未证明它已经回收**。
保持自然到期，后续需再次只读检查；全局差集不构成主动撤销授权。

仅回收原 leader 的两个 Released scratch PV：
`pvc-81073e0c-f9fa-41ab-87e0-e8ee49c88868`、
`pvc-a0841933-c371-4d7d-b87b-934a94ee0f5c`。
核对 preflight Bound 身份、当前 UID/完整 spec/Released、替代 PVC 已 Bound、无
VolumeAttachment 后，用 JSON Patch 前置条件将这两卷改为 Delete 并等待删除。
临时数据不可恢复；最终 LogicalVolume 数量 12。PD/TiKV 数据卷、StorageClass 和
旧 Ceph 数据均未修改。

两份本轮 `uid-delete` 辅助二进制核验 SHA-256
`32483d46be3b9b9a14fed06fc8957f4ac5ec6bffcaa6ad3ff0aeecbe801e2909` 后删除，可按源码重建。
下一步需确认自然到期、修正证据保存方式，再安排具有完整归档和终态的故障验收。
