# 本地盘持续负载回滚验收准备

## 执行结果（2026-09-17 更新）

本项已执行并结束，见[实际结果与诊断缺口](acceptance_local_rollback_63e0bd48_20260917_cn.md)：原 6000 次可用性门限通过，原镜像恢复及本轮清理完成；七次附加应用指标采集失败。以下为历史准备记录，不代表仍在 HOLD 等待执行，也不得重用一次性驱动重跑。

## 尚未完成的要求

[此前升级验收](acceptance_local_upgrade_8ac67ca3_20260917_cn.md)在负载完成后恢复旧镜像，
不能证明持续负载期间回滚的可用性。此处只记录下一项验收的准备，不宣称已通过。

2026-09-17 05:42:03 UTC 只读基线核验通过：`kubebrain-local` UID
`7d760f53-5bb5-4429-a2f8-651b89665616`，generation/observedGeneration=14，Ready/updated=3，
完整 spec 和实际容器身份与上轮恢复结果相同，仍为原固定镜像 index
`sha256:50b9938fe3e5ad379136957438abe3c2d4bf250dc6c5973c869e0ef1341d1537`。
本地 PD/TiKV 六个 Pod UID/containerStatuses 未变，旧 Ceph 前端完整 spec 未变且 Ready3。
本地 PV UID/spec/phase 与上轮清理结果相同：12 Bound、2 历史 Released；后两卷不在清理范围。
两份客户端证书通过至少 24 小时有效期检查。

私有证据 `local-rollback-preparation.IbvdS7pb`，`baseline.exit=0`，目录保持 HOLD。
这些快照只供准备，不能作为未来部署的持续有效准入；此次未修改集群。

## 执行设计与恢复边界

1. 核验选定候选的同源 CI、不可变镜像摘要、实际构建身份、两架构发布清单；
   旧目标镜像同样需要固定摘要和运行时身份。测试/工作流提交不能沿用别的源码的 CI 结论。
2. 新鲜核验命名空间/实例/backend/StorageClass 身份、原完整 spec、Ready、TLS、
   租约和专属测试前缀基线。保存 PV/PVC 历史 Bound 身份及原运行时身份。
3. 将隔离前端准备到候选镜像；这一步不计为回滚负载验收，单独确认收敛。
4. 使用现有 `run-kubebrain-rollout-availability.sh`，以旧固定镜像作为 `TARGET_IMAGE`，
   从候选执行反向滚动；保留默认 2PC、6000 次、100ms 间隔、公共 5 秒、直连 30 秒、
   TSO/Region 各 1 秒、rollout 后完成等待 900 秒的原门限。
5. 必须保持同一 probe UID；已有 `wait_for_rollout_with_active_probe` 会在 rollout
   期间检查其身份及 Running 状态，提前完成/失败或换 Pod 均不能算通过。
   完整验证 Watch、Lease、RangeStream、Snapshot 和原始 summary，不能只看 Ready。
6. **内层失败恢复不等于最终恢复**：反向测试的 source 是候选，因此内层失败可能恢复
   候选；外层必须另持原镜像完整 spec 的 UID/resourceVersion 防漂移恢复责任。
   成功则保持原镜像，不启用 `RESTORE_ORIGINAL_AFTER_SUCCESS` 把它再改回候选。
7. 最终验证原 spec/runtime/Ready、后端及旧前端未变；只回收本轮有完整历史 Bound
   PV/PVC 身份证据的临时卷，不改 StorageClass 或旧 Ceph 数据。不在流程运行时删工具。

尚需完成独立镜像核验、新鲜准入、外层恢复驱动审核和实际执行。

后续准备进展：外层恢复脚本已加入本轮 source/CI 绑定、原完整 spec/UID/resourceVersion
防漂移条件及运行时恢复核验；未取得 deployment claim 时退出 1，尚未执行恢复。
旧目标的不可变 index 已重新从 registry 获取，字节摘要和内容与历史审计一致，
amd64 `sha256:fb95e438dfe27898160c69c01d9a65e76b8c1e3e959db695c4a80e30e6831d25`、
arm64 `sha256:3651d4e9fa65eec2bdc89f1d6592c53021a40ba48d772d122f5baeb71761ebce`
均经现有 release verifier 验证。证据 `target-identity.8iP3YfNf`，验证进程退出 0。
这复用旧目标的历史二进制审计，不宣称重新执行旧二进制；新候选身份核验仍待 CI 完成。

内外驱动随后完成准备与 shell 语法检查，HOLD 下执行均在接触 Kubernetes 前退出 2。
内层使用新候选自带探针验证旧目标，不回退验证工具；外层负责最终原配置恢复及预拉取清理。
另一次只读存储基线 `deploy-baseline.2s5DUjSo` 退出 0：租约列表为空、
`/kubebrain-rollout-availability/kb-local-rollback-63e0-ibvds7pb/` 前缀为空，成员身份未变，
临时转发端口已释放。正式准入仍须重验，不能凭准备阶段结果直接执行。

后置脚本已准备：驱动终态、恢复和预拉取清理成功是前置条件，随后独立核验原完整 spec、
副本和实际镜像，以及后端、旧前端身份；不存在的执行目录会在访问 API 前被拒绝。
针对底层驱动运行七组安全回归（race，112.616 秒，退出 0），涵盖探针在滚动期间
失败/提前完成/替换、并发 spec 漂移、恢复身份或实际摘要漂移、独立固定探针镜像、
成功后恢复及互斥模式拒绝。证据 `runner-safety.log`。
这些使用模拟 Kubernetes 的驱动测试，不证明私有组合脚本或真实回滚已经通过。

三台 TiKV 的只读指标采集准备检查也已通过：采集前后 Pod UID、容器 ID、镜像、
启动时间及重启次数均符合固定记录，Ready 检查未放宽，Raft/gRPC histogram 可读取。
证据 `backend-readiness.3egchJmJ`、退出 0。这只验证诊断通道可用；单次累计指标
没有同一运行实例的区间差值，不能据此宣称当前磁盘延迟或 SLO 通过。

与上轮 Pod 删除测试不同，本项验证版本反向滚动，不替代节点断电、存储故障、
全控制器/调度器或多日稳定性验收。
