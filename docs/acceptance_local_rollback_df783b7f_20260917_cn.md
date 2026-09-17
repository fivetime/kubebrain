# 本地盘持续负载回滚验收：df783b7f

2026-09-17，本轮在专用 `kubebrain-local` / `kb-local` 实例完成候选到原固定
镜像的持续负载回滚。6000 次操作及原延迟门限通过；6 次应用采样、101 次
独立后端采样均完成且审核通过。原完整配置、运行镜像及后端身份已独立核验恢复。
这不是整体生产就绪、节点断电或所有兼容性差距已关闭的结论。

## 源码与发布身份

- 源码：`df783b7f658822db78173375550943b4ffcca766`。
- 镜像 CI `35236123582`、回归 CI `35236123694` 均成功；过期续租任期退出及
  服务层路由测试在普通和 race 回归中的 PASS 已从原始日志核对，日志 SHA 已校验。
- 候选索引：`sha256:6f668aae65a650de10e1feff67a60286297f15a80e59cc9c5571dda42dcc56e9`。
- amd64：`sha256:274d10bc094e2e271a549e604e5776dc0166b458ab0735f3cd762fe1645d62e7`。
- arm64：`sha256:23b3b4caad34cee1babf6dbe37bbe5a6b84ab951f6b1e8ec6add51413e3b8f04`。
- 本机独立审计实际 amd64 二进制的 Git SHA、版本、构建时间、Go 1.26.8、
  TiKV fork 和 gRPC 依赖；索引、架构摘要及提升后的标签一致。未宣称执行了 arm64 二进制。

审计证据：`/root/.local/state/kubebrain/release-df783b7f.sVPCfGpb/audit.8ttto5HJ/`。
官方 client/v3 的额外 gRPC 边界测试在此轮等待期间仅通过私有 Go overlay 验证，
**不在上述提交、CI 或镜像中**，不能混入本次发布的测试覆盖声明。

## 场景和门限

后端为独立 PD/TiKV 三副本、本地 TopoLVM；沿用默认 2PC 配置，未修改事务协议。
不初始化磁盘或 VG，不操作旧 Ceph 实例及 secondary 集群。
前端 generation `26 → 27 → 28`：先部署候选三成员，再由候选镜像中的探针持续
施加负载，将前端滚动回原 `bb89c3f8` 固定镜像：
`sha256:50b9938fe3e5ad379136957438abe3c2d4bf250dc6c5973c869e0ef1341d1537`。

保持 6000 次操作、100ms 间隔、滚动后 900s 完成上限、公共 5s、直连 30s、
PD TSO / TiKV Region 各 1s 门限。探针 UID
`9b0d3765-bffe-4193-a1db-566cf79c1d7b` 的实际存活覆盖滚动已由 runner 核验。
三 worker 预拉取及再次基线检查通过；准备 Pod 后来调度到 `k8s3-network2`，
额外冷拉取耗时约 6 分钟。准备阶段等待不冒充负载完成进度。

## 实测结果

| 检查 | 结果 |
| --- | --- |
| 操作 | 6000 成功、0 失败 |
| Watch | 公共 6000，三个直连各 6000；缺失、歧义、非法事件均为 0 |
| 最大延迟 | 公共 1935ms，Put 1863ms，直连 9137ms，TSO 102ms，Region 37ms |
| Lease | 公共存活、426 次响应、0 次重启；直连存活、1251 次响应、5 次重启、最大恢复 3494ms |
| 流式验证 | RangeStream 702，Snapshot 1；流重试 8，部分响应重试 0 |
| 应用诊断 | 6/6 成功；独立核验探针及前端各自镜像摘要、前后运行时身份及采样完成记录 |
| 独立后端诊断 | 101/101 回调完成，总退出码 0；逐次退出码也均为 0 |

探针最终 elapsed 为 839606ms，**包含滚动过程，不是滚动后的耗时**。
滚动期间确有客户端 Unavailable 重试日志，不宣称零错误日志。
诊断完成不等于所有性能指标或协议计数增量通过；后者仍需同运行时配对分析。
相较[此前回滚](acceptance_local_rollback_63e0bd48_20260917_cn.md)，本轮实际验证了
独立探针/前端镜像身份采样，不能反向把此前 7 次失败采样改判成功。

## 恢复、清理与证据

主 driver、runner、应用审核、后端审核均退出 0；恢复及镜像保持任务清理均为 0。
最终前端 generation 28、原 revision `kubebrain-local-568bd68448`、3/3 Ready，
完整 spec 和原运行镜像一致；六个本地 PD/TiKV Pod 身份与容器状态未变化，旧前端未变。
独立检查确认零租约、本轮专属前缀为空，探针及本轮候选预拉取 Pod 已不存在。

仅将两组历史 Bound PV/PVC 快照证明属于本轮的 12 个临时卷回收；执行前再次核验
Released/Retain、UID、完整 spec、替代 PVC 及无 VolumeAttachment，以 UID/RV/spec
前置条件改为 Delete。这些临时卷数据不可恢复。清理后为 12 Bound、2 历史 Released；
两个来源未确认的历史卷保留，其余 PV 身份/spec 未变。

私有证据根目录：`/root/.local/state/kubebrain/local-rollback-df783b7f.HVGB1VWE/`。
关键目录：`deploy-execute.VMbYRPmn`、`reverse-execute.q7A0elpQ`、
`restore.HOHRUOmc`、`postflight.VOIOQ1u6`、`proven-scratch.Z6qRt76L`、
`deploy-poststorage.uTl5rBNK`、`runtime` 和 `backend-observer-evidence`。
归档已逐文件 SHA 校验；`runtime/probe-final.log` SHA-256：
`80a1803c49645c112f638579e839dd67f7413b649caef2dedca47c606f7fa282`。
单次执行声明均已消费，HOLD 已恢复，不得复用该 owner 重跑实验。

## 未关闭范围

这是持续负载回滚回归，不保证命中过期租约撤销阻塞与任期丢失的精确竞态。
该边界仍需真实后端故障注入验证；参见[任期退出修复记录](lease_expired_renew_term_cn.md)。
完整控制器/规模、长期 soak、后端故障、备份恢复及生产故障域验收仍不能由此轮替代。
