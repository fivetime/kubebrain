# 602912b7 连续更新期间 leader 故障验收

## 结论

2026-09-17：20 个 ConfigMap 各更新 100 次，2000 条 MODIFIED 的独立完整性审计通过；
但租约清理失败，负载脚本及总驱动均退出 70，**本轮整体未通过**。
内部 coordination Lease 写入仍返回 `leader changed`，启动期间还观察到 Txn Range
响应 `Count=0`、实际包含 1 条 KV 的 DataLoss，以及故障后的内部 Watch revision 查询超时。
不能用 ConfigMap 门限通过替代全部 Kubernetes 写入、Watch 或生产就绪验收。

原固定镜像和完整 StatefulSet spec 已恢复，三个副本 Ready，预拉取容器已清理。
本轮 14 个有确切身份依据的临时卷已回收；原有两卷证据不足，继续保留。

## 身份与门限

- 源码 `602912b7ec8614175a7707aeecb349bda2682f75`，默认 2PC。
- 镜像及 CI 核验见[连接保留修复记录](proxy_bounded_retirement_cn.md)。
- 专用本地盘实例 `kubebrain-local`，独立 `kb-local` PD/TiKV；旧 Ceph 实例未切换。
- 官方 kube-apiserver v1.36.1，二进制 SHA-256
  `9b4dba0a5b945f1fe0ce18f47535c5ff0c46ae384f9222047bce39fe91b6023e`。
- 唯一前缀 `/registry-kubebrain-apiserver-local-failover-v1361-70no8l5k`。
- 保持 20×100、原超时预算、无更新前暂停、无外部 Watch 重启或 PATCH 重试。
- 私有证据目录 `apiserver-fault-602912b7.70nO8l5k`；驱动 session 1063 终态 70。

## 故障与观察

03:56:36 UTC，在观察到 7 条 MODIFIED 且负载未结束后，使用 UID/resourceVersion
前置条件和 grace=0 删除 leader `kubebrain-local-1`，其 UID 为
`cad8051c-e6c3-4375-a0db-4509d9b3d5a9`。入口为 local-2，后者成为新 leader。
该拓扑与上次入口未成为 leader 的实验不同，不构成严格 A/B。

后续复查旧 local-1 成员日志：03:56:37.1389 公共端口关闭，37.1546 停止领导权，
39.1418 内部 peer 端口关闭。尽管删除参数是 grace=0，实际进程仍执行了关闭流程；
本次不是已证实的瞬间 SIGKILL、节点断电或存储故障实验，也不能从 API 删除时间推定
旧进程当时已经死亡。日志不包含完整的 unary Txn 生命周期，不能据此判断提交结果。

内部 Lease Txn 在 03:56:37.059531 转发到旧 leader；37.737659 旧连接退出路由，
38.738663 返回 `leader changed`。apiserver 的 Txn 失败段为 1682 ms，整个 Update
为 1683 ms。约 1 秒间隔与连接保留预算相近，但仅凭时序不能证明具体取消来源、
事务是否提交或扩大预算即可解决。没有重放不确定写入。
ConfigMap `soak-12` 的一次 GET 为 2084 ms，后续更新继续，最终 2000 条完整性通过。

故障前 03:56:26.836578、27.324028、27.552297、27.664254 的四条 Txn 警告已经报告
`leader txn proxy returned invalid range payload at index 0: leader range proxy returned count 0 for 1 key-values`。
不能将其归因于之后的故障注入。源码初查发现 backend shim 的部分失败 Range 构造遗漏 Count；
尚需参考 etcd 的回归复现来确定受影响请求路径，不能通过放宽响应校验掩盖问题。

故障后还出现 `get revision from leader failed: context deadline exceeded`，包含 apiserver
内部 cacher/reflector Watch 重试。外部 ConfigMap Watch 完整性通过不代表内部 Watch 无错误。

## 清理、恢复与证据范围

归档原生结果为 operation_exit=0、cleanup_failed=1、archive_failed=0、runner_exit=70。
独立 Watch 审计和成员日志采集均退出 0；四条日志流已重新核对摘要及打开前后身份，
`complete_history=false`，不宣称已捕获全部历史或对端所有提交结果。

清理后前缀为空，但残留租约 `0001a0ad809e3301`。04:04:21 UTC 只读观察：
初始 TTL 3660 秒、剩余 3382 秒、关联键数 0。未执行 KeepAlive/Revoke，也不据此声称租约归属。
空租约可在删除键后继续存在，见[先前参考 etcd 对照](acceptance_apiserver_fault_logs_20260917_cn.md)；
这不改变本轮原清理门限失败的事实。后续部署须重新确认租约基线，不复用此次准入。

恢复证据 `restore.SgJOltG5` 验证原完整 spec 和运行镜像，generation 10→11→12，Ready/updated=3。
总驱动记录 operation_exit=70、restore_exit=0、holder_cleanup_exit=0。
后置检查 `postflight.3ySkZ2aS` 验证 PD/TiKV 六个 Pod 身份及容器状态、后端 spec 和旧前端未变。

清理 session 33382 终态 0：按三组历史 Bound PV/PVC 快照推导 14 个目标，再核验当前
Released、Retain、UID/spec、替代 PVC 指向不同卷及无 VolumeAttachment，以条件更新回收。
其余 PV 的 UID/spec 不变；最终本地卷 12 Bound、2 历史 Released。删除的临时数据不可恢复。
四个自建工具二进制经摘要核验后删除，证据保留；本轮端口无监听残留。试验目录已 HOLD，禁止重跑。

归档 SHA-256：

- Watch：`e03fb42517e232ef13a511ab44bff38aa477d69ad4b19489d52a7c288324cd40`。
- apiserver 日志：`9d5252732c722f9f5c905f53d5a8a6634f461649c217da3b4a6ac491ddd34e70`。

下一步分别复现 Txn Range Count 不一致、内部 Lease 不确定结果和 Watch revision 查询超时，
保留现有门限及历史失败记录，不把增加超时或自动重放写入当作等价修复。
