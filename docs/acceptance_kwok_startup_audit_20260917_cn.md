# KWOK 启动租约错误：参考后端复现与审计补强

2026-09-17，工作树基于 `3e813b99`。未修改产品事务/租约语义、KWOK 源码或 RBAC。
未部署共享集群，也未改变 60 秒清理门限。

候选测试原有 Metadata 审计只证明 List 200，不能判断列表内容。因此将控制面
审计策略提取为 `controlplane-audit-policy.jq`，仅在 KWOK 模式增加专用身份对
`kube-node-lease` 中 Lease 的 get/list RequestResponse。无 Secret/token 响应体，
不增加 watch 响应体审计。策略回归测试锁定完整规则及默认关闭模式。

同时发现验证器只比较两次续租 UID，未绑定预创建 UID。新增替换租约、缺失创建
UID 两个反例：旧实现均错误通过（测试 session 45165 退出 1），修复后拒绝。
启动等待及最终审计都检查预创建 UID。

## 验证结果

- KWOK 与策略回归 race 测试 session 26512 退出 0（1.176s）；go vet session
  32706 退出 0；shell 语法及 diff whitespace 检查通过。
- 固定参考 etcd、官方 v1.36.1 控制面、固定本地 KWOK 构建，隔离运行 session
  8872 退出 0；结果 operation/backend_cleanup/runner 均为 0。三 Pod 模拟状态、
  真实控制器/调度器写入及 KWOK 审计均通过。结束后 18453/13579/13580 无监听。
- 私有证据：`/root/.local/state/kubebrain/kwok-audit-reference.PnqgTXpl/`
  下 `controlplane-reference.zXC9B9WX`；runner.sha256 记录实际脚本，认证材料不入库。

参考后端也出现同样的启动 403，新增证据时间线（UTC）：

| 时间 | 实际证据 |
| --- | --- |
| 09:31:29.605561 | Lease List 200，响应 RV 227，包含 reference-node，UID `2e597640-3147-41ef-9700-2657adbc636a`、对象 RV 227 |
| 09:31:29.605802735 | KWOK 记录 Creating lease |
| 09:31:29.606851 | Create 被 403 拒绝 |
| 09:31:39.796348 | 同一 UID 的 Lease Update 200 |
| 09:31:50.056720 | 同一 UID 再次 Update 200 |

固定 KWOK 源码 `099ce5faf29193ac19f0d7529103327c48570f20` 的
`WatchWithCache` 启动异步 informer 后直接返回 getter，没有等首次同步；
`NodeLeaseController.sync` 在本地缓存未取得租约时尝试创建。这与本次参考后端
时间线一致，支持缓存启动时序解释，证明该现象并非 KubeBrain 独有。
未记录之前候选 List 响应体，故不能追溯断言候选那一次返回了相同列表。
不扩大 Lease create 权限，不用此复现免除候选的其他 Watch/版本问题。

本次是测试证据完整性改进及参考复现，不证明 KubeBrain 清理门限通过。
共享后端 3e813b99 一轮仍是退出 70，两条长租约的自然到期仍待核验。
