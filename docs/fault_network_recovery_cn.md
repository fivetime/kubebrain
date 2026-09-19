# 故障实验网络恢复组件

`hack/production/fault-policy-delete-plan.jq` 是策略删除计划的仓库权威
实现，从已退役实验的纯校验逻辑迁移而来，不调用 Kubernetes，也不
表示 Cilium 数据面已经恢复。旧实验 owner 和脚本不得重新执行。

输入必须包含五个字段：

- `binding`：独立准入的 namespace、namespaceUID、策略 name、激活
  nonce、预留 reservedNonce；两个 nonce 必须不同。
- `namespace`：新读取的 Namespace，必须匹配 UID 且不在删除中。
- `approved`：经准入的激活 CiliumNetworkPolicy 服务端 dry-run 对象。
- `expected`：真实预留创建回执，必须有 UID、resourceVersion，spec
  必须仅把 approved 的 fault-owner selector 改为预留 nonce。
- `current`：新读取的策略对象，明确 NotFound 时才可传 null；字段缺失
  不等于对象不存在。权限错误、网络错误不能转换为 null。

策略仍存在时，只有创建回执 UID 相同且 spec 为已批准激活/预留版本
才产生删除计划，计划保留当前 UID 和 resourceVersion 字符串。外层
删除必须同时使用这两个前置条件；冲突不能以无条件删除重试。策略
不存在时也先验证独立准入与真实创建回执，然后返回 absent=true。

命令示例（仅离线生成计划）：

```sh
jq -e -f hack/production/fault-policy-delete-plan.jq < admitted-observations.json
```

输入来源和文件完整性仍由独占恢复控制器保证；jq 不提供认证，也不
替代严格原始 JSON 读取。当前测试只覆盖计划构造，不执行删除。
外层尚须持久化预留回执、核对集群身份、等待策略消失、独立观察
Cilium enforcement 恢复，再进行协议恢复和标签撤回；完整驱动尚未接入。

## 标签恢复计划

`hack/production/fault-label-restore-plan.jq` 生成标签专用 JSON Patch，
输入包含 binding、namespace、expected（变更前真实 Pod）、current
（当前 Pod）、policies（完整的命名空间策略列表）。列表接受
`cilium.io/v2 / CiliumNetworkPolicyList`，以及 kubectl 输出的
`v1 / List`；非空项仍必须逐项为本命名空间 CiliumNetworkPolicy。
binding 含 namespace、namespaceUID、name（Pod）、uid（Pod）、nonce
及 policyName。身份信息必须来自独立准入，不能由待修改对象反推。

原 Pod 必须没有 fault-owner 标签，当前 Pod 的 UID/命名空间/名称必须
匹配且不在删除中。策略列表不能分页未完成，不能仍含本次策略名称，
也不能有 spec/specs 引用该 nonce。验证在返回“标签已不存在”的空
补丁前同样执行。标签属于其他 owner 时拒绝，不会覆盖或移除。

非空补丁依次 test UID、resourceVersion、当前标签 nonce，然后只
remove `/metadata/labels/kubebrain.io~1fault-owner`，保留其他标签。
它不发送 PATCH，也不证明数据面规则或 Cilium identity 收敛。外层
仍必须持有独占恢复职责，先完成协议恢复和网络检查，提交补丁后
复查 API 状态及实际 identity 收敛，不能把空补丁当作完整恢复证明。

两个计划的合成输入回归均纳入 probe CI，并由 workflow 契约测试锁定。
标签测试还使用 JSON Patch 实现实际应用生成的补丁，核对其恰好恢复
原 Pod JSON、保留其他标签，并验证 UID、资源版本、owner 变化或标签
被移除时旧补丁的 test 操作失败。这是本地补丁语义测试，不是 API
服务器集成测试，也不能消除外层独占恢复和现场复查的要求。

## 历史真实证据离线核验

使用已退役 `fault-peer-budget.mjraWKki` 的
`network-restore.NQxXLtn8` 和 `label-restore.AogktG2H` 保存对象做
只读兼容核验，两份 evidence.sha256 均通过。真实空列表为 v1/List，
据此修正了最初仅接受 CiliumNetworkPolicyList 的限制，并增加合法
通用列表及包含错误资源类型时的拒绝回归。

以历史独立 namespace/Pod 身份、批准 spec 和真实预留回执构造输入，
新策略计划与原 delete-plan.json、新标签计划与原 patch.json 的 JSON
结构分别完全相等。三轮计划 race 1.784s，diff-check 通过。没有执行
删除、PATCH、旧驱动或其他集群命令；旧实验完整 30 秒验收失败结论
不变，此项只证明历史数据格式及计划兼容性。
