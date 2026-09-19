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
（当前 Pod）、policies（完整的命名空间 CiliumNetworkPolicyList）。
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
