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
Cilium enforcement 恢复，再进行协议恢复和标签撤回。标签删除的
UID/RV/nonce 条件及标签收敛检查仍待迁移；完整驱动尚未接入。
