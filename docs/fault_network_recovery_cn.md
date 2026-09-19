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

## 删除请求传输衔接

`cmd/uid-delete` 的 `TestFaultPolicyPlanDeleteTransport` 将仓库 jq
生成的真实计划直接传给现有动态客户端删除函数，并由本地 HTTP
测试服务器检查 Cilium API 路径、DELETE 方法以及同时存在的 UID
和 resourceVersion 前置条件。资源版本使用超过 int64 范围
的十进制字符串，验证从 jq 到请求 JSON 全程未转为浮点数。
模拟成功、409 冲突和 404 缺席响应，后两者保留错误类别，三种情况
均只发送一次请求，不在冲突后退化为无条件删除。

该传输回归纳入 probe CI。它不调用真实集群，也不模拟 API 服务器
实际执行前置条件，不能证明策略已消失或 Cilium 数据面已恢复。
完整驱动仍需负责可信输入、独占恢复、准入及删除后的独立观察。

## Go 执行衔接与不确定响应回归

`internal/leasefault.RemoveNetworkPolicy` 已补充 API 删除执行：加载真实
预留回执，核对当前 UID 与完整激活/预留 spec，删除前重查回执及实时准入，
使用 UID 和当前 resourceVersion 双前置条件，最后 GET 确认 NotFound。
缺失回执、对象替换、冲突或删除后仍存在均不能视为恢复成功。

`TestNetworkLifecycleHTTP` 使用真实动态客户端串联预留、激活和删除。
HTTP 测试服务实际应用 JSON Patch，并模拟“激活已生效但返回 500”；
取消故障上下文后，独立恢复上下文仍能删除准确对象并确认缺席。
资源版本超过 uint64 范围仍按原字符串进入删除请求。再次恢复只读确认
缺席，不重复 DELETE，原始创建回执保留。

此回归不等于真实 Kubernetes/Cilium 测试。完整控制器仍须连接独占职责、
实时身份准入、worker join、Cilium 数据面撤销、协议恢复和标签恢复；
原 30 秒完整验收尚未通过。不得因本地组件测试成功启动未准入实验。

## 撤回标签后的策略复查

`deploy/test-cluster/observe-local-policy-state.sh` 新增显式模式
`absent-unlabelled`，用于标签撤回且 identity 收敛后的策略复查。
原 `absent` 模式仍要求 endpoint 带本次故障标签，不改变旧门限。
新模式要求独立保留的预期 Pod 和采集前后 Pod 均无 fault-owner 标签，
endpoint identity 中也不能有任何 fault-owner 标签；旧标签或其他 owner
均拒绝。仍核对同一 Pod 进程、CEP/agent 绑定、ready 状态、策略修订号
收敛以及原策略 name/UID 均已从 realized policy 消失。

该模式不是放宽为“没有标签即成功”：策略仍在或修订号未收敛仍返回
pending（75）；身份不一致返回 fatal（65）。观察器不证明数据包/RPC
连通性。恢复入口的真实 hook 仍须选择正确阶段、执行独立连通性验证，
并在整个恢复周期持有独占职责。新增回归使用模拟 kubectl，没有调用集群。

## 2026-09-19 真实集群只读观察兼容性

以全新私有目录
`/root/.local/state/kubebrain/recovery-observer-readonly.zC07kzvt`
运行仓库 `capture-local-cilium-endpoint.sh`，目标 `kubebrain-local-0`，
退出 0，证据位于 `endpoint.o4veAagd`，`evidence.sha256` 核对通过。
同一 Pod/CEP/agent 进程绑定通过，endpoint ID 2781、identity 211368，
状态 ready，期望与实际策略修订号均为 47。

随后运行 `observe-local-fault-label.sh` 的 absent 模式，使用前次实际
Pod JSON 作为同进程基准，退出 0；证据位于
`identity-observation.5SnY6NrH`，校验和通过，结果为 matched。
`term-readonly` 仅作为观察参数，未设置任何标签或创建策略。命名空间
当前 CiliumNetworkPolicy 列表为空，fault-owner 标签 Pod 列表为空。

本轮只执行 GET/LIST 和 agent 内只读 endpoint get，没有重跑旧实验、
部署镜像或注入故障。此证据确认现场采集及标签缺席观察可用；没有验证
本次故障策略撤销、PD/TiKV 连通性或原 30 秒故障验收，不能据此声称
完整恢复控制器或最终目标已完成。

### 原 Pod 到本地 PD/TiKV 的 TCP 基线

私有证据目录：
`/root/.local/state/kubebrain/recovery-tcp-readonly.EADCpv99`。
仅 `STRICT-SHA256SUMS` 覆盖的 `strict-*` 结果为有效基线：第一版 shell
没有开启失败即退出，不能确保握手错误被正确传播，故原 `results.tsv`
不用于结论。修正为 `bash -c 'set -e; exec 3<>…; …'` 后完整重跑。

从现有 `kubebrain-local-0` 容器内，对当前 `kb-local-pd-[012]:2379`
和 `kb-local-tikv-[012]:20160` 的 6 个实际 Pod IP 各进行一次 TCP 握手，
全部成功。每次连接使用 3 秒容器内 timeout 和 8 秒外层 timeout，
不发送业务请求、不安装工具、不创建资源。源 Pod 前后同进程检查通过，
目标 Pod 名称、UID、IP 前后匹配且未删除，严格结果的校验和通过。

这确认可用现有容器能力补充目标网络路径的只读观测；它不是 TLS、
PD/TiKV 协议健康或故障后的恢复证明。正式恢复 hook 仍需连接已准入
目标集合、Cilium 观察、失败传播和固定恢复期限，不能把这次无故障基线
替代真实实验的观测。

上述检查现已固化为 `deploy/test-cluster/observe-local-backend-tcp.sh`。
参数为私有 owner 目录、独立保留的源 Pod JSON、已准入的 6 个目标
`[{name,uid,ip,port}]` 文件。脚本限制目标为本地 PD/TiKV 实例，校验
namespace、源 Pod 同进程和 StatefulSet 归属、目标 UID/IP、完整列表，
使用位置参数发起有界 TCP 握手，并在前后重查身份、核验输入未变。
任何失败都不发布 `evidence.sha256`；调用方仍须施加整体恢复截止时间。

仓库脚本真实只读运行退出 0，证据位于同一私有目录的
`backend-tcp.KOfpqXEj`，校验和通过。模拟 kubectl 的 race 回归覆盖成功、
连接失败、目标替换、源进程重启、非法 IP 和分页未完成（4.812s）。
此脚本尚未连接完整故障执行入口，也不能替代协议健康检查。

### 组合网络恢复观察入口

`deploy/test-cluster/observe-local-network-restored.sh` 接收
`owner mode policyUID policyName expectedPod targets` 六个参数，其中 mode
只能是 `absent` 或 `absent-unlabelled`。必须传入真实持久化 CREATE 回执
的 policyUID，不得用同名 GET 或虚构 UID 替代。调用方校验独立准入的
参数/脚本，持有独占职责，并通过 `RunRecoveryObserver` 等机制施加同一
恢复截止时间、管理子进程和私有日志。

`absent-unlabelled` 阶段先执行同进程标签 identity 撤回观察：Pod 标签
已删除但 endpoint identity 仍收敛中时返回 pending，不能直接进入严格的
无标签策略观察而误报 fatal。其他 owner、进程漂移等错误仍失败。
`absent` 阶段不增加该步骤，保留仍带本次标签时的检查要求。

随后固定顺序为策略撤销观察 → 原 Pod 到后端 TCP → 再次策略撤销观察。
三步全成功且输入校验和未变才发布组合证据；退出 75 原样保留为 pending，
其他失败停止，不重试、不重置时钟、不执行任何恢复写操作。子观察器
各自的证据路径保留在私有日志中。

组合顺序回归使用子进程桩，覆盖 identity pending/fatal、前后 pending、TCP 失败、后置 fatal 和
输入变化；TCP 观察器另有模拟 API 测试及前述真实只读基线。组合入口
尚未用于新的真实故障恢复，不能将这些证据混称为完整控制器验收。
