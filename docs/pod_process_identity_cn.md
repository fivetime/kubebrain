# 故障采栈：进程身份与 Ready 状态分离

2026-09-18 的 `2ad79751` 实验在降主采栈前失败：复用通道的脚本把整个
`containerStatuses` 与故障前记录比较，而隔离使同一进程的 `ready` 从
true 变为 false。现场 Pod UID、spec、IP、容器 ID、重启次数及运行启动
时间均未变。原流在故障后 28.654137562 秒返回 TTL 10、任期 139，但
降主采栈和后续持久化断言未完成，因此本轮仍未通过。

`hack/production/same-pod-process.jq` 接受 `{expected: Pod, current: Pod}`，
用 `jq -e` 执行，仅验证进程身份。它要求完整且唯一的容器集合、非空
UID/运行标识、相同 Pod 名称和命名空间、完整 spec、IP、运行镜像摘要、
重启次数和启动时间，拒绝删除中或非运行状态的 Pod；Ready、启动探针
标志及一般状态更新不属于进程身份。

22 个反例/正例的 race 测试通过，重复 10 次通过（3.686 秒）。对本轮
保存的前后 Pod 记录只读复核也通过。该结果不证明 TLS、隔离生效、
原请求身份或完整故障验收，更不能生成缺失的采栈证据。

规则已接入仓库版[受保护采栈会话工具](protected_stack_session_cn.md)，
但尚未通过新的真实实验。本轮已冻结的私有脚本和失败记录不修改；
新实验驱动接入必须重新冻结工具并验收，不能追溯改判本轮结果。

## 后续：端点采集也必须使用进程身份规则

新 attempt `fault-2ad-recheck.HegMKL6X/deploy-execute.Pcc4VKVk` 已通过原流
响应、降主等待栈消失和键/租约检查，但在撤销隔离后的端点观察失败。
`endpoint.7az7R6au` 前后只有应用容器 Ready 从 false 变为 true，
UID/spec/IP、容器 ID/镜像、重启次数、启动时间不变；旧端点采集脚本
仍比较整个 `containerStatuses`，退出 1。故障清理退出 0。该实验仍
记为失败，不以响应在 28.698617070 秒返回替代完整流程结果。

新增 `deploy/test-cluster/capture-local-cilium-endpoint.sh OWNER POD`，
将这段只读采集纳入仓库，应用和 Cilium agent 的前后身份检查均使用
同一个 jq 规则。工具限定现有专用测试集群及固定 namespace/StatefulSet
UID，OWNER 必须为调用用户所有的真实 0700 目录，POD 限定
`kubebrain-local-[012]`；不是通用生产探针。未来调用者必须将脚本及
`same-pod-process.jq` 纳入冻结清单，使用新 owner 重新验收。

仍检查初始 Cilium agent Ready、endpoint ready、Pod/CEP/agent 对应
关系、CEP UID/identity/networking 稳定；忽略健康标志不代表允许换
进程或换端点。12 个完整脚本模拟场景的 race 测试通过，重复三次
通过（28.357 秒），实际保存 Pod 对的只读身份规则复核也通过。
测试不证明真实隔离或修复后的新实验成功。当前已消费 owner 的冻结
脚本未修改；完整恢复结果另外记录，不能仅凭故障清理成功推断已恢复。
