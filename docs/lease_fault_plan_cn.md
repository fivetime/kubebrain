# 原生故障实验身份配置校验

`hack/production/cmd/lease-fault-plan` 只校验实验计划的身份和指标预期部分，
不连接集群、不启动子进程、不获取认领，也没有 `--execute`。它不是完整
故障执行 CLI；成功输出明确为 `LOCAL_BINDINGS_VALID_NOT_EXPERIMENT_ADMISSION`。

```sh
go run ./hack/production/cmd/lease-fault-plan \
  --bindings /absolute/private/bindings.json \
  --approve-sha256 INDEPENDENTLY_APPROVED_SHA256
```

摘要必须来自独立审阅的配置，不应读取任意输入后即时计算摘要当作授权。
加载器要求私有普通文件、规范绝对路径、不跟随最终软链接、最大 1 MiB；
严格拒绝未知和重复 JSON 字段。

配置类型是 `leasefault.NativeExperimentBindings`：

- `version` 固定为 1；`network`、`protocol` 使用已有恢复意图的 JSON 格式，
  所有 owner、Namespace/StatefulSet 身份必须一致。
- `initial_term` 和 `observer_member_id` 是十进制字符串；protocol 中的
  cluster/member/lease ID 也使用字符串，避免 JSON 浮点精度损失。
  独立观察成员不得等于原 leader；初始配置不能指定 origin 或继任 term。
- `source` 为完整 40 位小写提交号，`source_hash` 为已审阅 lease 源文件
  的 SHA-256；`image` 必须是 `ghcr.io/fivetime/kubebrain@sha256:…`。
  格式校验不证明源码允许列表、CI 成功、镜像来源或实际 Pod 镜像匹配。
- `stack_ports` 为四个互不相同的 1024–65535 端口，顺序是 before 的
  info/anonymous，再是 after 的 info/anonymous。尚未检查端口可用性。
- `metrics` 使用 `retirementmetrics.WorkerExpectation` 的字段名：
  `Binding`、`Offset`、`Key`、`MinimumCount`、`RequireDuration`。
  `Offset` 单位为纳秒，必须小于 30 秒；最小计数必须显式填写，零也不能省略。
  各项绑定同一 Namespace/StatefulSet、spec SHA-256 和 cluster 标签；
  允许不同 Pod，具体 Pod 与 worker 命令映射仍需独立验证。

后续完整执行入口仍须提供命令及全部依赖的固定哈希、连接凭据、在线
源码/镜像/Pod/任期准入、实际认领、证据持久化、子进程 Join、故障后恢复
和恢复验证后的显式释放。此命令不替代这些要求，也不代表原 30 秒真实
验收已通过。测试覆盖本地配置及 CLI，不接触专用测试集群。

`leasefault.ObservationCommandPlan.Build` 提供执行入口所需的原始探针及
两次栈采集命令组装：身份 ID 保留整数精度，探针显式等待租约过期后仅发送
原始续约；before/after 分别绑定计数 1/0、独立 receipt 目录及四个端口。
命令使用 argv 和完整显式环境，拒绝重复环境变量、shell 启动钩子、端口
覆盖以及共享或非私有 stderr 文件。构造过程不创建目录、不执行命令。

`ObservationCommandPlan.Observation` 将上述命令直接绑定为现有
`FaultLifecycle.Observation` 所需的 `OriginalObservation`：身份取同一
配置，前后目录、源码摘要和计数不再由调用方重复填写。它强制要求原始
探针及栈采集的在线准入和持久化回调，按 before/after 路由回执，且原样
传播回调错误；构造时不会执行这些回调，也不会获取认领或启动实验。

该组装器尚未接入执行 CLI。调用方仍须审阅工具/凭据哈希，准备私有空
receipt 目录，核验环境内的集群身份，并提供在线准入、留存和恢复逻辑；
仅成功构造 argv 不构成实验许可或真实验收。

`ObservationCommandPlan.MetricCommands` 按同一配置的指标预期顺序组装
`protected-metrics-worker.sh`，以纳秒十进制参数传递对应 `Offset`，不再
接收另一份采集偏移。目标 Pod UID 必须匹配预期；所有指标 worker 的两
个端口必须彼此独立，也不得占用前后栈会话的四个端口。指标 stderr 必须
私有且与原始探针、栈会话和其他指标 worker 分离。目标 Pod 名称与 UID
的在线映射以及工具来源仍由执行入口准入检查，构造器不连接集群。

`ObservationCommandPlan.BindLifecycle` 将命令组装接入
`MeasuredNetworkFaultRuntime`，返回供 `RunFaultLifecycle` 使用的配置。
它核对准备阶段的 owner 目录、网络及协议身份、脚本目录，并拒绝覆盖已有
原始观察、外部故障命令、worker、指标预期、继任者或环境绑定。原始身份、
继任者预期及指标预期均取同一份计划，再安装已有的真实网络观察和指标
门限。此步骤仅本地组装，不调用在线回调，不执行子进程或申请集群认领。

执行入口仍需提供实际认领、连接、在线准入、原始时钟、证据持久化及 Join，
调用运行时后完成恢复验证和显式释放。组装测试使用模拟连接及回调，不能
作为真实故障执行或原 30 秒验收通过的证据。
