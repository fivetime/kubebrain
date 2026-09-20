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

执行入口仍需提供实际认领、连接、在线准入、其他证据持久化及 Join，
调用运行时后完成恢复验证和显式释放。组装测试使用模拟连接及回调，不能
作为真实故障执行或原 30 秒验收通过的证据。

原始时钟现由 `BindLifecycle` 安装 `NewDurableFaultOrigin`，拒绝外部
覆盖 `Metrics.Origin`。构造时冻结整份身份配置的 SHA-256，不进行 IO；
运行到原有 worker baseline 屏障之后，回调核对所有权，在私有 owner
目录独占创建 `fault-origin.json`，记录 owner、配置摘要、原始时间和
固定 30 秒截止时间（纳秒以十进制字符串保存）。文件及目录同步完成，
且再次核对所有权和目录身份后，才返回原始单调时钟给运行时。

持久化及后置检查消耗同一 30 秒预算，不在同步后重新取时间。已有文件、
软链接或部分回执都阻止新时钟；失败保留回执，不能删除后重试以重置门限。
该回执不是实验成功、故障已激活或恢复已完成的证明，也不能用于重新启动
旧的 monotonic clock。完整执行 CLI 的其他准入、留存、Join 和释放接入
仍未完成；此变更没有触碰测试集群。

验证：时钟及命令接线的定向竞态测试连续三次通过（1.253 秒）；随后增加
无界/超长上下文拒绝覆盖，`leasefault` 整包竞态回归通过（24.817 秒），
`go vet ./hack/production/internal/leasefault`、`git diff --check` 通过。
测试还覆盖八个独立构造器并发仅一个成功、保留部分回执、拒绝软链接、
所有权丢失、取消以及目录替换；未执行真实故障注入。

`TestDurableOriginSupervisorBarrier` 进一步通过真实 Bash 子进程和
`metricsworker.Run` 验证时钟回调的运行顺序：baseline 后才能选时钟，
激活回调执行前回执必须可读，worker 和完成回调收到同一时间；已有回执、
同步后所有权丢失、同步后耗尽调用者预算（即使所有权回调返回 nil）都禁止
激活和时钟下发，并确认 supervisor 已回收 worker。该集成测试竞态运行
连续三次通过（7.174 秒）。子进程是协议夹具，不是实际指标采集或集群故障。
随后 `leasefault` 整包竞态回归通过（27.172 秒），`git diff --check` 通过。

恢复命令的观察日志现复用 `RetainRecoveryObserver`：使用同一个私有目录
句柄独占创建 0600 日志、同步文件与目录、最后核验路径身份，拒绝目录
替换、软链接目录、不安全阶段名和超大记录，保留原目录/部分日志。
输出上限为 supervisor 的 1 MiB，错误文本上限 64 KiB；沿用原有
`recovery-*.json` 格式及输出/错误/时间字段。留存成功不等于观察成功。
此实现已接入 `lease-fault-recover`，不是声称完整实验的所有留存回调已接入。

该留存实现定向竞态测试连续三次通过（1.134 秒），恢复命令单独竞态回归
通过（2.187 秒）；随后两包完整竞态回归分别通过（26.675/2.190 秒），
两包 `go vet` 和 `git diff --check` 通过。未运行集群恢复或故障注入。

`ValidateFaultLifecycleConfiguration` 现可在申请实际认领前，检查上下文
预算、恢复配置、协议绑定、本地可执行文件和私有日志句柄，不访问 Kubernetes、
不启动进程、不调用准入/留存回调，也不要求已取得 owner。执行入口
`RunFaultLifecycle` 会重复该检查，然后核验 owner 与准备范围一致并调用
实际 `Owner.Check`，才进入原准备、实验及恢复流程；不能拿预检成功替代
认领、在线准入或镜像来源验证。

新增测试覆盖合法但未认领的配置，以及缺失工具、公开日志、缺少恢复连接、
协议不匹配、取消/无界上下文、混用外部故障、缺少准入等 10 类场景；
断言没有回调及 Kubernetes 请求，且执行仍拒绝无认领配置。定向竞态
连续三次通过（1.170 秒），既有准备与恢复流程回归通过（4.666 秒）。
完整执行 CLI 的认领编排尚未接入，不能据此声称实验入口已经完成。
随后 `leasefault` 整包竞态回归通过（27.110 秒），`go vet` 和
`git diff --check` 通过。

`ObservationCommandPlan.ClaimAndRun` 现将本地预检、独立在线准入、一次
持久化认领和原生执行/恢复流程串联起来。它拒绝已有 owner、复用尝试目录、
缺少在线准入回调及无效本地工具；在线准入返回后再次检查目录身份、取消状态
和 HOLD 等尝试标记，再调用一次 `AcquireFaultOwner`。实际取得的 owner
填入原生命周期捕获的同一对象，后续执行仍重复本地和在线所有权检查。

CREATE 响应丢失不能推断认领不存在，也不会重试或自动接管；执行或恢复
失败均保留认领，成功也不自动释放。返回的 Owner 为 nil 并不证明服务端
没有创建对象。调用方仍须提供真实准入、连接、留存与 Join，在取得新的
完整恢复证明后显式释放。此方法是执行编排接线，不是完整可用的 CLI，
不代表原 30 秒真实故障验收通过，且本轮未对集群进行写操作。

新增测试覆盖执行拒绝、CREATE 已提交但响应丢失、无效工具/恢复配置、
已有尝试标记/意图、准入拒绝/取消/新增 HOLD/替换目录、缺少准入及已有
owner。测试检查认领前失败没有 Kubernetes 请求，CREATE 不重试，执行
拒绝会进入独立恢复，并且所有失败场景均不删除认领。

本轮 `go test -race -count=1 -timeout=3m ./hack/production/internal/leasefault`
整包通过（27.691 秒），同包 `go vet` 通过。

`ObservationCommandPlan.BindEvidence` 为执行配置补齐五类真实文件留存
回调：原始探针、前后栈回执、网络观察、继任者 Status 和指标结果。拒绝
覆盖已有留存回调，冻结 owner 与完整配置摘要，并在每次留存前后核对创建
绑定时的私有目录身份。文件沿用目录句柄固定、独占创建及文件/目录同步的
实现，使用 `experiment-*.json` 前缀，与恢复观察记录分开。

外层保存观察错误文本与采集时间，output 内是包含 owner、配置 SHA-256
及具体载荷的 JSON；原始字节按 JSON base64 保存，继任者 RPC 错误显式
保存为文本，不依赖 error 接口的 JSON 编码。序列化后的完整载荷仍受
1 MiB 上限约束（包含编码开销），超限即失败，不能截断为成功证据。
取消的上下文仍先留存观察，再返回取消错误；无上下文的网络回调由调用者
核对原截止时间。留存成功不证明来源可信、在线身份正确、故障成功或恢复
完成，Join 与独立在线准入仍须由完整 CLI 提供。

本轮 `leasefault` 整包竞态回归通过（27.112 秒），同包 `go vet` 通过；
测试覆盖五类回调、错误文本、拒绝覆盖、非法阶段/索引、取消后保留日志及
目录替换拒绝。未操作集群，尚未将这些回调接入完整执行 CLI。

随后 `ClaimAndRun` 已直接安装上述文件留存回调，要求输入的五类留存
均未设置，避免执行入口接受空留存实现。独立准入与 Join 仍由调用方提供，
底层 `BindLifecycle` 保持可组合；不能先调用 `BindEvidence` 再调用
`ClaimAndRun`，否则因重复留存配置被拒绝。认领回归新增拒绝自定义留存
的场景，确认不会申请认领或进行其他 Kubernetes 请求。

这次执行接线后整包竞态回归通过（26.683 秒），`go vet` 通过。另新增
本地真实 KubeBrain gRPC 继任者观察与文件留存集成测试：在线 Status
结果与已保存样本逐字段相等，保留采集起止时间，成功返回前文件已存在。
该测试与认领测试连续三次竞态运行通过（1.452 秒）。测试使用本地服务
夹具，不是专用集群故障，不证明实际发生选主或 30 秒验收通过；完整
CLI 的配置、工具来源、在线准入及 Join/恢复释放接线仍未完成。
