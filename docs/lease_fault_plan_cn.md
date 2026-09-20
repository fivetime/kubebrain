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

认领执行入口在 `RunFaultLifecycle` 返回后，现在同步保存
`experiment-lifecycle.*.json`，包含精确认领 UID/范围、激活 API 是否确认、
执行错误、恢复错误、指标回执及可能不完整的原始结果。外层保存完整返回
错误，避免 error 接口被 JSON 编码为空对象。写入失败与原运行错误合并
返回，仍不删除认领；此记录不是 COMPLETE，也不授权释放。

`LifecycleResult.RecoveryAttempted` 显式标识是否进入恢复调用。认领或
生命周期前置校验失败时该字段为 false，即使 RecoveryError 为 nil 也
不能推断恢复通过。归档发生在执行/独立恢复返回之后，不重置原 30 秒时钟。
只有真正调用了生命周期才生成此返回记录；申请失败或崩溃仍应检查保留的
意图/认领回执，不能把缺少结果记录解释为未发生写操作。

认领测试验证失败执行和恢复的错误均可从结果文件读回，并模拟目录权限
变更导致归档失败：保留集群认领，同时返回执行与留存错误。整包竞态回归
通过（26.827 秒），同包 `go vet` 通过。本轮仍未进行真实集群故障验收。

恢复命令原有的 owner 专属 Join 脚本执行现抽为 `RunFaultJoin`，并已被
`lease-fault-recover` 使用。它要求不超过 5 分钟的调用者上下文、规范绝对
路径和私有 owner 目录；固定 Bash 参数及最小 PATH，不继承调用者环境。
脚本只执行一次，退出 75 也不会重试；仍使用进程组取消和有界输出，保留
`recovery-join.*.json`。执行前后检查批准输入，前置准入后及返回后检查
目录身份；错误不允许进入后续恢复阶段。

共享实现不替代 Join 脚本的审计：脚本仍须核验准确进程身份并处理逃逸的
子进程，不能以退出码或日志推断所有进程已退出。文件日志中的错误描述
脚本执行结果；后置输入校验失败仍以函数返回错误阻止后续操作。完整实验
入口仍需绑定该共享执行器到实际批准的脚本，尚未完成 CLI 的在线接入。

共享 Join 覆盖成功、非零/75 退出、前后准入拒绝、取消、无界上下文和
目录替换；定向竞态连续三次通过（1.170 秒）。leasefault 与恢复命令
整包竞态分别通过（26.837/2.230 秒），两包 `go vet` 通过。未运行集群
清理脚本或恢复操作。

新增 `CheckLivePodProcess` 供在线准入回调使用：针对独立批准的 Pod 快照
执行一次最新 GET，再以受限子进程调用仓库唯一的
`hack/production/same-pod-process.jq`。它不复制另一套进程比较逻辑，允许
同一进程在隔离期间 readiness 变化，但拒绝 UID、完整 spec、Pod IP、
containerID/imageID、重启次数或启动时间变化。谓词必须准确输出 true，
输入快照拒绝重复 JSON 字段，输入和子进程输出有大小限制。

GET/比较失败也交给必需的留存回调保存输入、当前对象与谓词输出；成功
后仍重新调用准入并检查原上下文。该函数无重试、无新预算、无集群写操作。
调用方必须独立固定 jq/谓词摘要并批准快照，且核验镜像来源、TLS、term
和所有权；此比较本身只证明读取时观察到的进程身份，不是分布式锁。
目前已完成组件与规范谓词的集成测试，尚未接入完整执行 CLI。

整包竞态回归通过（27.266 秒），同包 `go vet` 通过。新测试实际执行
仓库 jq 谓词并使用模拟 Kubernetes GET，覆盖相同进程、readiness 变化、
重启/镜像/spec/UID 变化、API 失败、留存失败及后置准入失败；没有调用
真实集群 API，不据此声称已完成在线镜像或故障验收。

检查命令组装时发现指标 Pod 映射可自相矛盾：此前仅核对每个指标期望
中的 UID，没有核对同一命名空间内名称与 UID 的一一对应关系。新增四个
反例（原始 UID 配其他名称、原始名称配其他 UID、两个采样同名不同 UID、
同 UID 不同名）均在修正前失败，表明错误配置原先被接受。

`MetricCommands` 现以原始探针/网络 Pod 的名称与 UID 初始化双向映射，
逐项核对全部指标目标，冲突时在本地组装阶段拒绝。允许一个 Pod 的多次
采样，也允许不同 Pod 使用各自一致的映射；新增正向测试验证原始 Pod
加同一 peer 的两个不同偏移采样。此静态一致性校验不替代最新 Pod GET
或独立批准的 UID 与成员身份映射，完整在线入口仍待接入。

修正后整包竞态回归通过（27.766 秒），`go vet` 通过；包含新增多 Pod
正向场景的指标命令定向竞态连续三次通过（1.186 秒）。未访问或修改集群。

`BindProcessAdmission` 现把实时进程比较接到命令计划的原始探针、before/
after 栈、全部指标、继任者及结果观察准入回调。每次比较仍调用原准入
前后检查，不替代镜像/工具来源、TLS、term 或所有权验证；网络阶段与
Join 不被此接线覆盖。原始快照来自同一网络计划，指标快照按目标顺序
核对名称和 UID，观察者必须是独立 Pod；同一 UID 的重复快照必须完全一致，
名称映射也不能冲突。构造时复制快照并检查 jq/谓词文件可用性，不调用
准入回调或 Kubernetes。结果保存为 `experiment-process-*.json`。

调用方仍须独立证明观察者 Pod 与 ObserverMemberID 的映射，单靠 Pod
快照不能证明 etcd 成员身份。该接线输出可传给 `ClaimAndRun`，不预填
后者负责安装的留存字段；完整 CLI 配置与来源准入仍未完成。

集成测试使用真实 jq 谓词和模拟 API，核对六条观察路径各自前后准入与
GET、六份留存文件、输入快照后续修改不影响已绑定内容，以及缺失工具、
目标不匹配、原始/观察者重合、矛盾快照、缺少准入等拒绝场景。首轮正向
夹具缺少恢复计划要求的 resourceVersion 被正确拒绝，补齐夹具后重新验证；
没有放宽生产校验。

修正后的定向竞态连续三次通过（1.282 秒），整包竞态通过（26.986 秒），
同包 `go vet` 通过。没有操作真实集群。

`VerifyInitialMembers` 补充故障前的只读协议身份准入：使用两个已批准的
直连 mTLS 客户端，分别读取原始端点与观察端点的 Maintenance.Status。
两个响应必须匹配计划中的 ClusterID、各自 MemberID、原始 Leader 和
InitialTerm，且无健康错误；保留完整 Status、起止时间、期望身份及错误。
完整宽度 ID 的期望值以十进制字符串保存，不经浮点数比较。每个 RPC
最多 5 秒且受原调用者剩余预算约束，无自动重试，无租约/网络/认领写入。

每次请求前后仍需调用方的独立准入，绑定端点与 Pod 进程、TLS、镜像和
源码；证据目录身份变化拒绝继续。该检查只适用于故障前，不能用来拒绝
隔离后的正常继任者选举。两个顺序 Status 读不是任期锁，原始探针在激活
前仍需复核任期；此组件尚待完整 CLI 的 preclaim 接入，不代表已经完成
在线实验准入或原 30 秒故障验收。

测试覆盖正确身份、错误集群/成员/领导者/任期、健康错误、RPC 失败、
错误观察者、返回时取消、前后准入失败和目录替换；断言不重试且留存所有
已完成的请求结果。整包竞态回归通过（27.407 秒），其后目录身份检查的
定向竞态回归亦已执行；未调用真实集群。

`RunVerified` 将上述组件组合为一条可调用执行路径：本地预检后，认领前
核对原始/观察/指标 Pod 进程与双端点初始 Status，随后进入 `ClaimAndRun`
的一次持久化认领、原生故障和独立恢复。它安装 `RunFaultJoin`，拒绝预置
Join；Owner 和留存仍按认领入口规则处理。恢复阶段只调用工具/来源准入，
不会重新要求故障前的初始任期，以免把正常选主当作恢复失败。

输入的 `AdmitTools` 仍必须真实验证固定工具、凭据、镜像/源码来源及端点
到 Pod 的映射；空回调仅允许在测试夹具中使用，不是实验准入。完整 JSON
CLI、实际来源校验和恢复后显式释放仍未完成，此组合入口不自动释放认领。

新集成测试运行真实 jq 与 Bash Join 夹具，使用模拟 Kubernetes/RPC：
成员身份错误时没有 CREATE 或 Join；身份通过后仅 CREATE 一次，随后
模拟准备拒绝，确认执行了独立 Join、进入恢复且认领未删除。没有调用
真实集群或启动实际故障注入。

组合入口加入后 `leasefault` 整包竞态回归通过（27.224 秒），同包
`go vet` 与 `git diff --check` 通过。

随后检查发现统一 `AdmitTools` 原先只在 preclaim 与 Join 调用，执行期间
仅依赖各阶段各自的准入实现。新增“CREATE 后工具来源变化”的反例表明，
准备阶段未先调用统一来源检查，错误仍来自后续准备回调。反例在旧实现
上明确失败，不能认为 preclaim 的来源结果在整个尝试中持续有效。

`RunVerified` 现为准备/所有权附加检查、网络、指标、原始探针、两次栈、
继任者与结果观察的准入统一加上前后 `AdmitTools`。缺失的阶段回调仍保持
nil 并由配置检查拒绝，不以工具检查替代阶段语义。来源变化时执行失败、
保留认领；若 Join 工具本身也不再通过准入，则不运行该脚本并报告恢复失败，
不能自动释放。所有检查消耗已有上下文预算，不增加原 30 秒门限。

修正后组合入口定向竞态连续三次通过（1.766 秒），整包竞态通过
（27.480 秒），同包 `go vet` 通过。没有操作真实集群。

`PrepareArtifacts` 补齐命令运行前的本地资源准备：按已有栈脚本契约独占
创建两个空的 0700 stack-worker 目录，以及原始探针、前后栈和每个指标
worker 各自独立的 0600 stderr 文件。指标回执目录仍由指标脚本创建。
方法不接受已设置的日志句柄，拒绝已有日志、栈目录、HOLD、认领或时钟
标记，并核对 owner 目录身份；不执行脚本、不调用在线准入或集群 API。

返回的 `CommandArtifacts` 包含带日志句柄的计划和指标目标，可交给
`RunVerified`。调用方必须在子进程 Join 后 Close；Close 同步并关闭
本次打开的文件，重复调用不会重复关闭。构造失败关闭已打开句柄，但不
删除部分目录/日志；应保留现场并使用新尝试，不能覆盖失败尝试重跑。
该方法只准备日志与栈目录，不生成或批准工具摘要、凭据及观察快照。

资源准备测试覆盖正确权限、空栈目录、独立可写日志、Close 后同步保留、
重复 Close、拒绝复用、已有日志不覆盖、HOLD/公开目录拒绝，以及配置
失败保留部分产物。整包竞态回归通过（30.852 秒），`go vet` 通过。

组合入口的集成测试随后改为直接调用 `PrepareArtifacts`，将其返回的
Plan 与 Targets 传给 `RunVerified`，不再使用手工日志句柄。覆盖的三条
路径（成员错误、认领后来源变化、准备拒绝）均在 native 子进程启动前
终止；测试确认关闭本次句柄后 stderr 文件保留，两次栈目录仍为空，认领
不会被删除。该串联测试定向竞态连续三次通过（1.927 秒），`go vet` 与
`git diff --check` 通过。它不证明实际故障子进程成功或完整 CLI 可用。

### 当前候选构建的等待点源码绑定（2026-09-20）

对候选提交 `0e7e75caebd96e2bb39ac2e0dc970a42c4923988` 的 Git 对象
单独读取 `pkg/server/etcd/lease.go`，SHA256 为
`3c98f802359a5f185dc6e618691ad6098641a54afa528668c6dfcaf8091ccd88`。
与已审查提交 `71bddd9a6de10723157ae5526797146e6a221407` 的该文件
执行 `git diff --exit-code` 返回 0；检查过期续租分支确认第 1645 行
仍为等待 revoked、termDone、请求取消和 freshness tick 的 `select`。

据此为 `expired-lease-wait-frames.jq` 添加该完整提交的精确绑定，未使用
短 SHA、分支名或通配规则。测试覆盖正常分类、错误文件摘要、短 SHA
拒绝，以及相邻行号不能匹配；现有 checked-out source 测试同时校验
实际文件摘要与等待点。命令
`go test -race -count=3 -timeout=2m ./hack/production -run '^TestExpiredLeaseWait'`
通过（1.525 秒），`git diff --check` 通过。

这只是候选栈帧的源码定位审查，不是 CI、镜像、运行容器或原始 RPC 的
准入证明。记录时该提交的镜像 CI `35490241957` 与回归 CI
`35490241952` 均仍在运行，未部署候选镜像、未执行集群故障实验；
原 30 秒真实故障验收仍未通过。

### 组合入口的 RPC 连接绑定（2026-09-20）

检查组合入口发现，初始原始成员校验曾通过独立的
`VerifiedCommandInputs.OriginalConnection` 进行，而准备操作使用
`Preparation.Connection`；二者可被调用方配置成不同连接。现在移除
重复入口，初始校验直接使用实际准备连接。结果核验和恢复连接由已通过
初始成员检查的 `SuccessorConnection` 统一赋值；禁止预配置
`RecoveryConnection`，避免另一个未经该成员检查的连接进入恢复路径。
这不替代 mTLS、端点到 Pod 映射或各阶段来源准入，也不保证网络连接在
后续时刻仍具备相同状态；原有实时检查保留。

集成反例覆盖准备连接指向错误成员（认领前拒绝），以及预配置恢复连接、
缺失准备/继任者连接（集群 API 调用前拒绝）。原有准备失败、成员错误、
认领后工具变化测试继续保留。实现修改后整包竞态测试通过（27.021 秒）；
补齐缺失连接反例后的定向竞态连续三次通过（1.899 秒），`go vet` 与
`git diff --check` 通过。没有执行集群故障，也没有放宽原 30 秒门限。

### 组合入口的镜像快照绑定（2026-09-20）

`RunVerified` 现在强制接收 `CommandRelease`，在集群访问和认领前调用
`CheckProcessImages`。复用镜像预拉取模块的 `ApprovedRuntimeDigests`：
验证 OCI 索引原始字节与不可变镜像摘要一致，且 linux/amd64、linux/arm64
子摘要与独立审查输入一致。随后检查原始 Pod、继任者 Pod 和全部指标
Pod 快照，要求目标容器唯一、spec 镜像精确匹配、容器运行身份完整、
imageID 匹配该 Pod 所在平台的子摘要或已验证索引摘要。不得把另一架构
的子摘要当作匹配；平台映射也不得夹带无关 Pod。

`CommandRelease.Platforms` 必须来自已准入 Node 的平台信息，而不是从
imageID 猜测。CI 成功状态、源码到镜像的对应关系、Node/Pod 映射和工具
来源仍由独立准入负责；任意 JSON 中自报的摘要不构成放行凭据。后续
实时进程比较继续防止快照之后发生镜像/容器替换。这还不是完整 JSON CLI。

反例覆盖错误架构、可变标签、索引字节变动、未知平台、无关 Pod、缺失
容器、重复状态、非运行状态、指标镜像不符和重复 JSON 字段。集成测试
确认错误索引在任何 Kubernetes API 调用前拒绝。整包竞态测试通过
（27.039 秒）；随后补上指标数量边界，最终定向竞态连续三次通过
（2.084 秒），`go vet` 和 `git diff --check` 通过。未操作真实集群。

提交 `957196d0` 上再次联合执行 `go test -race -count=1 -timeout=2m`
覆盖 `internal/leasefault`、`cmd/lease-fault-plan`、`cmd/lease-fault-recover`
和 `internal/imageprepull`（均位于 `hack/production`），四包全部通过，
分别耗时 27.330、1.113、2.225、4.912 秒；四包联合 `go vet` 通过。
同一提交的 `TestExpiredLeaseWait` 竞态测试也通过（1.253 秒）。这验证
新增镜像依赖与两个现有命令兼容，不表示这些命令已经提供完整实验入口。
同期远端 `0e7e75ca` 的镜像构建仍在 Build and push TiKV test image，
回归已推进到 Verify leadership freshness regressions，未推送打断 CI。

### 候选提交回归 CI 终态（2026-09-20 05:19 UTC）

回归 run `35490241952` 已于 `2026-09-20T05:19:17Z` 成功完成，
核验 head SHA 为 `0e7e75caebd96e2bb39ac2e0dc970a42c4923988`。
终态 JSON、完整日志及 SHA256SUMS 保存在
`/root/.local/state/kubebrain/ci-35490241952-terminal.qxPO3ebu`。
该结果不覆盖之后尚未推送的本地提交。记录时镜像 run `35490241957`
仍为 in_progress；未中断它，也未据此部署镜像或执行故障实验。

镜像 run `35490241957` 随后于 `2026-09-20T05:23:41Z` 成功完成，
head SHA 同为 `0e7e75caebd96e2bb39ac2e0dc970a42c4923988`。
终态、完整日志和 SHA256SUMS 已归档到
`/root/.local/state/kubebrain/ci-35490241957-terminal.GNnSpAeu`。
日志中的校验及推广步骤指向不可变镜像
`ghcr.io/fivetime/kubebrain@sha256:6dac8c87d80ce629c9d1c75b3bb94e106fc7788da02656b156792f0c54083798`。
该成功结果不覆盖后续本地提交，也不证明测试集群已部署此镜像或通过
真实故障验收。确认 dbaas 无运行中工作流后才推送后续提交。

### 认领前在线节点平台核验（2026-09-20）

`CommandRelease` 新增独立准入的 Node 名称到 UID 映射。静态镜像检查
要求每个快照中的 nodeName 都有对应 UID，同一 Node 不得配置矛盾的平台，
不得夹带无关节点。`RunVerified` 随后在认领前调用
`VerifyProcessPlatforms`：按名称排序、每个不同 Node 只做一次 GET，
单次最多 5 秒且受外层剩余预算限制，不重试；比较实际 UID、Node 类型、
名称、未删除状态及 `status.nodeInfo` 的操作系统/架构。不是从 imageID
或可修改标签推测平台。每次请求前后执行来源准入，期望映射在回调前复制。

实际/期望 UID 和平台、请求起止时间及失败信息留存在私有 owner 目录的
`experiment-node-platform.*.json`。该核验只发生于认领前，不声称持续
监控 Node，也不替代后续 Pod 进程比较、CI 来源验证或完整实验 CLI。

集成反例确认 Node 被替换、架构不符、API 拒绝及读后准入失败时不创建
认领、不运行 Join；多个快照位于同一节点时恰有一次 GET 和一份证据。
静态测试覆盖缺失及多余 Node 映射。定向竞态连续三次通过（2.548 秒），
复制期望 UID 后整包竞态通过（27.176 秒），`go vet` 和差异格式检查通过。
没有修改真实集群或放宽原 30 秒验收门限。

### bf4f1a28 候选等待位置审核

本轮两项 CI 成功且真实发布产物下载校验通过的候选为
`bf4f1a28be48ec8646f533afa7f09b20c2ec53c8`。独立读取该 Git 对象中的
`pkg/server/etcd/lease.go`，与已审核 0e7e75ca 版本无差异，完整文件摘要
仍为 `3c98f802359a5f185dc6e618691ad6098641a54afa528668c6dfcaf8091ccd88`；
第 1645 行仍为 refreshLeaseHoldingLocks 的 select 等待位置。分类器
仅增加这个完整提交号，不自动批准其后代或缩写 SHA。

新增正确候选、错误文件摘要、短 SHA、错误行号反例；定向竞态测试连续
三次通过（1.644 秒），差异检查通过。该登记只确认堆栈候选等待位置，
不证明原始 RPC 仍挂起、采样完整、进程未替换或真实故障验收成功。
完整执行入口及恢复确认仍需串联，未注入集群故障。

在提交 831b7c77 上联合验证 leasefault、lease-fault-plan、
lease-fault-recover、imageprepull 和 image-prepull 五包，竞态测试全部
通过（26.893、1.104、2.214、5.945、6.668 秒），联合 go vet 通过。
该检查覆盖现有组件兼容性，不等同完整执行路径测试。当前入口缺口仍为：
严格序列化配置载入及独立准入、同一配置驱动真实 TLS/API 连接与探针、
PrepareArtifacts 后实际调用 RunVerified、恢复后独立确认并显式释放认领。
现有 RunVerified 集成测试故意在准备阶段拒绝执行，不能用来证明原生
探针与真实网络故障已经端到端跑通。

### 命令连接组装

ObservationCommandPlan.OpenConnections 从原始探针自身的 Endpoint、
ServerName 和 TLS 文件字段生成初始/准备 RPC 连接；观察者使用相同
固定客户端凭据及独立审核的 endpoint/server name。两个目标必须是
不同的字面 IP，拒绝 DNS/Service 名称和同 IP 不同端口，避免将原 Pod
误当作独立观察者。沿用 NewFaultConnections 的固定文件字节、内嵌
mTLS kubeconfig、禁用代理/重试规则，创建的是惰性连接，不证明在线身份。

返回句柄统一管理关闭；第二个连接构造失败时关闭第一个，不返回部分
连接。Bind 只接受未安装 API/原始/后继/恢复连接的 runtime，绑定实际
句柄，后续 RunVerified 复用后继连接执行恢复。上层仍必须固定配置、
核对端点到实际 Pod 的映射并执行在线成员检查，不能以本地构造代替准入。

测试覆盖实际 gRPC target、句柄复用、同 IP、DNS、缺失 TLS 身份、原始
探针私钥路径错配及预配置 runtime 拒绝。整包竞态通过（27.560 秒），
go vet 和差异检查通过。此为执行入口的连接组装部件，完整 JSON CLI
和端到端原生故障测试仍未完成，未修改集群。

### 实际连接与 Pod 端点绑定

RunVerified 在任何 API 请求和认领操作前调用 CheckCommandEndpoints：
读取两个实际传输的 Target，要求 passthrough 字面 IP 和合法数字端口，
分别匹配已审核原始/观察者 Pod 快照的 status.podIP；原始目标还必须
与原始探针 Endpoint 完全一致。拒绝缺少目标信息的传输、DNS resolver、
错接观察者、同 IP 的两份快照及 hostNetwork Pod。快照自身仍须匹配
命名空间、原始名称/UID，并且两个 Pod 的名称和 UID 不同。

该项为强制本地一致性检查，不替代独立来源准入、实时进程比较和
认证成员 RPC 检查。集成反例检查端点错误时 API action 为空、没有
创建认领、没有运行 Join。正常夹具仍通过认领并在准备阶段故意退出，
不将其计为原生故障端到端成功。完整 JSON CLI 和真实 30 秒验收仍未完成。

本轮 leasefault、lease-fault-plan、lease-fault-recover 三包竞态测试
通过（31.302、1.111、2.527 秒），对应 go vet 与差异检查通过。测试耗时
不是故障验收耗时；本轮没有访问或修改真实测试集群。

### 序列化命令输入（仍非故障执行入口）

`lease-fault-plan --command-plan /absolute/private/command.json
--approve-sha256 <独立审核的完整文件摘要>` 新增命令输入检查模式，与
`--bindings` 互斥。成功仅输出
`LOCAL_COMMAND_INPUTS_VALID_NOT_EXPERIMENT_ADMISSION`，不接受 `--execute`。

NativeCommandPlan 的 version 为 1，包含 bindings、原始和观察者 TLS
端点、Kubernetes 连接信息、工具路径和 files 摘要表、Pod 进程快照、
release、指标目标、显式环境和恢复参数。duration/recovery_timeout
使用 `2m`/`30s` 一类带单位字符串；不允许数字隐含时间单位。term/member
继续沿用绑定中的字符串 uint64 编码，避免 JSON 浮点精度损失。
release.index 是原始 OCI index 字节的 base64，而非重新序列化的对象；
processes.observer/metrics 则为 Pod JSON 对象。文件描述符、回调、
已取得的认领以及预设故障起点不属于可序列化输入。

载入要求私有、非符号链接、最多 4 MiB 的绝对路径文件及匹配摘要；
拒绝未知/重复字段。使用与实际命令构造共用的配置检查验证环境、路径、
观测预算；验证指标端口不重叠、目标 UID 对应期望，检查必需文件摘要、
私钥和 kubeconfig 权限、执行文件权限、镜像/平台快照及实际连接端点。
连接仅惰性构造并关闭，不调用 API/RPC，不执行子进程，不创建日志或目录。

ObservationPlan 和 MetricTargets 转换结果不带日志句柄，供后续
PrepareArtifacts 接线使用。加载成功不是在线来源证明；files 表也不是
完整的传递依赖沙箱。运行前/运行中仍须独立核验 CI、工具依赖、环境和
实时身份，并串联实际 RunVerified、恢复确认与显式释放认领。当前尚未
提供执行这些动作的 JSON CLI，未将本地有效性检查计为故障验收。

三包整包竞态测试通过（leasefault 27.979 秒、lease-fault-plan 1.107 秒、
lease-fault-recover 2.259 秒）；随后补齐指标快照与目标映射的检查及反例，
两包定向竞态连续三次通过（1.675、1.102 秒）。go vet 与差异检查通过。
覆盖错误摘要、公共文件、符号链接、工具变更、缺失文件固定摘要、未知/
重复字段、数值 duration、伪造描述符、错误绑定、端口冲突、快照错配、
端点错配、恢复预算越界和 shell 启动钩子；载入前后 owner 目录内容不变。

### JSON 到实际生命周期的执行适配

RunNativeCommand 接受路径、独立批准的摘要和全部 CommandAdmission
在线准入钩子，载入并固定一份 NativeCommandPlan，将其真实 TLS/API
连接、PrepareArtifacts 日志、指标目标、恢复预算和网络观察参数接入
RunVerified。不允许缺失任何阶段准入；没有用本地校验成功代替来源或
在线身份证明。每次工具准入前后重新读取私有计划文件并比较原批准摘要，
覆盖认领前、观测和恢复阶段，禁止运行时静默切换计划。

适配层会执行真实生命周期，不能用于未授权集群。它不提供自动重试或
自动释放认领。返回时关闭父进程拥有的惰性连接和日志描述符，保留全部
证据；RunVerified 已回收其管理的子进程，但父描述符关闭并不证明逃逸
后代已退出。Join/恢复失败仍须独立恢复并取得新证据后才能显式释放认领。
Owner 为空也不被解释为认领 CREATE 一定未发生。

集成测试使用实际 JSON 载入、连接构造、产物建立和 RunVerified 接线，
在首次/认领前工具准入拒绝；另覆盖缺失准入、取消上下文、准入中计划
变更（产物建立前后）。这些反例必须在网络请求前退出，不产生认领或
故障时钟；建立过的空产物仍保留。该测试不是一次完整原生故障实验。
当前仍缺少 CLI 具体在线准入实现、恢复后独立释放流程和真实 30 秒验收。

本轮 leasefault、lease-fault-plan、lease-fault-recover 整包竞态测试
通过（27.597、1.117、2.249 秒），go vet、差异检查通过。未操作真实集群。
bc76eba3 的回归 35498089332 和镜像 35498089305 均已由 Runner 接手；
最近查询都处于 Go 环境准备步骤。本轮适配层变更先留本地，不推送打断作业。
