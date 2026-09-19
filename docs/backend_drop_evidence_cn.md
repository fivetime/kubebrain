# 后端丢包证据解析

仓库中的 `hack/production/monitor-stream.jq` 与 `backend-drops.jq` 是
后续故障驱动应使用的解析规则。它们迁自旧实验私有目录的已使用规则；
迁移只读取规则，没有执行旧实验脚本，也没有改变原来的丢包验收条件。
旧私有副本保留为历史证据，不应作为新驱动的运行依赖。

先用 `jq -Rn -f monitor-stream.jq` 解析完整 monitor stdout。只允许首行
可选的既有 Cilium 启动记录，其余必须是 drop 事件；未知日志、重复或
迟到的启动记录、非 drop JSON、损坏记录和无事件输入均失败。

然后将 events 数组中的每个事件交给 `backend-drops.jq`，独立传入实际
endpoint ID、原 Pod IP 及冻结的后端 `{ip,port}` 列表。只保留同源 endpoint/IP、
TCP、明确策略拒绝且目标 IP/端口匹配的事件。原实验条件仍要求至少一条
PD 2379 丢包和一条 TiKV 20160 丢包；仅有一种后端不能通过。

这些规则对应既有 Cilium 1.19.4 捕获格式，不保证其他版本格式兼容。
解析成功不证明来源可信、采集窗口正确、策略生效于所有连接、任期丢失
或原请求完成。调用方仍须绑定实际 capture 的 Pod/Agent/CEP 身份、
前后进程状态、策略实现及后端地址来源，并在原 30 秒预算内完成所有门限。
必须在调用 jq 前限制输入体积，并监督解析进程，不能把无限输入读入内存。
本次收录尚未构成可直接部署的完整 ObserveFault 驱动。

回归测试覆盖两类后端、已知启动行、denylist 拒绝，以及错误 endpoint、
源/目标 IP、端口、拒绝原因、UDP/空 TCP、异常日志和截断数据。
工作流包含明确的竞态测试命令及三份文件的路径触发契约。

## 独立继任者观察

原生驱动可在丢包门限验证后调用 `leasefault.ObserveSuccessor`，通过已
认证的健康成员连接获取 Status，不读取原续租响应来决定继任 term。
独立绑定包括集群、观察成员、旧 leader、旧 term 和原故障时钟；观察
成员不得是旧 leader。必须验证响应来自绑定的观察成员，并确认 leader
非零且不再是旧 leader、term 严格增大。整数比较保持完整 uint64 精度。

所有请求、准入检查和证据留存使用同一个原故障截止时间（不超过
origin+30s）。单次 Status 最多 5 秒，尚未换主或暂时不可用时相隔
200ms 再作独立只读观察；非暂态 RPC 错误或身份不符立即失败。
这不是原续租的重试。连接必须由调用方禁用传输层隐式重试并完成准入，
准入与留存回调必须遵守 context。每次响应/错误都先留存再决定结果，
留存失败、超时或认领丢失均不得接受；留存回调收到响应副本。

Status 的告警字段不作为健康结论；观察到新 leader/term 也不证明旧节点
已停止等待、丢包范围正确或原请求已完成。ObserveFault 仍须组合独立
策略/丢包门限，随后由原生生命周期执行降级栈、原响应、指标和恢复
检查。本地测试含模拟状态转换及实际 KubeBrain/memkv RPC，不是一次
真实集群选举或原 30 秒验收。

## 激活策略观察入口

`NetworkObserver.Active(ctx, origin)` 复用现有
`observe-local-policy-state.sh present`，不调用恢复阶段的后端 TCP 连通
检查。它在每次只读观察前后重新核对认领与 Pod 身份、实际 CREATE
回执、当前 API 策略 UID，以及唯一由预留 selector 切换而来的精确
已批准 active spec。策略尚未激活、UID 被替换或规则漂移均失败。

只有身份核验后的明确 pending（退出码 75）才会在同一原故障截止时间内
重采样；全部输出/状态必须留存。函数入口和返回都复核绝对截止时间，
不通过恢复预算延长故障观察。成功仅说明同一进程的策略实现状态匹配，
不是报文拒绝或任期切换证明；仍需丢包与独立继任者门限。

适配器测试覆盖 active、pending 后匹配、仍为 inactive 和被替换的策略，
并确认 active 路径不会调用恢复/TCP 观察。模拟 API 与脚本边界测试
不替代真实 Cilium 实现或完整实验验收。

## 原生故障门限组合

`FaultObservation.Observe` 可作为 `FaultLifecycle.ObserveFault`，依次执行
策略实现、两种后端丢包、独立 Status 换主、再次策略实现检查。所有步骤
共用调用方原故障 context/origin；入口拒绝超过 origin+30s 的截止时间，
每个门限前后及每次 Status 前后均复查准入和绝对截止时间。失败返回零 term，
不继续后续门限；最终策略失效或证据留存失败也不能交付继任 term。

`Active` 可绑定上述 `NetworkObserver.Active`；`Drops` 仍须由具体捕获
适配器完成同源、同窗口、PD/TiKV 双门限与留存，不能用只解析历史文件
代替。`CheckIsolation` 必须新鲜核对认领、进程、精确 active 策略及独立
连接准入，而不是复用缓存结果。Successor 的初始身份必须与原探针一致。
最后一次实现检查只是再次采样，不证明采样之间持续隔离。

本地顺序测试与原生生命周期集成测试覆盖门限失败不查询 Status、观察
后失去策略/取消、超预算、留存失败及既有恢复路径；网络和继任 Status
仍为模拟，不能作为在线 Cilium 丢包或原 30 秒验收证据。完整捕获适配器
及生产 CLI 组装仍待完成。

## 实时丢包观察脚本

`deploy/test-cluster/observe-local-backend-drops.sh OWNER EXPECTED_POD TARGETS SECONDS ORIGIN_NS`
调用仓库内 `capture-local-cilium-drops.sh`，不读取旧实验目录作为输入。
SECONDS 为 1–9 秒；调用方必须用原故障 context 监督整个进程组，不能
另开恢复预算。脚本另在入口、API 查询前后、捕获后和完成前复核原时钟。
它不安装策略、不重试捕获，也不自行恢复集群。

EXPECTED_POD 和 TARGETS 必须是独立准入的冻结文件。TARGETS 使用现有
TCP 观察器的六个后端 `{name,uid,ip,port}` 格式，捕获前后重新读取 Pod
列表核对 UID/IP；捕获中的两个 endpoint 快照均须匹配原 Pod 进程。
实际捕获子进程负责同一 Agent/CEP 身份与远端 timeout 回执；观察器核对
私有目录、成功回执、清单、endpoint、请求时长，以及本次调用之后且原
30 秒以内的采集区间。它只消费本次已准入子进程产生的清单，不是任意
外部证据导入器；脚本及依赖哈希、环境和集群身份仍须外层独立准入。

monitor stdout 超过 4 MiB 则拒绝进入 jq；完整解析后，必须同时匹配
PD 2379 与 TiKV 20160 的同源策略拒绝。输入文件在结束时再次验哈希，
输出私有 `backend-drops.*` 目录及明确不代表任期/RPC 成功的标记。
`observation.exit=0`、完整标记、原截止时间和子进程退出都必须由调用方
检查。不能只凭目录存在或 matches.json 非空接受。

本地测试运行真实观察脚本与 jq 规则，模拟捕获/API 边界，覆盖双后端
成功、单后端、源 Pod/目标后端替换、旧窗口、捕获失败、损坏/超大日志、
输入漂移及已过期时钟。尚未执行在线捕获；Go 适配器见下文，生产入口仍待组装。

`NetworkObserver.Drops(ctx, origin, seconds)` 现已连接上述脚本，可作为
`FaultObservation.Drops` 的闭包。它复用 Active 的实际预留回执、精确
active 策略 UID/spec、原 Pod 和独立后端输入哈希验证，使用同一故障
截止时间监督并等待子进程退出。只执行一次，退出码 75 也不能重试。
退出成功后还要求精确两行 stdout、同 owner 下的私有证据目录，以及
固定文件 `observation.exit=0` 和相同原时钟；不接受任意 exit-zero 输出。
所有执行/解析结果先交给强制 Retain，随后重新准入；留存失败、输入
漂移、策略替换和超时均拒绝成功。Retain 必须持久保存输出/状态及所指
证据目录；具体源码、环境、kubeconfig、排他归属仍由 Admit 独立确认。

适配器本地测试使用模拟 Kubernetes 与脚本边界，覆盖成功、75 不重试、
坏标记、外部目录、错时钟/退出回执、输入变化、留存失败、未激活策略及
非法捕获时长。与真实脚本的独立测试互补，不是在线故障实验；生产入口
仍需绑定具体独立连接、捕获时长、准入和证据留存。

`NetworkObserver.CheckActive(ctx, origin)` 用于独立 Status 轮询前后的
网络准入部分：每次重新读取 Pod/策略并检查实际预留 UID、精确 active
spec、输入哈希和调用方认领；不启动外部观察脚本，也不复用 Active 的
缓存结果。成功只证明该次 API/输入匹配，不能替代 Active 的 Cilium
实现观察或 Drops 的报文门限。生产组装的 CheckIsolation 还须验证独立
Status 连接的身份/来源。测试验证未激活、策略替换均拒绝，并验证一次
成功检查后的策略替换会被下一次检查发现。

## 具体网络生命周期组装

`NetworkFaultRuntime.Run` 将现有原生 `FaultLifecycle` 与仓库网络观察器
连接起来。调用方提供已有认领、原始探针/两个栈 worker、指标 hooks、
独立连接准入、证据留存、恢复连接及 Join；本入口不发现身份、不获取或
自动释放认领，也不替代 CI/镜像准入。

组装时要求原生命周期留空 NoncesSafe、ReservedReady、ObserveFault、NetworkRestored
和 IdentityRestored，避免静默覆盖或混入外部故障脚本。独立观察的 cluster、
old leader、old term 必须与原探针初始身份相同，观察成员必须不同；不允许
预填故障时钟。上述冲突在任何集群准备操作前失败。准备阶段、故障阶段
和恢复阶段共用同一个由 Preparation 绑定的 NetworkObserver；准备的 Own
同时核对实际 owner 与网络准入。

NoncesSafe 也固定绑定同一网络观察器，执行仓库的完整 nonce 扫描，不能
由调用方提供另一套替代扫描。它在准备过程及激活前重复观察，不允许把
准备时的旧快照作为激活时的安全证明；完整 collector/classifier 依赖仍需
纳入 AdmitNetwork 的工具哈希与集群准入。集成测试确认扫描发生在故障
观察前，且本地重复 hook 配置在集群变更前拒绝。

故障观察仅能调用一次：具体 Active → Drops → 独立 Status → Active，
Status 前后同时执行 CheckActive 和独立连接准入；原请求结果检查前后也
重新执行 CheckActive，沿用唯一原时钟。恢复仍由原生命周期在 Join 后
以独立预算执行，失败实验不能通过恢复转为验收成功。

组装测试覆盖初始身份不一致、重复 hooks、无准入、非法绑定、预置时钟、
提前 outcome 与重复执行的拒绝。它们是本地绑定测试，不证明在线全流程；
独立命令行计划/配置加载和实际部署验收仍未完成。

进一步的原生生命周期集成用例实际调用 `NetworkFaultRuntime.Run`，
连接真实子进程监督、原探针/两个栈 worker 与 memkv 协议恢复 RPC。
Kubernetes、网络观察脚本边界和独立 Status 仍为模拟。成功路径验证
六次准备检查、两次策略观察、一次丢包与独立换主，并沿用指标时钟；
丢包失败路径验证不会查询 Status，但仍先等待子进程退出，再执行具体
恢复观察、协议清理和标签恢复。测试继续保留显式 owner 释放前的恢复
校验，不以模拟成功宣告在线验收完成。

## 指标证据接线

`MeasuredNetworkFaultRuntime.Run` 在具体网络生命周期上安装已有的
`retirementmetrics.WorkerGates`，不接受调用方替换 Baseline/Completed 或
Inject。独立的指标预期必须与 worker 命令逐一对应，并明确最小计数、
采集偏移、指标键和是否要求耗时；预期不得从子进程输出反推。
所有 worker 必须属于同一实验 Namespace/StatefulSet、同一 spec 哈希和
cluster 标签，但可以观察不同 Pod。Pod 身份与命令对应关系仍由
AdmitMetrics 独立认证，不以字符串相等代替在线准入。

指标门限复用实际 capture/清单解析、baseline 不变性、原 30 秒采集时钟、
计数及可选耗时差值校验；每次指标准入前后也重新检查认领和网络准入，
留存失败不能通过。原时钟由调用方提供并留存，不由这个组装层重建。
本地测试覆盖配置冲突、跨实验身份、缺失门限、超预算偏移、取消及无
真实认领时拒绝；既有 WorkerGates 测试覆盖证据解析和计数/耗时差值。
这仍是生产命令入口所需的运行时接线，不是已完成的 CLI，也不证明真实
集群验收通过；本次没有部署或注入故障。
