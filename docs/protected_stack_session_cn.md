# 受保护采栈会话工具

`hack/production/protected-stack-session.sh` 将此前实验目录中的会话逻辑
纳入版本管理，并接入 `same-pod-process.jq`。Ready/启动探针标志不再
用作进程身份；Pod UID、完整 spec、IP、容器 ID/镜像标识、重启次数
和启动时间仍必须相同。这不改变产品代码，也不重写既有失败实验。

## 调用约定

仅在新实验 owner、已完成诊断部署后使用。控制器先启用
`set -euo pipefail`、`umask 077`，在同一拥有子进程的 Bash 中 source
该文件，并设置以下变量，不能在命令替换/子 shell 中调用会话函数：

- `stack_owner`：私有证据目录；要求 `deployment-claimed` 存在、`HOLD`
  和 `final-exit-code` 不存在。调用者仍负责新 owner 的单次消费约束。
- `stack_kubeconfig`、`stack_context`：明确的目标集群，不使用隐式上下文。
- `stack_namespace`、`stack_namespace_uid`、`stack_sts`、`stack_sts_uid`：
  本轮重新核验的资源身份。
- `stack_tls`：包含 `ca.crt`、`probe.crt`、`probe.key` 的私有目录。
- `stack_server_name`：info 服务 TLS 名称。

owner 下准备 `diagnostic-spec.json`（完整 StatefulSet spec）、`info.crt`
（预先核验的服务端证书）及 `bin/info-diagnostic-probe`。固定工具目录
应同时包含库文件和 jq 身份规则。`tools.sha256` 必须覆盖这两个文件
及探针，`diagnostic-inputs.sha256` 必须覆盖 spec 和证书；路径使用
对应实际绝对路径。清单完整覆盖和摘要均在 prepare/capture 时检查。
清单自身的可信来源、源码/镜像绑定由外层实验准入验证，不能靠重新
计算清单将未知修改视为已审核。

外层控制器必须安装 EXIT 清理 `stack_session_close`，TERM/INT 分别
退出 143/130，并负责其余故障资源的恢复。调用顺序：

1. `stack_session_prepare <StatefulSet名称-序号>`，预先建立两个指向
   精确 Pod 的 localhost info 转发，固定端口 18584/18585。
2. `stack_session_capture before-fault`，输出目录由 `stack_capture` 返回。
3. 故障激活前记录原始纳秒起点；所有后续 capture 传同一个起点。
4. 读取采集结果后执行外层原有等待栈/降主栈分类与响应断言，最后恢复。

仅支持单应用容器模板、info 端口 8080。每次采集检查 namespace/STS
身份、诊断配置、采集前后进程身份以及与预热时进程的一致性。TLS
正向鉴权、匿名拒绝、服务端公钥 pin 验证由实际 protected-stack 探针
执行。此库不以 Ready 作为采栈条件，也不替代服务可用性门限。

匿名 TLS 拒绝有时会令对应 kubectl 转发因连接重置退出。故障前每次
成功采栈完成身份检查后，库会停止并 join 旧匿名转发，再向同一个
Pod 名称创建新匿名转发，为下一次采栈预热；日志使用独立文件保留。
只操作本 shell 所拥有的子任务，先确认端口空闲，再等待监听就绪；
端口占用、启动失败或取消均不写此次 `COMPLETE`。正向通道不重建。
此步骤记录为 `rearm-anonymous`，仅允许在尚未设置故障起点时执行。
故障后的采栈不重连、不补充时间预算；任何通道提前失效仍失败。
新通道不代表身份已验证：下一次采栈仍须做完整前后 Pod 身份检查、
SPKI/mTLS 和新的匿名拒绝验证。此前拒绝证据不得替代下一次验证。

## 原时间预算与失败处理

故障后总预算始终为原起点加 30 秒；单个外部操作上限 25 秒且受剩余
预算限制。禁止重新传入 before-fault 或更换起点。控制器必须继续用
原截止时间限制整个故障流程，不把探针完成标记当作全流程验收通过。

同步命令使用 foreground timeout，外层控制器应由可取消整个进程组的
监护进程运行。关闭只处理当前 shell 拥有的后台任务，避免对陈旧 PID
误发信号。任何失败都不得继续执行成功断言。完整采集及最后期限检查
通过后才写 `COMPLETE`；部分目录保留作为失败证据。

计时依赖 Bash 5 的 `EPOCHREALTIME`。每个新采集目录另存
`timing.tsv`：Unix 秒（微秒精度）、阶段名、
`start/end`、退出码（start 为 `-`）。阶段覆盖输入摘要验证、采集前
资源快照、前后进程身份检查、受保护探针及采集后 Pod 快照。只记录
阶段名，不记录命令参数、凭据或响应正文。成功采集的摘要清单包含
计时文件；失败/外层取消可能仅留下 start，没有 end，不得据此推断
命令成功或准确完成时间。该墙钟计时仅供定位耗时，不能替代原故障
起点、扩大预算或证明故障验收通过。已有冻结实验不回填此证据。

探针 CLI 还向私有 `probe.stderr` 写入逐行 JSON 请求计时，scope 为
`info_probe_request_timing`。每条只含固定阶段名、start/end、结果分类
和以 Go 单调时钟测得的相对 `elapsed_ns`；不写 URL、证书路径、
凭据、响应体或原始错误。阶段区分已鉴权 ping/ready、匿名 profile
请求和已鉴权 profile 请求。匿名阶段的 `request_error` 可以是预期
TLS 拒绝，只有后续原有证据验证才能证明拒绝；不能单凭计时分类判断
鉴权安全。计时写入失败也会中止采集，不能因同时出现预期 TLS 拒绝
而忽略写入失败。进程被强制终止时可能缺少 end；成功采集的摘要清单
包含 `probe.stderr`。CLI 最后一行仍可能是普通错误信息，应按行区分。
HTTP/1、禁用连接复用/压缩、独立匿名通道、请求顺序及超时保持原样。

## 验证范围与剩余工作

仓库测试使用合成 Kubernetes 响应和假探针，覆盖 Ready 前后变化、
重启、资源身份/spec 不符、端口占用、断开的通道、探针失败/错误模式、
原预算过期/未来时钟/重置、已消费 owner、清单漏项和探针篡改。外层
取消测试确认两个转发及阻塞探针退出且不生成完成标记。

`go test -race -count=3 ./hack/production -run '^Test(ProtectedStackSession|SamePodProcess)'`
通过（41.935 秒），`go vet ./hack/production` 和 Bash 语法检查通过。
会话链路包含 20 个场景，另有多文档输入拒绝及外层取消测试。保存的
真实实验 Pod 与诊断模板容器定义也经只读核对一致，不据此推断新实验
的未来运行身份或 Ready 状态。

2026-09-19 增加分阶段计时后，`go test -race ./hack/production -run
'^TestProtectedStackSession' -count=3` 通过（41.307 秒），`go vet
./hack/production`、Bash 语法与 diff 检查通过。新增断言覆盖成功阶段、
探针退出 17、原预算超时 124 和外层取消留下的部分轨迹；原完成标记、
摘要验证和子进程清理断言保留。尚无使用该版本的新真实故障实验。

同日补充探针内部计时：探针包完整 `go test -race -count=3` 通过
（7.164 秒），外层会话三轮 race 回归通过（43.315 秒），两包 vet、
Bash 语法及 diff 检查通过。新增覆盖成功/取消/错误 pin、首条计时
写入失败、匿名拒绝结束计时写入失败，以及 stdout 单一结果 JSON 与
失败时不落盘栈文件。未更改传输设置、身份验证、原截止时间或产品
逻辑；不能据这些测试声称已定位真实超时根因或通过原故障验收。

匿名通道生命周期修正后，`go test -race ./hack/production -run
'^TestProtectedStackSession' -count=3` 在源码稳定后通过（59.234 秒）。
覆盖匿名拒绝后转发退出、重建时端口占用/子进程启动失败、重建期间
取消、旧/新转发回收，以及故障后不再创建转发。此前一轮在测试过程中
修改了脚本排版，触发摘要不一致，已作废并完整重跑；不计为通过。
当时尚须验证完整实验控制器接入和真实集群效果，本次修改未用于正在恢复
的冻结实验。原 30 秒故障门限没有改变。

后续完整控制器离线接入验证（2026-09-19）：在新私有目录
`/root/.local/state/kubebrain/anonymous-rearm-integration.eE7CPGpX`
使用 `6566953c` 的冻结工具源码及只读历史资源 fixture，不复用任何在线
部署 claim。`term-controller-tests.wVghAQBl` 的 27 个场景全部通过
（驱动 session 30836，终态 0），包括响应 term、持久化键值、身份待收敛/
失败、清理失败和外层取消。正常完整流程预期七个转发子进程：三个 RPC、
两个初始 info 和两次故障前匿名通道重建；三个采栈探针仍分别调用。
全部保存的转发及请求探针 PID 均已退出。

另以独立 mock 显式模拟每次成功采栈后匿名通道退出，
`term-controller-tests.UpRyWH3n` 的正常流程、外层取消和取消伴随清理失败
三个场景全部通过（session 96331，终态 0）。正常流程验证三个完整采集
及其摘要，两次故障前重建、故障阶段没有重建；保留原响应先于恢复、
successor 原截止时间、清理退出码和无子进程泄漏断言。mock 仅模拟通道
退出，**不证明真实 TLS 拒绝**。输入脚本摘要保存在 `offline-harness.sha256`，
两个 suite 各有日志摘要。没有调用集群、创建镜像或执行新故障实验。

仓库同时增加 `repeated-anonymous-reset` 回归，覆盖连续两次故障前采栈，
每次匿名通道退出后重建，保持正向通道 PID、原故障起点和完整摘要，
最终回收全部四个 info 转发子进程。完整会话 `go test -race
./hack/production -run '^TestProtectedStackSession' -count=3` 通过
（62.389 秒）；vet、Bash 语法和 diff 检查通过。真实集群效果及完整
30 秒验收仍未由这些离线结果证明。

这些测试不证明真实 mTLS、数据面隔离或 30 秒故障验收。新实验驱动
尚须使用新的冻结工具目录接入本库，完成准入、真实实验及恢复；不能
在已消费的 `fault-2ad79751.N8nMZWbk` 中替换脚本后重跑。

## 同一身份约束下的指标捕获

新增 `stack_session_capture_metrics before-fault` 或
`stack_session_capture_metrics "$fault_start_ns"`。仍需原有 prepare、
完整冻结输入及调用方 EXIT/TERM/INT 清理约束；不是独立可直接执行
的采集脚本。输出在新的 `metrics.*` 目录，路径仍由 `stack_capture`
返回。使用 `protected-metrics` 探针保存 `metrics.txt`，不输出栈文件。

复用原 namespace/StatefulSet/spec/Pod 进程校验及前后同进程比较，
不要求 Ready；任何身份漂移拒绝生成 COMPLETE。要求探针退出成功，
摘要模式及所有证明边界正确，文本语法验证为 true，语义证明为 false；
同时验证非符号链接原始文件、非空且不超过 8 MiB 的长度及实际
SHA-256 与摘要相符。文件、摘要、身份和时序
统一写入 evidence.sha256。COMPLETE 仍仅代表本次采集完整，不是指标
语义、业务可用性或整个故障验收通过。

故障起点沿用同一不可重置 30 秒预算；故障前成功采集后可重建匿名
通道，故障中不能重建或延长预算。不能用指标替换必需的降级等待栈
断言，也不能在已经接近截止时追加采集再扩大门限。持续采样调度和
目标 family/同进程差值分析仍待实现，未部署到真实集群。

新指标模式 21 项模拟用例与原栈流程一起 race 通过（28.791s），覆盖
Ready 改变但进程不变、进程重启、namespace/STS/spec 漂移、超时、
时钟重置拒绝、绑定缺失/篡改及原始文件哈希/长度不符。vet、Bash
语法和 diff-check 通过；模拟测试不代替探针的真实 TLS 测试或集群验收。

### 输入校验也受剩余故障预算约束

输入 manifest 的 sha256sum 和精确绑定匹配现在通过
`stack_session_run` 执行，每次使用原故障起点的剩余时间，结束后再
检查截止时间。不会因校验阻塞而继续进入后续探针，也不删减校验。
这用于严格拒绝超预算采集，不是缩短故障恢复延迟的修复。

新增阻塞哈希回归，模拟只剩一秒的故障预算，要求返回 124 且测试
进程在六秒外部安全界限内退出。初版完整回归失败：模拟器错误地在
任何 timeout 包裹命令成功时关闭匿名通道，且慢探针用例的一秒余量
不足以到达探针。已限制模拟器只在探针成功后关闭通道；慢探针用例
从 fault+28s 开始，仍要求原 fault+30s 截止及探针返回 124。

一次尝试把校验合并为子 shell 的方案被阻塞测试发现会遗留子进程，
已撤销，并精确结束其测试 sleep。最终实现直接约束各外部命令，不
保留该子 shell 方案。此前失败会话 65158、70444 不计为通过。
最终旧栈、新指标和阻塞哈希组合 race 通过（会话 16331，100.212s），
vet、Bash 语法与 diff-check 通过。未改集群，未追溯改变故障验收失败。
