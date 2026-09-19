# 退任通知可观测性补充

## 最新 CI 核验（2026-09-19）

恢复写阶段已补充真实 KubeBrain 服务集成测试：构造 RPCServer、
memkv 后端及真实 lease/alarm 实现，经内存 gRPC 创建测试租约/键、
激活 CORRUPT，再调用 RestoreProtocol 和独立最终核验。分别覆盖
有效租约和实际观察到 TTL<0、grantedTTL>0、键仍保留的过期租约，
恢复后再次调用也通过。首轮有效租约 race 1.216s；完整三轮包 race
11.237s，vet/diff-check 通过。对照服务源码确认 CORRUPT 下只读预检
仍可用，撤销须在 disarm 后。领导权及 Kubernetes 准入是固定 fixture，
没有 TiKV、TLS 或真实网络故障，不能替代集群验收；当前仍为本地改动。

恢复写阶段补充拒绝路径回归：模拟撤销返回精确 LeaseNotFound 但
租约实际仍保留，以及成功应答但未撤销，均必须走到最终 TTL 读取
并失败，不能靠写响应判成功或重试；另外验证 alarm 已清除后第二次
写前准入失效会阻止撤销、两次写后最终准入失效也不会返回成功。
三轮 leasefault race 1.631s，vet/diff-check 通过。本轮查询 b356b257
两项 CI 仍运行中；此测试扩展尚未推送，无集群操作。

候选 b356b257 已推送，镜像 `35444751084`、探针 `35444751082`
本轮查询均运行中。后续本地实现 RestoreProtocol 的受限协议写阶段：
持久化意图和独立准入 callback 每次写前重查，预读拒绝无关 alarm、
租约上的其他键及测试键的新归属；只清除指定 CORRUPT alarm、撤销
指定租约，随后执行完整协议只读核验，不直接删键或修改 Kubernetes。
对照 lease.go remainingTTL，保留过期租约 TTL 可低于 -1 的行为；
alarm 清除后正常过期撤销可能抢先完成，精确 LeaseNotFound 仅允许
进入最终核验，不直接判成功。其他写错误保留且不重试。模拟 RPC
覆盖归属冲突、缺失意图、准入失败、部分恢复错误和成功，三轮 race
1.614s、vet/diff-check 通过。未接入真实驱动、未执行集群 RPC，
网络/标签恢复与外层独占恢复责任仍需实现；不在当前 CI 源码中。

候选 2950044a 镜像 CI `35443211237` 已全部成功（30m46s），原
watch 正常退出。API/jobs/完整日志归档至
`image-ci-35443211237-terminal.8Dd4hMno`，源码 SHA、所有作业成功
终态及 SHA-256 核验通过；此候选镜像/探针均成功。
后续恢复代码组合 race 已通过：leasefault 1.248s、metricsworker
10.782s、响应 CLI 1.048s、原探针 2.197s、workflow 2.579s，相关
vet/diff-check 通过。准备推送恢复候选并独立验证 CI，尚未部署或
接入真实故障驱动，不改变原完整 30 秒验收失败的结论。

候选 2950044a 探针 CI `35443211258` 已全部成功，API/jobs/完整日志
归档至 `probe-ci-35443211258-terminal.5EvUsbgo`，源码 SHA、所有作业
成功终态及 SHA-256 核验通过。镜像 `35443211237` 仍运行中，未重跑。
探针成功覆盖响应 CLI，不覆盖后续本地恢复记录与恢复验证器。

本地新增 VerifyProtocolRecovery，只发送 Alarm(GET)、带 keys 的
LeaseTimeToLive 和精确键线性化 Range，逐条验证集群/header、alarm
为空、租约已撤销及 key/count/more 均为空。过期但 grantedTTL 非零
不视为恢复；调用方提供已认证健康连接及独立最多五分钟恢复预算，
不改变原 30 秒验收预算。模拟 ClientConn 验证方法、请求、失败路径，
补充进程内 gRPC/protobuf 回归验证大整数和过期/撤销区别。三轮 race
1.576s、vet/diff-check 通过。仅只读检查，无真实集群 RPC；实际恢复
写操作、网络/标签恢复、现场身份检查和驱动接入仍未完成。

协议恢复记录已增加实际子进程生命周期联动回归：父进程先持久化，
再启动 Bash 准备进程，覆盖准备失败、READY 后取消、故障 deadline
和成功。WithPreparedFault 返回后先核验直接子进程已消失，再以独立
绑定读取原记录；超时/取消保留对应错误。三轮 race 1.462s，vet 和
diff-check 通过。测试没有恢复 RPC，也不证明逃离进程组的后代清理
或真实集群验收；此扩展仍为本地提交，不在运行中的 2950044a CI 内。

已推送候选 2950044a，镜像 `35443211237`、探针 `35443211258`
本轮 API 查询均运行中，未部署。后续本地增加协议恢复意图持久化：
独立私有 owner 目录下 create-once 记录绑定 namespace/STS/cluster、
alarm member、lease 和测试 key，文件及目录 Sync 成功后才允许调用方
开始协议变更；恢复按“均可能已尝试”处理，不依赖子进程内存标志。
读取时严格绑定独立准入信息，拒绝链接/FIFO/公开权限/损坏/重复字段，
已有或不完整记录不覆盖。首轮测试发现临时目录非私有，修正 fixture
权限而未放松实现。三轮 race 1.128s；最终包及 CLI 组合 race 为
1.065s/1.036s，vet/diff-check 通过。该接口尚未接入真实驱动，网络
策略/标签身份、独立恢复隧道和现场核验仍待接入；不代表已能恢复，
不在当前 2950044a 的 CI 中。没有执行任何集群变更。

候选 c04e59c0 镜像 CI `35441571752` 已成功结束，包含发布后验证、
标签更新和清理。API/jobs/完整日志归档至私有目录
`image-ci-35441571752-terminal.ZUq21A67`，源码 SHA、所有作业成功
终态及 SHA-256 核对通过；该候选的镜像与探针 CI 均成功。
新 CLI 推送前组合 race 回归通过：CLI 1.036s、原探针 2.199s、
leasefault 1.049s、metricsworker 10.792s、workflow 2.586s；
实际 worker 即时/准备握手联动回归 34.043s，相关 vet/diff-check
通过。下一候选仍需独立 CI；真实驱动及外层恢复尚未接入，未部署。

候选 `c04e59c0289f9666bac5a9b040c31b8db2d9aa7a` 探针 CI
`35441571766` 已成功结束，API/jobs/完整日志归档至私有目录
`probe-ci-35441571766-terminal.Zxs8irzO`，源码 SHA、所有作业成功
终态及 SHA-256 均核对通过。镜像 `35441571752` 仍在运行，未重跑。
该 CI 不覆盖后续本地 CLI 提交 `0988024d`，未部署新镜像。

使用已退役 `fault-peer-budget.mjraWKki/deploy-execute.z3z8kYNU`
保存的真实 probe.jsonl 对新 CLI 做只读离线兼容核验。绑定来自
grant.json、successor/input.json 和独立健康观察者 sample-47/status.json
中的十进制字符串，未从待验证响应推导身份/后继任期。命令成功输出
fault_to_response_ns="29974426963"、ttl="10"、raft_term="268"，
并明确 fault_acceptance_proven=false。这与原始响应 29.974426963 秒
一致；原 driver 的 run.exit=124 及完整 30 秒验收失败结论不变。
未执行旧驱动、未修改旧证据、未写集群；它不是新实验或恢复证明。

候选 c04e59c0 的镜像 `35441571752` 与探针 `35441571766` 本轮
API 查询均仍为 in_progress，未重跑或部署。等待期间补充仓库内
`lease-fault-response` 命令行入口，供后续真实 shell 驱动调用严格
原始响应校验器：stdin 有界读取，六个独立绑定参数强制规范十进制，
拒绝重复/缺失参数，输出整数使用字符串避免下游浮点损失，明确
fault_acceptance_proven=false。调用方仍须保留原故障 deadline、
日志来源、原始探针 wait/退出码及恢复责任；真实驱动尚未接入。
命令、校验包及 workflow 契约 race 分别通过 1.036s、1.049s、
2.620s，命令 vet 和 diff-check 通过。新增显式 CI 路径和命令检查；
这些修改尚未推送，不在上述运行中的 c04e59c0 CI 内。

候选 082955e9 镜像 CI `35440159534` 已全部成功（30m19s），
含发布后验证、标签更新及清理；原 watch 正常退出。API、jobs 和
完整日志已归档至私有目录 `image-ci-35440159534-terminal.6Ir0mSdB`，
源码 SHA、成功终态和 SHA-256 核对通过。该候选镜像/探针均成功。
后续本地超时原因与原响应校验器的组合检查通过：metricsworker
race 10.779s、leasefault 1.048s、lease-term-probe 2.192s、workflow
2.602s；vet/diff-check 通过。准备推送新候选独立核验，不借用上一
候选结果；真实故障驱动和恢复接入尚未完成，无集群部署。

候选 `082955e9dc14a76070699e0e5e675e8e40167c40` 探针 CI
`35440159520` 已全部成功（24m40s），包括最终全量 race；原 watch
正常退出。API、jobs 和完整日志已归档至私有目录
`probe-ci-35440159520-terminal.3qVfKQnM`，源码 SHA、成功终态和
文件 SHA-256 核对通过。镜像 `35440159534` 仍运行中，未重跑。
此结果覆盖准备握手/实际 worker 联动，不覆盖后续本地超时错误链
修正或 leasefault 校验器；不是已完成真实故障接入或验收，未部署。

开始迁移旧真实驱动的原始响应校验到仓库 `internal/leasefault`：
严格读取三条 JSONL，绑定预检/发送/响应顺序、租约与集群身份、
初始及独立观察到的后继任期、原始响应 30 秒边界。整数不经浮点，
覆盖超过 2^53、uint64 上界及有符号租约 ID，拒绝重复/未知字段、
缺失 TTL、错任期和 1ns 超界。保留原驱动响应时间的包含上界规则，
外层全部验收的严格预算仍须独立执行。它不是故障隔离、退出码或
恢复证明；真实驱动接入尚未完成。实际 lease-term-probe 编码器的
本机 gRPC fixture 输出已由新校验器验证，合成时钟不用于真实验收。
三轮 race：probe 4.387s、validator 1.074s；workflow race 2.636s、
vet/diff-check 通过。CI 工作流新增显式校验器检查并由契约测试锁定；
本地改动未推送，不在正在运行的 082955e9 CI 中。

新候选 `082955e9dc14a76070699e0e5e675e8e40167c40` 已推送，镜像
`35440159534`、探针 `35440159520` 均运行中，独立跟踪且未部署。
等待期间补查准备握手超时的错误链：300ms 故障 deadline 终止脚本
后，缺失完成记录/管道关闭会掩盖 DeadlineExceeded，只剩 Canceled。
新增 errors.Is 断言先复现失败；现在所有注入返回路径同时保留故障
context 错误，原始脚本/协议错误不丢弃。完整协调器三轮 race 通过
（30.286s），vet/diff-check 通过；测试同时确认外层预算仍有效、
直接子进程已回收。只修正错误分类，不改变取消、恢复或原始门限。
该本地修正不在正在运行的 082955e9 CI 中。

候选 e13f3691 镜像 CI `35438633456` 已全部成功（32m6s），含
发布后验证、标签更新及清理；原 watch 正常退出。API、jobs、完整
日志归档至私有目录 `image-ci-35438633456-terminal.oqQjjqNf`，
源码 SHA、成功终态和 SHA-256 已核对。因此该候选镜像/探针两项
均已成功，但不覆盖后续本地修改，也不等于真实故障验收。

下一批准备握手/环境隔离修改的组合验证通过：metricsworker race
10.795s、retirementmetrics 2.348s、delta CLI 5.735s、workflow
2.676s，vet/diff-check 通过；实际 worker 联动回归的完整结果为
前述 38.090s。准备推送新候选，须独立核验它的 CI；未部署。

候选 `e13f36916ac705e8bd73879e050658f62a3c80f8` 的探针 CI
`35438633458` 已成功结束（24m45s），包括最终全量 race 回归，
原 watch 正常退出。API、jobs、完整日志已归档到私有目录
`probe-ci-35438633458-terminal.XV9np1hD`，源码 SHA、成功终态及
SHA-256 校验通过。镜像 `35438633456` 仍运行中，未重复触发。
此结果不覆盖后续环境隔离、准备握手和脚本联动本地提交；未部署，
不改变此前真实故障失败结论。

握手适配进一步接入实际 protected-metrics-worker/会话脚本回归，
覆盖单/双 worker、基线接受/拒绝，并与原即时命令路径共享相同
证据校验。准备阶段启动的合成子进程由同一 shell 在时钟交付后
wait，成功退出回执为 0；不是真实原始 RPC。修改前原 worker 组
race 通过（21.502s），扩展后完整 worker 组通过（38.090s），
vet/diff-check 通过。此测试扩展尚未进入 CI，无集群变更。

本地新增 WithPreparedFault，保持准备及故障阶段在同一个已准入
子 shell 中，避免把原始探针交给无权 wait 的新 shell。严格握手
绑定一次原始时钟，错误/缺失/超长完成记录、重复调用或忽略注入
错误均拒绝；恢复不在适配层执行。首轮完整协调器 race 通过
（10.474s），加入与 Run 的组合回归后三轮通过（29.926s）；补齐
缺失/超长完成记录后三轮握手回归通过（4.552s），vet/diff-check
通过。组合用例覆盖基线拒绝、故障失败、完成拒绝以及成功，均在
返回后核验故障脚本与采集直接子进程不存在。仅为本机合成脚本
验证，真实驱动及外层恢复尚未接入，不在当前 e13f3691 CI 中。

候选 `e13f36916ac705e8bd73879e050658f62a3c80f8` 的镜像
`35438633456`、探针 `35438633458` 本次查询仍运行中，未重复触发。
接入检查发现 worker 的 `Env=nil` 会意外继承父进程环境，与完整显式
环境契约不符。新增 nil/空/显式三种子进程回归，修改前 nil 场景
实际得到父环境哨兵值而失败；改为非 nil 环境副本后，协调器三轮
race 通过（26.777s），实际 worker 回归通过（21.487s），vet 和
diff-check 通过。此修正仅在本地，不在上述运行中的候选 CI 内；
未部署，真实故障驱动接入及原 30 秒门限验收仍未完成。

上述候选两项 CI 均结束后，本地外部回调适配与脚本联动的组合验证
全部通过：metricsworker race 9.613s、retirementmetrics 2.433s、
delta CLI 5.791s、workflow 2.665s，以及全部 MetricsWorker 回归
21.514s；vet/diff-check 通过。下一批源码需独立 CI 验证，尚未部署。

候选 `b57276d20212682475d713f8db14ab458e525868` 探针 CI
`35437154458` 已全部成功（26m26s），包括调度 CLI、基线时间与
协调器回归以及最终 race 组，原 watch 已正常退出。API、jobs 和
完整日志归档至私有目录 `probe-ci-35437154458-terminal.4QzKr1S4`，
源码 SHA、终态与文件哈希核对通过。镜像 `35437154435` 已全部成功
（30m55s），包括发布后校验、标签更新及清理，原 watch 正常退出。
API、jobs 和完整日志保存在私有目录
`image-ci-35437154435-terminal.kX9zD5xI`，源码 SHA、成功终态与哈希
已核对。该结果不覆盖后续本地 RunFaultCommand 与外部回调
联动测试，不是新集群实验或旧故障失败的改判；未部署。

本地实际 worker/会话集成进一步接入 RunFaultCommand，不再使用无操作
Inject：合成外部回调等待单/双 worker 的调度完成标记，证明其采集
在回调执行期间推进；Origin 独立验证每份基线阶段早于原始时钟。
回调日志必须保留精确原始纳秒值，返回后其直接 PID 不存在；基线
拒绝时日志为空、回调未启动，原 worker 退出/转发清理断言保持。
首轮 race 通过（17.512s），三轮重复通过（50.520s），vet/diff-check
通过。只证明本机脚本联动，不证明真实 Kubernetes 故障/网络/TLS，
新改动尚未推送，不在当前 `b57276d2` 的 CI 中。

新候选 `b57276d20212682475d713f8db14ab458e525868` 已推送，镜像
`35437154435`、探针 `35437154458` 均确认运行中；独立跟踪原 run，
未重复触发或部署。等待期间为真实驱动接入增加 RunFaultCommand：
Inject 可执行已独立准入的外部脚本，追加同一个原始纳秒时钟参数，
显式环境不继承控制进程变量，输出只进私有普通文件；超出原窗口的
deadline 等无效输入启动前拒绝，取消后等待直接子进程退出。
首版协调器三轮 race 通过（25.189s）；补齐私有日志/晚返回限制及
与 Run 的联动测试后三轮通过（26.736s），vet/diff-check 通过。
联动覆盖成功、脚本失败及剩余 500ms 原故障预算耗尽：失败不执行
完成验收回调，Run 返回时故障直接子进程和采集直接子进程均不存在。
不证明逃离进程组的后代已清理，也不授权脚本自行恢复或以 exit 0
替代原始故障检查。此本地适配尚未接入真实实验、不在当前 CI 中。

原候选全部 CI 终态后，后续本地修复的组合 race 检查再次通过：
metricsworker 8.790s、retirementmetrics 2.445s、delta CLI 5.771s、
workflow 2.643s；vet/diff-check 通过。准备将这些修复一起推送为新
候选，仍须核验新候选 CI，不能用上一候选结果替代。

候选 `a776293cb2fbe889a0c3f04bc3148e06ab995e23` 探针 CI
`35435337854` 已全部成功（24m14s），含此前失败的租约相关步骤、
采集证据和最终 race 回归；原 watch 已正常退出。API、jobs、完整
日志归档至私有目录 `probe-ci-35435337854-terminal.O1XyFzwr`，源码
SHA、成功终态与文件哈希已核对。镜像 CI `35435337855` 第二次尝试
现已全部成功（29m24s），包括原先 503 失败的扫描、发布后校验、
标签更新及清理，原 watch 正常退出。该次 API、attempt 2 jobs 和完整
日志保存在私有目录 `image-ci-35435337855-attempt2-terminal.K4B8TYB6`，
源码 SHA、attempt、成功终态与文件哈希已核对；首次 503 失败仍保留。
该探针成功不覆盖后续本地回调 deadline、调度 CLI 或基线时间校验
提交，也不将先前真实故障实验改判通过；未部署或重跑实验。

修复候选 `a776293c` 的镜像 CI `35435337855` 首次尝试失败：
kubectl 安全扫描查询 `vuln.go.dev/ID/GO-2021-0160.json.gz` 返回
HTTP 503，随后退出 1；不是漏洞命中或已确认的编译缺陷。原 API、
jobs、完整日志及核验过的 SHA-256 已归档至私有目录
`image-ci-35435337855-failed.2FsO6OKm`，源码 SHA 已核对。
仅对已失败的镜像作业发起一次重试，保持安全扫描不变；探针 CI
`35435337854` 继续运行，未取消或重复触发。

等待期间只读检查
旧实验驱动的故障安装、drop 证据、successor、原始响应及恢复顺序，
未执行或修改旧 owner。发现协调器虽然按原始 +30 秒取消，但
Inject/Completed 的 context Deadline() 仍暴露外层预算，命令适配器
可能据此算出过长的单步 timeout。现收紧为原故障截止与外层截止的
较早值，保留原 worker 取消计时器。新增测试覆盖两种截止先后关系，
同时检查两个回调所见的精确截止与返回后的进程退出。
完整协调器 race 五轮通过（39.740s），实际 worker 脚本的单/双进程
联动 race 通过（17.514s），vet/diff-check 通过；该本地修改不在
运行中的 `a776293c` CI 内，不借用其结果准入，也未接入真实实验。

为脚本实验驱动增加 `retirement-metrics-delta` 的可选调度核验入口：
同时要求独立提供 schedule 路径、原始纳秒时间戳和计划偏移，调用
既有 LoadScheduledCapture 验证后续采集；任一参数出现即禁止静默
退回普通差值模式。输出保留 fault/readiness/event-latency 为 false，
时钟用 JSON 字符串避免浮点精度丢失。子进程测试覆盖成功、结合
duration、1ns 错时钟/偏移、过早/过晚、缺失标记、坏清单及参数拒绝，
验证退出码与 stdout；CLI/底层解析三轮 race 分别通过 15.071s/4.874s，
vet/diff-check 通过。此入口初版不验证基线先于故障、worker 退出和恢复，
不等于真实驱动已接入；本地修改未在当前候选 CI 中验证。

后续补齐基线时间线：调度模式使用 LoadPrefaultCapture，要求整个
基线采集（含匿名重建）的最后阶段保守上界严格早于原始故障起点。
仅 probe 提前结束不够；微秒阶段时间戳使用 +1us 上界，等于边界也
拒绝。新增底层边界/身份/重建测试及 CLI 子进程负例：baseline probe
在起点前完成、identity-after 在起点后结束，两采集仍不重叠，必须
返回失败且无成功输出。底层与 CLI 三轮 race 通过（4.962s/15.196s），
vet/diff-check 通过。不证明文件落盘时间、时钟真实性、worker 退出
或整体故障验收；普通非调度模式保持不变。

候选 `69bc9334` 探针 CI `35433943696` 已失败，不能部署；失败在
租约任期测试的早期同步竞态，本地未修改版本重复测试已复现。
仅修正测试等待位置后，100 次 race 和完整租约相关 race 组通过，
详见[失败、修正与验证记录](lease_renewal_admission_test_20260919_cn.md)。
原失败日志已归档，后续未执行的探针步骤不能算通过。镜像任务
`35433943731` 已成功结束，证据归档至私有目录
`image-ci-35433943731-terminal.ZH0QuMOK`，源码 SHA 与哈希已核对。
原任务均结束后已推送修复候选 `a776293c`：新探针 CI `35435337854`
及镜像 CI `35435337855` 已创建，尚未取得终态，不准入部署。

新候选 `69bc9334` CI 执行期间，本地将实际 worker/会话协调器集成
扩展到双 worker：同一实验目录先准备一次共享冻结输入，后续独立
Bash 进程只读取输入，各用独立端口和 0/100ms 偏移。每个 worker
分别验证 LoadScheduledCapture、同进程计数差值、调度输入和退出
回执；最后一个基线被拒绝时不选择起点、不注入，两个 worker 都
以 143 清理退出。正常或拒绝返回时，六个模拟转发进程均不存在。
初轮通过（17.531s），三轮 race 通过（50.498s）；Kubernetes、TLS
和探针仍为模拟器，不宣称真实故障或端口竞争已验证。已随上述修复推送。
补充每个配置端口仅属于一个 session 的日志路径断言后，完整 worker
race 再跑通过（21.488s），vet/diff-check 通过。

上一批两项 CI 结束后，合并后的本地检查全部通过：完整 metricsworker
race 8.768s、retirementmetrics 2.370s、delta CLI 3.473s、workflow
2.611s、全部 worker 回归 13.219s；vet/diff-check 通过。准备将这些
后续修复作为新候选推送；不得借用上一候选的 CI 结果部署。

候选 `cc06a777179e7cfa37885bc1951765ecce80cc9c` 的探针 CI
`35432495145` 已完整成功，包括最终 race 组，原 watch 已正常退出。
API、jobs 和完整日志归档到私有目录
`probe-ci-35432495145-terminal.PmCpSQmb`，源码 SHA 与证据哈希核对
通过。镜像任务 `35432495109` 随后成功结束（30m43s），包括发布
校验、晋级及清理；源码 SHA 与证据哈希核对通过，日志/API/jobs
保存在 `image-ci-35432495109-terminal.p6lIRhsF`。两条原 watch 均
正常退出，此批无剩余运行任务。上述成功不覆盖
后续本地 TERM 清理、owner 观察、调度证据及 Origin/Inject 拆分，
也不改变原真实故障验收失败结论；未部署或触发新实验。

已修正下述时钟下发缺口：`Hooks` 拆分为只产生时间戳的 Origin 和
接收该时间戳的 Inject，二者均为必填。全部基线核验后，先确定原始
起点、启动同一 +30 秒取消计时并写入所有 worker 的输入管道，再
调用一次 Inject。worker 可在故障安装/观测回调执行期间运行；任一
worker 失败、owner 终止或原截止时间到达，均取消该回调的 context。
回调自身必须遵守取消，外部驱动仍须在 Run 返回后才能开始恢复。

双 worker 回归在 Inject 内等待两个 worker 写出所收到的时间戳，
从而验证广播不再依赖 Inject 返回；另验证注入失败或只剩约 0.2 秒
预算时不进入完成核验。实际脚本集成明确使用 no-op Inject，仍不能
宣称真实 Kubernetes 故障安装已验证。当前改动未推送，不覆盖远端
仍在运行的 `cc06a777` CI。
协议/owner/协调器三轮 race 通过（24.246s），实际 worker/会话集成
三轮通过（26.005s）；vet/diff-check 通过。此处修复的是调度顺序，
不改变任何历史 30 秒验收失败结论。

只读复查已退役 owner 的 `run-term-experiment.sh`：原驱动在约
171 行记录 fault_start_ns，随后串行安装策略、观察策略和采集丢包
证据；约 195 行观察 successor，之后才采 demoted stack。恢复脚本
`term-cleanup.sh` 先关闭原栈会话，再执行网络/协议恢复。未执行或
修改这些已退役脚本，未复用其 owner。

由此确认真实接入还存在顺序缺口：当前 `metricsworker.Run` 的 Inject
钩子返回后才广播原始时钟。若将旧驱动的故障安装/观测整段放入该
钩子，指标 worker 会延迟收到时钟，错过预定早期采样。现有成功
集成钩子只产生时间戳，不能作为真实故障安装已正确并行的证明。
下一步必须区分“确定并广播原始起点”和“执行故障操作”，同时保留
同一 +30 秒门限、错误取消及恢复前 join；基线就绪屏障应放在原
请求已确认阻塞和故障输入冻结之后。现有串行 demoted stack 门限
不能因引入指标采样而删除或放宽。

新增 `LoadScheduledCapture` 并接入实际 worker/协调器测试的完成钩子。
独立输入原故障起点、offset 和 Kubernetes 准入身份，核对固定三项
调度清单及采集路径/完成标记，再执行完整 LoadCapture 检查。采集
阶段和探针均受预定偏移及原 +30 秒窗口约束，拒绝故障期匿名重建。
普通文件限长读取拒绝符号链接和 FIFO；不以清单路径驱动任意读取。
阶段时间按微秒精度保守处理；不把哈希一致性称为来源认证，也不
以阶段终点代替清单写完时间或整个故障验收结论。

实际脚本集成三轮 race 通过（25.831s），完整 retirementmetrics 与
delta CLI 三轮 race 通过（4.877s / 8.263s），vet/diff-check 通过。
新测试覆盖错起点/偏移、过早/过晚阶段、故障期重建、清单和原始采集
篡改、异路径、符号链接、FIFO 及错误准入。当前仍为本地未推送修改，
原 `cc06a777` CI 正常运行，未部署或重跑集群实验。
最终全部 worker 回归再跑一次 race 通过（13.195s）。

随后补齐协调器自身的 owner 观察：启动绑定 owner/claim 目录身份，
在注入前后及证据钩子边界复查，并以 100ms 观察器取消阻塞钩子的
context。拒绝 HOLD/终态（包括悬空符号链接）和目录替换。完整
协议/协调器三轮 race 通过（23.583s），实际 worker/会话集成三轮
通过（25.967s）；未操作集群。这不是恢复控制器的原子互斥锁，
真实实验仍须独立串行化注入、取消回收与恢复。
追加的启动前终态不创建进程、注入期间终态不下发时钟及所有者
回归组再跑三轮通过（1.410s），vet/diff-check 通过。当前远端
`cc06a777` 两项 CI 仍在执行，本地补充未推送以免取消它们。

本地后续将实际 worker/会话脚本测试接到 `metricsworker.Run`，通过
Baseline/Completed 钩子核验前后 LoadCapture、计数差值、原始故障
起点/offset、调度清单、COMPLETE 和退出回执。正常路径三轮 race
通过（15.793s），并检查三个模拟转发子进程均已消失。此处只下发
时钟，没有注入真实 Kubernetes 故障。

取消改为 TERM 后最多 5 秒升级 KILL，宽限仅供回收，不计入成功
验收预算。增加实际基线拒绝路径后，首轮三次运行中一次缺少退出
回执（整个测试命令失败，24.578s）；代码分析发现关闭 stdin 与
异步 TERM 可竞争：EOF 可先触发 EXIT 清理，再被 TERM 退出打断。
现将 stdin 关闭移到 worker 等待结束之后；修复后成功/拒绝路径
五轮 race 通过（41.943s）。另以 TERM trap 内有界 read 明确检查
输入仍打开（应超时而非 EOF），防止仅靠概率性重复测试。
最终将失败回执严格限定为 TERM 的 `143` 后，再跑三轮实际脚本
集成通过（25.577s）；含确定性输入管道检查和 TERM 忽略后升级的
协调器三轮 race 通过（23.218s），vet/diff-check 通过。
这些修改晚于正在运行 CI 的 `cc06a777`，尚未由该候选的 CI 覆盖。

随后本地增加父进程 worker 协议读取器，并用于实际 worker/会话脚本
产物回归。首轮协议单测的空输入夹具错误追加了合法 READY，导致
预期拒绝断言失败；已移除该追加，另以显式非法消息测试永久失败。
修复后协议与全部 worker 三轮 race 通过（1.031s / 27.952s），
workflow 合约 race 通过（2.549s），vet 与 diff-check 通过。
这些本地修改尚未由远端 CI 验证，也尚非完整多 worker supervisor。

候选 `03d27d3881297984aef0b93e770a46f567a22cf4` 的探针任务
`35430808031` 已成功结束（25m51s），包括最终全探针 race 回归；
同源后端任务 `35430808015` 也已成功。探针 API、jobs 和完整日志
保存在私有证据目录 `probe-ci-35430808031-terminal.p3SctDO1`，已核对
源码身份和文件 SHA-256。此前缺少运行时 rg 导致的工作流失败已在
此候选验证通过，不回写旧候选的失败结论。

镜像任务 `35430807890` 随后成功结束，发布镜像校验及晋级全部通过。
API、jobs 和日志保存在 `image-ci-35430807890-terminal.Afau0SPw`，
源码身份和 SHA-256 已核对，原 watch 正常退出。上述 CI 不覆盖本地
后续 `ef9a2b20` 至 `e9256764` 的所有者终止
检查和独立 worker 实现。多 worker 协调及恢复前取消、回收仍需完成，
本次未部署候选、未重跑故障实验，原 30 秒验收失败结论不变。

协调器新增 `internal/metricsworker.Run`：并发启动最多 16 个独立
进程组，全部 READY 通过调用方基线核验后仅调用一次故障注入钩子，
再向各 worker 下发相同原始时间戳。原起点 +30 秒与调用方 deadline
取更早约束，不能从注入回调返回时间重算预算。任一失败取消所有
进程组；函数返回前等待各直接子进程和协议读取协程。拒绝复用证据
路径，完成消息、EOF 和退出成功之后还必须调用独立证据核验钩子。
真实实验准入、owner 状态/唯一端口检查、LoadCapture/调度/退出回执
核验及恢复驱动仍需接入，当前不是可直接运行的真实故障控制器。

协调器协议/进程测试三轮 race 通过（6.249s），覆盖双 worker 就绪
屏障及统一起点、屏障不完整时取消两进程、基线拒绝、提前退出、
挂起、过期/未来起点、完成证据拒绝、重复路径，以及仅剩约 0.2 秒
时不重置故障预算。最终组合 race：协议/协调器 2.768s、全部 worker
10.037s、工作流 2.597s；vet/diff-check 通过。直接子进程的 /proc
记录在 Run 返回时已消失；这不扩大为任意后代已回收的证明。

## 动机与边界

候选 `2fd00721` 的真实故障实验未通过原 30 秒完整门限，集群已恢复，
详见 [失败与恢复记录](peer_retirement_budget_ci_20260919_cn.md)。
源码复查发现 `peerRetirementSender.onTermRetired` 原来直接丢弃发送
结果，缺少区分未发送和未确认的指标。本次补充诊断，不是延迟根因
修复，也不改变该历史失败结论；没有重跑集群实验或改变门限。

对照本地 `/root/etcd/server/etcdserver/raft.go`：etcd 根据 Raft
SoftState 更新 leader/leadership 指标。KubeBrain 的 TiKV 锁选举及
额外退任通知不是同一机制，通知成功不能当作 etcd 等价的 leader
就绪事件，也不应据此填充或重定义兼容指标。

## 新指标

仅显式启用实验性 peer retirement 的构造路径注入指标客户端。
普通构造路径不启用该协议。发送回调每次产生一个计数和一个秒数
样本；没有指标客户端时继续按原路径工作。

| Prometheus 名称 | 含义 |
| --- | --- |
| `leader_retirement_peer_result` | 回调结果计数 |
| `leader_retirement_peer_duration_seconds` | 回调耗时直方图，包含跳过的回调，不含此前生命周期 join 和本地释放 |

唯一新增标签为固定枚举 `outcome`：`missing_condition`、
`canceled_before_send`、`confirmed`、`unconfirmed`。
`confirmed` 只代表原发送逻辑在预算内接受了 HTTP 204，**不证明
新 leader 已就绪或原业务请求已完成**。取消发生在发送过程中时，
归入 `unconfirmed`，不伪装成未发送。

不输出 holder、端点、scope、所有权条件、令牌或传输错误内容。
指标返回错误不会触发重试、重新激活旧任期或跳过安全检查。
仍保留原发送预算、单端点单次尝试、mTLS 和生命周期 join 顺序。
指标需及时采集；旧 Pod 删除后不能依靠新 Pod 的计数追溯旧进程，
本次新增指标也不能补全已经结束实验的历史数据。

## 本机验证

- `go test -race -count=1 ./pkg/server ./pkg/server/service/leader -run 'Test(PeerRetirement|ScopedPostJoin|PostJoinRelease)'`：通过，38.197s / 2.317s。
- `go test -race -count=1 ./pkg/metrics/prometheus`：通过，1.093s。
- `go vet ./pkg/server ./pkg/server/service/leader ./pkg/metrics/prometheus`：通过。

新增测试覆盖四种固定结果、跳过路径不发网络请求、指标写入失败不
重试、nil 依赖安全行为及导出的名称、标签、单位和说明。本次尚无
新 CI 或集群部署结果；下一步仍需定位退任与选举各阶段的真实延迟。

## 本地条件释放的独立观测

随后补充 `leader_retirement_local_result` 和
`leader_retirement_local_duration_seconds`。仅原实验性作用域释放
路径在实际调用本地条件释放后记录，缺少条件不会伪造一次本地调用。
固定 `outcome` 为 `confirmed`、`unconfirmed`、`deadline`、`canceled`。
同时检查返回错误和调用结束时的上下文状态，取消由我们自己的清理
操作触发之前采样；不输出原始错误或所有权条件。

本地耗时不含前面的生命周期 join 和后面的 peer 通知。未增加重试，
本地预算仍为一个 RetryPeriod，原始冻结条件原样传递给 peer。
这仍不是整段退任耗时，也不补证历史实验的发送结果。

首次两项 leader 测试在后台 mock 失败后阻塞于读取缺失的 peer 回调，
已分别取栈并终止（会话 56828、39442，非通过结果）。单例进一步
确认：测试的 10ms 预算下，内存后端 nil 返回发生在约 23ms，因而
指标正确为 deadline，而初版测试错误预期 confirmed。测试现按真实
上下文核验，并在 Campaign 已退出后先断言回调存在，避免无限等待；
确定性单测单独锁定晚到成功、包裹 deadline、取消和不确定提交分类。
生产预算、提交语义及旧任期安全约束未变。

后续验证：完整 leader race 包通过（4.301s），本地释放/指标相关
三轮 race 通过（4.709s），完整 Prometheus race 包通过（1.093s），
`TestPeerRetirementCampaignPartitionToStorageRelease` race 通过
（1.404s），相关 `go vet` 通过。一次误用名称的筛选没有执行测试，
不计覆盖；已按上述真实名称补测。未触发 CI 或变更集群。

## 组合验证与 CI 启动

候选 `4aab067fcec420e6443dde2dfac3c6c45ad55ac9` 组合 race 检查
通过：server 33.132s、leader 2.307s、Prometheus 1.052s（会话
93458；`retirement-observability-validation.yw2Rq2vc` 保存日志和摘要）。
推送会话 14029 退出 0，origin/dbaas 已更新，自动触发以下任务：

- 镜像：[35427286402](https://github.com/fivetime/kubebrain/actions/runs/35427286402)。
- 后端协议：[35427286386](https://github.com/fivetime/kubebrain/actions/runs/35427286386)。
- 探针回归：[35427286439](https://github.com/fivetime/kubebrain/actions/runs/35427286439)。

初始 API 记录均绑定该候选，尚无通过结论；证据目录
`retirement-observability-ci.fjZ3pXP3`。没有重复手动触发、部署或
故障注入，原集群基线保持。本节是启动记录，不是 CI 终态。

## 完整本地释放回退链路的覆盖补充

源码审查发现原 `TestPeerRetirementCampaignPartitionToStorageRelease`
包装锁仅实现条件快照接口，没有暴露 `RetiredOwnershipReleaser`。
因此它能验证 peer 的真实条件 CAS，但没有经过显式作用域配置下的
“先本地释放，再通知 peer”分支。原测试保留，不追溯扩大其覆盖。

新增 `TestPeerRetirementCampaignLocalTimeoutThenHealthyPeerRelease`：
旧节点包装锁暴露真实作用域，本地释放等待上下文截止，健康 helper
仍连接同一内存存储并通过真实 mTLS handler 执行条件 CAS。测试要求
本地调用仅一次、预算不超过 RetryPeriod、结束后才通知 peer；同时
保留生命周期 join、清理、不可重新激活、持有者清空等断言。

新旧两条链路连续五轮 race 通过（3.936s）；全部
`TestPeerRetirement*` race 通过（38.637s），`go vet ./pkg/server`
通过。该测试只补覆盖，不改变产品行为，不模拟真实 TiKV 的延迟，
也不能证明集群 30 秒门限已修复。

三个候选 CI 已由排队转为运行；当前查到镜像在 Go 安全检查环境
准备，后端和探针在 Go 环境准备，没有成功/失败终态。此处新增的
测试尚未推送，避免取消正在运行的候选 `4aab067f` 工作流；不将它
算作该候选 CI 的覆盖。

## 按实集群选举参数的本机计时回归

`go list -m` 确认实际使用 client-go `v0.36.2`。其 `renew` 每轮以
RetryPeriod 调度、在 RenewDeadline 内轮询；后端锁的 `genContext`
继承父上下文。KubeBrain 在 elector 结束后仍须等待生命周期 join、
清理旧租约，再执行本地条件释放及 peer 通知。因此 RenewDeadline
不是端到端 failover 上限，不能把剩余所有时间都归因于某一次 RPC。

新增 `TestPeerRetirementCampaignProductionTimers` 使用与专用集群
一致的 LeaseDuration=30s、RenewDeadline=25s、RetryPeriod=500ms，
保留旧节点本地释放超时、健康 peer mTLS + 条件 CAS 的完整路径。
从设置隔离到通知回调结束采用不可重置的 30 秒上限；没有改变既有
集群门限。本机 race 通过（会话 86354，包 27.194s），回调耗时
**26.041285651s**。原两条快速路径再跑三轮通过（2.791s），vet 通过。

这只说明该状态机在可控内存存储、快速生命周期清理条件下不必然
耗满 30 秒；没有测量真实 TiKV、下一任完整初始化或故障后诊断采集，
不能据此宣称实集群应当通过，也不能把余下差值直接归因于存储。
后续采集必须分别绑定退任、local release、peer notification、新任
初始化及请求转发时间，避免把状态观测延迟当成实际选举时刻。

候选 `4aab067f` 的三项 CI 最新仍在运行：镜像模块扫描、后端 race、
etcd 服务与 Watch 回归。新增计时测试仍仅本地提交，不取消当前 CI。

## 故障期间采集的现有工具边界

已审查 `capture-rollout-tls-metrics.sh`：它要求 stable rollout phase、
每个目标容器 Ready，且通过 probe Pod 内 curl 仅提供服务端 CA。
这与当前故障场景不兼容：旧 leader 可因后端隔离不 Ready，临时
diagnostic info listener 又要求客户端证书。不能原样调用后将失败
或缺少计数解释为“没有发送退任通知”，也不能删掉既有安全检查。

`info-diagnostic-probe` 已提供 SPKI 绑定、认证连接和独立匿名连接
拒绝证明，但目前采集的是 goroutine 栈，不是指标。
`pod-log-capture` 可在固定身份下保留不重连的日志流，但日志不含
新增的 Prometheus 计数，不能替代指标采集。

下一轮真实故障之前需要单独完成故障态指标采集能力：保留 namespace、
StatefulSet、Pod UID/containerID/imageID 前后身份核验，使用验证名称
和 SPKI 的 mTLS，明确不以 Ready 为前提、也不声称 Ready；限制响应
大小和总时长、输出不覆盖旧证据。采集应在恢复滚动之前结束并持久化，
缺失样本不可补零，跨进程计数不可相减。这些是尚未实现的准入要求，
不是已有采集结果；不为此重新启动旧实验或关闭 TLS 校验。

CI 只读复查证据保存于 `retirement-observability-ci.fjZ3pXP3/`
`progress.CX7oO7Se`，三个 run/job 仍实际运行；没有重复 dispatch。

## 受保护的原始指标采集模式已实现（尚未接入集群实验）

`info-diagnostic-probe --mode protected-metrics --metrics-output <新文件>`
复用现有固定服务端名称/SPKI、mTLS 及独立匿名通道的真实 TLS 拒绝
校验，访问 `/ping` 与 `/metrics`，不访问 `/ready` 或 pprof。
原 protected、protected-stack、disabled 模式行为及栈输出字段保留。
禁止同时指定 stack-output；其他模式不得指定 metrics-output。

受现有 25 秒总上下文和 8 MiB 响应上限约束，不跟随重定向；响应
必须非空且以换行结束，文件以 0600 排他创建，已有文件不覆盖。
新摘要字段为 `metrics_bytes`、`metrics_sha256`，不冒充 stack 字段。
这是**原始传输捕获**，未解析或证明 Prometheus 语义；摘要明确
`metric_semantics_proven=false`、`pod_identity_proven=false`、
`fault_acceptance_proven=false`、`readiness_checked=false`。
不能仅凭换行就宣称响应是有效指标，更不能把缺失计数补零。

完整探针包 race 通过（3.477s），vet 与 diff-check 通过。新增测试
覆盖 TLS 1.2/1.3、错误 pin/名称、未保护监听、重定向、空/截断/
超大响应、文件不覆盖、独立通道、模式参数隔离及输出权限/声明。
仍须补齐调用方的 Pod/进程身份前后核验、有限采样调度、指标语义及
同进程差值校验，才能接入真实故障实验；本次没有部署或集群采集。
候选 `4aab067f` 三项 CI 复查仍运行，本机新增工具改动不属于该 CI。

### 原始采集的语法校验补强

随后采集模式改为调用已有依赖中的 Prometheus 文本解析器，保存前
要求格式可解析且至少含一个样本；拒绝 HTML、仅注释/类型声明、
非法数值、非法标签和冲突 TYPE 声明。解析错误统一返回固定描述，
不泄露指标名称或值。解析后重新检查总上下文是否已经到期。
新增 `metrics_text_syntax_validated=true`，仍明确
`metric_semantics_proven=false`：NaN/Inf、目标类型/标签、缺失 family
以及跨进程重置需要专门语义验证，不因语法检查通过而获准做差值。
最终完整探针包三轮 race 通过（9.120s），vet/diff-check 通过，未部署。

候选 `4aab067f` 的后端协议 CI **35427286386 已成功结束**，精确 SHA、
run/job API 和完整日志已保存在 `retirement-observability-ci.fjZ3pXP3/`
`backend-terminal.CuWeti5w` 并生成摘要（会话 12356 退出 0）。镜像和
探针 CI 最新仍运行，不能称三项全通过；本机未推送工具改动不在这次
后端 CI 的验证范围内。

### 后续终态与进程清理回归

镜像 CI `35427286402` 已成功，API 核对源码为
`4aab067fcec420e6443dde2dfac3c6c45ad55ac9`。run/job API、完整日志和
已校验摘要保存在私有证据目录 `retirement-observability-ci.fjZ3pXP3/`
`image-terminal.IhjsW4hL`。这是候选源码的构建结果，不覆盖之后的本机工具改动。

新增 metrics session 外部取消回归，与 stack session 共用断言：
必须已进入探针阶段，外层返回 124，两个转发进程与阻塞探针均已退出，
没有 COMPLETE 或事后身份采样。哈希校验截止测试也记录并检查子进程 PID，
不能仅凭调用返回超时判定清理完成。

stack/metrics 外部取消、匿名通道重建取消、哈希截止四项三轮 race
通过（32.651s）；metrics 成功路径单独通过（4.737s），vet 和改动文件
diff-check 通过。未修改集群、未重跑故障验收；指标语义和同进程差值
校验仍待完成，不能据此宣称原 30 秒门限或整体生产就绪通过。

### 取消和写入失败边界回归

#### 离线采集目录加载与命令

#### 生产导出标签联测发现并修复不匹配

#### 退休阶段累计耗时差值

#### 候选 3f64735d 探针 CI 失败：采集测试缺少运行时工具

worker 的 READY 后输入等待改为单秒分段读取，持续复查 owner；原固定
60 秒总期限不因读取分片而重置，完整记录到达后再检查期限。新增保持
stdin 打开时 HOLD/终态退出、跨一次 read 超时的分片输入成功，以及
“read 成功但已到总期限”拒绝的测试。失败不输出 CAPTURED，仍检查子
进程回收。首轮出现 baseline-failed 合成用例达到 8 秒上下文上限；检查
发现合成库仅 TERM 后无限 wait，已改为有界 TERM/KILL/join。尚不能单凭
后续通过证明该超时的唯一原因，原失败保留。最终 worker 全组三轮 race
通过（27.706s），vet/bash/diff-check 通过。探针与镜像 CI 仍运行，后端
此前成功；新修改未推送或部署，不视为完成真实故障验收。

worker 增加真实会话库集成测试：仅复用 Kubernetes/转发/探针夹具，
启动实际独立入口与 session 库；父进程收到 READY 后先由 LoadCapture
核验基线，再交付原始故障起点，验证 CAPTURED 的产物、调度摘要清单和
完成标记、worker 退出码，并计算合成事件 0→1 的差值。退出后 auth、
原匿名及重建匿名三个转发 PID 均已不存在。最终 worker 三轮 race 通过
（16.885s），vet/diff-check 通过。这不是实际 TLS/Kubernetes/TiKV 故障
实验，控制进程的多 worker 协调与恢复集成仍待完成。

候选 `03d27d3881297984aef0b93e770a46f567a22cf4` 的后端协议 CI
`35430808015` 已成功。run/job API、完整日志和已验证摘要保存于
`backend-ci-35430808015-terminal.lqvidOKf`。探针 `35430808031` 与镜像
`35430807890` 仍运行，不能称三项 CI 全绿；本机后续 owner/worker 改动
不在该候选的验证范围。本轮未推送或部署。

新增独立进程入口 `protected-metrics-worker.sh`：自有转发与退出清理，
完成基线后发送 READY，再从 stdin 接收一个有长度/时间限制的故障起点，
执行一次原截止时间内的定时采集，输出 CAPTURED 并保存退出码。工具
自身必须纳入准入摘要，默认使用独立 18586/18587 端口；未启动真实进程
连接集群。合成库测试覆盖成功、基线/采集失败、清单漏项、EOF、非法/
未来起点、TERM、基线后 HOLD，并检查子进程已回收、失败不发成功消息。
首轮 race 发现测试在 Wait 前读取 stderr 缓冲区，改为仅在进程汇合后
读取；修复后 worker 三轮 race 通过（2.194s），build race 通过（2.611s），
vet、shell 语法及 diff-check 通过。CI 已纳入新入口路径和测试表达式。
这些仅验证 worker 协议，未完成外层多进程故障注入/汇合/恢复编排，也
不是原 30 秒门限通过的证据。候选三项 CI 仍运行，后续改动未推送或部署。

新增 owner 生命周期复查：准备完成后若外层写入 HOLD/终态或移除
deployment-claimed，后续 capture/rearm、定时等待每轮和完成标记写入前
都会拒绝继续。准备后终止时不调用探针；探针期间终止时可能保留原始
文件，但不写 COMPLETE、不重建匿名通道。测试仍检查原有转发子进程
由 owner 的清理路径回收。该复查不是并发恢复锁，外层仍必须先取消、
join 控制器后再恢复资源。
stack/metrics 的准备后/探针中终止，以及调度等待中 HOLD/终态的三轮
race 通过（51.211s），完整受保护会话/调度 race 通过（143.816s），
相关 vet/bash/diff-check 通过。候选 `03d27d38`
三项 CI 仍运行，本次后续改动不在其验证范围，未推送或部署。

后续镜像 CI `35429292124` 成功终态，源码确认仍为
`3f64735dbf29faeb3b226b6b195850680083a1dc`；run/job API、完整日志及
已验证摘要保存在 `image-ci-35429292124-terminal.1P8wCLHL`。同源探针
CI 失败仍有效，该镜像不获实验准入。两项旧任务均已结束后才推送后续修复。

新增单次定时入口 `stack_session_capture_metrics_at`：独立控制器 prepare
后传入原故障起点和小于 30 秒的偏移，等待分段检查同一截止时间。已过
目标偏移但未过期时立即采样，以实际时间为准；拒绝重复安排、更换起点、
未来/过期时钟，失败不重试。调度输入、结果路径和采集清单有独立摘要，
最终预算检查后才产生非验收性质的完成标记。外层并行进程启动/起点交付/
汇合恢复仍待实现，不据此开展真实实验。
首两轮模拟时钟测试因 Bash 动态作用域遮蔽同名 now 变量失败，改用独立
test_clock_ns 后三轮通过（9.505s）。取消测试记录每个 sleep 子进程，
断言退出且未采集。完整会话+调度集成 race 通过（129.961s），含真实
会话脚本到离线加载器的产物核验；build race 通过（2.613s），vet、shell
语法和 diff-check 通过。CI 路径和测试表达式已包含新增调度测试。

为后续独立指标控制器补上端口隔离：受保护会话可显式指定不同的 info/
anonymous 本地端口（1024–65535，无前导零），默认 18584/18585 不变。
prepare 后 capture/rearm 检查端口仍与准备时相同；所有监听检查、日志、
转发和探针 URL 使用该端口对。完整会话 race 通过（118.070s），随后新增
metrics 自定义/同端口/越界/漂移四场景三轮 race 通过（15.866s），相关
vet、shell 语法及 diff-check 通过。首次补丁因函数内已有注释未匹配而未
应用，核对当前文件后重新应用；无部分修改或集群操作。
这只是独立采样进程的前置条件，不宣称已实现并行采样调度。不同控制器
必须自行 prepare/拥有子进程，不能继承已准备 shell 的 job 表；外层仍
须固定故障起点并在恢复前收齐结果。旧镜像 CI 仍在运行，未取消/重发。

CI `35429292114` 已以 failure 结束，失败步骤为 protected capture evidence
and retirement deltas。日志明确为 `/usr/bin/timeout: failed to run command
‘rg’: No such file or directory`。离线校验库先通过，随后采集脚本在输入
绑定检查提前失败；部分预期失败场景因此未执行到目标分支，不能把它们的
PASS 解释为有效覆盖。后续整套探针回归步骤未完成，不算候选 CI 通过。
run/job API、完整日志与已验证摘要保存在私有目录
`probe-ci-35429292114-failed.ryCekcna`，原失败保留。

修复仅在仓库中进行，不登录或维护 Runner 主机：运行时精确行匹配改为
`grep -Fxq`，转发就绪日志用固定字符串 `grep -Fq`；保留 stack_session_run
原截止预算。测试夹具在 exercise 之前检查实际依赖，缺失即退出 127，
避免反例测试把环境错误当作预期业务拒绝。CI 也预检基础命令，工作流契约
测试锁定预检；另以失败 rg 替身验证成功采集不再调用 ripgrep。
build race 通过（2.737s），相关 vet/bash 语法检查通过；完整受保护采集
race 通过（113.678s），包括不依赖 rg、取消清理及哈希截止。
镜像 CI `35429292124` 仍运行，未取消或重发。本地修复不是 CI 成功证据。

补充整包验证（当前本地源码，非旧候选 CI）：server、leader、Prometheus
三个完整包 race 通过，分别 79.653s / 4.187s / 1.095s，不仅是新增测试的
筛选运行。工作流契约测试现已锁定退休差值命令、受保护采集脚本及身份谓词
的 push 路径，以及库/CLI/session 的 uncached race 执行命令；build 包 race
通过（2.720s），vet/diff-check 通过。候选 `3f64735d` 的两项 CI 仍运行，
未取消、重发或部署。本地后续修复仍需要新的同源构建验证。

离线工具增加可选 `--duration`：选定 local/peer 和 outcome 后，同时要求
直方图与结果 counter 对应，输出完成操作的 count/seconds 差值。不输出
总 failover latency，不覆盖 lifecycle join，也不能确定单个事件发生时刻。
累计 sum 的浮点舍入限制仍存在。counter 与 histogram 分两次写入，因此
抓取恰好发生在两次更新之间时可能不一致；校验器拒绝该样本，不自行补齐。

检查固定阶段类型、准入 cluster/outcome、有限非负 sum、整数 count、
完整 sum/count/+Inf 桶、桶累计单调与总数、跨样本桶边界一致及无回退。
零 count 不允许非零 sum，零增量不允许新增 sum。Prometheus 文本解析器
会合并直方图组件，为避免漏检重复 sum/count，另去除目标 TYPE 后按独立
untyped 组件解析、逐项查重；数值等价的重复桶边界同样拒绝。

真实产品适配器 `/metrics` 导出联测涵盖空 histogram 和一次 0.25 秒
观测；CLI 入口子进程涵盖三个观测增加 0.75 秒及缺失 histogram 时不输出
成功 JSON。库/CLI/探针三轮 race 分别通过（4.812s/8.315s/12.553s），
相关 vet/diff-check 通过。未部署，旧候选两个 CI 仍运行；新增代码不在其
验证范围，完整故障实验仍待新源码 CI、镜像核验及新准入。

启动顺序补验：在真实 `NewServerWithPeerRetirement` 构造测试的后台
PrevalidateLeadershipRevision 入口，复制指标注册快照，再进入原阻塞
夹具；核对八个零 counter 与八个各一次的 histogram 注册均已完成。
不只在构造函数返回后检查，避免漏掉后台线程先于初始化开始的回归。
不安全 TLS 的五类构造失败路径改用严格 metrics mock，禁止任何注册。
首次测试包装器只嵌入 Backend 基础接口，遗漏可选的预校验方法，导致
编译失败；改为嵌入原具体测试后端后重新验证，未把失败记为通过。
构造顺序/初始化/TLS 拒绝三项三轮 race 及 vet 已通过。本轮只改测试，
未部署；候选两个 CI 仍在既有 run 中进行，不重发构建。

`cmd/option/option.go` 实际以 `cluster=ClusterName` 构造 Prometheus
适配器。此前仅允许 outcome 标签的解析器会拒绝真实产品序列；已有合成
测试未覆盖该差异。因此候选 `3f64735d` 即使 CI 全绿，也不能作为真实
退休指标诊断工具准入，需包含零基线及本次 cluster 绑定修复的新版本。

新增 `ParseForCluster`，生产采集加载器和 CLI 必须接收独立准入的 cluster，
仅允许匹配的 cluster/outcome 两个标签。错误或缺失 cluster、额外标签、
重复标签均拒绝；空 cluster 的旧 Parse 只用于不带全局标签的单元夹具。
cluster 同时进入 Sample 进程身份比较，不能跨 cluster 做差值。

新增独立子进程测试使用产品 Prometheus 适配器真实 `/metrics` HTTP 输出，
验证八个零序列与一次 peer confirmed 后的 0→1 差值，而非手写导出文本。
该测试验证导出/解析联通，不声称启动了真实服务器或产生真实退休事件。
最终库/CLI 三轮 race 通过（4.507s/5.064s），会话产物加载成功路径 race
通过（4.700s），相关 vet/diff-check 通过；此前完整探针三轮 race 通过
（12.468s）。旧候选两个 CI 仍运行，分别在 etcd service/Watch 检查及
镜像构建；本次修复未推送、未部署，不取消或重复触发原构建。

后续补充 CLI 真正入口的子进程测试：完整的合成采集包前后计数 2→5，
核对退出码 0 和唯一成功 JSON（delta=3，三项 readiness/latency/acceptance
结论仍为 false）；基线缺失、回退、进程变化、倒序、重叠、篡改、不完整、
错误准入均核对退出码 1 且 stdout 为空。三轮 race 通过（4.885s），vet
通过。这覆盖命令入口，不把合成产物称为真实集群证据。

另修复首次退休事件前无法取得计数基线的问题：显式 opt-in 的
`NewServerWithPeerRetirement` 在全部配置验证后、启动 campaign 前注册
local/peer 共八个有限 outcome 的零值 counter；可选 HistogramRegistrar
仅注册空 histogram，绝不 Observe(0)。重复初始化仅 Add(0)，不重置已有
计数或发明事件。普通 NewServer 路径不变，指标注册错误不改变退休协议。
离线工具仍拒绝缺失序列：旧镜像或采集缺项不能追溯补零。

本地 etcd 对照：`/root/etcd/server/etcdserver/metrics.go` 的 init 注册
leaderChanges 等 collector；这里采用启动时可观察的基线原则，但退休
指标是 KubeBrain 自有协议诊断，不等于 etcd Raft 指标语义。
PeerRetirement/退休服务器构造 race 通过（59.389s），Prometheus 包三轮
race 通过（1.161s），相关 vet 通过。当前运行的候选 `3f64735d` CI
`35429292114` / `35429292124` 不包含这些后续改动；暂不推送以免取消
正在运行的任务。未部署，仍须新源码 CI 与镜像验证后才可用于实验。

新增 `retirementmetrics.LoadCapture` 与
`hack/production/cmd/retirement-metrics-delta`（使用方式和限制见其 README）。
加载器核对 COMPLETE、八个固定文件、原绝对路径清单及哈希；使用独立准入
传入的 namespace/STS/Pod UID 和规范化 spec 摘要检查对象，并比较采集前后
完整 Pod spec、地址、容器进程及镜像身份。允许 readiness 改变，不允许重启。
逐阶段记录必须按序成功，探针摘要时间必须位于 protected-probe 阶段内；
跨样本还要求身份一致、区间不重叠。命令只输出单个指定 outcome 的计数差值，
不计算故障门限、直方图延迟或 successor readiness。

首轮和随后三轮回归均暴露静态符号链接未被拒绝：仅向 os.Root.OpenFile
传入 O_NOFOLLOW 不足以实现所需契约。已增加 Root.Lstat 普通文件检查和
打开后 SameFile 比对；此前失败不计为通过。修复后校验库/新命令/真实探针
三轮 race 通过（分别 1.201s / 1.034s / 12.379s），vet 通过。
会话测试增加实际脚本产物到加载器的成功路径；完整受保护会话组（包含外部
取消、重建取消及哈希截止）race 通过（110.668s），工作流 race 测试通过
（2.616s）。新命令的 CLI 目前覆盖失败参数，成功加载由库与会话集成测试
覆盖，不宣称 CLI 已做真实实验端到端验证。

探针 CI 已加入对应路径触发和显式回归步骤。尚未执行新的集群故障实验；
完整文件包的哈希一致性不等于来源认证，原始准入、工具/镜像来源与故障时钟
仍须由实验驱动核验。旧指标首次出现仍为未知差值，不得补零或判通过。

新增离线校验库 `hack/production/internal/retirementmetrics`：仅解释
local/peer result counter，校验类型、唯一 outcome 标签、阶段对应的
有限 outcome 集、有限非负整数及 float64 精确计数范围。拒绝重复序列
和显式样本时间戳。缺失序列保持缺失，首次出现不能假定零基线。
差值接口要求前后非空且一致的 Pod UID/container ID/startedAt/restartCount，
拒绝计数回退。身份须由采集器的 Kubernetes 核验结果提供，而非指标标签。
三轮 race 和 vet 通过。该库尚未接入真实采样驱动，未校验 histogram、
采样顺序和文件证据绑定；不改变原始探针的 `metric_semantics_proven=false`，
不把局部计数差值当作切换成功或完整延迟解释。

后续补充 `NewSample` / `SampleDelta`：绑定探针真实 JSON 摘要的
字节数、SHA-256、显式能力标志和开始/结束时间；拒绝缺项、重复字段、
大小写别名、多个 JSON 文档、超过 25 秒或倒序的采集区间。样本内部
不暴露可修改的计数 map；前后区间必须严格不重叠。TLS 1.2/1.3 测试
分别执行两次真实探针采集，将实际文件和摘要送入该接口，核验零差值。
进程身份仍为测试夹具，不能称为真实 Kubernetes 验证。完整三轮 race
和 vet 通过后提交；尚未实现 COMPLETE/manifest/Pod 文件的离线加载器，
因此不能直接以这些 API 作为真实故障采样准入。哈希只约束文件配对，
不是来源认证；样本间计数变化也不提供事件精确时间或耗时。

新增故障模式测试验证 metrics 响应体传输中取消/截止；明确断言已经
进入响应体阶段，并检查对应 context 错误，避免把 TLS 阶段失败当作
响应取消覆盖。另验证时序输出失败、目标目录不存在、最终摘要输出
失败。最终探针包三轮 race 通过（12.556s），vet/diff-check 通过。

重要契约：最终摘要写出失败时，已完成的原始文件可能保留，作为
不完整采集证据，不能仅凭文件存在判定成功。调用方须同时核对成功
退出、有效摘要、字节数及哈希，再独立完成进程身份和指标语义验证。
取消或传输失败不保存响应体，未生成成功摘要；没有重试或覆盖旧文件。

此次仅新增本机回归测试，未改生产行为或集群。CI 复查：原候选探针
任务正在 endpoint transport/credential lifecycle 检查，镜像仍在构建
推送；后端已成功，另外两项尚无终态。不重复触发正在运行的任务。
