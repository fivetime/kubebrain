# 原 30 秒故障门限的接管观测

旧现场 owner `local-term-framed-3e6f3b15.wxeup3vz` 的原 KeepAlive 请求
在门限内未返回；旧脚本只保留最后一次 new-status.json，未完整保存
fault_start_ns 和 leader 采样历史。该 owner 已结束，不修改或重跑。

`hack/production/observe-successor.sh` 为下一轮实验提供独立观测组件。
接口为七个显式参数：新输出目录、故障开始 UTC 纳秒时间、cluster ID、
观测成员 ID、旧 leader ID、旧 term、绝对路径的请求回调。
回调接收 `BUDGET_SECONDS OUTPUT_JSON`，必须以已有 mTLS 身份对固定成员
执行一次 Status 请求，不重试、不改路由；输出原响应到指定文件。

组件不会注入/解除故障、创建租约或启动 KeepAlive 流。输出目录必须新建，
禁止覆盖。每个 sample-N 保存原 JSON、stdout/stderr、请求退出码、起止
UTC 纳秒和请求预算；输入、截止时间、回调文件摘要另存。纳秒和 uint64
身份作为十进制字符串保存，term 比较使用长度与字典序，不经浮点转换。

固定截止时间为传入 fault_start_ns 加 30 秒；每次回调最多五秒且不超过
剩余时间，采样间隔最多 200ms。回调超时可有最多一秒终止宽限，但任何
超出原截止时间的响应或解析决定都不接受。过期门限不发请求，未来开始
时间或观测到时间倒退则退出。它沿用原 UTC 墙钟门限，不声称能检测所有
时钟跳变；现场仍须记录时钟来源/同步状态。

接管判据要求：唯一 JSON 对象、响应文件不超过 64KiB、无 RPC error、
精确匹配 cluster 和观测成员、新 leader 非零且不同于旧 leader，term
严格增加。拒绝数值型/超出 uint64 的身份，禁止接受最后一个 JSON 覆盖
前面的异常数据。成功时仅记录 successor-sample，不覆盖任何采样。

测试使用可控回调，覆盖失败部分响应→旧 leader→新 leader 的完整历史、
大于 2^53 的 term、错误身份/term、多 JSON、迟到响应、已过期/未来时钟及
输出目录重用。它们不证明 Kubernetes、TLS、Cilium 或真实 TiKV 接管。

**尚未接入真实故障驱动**。下一轮必须在安装策略前持久化原开始时间，
将同一时间传给观测器，并在策略恢复和原始 KeepAlive 响应校验中继续
使用同一截止时间；不得把“观测到新 leader”当作整个实验成功。仍须
保持原单条流、确认服务端等待、绑定 Pod/容器与实际网络丢包、验证响应
term/租约结果，并有独立恢复和清理路径。新镜像需要新的源码栈绑定。

本轮定向 race 三轮通过（4.532s），相关 Go 包 vet、Bash 语法及 diff
检查通过。没有修改已结束的旧实验，也没有执行新的现场故障。

## 真实 mTLS Status 请求适配器与只读验证

`successor-status-request.sh` 实现上述回调，使用环境变量
`KB_SUCCESSOR_STATUS_CONFIG` 和 `KB_SUCCESSOR_STATUS_CONFIG_SHA256` 绑定
已审核配置。配置恰含 server_name、port、ca、cert、key 五项，后三项仅为
绝对文件路径，不嵌入私钥；复制到每个样本目录并校验哈希后才发请求。
端口范围 1–65535，连接始终通过 loopback port-forward，证书仍按原
server_name/CA 校验。脚本不建立隧道，隧道生命周期由故障驱动管理。

curl 首参数 --disable 禁用用户 .curlrc；禁止环境代理、重定向和重试，
请求及连接超时均为传入的剩余预算，响应上限 64KiB，仅 HTTP 200 成功。
保留 HTTP 状态及原始响应，错误不转换成空成功对象。回调配置或请求
路径变化须生成新的输入记录，不能复用旧 claim。

2026-09-18，在原固定镜像实例的 kubebrain-local-0 上完成真实只读 smoke：
owner `/root/.local/state/kubebrain/successor-status-smoke.4z33QWbw`，执行
41217 终态 0。一次基线请求之后，用实际 cluster/member/leader/term
开启独立 30 秒观测；未安装策略、未写业务数据、未创建租约或 KeepAlive。
保存 86 次成功状态响应，leader 均未变化，观测器按期退出 1，未生成
successor-sample。这是预期的**无接管负例**，不是故障验收成功。
Pod UID/spec/容器状态前后相同，端口 18990 的转发已退出；原响应、时间、
配置路径、脚本与 SHA-256 证据保存在 owner，凭据内容未输出或入仓库。

适配器与观测器定向 race 通过（2.506s），vet/语法/diff 检查通过。
另外核对已审计候选 02786d91 的 lease.go，过期等待 select 位于 1645 行，
而旧 3e6f3b15 绑定的是 1637；下一轮须先验证实际运行栈与精确源码，不能
仅更换源码 SHA 后复用旧断言。完整故障驱动接入和原请求验收仍未完成。

## 当前候选的等待栈绑定

`hack/production/expired-lease-wait-frames.jq` 新增独立于已退休 owner 的
分类器，仅批准产品源码 `02786d91ff406fe50d72e9baebdf2963b74b702c`，
对应冻结 lease.go 的 SHA-256 为
`3c98f802359a5f185dc6e618691ad6098641a54afa528668c6dfcaf8091ccd88`，
等待 select 位于 1645 行。调用时必须传入已核验的镜像源码和冻结源文件
摘要；其他 SHA 或摘要失败，不将旧 1637 行或其他函数误认为该等待点。

分类器只报告顶层处于 select 的 refreshLeaseHoldingLocks 候选及其
goroutine ID，不自行宣称对应某个 lease/RPC。多个候选完整保留，驱动
必须拒绝含糊关联。缺少尾换行、非栈内容及达到捕获上限的输入失败；
完整-looking 的截断无法仅靠文本识别，调用方仍须证明 HTTP 成功、响应
未达到字节上限、Pod/容器身份不变，并绑定唯一原请求与故障时序。

分类器及采样相关 race 三轮通过（5.577s），覆盖精确匹配、等待时长、
错误源码/文件摘要、旧行号/行号前缀、running 状态、相似函数名、深层
调用帧、明显截断/HTML 和两个候选；vet 与 diff 检查通过。

现场准备仍缺受保护的诊断接入：原基线含 --enable-pprof=false，普通
HTTPS 健康探针不带客户端证书。旧实验的 diagnostic-patch.jq 同时启用
info mTLS 并把健康探针换为带证书的 exec curl，但绑定旧源码和旧 spec，
不能直接执行。新故障驱动必须将诊断配置纳入独立审核/并发前置条件及
恢复路径，验证未认证客户端无法读取 pprof，然后才能采集等待栈。

## 诊断配置规划与 API dry-run

新增 `deploy/test-cluster/diagnostic-plan.jq`，输入为审核过的 baseline、
current、namespace UID、info DNS 及 enable/restore。它不修改镜像；
可在原基线或已启用 peer 协议的 spec 上生成一个独立诊断阶段。只改变
pprof/info mTLS 参数和三个健康探针，其余完整 spec 必须保留。

原配置必须关闭 pprof、有唯一受审核的 info TLS 参数、只读证书挂载和
预期 HTTPS 健康探针；拒绝已有不明诊断参数、明文配置、重复开关、
不支持的 rollout 或 spec 漂移。启用时同时强制 info 客户端证书认证和
trusted CA，把三个健康探针改为直接 argv 的 mTLS curl；禁用 .curlrc、
代理和重试，保留探针周期/阈值及原 timeoutSeconds，内部超时为 1/1/5 秒。

启用要求已观测的三个 Ready/current 副本；恢复即使未 Ready 也可执行，
但只接受 baseline 或精确推导的 enabled spec。补丁含 UID、resourceVersion
及完整 spec 并发前置条件，重复规划返回空补丁，不将空补丁当作运行验证。

针对性 race 通过（1.711s），覆盖原/协议布局、只修改许可字段、未就绪
回退、漂移、重复/不安全参数和并发冲突。随后在专用集群仅做 server
dry-run：owner `/root/.local/state/kubebrain/diagnostic-plan-check.CSDuZfgC`，
执行 31223 终态 0。API 返回候选 generation 57，规划逆向补丁也成功；
实际前后仍 generation 56、完整 spec 相同、3/3 Ready，没有开放 pprof。

这仅证明规划约束及 API 接收，不证明 curl 在容器中的证书可用性或实际
pprof 鉴权。下一步必须把诊断阶段纳入完整故障驱动的恢复链，先验证
无客户端证书被拒绝、合法身份可捕获完整栈，再进入真实故障门限。

完整 `go test -count=1 ./deploy/test-cluster` 通过（75.939s），vet 和 diff
检查通过。所有验证均未持久化诊断配置。

## 实际 info listener 的 mTLS 回归

新增 `TestInfoDiagnosticListenerRequiresMTLSAndExplicitPprof`，运行真实
`runMetricsServer`、root listener 和安全传输封装，而非仅调用 HTTP mux。
分别覆盖 pprof 开关：合法 CA 客户端在开启时获得有界 debug=2 栈响应，
关闭时为 404；无证书、不可信 CA 证书、错误服务端 DNS 和明文连接都
不能得到 HTTP profile 响应，之后合法客户端仍可访问。

首轮失败来自测试夹具复用证书目录：辅助函数固定写 tls.crt/tls.key，
客户端材料覆盖了动态读取的服务端材料。改为每个身份独立目录后，三轮
race 通过（1.599s）。没有据此修改产品 TLS 策略或放宽验证。
此测试使用真实 TLS socket 但无 TiKV 后端，不代替集群健康探针、证书
挂载或生产诊断权限验收；现场捕获及故障驱动仍需接入。

## 诊断阶段的受保护执行与回退

`run-peer-trust-expand.sh` 新增 `phase=diagnostics`，沿用一次性执行目录、
UID/resourceVersion/完整 spec 前置条件、API dry-run、强制前后验证回调
和失败后的独立相邻恢复。执行器的 expand 映射到诊断规划的 enable；
恢复只关闭本阶段新增的诊断配置，不改变进入阶段时的镜像或 peer 信任。
该阶段不读取 peer Secret。实际 Pod 校验同时比较三个健康探针，避免
仅参数匹配却仍使用未认证 HTTP 探针。

九种诊断场景覆盖成功、前置失败、已启用状态恢复、并发冲突、API 响应
丢失、配置漂移、rollout 失败/超时及后置验证失败。针对性 race 通过
（7.373s），完整部署工具包 race 通过（89.342s）。测试使用模拟集群；
实际诊断验证回调仍需实现，不能把现有 peer 验证器直接用作诊断验证器。
没有据此启用现场 pprof 或开展故障注入。

## 有界 info 诊断探针及原基线只读烟测

`hack/production/cmd/info-diagnostic-probe` 提供 protected/disabled 两种模式。
先验证带证书的健康接口；protected 模式确认无证书请求被 TLS 拒绝后才
采集有界栈，disabled 模式要求两种访问方式均返回 profile 404。固定使用
HTTPS loopback、正常 CA/DNS 校验及审核过的服务端 SPKI；无代理、重试、
重定向或压缩。栈最多 8 MiB、exclusive-create 0600；输出明确不证明 Pod
身份、栈对应哪条 RPC 或故障验收。

首轮真实 TLS 测试发现 TLS 1.2 无证书拒绝返回 handshake_failure，而非
TLS 1.3 的 certificate_required。探针现在要求已经验证服务端、观察到
服务端请求客户端证书、返回空证书后收到远端 TLS alert；不把普通连接
失败或 HTTP 错误文本视为鉴权证据。真实 CA 双向验证的 TLS 1.2/1.3、
禁用路由、未开启认证、伪造 alert 文本、重定向、超限、截断、错误身份、
取消和禁止覆盖文件测试通过三轮 race（3.635s）；CI 工作流契约三轮通过
（5.618s），vet/diff 通过。新增 CI 路径触发和独立回归步骤。

提交 `ca922277` 的只读现场烟测 owner：
`/root/.local/state/kubebrain/info-diagnostic-smoke.eYOZbv7d`。
一次性 run.sh 终态 0（45899），通过 namespace/StatefulSet/Pod 身份检查后
将 kubebrain-local-0 的 info 端口转发至 18584，正常服务端 TLS+SPKI 验证、
健康检查及两种访问方式的 pprof 404 全部通过。前后完整 StatefulSet spec、
generation 56、Pod UID/spec/containerStatuses 不变，3/3 Ready。

端口转发已退出，18584 无监听；完整证据摘要已验证，唯一编译出的临时
probe 二进制已删除，源码及哈希保留，可按相同源码重建。未复制私钥到
仓库，未启用 pprof，未改动集群资源或注入故障。此烟测只证明原基线的
disabled 模式；下一步仍须完成诊断阶段验证回调、开启后的逐 Pod 运行
验证及恢复链，然后才能运行原始 30 秒故障门限。

## 诊断阶段逐 Pod 验证回调

`deploy/test-cluster/verify-diagnostic-stage.sh` 接入守护执行器的五参数
MODE/PHASE/EVIDENCE/KUBECONFIG/CONTEXT 契约。诊断 receipt 的 verification
包含 diagnostic_probe、diagnostic_probe_sha256、info_server_cert、
info_server_cert_sha256、client_tls_dir（ca.crt/probe.crt/probe.key）；info_dns
仍位于 receipt 顶层。探针和服务端证书必须是审核过的普通文件，复制到
私有证据目录后核对哈希；验证证书用途、名称、密钥匹配及一小时有效期。
恢复预检不要求当前服务健康，避免将故障状态作为拒绝回退的理由。

after 阶段核对 namespace/StatefulSet/三个 Pod 与执行器快照；开启时运行
Pod 实际配置的 liveness/readiness exec curl，验证挂载的健康证书可用，
再做 TLS 身份测试和有界栈采集。关闭时检查两种访问方式的 profile 404。
每个成员前后 Pod UID、spec、containerStatuses 必须一致；外层执行器
还会复核整个阶段的最终完整 spec 和三个 Pod 状态。回调不读 peer Secret、
不创建或删除集群资源，也不把诊断通过当作故障验收。

protected 模式现在强制独立 anonymous-endpoint：负向 TLS alert 可能
终止 kubectl 的整个转发进程，因此合法身份和匿名检查各使用一个绑定
同一 Pod 的转发进程（18584/18585），每个成员结束或失败时均清理。这
不是重新连接同一条 KeepAlive，也不构成重试；KeepAlive 连续性仍由原
故障探针验证。

针对性模拟测试通过（6.313s）：预检、错误哈希/DNS/密钥、开启/恢复、
探针失败、实际健康命令失败及 Pod 被替换。首轮失败是 mock 转发命令
将端口取为第五参数而非第四参数，已修复夹具并确认失败场景到达相应
检查点；没有因此放宽运行条件。真实 TLS 探针更新后 race 通过（1.948s）。

随后完整 race：部署工具 93.495s、诊断探针 1.952s、工作流契约 2.568s，
全部通过，vet/bash -n/diff 检查通过。

冻结提交 `d60cc418` 做三个成员的 restore 模式只读烟测，owner 为
`/root/.local/state/kubebrain/diagnostic-verifier-smoke.PYqiLb34`；一次性
run.sh 63277 终态 0。真实预检和三成员验证均通过：各成员健康、带证书
及匿名 profile 请求都为 404，前后 Pod UID/spec/containerStatuses 不变，
StatefulSet 仍 generation 56、原完整 spec、3/3 Ready。这里的 restore
只是验证回调的模式，没有执行恢复补丁或 rollout。

证据摘要验证成功；两个本地端口均无残留监听，三个临时探针二进制
（主副本、预检副本、运行验证副本）已删除并保留哈希。私有源码、日志、
证书证据和 receipt 保留。该结果仍不覆盖 protected 模式的实际 rollout
与采栈，更不是原 30 秒故障验收；最新镜像 CI 35349468415 仍运行时未
推送新提交，避免取消它。

## 新提交 CI 与完整执行器只读接入

旧镜像作业 35349468415 随后成功，但同源码 `7fa4a6aa` 的探针作业
35349468426 保留失败，不能将镜像构建成功视为该提交所有门禁通过。
旧作业终态确认后，已推送 `da303fdbd460ee42f3aa158fac27c396faf6b58f`，
由 push 自动触发（没有额外 workflow_dispatch）：

- 镜像构建 35352626418。
- 探针回归 35352626497。
- 后端协议集成 35352626422。

三项均已观测到 in_progress，尚未据此认定通过。新提交包含 Watch
回放进度修复，需要自己的后端和探针结果，不能套用旧提交的成功记录。

同时在 owner
`/root/.local/state/kubebrain/diagnostic-driver-noop.cc35oAeZ` 冻结相同源码，
通过完整 `run-peer-trust-expand.sh` 的 diagnostics/restore 路径做现场
空操作接入验证。专用 kubectl 包装器只允许 get、rollout status、
port-forward，拒绝所有写命令；因此即使意外产生补丁也无法写入集群。
一次性脚本 90453 终态 0，before/admitted/after/final 的 patch 均为空，
没有 patch-result 或 recovery；真实证书预检、三个成员的 disabled
诊断验证、最终完整 spec 与 Pod 状态比较全部通过。

现场仍 generation 56、原完整 spec、3/3 Ready；证据摘要通过校验，
18584/18585 均无监听。三个临时二进制副本已删除并保留哈希，源码与
私有证据保留。这里只验证完整执行器的只读空操作路径，尚未验证实际
诊断启用、滚动更新和恢复，更未证明原 30 秒故障门限。

## 新候选源码的等待栈绑定

已比较 `02786d91` 与正在构建的 `da303fdbd460ee42f3aa158fac27c396faf6b58f`
的 `pkg/server/etcd/lease.go`：无差异，完整 SHA-256 仍为
`3c98f802359a5f185dc6e618691ad6098641a54afa528668c6dfcaf8091ccd88`，
`refreshLeaseHoldingLocks` 的过期等待 select 仍在第 1645 行。因此分类器
新增该精确源码 SHA 的绑定，不采用“最新提交”或仅匹配函数名的放宽规则。
未知源码、错误文件哈希及旧行号继续拒绝。

新增测试直接读取当前 checkout 的 lease.go，断言完整文件哈希以及
select/首个 case 的确切位置，避免仅靠合成栈样例掩盖产品源码漂移。
分类器针对性三轮 race 通过（1.284s）。源码绑定只是栈解释依据，不证明
镜像内容、CI 或实际 Pod；现场启用实验仍需独立镜像审核和门禁结果。

同时核对 `/root/etcd` 参考仍为
`5cd9f4ee13801e18825d661e5005ae599460bc3a`：其过期续租等待分支监听
revokec、demotec 和 stopC，KeepAlive 在 Renew 前构造 revision header。
本轮未修改租约产品逻辑，也未用源码比较替代停机、原始 stream 连续性
或真实 TiKV 故障验证。三项 da303fdb CI 仍在运行，未重复触发或取消。

后续终态：`da303fdbd460ee42f3aa158fac27c396faf6b58f` 的后端协议集成
35352626422 已成功。已核对步骤而非只看总状态：后端 race、夹具契约、
固定 PD/TiKV 镜像拉取、真实协议普通/race 测试及中断启动清理均成功。
这证明该作业覆盖的本地容器协议测试，不证明专用集群故障门限。记录时
探针 35352626497 和镜像 35352626418 仍在运行；本地完整
`go test -race -count=1 ./hack/production` 的会话 90010 也仍未终结。

会话 90010 后续得到 Go 失败终态：整包达到默认 10m 总时限，输出
`FAIL .../hack/production 600.054s`。当时运行的
`TestRolloutAvailabilityRunnerDeletesOwnedProbeWhenCreateResponseDrifts`
只开始了约 1 秒，因此该栈不能直接证明这个测试卡住。原 shell 后面继续
执行 vet/diff，最终 shell 退出 0；这里按 Go 的明确 FAIL 记录为失败，
不能因外层 0 改记为通过。超时输出已单独保留。

该测试随后独立 race 三轮通过（7.923s）。完整包改用显式 `-timeout=30m`
重新运行并保存逐测试 JSON、stderr、独立 Go 退出码；仅在原进程已经
终结后开始，会话为 8608，owner：
`/root/.local/state/kubebrain/production-full-regression.Zna8XATn`。
这调整的是本地整包测试时限，不是原始 30 秒集群故障验收门限，也没有
改动 CI 或将失败记录抹掉。尚须等待该整包测试的真实终态。

同一候选的探针 CI 35352626497 已确认完成并通过
`Verify etcd service and Watch regressions` 步骤，即覆盖上次失败的 Watch
回归；这仍只是该步骤的结论，不代表整个探针作业或镜像构建成功。

随后探针作业 35352626497 已确认 completed/success，head SHA 精确为
`da303fdbd460ee42f3aa158fac27c396faf6b58f`；持续观察会话 79557 终态 0。
因此该候选的探针和后端集成 CI 均通过，镜像作业 35352626418 仍未终结。
独立镜像审核准备在
`/root/.local/state/kubebrain/release-da303fdb.OAubJCfA`，尚未执行、没有
claimed/verified.json。审核脚本要求三项同源码 CI 全部成功；其精确容器
ID/归属标签清理保护已用模拟 Docker 验证，不涉及实际容器删除。
本地完整工具回归 8608 仍需单独收集终态，不能被这两项 CI 代替。

8608 后续也达到明确的 30m 整包总时限，独立退出码 1，JSON 记录
1800.056s aggregate fail；保留失败，不改写为通过。752 个顶层测试中
已有 712 个完成并通过，没有 named-test fail 事件，但整个调用仍失败。
本次超时发生在 `TestValidateDataplaneReadonlyProbe` 的子测试期间。

已生成完整测试清单 test-list.txt 和差集 partition.json，准确列出剩余
40 个顶层测试，包含被中断的整个父测试，不按已通过子测试跳过它。
初始源码 572b8e42 到分组源码 e7763cd1 的非 docs 内容无差异。追加两组
race/count=1/15m 验证：68407 跑该父测试，52684 跑其余 39 个；各有独立
JSON、stderr、退出码。尚需等两组终态并验证完整清单覆盖；即使分组全部
通过，也只能报告分组覆盖完成，不能把原 30m 整跑改记为成功。这不改变
任何集群故障门限或断言。

两组追加验证现均成功：68407 为 298.433s，52684 为 438.368s，独立退出码
均为 0。coverage.json 核对原先完成项与两组完成项的并集，752/752 个
顶层测试恰好覆盖，missing/extra/named_failures 均为空。证据摘要校验
通过。记录为完整清单的分组覆盖，不改写两个整跑超时的历史结果。

## 原镜像上的真实诊断启用与恢复

owner `/root/.local/state/kubebrain/diagnostic-live.ggqlxESW`，工具冻结自
已通过 CI 的 da303fdb。execute.sh 28660、enable、restore 均终态 0。
原固定镜像未改变，generation 56 → 57（诊断开启）→ 58（完整原 spec
恢复）。这不是候选镜像升级或原始 30 秒故障实验。

预检曾发现本地旧 client.crt 仅含 serverAuth，不能直接作为现场健康
身份的依据；实际挂载证书是另一个双用途叶证书，SHA-256 为
`3c5466cf76e9962cf20c5d78cf9d6e721a142b24c4853beb5d9c6e8976d6626d`。
变更前直接只读取得三个 Pod 的公有叶证书和 info CA，逐个验证客户端
用途、CA 一致性和有效期超过一小时；没有改证书或导出 Pod 私钥。

开启后三个成员均通过实际挂载证书的 liveness/readiness curl、合法
info TLS 身份访问、独立匿名隧道的 TLS 拒绝及有界 debug=2 采栈，栈大小
分别为 1,720,761、1,915,532、1,937,975 字节。随后立即恢复；三个成员
两种访问方式均收到 pprof 404，最终原完整 spec、3/3 Ready。其他已有
Pod 的 UID/spec/containerStatuses 保持不变，没有注入故障、创建租约/
alarm 或发送 retirement CAS；这些栈不作为候选源码的续租等待证明。

cleanup.sh 66499 终态 0，只回收 enable 阶段新建且已 Released 的六个
临时 scratch PV，逐卷核对创建记录、claim/Pod UID、当前无引用，以及
UID/resourceVersion/spec/status 前置条件。原有所有 PV 的 UID/spec
保留，包括本轮替换下来的六个原始 scratch PV。最终 12 Bound、32
Released。回收卷内容不可恢复，验证日志和栈证据保留。

证据摘要校验通过，五份临时探针二进制已删除并留存哈希；18584/18585
无残留监听。未进入自动恢复分支。后续候选故障实验必须重新 admission，
以 generation 58 和新的 PV 保护集合为基线，不得重用本次已消费目录。

## 故障期间采栈不能依赖业务就绪

重新阅读旧故障驱动后发现，准备复用的 `info-diagnostic-probe` 默认
`protected` 模式同时要求 `/ping` 和 `/ready` 返回 200。被故意隔离
PD/TiKV 的成员可能不就绪；此时要求 `/ready` 会在获取降主后栈之前
终止观察。旧私有采栈脚本只要求 `/ping`，不能无条件替换成新探针的
部署验证模式。这是观察工具接入约束，不是已证明的产品故障根因。

新增显式 `protected-stack` 模式：保留服务端 CA/DNS/SPKI、客户端身份、
独立匿名隧道的真实 TLS 拒绝、`/ping` 和有界 debug=2 采栈；不调用
`/ready`，成功结果明确 `readiness_checked=false`。现有 protected 和
disabled 模式不变，部署验证脚本仍固定使用它们，不能以故障采栈成功
代替部署就绪。调用方还须把采栈限制在原故障门限的剩余时间内。

真实 TLS socket 测试模拟 `/ready`=503：两种部署模式拒绝，故障采栈
模式成功且没有发送 readiness 请求。测试先在修改前失败（新模式尚
不支持），修改后整个探针包 race/count=3 通过（5.321s），vet 通过。
两种采栈模式均覆盖缺少客户端认证、伪造 TLS 错误文本、重定向、超限、
截断、错误 pin/DNS 和取消等拒绝路径。本轮没有更换集群镜像、注入
网络故障或改变验收门限；新故障驱动的完整接入与实际验收尚未完成。

部署侧 `TestDiagnosticStageVerifier` 和 `TestDiagnosticPlan*` 的定向
race/count=1 回归也通过（6.995s）；没有修改部署验证器的模式选择。

## 新候选故障驱动接入进度（尚未现场执行）

新独立 owner 为 `/root/.local/state/kubebrain/fault-da303fdb.txNW9FbJ`，
保留 HOLD，未创建执行 claim。只读现场快照确认原 StatefulSet 仍为
generation/observedGeneration 58、3 Ready。旧实验目录保持不变，未
复制旧 claim、已消费 receipt 或生成的证据摘要。

驱动绑定 da303fdb 及其三项成功 CI，接入新源码栈分类器和完整历史
Status 观察器。故障开始时间在 policy 安装前落盘；注入、策略观察、
丢包采集、降主后采栈、撤销策略各步骤仅可使用原 30 秒的剩余预算，
超时或迟到完成均失败。恢复清理仍使用独立预算，不能改变验收结果。

新 owner 中两轮主控制器离线测试均终态 0（98509、68788），每轮
11 个场景，包括成功、日志前缀、非法采集流、缺失等待栈/丢包、注入
失败、响应任期过旧、准备失败、清理失败。成功场景实际运行历史
观察器，保存失败的部分 JSON、旧任期及新任期三次结果。第二轮增加
步骤剩余预算。独立预算测试 77997 终态 0，使用真实 timeout 验证
及时完成、仅剩约 1 秒时终止慢步骤、已过期和未来起点拒绝。证据
摘要复核通过，全部脚本语法检查通过。

这些是离线调度证据，**不是现场故障通过**。主控制器测试模拟了
采栈/集群操作；新的 protected-stack 采栈适配器还需单独覆盖进程
变化和隧道清理等场景。完整受控升级/逆序恢复、PV 保护、admission
及 policy create 返回不确定时的恢复审核也未结束。因此 HOLD 未移除，
本轮没有部署候选镜像、修改网络、alarm、租约或测试卷。

### 采栈与策略响应丢失恢复的追加验证

同一新 owner 的采栈适配器 14 个离线场景通过（22667，终态 0），
覆盖匿名隧道退出、探针失败/部分输出、错误模式、Pod UID/spec/IP/
容器变化、命名空间/STS/镜像不符、端口占用和第二隧道启动失败。
失败场景不签发有效证据摘要，所有已启动隧道子进程均回收。

进一步审核确认旧策略流程存在恢复缺口：创建实际隔离策略时，若
服务端已成功但客户端没有收到响应，清理缺少 UID 和摘要。新流程
先创建不匹配任何 Pod 的独立 reserved selector 策略，确认真实 UID
并保存恢复摘要；原计时窗口内再通过 UID/resourceVersion/完整 spec
前置条件将它激活。即使激活响应丢失，也可从预存 UID 读取当前对象，
仅接受精确的未激活或已激活配置，然后按当前 RV 条件删除。

reserved selector 不会被赋给目标 Pod；计时仍从首次可能造成隔离的
激活操作之前开始，没有缩短租约时长或放宽 30 秒通过条件。若未激活
策略的创建本身返回不确定，准备阶段停止、不进入故障/租约/alarm
阶段，清理明确失败而不猜测 UID；可能迟到创建的对象仍不匹配目标。
这种情况不算资源已清干净，仍需后续核对。

纯恢复计划 10 个场景通过；实际 reserve/activate/remove shell 控制器
在模拟 API 边界下的 6 个场景通过（92136），包括激活响应丢失、patch
冲突、对象被替换、配置漂移和 reserve 响应丢失。主驱动扩充至 14 个
场景也通过（79765）。全部证据摘要复核通过。现场只读复查仍为原
generation/observedGeneration 58、3 Ready，未执行任何策略写入。

下一步仍是新 owner 的完整受控升级/逆序恢复、临时 PV 生命周期和
admission/hash 清单接入；HOLD 保留。这些离线通过不代替真实原门限
故障验收，也不构成整体生产就绪结论。
