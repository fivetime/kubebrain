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
