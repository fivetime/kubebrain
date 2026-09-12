# DBaaS 验收状态

核验日期：2026-09-11。当前验收环境为 `tk-001-003`；旧 `kind-kubebrain-dbaas` 结果单独保留在本页历史部分，不作为新环境现状。
产品要求及兼容性矩阵见 [兼容性计划](dbaas_compatibility_plan_cn.md)。本页列出当前证据的边界与下一步验收条件，不能代替完整矩阵。

总体状态：**尚未通过生产就绪验收**。已完成迭代编号、提交数和单元测试数量都不是整体完成百分比。

并发读取实时顺序补充（2026-09-12）：读取调用前记录成功写入已返回的最大
revision，要求结果不能落后于该下界，且顺序读取不能倒退到旧 revision 或空。
全部写入结束后增加一次 Range，保证即使十六次并发采样都先执行，也覆盖
写入确认后的可见性。十轮内存夹具 race 通过（1.525 秒），存储包 race／vet
通过（1.933 秒），本机真实 PD/TiKV 十三例 race 全通过。日志前缀为
`/root/.local/state/kubebrain/protocol-read-ack-floor-`，真实证据在
`/tmp/kubebrain-real-protocol.kwFnB1g8y6/`；临时容器、网络和测试二进制独立
核验清理。本次只加强测试判定，不修改产品或启用 1PC；原先 CI 不包含本次
新增下界约束，仍不声称完整故障历史的线性一致性或生产验收通过。

并发 Range 可见性补充（2026-09-12）：在上述独立双键事务用例中增加同时
放行的十六次 Range。每次结果必须为空或完整的同 revision 双键，值对应
某个成功事务，不能只出现一键或混合不同提交。所有写入与读取 goroutine
结束后才断言，避免测试失败时后台访问已关闭后端。内存夹具十轮 race
通过（1.510 秒），存储包 race／vet 通过（1.857 秒）；本机真实 PD/TiKV
十三例 race 全部通过，临时资源独立核验清理。证据：
`/root/.local/state/kubebrain/protocol-concurrent-reads-real-race.log` 及
`/tmp/kubebrain-real-protocol.IbbHKVumq5/`。这些有界采样不保证覆盖每个提交
交错窗口，不是完整线性一致性证明。提交 `7001fad0` 的 CI `34661956517`
随后通过：作业 `103466115675` 日志确认普通／race 各十三例、两轮清理及
PD／TiKV 两阶段中断清理通过。日志保留为
`/root/.local/state/kubebrain/7001fad0-real-protocol-ci.log`；不替代生产验收。

真实后端并发回归（2026-09-12）：隔离本机 PD/TiKV 8.5.3 的协议入口增加
第十三例 `TestRealTiKVBackendConcurrentWrites`，显式 2PC，四个同步放行
的双键写入分别提交 revision 101..104。验证 revision 唯一连续、双键事件
同批发布、最终版本和值一致及配额只计最终内容。十三例 race 全部通过，
临时容器／网络／编译测试文件独立核验不存在；完整日志为
`/root/.local/state/kubebrain/protocol-concurrent-final-real-race.log`，实际用例
证据在 `/tmp/kubebrain-real-protocol.EEaa94xK7J/`。存储／构建契约包 race
通过（2.006／2.501 秒），存储 vet 通过。随后提交 `0272fabf` 的远端 CI
`34660932207` 成功；作业 `103463101007` 实际日志确认普通／race 各十三例
通过，两轮 cleanup_failed=0，PD／TiKV 启动中断均退出 143 且资源不存在。
下载日志：`/root/.local/state/kubebrain/0272fabf-real-protocol-ci.log`。
本地余量断言十轮通过（1.876 秒）。开发时首次 CREATE 类型断言、合并前缀
的清理余量断言和新增场景白名单遗漏均先失败，修正后完整重跑通过；未放宽
100 键余量断言或 128 键总清理上限。该用例不保证每次都发生冲突，不替代
三副本故障、完整线性一致性检查、规模 soak 或正式升级验收。

认证调度条件复验（2026-09-12）：以 `GOMAXPROCS=2` 运行
`TestRestoredSnapshotClientOnlyTLSWithDefaultPasswordCost` 三轮 race，通过
（126.949 秒），未复现 invalid auth token。覆盖三个真实嵌入式 etcd 成员、
默认 bcrypt 成本、client-only TLS、恢复数据规模及权限矩阵；测试断言恢复目录
和临时身份清理为空。Go 调度并行度不等于容器 CPU 限额，此结果不排除容器
限流或其他时序条件，不是部署验收。未修改产品代码、重试或集群资源配置。
日志：`/root/.local/state/kubebrain/auth-restore-gomaxprocs2-race.log`。

对应本地依赖源码核对：恢复配置沿用 embed 默认 simple token／300 秒 TTL，
恢复验证上下文为 100 秒；`auth/simple_token.go` 等待 token 内索引的 applyWait，
`etcdserver/server.go:applyAll` 在 applyEntries 后触发等待者。正常应用路径
未显示“尚未应用认证条目就唤醒索引等待者”的顺序；这只是源码路径核对，
不是原故障现场证据，也不能据此排除其他 token 失效或上下文取消原因。

预取冲突回归补充：`TestAtomicPrefetchConflictRequiresFreshTransaction` 使用
客户端 SDK 与模拟 TiKV，分别覆盖预取时键已存在／不存在。竞争事务先提交后，
旧事务仍读到旧快照，但实际 Commit 必须报 WriteConflict，且不能发布伴随
payload；重建事务必须读到竞争者的新值，随后提交的键与 payload 均可读。
与原快照／读己之写用例一起运行十轮 race 通过（1.338 秒）。这是确定性
客户端事务语义测试，不是真实 PD/TiKV 并发或故障测试，不关闭以下性能门限。
日志：`/root/.local/state/kubebrain/atomic-prefetch-conflict-race.log`。
随后存储适配器包 race 通过（1.883 秒，未启用外部真实协议用例），vet 通过；
完整包日志为同目录 `atomic-prefetch-conflict-full-race.log`。

最新正式复验（2026-09-11，`2352b9bf`）：事务内 revision 预取优化已部署，
三副本完成滚动更新，探针覆盖滚动过程。保持 6000 次、操作后 100ms、公共
5s／直连流 30s、滚动完成后 900s 原门限，仍因未在完成窗口内结束而失败，
执行器退出 1。最后保留的 4380/6000 包含回滚期间操作，不是截止时完成量。
自动回滚后独立核验原 StatefulSet 完整 spec、三个副本 Ready 与运行镜像一致，
generation／observedGeneration=34，恢复原 `0ce85e66` 基线；本次探针和预拉取
Pod／Job 均不存在，测试键／用户／角色／租约清理通过。两个本次编译的清理
工具已核对摘要后删除，脚本、日志和校验记录保留在
`/root/.local/state/kubebrain/allocator-prefetch-release.ufJgJZZz/`。

同次验收的两次只读指标采样核对探针和所有服务 Pod 身份未变。活跃副本
`kubebrain-2` 的 396 个成功 Put 后端样本均值约 106.93ms，提交子样本约
84.03ms；子阶段不能重复相加。SDK 响应总体不同，包含后台请求与重试：
1122 个成功 Prewrite 中 271 个、1121 个成功 Commit 中 436 个因异常
persist_log 时长被标记 invalid_write，其时长样本整体排除，保留成功结果。
各阶段有效样本数分别为 851／685，证明真实环境中的过滤生效，不证明底层
异常已修复；过滤后的均值不能直接与旧版未过滤均值比较。详见私有证据中的
`write-window-analysis.md`。性能完成门限及认证根因仍未关闭。

事务内 revision 预取合并（发布及复验结果见上）：把持久化 revision 分配器的读取与已知
提交检查键放到同一事务快照预取，后续 Get、revision 边界检查、CAS 及冲突
保护写入仍执行；不支持预取的存储保留原路径。预取失败不分配 revision，
新增测试覆盖缺失、已有、损坏、耗尽及失败边界。真实单 Region、2PC、带配额
的 20 次写入，标记前台请求从 Get=60 降为 40，BatchGet=60、Prewrite=20、
Commit=20 不变；修正的是一次读取 RPC，不是减少两阶段提交或 PD TSO 请求。
本地十二项真实协议 race、边界十轮 race、完整后端／存储 race 均通过
（完整包 81.066／1.886 秒），etcd 接入层 Txn／Quota／Corrupt 筛选竞态回归
通过（91.270 秒，非整个接入层测试包），临时资源独立核验清理。日志前缀
`/root/.local/state/kubebrain/allocator-prefetch-`。此时尚未部署，不宣称正式性能收益。
其前置计数基线 `631fecff` 的协议 CI `34653805839` 已核验普通/race 各十二例
通过、清理成功、两阶段中断退出 143 且资源不存在；该 CI 不包含本次优化。

优化提交 `2352b9bf` 的协议 CI `34654692308` 随后通过，普通／race 各十二例、
清理和两阶段中断检查通过；镜像 CI `34654692368` 也已成功。独立核验发布
索引与平台摘要、dbaas 标签指向、实际 amd64 二进制提交号与 Go 版本、非 root
配置、固定 fork 依赖及写入／认证诊断字段。发布索引为
`sha256:8e0ef7ea9d8b7710e7d8dc9750be985febdfc74a89f5f4ea46c8ac93de2d6710`。
核验进程退出 0，临时容器和提取的两个二进制均已删除并独立确认不存在；
证据在 `/root/.local/state/kubebrain/allocator-prefetch-release.ufJgJZZz/`。
同期只读确认集群仍为原 `0ce85e66` 基线，generation／observedGeneration=32、
Ready=3。此时新镜像尚未部署；后续正式复验失败及恢复状态见本页顶部。

后端资源只读诊断（2026-09-11，非正式负载窗口）：三个 TiKV Pod 均 Ready、
重启次数为 0，实际容器 CPU 配额为 8 核，自身 cgroup 的 nr_throttled 与
throttled_usec 均为 0；当前 I/O PSI 有非零等待信号。两次指标抓取之间逐个
核对 Pod UID、容器 ID、镜像 ID、启动及重启状态一致。Raft Engine 日志同步
分别增加 381／381／639 次，均值约 18.26／20.42／16.98ms；其中一个副本
没有前台 storage command 增量，仍有日志同步，不能把前台命令数当作全部
副本 I/O。该窗口未新增压测负载，既不代表正式验收负载，也未证明 Ceph 根因。
不同指标总体不能混算或相加；未改变 CPU、同步持久化、租约或存储配置。
原始窗口与身份核验记录：
`/root/.local/state/kubebrain/tikv-request-window.RGwLwBRP/`；资源检查见
`/root/.local/state/kubebrain/tikv-{cpu-quota,pressure}-readonly-check.md`。

认证回归覆盖边界：`TestRestoredAuthTokensAcrossAllMemberPairs` 用原始生成的
RPC 客户端验证所有成员间 token 复用，刻意不经过官方 clientv3 刷新和重试。
当前依赖 client/v3 v3.7.1 的 `retry_interceptor.go:streamClientInterceptor`
会在建立流之前调用 `getToken`；用户名密码模式的 `client.go:getToken` 再次
Authenticate 并更新共享凭据。因此上述回归通过，不能排除真实 Watch／
KeepAliveOnce 的流创建、token 刷新或请求时序相关问题。已有官方客户端权限
矩阵回归也不是原偶发错误的稳定复现；尚无证据足以指定根因或修改重试策略。

官方客户端诊断回归：`TestRestoredAuthRPCTraceOfficialKeepAliveRefresh` 使用
真实 loopback gRPC 和 clientv3，服务端记录建流前的 Authenticate 次数，
确认 KeepAliveOnce 比客户端初始化多一次认证。受控服务随后返回
invalid auth token，测试确认原错误返回且未耗尽上下文，诊断保留成功认证
及被拒绝 Lease 流的真实 peer/status，不泄露密码或令牌。相关诊断测试
十轮 race 通过（1.440 秒），日志为
`/root/.local/state/kubebrain/auth-official-refresh-trace-race.log`。
这是显式注入拒绝的传输测试，不是恢复集群故障复现，不验证 token 在 etcd
成员间的真实有效性，也不改变产品认证或重试逻辑。

随后补齐 `TestRestoredAuthRPCTraceOfficialWatchRefresh`，与 Lease 用例共享
同一受控服务，确认官方 Watch 建流前再次认证，并通过 Canceled 响应交付
注入的 invalid token 错误而非静默关闭。两条路径均校验最后一次成功认证与
失败流的 peer/status；整组诊断测试十轮 race 通过（1.533 秒），日志为
`/root/.local/state/kubebrain/auth-official-watch-lease-refresh-race.log`。
同样只验证客户端与诊断路径，不证明恢复场景的 token 错误已定位或修复。

写入响应诊断候选正式验收（2026-09-11）：`11da1003` 镜像构建
`34640955142` 和协议 CI `34640955159` 成功，后者普通/race 各十一例
通过，两阶段中断清理通过。核验镜像来源、平台摘要和实际二进制后，部署
`sha256:047e2fbc053c5df123b3e0b6e7e00f270aec1dd972d96a71351c4c022d1f4db2`。
保持 6000 次、操作后 100ms、公共 5s／直连流 30s、升级完成后 900s
原门限，仍因 900s 内未完成而失败，执行退出 1。最终 4500/6000 进度
包含回滚期间操作，不是截止时计数，也不是完整通过。独立确认恢复原
`0ce85e66` 基线，generation/observedGeneration=32、Ready=3，
StatefulSet spec、三副本运行镜像摘要与执行前一致；本次探针和预拉取
Pod/Job 不存在，入口确认测试键、用户、角色和租约均为 0。
证据：`/root/.local/state/kubebrain/write-response-release.l4P2mgPY/`，
包括 `execute.log` 和 `restored-verification.log`。

同实例两次抓取间，成功 Prewrite/Commit 分别有 2060/2057 个样本，RPC
均值 36.420/33.143ms，commit_log 均值 32.160/28.660ms；这些是 SDK
总体样本，包含后台请求，不是 Put 专属，嵌套阶段不能相加。persist_log
出现异常巨值，451/2060 和 659/2057 个样本超过最后有限桶 41.94304s，
累计和数量级提示可能存在无符号下溢或哨兵值，但未捕获对应原始响应，
不能认定 TiKV 根因，更不能解释为真实磁盘延迟。
待部署防护 `5188ff16` 将超过有符号纳秒范围的明细单独计数，并排除整组耗时
样本（含 RPC），不改变请求成败、不钳制为零；不能据筛选后均值宣称
性能改善。针对性 race 十轮及完整存储包 race 通过，真实 PD/TiKV 十一例
race 通过，临时容器、网络和测试二进制独立确认清理。日志：
`/root/.local/state/kubebrain/write-response-invalid-real-race.log`。
该提交协议 CI `34647241447`（作业 `103420842638`）成功；实际日志确认
普通/race 各十一例通过，两次清理成功，PD/TiKV 两阶段中断均退出 143
且 resources_absent=true。日志位于
`/root/.local/state/kubebrain/5188ff16-real-protocol-ci.log`。
该提交镜像 CI `34647241350` 成功，独立核验来源、双平台摘要、推广标签、
实际 amd64 二进制版本、客户端 fork 依赖和新增异常指标字段，索引为
`sha256:93415578631841c550cac6897197a6f09f98f7781595f7ef513c83fbbf0b7471`。
核验进程退出 0，临时容器及提取的二进制独立确认清理；证据位于
`/root/.local/state/kubebrain/write-duration-guard-release.1vCI00wG/`。
该修正尚未部署，不关闭性能或认证根因问题；镜像身份通过不等于部署验收通过。

写入响应诊断接入的部署前验证（2026-09-11）：TiKV 客户端新增固定标签的响应
计数和阶段直方图，仅成功且原始 WriteDetail 存在的响应进入阶段统计；不增加
重试，不改变返回的响应或错误。明细缺失、错误与真实零值的边界见
[可观测性说明](observability_cn.md#tikv-写入响应诊断)。针对性十轮 race 通过，
完整存储／后端 race 通过（1.900／74.597 秒），etcd 接入层为通过的缓存结果。
真实协议十一例 race 再次通过，新增断言确认生产构造器安装包装且真实 RPC
产生的 prewrite/commit 样本可由默认 registry 导出，未人工 Observe 伪造样本。
临时容器、网络和测试二进制均独立确认不存在。日志：
`/root/.local/state/kubebrain/write-response-metrics-{core-race,export-real-race}.log`。
这只是诊断能力，后续集群数据与验收失败见本页上文，不关闭原正式验收失败。

协议门禁及写入明细验证（2026-09-11）：`30480bf8` 的专用 CI
`34638996704` 已成功，作业 `103393789160` 普通/race 各十一例明确 PASS，
清理均成功，两阶段中断均以 143 退出并独立确认资源不存在。日志：
`/root/.local/state/kubebrain/30480bf8-real-protocol-ci.log`。
随后本地测试直接检查原始响应：错误 Prewrite 有 ExecDetailsV2、无 WriteDetail；
两个成功 Prewrite 和主键 Commit 都有 WriteDetail，包含非零日志持久化、日志
同步、复制确认和应用耗时。已记录成功标志与字段存在性，不能将缺失字段当作
零耗时。新增诊断后的十一例 race 通过，临时资源独立确认清理；日志
`/root/.local/state/kubebrain/write-response-details-classified-race.log`。该诊断
变更不包含在上述旧提交 CI 结果中。单副本临时内存盘数值不代表 Ceph 集群，
此处只验证可用的响应字段；后续产品指标接入见上文，未宣称定位生产性能根因。

锁解析归因边界（2026-09-11）：隔离真实 PD/TiKV 的新回归暂停指定两阶段
事务的次要键 Commit 及后台 ResolveLock，读取仍返回已提交值；实际观察到
一次主键状态查询、零次前台 ResolveLock 完成。首轮测试错误地要求读取上下文
中出现 ResolveLock，真实执行否定了该假设；源码与修订后的暂停测试共同确认
客户端将物理锁清理放到后台。普通/race 各十一项协议用例通过，临时容器、网络
和测试二进制已独立确认清理。证据日志 `secondary-read-bypass-real-{normal,race}.log`
位于 `/root/.local/state/kubebrain/`。这不排除后台清理争用资源，也不证明其是
正式验收失败根因；SDK 总耗时不得当作前台逐请求等待时间。未修改产品运行逻辑。

元数据复用候选正式验收（2026-09-11）：将已核验的 `e3184226` 镜像
`sha256:f0e6a497b32e7d6a010fc482abff417515af4e48ed803f9baae2615d84b55bd1`
滚动部署至三副本，generation=29、Ready=3。保留 6000 次操作、操作后 100ms
间隔、公共 5s／直连流 30s、升级完成后 900s 的原门限；仍因未在 900s 内完成
而失败（执行退出 1）。最终保留的 4380/6000 进度包含回滚期间操作，不是截止
时计数，也没有完整成功 summary。已回滚原 `0ce85e66` 基线，generation/
observedGeneration=30、Ready=3。独立比较 StatefulSet spec、三副本实际镜像
摘要均恢复；本次探针及预拉取 Pod/Job 均不存在，入口清理核验临时键、用户、
角色、租约均为 0。两个临时编译恢复工具已删除，日志和脚本保留。
证据：`/root/.local/state/kubebrain/metadata-reuse-release.qepEUPOI/`。

同实例两轮指标增量的 305 次成功 Put：pre-backend 均值 17.346ms、backend
111.524ms；批次 prepare 5.840ms、commit 83.904ms，其中 SDK prewrite
51.038ms、primary commit 31.685ms。嵌套阶段不能相加，窗口亦非严格前后
对照，不能宣称元数据复用已带来性能收益。同窗口 SDK store 2004 的 ResolveLock
增量为 682 次、均值 36.433ms；该统计包含后台请求，不是 Put 专属，也未证明
锁解析或 Ceph 是单一根因。未启用 1PC/async commit，未修改 PD/TiKV 或 Ceph。

恢复认证跨成员回归（2026-09-11，仅本地测试）：新增
`TestRestoredAuthTokensAcrossAllMemberPairs`，将含认证数据的快照恢复为三个
真实嵌入式 etcd 成员，使用 TLS 和默认 bcrypt 成本。分别向每个成员认证，
把所得令牌直接用于每个目标成员的 Watch、LeaseKeepAlive，逐轮各覆盖 9 条
有向组合；校验 Authenticate 和 Watch 响应的成员 ID，避免只验证单一落点。
使用原始生成的 gRPC 客户端，不经过 etcd 客户端令牌刷新封装；租约场景在
目标成员 Grant/Put 后调用原探针的全成员复制屏障，确认只写用户读取租约键
被拒绝，再用同一令牌 KeepAlive/Revoke。管理员屏障使用独立上下文，不能
继承只写用户的出站令牌。初版三轮 race 通过（39.623 秒）；补齐复制屏障和
拒绝检查后三轮 race 通过（44.536 秒），`go vet` 通过。最新证据：
`/root/.local/state/kubebrain/restored-auth-cross-member-barrier-after-race.log`。
官方客户端在建立流前还会重新 Authenticate，此原始客户端测试不覆盖该刷新
与流创建之间的并发行为，也不据此排除原探针的完整调用顺序问题。
这补齐固定成员组合的回归覆盖，没有复现原偶发错误，也不是运行时修复；
未修改或部署产品代码，未改变 Kubernetes、PD/TiKV 或 Ceph。

恢复认证诊断补齐（2026-09-11，仅用于独立观察探针）：为每个恢复后的密码客户端记录
最近 16 条 RPC 的方法、目标地址和 gRPC 状态码，单独保留最近一次 Authenticate
结果，仅在权限矩阵验收失败时附加到原错误。诊断不读取请求/响应内容、元数据、
令牌或密码，不增加重试，不改变错误判定。真实 loopback gRPC 测试确认能关联
Authenticate/Watch 的目标地址，十轮 race 测试通过；三成员、TLS 客户端证书及
默认密码成本的恢复回归通过（47.232 秒）。原来的偶发 invalid auth token
尚未复现或修复，后续必须用失败现场的这些诊断区分认证与失败请求落点。

该诊断提交 `e3184226` 的镜像 CI `34627154923` 已成功；独立核验提交、
平台摘要、实际二进制版本及探针诊断字段后，使用索引
`sha256:f0e6a497b32e7d6a010fc482abff417515af4e48ed803f9baae2615d84b55bd1`
执行一次 OBSERVE_ONLY 观察。300/300 通过、Snapshot 完整验证 1 次、无流重试；
本次未复现认证失败。最后一次普通进度记录为 87.939 秒，等待全部流结束后的
最终诊断记录为 107.781 秒，不能混用两者计算逐操作均值。服务端仍为原
`0ce85e66` 基线，generation/observedGeneration=28、Ready=3，独立比较
StatefulSet spec 和三个服务 Pod UID/containerID/imageID/restarts/ready 均未变。
探针已删除，入口核验临时键/用户/角色/租约均为 0。镜像审计容器及提取的
二进制已移除。证据：`/root/.local/state/kubebrain/auth-rpc-observe.KQEk7hjm/`。
这只是一次通过的诊断观察，不消除原两次认证失败，也不替代 6000/900 正式验收。

真实协议替代 SQL mock（2026-09-11）：提交 `a5f8e0bd` 的 Runner 作业
`103343113136` 成功，普通与 race 各十项测试逐例明确 PASS，PD/TiKV 创建后
两个中断阶段均返回 143 并独立确认资源已清理。证据：
`/root/.local/state/kubebrain/a5f8-real-protocol-ci-success.log`。此前两类 Runner
失败分别为自动子网不允许静态 IP、容器 nofile=65536 低于 TiKV 所需 123880，
均保留记录；修复只改变临时 Docker 测试环境，未调整 Kubernetes/Ceph。

五类旧 mock 故障已逐项映射到真实测试，包括真实成功健康检查、上下文仍有效
时禁用 RPC 重试的已提交/未送达见证解析、默认重试和真实 Region 分裂回退。
覆盖及真实服务端返回差异见 [测试入口说明](../hack/backend-integration/README.md)。
已移除旧独立模块、两个测试文件、三个入口脚本和兼容补丁，Git 历史可恢复；
主 CI 改为复用同一真实协议工作流，根模块扫描包含 `-test`，无漏洞豁免。
迁移后本机构建契约 race 通过（2.478 秒），根模块 `govulncheck -test ./...`
通过：0 个可达漏洞；不可达模块级 openpgp 告警仍保留。当前所有 `go.mod`/
`go.sum` 已无 TiDB 条目。旧 SQL mock 引入的依赖已移除，不将此等同于整个
项目所有安全问题或生产验收已完成。
迁移提交 `bab7c2c5` 的专用协议工作流 `34624931162` 已成功：普通/race
各十项明确 PASS，清理结果均为 0，两阶段中断独立验证资源不存在。日志：
`/root/.local/state/kubebrain/bab7-retired-protocol-ci.log`。主 CI 的仓库内复用
调用通过本地结构契约，但 `gh workflow run ci.yml --ref dbaas` 返回 404：
默认分支没有登记此工作流。未为验证而修改 main，也不将专用工作流的成功
扩大为整个主 CI 已在远端执行通过。
以下为迁移前的历史记录，保留失败及当时未解决状态，不代表旧模块仍在当前树中。

历史真实协议本机对照（2026-09-11，入口当时尚在完善）：固定摘要的 PD/TiKV 8.5.3
在独立 Docker 内部网络、单副本及临时内存盘中运行，无宿主端口、无 Kubernetes/
Ceph 接入。根模块的七个真实适配器/后端测试全部执行通过：包含 1PC 提交、
丢成功响应后的默认重试和取消、后端对已提交/未送达结果的解析，以及新增
的后端默认重试两场景。两场景均恰好 2 次请求、1 次丢失，事务起始和提交
时间戳不变；返回 revision=101，两键同一 Watch 批次，下一次写入 revision=102，
未进入不确定结果解析。它验证的是调用方默认重试，不是生产环境启用 1PC。
每例前缀已清理，独立核对本次容器/网络不存在，测试二进制移除，日志保留于
`/tmp/kubebrain-real-protocol.SXH6pPIM3n`；编排记录为
`/root/.local/state/kubebrain/real-local-backend-retry-run.log`。
清理异常与参数拒绝契约新增 13 个清理情形及 4 个拒绝场景，重复竞态检查
10 次通过（4.139 秒）；旧实现的日志失败路径会遗漏空网络，修复后仍保持
失败退出、同时清理安全可删资源。七个真实测试再次通过，独立检查资源及
编译二进制均已移除，证据目录 `/tmp/kubebrain-real-protocol.gVVIff00d7`。
随后第八项真实 Region 分裂用例通过（0.08 秒）：首次 1PC prewrite 前实际
分裂，收到 1 次 epoch 错误，在 2 个 Region 成功 prewrite，观察到两阶段
commit，未出现 1PC 提交结果，事务时间戳一致；两键同一 revision=101/Watch
批次，下一次写入 102。记录于 `/tmp/kubebrain-real-protocol.9bcJbbSISw`，
该次八例全通过，独立核对容器/网络及二进制均已清理。分裂只在本机一次性
集群进行，没有修改 Kubernetes 测试集群 Region。
启动中断补验：PD 创建后、TiKV 创建后分别发送 SIGTERM，均返回 143 且独立
检查资源/二进制已移除。首次失败定位到清理标记被当时的命令重定向写入 ID
文件（资源实际已清理），修复为独立输出描述符后，两阶段实测与重定向回归
通过。证据：`/root/.local/state/kubebrain/real-local-interruption-{run,after}.log`、
`/tmp/kubebrain-protocol-interruption.obDLC6dvTn`。这不覆盖 SIGKILL 或宿主故障。
当时旧 mock 故障语义的完整替代审核仍待补齐，旧 SQL 模拟依赖未移除，
漏洞告警未关闭。单副本/内存盘结果不替代真实集群持久性、延迟或升级验收。

发布与全量回归补查（2026-09-11）：提交 `cc88957a` 的镜像 CI
`34604917676`、协议集成 CI `34604917691` 均成功；镜像索引为
`sha256:d0d8cd6f2cc6ea2a01496257c56d18bda72ddbf37796a49a65fe51866e5b17f4`，
尚未部署。随后直接运行根模块 `go test ./...` **失败**：主 CI 的动态
模块覆盖契约发现 `hack/backend-integration` 未列入检查，且生产工具包
触发 Go 默认的累计 10 分钟上限。超时时的单个用例只运行了 5 秒，隔离
复验通过（5.541 秒）；不能将包超时记为该用例卡死，也不能把全量运行
记为通过。已按现有 CI 的四分片入口重新验证全部 721 项生产工具测试，
分片分配为 174/197/184/166 项，全部通过，耗时分别为
482.595/543.670/405.991/815.564 秒。分片使用既有 15 分钟包级上限，
没有修改单项测试条件。证据：`cc88957a-production-partition.log` 和
`cc88957a-production-shard{0,1,2,3}.log`。这不消除下述漏洞扫描失败。

历史主 CI 覆盖补齐：后端模拟模块通过原隔离入口执行 vet、普通
及竞态测试，保留远程客户端版本校验和临时依赖补丁；漏洞扫描必须包含
`-test`，因为该模块只有测试文件。构建契约、入口契约和新增 vet/普通测试
通过，但真实 `govulncheck -test` 检出旧 TiDB 模拟依赖的
[GO-2024-3284](https://pkg.go.dev/vuln/GO-2024-3284)，此项**当时尚未解决**。
数据库条目为未审查自动导入，Go 版本范围标为全部版本、未映射修复版本；
其自定义版本说明为 TiDB 8.2.0 之前，不能据扫描输出推断上游从未修复。
[上游修复](https://github.com/pingcap/tidb/commit/3d68bd21240c610c6307713e2bd54a5e71c32608)
处理表达式函数构造失败；尚未完成本模拟器调用路径的可达性与修复方案验证。
没有增加漏洞豁免、关闭扫描或移除现有 1PC 提交结果测试。
数据面 `tikv`、`badger` 构建均通过“不包含 TiDB 包”的新增依赖边界检查
（0.843 秒），但此边界不替代测试依赖告警的处理。
证据：`/root/.local/state/kubebrain/cc88957a-root-full.log`、
`backend-module-ci-{contract,entry,vet-test}.log`、`backend-module-vuln-before.log`、
`backend-module-production-boundary.log`。

TxnApply 元数据校验结果复用（2026-09-11，实现阶段记录，当时尚未部署）：兼容模式更新已有
对象时，复用第一次旧对象校验得到的元数据，不再重复解码内联值或再次
查询旧格式元数据。保留适配层 Get/条件更新、版本初始化、旧值读取、配额
校验、CORRUPT 告警和租约迁移；非兼容模式保持原有校验及错误顺序。
计数回归在旧实现上失败（同一旧格式元数据读取 2 次），新实现为 1 次；
这是精确键的存储接口调用计数，不是 TiKV RPC 数量或真实延迟测量。
新增失败注入回归确认元数据读取错误保留错误链、无写结果、旧对象及版本
不变；两个新增测试 `-race -count=20` 通过（2.208 秒），相关租约迁移、
损坏旧对象和不确定提交测试组合 `-race -count=20` 通过（22.967 秒），
后端与 etcd 接入层 vet 通过。完整普通回归通过（后端 51.783 秒、etcd
接入层 135.939 秒）；完整竞态回归也通过（后端 78.260 秒、etcd 接入层
320.392 秒）。后续部署验收失败并回滚的结果见本页顶部。
证据：`/root/.local/state/kubebrain/txn-metadata-reuse-{before,focused,errors,vet,full,full-race}.log`。
此变更没有重新引入下述被否决的普通 Put 快捷路径，不关闭性能验收失败。

普通 Put 预读优化的否决记录（2026-09-11）：本地尝试直接调用原子
`TxnApply`，以省去适配层的 Get/条件 Update 循环。已有初始化版本的后端
测试通过了损坏旧值拒绝、旧格式租约迁移和不确定提交解析；但冷启动适配层
测试失败：首个普通 Put 已提交 revision=2，提交水位和 collector 游标仍为 0，
触发原 3 秒提交等待上限，返回不确定结果。进一步核对发现原 Get 还调用
`safeCurrentRevision` 来初始化空库 revision=1，且其读取/条件更新路径保留
了缺失索引恢复行为，不能把它视为单纯多余的对象读取。
未经等价验证的快捷路径及新增后端 API 已撤回，产品代码保持原样；候选补丁
和失败日志保留在 `/root/.local/state/kubebrain/unconditional-put-*.log`、
`unconditional-put-proposal-shim.patch` 及 `*.proposal.patch`。
新增 `TestPlainPutBootstrapsColdBackendWithoutPriorRead` 回归，检查没有预先
Range/租约写入/手工设置版本时，两个普通 Put 返回 revision=2/3，并立即可读且
保留正确生命周期。它是后续优化的约束，不是性能优化已完成的证明。
该回归与既有损坏当前值、Ignore 选项测试合并 `-race -count=20` 通过
（3.515 秒），vet 通过；证据 `put-bootstrap-regression.log`。

恢复权限校验错误链修复（2026-09-11）：授权 Watch 失败及预期拒绝却收到
其他错误的 Watch 分支，原先使用 `%v` 将客户端错误降为文本；新增回归在
旧实现上明确失败。改为 `%w` 保留 `WatchResponse.Err()` 的原始错误链，
缺少 Created 且无客户端错误时仍单独报校验失败，不伪造或包裹 nil 原因。
没有归一化/重解释客户端错误码，没有改变允许/拒绝条件或新增重试。
专项 race 连续 20 次通过（1.219 秒），vet 通过；完整探针包回归通过
（212.592 秒，包含恢复后的权限矩阵校验）。
证据：`/root/.local/state/kubebrain/restored-auth-error-chain-{before,after,full}.log`。
这修复的是错误传播，不是此前真实恢复集群间歇性 invalid-token 的根因；
此前两个失败仍保留，尚不能据此宣称认证问题已解决。

文件扩展开销对照准备（2026-09-11，未在集群执行）：同步探针保留默认
`append`，新增显式 `overwrite` 模式。后者先将新建的私有文件用随机数据
写满并同步，再从偏移 0 开始覆盖同样大小的区域；测量期间文件不再增长。
准备阶段字节/耗时单独记录，不混入逐次同步样本；准备与测量共用原软时限
和总 64MiB 写入上限，准备失败或取消不会生成测量样本。此模式不是 TiKV
日志分配算法的复刻，只为后续控制文件增长这一变量。目前尚未取得真实卷的
两模式对照结果，不能认定此前 48–56ms 均值由文件扩展造成。
专项 race 连续 20 次通过（1.150 秒），vet 通过；覆盖实际本地 fdatasync、
准备后文件大小/偏移、准备与测量样本分离、取消/失败及准备字节计入上限。

独立消费者卷同步测试（2026-09-11 12:10—12:12 UTC）：同步探针源码
`dbe2487c` 的静态二进制在三个 worker 上依次运行。每次新建独立 1Gi PVC，
StorageClass UID 固定为 `d3283670-e235-4676-ad5a-11f42ce6526e`，驱动及
PV 的 clusterID 均核对为消费者 `rook-ceph`，实际 `/test-volume` 为 RBD/ext4。
没有挂载 TiKV/PD 数据卷，没有使用 `rook-ceph-secondary`。每个样本群均为
256 次、每次 4096 字节随机追加后 fdatasync，共写 1MiB；全部完成，文件清理成功。

| Worker | fdatasync 均值 | 原始样本最近秩 p99 |
| --- | ---: | ---: |
| k8s3-worker1 | 52.026ms | 155.135ms |
| k8s3-worker2 | 55.627ms | 188.727ms |
| k8s3-worker3 | 48.459ms | 110.759ms |

每个 Pod 的 UID、节点、容器 ID、实际镜像 ID、重启数在采样前后相同，节点
UID 未变；远端探针 SHA-256 与本地构建记录一致。容器载体为已核验的
`136eb75f` 镜像，执行的是另行传入并校验的 `dbe2487c` 探针，不是重新部署
KubeBrain。无预分配的新文件追加包含扩展及文件系统开销，不能与 TiKV
预分配日志文件或不同负载窗口直接比较，也不能将这些数字视为纯物理设备延迟。
这说明脱离 KubeBrain/TiKV 请求路径的独立卷也有较高同步耗时，支持继续检查
存储同步路径；尚未确定具体 Ceph 组件、节点或共享负载的责任。
三组测试 Pod/PVC 按记录 UID 删除，PV 自动回收，执行器与后续独立 API
查询均确认三类资源不存在；两个临时工具二进制已删除，构建记录、校验和、
原始 JSON 和身份记录保留。原 KubeBrain StatefulSet 仍为 3/3 Ready。
证据：`/root/.local/state/kubebrain/storage-sync-probe.OiwRbUnX0TpP/` 下
`isolated.log` 和 `sync-dbe2487c-oiwr-{1,2,3}/`。此诊断不是生产验收通过，
下一步需控制文件分配方式并了解后端是否共享，再决定后续测试范围。

日志同步诊断补充（2026-09-11）：重用下述 `tikv-window.qaibg8Wqbqaz`
原始窗口，未重新施加载荷。`raft_engine_sync_log_duration_seconds` 在
`kb-tikv-0/1/2` 分别增加 954/711/1011 次，均值为
21.732/21.335/17.955ms，差值另存 `*-deltas-with-sync.log`，原分析保留。
现场 `tikv_server_info` 标识 v8.5.3、源码 `13b9af5c`；本机 `/root/tikv`
参考 HEAD 并非此版本，不用其依赖版本解释现场指标。
[v8.5.3 依赖锁](https://github.com/tikv/tikv/blob/v8.5.3/Cargo.lock)
固定 Raft Engine `2f9f6888`；对应
[日志同步实现](https://github.com/tikv/raft-engine/blob/2f9f6888dc2c88d3e2b582187c26e196b88b8ef3/src/file_pipe_log/log_file.rs#L114)
在文件句柄同步调用外计时。这进一步支持检查存储同步路径，但指标不是纯设备
耗时，不能分辨节点文件系统、RBD 网络、Ceph 副本或物理设备的责任，也不能
用不同计数的 sync/write 均值相减估计比例。
新增 [有界同步诊断工具](../hack/production/cmd/storage-sync-probe/README.md)，
仅允许显式指定测试目录并确认写入，限制次数、块大小与总字节数；测量独立
write/fdatasync，取消或失败返回非零，清理不递归删除目录。专项 race 连续
20 次及 vet 通过，包含本地实际 fdatasync、错误/取消、统计量与原始样本一致性、
既有文件和意外目录项保留测试。真实集群独立 PVC 测试见上，仍不能据此
宣称底层存储根因已确定。

最新正式复验失败与真实写批次采样（2026-09-11）：`136eb75f` 已通过全部 24 项本地
发布门禁（含完整 race 和 721 项生产脚本测试）、镜像 CI `34590401325`
及后端 CI `34590551289`，实际镜像源码、fork 和阶段观察器核验通过。
候选索引 `7c763acc`（amd64 `6a9bbc54`）已完成三副本滚动，探针确认覆盖
升级过程。原 6000 次、100ms 间隔、公共 5s／直连流 30s 和升级后 900s
完成门限不变。执行器已报告 `availability probe did not complete within 900s`，
本轮正式执行退出 1，辅助采样退出 0，两者结果独立保留。最后保留的
4320/6000 进度包含回滚期间继续运行，不是截止时计数，没有完整成功 summary。
自动回滚后独立核对 StatefulSet UID/spec 与升级前一致，generation 与
observedGeneration 均为 28，Ready=3，修订恢复 `kubebrain-855b5bfb88`；
三个实际运行镜像均恢复原 `0ce85e66`，重启数均为 0。
清理记录确认测试 keys/users/roles/leases 均为 0；另经 API 独立确认探针、
清理 Pod、所有权 ConfigMap 及三个预拉取 Pod 均不存在，预拉取回执全部
标记 removed。两个本轮临时恢复工具二进制校验后删除，源码、日志和校验和
保留，可重新构建。独立恢复证据见同目录 `restored-verification.log` 和
`restored.j8cLjUrVFGnl/`。完成时限问题及此前间歇性认证失败仍未关闭。

11:29:28—11:30:00 UTC，同实例 `kubebrain-2` 的成功本地 Put 新增 134 次：
进入后端前平均 17.404ms，后端调用 103.173ms。独立核对的批次样本群也
新增 134 次，begin/prepare/commit 分别平均 0.904/5.679/78.852ms；
SDK 写提交明细样本群新增 134 次，prewrite/commit_ts/primary_commit
分别平均 47.716/1.101/30.000ms。这次计数恰好一致不构成普遍一对一保证，
SDK 阶段包含在批次 commit 内，不重复相加；两个 follower 未产生本地 Put
序列，不能用零值替代。两次采样内外的 Pod UID、容器 ID、镜像 ID 和重启数
保持一致。这支持将后续排查重点放在提交路径，不证明磁盘或网络是根因。

另取 11:39:11—11:39:43 UTC TiKV 服务端窗口，三个 TiKV、PD、KubeBrain 和探针身份及
就绪状态在窗口前后保持一致。`kb-tikv-0/1/2` 的 Raft Engine write
独立直方图增量分别为 960/723/1018 次，平均 21.732/21.111/17.950ms；
Raft 写任务等待分别平均 11.220/7.566/9.326ms。接收事务请求的
`kb-tikv-0/2` 服务端 prewrite 平均 42.681/35.128ms，commit
38.543/29.502ms，而 Get 约 0.386/0.414ms。这些是混合服务端样本，
并非上面的 134 次 Put，也不与其处于同一时间窗口；不能相加或直接相减，
不能将 Raft Engine write 指标等同于设备 fsync 或据此宣称 Ceph 故障。
下一步需区分写队列、Raft 持久化和复制等待；未修改 TiKV/PD 或存储配置，
1PC/async-commit 仍关闭。
证据：`/root/.local/state/kubebrain/worker-cleanup-release.uXl9OvBK/`，
`batch-phase-deltas.log`、`sample.2NIFSDMK8yB7`、`sample.0dPZZCMm3xpB`；
服务端原始指标、身份和差值位于 `tikv-window.qaibg8Wqbqaz/`。

发布回归中的测试生命周期修复（2026-09-11）：`18a4657d` 的完整 race 门禁
曾在 `TestWatchContextCancellationDoesNotWaitForBlockedRecv` 的原 100ms
返回检查失败，无数据竞态报告。相同源码单项 race 连续 100 次及另一次完整
race 重测通过，不能抹去原失败或据此宣称根因已确定；原发布门禁仍未通过。
进一步检查确认公共测试辅助函数只调用 `stopLeases` 和空实现的
`memkv.Close`，没有按所有权关闭服务器与后端后台任务。新增确定性回归
在旧实现上失败：清理函数返回时，受该服务器管理的任务仍未退出。
修复改为关闭 RPCServer、保留的原始适配层及原始后端，再结束指标 mock；
后台任务须取消并退出后才能返回。新清理回归与原 Watch 取消测试一起
race 连续 100 次通过（7.668 秒），普通全量通过（138.753 秒），vet 通过；
修复后的完整 race 通过（334.967 秒）。本次仅修改测试代码，未放宽 100ms 或真实集群
5s/30s/900s 门限，不能认定已修复生产 Watch 或 Put 性能问题。
证据：`/root/.local/state/kubebrain/test-server-cleanup-*.log`，
原失败保留在 `batch-observer-release.xcJHEgka/post-backend-race.log`。

写批次阶段观测补充（2026-09-11，已用于上述当前复验）：在本地 Put 的后端调用上下文内
增加可选批次观察器，TiKV 适配层记录 begin、prepare 和实际 commit 调用耗时；
SDK 提供写事务明细时，再记录 prewrite、commit_ts、primary_commit。
指标前缀为 `write.batch.`，单位秒，标签仅 method=put 和该批次的 success。
这里的样本是批次尝试，不是公共 Put 最终结果；CAS/不确定结果重试可能产生多个
批次，不能与公共 Put 数量直接等同。SDK 阶段包含在 commit 调用内，不能重复相加。
空事务或未取得明细的批次不伪造 SDK 写入样本，已有调用方 SDK 诊断上下文保留。
不采集键值、事务 ID 或远端地址，不更改错误分类、提交协议或重试策略。
实际 fork 客户端配合 mock TiKV 的写入、空事务、准备失败、调用方诊断上下文、
提交前取消测试 `-race -count=20` 通过；取消仍保留不确定结果分类，错误原样
交给观察器分类。指标单位/样本群专项竞态测试通过；存储层全量 race、vet
及存储/后端/etcd 适配层全量普通回归通过（etcd 适配层 136.832 秒）。
最初测试断言误用指针比较导致失败，修正断言后重新完整运行通过；
这些测试不证明性能问题已修复，也不替代真实集群采样。

上一轮正式升级复验失败（2026-09-11 09:42 UTC）：提交 `a264e6ad` 的 Put
阶段指标版本通过全部本地发布回归（含 race、1PC 入口、721 项生产脚本测试）、
镜像 CI `34580139855` 和后端 CI `34580692142`，实际镜像源码、fork 依赖及
指标字符串核验通过。候选索引 `af25e8b4`（amd64 `6b402da9`）三副本收敛到
`kubebrain-68bfff9445`，探针覆盖滚动过程；但仍未在升级完成后的原 900 秒内
完成 6000 次操作，正式执行退出 1。原 100ms 间隔、公共 5s／直连流 30s 门限
均未调整。最后保留的 4800/6000 进度包含回滚期间继续运行，不是截止时计数，
没有完整成功 summary，不能记为通过。

本次取得同实例、同窗口的真实阶段差值：`kubebrain-2` 在
09:29:28—09:30:00 UTC 两次采样之间新增 143 次成功本地 Put，
进入后端前平均 **15.790 ms**，后端调用平均 **91.084 ms**。两组采样前后及
跨样本的 Pod UID、容器 ID、实际镜像 ID 和重启数一致，两个阶段计数配对。
这支持继续排查后端调用路径，但后者包含适配层锁、读取、CAS 和原子写入，
不是纯 TiKV 提交时间。同窗口混合 TiKV 请求中，Get 平均约 1.05/1.28 ms，
Prewrite 30.34/38.19 ms、Commit 25.49/30.50 ms（store 2001/2004），
ResolveLock 在 store 2004 为 27.79 ms；不能直接相加或归属到单次 Put。
SDK 通用 commit 直方图还统计无写入事务，不能用其 9.89 ms 混合均值替代
实际写提交成本。下一步可利用单事务提交明细区分准备、提交和时间戳等待，
当前已补充上述批次观察器，但尚未证明根因，不改变 1PC/async-commit 关闭状态。

最初辅助采集因漏考虑运行环境附加的 `cluster` 标签而退出 1；原始数据和
失败记录保留，正式验收未被中断。修正只读解析后另取两组样本成功，不重启
探针、不改变负载，也不把原失败记录改成成功。自动回滚后独立核对
StatefulSet UID/spec 与升级前一致，generation/observedGeneration=26，
Ready=3，修订恢复 `kubebrain-855b5bfb88`，实际镜像均为原 `0ce85e66`。
测试 keys/users/roles/leases 均为 0；探针/清理 Pod、所有权 ConfigMap 和三个
预拉取 Pod 均已删除。两个临时恢复工具二进制校验后删除，日志和校验和保留。
证据：`/root/.local/state/kubebrain/put-phase-release.tCYMw8gC/`；
有效样本 `sample.6MU23P8KGrt9`、`sample.Zc3fYnVIkqs5`，
差值 `put-phase-deltas-v2.log`、`tikv-mixed-window-deltas.json`。
原完成时限问题和此前间歇性认证失败均未关闭。

Put 阶段取证补充（2026-09-11，已用于上述复验）：增加成对直方图
`write.pre_backend.latency` / `write.backend.latency`，单位秒，标签固定为
method=put 和后端返回的 success。仅本地请求实际进入后端调用时产生两条样本；
前者包括请求开始至后端调用前的鉴权、配额、领导权与租约锁等待，后者包括
适配层读取/CAS 或租约原子写入，不是纯 TiKV 提交时间。早期拒绝和 follower
代理不计入，两者必须按相同实例、窗口和 success 比较，不能与不同样本群的
apply/总请求均值直接相减。专项 `-race -count=20` 通过（1.985 秒），etcd
适配层全量测试通过（137.653 秒），vet 通过；未改写入语义，真实阶段采样见上。

上一轮正式升级复验失败（2026-09-11 08:24 UTC）：`68de51b5` 的事务预取版本
通过完整发布回归（含全量 race、1PC 入口和 721 项生产测试）、两个 CI 以及
实际镜像核验后，从 `0ce85e66` 滚动到候选 `b1e1c756`。三个副本收敛到
`kubebrain-6b94dfd9cc`，实际运行摘要匹配，探针覆盖滚动过程；但仍未在滚动
完成后的原 900 秒内完成 6000 次操作，执行器退出 1。不得将此记为验收通过。
本次完成超时路径成功保留了回滚后的探针日志；最后记录 4560/6000 包含回滚
期间的继续运行，不是截止时计数，也没有完整成功 summary。候选收敛后、回滚前
进度 900→3960 的 3060 次操作耗时 712.998 秒：写入结果确认平均 114.83 ms、
随后 Watch 等待 11.59 ms、固定间隔 100.60 ms；这是混合探针的客户端累计量，
不是纯 TiKV 提交耗时，也不足以单独量化本次优化收益。
自动回滚恢复 `0ce85e66`，独立检查 StatefulSet UID/spec 与升级前一致，
generation/observedGeneration=24，Ready=3，修订恢复 `kubebrain-855b5bfb88`。
测试 keys/users/roles/leases 清理均为 0，探针/清理 Pod、所有权 ConfigMap 和
三个镜像预拉取 Pod 均已删除；两个临时恢复工具二进制校验后删除，日志与校验和保留。
证据：`/root/.local/state/kubebrain/atomic-prefetch-release.vuWbn6mP/`。

事务比较键预取优化（2026-09-11）：新增可选 `AtomicBatchPrefetcher`，TiKV
通过当前 `KVTxn.BatchGet` 预取 CORRUPT、配额、比较条件、当前版本及旧格式迁移
所需键。仍原样执行每个 Get 比较和全部冲突保护 Put/Del；不跨事务缓存，不取得
新时间戳，不修改提交协议，无此能力的引擎保留逐键路径。预取错误在写入前返回。
后端测试对照预取/逐键路径的读取与写入记录，覆盖 CORRUPT、配额、并发创建冲突、
错误中止、迁移键收集和去重。实际 fork 客户端配合 mock TiKV 测试证明一次 BatchGet
后存在/不存在键均无新增 Get RPC，后续 Put/Del 读己之写正确；其他事务提交后仍读取
原快照。两组新增测试分别 `-race -count=20` 通过（3.233/1.283 秒）。
后端全量测试通过（50.473 秒），TiKV 存储包本地测试通过，两个包 vet 通过。
这些本地测试不证明实际 TiKV 集群或跨 Region 故障能力；随后实际部署复验如上
仍失败，不能据此关闭原 6000 次/900 秒升级问题。

普通 Put 优化前置回归（2026-09-11）：适配层目前先 Get/解码当前值，再以
Create/Update 比较循环进入后端 TxnApply；后端还会准备和校验当前对象。
这是源码中的重复读取候选，不是已测得的延迟归因。新增
`TestPutValidatesCurrentInlineLifecycleWithoutPrevKV` 锁定：即使不请求 PrevKV，
截断内联格式、未来创建版本、首版本生命周期不符和不可能的版本计数，也必须
在写入前拒绝；正常更新保留原 revision 比较条件。该测试与 mutation-key-lock、
事务前值投影和锁所有权测试一起 `-race -count=20` 通过（2.839 秒）。
本次只加测试，没有修改产品写入路径；直接删除预读仍需证明旧格式、损坏检测、
IgnoreValue/IgnoreLease、PrevKV 和并发语义等价，不能据此宣称性能问题已修复。

恢复权限失败诊断补充：`auth_stage` 标识 Range、Watch 或具体租约操作，
`access_elapsed_ms` 是该用户/键的一整项权限检查耗时，不是单个 RPC 时间。
`context_done` 和 `deadline_exceeded` 在返回上层、关闭恢复集群之前采集，
避免把清理耗时误判为请求超时。保留原始错误链，不增加重试、不延长期限，
不输出自定义取消原因中的任意文本。该版本已用于下述真实集群观测，但未捕获
新的失败，不能据此把无效 token 归因为超时。

阶段诊断镜像观测（2026-09-11 06:35 UTC）：提交 `17a0afe9` 的镜像 CI
`34567818026` 成功，独立核验实际二进制版本、fork 依赖和诊断字段通过。
只使用新探针，服务仍为 `0ce85e66`，300 次 OBSERVE_ONLY 通过，未复现认证错误。
public watch=300、direct=300×3、Range=66、Snapshot=1，租约存活，无流式重试。
计数循环 73.952 秒，写入结果确认累计 34.421 秒，随后 Watch 等待 5.836 秒；
含循环后校验的最终进度为 82.492 秒，两种时间不可混用。public/direct 最大延迟
600/604 ms；原 5s/30s 门限和 900s 完成窗口未变。
规范清理后 keys/users/roles/leases 均为 0，独立复核三个服务 Pod UID、容器 ID、
镜像 ID、重启数、Ready 及 StatefulSet UID/spec/generation/revision 未变，
本轮探针、清理 Pod 和所有权 ConfigMap 均已删除。此结果不关闭既往两次认证失败，
也不替代原 6000 次滚动升级验收。证据：
`/root/.local/state/kubebrain/auth-stage-observe.vC1eolYa/`。

同镜像独立前缀复测（2026-09-11 06:44 UTC）：在 `k8s3-compute1` 再运行
300 次 OBSERVE_ONLY，通过且无重启，仍未复现认证错误。计数循环 77.257 秒，
含循环后校验的最终进度 92.548 秒；public/direct watch=300/300×3、Range=73、
Snapshot=1、租约存活且无流式重试。public/direct 最大延迟 604/605 ms。
清理 keys/users/roles/leases 均为 0；独立核对三个服务实例身份、容器、重启数和
Ready 不变，StatefulSet UID/spec/generation/revision 不变，测试 Pod 与所有权
ConfigMap 已删除。两次通过不能关闭间歇性认证故障，也不能替代滚动升级门禁。
证据：`/root/.local/state/kubebrain/auth-stage-repeat.XivH88cR/`。

恢复认证环境隔离检查（2026-09-11）：将默认 bcrypt 成本、`CN=root` 客户端证书、
三成员官方 etcd 恢复测试编译后放入独立非 root Pod，在 `k8s3-network2` 连续运行
5 次均通过，未复现无效 token。测试不连接 TiKV、不使用真实源快照，也没有公共
探针并发负载，因此不能排除源数据、节点差异或并发调度影响。上传前后 SHA256
一致、Pod 无重启；Pod 已按 UID 删除，临时二进制校验后删除，日志保留于
`/root/.local/state/kubebrain/restored-auth-pod.RRnvwEmd/`。认证故障仍未解决。

同窗口指标观测失败（2026-09-11）：新前缀下两组 TLS 指标采集及跨样本实例身份
核验成功，但第 218 次迭代发现官方恢复集群的授权租约续租返回 `invalid auth token`，
最终 217/300，整轮失败。这是先前 Watch 以外的第二种流式认证失败，根因仍未定。
清理通过，服务端配置/身份未变。样本窗口中 `kubebrain-2` 的 71 次 Put：
`write.latency` 平均145.57 ms，apply 平均134.43 ms；commit-wait 的71次观测累计
为0毫秒（存在取整限制）。这支持继续调查 leader 本地 apply 路径，但不是纯提交
耗时：同窗口 TiKV commit 统计混有只读事务和后台工作，不能直接推导每次 Put 成本。
原始样本及差值：`/root/.local/state/kubebrain/write-metrics-observe.Td2zUPSw/`。

最新分段观测（2026-09-11）：探针 `2ef9d266` 经 CI `34561935083` 和独立镜像
校验后，在服务端仍为 `0ce85e66`、无滚动条件下完成 300 次操作。循环耗时
89.955 秒；写入结果确认累计 46.776 秒（155.92 ms/次），随后公共 Watch 等待
7.678 秒（25.59 ms/次），固定间隔 30.162 秒。public watch=300、direct=300×3，
Range=70、Snapshot=1，lease 存活，无 stream 重试；清理及服务端身份不变检查通过。
写入确认是本组样本中最大的非固定耗时，但包括客户端重试/确认读取，不能直接
等同服务端 Put 时间，或与其他工作负载的 TiKV 均值相减。本次未复现下述认证
错误，不关闭该问题，也不代表原 6000 次/900 秒升级验收通过。
证据：`/root/.local/state/kubebrain/auth-diagnostic-observe.cPrG7d8R/`。

分段延迟探针实测失败（2026-09-11）：提交 `29038dabc382` 的镜像 CI
`34557288219` 及独立镜像身份校验通过；只更新探针，服务仍为 `0ce85e66`。
300 次非滚动观测在第 209 次迭代报告 Snapshot 恢复验证失败：官方 etcd
对 exact-reader 的授权 Watch 返回 `Unauthenticated: etcdserver: invalid auth token`。
此前同一权限检查中的 Range 未报错。最终进度为 208/300，不是一次通过的观测，
也不能替代原 6000 次升级验收。故障根因尚未确认，不将它归为产品或探针的已知缺陷。
测试数据及资源已清理，三个服务实例和 StatefulSet 配置/身份未变。
原有 TLS、三成员、启用认证恢复测试本机重复 10 次通过（265.139 秒），未复现；
默认 bcrypt 成本与 `CN=root` 客户端证书组合测试也重复 10 次通过（272.038 秒），
仍未复现实际故障；新增用例不是修复，不能关闭这一验收失败。
完整失败证据：`/root/.local/state/kubebrain/e2e-split-observe.u8lzxE0V/`；
本机调查：`/root/.local/state/kubebrain/restored-auth-investigation.8peghe7t/`。

针对该失败，探针新增固定格式的 Watch 响应诊断：响应头是否存在、集群/成员 ID、
修订号、Raft term、创建/取消状态、事件数量及上下文是否结束。上下文状态在主动
取消 Watch 之前采集；无响应头时明确标记未知，不推断服务成员。新增字段不包含
token、密码或事件正文，不新增 RPC/重试，不改变允许/拒绝权限判断。上述最新
观测已使用该诊断版本，但没有触发认证失败分支，不代表认证错误已修复。

基线端到端短时观测（2026-09-11）：当前运行镜像 `0ce85e66`、三副本、不滚动，
使用规范 runner 的 `OBSERVE_ONLY=true` 执行 300 次操作。public watch=300，
direct watch=300×3，lease 存活，Range=70、Snapshot=1，无 stream 重试；所有权清理通过。
计数循环耗时 84.405 秒，其中公共写入到 watch 接收累计 51.783 秒（172.61 ms/次），
固定间隔 30.164 秒、后端健康采样 1.829 秒、额外直连 watch 等待 0.621 秒。
这将本次主要非固定等待定位到公共路径，但不能从累计总量区分写入确认和 watch 等待。
已补充下一版探针的 `put_resolve_ms` / `watch_after_put_ms` 累计计数，复用原有时间戳；
前者包括异常 Put 的重试和确认读取，不等于纯服务端 Put RPC 时间。此次实际镜像尚无
这两个字段，不能虚构其均值。此项不是 6000 次滚动升级验收，也不能与不同代码/请求
形态的后端内部均值相减。三副本身份/配置未变，900 秒升级失败仍待解决。
证据：`/root/.local/state/kubebrain/e2e-observe.9rW2jxvi/`。

受控后端协议延迟（2026-09-11）：同一二进制/Pod，2 GiB 配额、单键 256 字节更新，
每组 10 次预热后测量 20 次，按 2PC/1PC/1PC/2PC 顺序运行。四组均通过真实协议计数、
连续修订/watch/读取/配额及所有权清理核验。每种协议合并 40 个样本，平均耗时
2PC=45.92 ms、1PC=29.41 ms，相差约 16.52 ms。此为小样本单 Region 的后端内部比较，
不包含认证/公共 RPC/代理/副本 watch，不能与旧混合指标直接相减或替代升级验收。
共享测试入口的已提交/未提交不确定结果两分支也在真实 TiKV 复验通过。
生产协议未变，900 秒升级失败仍未解决。原始样本、身份及清理证据：
`/root/.local/state/kubebrain/backend-latency.dNiFb5M6/`；运行约束见协议测试说明。

升级超时取证修复（2026-09-11）：核对执行脚本发现，完成窗口超时只退出，未设置
原先仅用于“滚动期间探针失败”的日志收集标记；随后探针删除，因此最近一次失败的
执行日志没有保存 `PROBE_PROGRESS` 阶段累计耗时。修复使完成超时也进入 EXIT 取证：
仍先请求回滚，再核验探针 UID、按既有时间/大小上限收集日志，然后继续所有权清理。
取证错误不能阻止清理；不延长 900 秒完成窗口，不改变 6000 次操作、100 ms 间隔或
public/direct 延迟门限。此修复不是性能改善或升级复验通过，尚未重新部署执行。
回滚后的日志可能包含截止之后的样本，不能把最后一条进度当作截止时刻完成数。
本地复现/验证记录：`/root/.local/state/kubebrain/rollout-timeout-evidence.VggBfAIa/`。

真实后端未提交分支（2026-09-11）：仅测试客户端在标记的 1PC prewrite 送达前中断
并取消调用者，实际适配器返回不确定结果、候选修订号 101。真实后端解析为
not_committed=1、committed=0；可见修订号保持 100、双键不存在。下一次成功 CREATE
复用 101 且为第一个 watch 变更，未出现被中断事务的事件或修订号空洞。
实际 attempts=1、drops=1、start TS=`468999762448220169`、commit TS=0。
同一构建另用新前缀复验已提交分支通过：committed=1、next=102；两组清理均通过。
这补齐受控调用者取消下的真实后端两种解析结果，不是网络分区、进程重启、跨 Region
或 Raft 故障证明；生产 1PC 仍关闭，900 秒升级失败仍待解决。
证据：`/root/.local/state/kubebrain/real-backend-absent.Ejf6ofNf/`。

真实后端持久见证解析（2026-09-11）：进程内 KubeBrain 后端连接真实 TiKV，
用户双键事务收到成功提交响应后丢弃并取消调用者，实际返回 `ErrUncertainResult`。
后台见证解析 committed=1、not_committed=0；双键 CREATE/读取均为修订号 101，
下一次单键 PUT 为 102，未在此有序边界前重放原事务。一次 prewrite/一次丢失，
start/commit TS 为 `468999580939452418` / `468999580939452420`。
后端关闭及所有权 CAS 清理通过，两个隔离范围均为空；Pod 和辅助二进制已清理。
此项补齐**真实 TiKV 已提交分支**的后端解析证据，不代表未提交分支、进程重启、
领导权切换、跨 Region 或 Raft 故障验证；生产 1PC 不变，900 秒升级失败仍待解决。
证据：`/root/.local/state/kubebrain/real-backend-onepc.52dX4Lzj/`；
运行与清理约束见 [协议测试说明](tikv_protocol_smoke_cn.md)。下方旧测试的证据边界保留。

真实 1PC 响应丢失后取消调用者（2026-09-11）：收到真实成功提交响应后丢弃一次，
取消该次提交的子上下文，实际 `uncertain=true attempts=1 drops=1`，适配器正确返回
`ErrUncertainResult`。起始/提交时间戳为 `468999354656751617` / `468999354656751619`；
独立有效上下文读取两键、更新及历史快照检查通过，所有权清理确认前缀为空。
此项不使用全局禁止重试开关，不改变生产提交协议；是存储层不确定结果分类证据，
不是 KubeBrain 后端见证解析或 900 秒升级验收。证据目录：
`/root/.local/state/kubebrain/real-onepc-cancel.5ZtqwUmT/`。

真实 1PC 响应丢失（2026-09-11）：独立测试客户端收到实际成功提交响应后丢弃一次，
保留默认重试。实际尝试 2 次、丢失 1 次，起始时间戳 `468999021104463881` 与
提交时间戳 `468999021104463883` 在重试间保持不变，最终返回成功（不是不确定结果）。
成功计数为 OnePC=1、TwoPC=0、AsyncCommit=0；新快照两键可见、后续更新/历史值和
所有权清理均通过，专用 Pod 及辅助二进制已清理。单次提交 4.512 秒是故障注入诊断，
不是性能或可用性验收。此真实返回分支与先前 mock 中的不确定返回不同，不能据此
宣称真实 KubeBrain 后端见证解析已验证。生产提交协议不变。
证据：`/root/.local/state/kubebrain/real-onepc-response-loss.4M8d48MK/`；运行约束见
[真实 TiKV 协议 smoke](tikv_protocol_smoke_cn.md)。

真实存储协议 smoke（2026-09-11）：在独立非特权测试 Pod 内，使用本项目 TiKV
适配器连接已核验的 PD 集群 `7683177044639569228`，2PC 与 1PC 各运行一个独立进程，
实际成功提交计数分别为 TwoPC=1 和 OnePC=1，AsyncCommit 均为 0；历史快照和当前
两键读值断言通过。两个全新专用前缀的所有权受保护清理均完成，测试 Pod 按 UID
删除，本机及 Pod 内辅助二进制已清理；生产服务仍为基线版本、generation 22/22、
Ready 3，生产 1PC/async commit 未启用。单次首次提交 24.589/140.968 ms 不是受控
性能对比，不代表 1PC 更快或更慢。此处第二键 `witness` 不是后端持久见证实现。
运行条件、清理及证据边界见 [真实 TiKV 协议 smoke](tikv_protocol_smoke_cn.md)。
证据：`/root/.local/state/kubebrain/real-protocol-smoke.MccB2Vmu/`；真实 Raft 故障、
跨 Region、后端不确定结果解析和 900 秒升级验收仍需独立证明。

协议回退测试补充（2026-09-11）：后端协议集成新增提交途中 mock Region 分裂用例。
在首次用户 1PC prewrite 已完成分组后分裂 Region，要求真实客户端保留起始时间戳，
回退到至少两个 Region 的 prewrite 和两阶段 commit，不能出现 1PC 提交时间戳；
KubeBrain 两键读取及 watch 同批事件使用修订 101，下一次写入及单个事件为 102。
隔离副本关闭 1PC 后，用例因未触发分裂明确失败。普通十轮、race 三十轮及入口/
非 root 清理契约通过。压力测试曾复现逐用例 `EnableFailpoints` 写入与上一用例后台
锁解析读取全局非原子开关的数据竞争；已改为测试进程 `TestMain` 在创建客户端前
一次初始化，失败日志保留，修正后重复验证通过。证据：
`/root/.local/state/kubebrain/onepc-split-review.KZbd8q99/`。
仅改变测试与文档，不改变产品配置；真实 TiKV 跨 Region 持久性、故障恢复及性能
仍未由此证明，生产 1PC/async commit 保持关闭，900 秒升级失败仍待解决。

旧对象读取去重（提交 `ff5dc1a8`，2026-09-10）：事务准备已取得修订索引时，普通更新/删除
直接读取该修订的对象，缺失对象和固定快照仍使用原历史查询；提交阶段的修订比较、
配额与损坏告警保护不变。新增正常读取、快照接口分派、历史回退、存储错误传播和
双实例受控修订冲突测试。隔离实验后端全包普通/race 通过，边界与冲突各十轮 race
通过；故意移除回退或修订比较后对应测试均失败。固定快照替身仅验证接口分派，
双实例测试显式投递已提交事件，均不冒充真实 TiKV 历史隔离或网络复制验证。
证据：`/root/.local/state/kubebrain/txn-previous-read.2Lsqqpz1/`。
完整 gRPC 诊断表明令牌与证书身份路径成本不同；证书身份上下文加 2 GiB 配额时，
本实验将普通 Get 从 10 次降到 9 次，BatchGet 4、Atomic Get 4、Commit 1 不变。
这些是 memkv API 调用数，不是实际网络次数或升级时限通过证据。正式源码提交前后
完整门禁均通过：基准普通/race 各三轮、旧对象与配额专项普通/race 各十轮、Watch
计时专项普通/race 各百轮、后端/服务层/TiKV 适配完整普通/race/vet、受控后端协议
集成普通十轮/race 三轮，以及 720 项生产测试清单与四个分片。提交后四分片分别为
478.616/538.627/392.509/767.961 秒；两轮最终源码摘要一致，总退出码均为 0。
证据：`/root/.local/state/kubebrain/txn-previous-read-gates-corrected.NgaLDsY9/`。
发布 `5fe3d007` 的后端 CI `34542018818`、镜像 CI `34542018828` 均成功，
独立镜像版本/标签/非 root 用户/实际客户端依赖及双架构摘要核验通过。
2026-09-11 实际滚动升级到
`sha256:34569ba7d4f33395ea75828184f0ac04db7772729fac7abf8dbb0e663aa6fb59`，
三个副本完成更新，但仍未在滚动完成后的原 900 秒期限内完成 6000 次操作，执行 exit 1。
**本轮严格验收失败，旧对象读取去重尚不足以解决超时**；公共 5 秒、直连 30 秒及
0.1 秒间隔未改。自动恢复基线镜像 `sha256:0ce85e66320b27bf4cdb8f11981a76cba58926fa0e835fc47dc663f709b588ce`，
最终 generation/observed 22/22、Ready 3、current/update 均为 `kubebrain-855b5bfb88`，
三个 Pod 实际运行镜像一致、重启计数为 0；探针及 holder 已清理，测试键/用户/角色/租约均无残留。
证据：`/root/.local/state/kubebrain/tk-001-003/txn-previous-5fe3d007.HS3eG0ol/`。
同一 server2 进程两组采样增量：323 次 Put apply 平均 80.330 ms、508 次后端提交
平均 47.433 ms；store 2005 的 Prewrite/Commit 平均 19.221/20.180 ms。
采样混合前后台请求，不是每次 Put 的调用数、受控 A/B 或磁盘根因证明；保留失败结果继续诊断。

提交前检查补充：两次全包 race 分别在未修改的 Watch 无可用来源重连测试中出现
536/613 ms 的耗时，超过其 500 ms 断言；没有数据竞争报告。该用例隔离 50 轮及
单独完整服务层 race 均通过，但原失败批次保留为失败。现将这个只检查计时器与
节点状态的用例改为标准库虚拟时间测试，不再启动无关后端工作线程；保留 500 ms、
指定错误和零重试断言，并要求只检查一次节点状态。100 轮 race 通过；私有 overlay
故意把生产等待从 100 ms 改为 600 ms 后，测试明确在 500 ms 断言失败。
正式生产 Watch 实现未改，真实集群验收门限未改；修正后提交前后完整门禁均通过。
该虚拟时间测试证明计时器/重试语义，不代表真实调度耗时或集群可用性 SLO 已通过。

写入成本测量边界补充（2026-09-10）：原 `BenchmarkBackendWriteStorageCalls`
仅覆盖关闭配额，不能代表真实集群启用 2 GiB 配额后的调用数。隔离测试 overlay 仅开启
配额并在计时前初始化，两种配置各运行 100 次、重复 3 轮，均通过；开启后 TxnApply
为 6 Get / 1 BatchGet / 4 Atomic Get / 1 Commit，GetThenUpdate 为
8 Get / 1 BatchGet / 4 Atomic Get / 1 Commit。关闭配额仍分别是 4 和 6.01 Get，
均为 3 Atomic Get，其余两项相同。这些是 memkv 存储 API 调用数，未覆盖完整 RPC
鉴权/准入，也不是 TiKV 网络次数或集群延迟；不能据此认定升级超时根因。
证据：`/root/.local/state/kubebrain/write-cost-quota.VR1vfst4/`。规范基准已补齐
两种配额模式，并在计时外校验最终配额使用量；四场景普通三轮、配额相关测试与基准
race 三轮、backend vet 均通过。私有 overlay 故意保留旧配额使用量后，两个开启配额
场景均在新增记账断言失败，正式产品源码未改动。基准提交为 `97bf54d6`；提交前后
均通过四场景普通/race 各三轮、backend 完整普通/race/vet、720 项生产测试清单及
全部四分片，最终源码摘要一致。提交后四分片分别为 468.174/531.989/394.055/762.236 秒。
门禁证据：`/root/.local/state/kubebrain/write-cost-quota-gates.Z7ARmxOv/`。
后续评估准备阶段独立读取的合并；保留事务内配额和损坏告警等原子保护。

事务准备阶段批读（提交 `31c2954f`，2026-09-10）：在配额开启且未固定快照的路径，通过
可选 BatchGetter 一次读取 tracking、usage 和写入所需的 NOSPACE alarm；按原顺序
校验告警及元数据，批读失败不换快照回退，提交事务内 usage 比较与更新保持不变。
新增 put/delete、脏/缺失元数据、告警优先级、取消/不可用、禁用配额及固定快照回退测试。
后端全包普通/race、vet 及三轮基准已通过；开启配额时 TxnApply 为
3 Get / 2 BatchGet / 4 Atomic Get / 1 Commit，GetThenUpdate 为
5 Get / 2 BatchGet / 4 Atomic Get / 1 Commit，禁用配额场景不变。
证据：`/root/.local/state/kubebrain/txn-quota-batch.v5jQtJo5/`。服务层配额/Put/Txn/
损坏告警专项也已通过（44.749 秒）。提交前后均通过基准普通/race 各三轮、配额专项
普通/race 各十轮、backend/server-etcd/storage-tikv 完整普通/race/vet、规范后端
协议集成普通十轮/race 三轮，以及 720 项生产测试清单与全部四分片；最终源码摘要一致。
提交后四分片为 468.869/530.909/394.662/761.400 秒。
门禁证据：`/root/.local/state/kubebrain/txn-quota-batch-gates.qH6NMSd6/`。
本候选以 `9a0fdd3b` 发布，后端 CI `34522265523` 和镜像 CI `34522265522`
均成功，实际镜像版本、客户端依赖及双架构摘要独立核验通过。真实滚动升级到
`sha256:edeb87ad2353c4f724449e905e2784bee3a2f9dfab95913855c6b3df3f4a1d60`
后，三个副本完成更新，但探针未在滚动完成后的原 900 秒期限内完成 6000 次操作，
执行 exit 1，**严格验收仍未通过**；没有放宽公共 5 秒、直连 30 秒和 0.1 秒间隔。
自动恢复修复版 `sha256:0ce85e66320b27bf4cdb8f11981a76cba58926fa0e835fc47dc663f709b588ce`，
最终 generation/observed 20/20、Ready 3、current/update revision 均为
`kubebrain-855b5bfb88`，三个 Pod 实际镜像一致、重启计数为 0；本轮探针及镜像 holder
均已删除，日志包含测试数据无残留和 `PREPULL_CLEANUP_CONFIRMED`。
证据：`/root/.local/state/kubebrain/tk-001-003/txn-quota-9a0fdd3b.ewMYJKjv/`。
同一 server2 进程两组指标增量：533 次 Put apply 平均 74.793 ms，847 次 backend
commit 平均 43.705 ms；store 2005 的 Prewrite/Commit 平均 17.729/18.883 ms。
这些是采样区间诊断，既非受控 A/B，也非磁盘根因证明；本次批读尚不足以解决验收超时。
上述调用数变化不是集群延迟或 900 秒验收通过证据。
并发保护补充：新增测试在批读取得旧 usage=0 后模拟其他写入将 usage 改为 40，
要求本次事务比较失败后重新准备、最终 usage=42，且只分配一个公共修订号；相关
用例普通及 race 各十轮通过。私有 overlay 移除事务内 usage 比较后，用例明确因
usage=2 而非 42 失败，确认不能以准备阶段批读代替提交原子保护。正式源码保留比较。

Put 去重读取的否定性验证（2026-09-10）：私有测试 overlay 在启用 etcd 元数据的
memkv 后端中保留对象、删除其修订索引，比较 Get/Update 与直接无条件 TxnApply。
正常索引两条路径都通过；旧版孤立对象上，现有 Get/Update 会修复索引、保留创建
修订号并更新为版本 2，直接 TxnApply 则返回 Created=true、PrevRevision=0，重置
创建修订号及版本为 1，违反此次替换所要求的等价性。现有路径 race 10 轮通过，
直接替换的反例在 race 3 轮均复现语义断言失败。因此未采用该捷径；正式产品代码
未修改。证据：`/root/.local/state/kubebrain/put-orphan-review.zzJss4TS/`。
这是后端隔离诊断，不是修改后公网 RPC 测试或真实集群损坏证明；后续优化须保留
旧索引恢复、损坏检查和事务内保护，且不能据此宣称升级耗时已有改善。

真实消费者 watch 补充（2026-09-10）：修复版 `0ce85e66` 基线上，规范入口
`hack/dev/apiserver-watch-soak.sh` 完成 20 个 ConfigMap、每个 10 次更新，更新前
空闲 60 秒，禁止脚本重启 watch；执行及清理均 exit 0。保留事件明细后的独立核验
确认 20 条初始 ADDED、恰好 200 条 MODIFIED，每个对象版本严格为 1–10、UID 稳定，
更新修订号全局严格递增。测试前缀为空、租约恢复为 0，临时 PKI、进程和端口已清理。
证据：`/root/.local/state/kubebrain/consumer-watch.HaIZeNBz/`；逐事件核验目前是私有诊断，
尚未纳入规范入口或 CI。此次仅为短时持续性回归，不是数天级 soak、KWOK 规模、
故障重连或升级验收；未部署新镜像，不改变下述 900 秒验收失败结论。

真实消费者补充（2026-09-10）：现有独立 Kubernetes v1.36.1 apiserver smoke 连接
`tk-001-003` 的真实 KubeBrain/TiKV 修复版基线，TLS 下完成基本读写、ConfigMap watch、
标签/字段选择器、分页、批量删除、Secret、Lease 和零副本 Deployment 操作，脚本 exit 0。
独立复核测试前缀为空、租约恢复为 0、临时 PKI/进程/端口已清理，提取的测试二进制已删除。
这是基本接入证据，不是 KWOK 规模、长时间 watch、默认重试的真实 TiKV 故障验证，
也不替代失败的 900 秒升级验收。运行版本仍是修复版 `0ce85e66`，未部署下面的新测试镜像。
完整作用域和证据见 [测试环境记录](test_environment_tk_001_003_cn.md)。

客户端测试维护进展（2026-09-10）：fork 提交
`6539bbb040f152980492281148d9d7dd289473b0` 已推送至 `kubebrain-v2.0.7`。
该提交前后均通过 fork build/vet/unit/race、专用 mock 1PC 普通 10 轮/race 3 轮，
以及 KubeBrain 各 720 项四分片门禁和摘要核验。但 CI 34483731290 **失败**：
security/test 作业成功，onepc 的普通用例通过后，非 root Runner 无法清理复制而来的
只读依赖目录，后续 race 未执行。不能将本地 root 下通过等同于 CI 通过。
当前修复只给私有临时副本的目录恢复所有者写权限、不跟随符号链接；非 root 回归验证了
成功/失败退出码保留、嵌套只读目录删除及外部链接目标不被修改，并有失败复现和反向测试。
清理修复已提交并推送为 `2155365951a870077becc05af4a49c8ed90231bf`。
该修复提交前后均通过非 root 清理契约、fork build/vet/unit/race、专用 mock 1PC
普通 10 轮/race 3 轮，以及 KubeBrain 各 720 项四分片门禁和最终源码摘要核验。
新 CI [34490393381](https://github.com/fivetime/tikv-client-go/actions/runs/34490393381)
已全部通过：test/security/onepc 三个作业均成功，日志确认非 root 清理契约通过，
且 onepc 普通与 race 两轮命令都执行成功。这关闭了旧 CI 的临时目录清理问题。
清理修复门禁证据：`/root/.local/state/kubebrain/onepc-cleanup-gates.WzjzlOOO/`。
KubeBrain 产品依赖仍固定在 fork `832b70fd622f`，未启用生产 1PC。
证据目录：`/root/.local/state/kubebrain/onepc-integration-gates.5V2i9vl7/`。

另在仓库外原型中，以真实 KubeBrain 存储适配器/后端连接 unistore，按调用上下文仅对
用户 1PC 提交丢弃已成功的响应，已验证 `ErrUncertainResult`、已提交见证解析指标、
两键同修订号及下一次写入只增加一个修订号；普通一次和三轮 race 通过。
后续补齐发送前失败：解析为未提交、两键均不存在、公共修订号不前进、下一次写入复用
预留修订号；两种情况普通 10 轮与 race 3 轮通过。进一步注册真实后端 watch，验证已提交
两键同批同修订号，并以随后一次确认成功的写入作为有序边界，检查此前没有额外事件；
未送达场景的首个事件只能来自下一次写入。该扩展普通 10 轮/race 3 轮通过。
初版 watch 断言混淆内部 CREATE 与对外 etcd PUT，失败后按现有适配层映射修正；未改产品代码。
隔离 overlay 删除已提交解析路径的事件发布后，测试在公共修订号恢复断言处失败，
说明能够检测该解析回归；另一个 overlay 仅发布两键中的一键，测试明确在“两事件同批”
断言处失败。两次反向测试均未修改实际仓库源码。该原型仍不是正式集成回归、外部 etcd Watch API 验收或真实 TiKV
持久性证明；尚缺默认重试开启路径和真实环境验证。
代码/日志在 `/root/.local/state/kubebrain/backend-onepc-integration.HabFHNJ0/`。
上述原型已纳入 [`hack/backend-integration`](../hack/backend-integration/README.md)，
提交 `5768d701ed8804918bd5e4d040de32a09f909cbe` 已推送至 dbaas：
独立模块使用与产品完全相同的远程客户端固定版本，入口拒绝本机替换、版本漂移和任意
端点/测试参数，兼容补丁仅用于临时 mock 依赖副本。已增加 self-hosted CI 作业；
该提交前后均通过入口与非 root 清理契约、专用 mock 普通 10 轮/race 3 轮、
后端与 TiKV 适配器完整普通/race/vet，以及各 720 项生产工具四分片门禁、最终源码摘要核验。
新增后端 CI [34498234277](https://github.com/fivetime/kubebrain/actions/runs/34498234277)
已全部通过，包括非 root 清理、入口契约、普通 10 轮和 race 3 轮。同期镜像 CI
`34498234203` 也已全部成功；未部署此测试变更镜像。以上仍不替代真实 TiKV 验收。门禁证据位于
`/root/.local/state/kubebrain/backend-onepc-canonical-gates.Y33UuAPw/`。
默认重试补充诊断（历史隔离阶段）：unistore 原虚拟地址不能响应真实 gRPC 健康检查，
首轮两例均只发生一次 RPC 并失败；提供本地健康服务、注册相同 mock StoreID 的可达地址后，
默认重试确实发生。发送前丢失经重试成功，提交后丢响应经重试仍返回不确定结果并由见证解析。
两个默认重试场景及原两个禁用重试场景合计通过普通 10 轮/race 3 轮，断言实际健康探测、
恰好两次提交 RPC、起始时间戳不变，以及原有修订号/watch 批次边界。
证据：`/root/.local/state/kubebrain/backend-onepc-retry.NV9YlpqB/`；数据 RPC 仍是 mock，
不是完整真实 TiKV 网络重试或持久性证明。
后续默认重试扩展已提交并推送为 `eddc122caea484a9ad7bb933174d963a7cf23c22`，
补充禁用重试严格一次、默认重试严格两次、禁止提交时间戳漂移和成功路径不得走不确定解析
等断言。其提交前后均通过四场景普通 10 轮/race 3 轮、后端完整普通/race/vet、入口/清理
契约、各 720 项生产工具四分片门禁和最终源码摘要核验。新增后端 CI
[34505205282](https://github.com/fivetime/kubebrain/actions/runs/34505205282) 全部通过；
镜像 CI `34505205098` 已全部成功，未部署新镜像。证据位于
`/root/.local/state/kubebrain/backend-onepc-retry-gates.Xp3gzfnd/`。
前述已通过 CI `34498234277` 只覆盖原来两个禁用重试用例，不能作为本扩展通过的证据。
生产协议、集群配置和下述 900 秒验收失败结论均未改变。

最新终态（2026-09-10，c7d9905e 正式复验）：配额批读优化的发布源码
`c7d9905e57526c4d2868f96805b7e276f37888c0` 已通过 CI 34470793561，
实际镜像身份、两种架构发布及非 root 运行配置已核验。但原 6000 次操作、
public 5s / direct 30s、滚动完成后 900s 完成窗口的升级测试仍因完成窗口超时失败。
执行器 exit 1，未取得完整 PROBE_SUMMARY，不认定通过或性能改善。
已核验自动回滚后的 generation/observed 18/18、Ready/updated 3/3，
current/update revision 均为 `kubebrain-855b5bfb88`；三 Pod 实际镜像及 imageID
均为修复版 `sha256:0ce85e66320b27bf4cdb8f11981a76cba58926fa0e835fc47dc663f709b588ce`，
无删除标记且重启数为 0。fixture 全零，探针及三个本轮预拉取 holder 已清理。
同一候选进程两次采样间 603 次 Put 平均约 87.17ms、959 次批提交约 45.77ms；
与上一轮采样涉及不同 TiKV store，不能据此作受控 A/B 结论或把存储耗时归因于物理盘。
下一步继续定位串行操作耗时，并在已通过基本消费者 smoke 的基础上开展长时/规模回归；
不盲目重跑或放宽门限。
提交协议优化的前置缺口：固定 fork `832b70fd622f` 的 `integration_tests` 是独立
Go 模块，不能用根模块 `go test ./integration_tests` 覆盖。进入该模块执行
`go test -mod=readonly . -run '^TestOnePC$' -count=1 -timeout=180s -args -with-tikv=false`
在测试执行前失败，提示需要更新 go.mod；这不是 1PC 行为测试失败，也不是通过。
源码中的 `Test1PCLinearizability` 使用 `begin()`，而非显式启用 1PC 的 `begin1PC()`，
且没有断言实际使用 1PC，不能仅凭用例名称宣称覆盖该协议的一致性。
后续若评估 1PC，需先修复隔离测试依赖/协议断言，再覆盖跨 Region 回退、响应丢失的不确定
提交及 KubeBrain 持久见证解析，最后做真实环境测量。当前没有启用 1PC 或 async commit。
隔离诊断补充（2026-09-10）：已复现上述覆盖缺口的 RED/GREEN——给原用例增加
实际 1PC 断言即失败，改用 `begin1PC()` 并增加提交后新事务的时间戳/读可见性检查后，
mock-store 用例通过 10 轮。旧 TiDB 测试依赖另有 `runtime.buildVersion` 私有链接问题；
仅在仓库外副本中改用 `runtime.Version()` 并对齐测试依赖后，默认链接检查及
`-mod=readonly` 下再次通过 10 轮。没有修改模块缓存、正式 fork 或生产提交协议。
KubeBrain 原有不确定提交见证/错误分类专项另通过三轮 race；这些证据不覆盖真实 TiKV
1PC 响应丢失、持久性或性能，不能据此启用 1PC。诊断输入、结果及后续条件见本机
`/root/.local/state/kubebrain/onepc-review.sQ6wPqWW/REVIEW.md`；副本不作为权威源码。
响应丢失诊断补充：隔离 mock 新增发送前失败和服务端 1PC 提交后丢失响应两例，
用普通传输错误验证客户端自行分类为不确定结果，并核对数据/见证两键的全无或全有。
两例通过 10 轮；整个修正后的 TestOnePC 组通过三轮 race（默认链接检查、只读依赖）。
临时移除客户端针对 1PC 的不确定分类，两例均在预期断言失败，证明能检测该回归。
测试禁止隐式成功重试，未覆盖开启重试后的恢复；也不等于 KubeBrain 实际解析器、
真实 TiKV Raft 持久性或性能验收。新增测试仍在上述隔离目录，尚需规范化集成到 fork。
以下均为历史阶段记录，集群现状以上述终态为准。

本地进展（历史）：`260d51e17b9b4c3d99ee95978770645d4c299425` 将启用配额时的
QuotaStatus 三个元数据点读合并为一次批量读取，固定快照及无批读能力的路径保持原行为。
提交前后各 720 项生产测试、inventory、专项 race/vet 均已通过；新增基准仅量化
本地存储 API 调用，不证明 TiKV 延迟改善。当时发布和真实环境验证尚未完成；后续结果见上文。

上一轮实际终态：scale-lab 整合已推送，发布源码 `3b15bd16` 的 CI
[34459431691](https://github.com/fivetime/kubebrain/actions/runs/34459431691) 全部成功。
从维护修复版基线执行的原 30s/900s 正式升级复验失败：候选三副本滚动完成，
但探针未在滚动完成后的 900s 内完成 6000 次操作。已自动回滚修复版 dd339bc1，
generation/observed 16/16、Ready/updated 3/3、current/update revision 均为
`kubebrain-855b5bfb88`，实际三 Pod 镜像及 imageID 均为
`sha256:0ce85e66320b27bf4cdb8f11981a76cba58926fa0e835fc47dc663f709b588ce`。
fixture 全零，探针和本次预拉取 holder 已清理。原正式验收仍未通过，不重复盲跑或放宽预算。

本轮新增局部证据：维护修复版旧 Pod2 同 UID 状态确认 exit 0 Completed，日志包含
三端口 root server shutdown 与 shutdown complete；真实候选三副本客户端指标采样通过。
同一进程两次采样间 896 次 Put 的服务端平均耗时约 79.93ms，1402 次批提交约
41.24ms；这不是尾延迟或根因证明，各层指标不可直接相加。详细证据和边界见测试环境交接记录。
现有 Kubernetes 接入与 KWOK 测试代码应复用并针对本环境回归，不应描述成尚未开发的能力。
以下“最新/当前”字样均为保留的历史阶段，以上述正式复验回滚终态及测试环境交接记录为准。

2026-09-10 最新终态：dd339bc1 的 CI 34417282642 和镜像验证全部成功，但真实升级
在 iteration=117 因 direct watch 恢复超时失败。已确认回滚原 a245c95f 镜像，
generation/observed 13/13、Ready/updated 3/3。旧实例仍以 15s 超时 exit 1；
候选实例在回滚时取得同 UID exit 0 的证据，局部退出改善不等于升级验收通过。
以下状态是历史阶段；完整新时间线见测试环境交接记录。

当前最新：TLS 最终关闭循环已复现并修复于 `12b4aedc`，提交前后各 720 项生产测试、
inventory、endpoint race/vet 全部通过，准备推送并恢复 CI，尚未部署。
4f2 的第二次受控升级仍因 direct watch 恢复超时失败，但取得旧进程耗尽 15s
退出窗口的明确日志与同 UID 终止状态；已确认回滚终态 generation/observed 11/11、
Ready/updated 3/3、原 a245c95f 镜像。以下记录均为历史阶段，详细证据见测试环境交接记录。

最新补充：4f2d2402 的 CI 34407926007 已全部成功，镜像身份和只读部署前检查通过。
实际受控升级却在 iteration=116 因 direct watch 恢复超时提前失败，未完成指标采样。
已回滚原 a245c95f 镜像，generation/observed 9/9、Ready/updated 3/3，临时资源清理完成。
探针超时早于候选容器启动，事件显示 Pod 替换期间的临时卷重建和挂载/启动间隔；
镜像已缓存，不能把本次失败归因于业务 Pod 下载镜像或候选启动后的指标注册。
以下 34057ee8 与待发布文字保留上一阶段记录，当前终态以本段及测试环境交接记录为准。

最新终态：34057ee8 在线升级验收失败，原因首先是探针未在 900 秒完成窗口内结束；
已回滚原镜像并确认三副本 Ready/updated，隔离 holder/探针清理完成。
generation/observed 7/7，current/update revision 回到 `kubebrain-696c87f8f9`。
下文升级执行中的观察是历史中间状态，不代表当前仍运行或通过。完整时间线见测试环境交接记录。

后续诊断提交 `36545f8107ca9231db01df54d8f0b7f270679f2b` 增加有界探针进度和阶段耗时，
未修改原有 SLO/次数/间隔。提交前后各 720 项四分片、inventory、探针包 race/vet 已通过；
CI [34394834203](https://github.com/fivetime/kubebrain/actions/runs/34394834203)
以 docs-only 后继 `34057ee8743c28753f9a078268a12783381dbe04` 构建发布成功，
索引为 `sha256:f4fc874cf18633b2cc65afc52113d1c98f20bf63439929ea95238408cd94b637`。
实际新候选预拉取及滚动覆盖通过，进度诊断已运行，但完整探针仍超时；不据此认定已修复。
下一步是细分 Put/存储提交耗时并核对串行负载与完成预算，不降低 SLO 或操作次数来覆盖失败。

后续产品提交 `826fd14cb5854b9d0da5a8cdc155fdcc9b49b36b` 修复 TiKV 客户端指标
未注册到默认 registry 的监控缺口。提交前后各 720 项四分片及 inventory 均通过，
适配器 race/vet 通过；新版本 CI 发布、真实指标采样及升级验收尚未完成。
这不改变上述回滚后的服务版本，也不证明 Put 耗时根因已关闭。

用户授权使用 `root@10.32.32.66` 控制的 `tk-001-003` 集群，并明确只使用 rook-ceph 消费者存储、禁止使用 rook-ceph-secondary。连接路径、安全边界、资源 UID、日志和执行结果统一记录于 [tk-001-003 测试环境交接记录](test_environment_tk_001_003_cn.md)。凭据与验收工具保存在仓库外私有目录，不写入本页。

## 历史阶段快照：84d22dcc 发布及升级启动时

以下表格及“下一步”保留当时证据，不表示当前进程仍在执行；最新终态以上文及测试环境交接记录为准。
测试通过不能跨版本、拓扑或故障范围外推。

| 验收项 | 已取得的证据 | 仍未证明的范围/下一步 |
| --- | --- | --- |
| 分支与源码对标 | 本轮待发布的产品/测试提交为 `a98b6db1a5ff2d186bb8730d1b59f0558ec60d9d`；推送前远端为 998b977e，生产逻辑仍与 05032758 相同；本地只读 etcd 基线仍为 `5cd9f4ee13801e18825d661e5005ae599460bc3a`，etcd 工作树干净；当前 go.mod 的 etcd API/client/server 为 v3.7.1 | a98b6db1 的提交前后检查已全过；发布源码以随后新 CI 的完整 headSha 为准（可含 docs-only 记录），固定对标版本不表示已完成支持版本矩阵 |
| 实际服务版本 | KubeBrain 三副本已部署，当前仍使用 `339381af` 的不可变镜像 `sha256:a245c95fea36c387358d86e3808a9d29073a327028d5a4e3a80e4d272663e865`；StatefulSet generation/observed 为 3/3、current/update revision 均为 `kubebrain-696c87f8f9` | 尚未部署本次 05032758 产品候选，不能将本地修复视为线上已生效 |
| 副本与放置 | 新鲜只读查询：KubeBrain/PD/TiKV 各 3/3 Ready、各容器 restart 0，同组件三副本分布在 `k8s3-worker1/2/3` | 节点无 topology zone/region 标签；物理宿主机及跨可用区独立性未证明，节点/网络故障须先确认授权范围 |
| 数据卷与后端健康 | 六个 PD/TiKV 数据 PVC 均 Bound，使用 `nvme-rep3-rbd-pool`；driver 为 `rook-ceph.rbd.csi.ceph.com`、clusterID 为 `rook-ceph`；本轮原样后端健康门禁 exit 0，含六卷实际 CSI 身份/容量隔离/Retain 与连续三次无异常 Region 检查 | StorageClass 默认 reclaimPolicy 仍为 Delete，不能与六个现存数据 PV 的 Retain 混为一谈；新增数据卷仍需逐卷验证保留策略；瞬时健康不等于故障恢复和长期稳定性 |
| 已有受控可用性验收 | 当前服务版本此前通过受控 leader Pod 删除：900/900 操作、public watch 900、三个 direct watch 各 900、lease 存活、官方 etcdutl restore 校验 | 不覆盖无主动释放的崩溃、节点失联、网络分区，也不是新候选升级验收 |
| 冷镜像在线升级 | de8a9e1f 的真实冷升级曾因 direct watch 超过 30s 门限失败并回滚；镜像拉取约 36.973s | 失败仍未关闭。05032758 增加隔离预拉取、运行时摘要核验与失败清理，但尚需新候选真实准备及完整升级验证，不能仅以缓存已热的重跑证明修复 |
| 当前候选本地门禁 | 05032758 提交前/后各 720 项四分片及 inventory 均通过，组件/CLI/build race 通过；已预编译恢复工具并验证缺少镜像回执时入口停止 | 本地测试不替代实际控制器、CRI 拉取、真实业务探针和回滚清理证据 |
| 当前镜像发布 | CI [34382354317](https://github.com/fivetime/kubebrain/actions/runs/34382354317)（源码 `84d22dcc424245128f05802a424ee598e7a20364`）全部成功；独立核对索引/双平台摘要、promotion、实际 amd64 版本/标签和 fork 客户端模块后已建立镜像回执；索引 `sha256:b3b5c25ac815f5b9388be6aa3f987a3dd6c602e3378f4fb142b6fa1f822ca725` | 正在执行只读集群升级前检查，尚未创建 holder/探针或切换业务镜像；镜像发布成功不等于升级或生产就绪验收成功。两次旧失败和本次 Node 弃用警告保留 |
| 总体验收 | 原环境的兼容性、恢复和压力实验仍作为各自范围的历史证据 | 新环境真实 apiserver 路径、新版本在线升级、后端 TLS/轮换、数天级 watch/故障恢复 soak、生产规模/版本矩阵，以及管理面、计量和外部系统验收仍开放；以完整兼容性计划逐项验收 |

本轮只读证据位于私有交接目录的 `security-prepull-acceptance-{serving-state,pods,storageclass,nodes}.json`；
最新执行补充：84d22dcc 的新候选准入检查与正式隔离准备/再次核验均通过，6000 次探针和
滚动升级已实际启动。当前正在从表中旧服务基线切换到新索引，最近中间状态为 generation 4、
Ready 3、updated 2；尚无完整升级与清理成功结论。执行日志为
`security-prepull-84d22dcc-upgrade.log`，进程句柄 93250，恢复 journal 为
`prepull.YczW7ofYlsNf/attempt`。切勿重复执行旧基线入口。

后端健康日志为 `security-prepull-acceptance-backend-health.log`。该日志的 `max_disk_used_percent=90` 是拒绝阈值，
不是实际磁盘使用率。当前交接证据不依赖旧 `/tmp` 镜像归档，但仍需按正式发布要求持久归档。

下一步：完成精确集群/存储/健康身份与新候选准入检查，执行隔离预拉取和
6000 次探针的真实在线升级；其后继续完整计划中的恢复、TLS、真实消费者和长时间验收。未获得授权前不操作
worker、PD/TiKV 重启或网络故障，不使用 secondary Ceph。进行中的状态文档暂不 push，避免取消原 CI。

## 历史基线：A5788 / kind（2026-09-08）

以下表格和收尾记录仅描述当时的 kind 环境，其单节点、hostPath、镜像和部署待办不能套用到当前 tk-001-003。

| 验收项 | 当前证据 | 尚需取得的证据 |
| --- | --- | --- |
| 分支与源码对标 | 当前为 `dbaas`；本地 etcd 基线为 `5cd9f4ee13801e18825d661e5005ae599460bc3a`，etcd 工作树干净 | 支持版本窗口内的完整差分结果；本地 HEAD 不代表已查询远端最新版本 |
| A5788 镜像引用门禁 | 代码 `aca51b7f`；三个 RED 已复现误放行；修复后 focused/race、完整 readonly probe、提交前后四分片通过；候选 generation 1011 完整门禁及回滚后 generation 1012 稳定适用门禁通过，均 3/3 Ready、restart 0 | 本轮子项已完成；不证明整个产品已生产就绪 |
| 数据面副本健康 | 3 KubeBrain、3 PD、3 TiKV 当前 Ready | Ready 只证明当前服务状态，需另外验证隔离、恢复、故障历史和 SLO |
| 故障域隔离 | 集群只有 `kubebrain-dbaas-control-plane` 一个节点，无 zone 标签；6 个 PD/TiKV Pod 均运行在该节点 | 真实独立节点/故障域部署及对应分区、节点故障、恢复测试；同宿主多容器不足以证明跨可用区能力 |
| 持久卷与容量隔离 | 唯一 StorageClass 为 `standard`，provisioner 为 `rancher.io/local-path`；PD/TiKV 数据 PV 为 hostPath，共享宿主文件系统 | 独立 CSI 卷的 driver/volumeHandle/claimRef、实际容量及剩余空间通过严格存储门禁 |
| 存储生产门禁 | 清理后原样执行 `KUBE_CONTEXT=kind-kubebrain-dbaas hack/production/validate-tikv-region-health.sh` 仍返回 1；磁盘压力已消除，6 个卷各有 CSI 身份和容量隔离错误 | 修复实际基础设施后原样通过；不能放宽阈值或改写 hostPath 为 CSI 元数据 |
| 恢复与长期验证 | 计划 P2 明确保留真实 CSI target retirement→provisioning→durable restore、加密 key promotion/撤权、版本矩阵、生产规模和长时间 soak | 对每个开放项提供新鲜、可追溯的真实执行结果；已有实验结果不能外推未测拓扑/规模 |
| 管理面与计量 | 计划 P1 仍列出跨 cluster/region 调度、管理面/外部 IdP HA soak、预生产 Prometheus evaluation/连续 24 小时采样及外部财务系统真实回执等开放项 | 本轮未验证这些外部系统；须按完整计划分别取得实际证据，不能由数据面门禁替代 |

下一步顺序：完成[安全升级门禁](security_baseline_20260908_cn.md)，取得新基线 CI 镜像后部署 KubeBrain 并完成客户端语义测试，再推进 P2 恢复、故障历史与长时间测试。原 kind 环境及其 hostPath 卷保留，不迁移或覆盖；新环境固定使用仓库外独立 kubeconfig。三 worker 的物理宿主机/可用区独立性尚未证明，节点级故障还涉及用户已有工作负载，需先确认具体授权范围。

原 kind 部署入口复核：`deploy/production/kubebrain.yaml` 和 `deploy/production/tidb-cluster.yaml` 均要求同组件副本按 `kubernetes.io/hostname` 硬反亲和，不能在该单节点上满足三副本调度；zone 规则仅为偏好，模板本身不证明跨区放置。该 kind 集群 `CSIDriver` 数量为 0，CRD 清单没有 VolumeSnapshot、Prometheus 或 ServiceMonitor API。现有模板仍需目标环境注入镜像、存储、证书、网络与监控配置，不能直接 apply 到实验实例。上述是相应部署/恢复/计量验收的环境前置缺口，不应记作已经复现的 etcd API 语义缺陷，也不代表所有产品代码工作都依赖这些环境。

2026-09-08 构建清理：按明确 ID 回收 84 条 KubeBrain 编译缓存（约 348 GB），删除 1,744 个旧 OCI/解包临时文件（41,548,937,075 bytes）及 3 个本地编译二进制；未删除源码、测试记录或数据卷。根分区可用空间由约 1.7 GiB 恢复至 327 GiB；本轮镜像导入后约 324 GiB。旧实施记录中的 `/tmp` 镜像归档位置是历史位置，清理后不再代表可恢复实物；对应构建产物需要从记录的提交重新生成。

当前 OCI 和日志保存在 `/tmp` 的 tmpfs 上，只能视为本次运行的临时证据。重启或清理会丢失，生产验收还需要持久保存镜像、日志和验收结果。

A5788 回滚终态：StatefulSet generation/observed 为 `1012/1012`、resourceVersion `9119057`，22 个启动参数不变；runtime 恢复稳定摘要 `sha256:bc6b443ff3508482908155bfaf234d924305f1dce215fd8f7de14093e83d899b`。候选与稳定门禁的 revision/HashKV 均为 `75044/1984703050`，term 随滚动选主由 `894` 变为 `896`。候选 CRI container/image、containerd 镜像引用及四份关键 manifest/config content 已清零；六个临时端口监听均已关闭，PD/TiKV 六副本仍 Ready，三个 store 为 Up。未触碰其他轮次运行时索引。
