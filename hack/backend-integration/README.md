# 后端 TiKV 协议集成回归

本目录维护根模块的本机真实 PD/TiKV 测试入口；旧独立 SQL mock 模块已退役。
不接入 Kubernetes，不需要 kubeconfig。

当前入口共 23 个真实协议用例；下文的“第 N 例”是历史加入编号，不代表
当前执行顺序。两个单 Region 后端响应丢失用例先于主动 Region 分裂执行；
后续增加用例时以入口中的测试名清单及明确 PASS 回执为准，不累加历史轮次。

启动实验入口：命令行新增 `--experimental-tikv-enable-async-commit`，默认
`false`，与 `--experimental-tikv-enable-1pc` 互斥。冲突配置在创建客户端前
拒绝，且不修改 SDK 全局配置。它仅在进程启动时设置 SDK 默认值，影响该
进程的所有新事务（包括内部事务），不是热切换；恢复默认需要移除开关并
重启。下述事务级 opt-in 回归不能代替全进程模式验证或生产准入，不要在
业务集群开启。当前专用集群也未启用该开关；远端对照实验需另行确认范围
和恢复方案，不能仅因编译／单测通过而部署启用。

## 本机真实协议与 CI 门禁

`backend-integration.yml` 新增独立 `real-protocol` 门禁：在可信 self-hosted
Linux amd64 Runner 上拉取入口使用的固定镜像摘要，运行构建契约、vet、
入口列出的真实协议普通版与 race 版，以及两个启动阶段的中断清理测试。每次入口
使用全新的临时集群，不能用同一前缀的 `-count` 重复 Region 分裂测试。
主 CI 通过仓库内可复用工作流调用同一门禁，旧 mock 门禁已移除。
Runner 在 `a5f8e0bd` 的作业 `103343113136` 已完成：普通/race 各十项
明确 PASS，两阶段启动中断清理通过；完整日志已留存，不将单副本临时测试
描述为生产性能或多副本持久性验收。

第十八例 `TestRealTiKVAsyncExperimentReadsBeforeCommitCleanup` 是独立协议实验，
需要额外的 `KUBEBRAIN_TIKV_PROTOCOL_ASYNC_EXPERIMENT=1`；本机入口只为该例
设置此值。夹具初始化与清理仍用 2PC，仅目标事务使用 SDK 的事务级 async
commit 开关。真实分裂两个 Region，要求两个 Region 的 Prewrite 都接受 async
commit；路由错误重试单独计数，不将请求尝试数当作 Region 数。该事务的全部
Commit RPC（含主键）被阻断时，批次必须已成功返回，读取两键必须得到提交值
并查询异步 secondary lock；还要确认后台主键清理未被计入前台主提交指标。
测试结束释放所有后台 RPC，再按原 ownership fence 清理。模拟 TiKV 的响应
不提供所需的 `MinCommitTS`，不能用该 mock 证明 async 路径，也不能伪造响应
替代真实实验。此例不启用任何产品开关、不改变专用 Kubernetes 集群，不证明
多副本持久性、故障恢复或性能达标。此例还固定旧快照，再让另一 2PC 事务
更新 CAS 目标键；异步批次必须实际进入 Prewrite 后返回 CAS 冲突，且同批另一
Region 的写入不能发布。该冲突覆盖不替代完整后端 fence／响应丢失验证。

第十九／二十例 `TestRealTiKVAsyncExperimentLeadershipConflict`／
`TestRealTiKVAsyncExperimentRestorationConflict` 复用完整后端防护夹具和
640-key 有界清理预算，同样需要上述额外实验许可。只在带实验上下文的目标
事务执行 Atomic 回调时启用事务级 async commit，初始化、竞争事务及清理
仍为 2PC。真实预取后更改对应防护 token，原事务必须实际发出 async Prewrite，
返回对应 LeadershipFenced／RestorationFenced，且该事务 Commit RPC 为零。
沿用原夹具断言：用户索引、对象及事件不存在，公共修订号、持久修订号和
配额不变。这是两类安排好的事务竞争，不是响应丢失、故障恢复或性能验收。

第二十一例 `TestRealTiKVAsyncExperimentBackendPublication` 沿用完整防护成功
路径，只有三次带测量上下文的用户事务启用 async commit。每个事务都要收到
真实 TiKV 的正 `MinCommitTS`；Region 错误重试单独核对，不能将静默回退
当作 async 成功。验证连续修订号 101–103、重复更新同一键的写后读，以及
领导权初始化扫描持久见证后修订号／最终值一致且无损坏告警。后台主键
Commit 不进入前台主提交观测；默认 2PC 例保留原服务端主提交明细断言。
该用例不注入故障，不覆盖独立 Watch 历史、进程重启或多副本持久性。

第十八例后续还覆盖同一 Region 的双键 async Prewrite 成功响应丢失：仅在
真实服务端接受目标事务、返回正 `MinCommitTS` 后丢弃一次响应并取消调用者，
适配器必须返回 `ErrUncertainResult`，新上下文读取仍须得到两键完整提交值。
断言被丢弃响应对应两项 mutation，不能把部分跨 Region 预写误当作已提交。
辅助键由独立的精确键删除和同事务 owner CAS 清理，原固定清理清单和最终
空前缀断言不变。该场景验证存储适配器与 SDK 读取恢复，不等于完整后端的
持久见证不确定结果解析或多 Region 部分送达恢复。

新增 `TestRealTiKVAsyncExperimentBackendResponseLoss` 把相同丢响应机制接入
真实后端。此例在所有主动 Region 分裂之前运行，且断言
丢失的成功 Prewrite 包含整笔事务的 mutation 数。仅目标事务开启 async；
调用方收到不确定结果并取消后，后台持久见证必须解析为 committed=1、
absent=0，发布同修订号 101 的双键 CREATE 事件，读回两键一致；下一次写入
必须得到 102 和对应 PUT 事件，不能先重复发布已解析事务。初始化／下一次
写入／清理仍是 2PC。该例复用原不确定结果夹具，不安装完整领导权防护，
不替代完整防护下的跨 Region 部分送达、进程重启或多副本恢复实验。

第十八例另加入存储层跨 Region 部分送达：真实 secondary async Prewrite
成功后才丢弃尚未送达的 primary Prewrite，并取消调用者。必须观察到实际
secondary 接受、primary 阻断、目标事务零 Commit RPC；调用者收到不确定
结果后，新的上下文读取两键仍为旧值，不能发布半笔事务。注入器只匹配
目标 startTS 且请求明确启用 async commit；普通 2PC 回退透传，不能消耗故障。
单测还确认 Region/key/transport 错误和非正 MinCommitTS 不释放 secondary
屏障。等待 secondary 有 10 秒上限，不伪造服务端成功响应。它验证
存储层两 Region 的受控失败，不等于完整后端防护、重启或 Raft 故障验收。
该场景随后按两个方向各执行一次：secondary 已接受而 primary 未送达，
以及 primary 已接受而 secondary 未送达。分别核对对应接受／阻断计数，
另一类 Prewrite 接受数必须为零；两种方向都要求零 Commit RPC 和两键旧值。

`TestRealTiKVAsyncExperimentGuardedResponseLoss` 将已提交但丢响应的场景
扩展到完整领导权／恢复防护。入口共 23 例，此例同样在主动分裂前运行，
使用 640-key 清理预算。安装两族 token 并携带领导权 epoch，实际 RPC 要求
成对预取、零防护单键 Get，以及两族 guard mutation；被丢弃的成功 Prewrite
必须包含整笔事务。复用见证解析、双键 Watch／读回、下一修订号和防重复
发布检查。仍只在用户目标事务开启 async，初始化／清理保持 2PC；没有
注入并发领导权切换、跨 Region 部分送达、进程崩溃或多副本故障。

新增 `TestRealTiKVReadBypassesPendingSecondaryCleanup` 单独使用 `2pc` 模式：
在隔离集群分裂两个键的 Region，真实提交主键，客户端仅暂停该事务的次要键
Commit 和后台 ResolveLock。读取仍返回已提交值，且观察到主键状态查询。
这验证后台锁清理无需阻塞该读取，不是证明锁清理没有负载成本；不能将 SDK
ResolveLock 总耗时直接归入前台 Put/Range 延迟，也不能据此启用生产 1PC。
2026-09-11 本地普通/race 各十一例通过，独立检查临时容器、网络、编译测试
文件均已清理。`30480bf8` 的远端作业 `103393789160` 已另行核验：普通/race
各十一例通过，清理及两阶段中断检查通过，不沿用上面的十例历史结果。
后续测试诊断记录原始 Prewrite/Commit 的 WriteDetail 存在性和成功标志；
字段缺失不等于耗时为零，记录的各阶段存在嵌套，不应直接相加。

新增第十二例 `TestRealTiKVBackendProtocolLatency` 使用 `2pc` 模式，固定预热
后进行 20 次后端写入，校验连续 revision、Watch、最终读取和配额。除成功响应
外独立统计标记上下文的实际 RPC 尝试，包含失败尝试；输出固定类型的 Get、
BatchGet、Prewrite、Commit 等计数，不记录键或请求内容。单 Region 写请求尝试
数必须与预期协议匹配，不能以“成功次数正确”掩盖重试。该指标排除未标记及
后台上下文，不是总请求数；本机单副本临时存储的耗时不设置生产性能通过线。
上文十一例历史记录不代表新增第十二例已在远端验证。

覆盖边界：该顺序延迟夹具没有创建生产资源锁及完整领导权／恢复防护，
因此其单 Region 计数不代表生产提交形态，也不能验证只在两个防护均启用
时执行的成对预取路径。`TestProtocolProductionFencedShape` 和
`TestPrefetchedProductionFenceRejectsChangedToken` 补充了三 Region 模拟 TiKV
上的形态与预取后 token 冲突验证，但不是本入口的真实 Raft 用例。完整防护
初始化涉及 512 个分片键；第十四例按下述独立预算覆盖真实防护写入，
不提高旧例的 128-key 上限，也不把模拟结果算作真实集成结果。

第十四例 `TestRealTiKVBackendProductionFences` 显式使用 2PC，关闭 async commit，
通过真实资源锁 Create 安装领导权／恢复防护，完成三次连续修订号的写后读。
标记上下文的实际 RPC 必须包含至少三次成对防护 BatchGet、零次防护单键 Get，
两个防护族分别至少三次预写 mutation；重试可能增加次数，不把次数当作事务数。
此例不强制 Region 布局，不证明多副本故障、真实 token 冲突或生产性能通过。
此例及下面两项完整防护冲突用例采用独立清理预算：精确白名单为本次随机前缀下两族各 256 个小写
两位十六进制分片键，另保留 128 个普通键预算（含 owner），总上限 640。
近似分片名不获豁免；旧例仍只有 128 键。所有权检查、同事务 owner CAS 与
删除、后台退出后清理、删除后缺席确认保持不变。清理单测覆盖上限边界、
超限、近似名、owner 缺失／变化／竞争及无关键保留。

第十五／十六例 `TestRealTiKVPrefetchedLeadershipConflict`／
`TestRealTiKVPrefetchedRestorationConflict` 使用同一安全夹具，各自独立进程
和随机前缀，显式 2PC／async commit 关闭。真实快照 Prefetch 完成后，另一
真实事务 CAS 修改被预取的指定防护 token；原快照仍读取旧 token，原写入
必须因提交冲突返回对应 LeadershipFenced／RestorationFenced。检查用户
索引、对象、事件不存在，revision 可见性不前进，修订号分配器和配额不变。
清理先以精确 CAS 恢复测试改写，再关闭后台与有界删除本次所有权范围。
这是有意安排的真实事务竞争，不是进程崩溃、网络故障或多副本 Raft 验收。
本地十六例 race 已通过，终态 result=0、cleanup_failed=0；证据目录
`/tmp/kubebrain-real-protocol.0qhMwcpnxl/`。旧远端十三例结果不包含新增三例。

第十三例 `TestRealTiKVBackendConcurrentWrites` 显式使用 2PC，在独立前缀中
完成原并发验证后，对已有双键再做两次顺序更新，标记上下文的真实 RPC 必须
恰为 8 次 BatchGet、2 次 Prewrite、2 次 Commit，无 Get 或其他请求／重试。
逐键检查 PrevValue、PrevRevision 和最终读回，锁定旧对象合并读取的实际路径；
计数排除并发阶段、读回及后台工作。内存夹具同时验证追加操作仍在原清理预算内。
这是单 Region 双键事务的读取形态回归，不是远端滚动升级延迟验收。
原并发阶段在独立前缀中
同步放行四个竞争事务，每个事务写入相同的两个键。校验四个唯一且连续的
revision、每个 revision 的双键 Watch 批次、最终两键版本和值一致，以及配额
只计最终键值。后续增加同时放行的十六次 Range，逐次检查结果为空或完整的
同 revision 双键，且值对应某个成功事务；采样不保证每次都覆盖提交交错窗口。
读取还检查调用前已确认写入的最大 revision 下界和顺序读取不倒退；全部写入
返回后再读一次，确保即使并发采样都早于提交，也会执行已确认写入的可见性检查。
与顺序 RPC 计数用例分开，保持各自 100 键余量断言和总计
128 键清理上限。本测试不保证每次调度都产生冲突，也不是多副本故障、完整
线性一致性历史检查或长时间 soak；旧十二例 CI 不能证明新增用例通过。
提交 `0272fabf` 的 CI `34660932207` 已核验普通／race 各十三例通过，两轮
清理成功，PD／TiKV 两阶段中断均退出 143 且确认资源不存在。该结果仍不
替代正式集群升级、性能或多副本持久性验收，也不包含后加的 Range 采样断言。
随后 `7001fad0` 的 CI `34661956517` 已核验普通／race 各十三例以及两轮
清理、两阶段中断清理通过，包含十六次 Range 可见性断言，覆盖边界不变。

首轮 Runner 验证在创建容器前失败：该 Docker 版本拒绝在自动分配子网的网络
上指定静态 IP。本机版本未拒绝，因此保留这次 CI 失败记录。入口现先由 Docker
选择子网，检查本次预留网络的所有权和空容器集合，按确切 ID 移除，再显式
指定相同子网和网关重建；并发分配冲突会失败退出，不复用或修改外部网络。
所有权变化、网络被占用、状态缺失、删除失败及重建冲突均有失败关闭契约。

第二轮 Runner 已越过静态 IP 创建错误，但在 90 秒就绪检查处超时，协议测试
尚未开始；该次日志报告清理成功。当时根因未确定，不能将那次运行算作通过。
入口在就绪失败时输出最后一次 HTTP 错误、PD stores 响应和本次两个容器的
有限状态字段；失败退出清理时还输出各容器最后 100 行启动日志。没有放宽
就绪期限，不打印完整容器环境或宿主配置。

诊断版 Runner 随后确认根因：PD 可访问且仍在运行，TiKV 因容器文件描述符
上限 65,536 低于其启动要求 123,880 而以退出码 1 退出（非 OOM）。入口现给
本次临时容器显式设置 `--ulimit nofile=262144:262144`，不修改宿主 sysctl、
不启用 privileged，也不改变真实 Kubernetes 部署。修复已通过上述 Runner 验证。

```sh
bash hack/backend-integration/run-real-local.sh --allow-local-containers
bash hack/backend-integration/run-real-local.sh --allow-local-containers --race
```

此独立入口仅连接本机 `/var/run/docker.sock`，需要 Linux Docker、Go、jq、
curl、openssl 和 timeout。预先准备脚本中固定摘要的 PD/TiKV 8.5.3 amd64
镜像；入口不自动拉取、不接受外部 PD 地址。它创建一个内部网络及各一个
PD/TiKV 容器，不发布宿主端口，以非 root 用户、只读根文件系统、受限 CPU/
内存和临时内存盘运行；`local-*.toml` 只用于此单副本临时环境，不能部署到
共享集群。单副本、内存盘测试不证明 Raft 多副本持久性或磁盘性能。

入口编译根模块的真实存储测试，分别运行 1PC、丢响应后默认重试、丢响应后
取消，以及后端对已提交/未送达结果的解析与默认重试。每例使用随机前缀和实际集群 ID；
退出时校验本次资源所有权再清理容器/网络，移除自己的编译二进制，保留
打印出的 `/tmp/kubebrain-real-protocol.*` 目录中的日志。失败清理会返回失败，
不能将未知状态视为资源已消失。扩展后的十例普通运行全部通过。
入口还要求每个指定用例输出对应名称的 PASS；SKIP、未匹配用例或
非零退出均不能通过。失败时输出最后 100 行用例日志，保留原始退出码及完整
本机日志；CI 控制台逐例打印确认标记。两个后端默认重试
场景均只重试一次、保持事务时间戳不变、两键只发布一个修订号/Watch 批次，
下一次写入只递增一个修订号。清理函数的 13 个情形和入口参数拒绝已用
无 Docker 的替身测试覆盖，`-race -count=10` 通过；包括所有权变化、查询失败、
资源仍存在、删除失败、日志导出失败、缺失网络状态及符号链接保护。日志失败
仍导致失败退出，但不妨碍删除独立确认归本次所有且为空的网络。
真实 Region 分裂用例在已分组的首次 1PC prewrite 发送前调用 SplitRegions，
要求实际 epoch 错误、多 Region prewrite、两阶段 commit 及相同事务/Watch
语义；额外的 `KUBEBRAIN_TIKV_PROTOCOL_ALLOW_REGION_SPLIT=1` 仅由隔离入口设置。
不要对共享集群运行此用例，删除测试键不会恢复 Region 边界。
启动中断可单独验证（同样需要预先准备固定镜像）：

```sh
bash hack/backend-integration/run-real-local-interruption-test.sh --allow-local-containers
```

该测试在本次 PD/TiKV 创建后分别向自己的入口进程发送 SIGTERM，检查退出码
143、资源实际不存在及编译二进制移除。两阶段实测通过。曾发现信号处理期间
清理标记被重定向进资源 ID 文件，现保留独立输出描述符，并用替身和真实中断
验证修复。它不测试 SIGKILL、Docker daemon 崩溃或宿主机失联。
下方列出保留下来的行为覆盖及有意接受的真实服务端差异。

### 旧 mock 与真实测试的覆盖对照

以下名称均为根模块 `TestRealTiKVBackend` 后缀；不是仅检查进程退出码。

| 旧 mock 场景 | 真实用例后缀 | 必须保持的结果 |
| --- | --- | --- |
| 未送达、禁用 RPC 重试 | `NoRPCRetryUndeliveredOnePC` | 上下文仍有效，仅一次发送，不确定结果解析为未提交，候选修订号可复用 |
| 已提交、禁用 RPC 重试 | `NoRPCRetryCommittedOnePC` | 上下文仍有效，仅一次发送，不确定结果解析为已提交，两键同修订号和 Watch 批次 |
| 未送达、默认重试 | `RetriesUndeliveredOnePC` | 两次发送、相同事务时间戳、成功健康检查、只提交一次 |
| 已提交、默认重试 | `RetriesCommittedOnePC` | 两次发送、相同事务时间戳、成功健康检查、无重复提交或 Watch 批次 |
| 首次 prewrite 前分裂 | `RegionSplitFallback` | 真实 epoch 错误、至少两个 Region 成功 prewrite、两阶段 commit、原子修订号 |

默认重试两例在 TxnApply 前后读取 SDK 的 `StatusCountWithOK` 增量。当前固定
客户端版本仅在真实 gRPC Health/Check 返回 SERVING 后递增该计数；隔离入口
每例独立进程，不以伪造健康服务或成功的数据 RPC 数替代健康检查。补充断言后
十例真实 race 运行通过，两种默认重试均观测到一次成功健康检查。

有意保留的服务端差异：旧 unistore 在提交后丢响应并重试时仍返回不确定结果，
真实 TiKV 8.5.3 则确认成功。因此不能硬编码旧 mock 的返回错误作为真实服务端
规范；两者必须共同满足同一事务、单次持久提交和单次事件发布。真实“已提交但
调用返回不确定”的见证解析由禁用 RPC 重试且上下文仍有效的用例单独锁定。
取消上下文两例、协议 smoke 和原始丢响应测试是额外覆盖。上述 Runner 已验证
这些用例；此对照不证明多副本故障恢复或生产性能达标。

根模块的 `govulncheck@v1.6.0 -test ./...` 已通过，`-show verbose` 结果为
0 个符号级、0 个导入包级漏洞；模块级仍报告未被这些代码导入的
`golang.org/x/crypto/openpgp` 告警 GO-2026-5932，不将其描述为完全没有模块告警。
`build` 中的依赖边界测试显式包含真实协议测试二进制，禁止重新引入整个
`github.com/pingcap/tidb` 包前缀。旧独立模块已移除，根模块扫描保留 `-test`，
不以源文件扫描替代测试代码扫描。

新增的禁用重试两例不取消调用上下文，以对照旧 mock 的普通 RPC 错误场景。
只有显式测试环境变量允许时，测试进程才在 `TestMain` 一次性启用 SDK
failpoint 支持；每例在创建客户端之前设置 `noRetryOnRpcError`，关闭所有
客户端后撤销，不在后台工作期间反复写 SDK 的非原子总开关。默认测试不启用
该开关，普通重试用例检查自己未继承禁用重试状态。此配置不进入产品二进制。

旧 SQL mock 的独立模块、兼容补丁和三个运行入口已退役；历史源码可从
`a5f8e0bd` 的本目录恢复，不保留第二个可误用的当前入口。旧模块扫描曾检出
GO-2024-3284，处理方式是完成真实协议替代后移除依赖，不是漏洞豁免。
历史失败及替代证据见 [验收记录](../../docs/dbaas_acceptance_status_cn.md)。
