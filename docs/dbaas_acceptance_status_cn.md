# DBaaS 验收状态

核验日期：2026-09-11。当前验收环境为 `tk-001-003`；旧 `kind-kubebrain-dbaas` 结果单独保留在本页历史部分，不作为新环境现状。
产品要求及兼容性矩阵见 [兼容性计划](dbaas_compatibility_plan_cn.md)。本页列出当前证据的边界与下一步验收条件，不能代替完整矩阵。

总体状态：**尚未通过生产就绪验收**。已完成迭代编号、提交数和单元测试数量都不是整体完成百分比。

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
